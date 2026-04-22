"""
indexer.py
----------
Reads enriched paper records from Hive via pyhive and bulk-indexes them
into Elasticsearch to power the dashboard's Search & Entity Explorer and
Knowledge Table views.

Handles two indices:
  - research_intel_papers    — from the Hive papers table
  - research_intel_fulltext  — from the Hive paper_fulltext table

Uses pyhive (direct JDBC connection to HiveServer2) rather than PySpark
so the indexer can run from the local Python environment without needing
a Spark cluster. Hive is accessed at localhost:10000 which is exposed
by the Docker stack.

Supports two indexing modes:
  - Full reindex: drops and recreates the index, then indexes everything.
  - Incremental: indexes only records from a specific date partition.

Dependencies: elasticsearch-py, pyhive, thrift
"""

import logging
from datetime import datetime, timezone
from pyhive import hive

from .client import ESClient
from .mappings import (
    papers_mapping,
    fulltext_mapping,
    PAPERS_INDEX,
    FULLTEXT_INDEX,
)

logger = logging.getLogger(__name__)

def _clean_datetime(value):
    """Normalize datetime strings - strip timezone offset and microseconds."""
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()[:19]
    if isinstance(value, str):
        # Remove +00:00 timezone offset
        v = value.replace("+00:00", "").replace("Z", "")
        # Truncate microseconds (keep only up to seconds: 19 chars after T)
        if "T" in v and len(v) > 19:
            v = v[:19]
        return v
    return value


HIVE_HOST = "localhost"
HIVE_PORT = 10000
HIVE_DATABASE = "research_intel"


def _get_hive_connection():
    """Open a pyhive connection to HiveServer2."""
    return hive.Connection(
        host=HIVE_HOST,
        port=HIVE_PORT,
        database=HIVE_DATABASE,
        auth="NONE",
    )


def _fetch_in_batches(cursor, batch_size: int):
    """
    Yield rows from a cursor in batches of batch_size.
    Avoids loading the full result set into memory at once.
    """
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows


class PapersIndexer:
    """
    Indexes enriched paper records from the Hive papers table into
    the research_intel_papers Elasticsearch index.

    Parameters
    ----------
    es_client : ESClient
        Initialized and connected ESClient instance.
    batch_size : int
        Number of documents per bulk indexing request. Default: 500.
    """

    def __init__(self, es_client: ESClient, batch_size: int = 500):
        self.es_client  = es_client
        self.batch_size = batch_size

    def full_reindex(self) -> dict:
        """
        Drop and recreate the papers index, then index all records from Hive.

        Returns
        -------
        dict
            Indexing summary: total_read, total_indexed, total_failed,
            duration_seconds.
        """
        self.es_client.delete_index(PAPERS_INDEX)
        self.es_client.create_index(PAPERS_INDEX, papers_mapping())

        start_time = datetime.now(timezone.utc)
        total_read, total_indexed, total_failed = self._index_from_hive()
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        summary = {
            "total_read":       total_read,
            "total_indexed":    total_indexed,
            "total_failed":     total_failed,
            "duration_seconds": duration,
        }
        logger.info("Papers full reindex complete: %s", summary)
        return summary

    def incremental_index(self, year_month: str) -> dict:
        """
        Index only records from a specific Hive partition (ingest_year_month).

        Parameters
        ----------
        year_month : str
            Partition value, e.g. '2026-04'.

        Returns
        -------
        dict
            Indexing summary.
        """
        start_time = datetime.now(timezone.utc)
        total_read, total_indexed, total_failed = self._index_from_hive(
            year_month=year_month
        )
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        summary = {
            "total_read":       total_read,
            "total_indexed":    total_indexed,
            "total_failed":     total_failed,
            "duration_seconds": duration,
        }
        logger.info("Papers incremental index complete for %s: %s", year_month, summary)
        return summary

    def _index_from_hive(self, year_month: str = None) -> tuple[int, int, int]:
        """
        Query Hive papers table and bulk-index results into Elasticsearch.

        Parameters
        ----------
        year_month : str, optional
            If provided, filters by ingest_year_month partition.

        Returns
        -------
        tuple[int, int, int]
            (total_read, total_indexed, total_failed)
        """
        conn   = _get_hive_connection()
        cursor = conn.cursor()

        if year_month:
            query = f"""
                SELECT paper_id, title, abstract, authors, submitted_date,
                       updated_date, primary_category, categories,
                       citation_count, reference_count,
                       influential_citation_count, topic_cluster_id,
                       topic_cluster, umap_x, umap_y, methods, datasets,
                       tasks, source, ingested_at
                FROM papers
                WHERE ingest_year_month = '{year_month}'
            """
        else:
            query = """
                SELECT paper_id, title, abstract, authors, submitted_date,
                       updated_date, primary_category, categories,
                       citation_count, reference_count,
                       influential_citation_count, topic_cluster_id,
                       topic_cluster, umap_x, umap_y, methods, datasets,
                       tasks, source, ingested_at
                FROM papers
            """

        logger.info("Executing Hive query for papers index...")
        cursor.execute(query)

        columns = [desc[0].split(".")[-1] for desc in cursor.description]
        total_read    = 0
        total_indexed = 0
        total_failed  = 0

        for batch in _fetch_in_batches(cursor, self.batch_size):
            actions = []
            for row in batch:
                doc = dict(zip(columns, row))
                # Convert date objects to ISO strings
                for k, v in doc.items():
                    doc[k] = _clean_datetime(v) if (hasattr(v, "isoformat") or (isinstance(v, str) and "T" in str(v) and ":" in str(v))) else v
                actions.append({
                    "_index":  PAPERS_INDEX,
                    "_id":     doc["paper_id"],
                    "_source": doc,
                })
            total_read += len(actions)
            indexed, failed = self.es_client.bulk(actions)
            total_indexed  += indexed
            total_failed   += len(failed)
            logger.info("  Indexed %d / %d so far...", total_indexed, total_read)

        cursor.close()
        conn.close()
        return total_read, total_indexed, total_failed


class FulltextIndexer:
    """
    Indexes full-text paper records from the Hive paper_fulltext table
    into the research_intel_fulltext Elasticsearch index.

    Parameters
    ----------
    es_client : ESClient
        Initialized and connected ESClient instance.
    batch_size : int
        Number of documents per bulk request. Default: 200.
    """

    def __init__(self, es_client: ESClient, batch_size: int = 200):
        self.es_client  = es_client
        self.batch_size = batch_size

    def full_reindex(self) -> dict:
        """
        Drop and recreate the fulltext index, then index all records.

        Returns
        -------
        dict
            Indexing summary.
        """
        self.es_client.delete_index(FULLTEXT_INDEX)
        self.es_client.create_index(FULLTEXT_INDEX, fulltext_mapping())

        start_time = datetime.now(timezone.utc)
        total_read, total_indexed, total_failed = self._index_from_hive()
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        summary = {
            "total_read":       total_read,
            "total_indexed":    total_indexed,
            "total_failed":     total_failed,
            "duration_seconds": duration,
        }
        logger.info("Fulltext full reindex complete: %s", summary)
        return summary

    def incremental_index(self, date: str) -> dict:
        """
        Index full-text records ingested on a specific date.

        Parameters
        ----------
        date : str
            ISO date string (YYYY-MM-DD).

        Returns
        -------
        dict
            Indexing summary.
        """
        start_time = datetime.now(timezone.utc)
        total_read, total_indexed, total_failed = self._index_from_hive(date=date)
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        summary = {
            "total_read":       total_read,
            "total_indexed":    total_indexed,
            "total_failed":     total_failed,
            "duration_seconds": duration,
        }
        logger.info("Fulltext incremental index complete for %s: %s", date, summary)
        return summary

    def _index_from_hive(self, date: str = None) -> tuple[int, int, int]:
        """
        Query Hive paper_fulltext table and bulk-index into Elasticsearch.

        Parameters
        ----------
        date : str, optional
            ISO date filter for ingested_at. If None, reads all records.

        Returns
        -------
        tuple[int, int, int]
            (total_read, total_indexed, total_failed)
        """
        conn   = _get_hive_connection()
        cursor = conn.cursor()

        if date:
            query = f"""
                SELECT paper_id, arxiv_id, corpusid, doi,
                       full_text, sections, ingested_at
                FROM paper_fulltext
                WHERE ingested_at >= '{date}T00:00:00'
                  AND ingested_at <  '{date}T23:59:59'
            """
        else:
            query = """
                SELECT paper_id, arxiv_id, corpusid, doi,
                       full_text, sections, ingested_at
                FROM paper_fulltext
            """

        logger.info("Executing Hive query for fulltext index...")
        cursor.execute(query)

        columns = [desc[0].split(".")[-1] for desc in cursor.description]
        total_read    = 0
        total_indexed = 0
        total_failed  = 0

        for batch in _fetch_in_batches(cursor, self.batch_size):
            actions = []
            for row in batch:
                doc      = dict(zip(columns, row))
                # sections comes back as a string from Hive — parse it
                # into a list of {heading, text} dicts if needed
                sections = doc.get("sections") or []
                if isinstance(sections, str):
                    # Hive returns ARRAY<STRUCT> as a formatted string
                    # Keep as-is for ES — it will store as text
                    sections = []
                actions.append({
                    "_index":  FULLTEXT_INDEX,
                    "_id":     doc["paper_id"],
                    "_source": {
                        "paper_id":    doc["paper_id"],
                        "arxiv_id":    doc.get("arxiv_id"),
                        "corpusid":    doc.get("corpusid"),
                        "doi":         doc.get("doi"),
                        "full_text":   doc.get("full_text"),
                        "sections":    sections,
                        "ingested_at": _clean_datetime(doc.get("ingested_at")),
                    },
                })
            total_read    += len(actions)
            indexed, failed = self.es_client.bulk(actions)
            total_indexed += indexed
            total_failed  += len(failed)
            logger.info("  Indexed %d / %d so far...", total_indexed, total_read)

        cursor.close()
        conn.close()
        return total_read, total_indexed, total_failed