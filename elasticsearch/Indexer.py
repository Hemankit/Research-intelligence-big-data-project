"""
indexer.py
----------
Reads enriched paper records from Hive and bulk-indexes them into
Elasticsearch to power the dashboard's Search & Entity Explorer and
Knowledge Table views.

Handles two indices:
  - research_intel_papers    — from the Hive papers table
  - research_intel_fulltext  — from the Hive paper_fulltext table

Reads from Hive using PySpark since the papers table is partitioned
Parquet and too large to load into memory directly. Converts Spark
rows to Elasticsearch bulk action dicts and sends them in configurable
batch sizes using ESClient.bulk().

Supports two indexing modes:
  - Full reindex: drops and recreates the index, then indexes everything.
    Use when the mapping has changed or for the initial population.
  - Incremental: indexes only records from a specific date partition.
    Use for nightly Airflow runs after new data is consolidated.

Dependencies: elasticsearch/client.py, elasticsearch/mappings.py, pyspark
"""

import logging
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame

from client import ESClient
from .mappings import (
    papers_mapping,
    fulltext_mapping,
    PAPERS_INDEX,
    FULLTEXT_INDEX,
)

logger = logging.getLogger(__name__)


class PapersIndexer:
    """
    Indexes enriched paper records from the Hive papers table into
    the research_intel_papers Elasticsearch index.

    Reads from Hive via PySpark, converts rows to ES documents, and
    bulk-indexes in configurable batch sizes. Supports full reindexing
    and incremental partition-based updates.

    Parameters
    ----------
    es_client : ESClient
        Initialized and connected ESClient instance.
    spark : SparkSession
        Active SparkSession for reading from Hive.
    batch_size : int
        Number of documents per bulk indexing request. Default: 500.
        Increase for higher throughput, decrease if hitting ES memory limits.
    """

    def __init__(self, es_client: ESClient, spark: SparkSession, batch_size: int = 500):
        self.es_client = es_client
        self.spark = spark
        self.batch_size = batch_size

    def full_reindex(self) -> dict:
        """
        Drop and recreate the papers index, then index all records from Hive.

        Deletes the existing index if present, creates a fresh one with the
        current mapping from mappings.papers_mapping(), reads all records
        from the Hive papers table, and indexes them in batches.

        Use this when the mapping has changed or for the first-time population.
        This is a destructive operation — the index will be unavailable
        during reindexing. In production, consider using an alias and
        index swap pattern to avoid downtime.

        Returns
        -------
        dict
            Indexing summary with keys:
            - total_read (int): Records read from Hive
            - total_indexed (int): Documents successfully indexed
            - total_failed (int): Documents that failed to index
            - duration_seconds (float): Wall-clock time for the operation
        """
        if not self.es_client.index_exists(PAPERS_INDEX):
            self.es_client.create_index(PAPERS_INDEX, papers_mapping())
        else:
            logger.warning(f"Index {PAPERS_INDEX} already exists. Skipping creation.")
        start_time = datetime.now(timezone.utc)
        df = self._read_papers_from_hive()
        total_read = df.count()
        total_indexed, total_failed = self._index_dataframe(df)
        duration_seconds = (datetime.now(timezone.utc) - start_time).total_seconds()
        summary = {
            "total_read": total_read,
            "total_indexed": total_indexed,
            "total_failed": total_failed,
            "duration_seconds": duration_seconds,
        }
        logger.info(f"Full reindexing completed: {summary}")
        return summary

    def incremental_index(self, year_month: str) -> dict:
        """
        Index only records from a specific Hive partition (ingest_year_month).

        Reads a single partition from the papers table and upserts those
        documents into Elasticsearch. Existing documents with the same
        paper_id are overwritten. Documents in other partitions are untouched.

        Used by the nightly Airflow DAG after spark_consolidate.py has
        written new records to the latest partition.

        Parameters
        ----------
        year_month : str
            Partition value to index, e.g. '2024-03'. Must match the
            ingest_year_month partition key format in the Hive table.

        Returns
        -------
        dict
            Indexing summary with the same keys as full_reindex().
        """
        # reading partition from papers table
        start_time = datetime.now(timezone.utc)
        df = self._read_papers_from_hive(year_month)
        # counting records in the partition
        total_read = df.count()
        total_indexed, total_failed = self._index_dataframe(df)
        # calculating duration
        duration_seconds = (datetime.now(timezone.utc) - start_time).total_seconds()
        summary = {
            "total_read": total_read,
            "total_indexed": total_indexed,
            "total_failed": total_failed,
            "duration_seconds": duration_seconds,
        }
        logger.info(f"Incremental indexing completed for partition {year_month}: {summary}")
        return summary

    def _read_papers_from_hive(self, year_month: str = None) -> DataFrame:
        """
        Read records from the Hive papers table into a Spark DataFrame.

        If year_month is provided, applies a partition filter to read
        only that partition. Otherwise reads the full table.

        Parameters
        ----------
        year_month : str, optional
            Partition filter value. If None, reads all partitions.

        Returns
        -------
        DataFrame
            Spark DataFrame with all columns from the papers table.
        """
        if year_month is not None:
            df = self.spark.read.table("papers").filter(f"ingest_year_month = '{year_month}'")
        else:
            df = self.spark.read.table("papers")
        return df

    def _row_to_action(self, row) -> dict:
        """
        Convert a single Spark Row from the papers table to an
        Elasticsearch bulk action dict.

        Maps Spark Row fields to a flat dict suitable for indexing.
        Handles Spark-specific types: converts ArrayType columns to
        Python lists, DateType to ISO strings, and None values are
        passed through as null (Elasticsearch ignores null fields).

        Parameters
        ----------
        row : pyspark.sql.Row
            A single row from the papers DataFrame.

        Returns
        -------
        dict
            Elasticsearch bulk action with keys:
            - _index (str): Target index name
            - _id (str): Document ID (paper_id)
            - _source (dict): Document fields to index
        """
        # converting row fields of spark types to plain Python types
        source = row.asDict()
        for field, value in source.items():
          # convert Spark ArrayType to Python list, DateType to ISO string, and leave None as null
          if isinstance(value, list):
            source[field] = list(value)
          elif hasattr(value, 'isoformat'):  # catches date and datetime
            source[field] = value.isoformat()
        return {
        "_index": PAPERS_INDEX,
        "_id": source["paper_id"],
        "_source": source,
    }

    def _index_dataframe(self, df: DataFrame) -> tuple[int, int]:
      total_indexed = 0
      total_failed = 0
      # iterate over DataFrame partitions to avoid collecting large data into memory
      for partition in df.rdd.toLocalIterator():
        actions = []
        # convert each row in the partition to a bulk action dict and add to the batch
        actions.append(self._row_to_action(partition))
        # when batch size is reached, send the bulk request and reset the batch
        if len(actions) >= self.batch_size:
            indexed, failed = self.es_client.bulk(actions)
            total_indexed += indexed
            total_failed += len(failed)
            actions = []
    # flush remaining actions
      if actions:
        indexed, failed = self.es_client.bulk(actions)
        total_indexed += indexed
        total_failed += len(failed)
      return total_indexed, total_failed


class FulltextIndexer:
    """
    Indexes full-text paper records from the Hive paper_fulltext table
    into the research_intel_fulltext Elasticsearch index.

    Follows the same pattern as PapersIndexer but targets the fulltext
    table and index. Handles the nested sections field which requires
    special conversion from Spark StructType to Python dicts.

    Parameters
    ----------
    es_client : ESClient
        Initialized and connected ESClient instance.
    spark : SparkSession
        Active SparkSession for reading from Hive.
    batch_size : int
        Number of documents per bulk indexing request. Default: 200.
        Lower default than PapersIndexer because full-text documents
        are significantly larger.
    """

    def __init__(self, es_client: ESClient, spark: SparkSession, batch_size: int = 200):
        self.es_client = es_client
        self.spark = spark
        self.batch_size = batch_size

    def full_reindex(self) -> dict:
        """
        Drop and recreate the fulltext index, then index all records from Hive.

        Same pattern as PapersIndexer.full_reindex() but targets the
        paper_fulltext table and research_intel_fulltext index.

        Returns
        -------
        dict
            Indexing summary with total_read, total_indexed, total_failed,
            and duration_seconds keys.
        """
        if not self.es_client.index_exists(FULLTEXT_INDEX):
            self.es_client.create_index(FULLTEXT_INDEX, fulltext_mapping())
        else:
            # In production, consider using an alias and index swap pattern to avoid downtime during reindexing.
            self.es_client.delete_index(FULLTEXT_INDEX) # delete existing index to ensure a clean slate for the new mapping
            self.es_client.create_index(FULLTEXT_INDEX, fulltext_mapping()) # create new index with the correct mapping
        start_time = datetime.now(timezone.utc)
        df = self._read_fulltext_from_hive()
        total_read = df.count()
        total_indexed, total_failed = self._index_dataframe(df)
        duration_seconds = (datetime.now(timezone.utc) - start_time).total_seconds()
        summary = {
            "total_read": total_read,
            "total_indexed": total_indexed,
            "total_failed": total_failed,
            "duration_seconds": duration_seconds,
        }
        logger.info(f"Full-text reindexing completed: {summary}")
        return summary

    def incremental_index(self, date: str) -> dict:
        """
        Index full-text records ingested on a specific date.

        Filters the paper_fulltext table by ingested_at date rather than
        a Hive partition key (paper_fulltext is not partitioned).

        Parameters
        ----------
        date : str
            ISO date string (YYYY-MM-DD) to filter records by ingested_at.

        Returns
        -------
        dict
            Indexing summary with the same keys as full_reindex().
        """
        start_time = datetime.now(timezone.utc)
        df = self._read_fulltext_from_hive(date)
        total_read = df.count()
        total_indexed, total_failed = self._index_dataframe(df)
        duration_seconds = (datetime.now(timezone.utc) - start_time).total_seconds()
        summary = {
            "total_read": total_read,
            "total_indexed": total_indexed,
            "total_failed": total_failed,
            "duration_seconds": duration_seconds,
        }
        logger.info(f"Incremental full-text indexing completed for date {date}: {summary}")
        return summary

    def _read_fulltext_from_hive(self, date: str = None) -> DataFrame:
        """
        Read records from the Hive paper_fulltext table into a Spark DataFrame.

        Optionally filters by ingested_at date for incremental indexing.

        Parameters
        ----------
        date : str, optional
            ISO date string filter for ingested_at. If None, reads all records.

        Returns
        -------
        DataFrame
            Spark DataFrame with all columns from the paper_fulltext table.
        """
        if date is not None:
            df = self.spark.read.table("paper_fulltext").filter(f"ingested_at >= '{date}T00:00:00' AND ingested_at < '{date}T23:59:59'")
        else:
            df = self.spark.read.table("paper_fulltext")
        return df

    def _row_to_action(self, row) -> dict:
        """
        Convert a single Spark Row from the paper_fulltext table to an
        Elasticsearch bulk action dict.

        Handles the nested sections field — converts each Spark Row in
        the sections array to a plain Python dict with 'heading' and
        'text' keys, matching the nested mapping defined in mappings.py.

        Parameters
        ----------
        row : pyspark.sql.Row
            A single row from the paper_fulltext DataFrame.

        Returns
        -------
        dict
            Elasticsearch bulk action with _index, _id, and _source keys.
            _id is set to paper_id for consistent cross-index lookups.
        """
        sections = [
    {"heading": s.heading, "text": s.text}
    for s in (row.sections or [])  # guard against null sections
]
        action = {
            "_index": FULLTEXT_INDEX,
            "_id": row.paper_id,
            "_source": {
                "paper_id": row.paper_id,
                "title": row.title,
                "abstract": row.abstract,
                "sections": sections,
                "ingested_at": row.ingested_at,
            },
        }
        return action