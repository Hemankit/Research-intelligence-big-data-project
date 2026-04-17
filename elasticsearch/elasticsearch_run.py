"""
run.py
------
Entrypoint for the Elasticsearch indexing pipeline.

Orchestrates indexing of enriched paper records from Hive into
Elasticsearch for the two indices that power the dashboard:

  - research_intel_papers   : Search & Entity Explorer, Knowledge Table
  - research_intel_fulltext : Full-text search across complete paper bodies

Supports two execution modes:
  - Full reindex  : Drops and recreates both indices from scratch.
                    Use for initial population or after mapping changes.
  - Incremental   : Indexes only new records from a specific partition/date.
                    Used by the nightly Airflow DAG after consolidation.

Usage:
    # Full reindex of both indices:
    python run.py --mode full

    # Incremental update for a specific month partition (papers):
    python run.py --mode incremental --year-month 2024-03

    # Full reindex of papers index only:
    python run.py --mode full --index papers

    # Full reindex of fulltext index only:
    python run.py --mode full --index fulltext

Dependencies: elasticsearch/client.py, elasticsearch/indexer.py,
              elasticsearch/mappings.py, pyspark
"""

import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from elasticsearch.client import ESClient
from elasticsearch.indexer import PapersIndexer, FulltextIndexer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("es_run")


def build_spark_session(local: bool = False) -> SparkSession:
    """
    Build and return a SparkSession configured for Hive access.

    Enables Hive support and connects to the Hive metastore so that
    PapersIndexer and FulltextIndexer can query Hive tables directly
    using spark.table().

    Parameters
    ----------
    local : bool
        If True, runs Spark in local mode for development and testing.
        If False, connects to the Spark cluster via spark-master.

    Returns
    -------
    SparkSession
        Active SparkSession with Hive support enabled.
    """
    if bool (local):
        logger.info("Building SparkSession in local mode for development.")
        # Local mode is useful for development and testing on a laptop. It runs Spark in-process and does not require a connection to a cluster. In this mode, the SparkSession is configured with master("local[*]") to use all available CPU cores. Hive support is still enabled so that the indexers can read from Hive tables as usual.
        spark = SparkSession.builder.appName("ESIndexerLocal").master("local[*]").enableHiveSupport().getOrCreate()
    else:
        logger.info("Building SparkSession for cluster mode.")
        # In production, the SparkSession will connect to the cluster via spark-master.
        spark = SparkSession.builder.appName("ESIndexer").enableHiveSupport().getOrCreate()
    return spark


def build_es_client() -> ESClient:
    """
    Instantiate and connect an ESClient from environment variables.

    Reads ES_HOST, ES_USERNAME, ES_PASSWORD, and ES_API_KEY from the
    environment. Calls connect() to verify the cluster is reachable
    before returning. Raises ConnectionError if the cluster is down.

    Returns
    -------
    ESClient
        Connected ESClient instance ready for indexing operations.
    """
    try:
        es_client = ESClient()
        es_client.connect()
        return es_client
    except Exception as e:
        logger.error(f"Failed to connect to Elasticsearch cluster at {es_client.host}: {e}")
        raise ConnectionError(f"Could not connect to Elasticsearch cluster at {es_client.host}") from e


def run_full_reindex(
    es_client: ESClient,
    spark: SparkSession,
    index: str = "both",
    batch_size_papers: int = 500,
    batch_size_fulltext: int = 200,
) -> dict:
    """
    Run a full reindex of one or both Elasticsearch indices.

    Instantiates the appropriate indexer(s) and calls full_reindex().
    Logs a summary of records indexed and any failures on completion.

    Parameters
    ----------
    es_client : ESClient
        Connected ESClient instance.
    spark : SparkSession
        Active SparkSession for reading from Hive.
    index : str
        Which index to reindex. One of 'papers', 'fulltext', or 'both'.
        Default: 'both'.
    batch_size_papers : int
        Batch size for the papers indexer. Default: 500.
    batch_size_fulltext : int
        Batch size for the fulltext indexer. Default: 200.

    Returns
    -------
    dict
        Combined indexing summary with per-index results under
        'papers' and 'fulltext' keys.
    """
    summary = {}
    if index in ("papers", "both"):
        papers_indexer = PapersIndexer(es_client, spark, batch_size=batch_size_papers)
        summary["papers"] = papers_indexer.full_reindex()
    if index in ("fulltext", "both"):
        fulltext_indexer = FulltextIndexer(es_client, spark, batch_size=batch_size_fulltext)
        summary["fulltext"] = fulltext_indexer.full_reindex()
    return summary


def run_incremental(
    es_client: ESClient,
    spark: SparkSession,
    year_month: str,
    date: str = None,
    index: str = "both",
) -> dict:
    """
    Run an incremental index update for a specific time partition.

    Indexes only records from the specified year_month partition (papers)
    or ingested_at date (fulltext). Used by the nightly Airflow DAG
    after spark_consolidate.py has written new records.

    Parameters
    ----------
    es_client : ESClient
        Connected ESClient instance.
    spark : SparkSession
        Active SparkSession for reading from Hive.
    year_month : str
        Partition key for the papers index, e.g. '2024-03'.
    date : str, optional
        ISO date string for fulltext incremental filtering.
        Defaults to year_month + '-01' if not provided.
    index : str
        Which index to update. One of 'papers', 'fulltext', or 'both'.

    Returns
    -------
    dict
        Combined indexing summary with per-index results.
    """
    summary = {}
    if index in ("papers", "both"):
        papers_indexer = PapersIndexer(es_client, spark)
        summary["papers"] = papers_indexer.incremental_index(year_month)
    if index in ("fulltext", "both"):
        fulltext_indexer = FulltextIndexer(es_client, spark)
        summary["fulltext"] = fulltext_indexer.incremental_index(date or f"{year_month}-01")
    return summary

def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the Elasticsearch indexing run.

    Supports the following flags:
    --mode        : 'full' for full reindex or 'incremental' for
                    partition-based update. Required.
    --index       : Which index to populate: 'papers', 'fulltext',
                    or 'both'. Default: 'both'.
    --year-month  : Partition key for incremental mode (e.g. '2024-03').
                    Required when --mode incremental.
    --date        : ISO date for fulltext incremental filtering.
                    Defaults to first day of year-month if not provided.
    --batch-size-papers   : Bulk batch size for papers indexer. Default 500.
    --batch-size-fulltext : Bulk batch size for fulltext indexer. Default 200.
    --local       : Run Spark in local mode for development.

    Returns
    -------
    argparse.Namespace
        Parsed arguments object.
    """
    parser = argparse.ArgumentParser(description="Run Elasticsearch indexing pipeline.")
    parser.add_argument("--mode", choices=["full", "incremental"], required=True, help="Indexing mode: 'full' or 'incremental'.")
    parser.add_argument("--index", choices=["papers", "fulltext", "both"], default="both", help="Which index to populate.")
    parser.add_argument("--year-month", help="Partition key for incremental mode (e.g. '2024-03'). Required if mode is incremental.")
    parser.add_argument("--date", help="ISO date for fulltext incremental filtering. Defaults to first day of year-month if not provided.")
    parser.add_argument("--batch-size-papers", type=int, default=500, help="Bulk batch size for papers indexer. Default 500.")
    parser.add_argument("--batch-size-fulltext", type=int, default=200, help="Bulk batch size for fulltext indexer. Default 200.")
    parser.add_argument("--local", action="store_true", help="Run Spark in local mode for development.")
    args = parser.parse_args()

    if args.mode == "incremental" and not args.year_month:
        parser.error("--year-month is required when --mode is incremental.")

    return args


if __name__ == "__main__":
    """
    CLI entry point. Parses args, builds Spark and ES clients, then
    dispatches to run_full_reindex() or run_incremental() based on --mode.

    Logs total documents indexed per index, failure counts, and
    wall-clock time on completion. Exits with code 1 on any error
    so Airflow can detect and alert on failures.
    """
    pass