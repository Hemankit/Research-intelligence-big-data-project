"""
elasticsearch_run.py
--------------------
Entrypoint for the Elasticsearch indexing pipeline.

Runs from the local Python environment (outside Docker) since both
Elasticsearch (localhost:9200) and Hive (localhost:10000) are exposed
by the Docker stack. No Spark cluster needed — pyhive reads directly
from HiveServer2.

Usage:
    # Full reindex of both indices (initial setup):
    python -m elasticsearch.elasticsearch_run --mode full

    # Full reindex of papers index only:
    python -m elasticsearch.elasticsearch_run --mode full --index papers

    # Full reindex of fulltext index only:
    python -m elasticsearch.elasticsearch_run --mode full --index fulltext

    # Incremental update after ingestion run:
    python -m elasticsearch.elasticsearch_run --mode incremental --year-month 2026-04

Dependencies: elasticsearch-py, pyhive, thrift
Install: pip install elasticsearch pyhive thrift
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from .client import ESClient
from .indexer import PapersIndexer, FulltextIndexer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("es_run")


def build_es_client() -> ESClient:
    """
    Instantiate and connect an ESClient.

    Reads ES_HOST (default: http://localhost:9200), ES_USERNAME,
    ES_PASSWORD, ES_API_KEY from environment variables.

    Returns
    -------
    ESClient
        Connected ESClient ready for indexing operations.
    """
    es_client = ESClient()
    try:
        es_client.connect()
    except Exception as e:
        logger.error("Failed to connect to Elasticsearch at %s: %s", es_client.host, e)
        raise ConnectionError(
            f"Could not connect to Elasticsearch at {es_client.host}. "
            "Is the Docker stack running?"
        ) from e
    return es_client


def run_full_reindex(
    es_client: ESClient,
    index: str = "both",
    batch_size_papers: int = 500,
    batch_size_fulltext: int = 200,
) -> dict:
    """
    Run a full reindex of one or both Elasticsearch indices.

    Parameters
    ----------
    es_client : ESClient
    index : str
        'papers', 'fulltext', or 'both'. Default: 'both'.
    batch_size_papers : int
        Batch size for papers indexer. Default: 500.
    batch_size_fulltext : int
        Batch size for fulltext indexer. Default: 200.

    Returns
    -------
    dict
        Per-index results under 'papers' and/or 'fulltext' keys.
    """
    summary = {}
    if index in ("papers", "both"):
        logger.info("Starting full reindex of papers index...")
        indexer = PapersIndexer(es_client, batch_size=batch_size_papers)
        summary["papers"] = indexer.full_reindex()
    if index in ("fulltext", "both"):
        logger.info("Starting full reindex of fulltext index...")
        indexer = FulltextIndexer(es_client, batch_size=batch_size_fulltext)
        summary["fulltext"] = indexer.full_reindex()
    return summary


def run_incremental(
    es_client: ESClient,
    year_month: str,
    date: str = None,
    index: str = "both",
) -> dict:
    """
    Run an incremental index update for a specific time partition.

    Parameters
    ----------
    es_client : ESClient
    year_month : str
        Partition key for papers index, e.g. '2026-04'.
    date : str, optional
        ISO date for fulltext filter. Defaults to year_month + '-01'.
    index : str
        'papers', 'fulltext', or 'both'.

    Returns
    -------
    dict
        Per-index results.
    """
    summary = {}
    if index in ("papers", "both"):
        logger.info("Starting incremental papers index for %s...", year_month)
        indexer = PapersIndexer(es_client)
        summary["papers"] = indexer.incremental_index(year_month)
    if index in ("fulltext", "both"):
        fulltext_date = date or f"{year_month}-01"
        logger.info("Starting incremental fulltext index for %s...", fulltext_date)
        indexer = FulltextIndexer(es_client)
        summary["fulltext"] = indexer.incremental_index(fulltext_date)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Elasticsearch indexing pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m elasticsearch.elasticsearch_run --mode full
  python -m elasticsearch.elasticsearch_run --mode full --index papers
  python -m elasticsearch.elasticsearch_run --mode incremental --year-month 2026-04
        """
    )
    parser.add_argument(
        "--mode", choices=["full", "incremental"], required=True,
        help="'full' drops and recreates; 'incremental' upserts a partition."
    )
    parser.add_argument(
        "--index", choices=["papers", "fulltext", "both"], default="both",
        help="Which index to populate (default: both)."
    )
    parser.add_argument(
        "--year-month",
        help="Partition key for incremental mode, e.g. '2026-04'."
    )
    parser.add_argument(
        "--date",
        help="ISO date for fulltext incremental filter."
    )
    parser.add_argument(
        "--batch-size-papers", type=int, default=500,
        help="Bulk batch size for papers indexer (default: 500)."
    )
    parser.add_argument(
        "--batch-size-fulltext", type=int, default=200,
        help="Bulk batch size for fulltext indexer (default: 200)."
    )
    args = parser.parse_args()

    if args.mode == "incremental" and not args.year_month:
        parser.error("--year-month is required when --mode is incremental.")

    return args


if __name__ == "__main__":
    args  = parse_args()
    start = datetime.now(timezone.utc)

    logger.info("=" * 60)
    logger.info("Elasticsearch Indexing Pipeline starting")
    logger.info("  mode  : %s", args.mode)
    logger.info("  index : %s", args.index)
    if args.mode == "incremental":
        logger.info("  year-month : %s", args.year_month)
    logger.info("=" * 60)

    try:
        es_client = build_es_client()
        health    = es_client.health()
        logger.info("Cluster health: %s", health.get("status", "unknown"))

        if args.mode == "full":
            summary = run_full_reindex(
                es_client,
                index=args.index,
                batch_size_papers=args.batch_size_papers,
                batch_size_fulltext=args.batch_size_fulltext,
            )
        else:
            summary = run_incremental(
                es_client,
                year_month=args.year_month,
                date=args.date,
                index=args.index,
            )

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info("=" * 60)
        logger.info("Indexing complete in %.1fs", elapsed)
        for idx_name, result in summary.items():
            logger.info(
                "  [%s] indexed=%d  failed=%d  read=%d  time=%.1fs",
                idx_name,
                result.get("total_indexed", 0),
                result.get("total_failed",  0),
                result.get("total_read",    0),
                result.get("duration_seconds", 0),
            )
        logger.info("=" * 60)

    except Exception as e:
        logger.error("Indexing pipeline failed: %s", e, exc_info=True)
        sys.exit(1)