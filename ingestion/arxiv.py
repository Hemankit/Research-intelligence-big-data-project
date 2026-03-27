"""
arxiv.py
--------
Ingestion of papers from the arXiv API.

Supports two modes:

  INCREMENTAL (default — run daily via Airflow):
      Pulls papers submitted in the last N days per category.
      Safe for daily scheduling, low memory usage.

      python -m ingestion.arxiv
      python -m ingestion.arxiv --lookback 7

  BULK (one-time historical backfill):
      Pulls as many papers as possible per category with no date filter.
      Writes to HDFS in batches so memory never blows up regardless of
      how many papers are fetched.
      This is the mode to use before clustering with BERTopic.

      python -m ingestion.arxiv --bulk
      python -m ingestion.arxiv --bulk --max 5000 --batch-size 500

      Note: arXiv API hard-caps results at ~30,000 per query. For a
      deeper historical pull use the arXiv bulk S3 dataset instead.
"""

import argparse
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import arxiv
from tqdm import tqdm

from ingestion.hdfs_client import HDFSClient

logger = logging.getLogger(__name__)

# Configuration 

CATEGORIES: list[str] = [
    "cs.LG",  # Machine Learning
    "cs.CL",  # Computation & Language
    "cs.CV",  # Computer Vision
    "cs.AI",  # Artificial Intelligence
    "cs.IR",  # Information Retrieval
]

# Incremental mode defaults
LOOKBACK_DAYS: int = int(os.getenv("ARXIV_LOOKBACK_DAYS", "1"))
MAX_RESULTS_PER_CATEGORY: int = int(os.getenv("ARXIV_MAX_RESULTS", "500"))

# Bulk mode defaults
BULK_MAX_RESULTS: int = int(os.getenv("ARXIV_BULK_MAX_RESULTS", "10000"))
# How many records to accumulate before flushing a batch to HDFS
# Keeps memory usage flat regardless of total corpus size
BULK_BATCH_SIZE: int = int(os.getenv("ARXIV_BULK_BATCH_SIZE", "500"))

# arXiv recommends >= 3s between requests
# Bulk mode uses a longer delay to avoid 429 rate limits
REQUEST_DELAY: float = 3.0
BULK_REQUEST_DELAY: float = float(os.getenv("ARXIV_BULK_REQUEST_DELAY", "5.0"))


# Schema normalisation

def _normalise(paper: arxiv.Result, queried_category: str) -> dict:
    """
    Flatten an arxiv.Result object into the pipeline's standard paper dict.

    Fields
    ------
    paper_id        : str   Short arXiv ID, e.g. '2401.12345'
    source          : str   Always 'arxiv'
    title           : str
    abstract        : str   Full abstract — used by BERTopic + NER
    authors         : list  List of author name strings
    submitted_date  : str   ISO date string, e.g. '2024-01-08'
    updated_date    : str   ISO date of last revision
    primary_category: str   Primary arXiv subject category
    all_categories  : list  All assigned categories
    pdf_url         : str   Direct link to PDF
    arxiv_url       : str   Abstract page URL
    queried_category: str   Category filter used to retrieve this paper
    ingested_at     : str   UTC timestamp of ingestion
    """
    pdf_url = next(
        (link.href for link in paper.links if link.title == "pdf"),
        ""
    )
    return {
        "paper_id":          paper.get_short_id().split("v")[0],
        "source":            "arxiv",
        "title":             paper.title.strip(),
        "abstract":          paper.summary.strip(),
        "authors":           [a.name for a in paper.authors],
        "submitted_date":    paper.published.strftime("%Y-%m-%d"),
        "updated_date":      paper.updated.strftime("%Y-%m-%d"),
        "primary_category":  paper.primary_category,
        "all_categories":    paper.categories,
        "pdf_url":           pdf_url,
        "arxiv_url":         paper.entry_id,
        "queried_category":  queried_category,
        "ingested_at":       datetime.now(timezone.utc).isoformat(),
    }


# Date filter (incremental mode only)

def _is_within_lookback(paper: arxiv.Result, lookback_days: int) -> bool:
    """
    Returns True if the paper was submitted within the last lookback_days.
    Used in incremental mode only — bulk mode skips this entirely.
    The submittedDate range filter in arXiv's query API causes HTTP 500s
    so we filter client-side after fetching instead.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    submitted = paper.published
    if submitted.tzinfo is None:
        submitted = submitted.replace(tzinfo=timezone.utc)
    return submitted >= cutoff


# Incremental ingestion 

def ingest_arxiv(
    categories: list[str] = None,
    lookback_days: int = None,
    max_results: int = None,
    hdfs_client: HDFSClient = None,
) -> dict[str, int]:
    """
    Incremental ingestion — pulls papers from the last N days.
    Called daily by the Airflow DAG.

    Parameters
    ----------
    categories    : arXiv category strings. Defaults to CATEGORIES.
    lookback_days : days back to query. Defaults to LOOKBACK_DAYS env var.
    max_results   : max papers per category. Defaults to MAX_RESULTS_PER_CATEGORY.
    hdfs_client   : HDFSClient instance. Created with defaults if not passed.

    Returns
    -------
    dict mapping each category to the number of papers ingested.
    """
    categories    = categories    or CATEGORIES
    lookback_days = lookback_days or LOOKBACK_DAYS
    max_results   = max_results   or MAX_RESULTS_PER_CATEGORY
    hdfs          = hdfs_client   or HDFSClient()
    client        = arxiv.Client()
    summary: dict[str, int] = {}

    for category in categories:
        logger.info(
            "INCREMENTAL | category=%s | lookback=%dd | max=%d",
            category, lookback_days, max_results
        )

        search = arxiv.Search(
            query=f"cat:{category}",
            max_results=max_results,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending,
        )

        records: list[dict] = []
        try:
            for paper in tqdm(client.results(search), desc=f"arXiv {category}", unit="paper"):
                if not _is_within_lookback(paper, lookback_days):
                    break  # sorted newest-first so we can stop early
                records.append(_normalise(paper, category))
        except Exception as exc:
            logger.error("Error fetching %s: %s", category, exc)
            summary[category] = 0
            continue

        if records:
            hdfs.write_json(records, source="arxiv", category=category)
            logger.info("Wrote %d papers for %s", len(records), category)
        else:
            logger.warning("No papers returned for %s", category)

        summary[category] = len(records)
        time.sleep(REQUEST_DELAY)

    total = sum(summary.values())
    logger.info("Incremental ingestion complete | total=%d | %s", total, summary)
    return summary


# Bulk ingestion

def bulk_ingest_arxiv(
    categories: list[str] = None,
    max_results: int = None,
    batch_size: int = None,
    hdfs_client: HDFSClient = None,
) -> dict[str, int]:
    """
    Bulk ingestion — pulls the full historical corpus per category
    with no date filter. Writes to HDFS in batches so memory stays
    flat regardless of how many papers are fetched.

    This is the function to run once before BERTopic clustering.
    After this, the incremental ingest_arxiv() keeps the corpus fresh.

    Parameters
    ----------
    categories  : arXiv categories to bulk-pull. Defaults to CATEGORIES.
    max_results : total papers to pull per category. Default 10,000.
                  arXiv API hard-caps at ~30,000 per query.
    batch_size  : records to accumulate before flushing to HDFS.
                  Default 500 — keeps memory usage low.
    hdfs_client : HDFSClient instance. Created with defaults if not passed.

    Returns
    -------
    dict mapping each category to total papers written.
    """
    categories  = categories  or CATEGORIES
    max_results = max_results or BULK_MAX_RESULTS
    batch_size  = batch_size  or BULK_BATCH_SIZE
    hdfs        = hdfs_client or HDFSClient()
    client      = arxiv.Client()
    summary: dict[str, int] = {}

    logger.info(
        "BULK INGEST START | categories=%s | max_per_cat=%d | batch_size=%d",
        categories, max_results, batch_size
    )

    for category in categories:
        logger.info(
            "BULK | category=%s | pulling up to %d papers...",
            category, max_results
        )

        search = arxiv.Search(
            query=f"cat:{category}",
            max_results=max_results,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending,
        )

        batch: list[dict] = []
        total_written = 0
        batch_num = 0
        consecutive_429s = 0

        try:
            pbar = tqdm(
                client.results(search),
                desc=f"BULK arXiv {category}",
                unit="paper",
                total=max_results
            )
            for paper in pbar:
                batch.append(_normalise(paper, category))

                # Flush to HDFS when batch is full
                if len(batch) >= batch_size:
                    hdfs.write_json(batch, source="arxiv", category=category)
                    total_written += len(batch)
                    batch_num += 1
                    logger.info(
                        "BULK | %s | flushed batch %d | %d papers written so far",
                        category, batch_num, total_written
                    )
                    batch = []
                    # Extra pause after each HDFS flush to reduce 429 risk
                    time.sleep(BULK_REQUEST_DELAY)

            # Flush remaining records that didn't fill a full batch
            if batch:
                hdfs.write_json(batch, source="arxiv", category=category)
                total_written += len(batch)
                batch_num += 1
                logger.info(
                    "BULK | %s | flushed final batch %d | %d total papers written",
                    category, batch_num, total_written
                )

        except Exception as exc:
            logger.error("BULK | Error fetching %s: %s", category, exc)
            if "429" in str(exc):
                # Rate limited — wait longer before next category
                wait = 30
                logger.warning("BULK | Rate limited by arXiv — waiting %ds before next category", wait)
                time.sleep(wait)
            # Save whatever we have so far rather than losing the partial pull
            if batch:
                hdfs.write_json(batch, source="arxiv", category=category)
                total_written += len(batch)
                logger.info(
                    "BULK | %s | saved partial batch on error (%d papers)",
                    category, len(batch)
                )

        summary[category] = total_written
        logger.info("BULK | %s complete | total=%d", category, total_written)
        time.sleep(REQUEST_DELAY)

    grand_total = sum(summary.values())
    logger.info(
        "BULK INGEST COMPLETE | grand_total=%d | breakdown=%s",
        grand_total, summary
    )
    return summary


# CLI entry point

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Ingest papers from the arXiv API into HDFS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Daily incremental pull (last 1 day, all categories)
  python -m ingestion.arxiv

  # Incremental pull for last 7 days
  python -m ingestion.arxiv --lookback 7

  # Bulk historical pull (10,000 papers per category)
  python -m ingestion.arxiv --bulk

  # Bulk pull with custom limits
  python -m ingestion.arxiv --bulk --max 5000 --batch-size 250

  # Bulk pull for specific categories only
  python -m ingestion.arxiv --bulk --categories cs.LG cs.CL
        """
    )

    parser.add_argument(
        "--bulk",
        action="store_true",
        help="Run bulk historical ingestion instead of incremental"
    )
    parser.add_argument(
        "--categories",
        nargs="+",
        default=None,
        help="arXiv categories to ingest (default: all configured categories)"
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="Max papers per category. Incremental default: 500. Bulk default: 10000."
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=None,
        help="Days back to look (incremental mode only, default: 1)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        dest="batch_size",
        help="Records per HDFS batch flush (bulk mode only, default: 500)"
    )

    args = parser.parse_args()

    if args.bulk:
        print(f"\nRunning BULK ingestion...")
        print(f"  Categories : {args.categories or CATEGORIES}")
        print(f"  Max/cat    : {args.max or BULK_MAX_RESULTS}")
        print(f"  Batch size : {args.batch_size or BULK_BATCH_SIZE}")
        print(f"  Note: arXiv API caps at ~30,000 papers per query\n")

        results = bulk_ingest_arxiv(
            categories=args.categories,
            max_results=args.max,
            batch_size=args.batch_size,
        )
    else:
        print(f"\nRunning INCREMENTAL ingestion...")
        print(f"  Categories : {args.categories or CATEGORIES}")
        print(f"  Lookback   : {args.lookback or LOOKBACK_DAYS} days")
        print(f"  Max/cat    : {args.max or MAX_RESULTS_PER_CATEGORY}\n")

        results = ingest_arxiv(
            categories=args.categories,
            lookback_days=args.lookback,
            max_results=args.max,
        )

    print("\nSummary:")
    for cat, count in results.items():
        print(f"  {cat}: {count} papers")
    print(f"  TOTAL: {sum(results.values())} papers")