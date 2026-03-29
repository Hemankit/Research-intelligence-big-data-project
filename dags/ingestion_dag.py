"""
dags/ingestion_dag.py
---------------------
Airflow DAG that runs the three-step ingestion pipeline every 2 days.

Schedule: every 2 days at 03:00 UTC — off-peak window when arXiv rate
          limits are most relaxed (before US east coast wakes up).
          Each task also adds a random 0-300s jitter so requests don't
          all land at exactly the same second.

Task flow:
    check_api_health          ← pre-flight: skip run if rate limits active
        ↓
    ingest_arxiv              ← pulls new papers from arXiv
        ↓  (passes paper IDs via XCom)
    enrich_s2orc              ← enriches with citation metadata + edges
        ↓  (passes same IDs)
    enrich_openalex           ← enriches with author/concept metadata

Rate limit handling:
    - Pre-flight health check skips the entire DAG run if APIs are throttled
    - Per-task jitter spreads requests across a 5-minute window
    - Every-2-days schedule gives rate limits time to fully reset
    - Each ingestion script has its own backoff logic (30s for arXiv 429s)

Each task is a PythonOperator that imports and calls the ingestion
functions directly — no Spark needed at this stage.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ── DAG default arguments ─────────────────────────────────────────────────────

default_args = {
    "owner":             "research-intelligence",
    "depends_on_past":   False,
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),  # kill task if it runs > 3 hours
}

# ── Categories to ingest ──────────────────────────────────────────────────────

CATEGORIES = ["cs.LG", "cs.CL", "cs.CV", "cs.AI", "cs.IR"]

# Maximum random jitter in seconds added before each task starts
# Spreads API requests across a 5-minute window instead of all hitting at once
JITTER_MAX_SECONDS = 300


# ── Task functions ────────────────────────────────────────────────────────────

def task_check_api_health(**kwargs) -> None:
    """
    Pre-flight check: verify arXiv and S2ORC are not rate limiting before
    running the full ingestion pipeline.

    If either API returns 429, raises an exception which marks this task
    as failed and skips all downstream tasks for this DAG run. This is
    cleaner than letting each task partially fail mid-run.

    A skipped run is not a failure — the DAG will try again in 2 days.
    """
    import requests

    logger.info("Running pre-flight API health check...")

    # ── Check arXiv ───────────────────────────────────────────────────────
    try:
        resp = requests.get(
            "https://export.arxiv.org/api/query",
            params={"search_query": "cat:cs.LG", "max_results": 1},
            timeout=15,
        )
        if resp.status_code == 429:
            raise Exception(
                "arXiv rate limit is active — skipping this DAG run. "
                "Will retry in 2 days."
            )
        logger.info("arXiv health check passed (status %d)", resp.status_code)
    except requests.RequestException as e:
        raise Exception(f"arXiv health check failed: {e}") from e

    # ── Check Semantic Scholar ────────────────────────────────────────────
    try:
        resp = requests.get(
            "https://api.semanticscholar.org/graph/v1/paper/search",
            params={"query": "machine learning", "limit": 1, "fields": "title"},
            timeout=15,
        )
        if resp.status_code == 429:
            raise Exception(
                "S2ORC rate limit is active — skipping this DAG run. "
                "Will retry in 2 days."
            )
        logger.info("S2ORC health check passed (status %d)", resp.status_code)
    except requests.RequestException as e:
        raise Exception(f"S2ORC health check failed: {e}") from e

    logger.info("All API health checks passed — proceeding with ingestion.")


def task_ingest_arxiv(ti, **kwargs) -> None:
    """
    Step 1: Pull new papers from arXiv and write to HDFS.

    Adds random jitter before starting to avoid hitting rate limits at
    exactly 03:00 UTC every run. Pushes paper IDs to XCom for downstream.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from dotenv import load_dotenv
    load_dotenv("/opt/airflow/.env")

    from ingestion.arxiv import ingest_arxiv
    from ingestion.hdfs_client import HDFSClient

    # Random jitter — spread requests across a 5-minute window
    jitter = random.randint(0, JITTER_MAX_SECONDS)
    logger.info("arXiv task waiting %ds jitter before starting...", jitter)
    time.sleep(jitter)

    hdfs    = HDFSClient()
    summary = ingest_arxiv(categories=CATEGORIES, hdfs_client=hdfs)

    paper_ids = _read_todays_ids(hdfs, CATEGORIES, source="arxiv")

    logger.info(
        "arXiv task complete | papers=%d | ids_collected=%d",
        sum(summary.values()), len(paper_ids)
    )

    ti.xcom_push(key="paper_ids",     value=paper_ids)
    ti.xcom_push(key="arxiv_summary", value=summary)


def task_enrich_s2orc(ti, **kwargs) -> None:
    """
    Step 2: Enrich the arXiv paper IDs from Step 1 via Semantic Scholar.

    Adds jitter before starting. Writes enriched metadata + citation
    edges to HDFS. Skips gracefully if no IDs came from arXiv task.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from dotenv import load_dotenv
    load_dotenv("/opt/airflow/.env")

    from ingestion.s2orc import enrich_papers
    from ingestion.hdfs_client import HDFSClient

    paper_ids: list[str] = (
        ti.xcom_pull(task_ids="ingest_arxiv", key="paper_ids") or []
    )

    if not paper_ids:
        logger.warning(
            "No paper IDs from arXiv task — skipping S2ORC enrichment."
        )
        return

    # Jitter before hitting S2ORC API
    jitter = random.randint(0, JITTER_MAX_SECONDS)
    logger.info("S2ORC task waiting %ds jitter before starting...", jitter)
    time.sleep(jitter)

    hdfs = HDFSClient()
    total_papers, total_edges = 0, 0

    for category in CATEGORIES:
        cat_ids = _read_todays_ids(hdfs, [category], source="arxiv")
        if not cat_ids:
            continue
        papers, edges = enrich_papers(
            arxiv_ids=cat_ids, category=category, hdfs_client=hdfs
        )
        total_papers += papers
        total_edges  += edges

        # Brief pause between categories to avoid back-to-back bursts
        time.sleep(5)

    logger.info(
        "S2ORC task complete | enriched=%d | edges=%d",
        total_papers, total_edges
    )
    ti.xcom_push(key="s2_papers", value=total_papers)
    ti.xcom_push(key="s2_edges",  value=total_edges)


def task_enrich_openalex(ti, **kwargs) -> None:
    """
    Step 3: Enrich the same arXiv IDs via OpenAlex.

    Adds jitter before starting. Adds citation counts, author
    institutions, and concept tags. Skips gracefully if no IDs available.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from dotenv import load_dotenv
    load_dotenv("/opt/airflow/.env")

    from ingestion.openalex import enrich_openalex
    from ingestion.hdfs_client import HDFSClient

    paper_ids: list[str] = (
        ti.xcom_pull(task_ids="ingest_arxiv", key="paper_ids") or []
    )

    if not paper_ids:
        logger.warning(
            "No paper IDs from arXiv task — skipping OpenAlex enrichment."
        )
        return

    # Jitter before hitting OpenAlex API
    jitter = random.randint(0, JITTER_MAX_SECONDS)
    logger.info("OpenAlex task waiting %ds jitter before starting...", jitter)
    time.sleep(jitter)

    hdfs  = HDFSClient()
    total = 0

    for category in CATEGORIES:
        cat_ids = _read_todays_ids(hdfs, [category], source="arxiv")
        if not cat_ids:
            continue
        count = enrich_openalex(
            arxiv_ids=cat_ids, category=category, hdfs_client=hdfs
        )
        total += count

        # Brief pause between categories
        time.sleep(3)

    logger.info("OpenAlex task complete | enriched=%d", total)
    ti.xcom_push(key="oa_papers", value=total)


# ── Helper: read today's paper IDs back from HDFS ────────────────────────────

def _read_todays_ids(hdfs, categories: list[str], source: str) -> list[str]:
    """
    Read today's JSONL files from HDFS for the given source and categories.
    Returns a deduplicated list of paper_id values.
    """
    import requests as req
    from datetime import date

    today   = date.today().isoformat()
    all_ids: set[str] = set()

    for category in categories:
        dir_path = f"{hdfs.base_path}/raw/{source}/{category}/{today}"
        list_url = hdfs._url(dir_path) + "&op=LISTSTATUS"

        resp = req.get(list_url)
        if resp.status_code != 200:
            continue

        file_statuses = (
            resp.json().get("FileStatuses", {}).get("FileStatus", [])
        )
        for fs in file_statuses:
            file_path = f"{dir_path}/{fs['pathSuffix']}"
            try:
                records = hdfs.read_json(file_path)
                for r in records:
                    pid = r.get("paper_id")
                    if pid:
                        all_ids.add(pid)
            except Exception as e:
                logger.warning("Could not read %s: %s", file_path, e)

    return list(all_ids)


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="research_intelligence_ingestion",
    description=(
        "Ingestion of academic papers from arXiv, S2ORC, and OpenAlex "
        "every 2 days at 03:00 UTC with rate limit health checks and jitter"
    ),
    schedule_interval="0 3 */2 * *",   # 03:00 UTC every 2 days (off-peak)
    start_date=datetime(2026, 3, 25),
    catchup=False,                      # don't backfill missed runs
    default_args=default_args,
    tags=["ingestion", "arxiv", "s2orc", "openalex"],
) as dag:

    check_health_task = PythonOperator(
        task_id="check_api_health",
        python_callable=task_check_api_health,
        execution_timeout=timedelta(minutes=2),  # health check should be fast
    )

    ingest_arxiv_task = PythonOperator(
        task_id="ingest_arxiv",
        python_callable=task_ingest_arxiv,
    )

    enrich_s2orc_task = PythonOperator(
        task_id="enrich_s2orc",
        python_callable=task_enrich_s2orc,
    )

    enrich_openalex_task = PythonOperator(
        task_id="enrich_openalex",
        python_callable=task_enrich_openalex,
    )

    # Task dependency chain: health check → arXiv → S2ORC → OpenAlex
    # If health check fails, all downstream tasks are automatically skipped
    check_health_task >> ingest_arxiv_task >> enrich_s2orc_task >> enrich_openalex_task