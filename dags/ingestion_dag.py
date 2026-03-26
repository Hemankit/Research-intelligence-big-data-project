"""
dags/ingestion_dag.py

Airflow DAG that runs the three-step ingestion pipeline daily.

Schedule: every day at 06:00 UTC (after arXiv's nightly submission
          processing window closes around 04:00 UTC).

Task flow:
    ingest_arxiv
        ↓  (passes paper IDs via XCom)
    enrich_s2orc
        ↓  (passes same IDs)
    enrich_openalex

Each task is a PythonOperator that imports and calls the ingestion
functions directly — no Spark needed at this stage since ingestion
is handled by the Python layer.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# DAG default arguments

default_args = {
    "owner":            "research-intelligence",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# Categories to ingest

CATEGORIES = ["cs.LG", "cs.CL", "cs.CV", "cs.AI", "cs.IR"]


# Task functions

def task_ingest_arxiv(ti, **kwargs) -> None:
    """
    Step 1: Pull new papers from arXiv and write to HDFS.
    Pushes the collected paper IDs to XCom so downstream tasks can use them.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from dotenv import load_dotenv
    load_dotenv("/opt/airflow/.env")

    from ingestion.arxiv import ingest_arxiv
    from ingestion.hdfs_client import HDFSClient

    hdfs    = HDFSClient()
    summary = ingest_arxiv(categories=CATEGORIES, hdfs_client=hdfs)

    # Collect all paper IDs written today so S2 and OA tasks can use them
    paper_ids = _read_todays_ids(hdfs, CATEGORIES, source="arxiv")

    logger.info("arXiv task complete | papers=%d | ids_collected=%d",
                sum(summary.values()), len(paper_ids))

    # Push IDs to XCom — downstream tasks pull with ti.xcom_pull()
    ti.xcom_push(key="paper_ids", value=paper_ids)
    ti.xcom_push(key="arxiv_summary", value=summary)


def task_enrich_s2orc(ti, **kwargs) -> None:
    """
    Step 2: Enrich the arXiv paper IDs from Step 1 via Semantic Scholar.
    Writes enriched metadata + citation edges to HDFS.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from dotenv import load_dotenv
    load_dotenv("/opt/airflow/.env")

    from ingestion.s2orc import enrich_papers
    from ingestion.hdfs_client import HDFSClient

    # Pull paper IDs from the upstream arXiv task
    paper_ids: list[str] = ti.xcom_pull(task_ids="ingest_arxiv", key="paper_ids") or []

    if not paper_ids:
        logger.warning("No paper IDs from arXiv task — skipping S2ORC enrichment.")
        return

    hdfs = HDFSClient()

    # Enrich per category so HDFS output paths are organised correctly
    arxiv_summary: dict = ti.xcom_pull(task_ids="ingest_arxiv", key="arxiv_summary") or {}
    total_papers, total_edges = 0, 0

    for category in CATEGORIES:
        cat_ids = _read_todays_ids(hdfs, [category], source="arxiv")
        if not cat_ids:
            continue
        papers, edges = enrich_papers(arxiv_ids=cat_ids, category=category, hdfs_client=hdfs)
        total_papers += papers
        total_edges  += edges

    logger.info("S2ORC task complete | enriched=%d | edges=%d", total_papers, total_edges)
    ti.xcom_push(key="s2_papers", value=total_papers)
    ti.xcom_push(key="s2_edges",  value=total_edges)


def task_enrich_openalex(ti, **kwargs) -> None:
    """
    Step 3: Enrich the same arXiv IDs via OpenAlex.
    Adds citation counts, author institutions, and concept tags.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from dotenv import load_dotenv
    load_dotenv("/opt/airflow/.env")

    from ingestion.openalex import enrich_openalex
    from ingestion.hdfs_client import HDFSClient

    paper_ids: list[str] = ti.xcom_pull(task_ids="ingest_arxiv", key="paper_ids") or []

    if not paper_ids:
        logger.warning("No paper IDs from arXiv task — skipping OpenAlex enrichment.")
        return

    hdfs = HDFSClient()
    total = 0

    for category in CATEGORIES:
        cat_ids = _read_todays_ids(hdfs, [category], source="arxiv")
        if not cat_ids:
            continue
        count = enrich_openalex(arxiv_ids=cat_ids, category=category, hdfs_client=hdfs)
        total += count

    logger.info("OpenAlex task complete | enriched=%d", total)
    ti.xcom_push(key="oa_papers", value=total)


# Helper: read today's paper IDs back from HDFS

def _read_todays_ids(hdfs, categories: list[str], source: str) -> list[str]:
    """
    Read today's JSONL files from HDFS for the given source and categories,
    extract and return a deduplicated list of paper_id values.
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

        file_statuses = resp.json().get("FileStatuses", {}).get("FileStatus", [])
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


# DAG definition

with DAG(
    dag_id="research_intelligence_ingestion",
    description="Daily ingestion of academic papers from arXiv, S2ORC, and OpenAlex",
    schedule_interval="0 6 * * *",      # 06:00 UTC every day
    start_date=datetime(2026, 3, 25),
    catchup=False,                       # don't backfill missed runs on first deploy
    default_args=default_args,
    tags=["ingestion", "arxiv", "s2orc", "openalex"],
) as dag:

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

    # Task dependency chain: arXiv → S2ORC → OpenAlex
    ingest_arxiv_task >> enrich_s2orc_task >> enrich_openalex_task
