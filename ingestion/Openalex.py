"""
openalex.py
-----------
Supplementary enrichment from the OpenAlex API.

What this script does:
  1. Takes a list of arXiv IDs and looks each one up in OpenAlex to fetch:
       - Normalised citation counts
       - Author profiles + institution affiliations
       - OpenAlex concept/topic tags with confidence scores
       - Open-access URL if available
  2. Writes enriched records to HDFS:
       raw/openalex/{category}/{date}/*.jsonl

How arXiv ID lookup works:
  OpenAlex does not support arXiv IDs as a direct URN lookup (only doi,
  mag, pmid, pmcid are supported). Instead we use:
    filter=indexed_in:arxiv,title_and_abstract.search:{arxiv_id}
  This finds papers indexed from arXiv that match the ID in their metadata.

Run directly:
    python -m ingestion.openalex --ids [any arXiv IDs in the HDFS database] --category cs.LG

Or import enrich_openalex() into a pipeline.
"""

import argparse
import logging
import os
import time
from datetime import datetime, timezone

import requests
from tqdm import tqdm

from ingestion.hdfs_client import HDFSClient

logger = logging.getLogger(__name__)

# Configuration 

OA_BASE_URL = "https://api.openalex.org"
OA_EMAIL: str = os.getenv("OPENALEX_EMAIL", "")
REQUEST_DELAY: float = float(os.getenv("OA_REQUEST_DELAY", "0.15"))
BATCH_SIZE: int = 50


# HTTP helper

def _get(url: str, params: dict = None, retries: int = 3) -> dict | None:
    """GET with retry and exponential back-off. No select parameter injected."""
    # Always add email for polite pool if configured
    p = dict(params or {})
    if OA_EMAIL:
        p["mailto"] = OA_EMAIL

    for attempt in range(retries):
        try:
            resp = requests.get(url, params=p, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** attempt * 5
                logger.warning("Rate limited by OpenAlex — waiting %ds", wait)
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            logger.error("Request failed (attempt %d/%d): %s", attempt + 1, retries, exc)
            time.sleep(2 ** attempt)
    return None


# Schema normalisation 

def _normalise(work: dict, arxiv_id: str) -> dict:
    """Flatten an OpenAlex work object into the pipeline schema."""
    authors = []
    for authorship in (work.get("authorships") or []):
        author_info  = authorship.get("author") or {}
        institutions = authorship.get("institutions") or []
        inst         = institutions[0] if institutions else {}
        authors.append({
            "name":                author_info.get("display_name", ""),
            "orcid":               author_info.get("orcid", ""),
            "institution_name":    inst.get("display_name", ""),
            "institution_country": inst.get("country_code", ""),
        })

    concepts = sorted(
        [
            {"name": c.get("display_name", ""), "score": c.get("score", 0.0)}
            for c in (work.get("concepts") or [])
        ],
        key=lambda x: x["score"],
        reverse=True,
    )

    oa     = work.get("open_access") or {}
    oa_url = oa.get("oa_url", "")
    is_oa  = oa.get("is_oa", False)

    return {
        "paper_id":         arxiv_id,
        "source":           "openalex",
        "openalex_id":      work.get("id", ""),
        "doi":              work.get("doi", ""),
        "title":            (work.get("title") or "").strip(),
        "publication_date": work.get("publication_date", ""),
        "publication_year": work.get("publication_year"),
        "cited_by_count":   work.get("cited_by_count", 0),
        "authors":          authors,
        "concepts":         concepts,
        "is_open_access":   is_oa,
        "oa_url":           oa_url,
        "ingested_at":      datetime.now(timezone.utc).isoformat(),
    }


# Main ingestion function 

def enrich_openalex(
    arxiv_ids: list[str],
    category: str = "general",
    hdfs_client: HDFSClient = None,
) -> int:
    """
    Look up each arXiv ID in OpenAlex and write enriched records to HDFS.

    Uses filter=indexed_in:arxiv combined with the arXiv ID as a search
    term to find matching papers. Normalises the OpenAlex work object into a flat schema with
    selected fields for authors, concepts, citation counts, and open-access info.

    Parameters
    ----------
    arxiv_ids   : list of arXiv IDs to enrich
    category    : arXiv category label for HDFS path organisation
    hdfs_client : HDFSClient instance; created with defaults if not passed.

    Returns
    -------
    Number of records written to HDFS.
    """
    hdfs    = hdfs_client or HDFSClient()
    records: list[dict] = []
    missing: list[str]  = []

    for arxiv_id in tqdm(arxiv_ids, desc="OpenAlex enrichment", unit="paper"):
        # Search for the paper using its arXiv ID as a search term,
        # filtered to only papers that are indexed in arXiv.
        params = {
            "filter":   "indexed_in:arxiv",
            "search":   arxiv_id,
            "per_page": 1,
        }
        data = _get(f"{OA_BASE_URL}/works", params)

        if not data:
            missing.append(arxiv_id)
            time.sleep(REQUEST_DELAY)
            continue

        results = data.get("results") or []
        if not results:
            missing.append(arxiv_id)
            time.sleep(REQUEST_DELAY)
            continue

        records.append(_normalise(results[0], arxiv_id))
        time.sleep(REQUEST_DELAY)

    if missing:
        logger.warning(
            "%d arXiv IDs had no OpenAlex match: %s%s",
            len(missing),
            ", ".join(missing[:5]),
            "..." if len(missing) > 5 else "",
        )

    if records:
        hdfs.write_json(records, source="openalex", category=category)
        logger.info("Wrote %d OpenAlex records to HDFS", len(records))
    else:
        logger.warning("No OpenAlex records to write for category %s", category)

    return len(records)


# CLI entry point 
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(description="Enrich papers via OpenAlex API")
    parser.add_argument(
        "--ids",
        nargs="+",
        required=True,
        help="One or more arXiv IDs, e.g. --ids 2312.05934 2310.12321",
    )
    parser.add_argument(
        "--category",
        default="general",
        help="arXiv category label for HDFS output path organisation",
    )
    args = parser.parse_args()

    count = enrich_openalex(args.ids, category=args.category)
    print(f"\nDone: {count} OpenAlex records written to HDFS.")