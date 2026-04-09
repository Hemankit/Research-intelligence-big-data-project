"""
ingestion_main.py
-----------------
Entry point for the research intelligence ingestion pipeline.

Orchestrates the sequential execution of all three ingestion stages:
  1. ArxivIngester      — primary preprint metadata (incremental or bulk)
  2. S2ORC enrichment   — citation edges + Semantic Scholar metadata
  3. OpenAlex enrichment — author institutions, concept tags, citation counts

Usage examples:

  # Incremental run (last 1 day, all configured categories):
  python -m pipelines.ingestion_main

  # Incremental run with a config file:
  python -m pipelines.ingestion_main --config config/ingestion.yaml

  # Bulk historical backfill:
  python -m pipelines.ingestion_main --bulk

  # Run only specific sources:
  python -m pipelines.ingestion_main --sources arxiv,s2orc

  # Custom query and output path:
  python -m pipelines.ingestion_main --query "cat:cs.LG" --output_path /data/ingestion/raw
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Logging setup ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Default configuration ─────────────────────────────────────────────────────

DEFAULT_CATEGORIES: list[str] = [
    "cs.LG",  # Machine Learning
    "cs.CL",  # Computation & Language
    "cs.CV",  # Computer Vision
    "cs.AI",  # Artificial Intelligence
    "cs.IR",  # Information Retrieval
]

DEFAULT_OUTPUT_PATH: str = "/data/ingestion/raw"
ALL_SOURCES: list[str] = ["arxiv", "s2orc", "openalex"]


# ── Config loader ─────────────────────────────────────────────────────────────

def load_config(config_path: str) -> dict:
    """
    Load ingestion configuration from a YAML or JSON config file.

    The config file specifies queries, output paths, date ranges, and
    any source-specific parameters for each ingester. Using a config
    file rather than hardcoded values makes it easy to adjust ingestion
    runs without modifying source code.

    Expected config structure (YAML example)::

        output_path: /data/ingestion/raw
        categories:
          - cs.LG
          - cs.CL
        arxiv:
          lookback_days: 3
          max_results: 1000
          bulk: false
          bulk_max_results: 10000
          bulk_batch_size: 500
        s2orc:
          api_key: ""          # falls back to S2ORC_API_KEY env var
          batch_size: 100
        openalex:
          email: ""            # falls back to OPENALEX_EMAIL env var

    Parameters
    ----------
    config_path : str
        Path to a YAML or JSON configuration file.

    Returns
    -------
    dict
        Parsed configuration dictionary. Returns an empty dict and logs
        a warning if the file cannot be read or parsed.
    """
    path = Path(config_path)
    if not path.exists():
        logger.warning("Config file not found: %s — using defaults.", config_path)
        return {}

    try:
        suffix = path.suffix.lower()

        if suffix in (".yaml", ".yml"):
            try:
                import yaml  # type: ignore
            except ImportError:
                logger.error(
                    "PyYAML is not installed. Install it with: pip install pyyaml"
                )
                return {}
            with open(path) as f:
                config = yaml.safe_load(f) or {}

        elif suffix == ".json":
            with open(path) as f:
                config = json.load(f)

        else:
            logger.warning(
                "Unsupported config format '%s'. Use .yaml or .json.", suffix
            )
            return {}

        logger.info("Loaded config from %s", config_path)
        return config

    except Exception as exc:
        logger.error("Failed to parse config file %s: %s", config_path, exc)
        return {}


# ── Individual stage runners ──────────────────────────────────────────────────

def _run_arxiv(config: dict) -> list[str]:
    """
    Run the arXiv ingestion stage.

    Supports both incremental (daily) and bulk (historical) modes.
    Returns the list of paper_ids ingested, for downstream enrichment.

    Parameters
    ----------
    config : dict
        Full pipeline config. Reads from config['arxiv'] and config['categories'].

    Returns
    -------
    list[str]
        Deduplicated list of paper_ids written to HDFS during this run.
    """
    from ingestion.arxiv import ingest_arxiv, bulk_ingest_arxiv
    from ingestion.hdfs_client import HDFSClient

    arxiv_cfg  = config.get("arxiv", {})
    categories = config.get("categories", DEFAULT_CATEGORIES)
    is_bulk    = arxiv_cfg.get("bulk", False)
    hdfs       = HDFSClient()

    logger.info(
        "=== arXiv ingestion | mode=%s | categories=%s ===",
        "BULK" if is_bulk else "INCREMENTAL",
        categories,
    )

    if is_bulk:
        summary = bulk_ingest_arxiv(
            categories=categories,
            max_results=arxiv_cfg.get("bulk_max_results"),
            batch_size=arxiv_cfg.get("bulk_batch_size"),
            hdfs_client=hdfs,
        )
    else:
        summary = ingest_arxiv(
            categories=categories,
            lookback_days=arxiv_cfg.get("lookback_days"),
            max_results=arxiv_cfg.get("max_results"),
            hdfs_client=hdfs,
        )

    total = sum(summary.values())
    logger.info("arXiv stage complete | total=%d | breakdown=%s", total, summary)

    # Collect the paper_ids written today so downstream stages can enrich them
    paper_ids = _collect_todays_ids(hdfs, categories, source="arxiv")
    logger.info(
        "Collected %d arXiv paper_ids for downstream enrichment", len(paper_ids)
    )
    return paper_ids


def _run_s2orc(paper_ids: list[str], config: dict) -> tuple[int, int]:
    """
    Run the S2ORC enrichment stage.

    Looks up each arXiv paper_id in Semantic Scholar, writes enriched
    metadata and citation edges to HDFS.

    Parameters
    ----------
    paper_ids : list[str]
        arXiv IDs collected from the arXiv ingestion stage.
    config : dict
        Full pipeline config. Reads from config['s2orc'] and config['categories'].

    Returns
    -------
    (papers_written, edges_written) : tuple[int, int]
    """
    from ingestion.S2orc import enrich_papers
    from ingestion.hdfs_client import HDFSClient

    if not paper_ids:
        logger.warning("S2ORC stage: no paper_ids to enrich — skipping.")
        return 0, 0

    s2orc_cfg  = config.get("s2orc", {})
    categories = config.get("categories", DEFAULT_CATEGORIES)
    hdfs       = HDFSClient()
    api_key    = s2orc_cfg.get("api_key") or None  # falls back to env var

    logger.info(
        "=== S2ORC enrichment | papers=%d | categories=%s ===",
        len(paper_ids), categories,
    )

    total_papers, total_edges = 0, 0

    for category in categories:
        # Re-read per-category IDs from HDFS so each category gets its own
        # targeted enrichment rather than enriching the whole pool each time.
        cat_ids = _collect_todays_ids(hdfs, [category], source="arxiv")
        if not cat_ids:
            logger.info("S2ORC: no IDs for category %s today — skipping.", category)
            continue

        logger.info(
            "S2ORC enriching %d papers for category %s", len(cat_ids), category
        )
        papers, edges = enrich_papers(
            arxiv_ids=cat_ids,
            category=category,
            hdfs_client=hdfs,
            api_key=api_key,
        )
        total_papers += papers
        total_edges  += edges
        time.sleep(5)  # brief pause between categories to avoid burst throttling

    logger.info(
        "S2ORC stage complete | papers=%d | edges=%d",
        total_papers, total_edges,
    )
    return total_papers, total_edges


def _run_openalex(paper_ids: list[str], config: dict) -> int:
    """
    Run the OpenAlex enrichment stage.

    Looks up each arXiv ID in OpenAlex and writes enriched records
    (citation counts, author affiliations, concept tags) to HDFS.
    OpenAlex runs last because it supplements records already written
    by the first two stages.

    Parameters
    ----------
    paper_ids : list[str]
        arXiv IDs collected from the arXiv ingestion stage.
    config : dict
        Full pipeline config. Reads from config['openalex'] and config['categories'].

    Returns
    -------
    int
        Total number of OpenAlex records written.
    """
    from ingestion.Openalex import enrich_openalex
    from ingestion.hdfs_client import HDFSClient

    if not paper_ids:
        logger.warning("OpenAlex stage: no paper_ids to enrich — skipping.")
        return 0

    categories = config.get("categories", DEFAULT_CATEGORIES)
    hdfs       = HDFSClient()

    logger.info(
        "=== OpenAlex enrichment | papers=%d | categories=%s ===",
        len(paper_ids), categories,
    )

    total = 0

    for category in categories:
        cat_ids = _collect_todays_ids(hdfs, [category], source="arxiv")
        if not cat_ids:
            logger.info(
                "OpenAlex: no IDs for category %s today — skipping.", category
            )
            continue

        logger.info(
            "OpenAlex enriching %d papers for category %s", len(cat_ids), category
        )
        count = enrich_openalex(
            arxiv_ids=cat_ids,
            category=category,
            hdfs_client=hdfs,
        )
        total += count
        time.sleep(3)  # brief pause between categories

    logger.info("OpenAlex stage complete | total=%d", total)
    return total


# ── Main orchestrator ─────────────────────────────────────────────────────────

def run_all(config: dict) -> dict:
    """
    Instantiate and run all three ingestion stages sequentially using
    the provided configuration.

    Order of execution:
      1. ArxivIngester   — primary preprint metadata
      2. S2ORC enrichment — full citation edges and Semantic Scholar metadata
      3. OpenAlex enrichment — supplementary author/concept metadata

    OpenAlex runs last because it enriches records already written by
    the first two stages.

    Parameters
    ----------
    config : dict
        Configuration dictionary as returned by load_config() or built
        by parse_args(). Must contain 'categories' and optional
        'arxiv', 's2orc', 'openalex' sub-dicts for per-source tuning.

    Returns
    -------
    dict
        Summary of records written per source::

            {
                "arxiv":   {"paper_ids_collected": 320, "categories": {...}},
                "s2orc":   {"papers": 280, "edges": 14300},
                "openalex": {"papers": 265},
                "duration_seconds": 184.3,
            }
    """
    sources = config.get("sources", ALL_SOURCES)
    summary: dict = {}
    paper_ids: list[str] = []

    # ── Stage 1: arXiv ────────────────────────────────────────────────────
    if "arxiv" in sources:
        try:
            paper_ids = _run_arxiv(config)
            summary["arxiv"] = {"paper_ids_collected": len(paper_ids)}
        except Exception as exc:
            logger.error("arXiv stage failed: %s", exc, exc_info=True)
            summary["arxiv"] = {"error": str(exc)}
            # Abort downstream if arXiv failed and we have no IDs
            if not paper_ids:
                logger.error(
                    "No paper IDs available — skipping S2ORC and OpenAlex."
                )
                return summary
    else:
        logger.info("arXiv stage skipped (not in --sources).")

    # ── Stage 2: S2ORC ────────────────────────────────────────────────────
    if "s2orc" in sources:
        try:
            papers, edges = _run_s2orc(paper_ids, config)
            summary["s2orc"] = {"papers": papers, "edges": edges}
        except Exception as exc:
            logger.error("S2ORC stage failed: %s", exc, exc_info=True)
            summary["s2orc"] = {"error": str(exc)}
    else:
        logger.info("S2ORC stage skipped (not in --sources).")

    # ── Stage 3: OpenAlex ─────────────────────────────────────────────────
    if "openalex" in sources:
        try:
            oa_count = _run_openalex(paper_ids, config)
            summary["openalex"] = {"papers": oa_count}
        except Exception as exc:
            logger.error("OpenAlex stage failed: %s", exc, exc_info=True)
            summary["openalex"] = {"error": str(exc)}
    else:
        logger.info("OpenAlex stage skipped (not in --sources).")

    return summary


# ── Argument parser ───────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for running the ingestion pipeline.

    Flags
    -----
    --config      : Path to a YAML or JSON config file (overrides other flags).
    --query       : arXiv category filter passed to each ingester (e.g. 'cat:cs.LG').
    --output_path : Destination path for all ingester outputs.
    --sources     : Comma-separated list of sources to run (default: arxiv,s2orc,openalex).
    --bulk        : Switch arXiv stage to bulk historical mode (default: incremental).
    --lookback    : Days back for arXiv incremental mode (default: 1).
    --max         : Max papers per arXiv category (incremental default: 500).

    Returns
    -------
    argparse.Namespace
        Parsed argument object with attributes for each flag.
    """
    parser = argparse.ArgumentParser(
        prog="ingestion_main",
        description="Research Intelligence — ingestion pipeline entry point",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Incremental run (default, last 1 day, all categories):
  python -m pipelines.ingestion_main

  # Run with a YAML config file:
  python -m pipelines.ingestion_main --config config/ingestion.yaml

  # Bulk historical backfill of arXiv only:
  python -m pipelines.ingestion_main --bulk --sources arxiv

  # Incremental, last 7 days, only arXiv + OpenAlex:
  python -m pipelines.ingestion_main --lookback 7 --sources arxiv,openalex

  # Custom output path:
  python -m pipelines.ingestion_main --output_path /data/ingestion/raw
        """,
    )

    parser.add_argument(
        "--config",
        default=None,
        metavar="PATH",
        help="Path to a YAML or JSON config file. Overrides all other flags.",
    )
    parser.add_argument(
        "--query",
        default=None,
        help="arXiv category filter, e.g. 'cat:cs.LG'. Ignored when --config is set.",
    )
    parser.add_argument(
        "--output_path",
        default=DEFAULT_OUTPUT_PATH,
        help=f"Destination path for ingester outputs (default: {DEFAULT_OUTPUT_PATH}).",
    )
    parser.add_argument(
        "--sources",
        default=",".join(ALL_SOURCES),
        help=(
            "Comma-separated list of sources to run. "
            f"Available: {', '.join(ALL_SOURCES)}. "
            "Default: all three."
        ),
    )
    parser.add_argument(
        "--bulk",
        action="store_true",
        default=False,
        help="Run arXiv in bulk historical mode instead of incremental.",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=None,
        help="Days back for arXiv incremental mode (default: 1 or ARXIV_LOOKBACK_DAYS env var).",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        dest="max_results",
        help="Max papers per arXiv category in incremental mode (default: 500).",
    )

    return parser.parse_args()


# ── HDFS ID collector (shared by stage runners) ───────────────────────────────

def _collect_todays_ids(hdfs, categories: list[str], source: str) -> list[str]:
    """
    Read today's JSONL files from HDFS for the given source and categories.
    Returns a deduplicated list of paper_id values.

    This mirrors the helper used in the Airflow DAG so the standalone
    pipeline runner and the DAG share the same ID-collection logic.

    Parameters
    ----------
    hdfs       : HDFSClient instance
    categories : arXiv category strings to look up
    source     : 'arxiv', 's2orc', or 'openalex'

    Returns
    -------
    list[str]
        Deduplicated paper_ids found in today's HDFS output.
    """
    import requests as req
    from datetime import date

    today   = date.today().isoformat()
    all_ids: set[str] = set()

    for category in categories:
        dir_path = f"{hdfs.base_path}/raw/{source}/{category}/{today}"
        list_url = hdfs._url(dir_path) + "&op=LISTSTATUS"

        try:
            resp = req.get(list_url, timeout=10)
        except Exception as exc:
            logger.warning("Could not list HDFS dir %s: %s", dir_path, exc)
            continue

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
                        all_ids.add(str(pid))
            except Exception as exc:
                logger.warning("Could not read HDFS file %s: %s", file_path, exc)

    return list(all_ids)


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = parse_args()

    # ── Build effective config ────────────────────────────────────────────
    if args.config:
        # Config file takes full precedence
        config = load_config(args.config)
    else:
        # Build config from CLI flags
        config = {
            "output_path": args.output_path,
            "categories":  DEFAULT_CATEGORIES,
            "sources":     [s.strip() for s in args.sources.split(",") if s.strip()],
            "arxiv": {
                "bulk":         args.bulk,
                "lookback_days": args.lookback,
                "max_results":  args.max_results,
            },
            "s2orc":     {},
            "openalex":  {},
        }

    # If --sources was passed alongside --config, CLI flag overrides config file
    if not args.config:
        config["sources"] = [s.strip() for s in args.sources.split(",") if s.strip()]

    # Validate sources
    unknown = [s for s in config.get("sources", ALL_SOURCES) if s not in ALL_SOURCES]
    if unknown:
        logger.error("Unknown source(s): %s. Valid options: %s", unknown, ALL_SOURCES)
        sys.exit(1)

    # ── Banner ────────────────────────────────────────────────────────────
    start_time = datetime.now(timezone.utc)
    arxiv_cfg  = config.get("arxiv", {})

    print("\n" + "=" * 60)
    print("  Research Intelligence — Ingestion Pipeline")
    print("=" * 60)
    print(f"  Start time   : {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Mode         : {'BULK' if arxiv_cfg.get('bulk') else 'INCREMENTAL'}")
    print(f"  Sources      : {', '.join(config.get('sources', ALL_SOURCES))}")
    print(f"  Categories   : {config.get('categories', DEFAULT_CATEGORIES)}")
    print(f"  Output path  : {config.get('output_path', DEFAULT_OUTPUT_PATH)}")
    if not arxiv_cfg.get("bulk"):
        lookback = arxiv_cfg.get("lookback_days", 1)
        print(f"  Lookback     : {lookback} day(s)")
    print("=" * 60 + "\n")

    # ── Run pipeline ──────────────────────────────────────────────────────
    try:
        summary = run_all(config)
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user.")
        sys.exit(130)
    except Exception as exc:
        logger.critical("Pipeline failed with unhandled exception: %s", exc, exc_info=True)
        sys.exit(1)

    # ── Final summary ─────────────────────────────────────────────────────
    end_time  = datetime.now(timezone.utc)
    elapsed   = (end_time - start_time).total_seconds()
    summary["duration_seconds"] = round(elapsed, 1)

    print("\n" + "=" * 60)
    print("  Pipeline Summary")
    print("=" * 60)

    arxiv_s = summary.get("arxiv", {})
    if "error" in arxiv_s:
        print(f"  arXiv        : ERROR — {arxiv_s['error']}")
    else:
        print(f"  arXiv        : {arxiv_s.get('paper_ids_collected', 0)} papers ingested")

    s2orc_s = summary.get("s2orc", {})
    if "error" in s2orc_s:
        print(f"  S2ORC        : ERROR — {s2orc_s['error']}")
    elif s2orc_s:
        print(
            f"  S2ORC        : {s2orc_s.get('papers', 0)} papers, "
            f"{s2orc_s.get('edges', 0)} citation edges"
        )

    oa_s = summary.get("openalex", {})
    if "error" in oa_s:
        print(f"  OpenAlex     : ERROR — {oa_s['error']}")
    elif oa_s:
        print(f"  OpenAlex     : {oa_s.get('papers', 0)} papers enriched")

    print(f"  Duration     : {elapsed:.1f}s")
    print(f"  Finished at  : {end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60 + "\n")
