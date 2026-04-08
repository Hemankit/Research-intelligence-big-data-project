"""
s2orc_bulk_download.py
======================
Downloader for the Semantic Scholar S2ORC bulk dataset (full-text corpus).

The Semantic Scholar Graph API (used by ingestion/s2orc.py's API mode)
only returns metadata + abstracts. To get real parsed full text —
including sections, inline citation markers, and bibliography — you need
the S2ORC bulk dataset served through the Datasets API:

    https://api.semanticscholar.org/datasets/v1/release/{release}/dataset/s2orc

Each release exposes ~30 shards as .gz files (gzipped JSONL). Each line
is one paper with fields like:
    {
        "corpusid": int,
        "externalids": {"arxiv": "...", "doi": "...", ...},
        "content": {
            "source": {...},
            "text": "<full body text>",
            "annotations": {
                "title": "[{\"start\":0,\"end\":42}]",
                "abstract": "[...]",
                "author": "[...]",
                "section_header": "[...]",
                "paragraph": "[...]",
                ...
            }
        }
    }

This script downloads shard files to local disk, then optionally chains
into the existing S2ORCIngester corpus mode (which now understands the
bulk format thanks to the normalize() patch) to land the records as
normalized JSONL in HDFS under /raw/s2orc/<category>/<date>/, where
spark_consolidate.py will pick them up on the next run.

Usage:
    # List available shards without downloading (verifies API key + access)
    python -m ingestion.s2orc_bulk_download --list-only

    # Download 1 shard for testing (1–3 GB)
    python -m ingestion.s2orc_bulk_download --shards 1 --local-dir ./s2orc_shards

    # Download 3 shards and immediately ingest them into HDFS as normalized
    # records, deleting the raw .gz files after each successful ingest
    python -m ingestion.s2orc_bulk_download \
        --shards 3 \
        --local-dir ./s2orc_shards \
        --ingest \
        --category s2orc_fulltext \
        --cleanup

    # Cap how many records get pulled from each shard (for quick tests)
    python -m ingestion.s2orc_bulk_download \
        --shards 1 --local-dir ./s2orc_shards \
        --ingest --max-per-shard 500

Requires:
    S2ORC_API_KEY in .env — the bulk Datasets API requires authentication,
    and your key must have access to the s2orc dataset specifically
    (Graph API access is *not* the same thing as bulk dataset access).

Notes:
    - Shards are typically 1–3 GB compressed, decompressing to 8–15 GB.
      Plan disk space accordingly before downloading many shards.
    - The Datasets API returns pre-signed S3 URLs that expire after a few
      minutes — we fetch the URL list and start downloading immediately.
    - Raw shards are NOT uploaded to HDFS directly. Only the normalized
      records produced by S2ORCIngester land in HDFS via write_json().
      This matches how every other ingester in the project works.
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("s2orc_bulk_download")

# Constants

DATASETS_API = "https://api.semanticscholar.org/datasets/v1"
DATASET_NAME = "s2orc"

# Datasets API helpers

def get_latest_release(api_key: str) -> str:
    """
    Query the Datasets API for the latest available release ID.
    Returns a date string like '2024-12-17'.
    """
    url = f"{DATASETS_API}/release/latest"
    headers = {"x-api-key": api_key} if api_key else {}

    logger.info("Fetching latest release info from %s", url)
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    release_id = data.get("release_id")
    if not release_id:
        raise RuntimeError(f"No release_id in response: {data}")

    logger.info("Latest release: %s", release_id)
    return release_id


def get_shard_urls(release_id: str, api_key: str) -> list[str]:
    """
    Fetch the list of pre-signed shard download URLs for the s2orc dataset
    in the given release. URLs expire quickly (~5 min), so download soon
    after calling.
    """
    url = f"{DATASETS_API}/release/{release_id}/dataset/{DATASET_NAME}"
    headers = {"x-api-key": api_key} if api_key else {}

    logger.info("Fetching shard URLs for release=%s dataset=%s",
                release_id, DATASET_NAME)
    resp = requests.get(url, headers=headers, timeout=30)

    if resp.status_code == 401:
        raise RuntimeError(
            "401 Unauthorized — set S2ORC_API_KEY in .env. "
            "Note that Graph API keys and bulk Datasets API access are "
            "sometimes gated separately."
        )
    if resp.status_code == 403:
        raise RuntimeError(
            "403 Forbidden — your API key does not have access to the "
            "s2orc bulk dataset. Request access at "
            "https://www.semanticscholar.org/product/api#api-key-form "
            "and specify that you need the bulk datasets endpoint."
        )
    resp.raise_for_status()

    data = resp.json()
    files = data.get("files", [])
    if not files:
        raise RuntimeError(f"No files in dataset response: {data}")

    logger.info("Release %s has %d shards available", release_id, len(files))
    return files


# Download

def download_shard(
    url: str,
    dest_path: Path,
    chunk_size: int = 1024 * 1024,
) -> int:
    """
    Stream a shard URL to a local file. Returns total bytes written.
    Logs progress every ~50 MB.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    bytes_written = 0
    last_logged = 0
    start = time.time()

    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        total_size = int(resp.headers.get("content-length", 0))

        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                f.write(chunk)
                bytes_written += len(chunk)

                # Log progress every 50 MB
                if bytes_written - last_logged >= 50 * 1024 * 1024:
                    last_logged = bytes_written
                    if total_size:
                        pct = 100 * bytes_written / total_size
                        logger.info(
                            "  ...%.1f%% (%.1f / %.1f MB)",
                            pct,
                            bytes_written / 1024 / 1024,
                            total_size / 1024 / 1024,
                        )
                    else:
                        logger.info(
                            "  ...%.1f MB downloaded",
                            bytes_written / 1024 / 1024,
                        )

    elapsed = time.time() - start
    mb = bytes_written / 1024 / 1024
    logger.info(
        "Saved %s (%.1f MB in %.1fs, %.2f MB/s)",
        dest_path.name, mb, elapsed, mb / max(elapsed, 0.1),
    )
    return bytes_written


# Main

def main():
    parser = argparse.ArgumentParser(
        description="Download S2ORC bulk full-text shards.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick test: download 1 shard locally
  python -m ingestion.s2orc_bulk_download --shards 1 --local-dir ./s2orc_shards

  # Download 3 shards, ingest into HDFS as normalized records, clean up raw .gz
  python -m ingestion.s2orc_bulk_download \\
      --shards 3 --local-dir ./s2orc_shards \\
      --ingest --category s2orc_fulltext --cleanup

  # Pilot run: only pull 1000 records out of each shard
  python -m ingestion.s2orc_bulk_download \\
      --shards 1 --local-dir ./s2orc_shards \\
      --ingest --max-per-shard 1000
        """,
    )

    parser.add_argument(
        "--release", default="latest",
        help="S2ORC release ID (default: latest)",
    )
    parser.add_argument(
        "--shards", type=int, default=1,
        help="Number of shards to download (default: 1)",
    )
    parser.add_argument(
        "--start-shard", type=int, default=0,
        help="Index of first shard (default: 0). Useful for resuming.",
    )
    parser.add_argument(
        "--local-dir", default=None,
        help="Directory to save shards to (required unless --list-only)",
    )
    parser.add_argument(
        "--list-only", action="store_true",
        help="Just list available shard URLs and exit (no download)",
    )
    parser.add_argument(
        "--ingest", action="store_true",
        help=(
            "After each shard downloads, run the S2ORCIngester corpus "
            "pipeline on it so normalized records land in HDFS via "
            "write_json(). Requires --local-dir."
        ),
    )
    parser.add_argument(
        "--category", default="s2orc_fulltext",
        help="HDFS folder label for ingested records (default: s2orc_fulltext)",
    )
    parser.add_argument(
        "--max-per-shard", type=int, default=None,
        help=(
            "Cap how many records to pull from each shard during ingest. "
            "Useful for pilot runs. Default: read all records."
        ),
    )
    parser.add_argument(
        "--cleanup", action="store_true",
        help="Delete the raw .gz shard file after successful ingest",
    )
    parser.add_argument(
        "--api-key", default=None,
        help="Semantic Scholar API key (overrides S2ORC_API_KEY env var)",
    )

    args = parser.parse_args()

    api_key = args.api_key or os.getenv("S2ORC_API_KEY", "")
    if not api_key:
        logger.error(
            "No API key found. Set S2ORC_API_KEY in .env or pass --api-key."
        )
        sys.exit(1)

    if not args.list_only and not args.local_dir:
        parser.error("--local-dir is required unless using --list-only")

    # 1. Resolve release ID
    release_id = (
        get_latest_release(api_key) if args.release == "latest"
        else args.release
    )

    # 2. Get shard URLs
    shard_urls = get_shard_urls(release_id, api_key)

    if args.list_only:
        print(f"\nRelease: {release_id}")
        print(f"Total shards: {len(shard_urls)}\n")
        for i, url in enumerate(shard_urls):
            # Strip query string (pre-signed URL tokens) for readability
            filename = url.split("?")[0].split("/")[-1]
            print(f"  [{i:3d}] {filename}")
        return

    # 3. Pick slice of shards
    end_shard = min(args.start_shard + args.shards, len(shard_urls))
    selected = shard_urls[args.start_shard:end_shard]

    logger.info(
        "Selected shards %d..%d of %d (release=%s)",
        args.start_shard, end_shard - 1, len(shard_urls), release_id,
    )

    # 4. Set up local dir
    local_dir = Path(args.local_dir) / release_id
    local_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Local destination: %s", local_dir)

    # 5. Optional: preload the ingester once so we fail fast if imports break
    ingester_cls = None
    if args.ingest:
        try:
            from ingestion.S2orc import S2ORCIngester
            ingester_cls = S2ORCIngester
            logger.info("S2ORCIngester loaded — will chain into corpus ingest")
        except ImportError as e:
            logger.error(
                "Could not import S2ORCIngester (%s). "
                "Make sure you run this from the project root with "
                "`python -m ingestion.s2orc_bulk_download`.",
                e,
            )
            sys.exit(1)

    # 6. Download + optional ingest loop
    total_downloaded = 0
    total_ingested = 0
    failed_shards = []

    for i, url in enumerate(selected):
        shard_idx = args.start_shard + i
        filename = url.split("?")[0].split("/")[-1]
        if not filename.endswith(".gz"):
            filename = f"shard_{shard_idx:03d}.jsonl.gz"

        logger.info("─" * 60)
        logger.info("Shard %d/%d: %s", i + 1, len(selected), filename)

        local_path = local_dir / filename

        # Download
        try:
            bytes_w = download_shard(url, local_path)
            total_downloaded += bytes_w
        except Exception as e:
            logger.error("Download failed for shard %d: %s", shard_idx, e)
            failed_shards.append((shard_idx, "download", str(e)))
            continue

        # Ingest
        if args.ingest and ingester_cls is not None:
            logger.info("Ingesting %s via S2ORCIngester corpus mode...", filename)
            try:
                ingester = ingester_cls(config={
                    "mode":        "corpus",
                    "corpus_path": str(local_path),
                })
                records = ingester.run(
                    query=str(local_path),
                    category=args.category,
                    batch_size=1000,
                    max_records=args.max_per_shard,
                )
                n = len(records)
                total_ingested += n
                logger.info("  → %d records normalized and written to HDFS", n)

                # Cleanup raw shard after successful ingest
                if args.cleanup:
                    try:
                        local_path.unlink()
                        logger.info("  → deleted raw shard %s", filename)
                    except OSError as e:
                        logger.warning("  Could not delete %s: %s", filename, e)

            except Exception as e:
                logger.error("Ingest failed for %s: %s", filename, e)
                failed_shards.append((shard_idx, "ingest", str(e)))
                continue

    # 7. Summary
    logger.info("═" * 60)
    logger.info(
        "Downloaded %.1f MB across %d shards",
        total_downloaded / 1024 / 1024,
        len(selected) - sum(1 for s in failed_shards if s[1] == "download"),
    )
    if args.ingest:
        logger.info("Ingested %d normalized records to HDFS", total_ingested)
    if failed_shards:
        logger.warning("Failed shards:")
        for idx, stage, err in failed_shards:
            logger.warning("  [%d] %s: %s", idx, stage, err)

    logger.info("Done.")


if __name__ == "__main__":
    main()
