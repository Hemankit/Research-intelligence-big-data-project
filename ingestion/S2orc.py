"""
s2orc.py

Ingester for the Semantic Scholar Open Research Corpus (S2ORC).
Responsible for loading and processing the S2ORC dataset, extracting
paper metadata, full-text content where available, and citation edge lists.

Fixes applied over original version:
  1. paper_id now uses arXiv ID (from externalIds.ArXiv) so it matches
     the arXiv corpus. Falls back to S2ORC corpusid/paperId only if no
     arXiv ID is available. This ensures Spark joins work correctly.
  2. Citation edges are extracted from RAW records before normalization.
     The base class run() normalizes first then saves, which strips the
     references/citations fields. We override run() to extract edges first.
  3. save() writes to HDFS via HDFSClient instead of local filesystem.
     This keeps all pipeline data in HDFS where Spark can read it.
  4. Removed duplicate pandas and datetime imports.
"""

import gzip
import json
import logging
import os
import requests
from datetime import datetime, timezone
from pathlib import Path

from ingestion.base_ingestor2 import (
    BaseIngester,
    SourceConnectionError,
    FetchError,
    NormalizationError,
    SaveError,
)
from ingestion.hdfs_client import HDFSClient

logger = logging.getLogger(__name__)


class S2ORCIngester(BaseIngester):
    """
    Ingests paper metadata and citation edges from the Semantic Scholar
    Open Research Corpus (S2ORC) at scale for bulk storage and indexing.

    **Architecture Context**:
    This ingester performs BLIND, large-scale metadata ingestion during the
    initial pipeline stage. It fetches and stores all available metadata
    broadly without query-based filtering. Query-driven retrieval and
    selective full-text analysis happen later during semantic clustering
    and analysis phases, after papers are indexed and clustered.

    **Modes**:
    - Corpus mode: Processes bulk S2ORC downloads (large JSONL/Parquet shards)
    - API mode: Performs bulk metadata pulls via Semantic Scholar API

    **Output**: Normalized metadata records + citation edge lists for GraphX.
    **Full-text**: NOT retrieved at this stage - only selectively later for
    high-value papers identified through metadata signals.
    """

    def __init__(self, config: dict):
        """
        Parameters
        ----------
        config : dict
            Configuration for this ingester.

            Expected keys:
            - mode         : 'corpus' or 'api'
            - corpus_path  : (corpus mode) path to local shard files
            - api_base_url : (api mode) Semantic Scholar API base URL
            - api_key      : (api mode, optional) API key for higher rate limits
            - hdfs_host    : HDFS NameNode hostname (default: from env HDFS_HOST)
            - hdfs_port    : WebHDFS port (default: 9870)
            - hdfs_user    : Hadoop user (default: 'hadoop')
            - hdfs_base_path: Base HDFS path (default: /user/research-intelligence)
        """
        self.config = config

        self.api_headers = {}
        if self.config.get("api_key"):
            self.api_headers["x-api-key"] = self.config["api_key"]

        # Initialise HDFSClient from config (falls back to env vars / defaults)
        self.hdfs = HDFSClient(
            host      = self.config.get("hdfs_host"),
            port      = int(self.config.get("hdfs_port", 9870)),
            user      = self.config.get("hdfs_user", "hadoop"),
            base_path = self.config.get(
                "hdfs_base_path", "/user/research-intelligence"
            ),
        )

    # ── connect ───────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Validate access to S2ORC (local corpus or API)."""
        try:
            mode = self.config.get("mode")

            if mode == "corpus":
                corpus_path = self.config.get("corpus_path")
                if not corpus_path:
                    raise SourceConnectionError(
                        "Missing 'corpus_path' in config."
                    )
                if not os.path.exists(corpus_path):
                    raise SourceConnectionError(
                        f"S2ORC corpus path not found: {corpus_path}"
                    )

            elif mode == "api":
                base_url = self.config.get("api_base_url")
                if not base_url:
                    raise SourceConnectionError(
                        "Missing 'api_base_url' in config."
                    )
                response = requests.get(
                    base_url, headers=self.api_headers, timeout=5
                )
                response.raise_for_status()

            else:
                raise SourceConnectionError(
                    f"Unknown mode '{mode}'. Expected 'corpus' or 'api'."
                )

        except SourceConnectionError:
            raise
        except requests.RequestException as e:
            raise SourceConnectionError(
                f"S2ORC API connection failed: {e}"
            ) from e
        except Exception as e:
            raise SourceConnectionError(
                f"Unexpected error during S2ORC connection: {e}"
            ) from e

    # ── fetch ─────────────────────────────────────────────────────────────────

    def fetch(self, query: str, **kwargs) -> list[dict]:
        """
        Fetch a batch of raw paper metadata from S2ORC for bulk ingestion.

        Parameters
        ----------
        query : str
            - Corpus mode : path to a shard file
            - API mode    : date range ('2020-01-01:2023-12-31') or
                            field-of-study category (e.g. 'Computer Science')
        **kwargs :
            - batch_size (int) : records per batch (default 1000)
            - offset (int)     : pagination offset (API mode)
            - fields (list)    : metadata fields to retrieve

        Returns
        -------
        list[dict]
            Raw paper metadata records as returned by the source.
        """
        batch_size = kwargs.get("batch_size", 1000)
        offset     = kwargs.get("offset", 0)
        fields     = kwargs.get("fields", [
            "paperId", "corpusId", "title", "abstract", "authors",
            "year", "publicationDate", "venue", "publicationVenue",
            "fieldsOfStudy", "s2FieldsOfStudy", "citationCount",
            "referenceCount", "externalIds",
            "isOpenAccess", "openAccessPdf",
            # Note: 'references' and 'citations' are NOT supported on the
            # bulk search endpoint — they are stripped automatically in
            # _fetch_from_api(). Only available via /paper/{id}/references.
        ])

        mode = self.config.get("mode")

        if mode == "corpus":
            return self._fetch_from_shard(query, batch_size)
        elif mode == "api":
            return self._fetch_from_api(query, batch_size, offset, fields)
        else:
            raise FetchError(
                f"Unknown mode '{mode}'. Expected 'corpus' or 'api'."
            )

    def _fetch_from_shard(self, shard_path: str, batch_size: int) -> list[dict]:
        """Read a shard file (JSONL or gzipped JSONL) line by line."""
        path = Path(shard_path)
        if not path.exists():
            raise FetchError(f"Shard file not found: {shard_path}")

        records = []
        try:
            opener = (
                gzip.open(path, "rt") if path.suffix == ".gz"
                else open(path, "r")
            )
            with opener as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    records.append(json.loads(line))
                    if len(records) >= batch_size:
                        break
        except (OSError, json.JSONDecodeError) as e:
            raise FetchError(
                f"Failed to read shard '{shard_path}': {e}"
            ) from e

        return records

    def _fetch_from_api(
        self,
        query: str,
        batch_size: int,
        offset: int,
        fields: list[str],
    ) -> list[dict]:
        """Bulk metadata pull from Semantic Scholar API.

        Uses /paper/search/bulk which accepts:
        - query      : keyword search matched against title + abstract
        - year       : year range filter e.g. '2020-2024'
        - fields     : comma-separated list of fields to return

        Note: 'references' and 'citations' are NOT supported on the bulk
        endpoint — only available via /paper/{id}/references. We strip them
        from the fields list automatically to avoid 400 errors.
        """
        base_url = self.config.get("api_base_url")
        endpoint = f"{base_url}/paper/search/bulk"

        # references and citations are not supported on bulk search endpoint
        # they cause a 400 Bad Request — strip them out automatically
        unsupported = {"references", "citations"}
        safe_fields = [f for f in fields if f not in unsupported]

        params = {
            "fields": ",".join(safe_fields),
            "limit":  min(batch_size, 1000),  # bulk endpoint max is 1000
        }

        # Parse query: date range → year filter, otherwise keyword search
        if ":" in query and query[:4].isdigit():
            # Date range format: '2023-01-01:2024-12-31' or '2020:2024'
            start, end = query.split(":", 1)
            # Extract just the year portion for the year filter
            start_year = start.split("-")[0]
            end_year   = end.split("-")[0]
            params["year"]  = f"{start_year}-{end_year}"
            params["query"] = "computer science"  # broad keyword to get CS papers
        else:
            # Field of study or keyword — use as search query
            params["query"] = query

        # Pagination token (bulk endpoint uses token, not offset)
        # For first request leave token out; subsequent pages use the
        # token returned in the previous response
        if offset:
            params["token"] = offset

        try:
            response = requests.get(
                endpoint,
                params=params,
                headers=self.api_headers,
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            raise FetchError(f"S2ORC API fetch failed: {e}") from e

        data    = response.json()
        records = data.get("data", [])

        logger.info(
            "S2ORC bulk fetch returned %d records (total: %s)",
            len(records),
            data.get("total", "unknown"),
        )
        return records

    # ── normalize ─────────────────────────────────────────────────────────────

    def normalize(self, raw_record: dict) -> dict:
        """
        Map a raw S2ORC record to the unified paper schema.

        Uses the arXiv ID from externalIds as paper_id so records join
        correctly with arXiv records in Spark. Falls back to S2ORC's
        internal corpusId or paperId only when no arXiv ID is available.

        Parameters
        ----------
        raw_record : dict
            A single raw record from fetch().

        Returns
        -------
        dict
            Normalized record matching the pipeline schema.

        Raises
        ------
        NormalizationError
            If required fields are missing or normalization fails.
        """
        try:
            # ── paper_id: prefer arXiv ID for join compatibility ──────────
            # Using arXiv ID ensures Spark can join S2ORC records with
            # arXiv records on paper_id without any key translation.
            external_ids = raw_record.get("externalIds") or {}
            arxiv_id     = (
                external_ids.get("ArXiv") or external_ids.get("arxiv")
            )
            paper_id = (
                arxiv_id
                or raw_record.get("corpusid")
                or raw_record.get("corpusId")
                or raw_record.get("paperId")
            )
            if not paper_id:
                raise NormalizationError(
                    f"Record missing arXiv ID and all S2ORC IDs: {raw_record}"
                )

            # Strip arXiv version suffix if present (e.g. '2401.12345v2')
            if arxiv_id and "v" in str(arxiv_id):
                paper_id = str(arxiv_id).split("v")[0]

            # ── title ────────────────────────────────────────────────────
            title = (raw_record.get("title") or "").strip()

            # ── abstract ─────────────────────────────────────────────────
            abstract = (raw_record.get("abstract") or "").strip()

            # ── authors ──────────────────────────────────────────────────
            raw_authors = raw_record.get("authors") or []
            authors = [
                a["name"]
                for a in raw_authors
                if isinstance(a, dict) and a.get("name")
            ]

            # ── date ─────────────────────────────────────────────────────
            date = (
                raw_record.get("publicationDate")
                or str(raw_record.get("year", ""))
                or None
            )

            # ── venue / categories ────────────────────────────────────────
            raw_venue = (
                raw_record.get("venue")
                or raw_record.get("publicationVenue")
            )
            if isinstance(raw_venue, dict):
                venue = raw_venue.get("name") or ""
            else:
                venue = raw_venue or ""

            raw_fields = raw_record.get("fieldsOfStudy") or []
            categories = []
            for f in raw_fields:
                if isinstance(f, dict):
                    categories.append(f.get("category", ""))
                elif isinstance(f, str):
                    categories.append(f)
            categories = [c for c in categories if c]

            # ── additional S2ORC-specific fields ──────────────────────────
            citation_count  = raw_record.get("citationCount", 0)
            reference_count = raw_record.get("referenceCount", 0)
            is_open_access  = raw_record.get("isOpenAccess", False)
            open_pdf        = (
                raw_record.get("openAccessPdf") or {}
            ).get("url", "")
            s2_paper_id     = raw_record.get("paperId", "")
            doi             = external_ids.get("DOI", "")

        except NormalizationError:
            raise
        except Exception as e:
            raise NormalizationError(
                f"Failed to normalize S2ORC record: {e}\nRecord: {raw_record}"
            ) from e

        return {
            "paper_id":        str(paper_id),
            "source":          "s2orc",
            "s2_paper_id":     s2_paper_id,
            "title":           title,
            "abstract":        abstract,
            "authors":         authors,
            "date":            date,
            "venue":           venue,
            "categories":      categories,
            "citation_count":  citation_count,
            "reference_count": reference_count,
            "is_open_access":  is_open_access,
            "open_pdf":        open_pdf,
            "doi":             doi,
            "ingested_at":     datetime.now(timezone.utc).isoformat(),
        }

    # ── extract_citation_edges ────────────────────────────────────────────────

    def extract_citation_edges(self, raw_record: dict) -> list[dict]:
        """
        Extract citation edges from a single RAW S2ORC record.

        Must be called on raw records BEFORE normalization — the references
        and citations fields are not carried through to normalized records.

        Returns list of dicts (not tuples) so they can be written to HDFS
        as JSONL via HDFSClient.write_json().

        Parameters
        ----------
        raw_record : dict
            A single raw record from fetch() containing references/citations.

        Returns
        -------
        list[dict]
            Edge dicts with keys: citing_id, cited_id, ingested_at.
        """
        external_ids = raw_record.get("externalIds") or {}
        arxiv_id     = (
            external_ids.get("ArXiv") or external_ids.get("arxiv")
        )
        citing_id    = (
            arxiv_id
            or raw_record.get("corpusid")
            or raw_record.get("corpusId")
            or raw_record.get("paperId")
        )
        if not citing_id:
            return []

        citing_id = str(citing_id).split("v")[0]
        now       = datetime.now(timezone.utc).isoformat()
        edges: list[dict] = []

        for field in ["references", "citations"]:
            refs = raw_record.get(field)
            if not isinstance(refs, list):
                continue
            for ref in refs:
                # refs can be IDs (str/int) or dicts with a paperId/corpusId
                if isinstance(ref, dict):
                    cited_id = (
                        ref.get("paperId")
                        or ref.get("corpusId")
                        or ref.get("corpusid")
                    )
                else:
                    cited_id = ref

                if cited_id and str(cited_id) != citing_id:
                    edges.append({
                        "citing_id":   citing_id,
                        "cited_id":    str(cited_id),
                        "ingested_at": now,
                    })
            if edges:
                # Found edges in this field — no need to check the other
                break

        return edges

    # ── save ─────────────────────────────────────────────────────────────────

    def save(
        self,
        records: list[dict],
        output_path: str = None,
        partition_date: str = None,
        edges: list[dict] = None,
        category: str = "general",
    ) -> None:
        """
        Write normalized paper records and citation edges to HDFS.

        Writes to:
            raw/s2orc/{category}/{date}/*.jsonl   <- paper metadata
            raw/s2orc/edges/{date}/*.jsonl        <- citation edges

        Uses HDFSClient.write_json() so data lands in HDFS where Spark
        can read it — NOT on the local filesystem.

        Parameters
        ----------
        records        : normalized paper records to persist
        output_path    : ignored (kept for base class interface compatibility)
        partition_date : ignored — HDFSClient uses current UTC date
        edges          : citation edge dicts extracted before normalization
        category       : arXiv category label for HDFS path organisation
        """
        try:
            if records:
                self.hdfs.write_json(
                    records, source="s2orc", category=category
                )
                logger.info(
                    "Wrote %d S2ORC paper records to HDFS (category=%s)",
                    len(records), category,
                )

            if edges:
                self.hdfs.write_json(
                    edges, source="s2orc", category="edges"
                )
                logger.info(
                    "Wrote %d citation edges to HDFS", len(edges)
                )

        except Exception as e:
            raise SaveError(
                f"Failed to save S2ORC records to HDFS: {e}"
            ) from e

    # ── run (override) ────────────────────────────────────────────────────────

    def run(
        self,
        query: str,
        output_path: str = None,
        partition_date: str = None,
        category: str = "general",
        **kwargs,
    ) -> list[dict]:
        """
        Override base class run() to extract citation edges from RAW
        records before normalization strips the references/citations fields.

        Flow:
            connect -> fetch -> extract_edges(raw) -> normalize -> save

        Parameters
        ----------
        query          : shard path (corpus) or date range / category (API)
        output_path    : ignored — writes to HDFS via HDFSClient
        partition_date : ignored — HDFSClient uses current UTC date
        category       : arXiv category label for HDFS path organisation
        **kwargs       : forwarded to fetch() (batch_size, offset, fields)

        Returns
        -------
        list[dict]
            The normalized paper records that were saved.
        """
        self.connect()

        raw_records = self.fetch(query, **kwargs)
        logger.info("Fetched %d raw S2ORC records", len(raw_records))

        # ── Extract edges from RAW records BEFORE normalization ───────────
        # Critical override: base class normalizes first which strips
        # references/citations fields. Extract here while they still exist.
        all_edges: list[dict] = []
        for raw in raw_records:
            all_edges.extend(self.extract_citation_edges(raw))
        logger.info("Extracted %d citation edges", len(all_edges))

        # ── Normalize ─────────────────────────────────────────────────────
        normalized: list[dict] = []
        for raw in raw_records:
            try:
                normalized.append(self.normalize(raw))
            except Exception as exc:
                logger.warning(
                    "Skipping record due to normalization error: %s", exc
                )

        logger.info("Normalized %d records", len(normalized))

        # ── Save both to HDFS ─────────────────────────────────────────────
        self.save(
            normalized,
            output_path=output_path,
            partition_date=partition_date,
            edges=all_edges,
            category=category,
        )

        return normalized


# ── Convenience function for pipeline runner compatibility ────────────────────

def enrich_papers(
    arxiv_ids: list[str],
    category: str = "general",
    hdfs_client: HDFSClient = None,
    api_key: str = None,
) -> tuple[int, int]:
    """
    Convenience wrapper used by ingestion_pipeline.py.

    Creates an S2ORCIngester in API mode and enriches the given arXiv IDs,
    maintaining compatibility with the pipeline runner interface.

    Parameters
    ----------
    arxiv_ids   : list of arXiv IDs to enrich
    category    : arXiv category for HDFS path organisation
    hdfs_client : existing HDFSClient instance (optional)
    api_key     : Semantic Scholar API key (optional)

    Returns
    -------
    (papers_written, edges_written)
    """
    import os
    from tqdm import tqdm
    import time

    S2_BASE_URL    = "https://api.semanticscholar.org/graph/v1"
    S2_API_KEY     = api_key or os.getenv("S2ORC_API_KEY", "")
    REQUEST_DELAY  = float(os.getenv("S2_REQUEST_DELAY", "0.15"))
    BATCH_SIZE     = 100

    hdfs = hdfs_client or HDFSClient()

    headers = {"Accept": "application/json"}
    if S2_API_KEY:
        headers["x-api-key"] = S2_API_KEY

    PAPER_FIELDS = ",".join([
        "title", "abstract", "year", "publicationDate", "authors",
        "externalIds", "citationCount", "referenceCount",
        "fieldsOfStudy", "s2FieldsOfStudy", "references",
        "isOpenAccess", "openAccessPdf",
    ])

    all_papers: list[dict] = []
    all_edges:  list[dict] = []

    s2_ids       = [f"ArXiv:{aid}" for aid in arxiv_ids]
    batches      = [s2_ids[i:i+BATCH_SIZE] for i in range(0, len(s2_ids), BATCH_SIZE)]
    arxiv_batches= [arxiv_ids[i:i+BATCH_SIZE] for i in range(0, len(arxiv_ids), BATCH_SIZE)]

    ingester = S2ORCIngester(config={
        "mode":        "api",
        "api_base_url": S2_BASE_URL,
        "api_key":      S2_API_KEY,
    })

    for batch_idx, (s2_batch, arxiv_batch) in enumerate(
        tqdm(zip(batches, arxiv_batches), total=len(batches),
             desc="S2 batch enrichment")
    ):
        url     = f"{S2_BASE_URL}/paper/batch?fields={PAPER_FIELDS}"
        payload = {"ids": s2_batch}

        for attempt in range(3):
            try:
                resp = requests.post(
                    url,
                    headers={**headers, "Content-Type": "application/json"},
                    json=payload,
                    timeout=60,
                )
                if resp.status_code == 429:
                    time.sleep(2 ** attempt * 5)
                    continue
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.RequestException:
                time.sleep(2 ** attempt)
                data = None

        if not data:
            time.sleep(REQUEST_DELAY)
            continue

        for arxiv_id, paper_data in zip(arxiv_batch, data):
            if not paper_data:
                continue
            # Extract edges from raw response before normalizing
            all_edges.extend(ingester.extract_citation_edges(paper_data))
            try:
                all_papers.append(ingester.normalize(paper_data))
            except Exception:
                pass

        time.sleep(REQUEST_DELAY)

    papers_written = 0
    edges_written  = 0

    if all_papers:
        hdfs.write_json(all_papers, source="s2orc", category=category)
        papers_written = len(all_papers)

    if all_edges:
        hdfs.write_json(all_edges, source="s2orc", category="edges")
        edges_written = len(all_edges)

    return papers_written, edges_written


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Ingest or enrich papers from Semantic Scholar (S2ORC)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modes:

  ENRICH — look up existing arXiv IDs and add S2ORC metadata + citation edges
  (this is what the pipeline runner uses):

    python -m ingestion.s2orc --enrich --ids 2401.12345 2401.67890 --category cs.LG

  INGEST (API bulk) — pull papers directly from S2ORC by date range or field,
  independent of arXiv. Requires S2ORC_API_KEY in .env:

    python -m ingestion.s2orc --ingest --query "2023-01-01:2024-12-31" --category cs.LG
    python -m ingestion.s2orc --ingest --query "Computer Science" --category cs.LG --batch-size 500

  INGEST (corpus shard) — process a locally downloaded S2ORC shard file:

    python -m ingestion.s2orc --ingest --corpus /path/to/shard_42.jsonl.gz --category cs.LG
        """
    )

    # ── Mode ──────────────────────────────────────────────────────────────────
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--enrich",
        action="store_true",
        help="Enrich existing arXiv IDs with S2ORC metadata and citation edges"
    )
    mode_group.add_argument(
        "--ingest",
        action="store_true",
        help="Ingest papers directly from S2ORC (API bulk or corpus shard)"
    )

    # ── Enrich options ────────────────────────────────────────────────────────
    parser.add_argument(
        "--ids",
        nargs="+",
        default=None,
        help="(enrich mode) arXiv IDs to enrich, e.g. --ids 2401.12345 2401.67890"
    )

    # ── Ingest options ────────────────────────────────────────────────────────
    parser.add_argument(
        "--query",
        default=None,
        help=(
            "(ingest API mode) date range 'YYYY-MM-DD:YYYY-MM-DD' "
            "or field of study e.g. 'Computer Science'"
        )
    )
    parser.add_argument(
        "--corpus",
        default=None,
        dest="corpus_path",
        help="(ingest corpus mode) path to a local S2ORC shard file (.jsonl or .jsonl.gz)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        dest="batch_size",
        help="Records per batch (default: 1000)"
    )

    # ── Shared options ────────────────────────────────────────────────────────
    parser.add_argument(
        "--category",
        default="general",
        help="arXiv category label for HDFS path organisation (default: general)"
    )
    parser.add_argument(
        "--api-key",
        default=None,
        dest="api_key",
        help="Semantic Scholar API key (overrides S2ORC_API_KEY env var)"
    )

    args = parser.parse_args()

    api_key = args.api_key or os.getenv("S2ORC_API_KEY", "")
    S2_BASE = "https://api.semanticscholar.org/graph/v1"

    # ── Enrich mode ───────────────────────────────────────────────────────────
    if args.enrich:
        if not args.ids:
            parser.error("--enrich requires --ids")

        print(f"\nRunning S2ORC ENRICHMENT...")
        print(f"  IDs       : {args.ids}")
        print(f"  Category  : {args.category}")
        print(f"  API key   : {'set' if api_key else 'not set (lower rate limits)'}\n")

        papers, edges = enrich_papers(
            arxiv_ids=args.ids,
            category=args.category,
            api_key=api_key,
        )
        print(f"\nDone: {papers} paper records, {edges} citation edges written to HDFS.")

    # ── Ingest mode ───────────────────────────────────────────────────────────
    elif args.ingest:
        if not args.query and not args.corpus_path:
            parser.error("--ingest requires either --query or --corpus")

        # Corpus shard mode
        if args.corpus_path:
            print(f"\nRunning S2ORC CORPUS INGEST...")
            print(f"  Shard     : {args.corpus_path}")
            print(f"  Category  : {args.category}")
            print(f"  Batch size: {args.batch_size}\n")

            ingester = S2ORCIngester(config={
                "mode":        "corpus",
                "corpus_path": args.corpus_path,
            })
            records = ingester.run(
                query=args.corpus_path,
                category=args.category,
                batch_size=args.batch_size,
            )
            print(f"\nDone: {len(records)} records ingested from shard.")

        # API bulk mode
        else:
            if not api_key:
                print(
                    "\nWarning: No S2ORC_API_KEY set. "
                    "Set it in .env for higher rate limits.\n"
                )

            print(f"\nRunning S2ORC API BULK INGEST...")
            print(f"  Query     : {args.query}")
            print(f"  Category  : {args.category}")
            print(f"  Batch size: {args.batch_size}")
            print(f"  API key   : {'set' if api_key else 'not set'}\n")

            ingester = S2ORCIngester(config={
                "mode":        "api",
                "api_base_url": S2_BASE,
                "api_key":      api_key,
            })
            records = ingester.run(
                query=args.query,
                category=args.category,
                batch_size=args.batch_size,
            )
            print(f"\nDone: {len(records)} records ingested from S2ORC API.")