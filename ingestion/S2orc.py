"""
s2orc.py

Ingester for the Semantic Scholar Open Research Corpus (S2ORC).
Responsible for loading and processing the S2ORC dataset, extracting
paper metadata, full-text content where available, and citation edge lists.

Fixes applied over teammate's version:
  1. paper_id now uses arXiv ID (from externalIds.ArXiv) so it matches
     the arXiv corpus and Spark joins work correctly. Falls back to
     S2ORC corpusid/paperId only if no arXiv ID is available.
  2. Citation edges extracted from RAW records BEFORE normalization.
     Base class run() normalizes first which strips references/citations.
     We override run() to extract edges while raw fields still exist.
  3. save() writes to HDFS via HDFSClient instead of local filesystem.
     All pipeline data must be in HDFS for Spark to read it.
  4. _fetch_from_api() uses correct bulk search parameters:
     - 'query' keyword (not 'fieldsOfStudy' which causes 400)
     - 'year' for date ranges (not 'publicationDateOrYear')
     - strips 'references'/'citations' which bulk endpoint doesn't support
     - uses cursor token pagination (not numeric offset)

Preserved from teammate's version:
  - Auto-loads S2ORC_API_KEY from .env via load_dotenv()
  - Sensible __init__ defaults (mode='api', api_base_url set automatically)
  - enable_full_text_download config flag and _fetch_full_paper_content()
  - full_text field carried through normalize()
"""

import gzip
import json
import logging
import os
import requests
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from ingestion.base_ingestor2 import (
    BaseIngester,
    SourceConnectionError,
    FetchError,
    NormalizationError,
    SaveError,
)
from ingestion.hdfs_client import HDFSClient

# Load environment variables from .env
load_dotenv()

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

    def __init__(self, config: dict = None):
        """
        Parameters
        ----------
        config : dict, optional
            Configuration for this ingester.

            Expected keys:
            - mode                    : 'corpus' or 'api' (default: 'api')
            - corpus_path             : (corpus mode) path to local shard files
            - api_base_url            : (api mode) defaults to Semantic Scholar API
            - api_key                 : optional, auto-loaded from S2ORC_API_KEY env var
            - enable_full_text_download: bool (default False) — when True, fetches
                                        full paper content for selective analysis
            - hdfs_host / hdfs_port / hdfs_user / hdfs_base_path: HDFS connection config
        """
        self.config = config or {}

        # Sensible defaults so the ingester works with minimal config
        if "mode" not in self.config:
            self.config["mode"] = "api"
        if "api_base_url" not in self.config:
            self.config["api_base_url"] = "https://api.semanticscholar.org/graph/v1"
        if "enable_full_text_download" not in self.config:
            self.config["enable_full_text_download"] = False

        # Auto-load API key from .env if not explicitly provided
        if not self.config.get("api_key"):
            self.config["api_key"] = os.getenv("S2ORC_API_KEY", "")

        # Build request headers
        self.api_headers = {}
        if self.config.get("api_key"):
            self.api_headers["x-api-key"] = self.config["api_key"]

        # Initialise HDFSClient — used by save() to write to HDFS
        self.hdfs = HDFSClient(
            host      = self.config.get("hdfs_host"),
            port      = int(self.config.get("hdfs_port", 9870)),
            user      = self.config.get("hdfs_user", "hadoop"),
            base_path = self.config.get(
                "hdfs_base_path", "/user/research-intelligence"
            ),
        )

    # connect 

    def connect(self) -> None:
        """Validate access to S2ORC (local corpus or API)."""
        try:
            mode = self.config.get("mode")

            if mode == "corpus":
                corpus_path = self.config.get("corpus_path")
                if not corpus_path:
                    raise SourceConnectionError("Missing 'corpus_path' in config.")
                if not os.path.exists(corpus_path):
                    raise SourceConnectionError(
                        f"S2ORC corpus path not found: {corpus_path}"
                    )

            elif mode == "api":
                base_url = self.config.get("api_base_url")
                if not base_url:
                    raise SourceConnectionError("Missing 'api_base_url' in config.")
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

    # fetch 

    def fetch(self, query: str, **kwargs) -> list[dict]:
        """
        Fetch a batch of raw paper metadata from S2ORC for bulk ingestion.

        Parameters
        ----------
        query : str
            - Corpus mode : path to a shard file
            - API mode    : keyword search (e.g. 'machine learning') or
                            date range (e.g. '2023-01-01:2024-12-31')
        **kwargs :
            - batch_size (int) : records per batch (default 1000)
            - offset (int)     : pagination cursor token (API mode)
            - fields (list)    : metadata fields to retrieve

        Returns
        -------
        list[dict]
            Raw paper metadata records as returned by the source.
        """
        batch_size  = kwargs.get("batch_size", 1000)
        offset      = kwargs.get("offset", 0)
        max_records = kwargs.get("max_records", None)
        fields      = kwargs.get("fields", [
            "paperId", "corpusId", "title", "abstract", "authors",
            "year", "publicationDate", "venue", "publicationVenue",
            "fieldsOfStudy", "s2FieldsOfStudy", "citationCount",
            "referenceCount", "externalIds",
            "isOpenAccess", "openAccessPdf",
            # Note: 'references' and 'citations' are NOT supported on the
            # bulk search endpoint — stripped automatically in _fetch_from_api
        ])

        mode = self.config.get("mode")

        if mode == "corpus":
            return self._fetch_from_shard(query, batch_size)
        elif mode == "api":
            return self._fetch_from_api(
                query, batch_size, offset, fields, max_records=max_records
            )
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
        max_records: int = None,
    ) -> list[dict]:
        """
        Bulk metadata pull from Semantic Scholar API with full pagination.

        Uses /paper/search/bulk with cursor-based pagination — each response
        includes a 'token' used to fetch the next page. Continues paginating
        until all results are fetched or max_records is reached.

        Parameters
        ----------
        query       : keyword search or date range ('2023:2024')
        batch_size  : records per page (max 1000 per S2ORC limit)
        offset      : starting cursor token (usually None for first request)
        fields      : metadata fields to return
        max_records : total record cap — None means fetch everything

        Note: 'references' and 'citations' are NOT supported on the bulk
        endpoint — stripped automatically to avoid 400 errors.
        """
        import time as _time

        base_url = self.config.get("api_base_url")
        endpoint = f"{base_url}/paper/search/bulk"

        # Strip fields not supported by the bulk endpoint
        unsupported = {"references", "citations", "fieldsOfStudy"}
        safe_fields = [f for f in fields if f not in unsupported]

        base_params = {
            "fields": ",".join(safe_fields),
            "limit":  min(batch_size, 1000),
        }

        # Date range format: '2023-01-01:2024-12-31' or '2020:2024'
        if ":" in query and query[:4].isdigit():
            start, end = query.split(":", 1)
            start_year = start.split("-")[0]
            end_year   = end.split("-")[0]
            base_params["year"]  = f"{start_year}-{end_year}"
            base_params["query"] = "computer science"
        else:
            base_params["query"] = query

        all_records: list[dict] = []
        token = offset if offset else None
        page  = 0

        while True:
            params = dict(base_params)
            if token:
                params["token"] = token

            try:
                response = requests.get(
                    endpoint,
                    params=params,
                    headers=self.api_headers,
                    timeout=30,
                )
                if response.status_code == 429:
                    wait = 30
                    logger.warning(
                        "S2ORC rate limited — waiting %ds before retry", wait
                    )
                    _time.sleep(wait)
                    continue
                response.raise_for_status()
            except requests.RequestException as e:
                raise FetchError(f"S2ORC API fetch failed: {e}") from e

            data         = response.json()
            page_records = data.get("data", [])
            token        = data.get("token")
            total        = data.get("total", "unknown")
            page        += 1

            all_records.extend(page_records)

            logger.info(
                "S2ORC bulk page %d — fetched %d | running total: %d / %s",
                page, len(page_records), len(all_records), total
            )

            # Optionally fetch full paper content if enabled
            if self.config.get("enable_full_text_download", False):
                for record in page_records:
                    pid = record.get("paperId")
                    if pid:
                        full_text = self._fetch_full_paper_content(pid)
                        if full_text:
                            record["full_text"] = full_text

            # Stop if we have hit the max_records cap
            if max_records and len(all_records) >= max_records:
                all_records = all_records[:max_records]
                logger.info(
                    "Reached max_records cap of %d — stopping pagination",
                    max_records
                )
                break

            # Stop if no more pages
            if not token or not page_records:
                logger.info(
                    "S2ORC pagination complete — %d total records fetched",
                    len(all_records)
                )
                break

            # Polite delay between pages
            _time.sleep(1.0)

        return all_records

    def _fetch_full_paper_content(self, paper_id: str) -> str:
        """
        Fetch full paper content for a specific paper (optional capability).

        Used only when enable_full_text_download=True. Not part of the
        default bulk metadata pipeline — reserved for selective full-text
        analysis of high-value papers identified after clustering.

        Parameters
        ----------
        paper_id : str
            The Semantic Scholar paper ID.

        Returns
        -------
        str
            Full text content or PDF URL if available, empty string otherwise.
        """
        base_url = self.config.get("api_base_url")
        endpoint = f"{base_url}/paper/{paper_id}"

        params = {"fields": "title,abstract,isOpenAccess,openAccessPdf"}

        try:
            response = requests.get(
                endpoint,
                params=params,
                headers=self.api_headers,
                timeout=30,
            )
            response.raise_for_status()

            paper_data      = response.json()
            open_access_pdf = paper_data.get("openAccessPdf")

            if open_access_pdf and open_access_pdf.get("url"):
                return f"PDF available at: {open_access_pdf['url']}"

            # Fallback to abstract if no full text available
            return paper_data.get("abstract", "")

        except requests.RequestException as e:
            logger.warning(
                "Could not fetch full text for paper %s: %s", paper_id, e
            )
            return ""

    # normalize 

    def normalize(self, raw_record: dict) -> dict:
        """
        Map a raw S2ORC record to the unified paper schema.

        Uses arXiv ID from externalIds as paper_id so records join correctly
        with arXiv records in Spark. Falls back to S2ORC's internal corpusId
        or paperId only when no arXiv ID is available.

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
            # ── paper_id: prefer arXiv ID for Spark join compatibility ────
            # Using arXiv ID ensures S2ORC records join with arXiv records
            # on paper_id without any key translation step.
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

            # title 
            title = (raw_record.get("title") or "").strip()

            # abstract 
            abstract = (raw_record.get("abstract") or "").strip()

            # authors 
            raw_authors = raw_record.get("authors") or []
            authors = [
                a["name"]
                for a in raw_authors
                if isinstance(a, dict) and a.get("name")
            ]

            # date
            date = (
                raw_record.get("publicationDate")
                or str(raw_record.get("year", ""))
                or None
            )

            # venue / categories 
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

            # additional S2ORC-specific fields 
            citation_count  = raw_record.get("citationCount", 0)
            reference_count = raw_record.get("referenceCount", 0)
            is_open_access  = raw_record.get("isOpenAccess", False)
            open_pdf        = (
                raw_record.get("openAccessPdf") or {}
            ).get("url", "")
            s2_paper_id     = raw_record.get("paperId", "")
            doi             = external_ids.get("DOI", "")

            # full_text (optional) 
            # Only present if enable_full_text_download=True was set
            full_text = raw_record.get("full_text", "")

        except NormalizationError:
            raise
        except Exception as e:
            raise NormalizationError(
                f"Failed to normalize S2ORC record: {e}\nRecord: {raw_record}"
            ) from e

        normalized = {
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

        # Include full_text only if it was actually downloaded
        if full_text:
            normalized["full_text"] = full_text

        return normalized

    # extract_citation_edges 
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
        citing_id = (
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
                break

        return edges

    # save 

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

    # run (override) 

    def run(
        self,
        query: str,
        output_path: str = None,
        partition_date: str = None,
        category: str = "general",
        max_records: int = None,
        **kwargs,
    ) -> list[dict]:
        """
        Override base class run() to extract citation edges from RAW
        records before normalization strips references/citations fields.

        Flow:
            connect -> fetch -> extract_edges(raw) -> normalize -> save

        Parameters
        ----------
        query          : shard path (corpus) or keyword / date range (API)
        output_path    : ignored — writes to HDFS via HDFSClient
        partition_date : ignored — HDFSClient uses current UTC date
        category       : arXiv category label for HDFS path organisation
        max_records    : total records cap across all pages (None = fetch all)
        **kwargs       : forwarded to fetch() (batch_size, offset, fields)

        Returns
        -------
        list[dict]
            The normalized paper records that were saved.
        """
        self.connect()

        if max_records:
            kwargs["max_records"] = max_records

        raw_records = self.fetch(query, **kwargs)
        logger.info("Fetched %d raw S2ORC records", len(raw_records))

        # Extract edges from RAW records BEFORE normalization
        # Critical: base class normalizes first which strips
        # references/citations fields making edge extraction impossible.
        all_edges: list[dict] = []
        for raw in raw_records:
            all_edges.extend(self.extract_citation_edges(raw))
        logger.info("Extracted %d citation edges", len(all_edges))

        # Normalize 
        normalized: list[dict] = []
        for raw in raw_records:
            try:
                normalized.append(self.normalize(raw))
            except Exception as exc:
                logger.warning(
                    "Skipping record due to normalization error: %s", exc
                )

        logger.info("Normalized %d records", len(normalized))

        # Save both to HDFS 
        self.save(
            normalized,
            output_path=output_path,
            partition_date=partition_date,
            edges=all_edges,
            category=category,
        )

        return normalized


# ID type detection helper 

def _is_arxiv_id(pid: str) -> bool:
    """
    Return True if pid looks like an arXiv ID (e.g. '2603.24594').

    arXiv IDs are in the format YYMM.NNNNN or YYMM.NNNNNN — four digits,
    a dot, then four or five digits. S2ORC corpus IDs are long integers
    with no dot. DOIs contain a slash. We use this to decide whether to
    prefix with 'ArXiv:' or pass the ID directly to the S2ORC batch API.
    """
    import re
    # arXiv format: YYMM.NNNNN[N] — e.g. 2603.24594 or 1706.03762
    return bool(re.match(r"^\d{4}\.\d{4,6}(v\d+)?$", pid.strip()))


# Convenience function for pipeline runner compatibility 

def enrich_papers(
    arxiv_ids: list[str],
    category: str = "general",
    hdfs_client: HDFSClient = None,
    api_key: str = None,
) -> tuple[int, int]:
    """
    Convenience wrapper used by ingestion_pipeline.py.

    Enriches a list of paper IDs with S2ORC metadata and citation edges
    using the Semantic Scholar batch paper endpoint (not bulk search).

    Supports both arXiv IDs (e.g. '2603.24594') and native S2ORC paper
    IDs (e.g. '649def34f8be52c8b66281af98ae884c09aef38a'). The function
    detects which format each ID is in and prefixes arXiv IDs with
    'ArXiv:' automatically. S2ORC IDs are passed through unchanged.

    This means you can enrich papers from both sources in one call:
        enrich_papers(['2603.24594', '649def34f8be52c8b66281af98ae884c09aef38a'])

    Parameters
    ----------
    arxiv_ids   : list of paper IDs — arXiv format OR S2ORC paper IDs
    category    : HDFS folder label for output organisation
    hdfs_client : existing HDFSClient instance (optional)
    api_key     : Semantic Scholar API key (overrides S2ORC_API_KEY env var)

    Returns
    -------
    (papers_written, edges_written)
    """
    import time
    from tqdm import tqdm

    S2_BASE_URL   = "https://api.semanticscholar.org/graph/v1"
    S2_API_KEY    = api_key or os.getenv("S2ORC_API_KEY", "")
    REQUEST_DELAY = float(os.getenv("S2_REQUEST_DELAY", "0.15"))
    BATCH_SIZE    = 100

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

    # Resolve each ID to the correct S2ORC lookup format 
    # arXiv IDs → prefix with 'ArXiv:' (e.g. 'ArXiv:2603.24594')
    # S2ORC IDs → pass through unchanged (e.g. '649def34f8be52c...')
    # This lets us enrich papers from both sources in one pass.
    resolved_ids = []
    for pid in arxiv_ids:
        pid = pid.strip()
        if _is_arxiv_id(pid):
            resolved_ids.append(f"ArXiv:{pid}")
        else:
            # Native S2ORC paper ID — use directly
            resolved_ids.append(pid)

    arxiv_count = sum(1 for pid in arxiv_ids if _is_arxiv_id(pid.strip()))
    s2orc_count = len(arxiv_ids) - arxiv_count
    logger.info(
        "Enriching %d papers (%d arXiv IDs, %d S2ORC IDs)",
        len(arxiv_ids), arxiv_count, s2orc_count
    )

    batches          = [
        resolved_ids[i:i+BATCH_SIZE]
        for i in range(0, len(resolved_ids), BATCH_SIZE)
    ]
    original_batches = [
        arxiv_ids[i:i+BATCH_SIZE]
        for i in range(0, len(arxiv_ids), BATCH_SIZE)
    ]

    ingester = S2ORCIngester(config={
        "mode":         "api",
        "api_base_url": S2_BASE_URL,
        "api_key":      S2_API_KEY,
    })

    for resolved_batch, original_batch in tqdm(
        zip(batches, original_batches),
        total=len(batches),
        desc="S2 batch enrichment",
    ):
        url     = f"{S2_BASE_URL}/paper/batch?fields={PAPER_FIELDS}"
        payload = {"ids": resolved_batch}

        data = None
        for attempt in range(3):
            try:
                resp = requests.post(
                    url,
                    headers={**headers, "Content-Type": "application/json"},
                    json=payload,
                    timeout=60,
                )
                if resp.status_code == 429:
                    wait = 2 ** attempt * 5
                    logger.warning(
                        "S2ORC rate limited — waiting %ds (attempt %d/3)",
                        wait, attempt + 1
                    )
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.RequestException as e:
                logger.warning("Request failed (attempt %d/3): %s", attempt + 1, e)
                time.sleep(2 ** attempt)

        if not data:
            logger.warning(
                "Skipping batch starting with %s — all retries failed",
                original_batch[0] if original_batch else "unknown"
            )
            time.sleep(REQUEST_DELAY)
            continue

        for original_id, paper_data in zip(original_batch, data):
            if not paper_data:
                # S2ORC returned null for this ID — paper not found
                logger.debug("No S2ORC record found for ID: %s", original_id)
                continue
            all_edges.extend(ingester.extract_citation_edges(paper_data))
            try:
                normalized = ingester.normalize(paper_data)
                # If the paper has no arXiv ID, preserve the original ID
                # so it stays consistent with whatever was in HDFS
                if not normalized.get("paper_id") or normalized["paper_id"] == "":
                    normalized["paper_id"] = original_id
                all_papers.append(normalized)
            except Exception as exc:
                logger.warning(
                    "Normalization failed for %s: %s", original_id, exc
                )

        time.sleep(REQUEST_DELAY)

    papers_written = 0
    edges_written  = 0

    if all_papers:
        hdfs.write_json(all_papers, source="s2orc", category=category)
        papers_written = len(all_papers)
        logger.info("Wrote %d enriched paper records to HDFS", papers_written)

    if all_edges:
        hdfs.write_json(all_edges, source="s2orc", category="edges")
        edges_written = len(all_edges)
        logger.info("Wrote %d citation edges to HDFS", edges_written)

    return papers_written, edges_written


# CLI entry point 

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

  ENRICH — look up existing arXiv IDs and add S2ORC metadata + citation edges:
    python -m ingestion.s2orc --enrich --ids 2401.12345 2401.67890 --category cs.LG

  INGEST (API bulk) — pull papers directly from S2ORC by keyword or date range.
  --category is just an HDFS folder label, not an S2ORC filter:
    python -m ingestion.s2orc --ingest --query "machine learning" --category s2orc_bulk
    python -m ingestion.s2orc --ingest --query "2023-01-01:2024-12-31" --category s2orc_bulk

  INGEST (corpus shard) — process a locally downloaded S2ORC shard file:
    python -m ingestion.s2orc --ingest --corpus /path/to/shard_42.jsonl.gz --category s2orc_bulk
        """
    )

    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--enrich", action="store_true",
        help="Enrich existing arXiv IDs with S2ORC metadata and citation edges"
    )
    mode_group.add_argument(
        "--ingest", action="store_true",
        help="Ingest papers directly from S2ORC (API bulk or corpus shard)"
    )

    parser.add_argument(
        "--ids", nargs="+", default=None,
        help=(
            "(enrich mode) paper IDs to enrich — accepts arXiv IDs "
            "(e.g. 2603.24594) or native S2ORC paper IDs. Both types "
            "can be mixed in the same command."
        )
    )
    parser.add_argument(
        "--from-hdfs", default=None, dest="from_hdfs",
        help=(
            "(enrich mode) read paper_ids from an existing HDFS JSONL file "
            "and enrich them. Useful for enriching your S2ORC bulk corpus. "
            "Example: --from-hdfs /user/research-intelligence/raw/s2orc/s2orc_bulk/2026-03-31/file.jsonl"
        )
    )
    parser.add_argument(
        "--query", default=None,
        help="(ingest API mode) keyword or date range e.g. 'machine learning' or '2023:2024'"
    )
    parser.add_argument(
        "--corpus", default=None, dest="corpus_path",
        help="(ingest corpus mode) path to a local S2ORC shard file"
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, dest="batch_size",
        help="Records per API page, max 1000 (default: 1000)"
    )
    parser.add_argument(
        "--max", type=int, default=None, dest="max_records",
        help="Max total records to fetch across all pages (default: fetch all)"
    )
    parser.add_argument(
        "--category", default="general",
        help="HDFS folder label for output organisation (default: general)"
    )
    parser.add_argument(
        "--api-key", default=None, dest="api_key",
        help="Semantic Scholar API key (overrides S2ORC_API_KEY env var)"
    )

    args    = parser.parse_args()
    api_key = args.api_key or os.getenv("S2ORC_API_KEY", "")
    S2_BASE = "https://api.semanticscholar.org/graph/v1"

    if args.enrich:
        if not args.ids and not args.from_hdfs:
            parser.error("--enrich requires either --ids or --from-hdfs")

        paper_ids = list(args.ids) if args.ids else []

        # Load IDs from an HDFS file if --from-hdfs was given
        if args.from_hdfs:
            from ingestion.hdfs_client import HDFSClient as _HDFSClient
            _hdfs = _HDFSClient()
            try:
                hdfs_records = _hdfs.read_json(args.from_hdfs)
                hdfs_ids = [
                    r.get("paper_id") or r.get("paperId") or r.get("corpusid")
                    for r in hdfs_records
                    if r.get("paper_id") or r.get("paperId") or r.get("corpusid")
                ]
                paper_ids.extend(hdfs_ids)
                print(f"  Loaded {len(hdfs_ids)} IDs from HDFS file")
            except Exception as e:
                parser.error(f"Could not read HDFS file {args.from_hdfs}: {e}")

        paper_ids = list(dict.fromkeys(paper_ids))
        arxiv_count = sum(1 for pid in paper_ids if _is_arxiv_id(pid.strip()))
        s2orc_count = len(paper_ids) - arxiv_count

        print(f"\nRunning S2ORC ENRICHMENT...")
        print(f"  Total IDs  : {len(paper_ids)}")
        print(f"  arXiv IDs  : {arxiv_count}")
        print(f"  S2ORC IDs  : {s2orc_count}")
        print(f"  Category   : {args.category}")
        print(f"  API key    : {'set' if api_key else 'not set (lower rate limits)'}\n")

        papers, edges = enrich_papers(
            arxiv_ids=paper_ids,
            category=args.category,
            api_key=api_key,
        )
        print(f"\nDone: {papers} paper records, {edges} citation edges written to HDFS.")

    elif args.ingest:
        if not args.query and not args.corpus_path:
            parser.error("--ingest requires either --query or --corpus")

        if args.corpus_path:
            print(f"\nRunning S2ORC CORPUS INGEST...")
            print(f"  Shard    : {args.corpus_path}")
            print(f"  Category : {args.category}\n")

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

        else:
            if not api_key:
                print(
                    "\nWarning: No S2ORC_API_KEY set. "
                    "Set it in .env for higher rate limits.\n"
                )

            print(f"\nRunning S2ORC API BULK INGEST...")
            print(f"  Query    : {args.query}")
            print(f"  Category : {args.category} (HDFS folder label only)")
            print(f"  Batch    : {args.batch_size}")
            print(f"  API key  : {'set' if api_key else 'not set'}\n")

            ingester = S2ORCIngester(config={
                "mode":        "api",
                "api_base_url": S2_BASE,
                "api_key":      api_key,
            })
            records = ingester.run(
                query=args.query,
                category=args.category,
                batch_size=args.batch_size,
                max_records=args.max_records,
            )
            print(f"\nDone: {len(records)} records ingested from S2ORC API.")