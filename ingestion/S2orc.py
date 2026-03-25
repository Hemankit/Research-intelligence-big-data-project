"""
s2orc.py
 
Ingester for the Semantic Scholar Open Research Corpus (S2ORC).
Responsible for loading and processing the S2ORC dataset, extracting
paper metadata, full-text content where available, and citation edge lists.

"""
 
from ingestion.base_ingestor2 import (
    BaseIngester,
    SourceConnectionError,
    FetchError,
    NormalizationError,
    SaveError,
)

import requests
import os
import gzip
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import pandas as pd
from datetime import datetime
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
            - mode: 'corpus' or 'api'
            - corpus_path (if corpus mode)
            - api_base_url (if api mode)
            - api_key (optional, for api mode)
        """
        self.config = config

        # Prepare headers once (used later too)
        self.api_headers = {}
        if self.config.get("api_key"):
            self.api_headers["x-api-key"] = self.config["api_key"]

    def connect(self) -> None:
        """
        Validate access to S2ORC (local or API).
        """
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
                    base_url,
                    headers=self.api_headers,
                    timeout=5,
                )

                # This is better than manual status check
                response.raise_for_status()

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

        
 
    def fetch(self, query: str, **kwargs) -> list[dict]:
        """
        Fetch a batch of raw paper metadata from S2ORC for bulk ingestion.
 
        This performs BLIND, large-scale metadata ingestion without filtering
        by user queries. Query-driven retrieval and selective full-text analysis
        happen later in the pipeline during semantic clustering and analysis phases.
 
        **Corpus Mode**: Reads an entire shard file sequentially from the local
        S2ORC bulk download. The system processes all shards to build a complete
        metadata index.
 
        **API Mode**: Performs bulk metadata pulls using date ranges or broad
        field categories (e.g., all Computer Science papers from 2020-2023),
        NOT targeted keyword searches.
 
        Parameters
        ----------
        query : str
            Batch identifier for bulk ingestion:
            - Corpus mode: Path to a shard file (e.g., '/data/s2orc/shard_42.jsonl')
            - API mode: Date range or category ID (e.g., '2020-01-01:2023-12-31')
        **kwargs : dict
            Optional parameters:
            - batch_size (int): Records per batch, default 1000.
            - offset (int): Starting offset for pagination (API mode).
            - fields (list[str]): Metadata fields to retrieve (exclude full-text by default).
 
        Returns
        -------
        list[dict]
            Raw paper metadata records (title, authors, abstract, citations, venue, year).
            Full-text content is NOT included at this stage - it's retrieved selectively later.

        Raises
        ------
        FetchError
            If the shard file cannot be read or the API request fails.
        """

        batch_size = kwargs.get("batch_size", 1000)
        offset     = kwargs.get("offset", 0)
        fields     = kwargs.get("fields", [
            "paperId", "title", "abstract", "authors",
            "year", "venue", "citationCount", "externalIds"
        ])

        mode = self.config.get("mode")

        # ── Corpus mode ────────────────────────────────────────────────────
        if mode == "corpus":
            return self._fetch_from_shard(query, batch_size)

        # ── API mode ────────────────────────────────────────────────────────
        elif mode == "api":
            return self._fetch_from_api(query, batch_size, offset, fields)

        else:
            raise FetchError(f"Unknown mode '{mode}'. Expected 'corpus' or 'api'.")

    # helper methods for fetch() in different modes
    def _fetch_from_shard(self, shard_path: str, batch_size: int) -> list[dict]:
        
        # Reads a shard file (JSONL or gzipped JSONL) and returns a batch of raw records.
        path = Path(shard_path)
        if not path.exists():
            raise FetchError(f"Shard file not found: {shard_path}")

        records = []
        # We read line by line to avoid loading the entire shard into memory, which can be very large.
        try:
            opener = gzip.open(path, "rt") if path.suffix == ".gz" else open(path, "r")
            with opener as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    records.append(json.loads(line))
                    if len(records) >= batch_size:
                        break
        except (OSError, json.JSONDecodeError) as e:
            raise FetchError(f"Failed to read shard '{shard_path}': {e}") from e

        return records

    def _fetch_from_api(
        self, query: str, batch_size: int, offset: int, fields: list[str]
    ) -> list[dict]:
        # Performs a bulk metadata pull from the S2ORC API using date range or category filters.
        base_url = self.config.get("api_base_url")
        endpoint = f"{base_url}/paper/search/bulk"

        params = {
            "fields": ",".join(fields),
            "limit":  batch_size,
            "offset": offset,
        }

        # Parse query: date range vs. field-of-study category
        if ":" in query and query[:4].isdigit():
            start, end = query.split(":", 1)
            params["publicationDateOrYear"] = f"{start}:{end}"
        else:
            params["fieldsOfStudy"] = query

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

        data = response.json().get("data", [])
        return data
        
 
    def normalize(self, raw_record: dict) -> dict:
        """
        Map a raw S2ORC record to the unified paper schema.
 
        Extracts core metadata fields and flattens nested structures.
        Author objects are reduced to name strings. Venue and year fields
        are normalized. The S2ORC paper ID (corpusId) is used as paper_id.
 
        Parameters
        ----------
        raw_record : dict
            A single raw record from S2ORC as returned by fetch().
 
        Returns
        -------
        dict
            Normalized record with keys: paper_id, title, abstract,
            authors, date, source ('s2orc'), categories.

        Raises
        ------
        NormalizationError
            If the raw record is missing required fields or cannot be
            transformed into the shared schema.
        """
        try:
            # ── paper_id ───────────────────────────────────────────────────
            # Corpus mode uses 'corpusid', API mode uses 'paperId'
            paper_id = (
                raw_record.get("corpusid")
                or raw_record.get("paperId")
            )
            if not paper_id:
                raise NormalizationError(
                    f"Record missing both 'corpusid' and 'paperId': {raw_record}"
                )

            # ── title ──────────────────────────────────────────────────────
            title = raw_record.get("title") or ""

            # ── abstract ──────────────────────────────────────────────────
            abstract = raw_record.get("abstract") or ""

            # ── authors ───────────────────────────────────────────────────
            # Corpus mode: [{"name": "Alice"}, ...]
            # API mode:    [{"authorId": "...", "name": "Alice"}, ...]
            raw_authors = raw_record.get("authors") or []
            authors = [
                a["name"] for a in raw_authors if isinstance(a, dict) and a.get("name")
            ]

            # ── date ──────────────────────────────────────────────────────
            # Prefer full date string, fall back to year int
            date = (
                raw_record.get("publicationDate")
                or str(raw_record.get("year", ""))
                or None
            )

            # ── venue / categories ────────────────────────────────────────
            # Venue is a string in API mode, may be a dict in corpus mode
            raw_venue = raw_record.get("venue") or raw_record.get("publicationVenue")
            if isinstance(raw_venue, dict):
                venue = raw_venue.get("name") or ""
            else:
                venue = raw_venue or ""

            # fieldsOfStudy is a list of strings in corpus mode,
            # list of {"category": ..., "source": ...} dicts in API mode
            raw_fields = raw_record.get("fieldsOfStudy") or []
            categories = []
            for f in raw_fields:
                if isinstance(f, dict):
                    categories.append(f.get("category", ""))
                elif isinstance(f, str):
                    categories.append(f)
            categories = [c for c in categories if c]  # drop empty strings

        except NormalizationError:
            raise
        except Exception as e:
            raise NormalizationError(
                f"Failed to normalize S2ORC record: {e}\nRecord: {raw_record}"
            ) from e

        return {
            "paper_id":  str(paper_id),
            "title":     title,
            "abstract":  abstract,
            "authors":   authors,
            "date":      date,
            "venue":     venue,
            "source":    "s2orc",
            "categories": categories,
        }

 
    def extract_citation_edges(self, raw_record: dict) -> list[tuple]:
        """
        Extract citation relationships from a single raw S2ORC record.
 
        Parses the references field of the record and produces a list of
        directed edges representing citations. These edges are stored
        separately from the normalized paper record and later loaded by
        Spark to construct the GraphX citation graph.
 
        Parameters
        ----------
        raw_record : dict
            A single raw record containing a references or citations field.
 
        Returns
        -------
        list[tuple]
            A list of (citing_paper_id, cited_paper_id) tuples representing
            directed citation edges originating from this paper.
        """
        for field in ["references", "citations"]:
            if field in raw_record and isinstance(raw_record[field], list):
                citing_id = raw_record.get("corpusid") or raw_record.get("paperId")
                edges = []
                for cited_id in raw_record[field]:
                    if citing_id and cited_id:
                        edges.append((str(citing_id), str(cited_id)))
                return edges
        return []
 
    def save(self, records: list[dict], output_path: str) -> None:
        """
        Write normalized paper records and citation edges to storage.
 
        Paper records are saved as Parquet or JSON files partitioned by
        source and ingestion date. Citation edges are written as a separate
        edge list file (CSV or Parquet) under output_path/citation_edges/,
        formatted for direct loading by Spark GraphX.
 
        Parameters
        ----------
        records : list[dict]
            Normalized paper records to persist.
        output_path : str
            Base output path. Files will be written under
            output_path/source=s2orc/date=<YYYY-MM-DD>/.

        Raises
        ------
        SaveError
            If records cannot be written to the target location.
        """
        try:
            # Get current date for partitioning
            today = datetime.now().strftime("%Y-%m-%d")
            
            # Create partitioned directory structure
            papers_dir = Path(output_path) / "source=s2orc" / f"date={today}"
            papers_dir.mkdir(parents=True, exist_ok=True)
            
            # Save paper records as Parquet
            df = pd.DataFrame(records)
            papers_file = papers_dir / "batch.parquet"
            df.to_parquet(papers_file, compression='snappy', index=False)

            # Extract and save citation edges
            edges = []
            for record in records:
                edges.extend(self.extract_citation_edges(record))

            if edges:
                edges_dir = Path(output_path) / "citation_edges" / "source=s2orc" / f"date={today}"
                edges_dir.mkdir(parents=True, exist_ok=True)
                
                edges_df = pd.DataFrame(edges, columns=["citing_paper_id", "cited_paper_id"])
                edges_file = edges_dir / "edges.parquet"
                edges_df.to_parquet(edges_file, compression='snappy', index=False)
                
        except Exception as e:
            raise SaveError(f"Failed to save S2ORC records: {e}") from e
                
        
 