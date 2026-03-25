"""
base.py

Defines the abstract base class for all data source ingesters.

All ingester classes (for example, ArxivIngester, S2ORCIngester,
OpenAlexIngester) must inherit from BaseIngester and implement the
source-specific ingestion steps to ensure a consistent interface across
all data sources.
"""

from abc import ABC, abstractmethod


class IngestionError(Exception):
    """Base exception for all ingestion-related failures."""


class SourceConnectionError(IngestionError):
    """Raised when a source cannot be reached or authenticated."""


class FetchError(IngestionError):
    """Raised when raw records cannot be fetched from the source."""


class NormalizationError(IngestionError):
    """Raised when a raw record cannot be normalized."""


class SaveError(IngestionError):
    """Raised when normalized records cannot be saved."""


class BaseIngester(ABC):
    """
    Abstract base class for source-specific ingesters.

    **Architecture**: This class implements BREADTH-FIRST bulk ingestion
    at scale. Ingesters fetch and store large-scale metadata broadly
    during the initial pipeline stage, without query-based filtering.
    Query-driven retrieval and selective full-text analysis happen later
    during semantic clustering and analysis phases.

    Each subclass represents one data source and must implement the
    source-specific ingestion steps:
        1. connect - validate access to the data source
        2. fetch - bulk retrieve metadata records (not query-driven)
        3. normalize - map to unified schema
        4. save - persist to storage

    The full ingestion flow is shared through the concrete run() method.
    """

    @abstractmethod
    def connect(self) -> None:
        """
        Establish access to the data source and validate readiness.

        This method should perform only the minimum setup required for
        ingestion, such as:
        - validating credentials
        - preparing a client or session
        - confirming the source endpoint is reachable

        Raises
        ------
        SourceConnectionError
            If the source is unreachable or authentication fails.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch(self, query: str, **kwargs) -> list[dict]:
        """
        Fetch a batch of raw metadata records from the data source for
        large-scale bulk ingestion.

        This method performs BREADTH-FIRST metadata ingestion at scale,
        not query-driven searches. The system ingests all available metadata
        broadly during this phase. Query-driven retrieval and selective
        full-text analysis happen later during semantic clustering and
        analysis stages, after papers are indexed.

        Parameters
        ----------
        query : str
            Batch identifier for bulk ingestion:
            - Category/field codes (e.g., 'cs.LG' for arXiv)
            - Date ranges (e.g., '2020-01-01:2023-12-31')
            - Shard file paths for local corpus processing
            NOT for user keyword searches.
        **kwargs : dict
            Additional source-specific parameters such as date ranges,
            page size, cursors, or field filters.

        Returns
        -------
        list[dict]
            A list of raw metadata records exactly as returned by the source,
            before normalization. Full-text content is excluded by default.

        Raises
        ------
        FetchError
            If the source request fails or returns unusable data.
        """
        raise NotImplementedError

    @abstractmethod
    def normalize(self, raw_record: dict) -> dict:
        """
        Normalize one raw record into the project's unified paper schema.

        Parameters
        ----------
        raw_record : dict
            A single raw record returned by fetch().

        Returns
        -------
        dict
            A normalized record conforming to the shared schema.

        Raises
        ------
        NormalizationError
            If the raw record is missing required fields or cannot be
            transformed into the shared schema.
        """
        raise NotImplementedError

    @abstractmethod
    def save(self, records: list[dict], output_path: str) -> None:
        """
        Persist normalized records to storage.

        Parameters
        ----------
        records : list[dict]
            A list of normalized records.
        output_path : str
            Destination path for the saved output.

        Raises
        ------
        SaveError
            If records cannot be written to the target location.
        """
        raise NotImplementedError

    def run(self, query: str, output_path: str, **kwargs) -> list[dict]:
        """
        Execute the full ingestion cycle for a single source:

            connect -> fetch -> normalize -> save

        Parameters
        ----------
        query : str
            The search query or category filter to ingest.
        output_path : str
            Destination path for the saved output.
        **kwargs : dict
            Additional source-specific parameters forwarded to fetch().

        Returns
        -------
        list[dict]
            The list of normalized records that were saved.
        """
        self.connect()
        raw_records = self.fetch(query, **kwargs)
        normalized_records = [self.normalize(record) for record in raw_records]
        self.save(normalized_records, output_path)
        return normalized_records