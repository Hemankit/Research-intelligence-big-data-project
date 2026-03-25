"""
base.py

Defines the abstract base class for all data source ingesters.
All ingester classes (ArxivIngester, S2ORCIngester, OpenAlexIngester) must
inherit from BaseIngester and implement its abstract methods to ensure a
consistent interface across all data sources.
"""

from abc import ABC, abstractmethod


class BaseIngester(ABC):
    """
    Abstract base class that enforces a common ingestion interface.
    Each subclass represents a single data source and must implement
    all abstract methods below.
    """

    @abstractmethod
    def connect(self):
        """
        Establish connection or validate access to the data source.
        For API-based sources, this may involve verifying credentials,
        setting request headers, or confirming endpoint availability.
        Should raise a clear error if the source is unreachable.
        """
        pass

    @abstractmethod
    def fetch(self, query: str, **kwargs):
        """
        Fetch a batch of raw records from the data source.

        Parameters
        ----------
        query : str
            A search query or category filter (e.g., 'cs.LG' for arXiv,
            a keyword for Semantic Scholar).
        **kwargs : dict
            Additional source-specific parameters such as date ranges,
            page size, or field filters.

        Returns
        -------
        list[dict]
            A list of raw records as returned by the source API or corpus,
            before any normalization.
        """
        pass

    @abstractmethod
    def normalize(self, raw_record: dict) -> dict:
        """
        Normalize a single raw record into the project's unified paper schema.

        Parameters
        ----------
        raw_record : dict
            A single raw record as returned by fetch().

        Returns
        -------
        dict
            A normalized record conforming to the shared Paper schema
            (paper_id, title, abstract, authors, date, source, categories).
        """
        pass

    @abstractmethod
    def save(self, records: list[dict], output_path: str):
        """
        Persist a list of normalized records to the storage backend.

        Parameters
        ----------
        records : list[dict]
            A list of normalized paper records to save.
        output_path : str
            Destination path on HDFS or local filesystem where the
            output Parquet or JSON file will be written.
        """
        pass

    @abstractmethod
    def run(self, query: str, output_path: str, **kwargs):
        """
        Orchestrates the full ingestion cycle for this source:
        connect → fetch → normalize → save.

        Parameters
        ----------
        query : str
            The search query or category filter to ingest.
        output_path : str
            Destination path for the saved output.
        **kwargs : dict
            Additional parameters forwarded to fetch().
        """
        pass