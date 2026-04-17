"""
client.py
---------
Thin wrapper around the official Elasticsearch Python client.

Handles connection setup, authentication, and health checking so that
no other module in the elasticsearch/ package needs to know connection
details. All other modules receive an initialized ESClient instance
rather than constructing their own connections.

Reads connection parameters from environment variables by default so
that credentials are never hardcoded. Supports both local development
(no auth, single node) and production (basic auth or API key, cluster).

Dependencies: elasticsearch-py
"""

import os
import logging
from elasticsearch import Elasticsearch, helpers

logger = logging.getLogger(__name__)


class ESClient:
    """
    Wraps the Elasticsearch Python client with connection management,
    health checking, and index lifecycle helpers.

    Designed to be instantiated once and passed to indexer.py rather
    than recreated per operation. Exposes the raw client at self.client
    for callers that need direct access.

    Parameters
    ----------
    host : str
        Elasticsearch host URL including scheme and port,
        e.g. 'http://localhost:9200'. Reads ES_HOST env var if not
        passed explicitly.
    username : str, optional
        Basic auth username. Reads ES_USERNAME env var if not passed.
        If neither is set, no authentication is used (local dev mode).
    password : str, optional
        Basic auth password. Reads ES_PASSWORD env var if not passed.
    api_key : str, optional
        API key for authentication. If provided, takes precedence over
        basic auth. Reads ES_API_KEY env var if not passed.
    timeout : int
        Request timeout in seconds. Default: 30.
    max_retries : int
        Number of retries on connection failure. Default: 3.
    """

    def __init__(
        self,
        host: str = None,
        username: str = None,
        password: str = None,
        api_key: str = None,
        timeout: int = 30,
        max_retries: int = 3,
    ):
        self.host = host or os.getenv("ES_HOST", "http://localhost:9200")
        self.username = username or os.getenv("ES_USERNAME")
        self.password = password or os.getenv("ES_PASSWORD")
        self.api_key = api_key or os.getenv("ES_API_KEY")
        self.timeout = timeout
        self.max_retries = max_retries
        self.client = None  # will be set after connect() is called

    def connect(self) -> None:
        """
        Initialize the Elasticsearch client and verify connectivity.

        Builds the client with the configured connection parameters and
        calls ping() to confirm the cluster is reachable. Logs the
        cluster name and version on successful connection.

        Raises
        ------
        ConnectionError
            If the cluster is unreachable after max_retries attempts.
        """
        # build the Elasticsearch client with the provided parameters
        client = Elasticsearch(
            hosts=[self.host],
            http_auth=(self.username, self.password) if self.username and self.password else None,
            api_key=self.api_key,
            timeout=self.timeout,
            max_retries=self.max_retries,
        )
        # ping the cluster to verify connectivity
        if not client.ping():
            raise ConnectionError(f"Elasticsearch cluster at {self.host} is unreachable.")
        self.client = client
        logger.info(f"Connected to Elasticsearch cluster at {self.host}")

    def health(self) -> dict:
        """
        Return the cluster health status.

        Calls the Elasticsearch cluster health API and returns the
        response dict. Useful for pre-flight checks before starting
        a bulk indexing job.

        Returns
        -------
        dict
            Cluster health response with keys including status
            ('green', 'yellow', 'red'), number_of_nodes, and
            active_shards.
        """
        # call the cluster health API and return the response and wait 30 seconds for at least yellow status
        response = self.client.cluster.health(wait_for_status="yellow", timeout="30s")
        return response

    def index_exists(self, index_name: str) -> bool:
        """
        Return True if the specified index exists in the cluster.

        Used by indexer.py to decide whether to create a new index
        or update an existing one.

        Parameters
        ----------
        index_name : str
            Name of the Elasticsearch index to check.

        Returns
        -------
        bool
            True if the index exists, False otherwise.
        """
        return self.client.indices.exists(index=index_name)

    def create_index(self, index_name: str, mapping: dict) -> None:
        """
        Create an Elasticsearch index with the provided mapping.

        Only creates the index if it does not already exist. Logs
        a warning and returns silently if the index is already present
        rather than raising an error.

        Parameters
        ----------
        index_name : str
            Name of the index to create.
        mapping : dict
            Full index mapping dict including 'settings' and 'mappings'
            keys as returned by mappings.py.
        """
        if not self.index_exists(index_name):
            self.client.indices.create(index=index_name, body=mapping)
            logger.info(f"Created Elasticsearch index: {index_name}")
        else:
            logger.warning(f"Elasticsearch index {index_name} already exists. Skipping creation.")

    def delete_index(self, index_name: str) -> None:
        """
        Delete an Elasticsearch index.

        Used during full reindexing to drop and recreate an index
        with a fresh mapping. Logs a warning if the index does not exist.

        Parameters
        ----------
        index_name : str
            Name of the index to delete.
        """
        if self.index_exists(index_name):
            self.client.indices.delete(index=index_name)
            logger.info(f"Deleted Elasticsearch index: {index_name}")
        else:
            logger.warning(f"Elasticsearch index {index_name} does not exist. Skipping deletion.")

    def bulk(self, actions: list[dict]) -> tuple[int, list]:
        """
        Execute a bulk indexing operation.

        Wraps the elasticsearch-py helpers.bulk() call with error handling
        and logging. Returns the number of successfully indexed documents
        and a list of any failed actions.

        Parameters
        ----------
        actions : list[dict]
            List of bulk action dicts in the format expected by
            elasticsearch-py's helpers.bulk(), each containing
            '_index', '_id', and '_source' keys.

        Returns
        -------
        tuple[int, list]
            (success_count, failed_actions) where success_count is the
            number of documents successfully indexed and failed_actions
            is a list of dicts describing any failures.
        """
        # use the helpers.bulk() function to execute the bulk request and capture successes and failures
        success_count, failed_actions = helpers.bulk(self.client, actions, stats_only=False, raise_on_error=False)
        return success_count, failed_actions

    def get_document_count(self, index_name: str) -> int:
        """
        Return the number of documents currently in an index.

        Useful for validating that indexing completed successfully
        by comparing the Elasticsearch count to the Hive record count.

        Parameters
        ----------
        index_name : str
            Name of the index to count documents in.

        Returns
        -------
        int
            Total document count for the index.
        """
        if self.index_exists(index_name):
            count = self.client.count(index=index_name)["count"]
            logger.info(f"Index {index_name} contains {count} documents.")
            return count
        else:
            logger.warning(f"Elasticsearch index {index_name} does not exist. Returning 0.")
            return 0