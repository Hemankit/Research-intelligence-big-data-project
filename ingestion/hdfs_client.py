"""
hdfs_client.py

Thin wrapper around the WebHDFS REST API.
All three ingesters use this to write files to HDFS so that
storage logic is never duplicated across arxiv.py / s2orc.py / openalex.py.

WebHDFS runs on port 9870 by default on the NameNode.
No extra Python library needed.
"""

import json
import logging
import os
from datetime import datetime

import requests

logger = logging.getLogger(__name__)


class HDFSClient:
    """
    Writes JSON-serialisable records to HDFS via the WebHDFS REST API.

    Parameters
    
    host : str
        Hostname or IP of the HDFS NameNode. Reads HDFS_HOST env var if not
        passed explicitly.
    port : int
        WebHDFS port (default 9870).
    user : str
        Hadoop user for WebHDFS operations (default 'hadoop').
    base_path : str
        Root HDFS path under which all pipeline data is stored.
        Default: /user/research-intelligence
    """

    def __init__(
        self,
        host: str = None,
        port: int = 9870,
        user: str = "hadoop",
        base_path: str = "/user/research-intelligence",
    ):
        self.host = host or os.getenv("HDFS_HOST", "localhost")
        self.port = port
        self.user = user
        self.base_path = base_path
        self.base_url = f"http://{self.host}:{self.port}/webhdfs/v1"

    #Internal helpers

    def _url(self, hdfs_path: str) -> str:
        """Build a full WebHDFS URL for a given HDFS path."""
        return f"{self.base_url}{hdfs_path}?user.name={self.user}"

    def _mkdirs(self, hdfs_path: str) -> None:
        """Create a directory (and parents) on HDFS if it does not exist."""
        url = self._url(hdfs_path) + "&op=MKDIRS"
        resp = requests.put(url)
        resp.raise_for_status()

    #Public API

    def write_json(self, records: list[dict], source: str, category: str = "general") -> str:
        """
        Serialize a list of records to a single newline-delimited JSON file
        and write it to HDFS.

        Files are stored under:
            {base_path}/raw/{source}/{category}/{YYYY-MM-DD}/{timestamp}.jsonl

        Parameters
        
        records : list[dict]
            The paper records to persist.
        source : str
            Which API produced these records — 'arxiv', 's2orc', or 'openalex'.
        category : str
            arXiv category or domain label, e.g. 'cs.LG'.

        Returns
        
        str
            The full HDFS path the file was written to.
        """
        if not records:
            logger.warning("write_json called with empty records list — skipping.")
            return ""

        today = datetime.utcnow().strftime("%Y-%m-%d")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        dir_path = f"{self.base_path}/raw/{source}/{category}/{today}"
        file_path = f"{dir_path}/{timestamp}.jsonl"

        # Ensure directory exists
        self._mkdirs(dir_path)

        # Serialize records as newline-delimited JSON
        payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)

        # WebHDFS two-step CREATE: first request gets a redirect to a DataNode
        create_url = self._url(file_path) + "&op=CREATE&overwrite=true"
        resp = requests.put(create_url, allow_redirects=False)

        if resp.status_code == 307:
            # Follow redirect to DataNode and upload data
            datanode_url = resp.headers["Location"]
            upload_resp = requests.put(
                datanode_url,
                data=payload.encode("utf-8"),
                headers={"Content-Type": "application/octet-stream"},
            )
            upload_resp.raise_for_status()
        else:
            resp.raise_for_status()

        logger.info("Wrote %d records to HDFS: %s", len(records), file_path)
        return file_path

    def file_exists(self, hdfs_path: str) -> bool:
        """Return True if a file or directory exists on HDFS."""
        url = self._url(hdfs_path) + "&op=GETFILESTATUS"
        resp = requests.get(url)
        return resp.status_code == 200

    def read_json(self, hdfs_path: str) -> list[dict]:
        """
        Read a newline-delimited JSON file from HDFS and return
        a list of dicts. Useful for testing and validation.
        """
        url = self._url(hdfs_path) + "&op=OPEN"
        resp = requests.get(url)
        resp.raise_for_status()
        return [json.loads(line) for line in resp.text.strip().splitlines() if line]
