"""
outputs.py
----------
Abstracted output layer for persisting topic modeling results.

Writes three categories of output produced by the topic modeling pipeline:
  1. Topic assignments  — per-paper topic ID, label, and probability
  2. Topic metadata     — per-topic keyword lists, sizes, and labels
  3. 2D coordinates     — per-paper (x, y) positions for the Landscape Map

Currently writes to local disk as JSONL files. Designed with a clean
interface so the team can swap the storage backend to HDFS or
Elasticsearch after the team meeting without modifying topic_model.py
or run.py.

The storage backend is selected via the `backend` parameter. Adding a
new backend requires only implementing the corresponding _save_*_<backend>
private methods and registering the name in save().

Dependencies: json (stdlib), os (stdlib), pathlib (stdlib)
"""

"""
outputs.py — persists BERTopic results to local disk or HDFS.
"""
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from ingestion.hdfs_client import HDFSClient

logger = logging.getLogger(__name__)


class TopicModelOutputs:
    def __init__(self, output_path: str, backend: str = "local"):
        self.output_path = output_path
        self.backend = backend
        if backend not in ("local", "hdfs"):
            raise ValueError(f"Unknown backend '{backend}'. Use 'local' or 'hdfs'.")

    def save(
        self,
        assignments: list[dict],
        topic_info: list[dict],
        coordinates: list[dict],
        run_id: str = None,
    ) -> dict:
        run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        save_fn = self._save_local if self.backend == "local" else self._save_hdfs
        paths = {
            "assignments_path": save_fn(assignments, f"{run_id}/assignments.jsonl"),
            "topic_info_path":  save_fn(topic_info,  f"{run_id}/topic_info.jsonl"),
            "coordinates_path": save_fn(coordinates, f"{run_id}/coordinates.jsonl"),
        }
        logger.info("BERTopic outputs saved for run_id=%s: %s", run_id, paths)
        return paths

    def _save_local(self, records: list[dict], filename: str) -> str:
        full_path = Path(self.output_path) / filename
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        logger.info("Wrote %d records to %s", len(records), full_path)
        return str(full_path)

    def _save_hdfs(self, records: list[dict], filename: str) -> str:
        hdfs = HDFSClient()
        hdfs_path = f"{self.output_path}/{filename}"
        # HDFSClient.write_json expects source/category routing — write raw instead
        # by using the full path directly via the two-step WebHDFS CREATE.
        return hdfs.write_json(records, source="bertopic", category=filename.split("/")[0])

    def load_assignments(self, run_id: str) -> list[dict]:
        return self._load_local(f"{run_id}/assignments.jsonl")

    def load_topic_info(self, run_id: str) -> list[dict]:
        return self._load_local(f"{run_id}/topic_info.jsonl")

    def _load_local(self, filename: str) -> list[dict]:
        full_path = Path(self.output_path) / filename
        if not full_path.exists():
            raise FileNotFoundError(f"Output file not found: {full_path}")
        records = []
        with open(full_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def list_runs(self) -> list[str]:
        base = Path(self.output_path)
        if not base.exists():
            return []
        runs = sorted(
            [d.name for d in base.iterdir() if d.is_dir()],
            reverse=True
        )
        return runs