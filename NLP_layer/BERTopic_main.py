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

import json
import os
from pathlib import Path


class TopicModelOutputs:
    """
    Persists topic modeling results to a configurable storage backend.

    Separates the three output types (assignments, metadata, coordinates)
    into distinct files for clean downstream consumption by Spark and
    the dashboard API.

    Parameters
    ----------
    output_path : str
        Base path where output files will be written. For local backend,
        this is a filesystem directory. For HDFS backend, an HDFS path.
    backend : str
        Storage backend to use. Currently supported: 'local'.
        Planned: 'hdfs', 'elasticsearch'. Default: 'local'.
    """

    def __init__(self, output_path: str, backend: str = "local"):
        pass

    def save(
        self,
        assignments: list[dict],
        topic_info: list[dict],
        coordinates: list[dict],
        run_id: str = None,
    ) -> dict:
        """
        Write all three output types to the configured storage backend.

        Primary entry point called by run.py after the topic model has
        been fitted. Delegates to backend-specific private methods for
        each output type. Returns the paths where outputs were written
        for logging and downstream reference.

        Parameters
        ----------
        assignments : list[dict]
            Per-paper topic assignments as returned by
            TopicModeler.get_topic_assignments().
        topic_info : list[dict]
            Per-topic metadata as returned by
            TopicModeler.get_topic_info().
        coordinates : list[dict]
            Per-paper 2D coordinates as returned by
            TopicModeler.get_2d_coordinates().
        run_id : str, optional
            Identifier for this pipeline run used to namespace output
            files (e.g., a timestamp or experiment name). If None,
            a timestamp is generated automatically.

        Returns
        -------
        dict
            Paths where each output type was written:
            - assignments_path (str)
            - topic_info_path (str)
            - coordinates_path (str)
        """
        pass

    def _save_local(self, records: list[dict], filename: str) -> str:
        """
        Write a list of records as a JSONL file to the local filesystem.

        Creates parent directories if they do not exist. Writes one
        JSON object per line for compatibility with Spark's JSON reader.

        Parameters
        ----------
        records : list[dict]
            Records to serialize.
        filename : str
            Output filename including extension (e.g., 'assignments.jsonl').

        Returns
        -------
        str
            Full path to the written file.
        """
        pass

    def _save_hdfs(self, records: list[dict], filename: str) -> str:
        """
        Write a list of records as a JSONL file to HDFS.

        Placeholder for HDFS backend implementation. Will use HDFSClient
        once the team confirms HDFS as the output destination for
        topic modeling results.

        Parameters
        ----------
        records : list[dict]
            Records to serialize.
        filename : str
            Output filename.

        Returns
        -------
        str
            Full HDFS path to the written file.

        Raises
        ------
        NotImplementedError
            Until the team confirms the HDFS output path structure.
        """
        raise NotImplementedError(
            "HDFS backend not yet implemented. "
            "Confirm output path structure with the team first."
        )

    def load_assignments(self, run_id: str) -> list[dict]:
        """
        Load previously saved topic assignments for a given run.

        Useful for reloading results without re-fitting the model —
        for example, when regenerating visualizations or re-indexing
        into Elasticsearch.

        Parameters
        ----------
        run_id : str
            The run identifier used when the outputs were saved.

        Returns
        -------
        list[dict]
            Per-paper topic assignment records.
        """
        pass

    def load_topic_info(self, run_id: str) -> list[dict]:
        """
        Load previously saved topic metadata for a given run.

        Parameters
        ----------
        run_id : str
            The run identifier used when the outputs were saved.

        Returns
        -------
        list[dict]
            Per-topic metadata records.
        """
        pass

    def list_runs(self) -> list[str]:
        """
        Return a list of all run IDs for which outputs exist.

        Scans the output directory for subdirectories corresponding to
        past pipeline runs. Useful for comparing results across
        hyperparameter experiments.

        Returns
        -------
        list[str]
            List of run ID strings, sorted by recency (newest first).
        """
        pass