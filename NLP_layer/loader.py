"""
loader.py
---------
Loads and merges paper abstracts from HDFS across all three ingested
sources (arXiv, S2ORC, OpenAlex) into a single flat corpus for BERTopic.

Unlike the NER pipeline which processes documents independently, BERTopic
requires the full corpus to be available as a single list before fitting
begins. This module is responsible for assembling that list efficiently.

Also handles deduplication of abstracts that may appear across multiple
sources (e.g., a paper ingested from both arXiv and S2ORC), and filters
out records with missing or very short abstracts that would degrade
topic model quality.

Dependencies: ingestion/hdfs_client.py, shared/text_cleaner.py
"""
import logging
from ingestion.hdfs_client import HDFSClient
from NLP_layer.shared.text_preprocessing import TextCleaner

logger = logging.getLogger(__name__)


class CorpusLoader:
    def __init__(self, hdfs_client: HDFSClient, min_abstract_length: int = 100):
        self.hdfs = hdfs_client
        self.min_abstract_length = min_abstract_length
        self.cleaner = TextCleaner()

    def load(
        self,
        input_path: str,
        sources: list[str] = None,
        date_from: str = None,
        date_to: str = None,
    ) -> tuple[list[str], list[str]]:
        sources = sources or ["arxiv", "s2orc", "openalex"]
        all_records = []
        for source in sources:
            records = self._load_source(input_path, source, date_from, date_to)
            all_records.extend(records)
        all_records = self._deduplicate(all_records)
        all_records = self._filter_abstracts(all_records)
        paper_ids = [r["paper_id"] for r in all_records]
        abstracts = [self.cleaner.clean(r["abstract"]) for r in all_records]
        logger.info("CorpusLoader: %d papers ready for embedding", len(paper_ids))
        return paper_ids, abstracts

    def _load_source(self, input_path: str, source: str, date_from: str, date_to: str) -> list[dict]:
        source_path = f"{input_path}/{source}"
        records = []
        categories = self.hdfs.list_directory(source_path)
        for category in categories:
            # Skip edge/fulltext partitions — metadata only for BERTopic
            if category in ("edges", "s2orc_fulltext"):
                continue
            cat_path = f"{source_path}/{category}"
            dates = self.hdfs.list_directory(cat_path)
            for date in dates:
                if date_from and date < date_from:
                    continue
                if date_to and date > date_to:
                    continue
                date_path = f"{cat_path}/{date}"
                files = self.hdfs.list_directory(date_path)
                for fname in files:
                    if not fname.endswith(".jsonl"):
                        continue
                    try:
                        batch = self.hdfs.read_json(f"{date_path}/{fname}")
                        records.extend(batch)
                    except Exception as e:
                        logger.warning("Could not read %s/%s: %s", date_path, fname, e)
        logger.info("Loaded %d raw records from source=%s", len(records), source)
        return records

    def _deduplicate(self, records: list[dict]) -> list[dict]:
        SOURCE_PRIORITY = {"arxiv": 0, "s2orc": 1, "openalex": 2}
        seen: dict[str, dict] = {}
        for rec in records:
            pid = rec.get("paper_id")
            if not pid:
                continue
            if pid not in seen:
                seen[pid] = rec
            else:
                existing_priority = SOURCE_PRIORITY.get(seen[pid].get("source", ""), 99)
                new_priority = SOURCE_PRIORITY.get(rec.get("source", ""), 99)
                if new_priority < existing_priority:
                    seen[pid] = rec
        logger.info("After dedup: %d unique papers", len(seen))
        return list(seen.values())

    def _filter_abstracts(self, records: list[dict]) -> list[dict]:
        before = len(records)
        filtered = [
            r for r in records
            if r.get("abstract") and len(r["abstract"].strip()) >= self.min_abstract_length
        ]
        logger.info(
            "Abstract filter: kept %d / %d (removed %d short/empty)",
            len(filtered), before, before - len(filtered)
        )
        return filtered