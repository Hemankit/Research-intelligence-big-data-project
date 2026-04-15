"""
loader.py — loads and merges paper abstracts from HDFS for BERTopic.

Uses TextCleaner for LaTeX/URL/Unicode cleaning and spaCy (via Tokenizer)
for sentence splitting. Sentence splitting ensures clean sentence boundaries
before embedding — BERTopic produces better topic coherence when abstracts
are properly segmented rather than treated as one continuous string.
"""
import logging
from ingestion.hdfs_client import HDFSClient
from NLP_layer.shared.text_preprocessing import TextCleaner
from NLP_layer.shared.Tokenizer import Tokenizer

logger = logging.getLogger(__name__)


class CorpusLoader:
    def __init__(self, hdfs_client: HDFSClient, min_abstract_length: int = 100):
        self.hdfs    = hdfs_client
        self.min_abstract_length = min_abstract_length
        self.cleaner  = TextCleaner()
        # Load spaCy once — reused across all documents.
        # Disable NER and lemmatizer since we only need sentence splitting.
        self.tokenizer = Tokenizer(
            model_name="en_core_web_sm",
            disable=["ner", "lemmatizer"],
        )
        logger.info("CorpusLoader initialized (spaCy sentence splitter loaded)")

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

        # 1. Clean with TextCleaner (LaTeX, URLs, Unicode, whitespace)
        # 2. Sentence-split with spaCy, then rejoin with spaces so the
        #    abstract is a clean, properly segmented string for embedding.
        #    spaCy's sentence splitter handles abbreviations like "et al."
        #    and "Fig." better than naive punctuation splitting.
        raw_abstracts = [r["abstract"] for r in all_records]
        cleaned       = self.cleaner.clean_batch(raw_abstracts)
        abstracts     = self._sentence_split_batch(cleaned)

        logger.info("CorpusLoader: %d papers ready for embedding", len(paper_ids))
        return paper_ids, abstracts

    def _sentence_split_batch(self, texts: list[str]) -> list[str]:
        """
        Split each abstract into sentences via spaCy then rejoin with a
        single space. This normalizes sentence boundaries and removes any
        stray newlines or irregular spacing left after cleaning.

        Uses spaCy's nlp.pipe() for efficient batch processing.
        """
        logger.info(
            "Sentence-splitting %d abstracts via spaCy...", len(texts)
        )
        result = []
        # process_batch returns list[list[str]] — one sentence list per abstract
        sentence_lists = self.tokenizer.process_batch(texts, batch_size=64)
        for sentences in sentence_lists:
            result.append(" ".join(sentences) if sentences else "")
        logger.info("Sentence splitting complete")
        return result

    def _load_source(self, input_path: str, source: str, date_from: str, date_to: str) -> list[dict]:
        source_path = f"{input_path}/{source}"
        records = []
        categories = self.hdfs.list_directory(source_path)
        for category in categories:
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
                new_priority      = SOURCE_PRIORITY.get(rec.get("source", ""), 99)
                if new_priority < existing_priority:
                    seen[pid] = rec
        logger.info("After dedup: %d unique papers", len(seen))
        return list(seen.values())

    def _filter_abstracts(self, records: list[dict]) -> list[dict]:
        before   = len(records)
        filtered = [
            r for r in records
            if r.get("abstract") and len(r["abstract"].strip()) >= self.min_abstract_length
        ]
        logger.info(
            "Abstract filter: kept %d / %d (removed %d short/empty)",
            len(filtered), before, before - len(filtered)
        )
        return filtered