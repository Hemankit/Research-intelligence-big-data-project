"""
extractor.py
------------
Per-document entity extraction logic for the NER pipeline.

Takes a single cleaned abstract and a loaded NERModel instance, runs
inference, and returns structured entity mentions categorized as
METHOD, DATASET, or TASK.

This module is the unit of parallelization — extractor.py's extract()
function is the callable passed to ParallelExecutor in run.py. Each
call is fully independent (no shared mutable state), making it safe
to run across multiple CPU cores via joblib.

Post-processing responsibilities handled here:
  - BIO tag decoding: converts B-/I- token sequences into contiguous spans
  - Span aggregation: merges subword tokens back into full entity strings
  - Score thresholding: filters low-confidence predictions
  - Entity type normalization: maps raw label strings to METHOD/DATASET/TASK

Dependencies: shared/text_cleaner.py, ner_pipeline/model.py

Label mapping for dslim/bert-base-NER:
    ORG  → METHOD  (model names, frameworks: "BERT", "PyTorch", "ResNet")
    MISC → TASK    (research tasks: "machine translation", "object detection")
    PER  → skip    (person names — not useful for research paper NER)
    LOC  → skip    (locations — not useful for research paper NER)

If a fine-tuned scientific NER model with native METHOD/DATASET/TASK labels
becomes available, remove LABEL_MAP and the _map_entity_type() call —
the rest of the pipeline will work unchanged.
"""
"""
extractor.py — per-document entity extraction with BIO tag decoding.

Label mapping for dslim/bert-base-NER:
    ORG  → METHOD  (model names, frameworks: "BERT", "PyTorch", "ResNet")
    MISC → TASK    (research tasks: "machine translation", "object detection")
    PER  → skip
    LOC  → skip
"""
from .ner_model import NERModel

LABEL_MAP = {
    "ORG":  "METHOD",
    "MISC": "TASK",
    "PER":  None,
    "LOC":  None,
}


class EntityExtractor:
    def __init__(self, model: NERModel, confidence_threshold: float = 0.80):
        self.model = model
        self.confidence_threshold = confidence_threshold

    def extract(self, paper: dict) -> dict:
        """Single-paper extraction — used when n_jobs > 1."""
        paper_id = paper["paper_id"]
        source   = paper.get("source", "")
        abstract = paper.get("abstract", "")
        token_predictions = self.model.predict(abstract)
        return self._build_result(paper_id, source, token_predictions)

    def extract_batch(self, papers: list[dict], batch_size: int = 32) -> list[dict]:
        """
        Batch extraction — much faster than extract() in a loop on CPU.
        Processes all abstracts in a single batched forward pass per batch_size.
        Used when n_jobs=1 to avoid joblib multiprocessing overhead.
        """
        abstracts = [p.get("abstract", "") for p in papers]
        all_token_preds = self.model.predict_batch(abstracts, batch_size=batch_size)
        results = []
        for paper, token_predictions in zip(papers, all_token_preds):
            results.append(
                self._build_result(
                    paper["paper_id"],
                    paper.get("source", ""),
                    token_predictions,
                )
            )
        return results

    def _build_result(self, paper_id: str, source: str, token_predictions: list[dict]) -> dict:
        spans    = self._decode_bio_tags(token_predictions)
        spans    = self._filter_by_confidence(spans)
        entities = [
            self._build_entity_record(span, paper_id)
            for span in spans
            if span.get("mapped_type") is not None
        ]
        entity_counts = {"METHOD": 0, "DATASET": 0, "TASK": 0}
        for entity in entities:
            entity_counts[entity["entity_type"]] = (
                entity_counts.get(entity["entity_type"], 0) + 1
            )
        return {
            "paper_id":      paper_id,
            "source":        source,
            "entities":      entities,
            "entity_counts": entity_counts,
        }

    def _map_entity_type(self, raw_label: str) -> str | None:
        return LABEL_MAP.get(raw_label)

    def _decode_bio_tags(self, token_predictions: list[dict]) -> list[dict]:
        spans = []
        current_span = None
        for token_pred in token_predictions:
            label = self.model.label_map.get(token_pred["label_id"], "O")
            if label == "O":
                if current_span:
                    spans.append(current_span)
                    current_span = None
            elif label.startswith("B-"):
                if current_span:
                    spans.append(current_span)
                raw_type = label[2:]
                current_span = {
                    "tokens": [token_pred],
                    "label":  raw_type,
                    "start":  token_pred["start"],
                    "end":    token_pred["end"],
                }
            elif label.startswith("I-"):
                raw_type = label[2:]
                if current_span and current_span["label"] == raw_type:
                    current_span["tokens"].append(token_pred)
                    current_span["end"] = token_pred["end"]
                else:
                    if current_span:
                        spans.append(current_span)
                    current_span = {
                        "tokens": [token_pred],
                        "label":  raw_type,
                        "start":  token_pred["start"],
                        "end":    token_pred["end"],
                    }
        if current_span:
            spans.append(current_span)

        result = []
        for span in spans:
            mapped = self._map_entity_type(span["label"])
            result.append({
                "text":        self._aggregate_subwords(span["tokens"]),
                "label":       span["label"],
                "mapped_type": mapped,
                "score":       sum(t["score"] for t in span["tokens"]) / len(span["tokens"]),
                "start":       span["start"],
                "end":         span["end"],
            })
        return result

    def _aggregate_subwords(self, tokens: list[dict]) -> str:
        result = ""
        for token in tokens:
            if token["token"].startswith("##"):
                result += token["token"][2:]
            else:
                result += (" " + token["token"]) if result else token["token"]
        return result.strip()

    def _filter_by_confidence(self, spans: list[dict]) -> list[dict]:
        return [s for s in spans if s["score"] >= self.confidence_threshold]

    def _build_entity_record(self, span: dict, paper_id: str) -> dict:
        return {
            "entity_text": span["text"],
            "entity_type": span["mapped_type"],
            "confidence":  span["score"],
            "paper_id":    paper_id,
            "char_start":  span["start"],
            "char_end":    span["end"],
        }