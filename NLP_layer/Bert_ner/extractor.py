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
"""

from cProfile import label
from html import entities
from unittest import result

from nlp.shared.text_cleaner import TextCleaner
from NLP_layer.ner_model import NERModel


class EntityExtractor:
    """
    Extracts named entities from a single academic paper abstract.

    Handles the full per-document extraction workflow: clean text →
    run model inference → decode BIO tags → aggregate spans → filter
    by confidence threshold → return structured entity records.

    Designed to be instantiated once per worker process and reused
    across all documents assigned to that worker by joblib.

    Parameters
    ----------
    model : NERModel
        A loaded NERModel instance ready for inference.
    confidence_threshold : float
        Minimum softmax score required to accept a predicted entity span.
        Predictions below this threshold are discarded. Default: 0.80.
    """

    def __init__(self, model: NERModel, confidence_threshold: float = 0.80):
        self.model = model
        self.confidence_threshold = confidence_threshold

    def extract(self, paper: dict) -> dict:
        """
        Extract all named entities from a single paper record.

        Primary entry point and the function parallelized by ner_main.py.
        Accepts a full paper dict (as written by the ingestion layer),
        pulls the abstract field, runs the full extraction pipeline,
        and returns a structured result linked to the paper's ID.

        Parameters
        ----------
        paper : dict
            A normalized paper record containing at minimum:
            - paper_id (str): Unique identifier for the paper
            - abstract (str): Raw or cleaned abstract text
            - source (str): Origin source ('arxiv', 's2orc', 'openalex')

        Returns
        -------
        dict
            Extraction result with keys:
            - paper_id (str): Passed through from input
            - source (str): Passed through from input
            - entities (list[dict]): List of extracted entity records
              (see _build_entity_record for structure)
            - entity_counts (dict): Count of entities per type
              e.g. {'METHOD': 3, 'DATASET': 1, 'TASK': 2}
        """
        # extract all named entities from a single paper record
        paper_id = paper["paper_id"]
        source = paper["source"]
        abstract = paper["abstract"]
        # run NER model inference to get per-token predictions
        token_predictions = self.model.predict(abstract)
        spans = self._decode_bio_tags(token_predictions)   # group B/I tokens into spans
        spans = self._filter_by_confidence(spans)           # drop low-confidence ones
        entities = [self._build_entity_record(span, paper_id) for span in spans] # build final entity records
        # count entities by type for downstream analytics
        entity_counts = {"METHOD": 0, "DATASET": 0, "TASK": 0}
        for entity in entities:
            entity_counts[entity["entity_type"]] += 1

        return {
    "paper_id": paper_id,
    "source": source,
    "entities": entities,
    "entity_counts": entity_counts,
}
        
    
    def _decode_bio_tags(self, token_predictions: list[dict]) -> list[dict]:
        """
        Decode BIO-tagged token predictions into contiguous entity spans.
 
        Iterates over the per-token predictions returned by NERModel.predict()
        and groups consecutive B-/I- tokens of the same entity type into
        single spans. O-tagged tokens are discarded.
 
        Handles edge cases such as:
        - I- tag appearing without a preceding B- tag (treated as B-)
        - Entity type change mid-sequence without an O token separator
        - Subword tokens (## prefixed) that belong to the same word
 
        Parameters
        ----------
        token_predictions : list[dict]
            Per-token prediction dicts as returned by NERModel.predict(),
            each with keys: token, label_id, score, start, end.
 
        Returns
        -------
        list[dict]
            List of span dicts, each with keys:
            - text (str): Aggregated span text
            - label (str): Entity type string (e.g. 'METHOD')
            - score (float): Mean confidence score across span tokens
            - start (int): Character start offset
            - end (int): Character end offset
        """
        spans = []
        current_span = None
        for token_pred in token_predictions:
            label = self.model.label_map[token_pred["label_id"]]  # e.g. "B-METHOD"

        if label == "O":
            if current_span:
                spans.append(current_span)
                current_span = None

        elif label.startswith("B-"):
            if current_span:          # save whatever we were building
                spans.append(current_span)
            entity_type = label[2:]   # strip "B-" → "METHOD"
            current_span = {
            "tokens": [token_pred],
            "label": entity_type,
            "start": token_pred["start"],
            "end": token_pred["end"],
        }
        
        elif label.startswith("I-"):
            entity_type = label[2:]
            if current_span and current_span["label"] == entity_type:
            # normal case — continue building the span
                current_span["tokens"].append(token_pred)
                current_span["end"] = token_pred["end"]
            else:
                # edge case — I- without matching B-, treat as new span
                if current_span:
                    spans.append(current_span)
                current_span = {
                "tokens": [token_pred],
                "label": entity_type,
                "start": token_pred["start"],
                "end": token_pred["end"],
            }

# don't forget the last span if the abstract ends mid-entity
        if current_span:
            spans.append(current_span)
        result = []
        for span in spans:
            result.append({
        "text": self._aggregate_subwords(span["tokens"]),
        "label": span["label"],
        "score": sum(t["score"] for t in span["tokens"]) / len(span["tokens"]),
        "start": span["start"],
        "end": span["end"],
    })
        return result


        

    def _aggregate_subwords(self, tokens: list[dict]) -> str:
        """
        Reconstruct a full entity string from a list of subword tokens.

        BERT tokenization splits words into subword pieces (e.g.,
        "Transformer" → ["Transform", "##er"]). This method merges
        subword tokens back into readable entity strings by stripping
        the ## prefix and joining without spaces where appropriate.

        Parameters
        ----------
        tokens : list[dict]
            List of per-token prediction dicts belonging to a single
            entity span, as grouped by _decode_bio_tags().

        Returns
        -------
        str
            Reconstructed entity string with subword tokens merged.
        """
        result = ""
        for token in tokens:
            if token["token"].startswith("##"):
                result += token["token"][2:]  # strip ## and attach directly
            else:
                result += " " + token["token"] if result else token["token"]
            return result.strip()  # remove leading space if present

    def _filter_by_confidence(self, spans: list[dict]) -> list[dict]:
        """
        Remove entity spans whose mean confidence score falls below
        the instance's confidence_threshold.

        Parameters
        ----------
        spans : list[dict]
            List of decoded span dicts as returned by _decode_bio_tags().

        Returns
        -------
        list[dict]
            Filtered list containing only spans that meet or exceed
            the confidence threshold.
        """
        return [span for span in spans if span["score"] >= self.confidence_threshold]

    def _build_entity_record(self, span: dict, paper_id: str) -> dict:
        """
        Construct a structured entity record from a decoded span.

        Produces the final output format for a single entity mention,
        linking it to its source paper and normalizing the entity text
        (e.g., lowercasing, stripping punctuation artifacts).

        Parameters
        ----------
        span : dict
            A decoded span dict with keys: text, label, score, start, end.
        paper_id : str
            The ID of the paper this entity was extracted from.

        Returns
        -------
        dict
            Entity record with keys:
            - entity_text (str): Normalized entity surface form
            - entity_type (str): One of 'METHOD', 'DATASET', 'TASK'
            - confidence (float): Mean span confidence score
            - paper_id (str): Source paper identifier
            - char_start (int): Character offset in abstract
            - char_end (int): Character offset in abstract
        """
        return {
            "entity_text": span["text"],
            "entity_type": span["label"],
            "confidence": span["score"],
            "paper_id": paper_id,
            "char_start": span["start"],
            "char_end": span["end"],
        }