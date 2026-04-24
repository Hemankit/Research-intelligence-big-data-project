"""
fulltext_analyzer.py
--------------------
Targeted NLP analysis on a small subset of selected high-value papers.

Applies deeper inspection to full-text content retrieved from the Hive
paper_fulltext table for papers selected by the Selection Engine. Unlike
the batch NER pipeline which runs across the entire corpus, this module
processes only 10-50 papers on-demand per user query.

Extracts four categories of structured information per paper (Section 2.5.2):
  1. Methodological details  — key techniques beyond high-level NER labels
  2. Limitations             — sentences indicating failure cases or constraints
  3. Contribution statements — claimed innovations vs prior work
  4. Evaluation context      — datasets, benchmarks, experimental setups

Uses the existing shared NLP stack (spaCy, HuggingFace Transformers) but
applies more computationally intensive sentence-level classification that
would be too expensive to run across the full corpus.

Since this runs on-demand for a small subset, processing is sequential
rather than parallelized — the overhead of joblib is not justified for
10-50 documents.

Dependencies: spacy, transformers, shared/text_preprocessing.py,
              shared/Tokenizer.py, pyhive

Bugs fixed vs original:
  - analyze() re-implemented the logic of _analyze_single_paper() redundantly.
    Fixed to use the helper directly.
  - _fetch_fulltext() queried one paper at a time in a loop and used
    hive_conn.query() which doesn't exist in pyhive. Fixed to batch query
    all paper IDs at once using a cursor.
  - _classify_sentences() is a generator (yield) but was consumed twice
    in analyze(). Fixed by wrapping in list() in _analyze_single_paper().

Dependencies: spacy, transformers, NLP_layer/shared/text_preprocessing.py,
              NLP_layer/shared/Tokenizer.py, pyhive
"""

import logging
import numpy as np
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

from NLP_layer.shared.text_preprocessing import TextCleaner
from NLP_layer.shared.Tokenizer import Tokenizer

logger = logging.getLogger(__name__)


class FullTextAnalyzer:
    """
    Applies targeted NLP analysis to full-text content of selected papers.

    Parameters
    ----------
    hive_conn : pyhive.hive.Connection
        Hive connection for reading from paper_fulltext table.
    text_cleaner : TextCleaner
        Initialized TextCleaner instance.
    tokenizer : Tokenizer
        Initialized spaCy Tokenizer instance.
    model_name : str
        HuggingFace model for sentence classification.
        Default: 'allenai/scibert_scivocab_cased'.
    confidence_threshold : float
        Minimum confidence for keeping a classified sentence. Default: 0.75.
    """

    def __init__(
        self,
        hive_conn,
        text_cleaner: TextCleaner,
        tokenizer: Tokenizer,
        model_name: str = "allenai/scibert_scivocab_cased",
        confidence_threshold: float = 0.75,
    ):
        self.hive_conn            = hive_conn
        self.text_cleaner         = text_cleaner
        self.tokenizer            = tokenizer
        self.model_name           = model_name
        self.confidence_threshold = confidence_threshold

        # Load model once at init — avoid repeated loading overhead per paper
        logger.info("Loading sentence classifier: %s", model_name)
        self.classifier_tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.classifier_model     = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.classifier_model.eval()
        logger.info("Sentence classifier loaded")

    def analyze(self, selected_papers: list[dict]) -> list[dict]:
        """
        Run targeted NLP analysis on a list of selected papers.

        Batch-fetches full text from Hive for all papers at once, then
        runs the full extraction pipeline per paper using _analyze_single_paper().

        Parameters
        ----------
        selected_papers : list[dict]
            Papers from SelectionEngine.select(), each with paper_id and abstract.

        Returns
        -------
        list[dict]
            Annotation records, one per paper, with paper_id, has_fulltext,
            and the four extraction category lists.
        """
        # Batch fetch all full texts at once — one Hive query for all papers
        paper_ids    = [p["paper_id"] for p in selected_papers]
        fulltext_map = self._fetch_fulltext(paper_ids)

        annotations = []
        for paper in selected_papers:
            annotation = self._analyze_single_paper(paper, fulltext_map)
            annotations.append(annotation)
            logger.info(
                "Analyzed paper %s (has_fulltext=%s)",
                paper["paper_id"], annotation["has_fulltext"]
            )
        return annotations

    def _fetch_fulltext(self, paper_ids: list[str]) -> dict[str, str]:
        """
        Batch-retrieve full text for a list of paper IDs from Hive.

        Uses a single query with IN clause rather than one query per paper.
        Falls back to checking arxiv_id as secondary join key.

        Parameters
        ----------
        paper_ids : list[str]
            Paper IDs to fetch full text for.

        Returns
        -------
        dict[str, str]
            Mapping of paper_id to full_text. Papers not in the table
            are omitted — callers fall back to abstract for missing entries.
        """
        if not paper_ids:
            return {}

        ids_str = ",".join(f"'{pid}'" for pid in paper_ids)
        cursor  = self.hive_conn.cursor()
        try:
            # Try primary join on paper_id first
            cursor.execute(
                f"SELECT paper_id, full_text FROM paper_fulltext "
                f"WHERE paper_id IN ({ids_str}) AND full_text IS NOT NULL"
            )
            rows = cursor.fetchall()

            result = {row[0]: row[1] for row in rows if row[1]}

            # For any IDs not found, try arxiv_id secondary join
            missing = [pid for pid in paper_ids if pid not in result]
            if missing:
                missing_str = ",".join(f"'{pid}'" for pid in missing)
                cursor.execute(
                    f"SELECT arxiv_id, full_text FROM paper_fulltext "
                    f"WHERE arxiv_id IN ({missing_str}) AND full_text IS NOT NULL"
                )
                for row in cursor.fetchall():
                    if row[0] and row[1]:
                        result[row[0]] = row[1]
        finally:
            cursor.close()

        logger.info(
            "Fetched full text for %d / %d papers from Hive",
            len(result), len(paper_ids)
        )
        return result

    def _classify_sentences(self, sentences: list[str]) -> list[dict]:
        """
        Classify each sentence into one of the four extraction categories.

        Runs HuggingFace sentence classifier to assign each sentence to:
          METHODOLOGY, LIMITATION, CONTRIBUTION, EVALUATION, or OTHER.

        Sentences classified as OTHER or below confidence_threshold are
        discarded. Returns a list (not a generator) for safe multi-use.

        Parameters
        ----------
        sentences : list[str]
            Cleaned sentences from a single paper.

        Returns
        -------
        list[dict]
            Accepted sentences with 'sentence', 'category', 'confidence' keys.
        """
        accepted = []
        for sentence in sentences:
            if not sentence.strip():
                continue
            inputs = self.classifier_tokenizer(
                sentence, return_tensors="pt", truncation=True, max_length=512
            )
            with torch.no_grad():
                outputs = self.classifier_model(**inputs)
            probs          = torch.softmax(outputs.logits, dim=1).cpu().numpy()[0]
            category_idx   = int(np.argmax(probs))
            confidence     = float(probs[category_idx])
            category_label = self.classifier_model.config.id2label.get(category_idx, "OTHER")

            if confidence >= self.confidence_threshold and category_label != "OTHER":
                accepted.append({
                    "sentence":   sentence,
                    "category":   category_label,
                    "confidence": confidence,
                })
        return accepted

    def _aggregate_by_category(self, classified_sentences: list[dict]) -> dict:
        """
        Group classified sentences by category into the four extraction lists.

        Sorts by confidence descending and caps each category at 5 sentences.

        Parameters
        ----------
        classified_sentences : list[dict]
            Classified sentence records from _classify_sentences().

        Returns
        -------
        dict
            Keys: methodological_details, limitations, contributions,
            evaluation_context. Each is a list of up to 5 sentence strings.
        """
        MAX_PER_CATEGORY = 5
        buckets = {
            "METHODOLOGY":   [],
            "LIMITATION":    [],
            "CONTRIBUTION":  [],
            "EVALUATION":    [],
        }
        for record in classified_sentences:
            cat = record["category"]
            if cat in buckets:
                buckets[cat].append((record["sentence"], record["confidence"]))

        def top_sentences(items):
            return [s for s, _ in sorted(items, key=lambda x: x[1], reverse=True)[:MAX_PER_CATEGORY]]

        return {
            "methodological_details": top_sentences(buckets["METHODOLOGY"]),
            "limitations":            top_sentences(buckets["LIMITATION"]),
            "contributions":          top_sentences(buckets["CONTRIBUTION"]),
            "evaluation_context":     top_sentences(buckets["EVALUATION"]),
        }

    def _analyze_single_paper(self, paper: dict, fulltext_map: dict[str, str]) -> dict:
        """
        Run the full extraction pipeline for a single paper.

        Falls back to abstract if full text is not available.

        Parameters
        ----------
        paper : dict
            Paper dict with paper_id and abstract fields.
        fulltext_map : dict[str, str]
            Mapping of paper_id to full_text from _fetch_fulltext().

        Returns
        -------
        dict
            Annotation record with paper_id, has_fulltext, and the
            four extraction category lists.
        """
        paper_id     = paper["paper_id"]
        full_text    = fulltext_map.get(paper_id)
        has_fulltext = full_text is not None

        text_to_analyze  = full_text if has_fulltext else paper.get("abstract", "")
        cleaned          = self.text_cleaner.clean(text_to_analyze)
        sentences        = self.tokenizer.split_sentences(cleaned)
        classified       = self._classify_sentences(sentences)
        aggregated       = self._aggregate_by_category(classified)

        return {
            "paper_id":    paper_id,
            "has_fulltext": has_fulltext,
            **aggregated,
        }