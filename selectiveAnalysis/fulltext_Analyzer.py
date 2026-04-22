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
              shared/Tokenizer.py
"""

import logging
from NLP_layer.shared.text_preprocessing import TextCleaner
from NLP_layer.shared.Tokenizer import Tokenizer
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import numpy as np
logger = logging.getLogger(__name__)


class FullTextAnalyzer:
    """
    Applies targeted NLP analysis to full-text content of selected papers.

    Retrieves full text from Hive, runs sentence-level classification to
    extract structured insights, and returns enriched annotation records
    linked to each paper's ID for downstream use by organizer.py and
    the dashboard API.

    Parameters
    ----------
    hive_conn : object
        Hive connection or SparkSession for reading from paper_fulltext table.
    text_cleaner : TextCleaner
        Initialized TextCleaner instance from shared/text_preprocessing.py.
    tokenizer : Tokenizer
        Initialized Tokenizer instance from shared/Tokenizer.py.
    model_name : str
        HuggingFace model for sentence classification. Should be a model
        fine-tuned for scientific sentence classification (e.g. identifying
        contribution and limitation sentences).
        Default: 'allenai/scibert_scivocab_cased'.
    confidence_threshold : float
        Minimum confidence for sentence classification. Sentences below
        this threshold are discarded. Default: 0.75.
    """

    def __init__(
        self,
        hive_conn,
        text_cleaner: TextCleaner,
        tokenizer: Tokenizer,
        model_name: str = "allenai/scibert_scivocab_cased",
        confidence_threshold: float = 0.75,
    ):
        self.hive_conn = hive_conn
        self.text_cleaner = text_cleaner
        self.tokenizer = tokenizer
        self.model_name = model_name
        self.confidence_threshold = confidence_threshold
        # Load the HuggingFace model and tokenizer once at initialization
        # to avoid repeated loading overhead per paper.
        self.classifier_tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.classifier_model = AutoModelForSequenceClassification.from_pretrained(self.model_name)

    def analyze(self, selected_papers: list[dict]) -> list[dict]:
        """
        Run full targeted NLP analysis on a list of selected papers.

        Primary entry point called by run.py. For each paper, retrieves
        its full text from Hive, cleans and sentences the text, runs
        sentence classification, and aggregates results into a structured
        annotation record.

        Papers without available full text (not in paper_fulltext table)
        are analyzed using their abstract only, with a flag indicating
        reduced analysis depth.

        Parameters
        ----------
        selected_papers : list[dict]
            Papers selected by SelectionEngine.select(), each containing
            at minimum paper_id, title, and abstract fields.

        Returns
        -------
        list[dict]
            Annotation records, one per input paper, each containing:
            - paper_id (str): Paper identifier
            - has_fulltext (bool): Whether full text was available
            - methodological_details (list[str]): Extracted method sentences
            - limitations (list[str]): Extracted limitation sentences
            - contributions (list[str]): Extracted contribution sentences
            - evaluation_context (list[str]): Extracted evaluation sentences
        """
        # for each paper check for full text in Hive
        for paper in selected_papers:
            text = self._fetch_fulltext([paper["paper_id"]])
            # If paper not in fulltext table, fall back to abstract and set has_fulltext=False
            if text:
                paper["has_fulltext"] = True
                full_text = text[paper["paper_id"]]
            else:
                paper["has_fulltext"] = False
                full_text = paper["abstract"]
            # Clean and sentence the text
            cleaned_text = self.text_cleaner.clean(full_text)
            cleaned_sentences = self.tokenizer.split_sentences(cleaned_text)
            # Classify sentences and aggregate results
            classified = self._classify_sentences(cleaned_sentences)
            aggregated = self._aggregate_by_category(classified)
            paper.update(aggregated)
        return selected_papers

    def _fetch_fulltext(self, paper_ids: list[str]) -> dict[str, str]:
        """
        Retrieve full text for a list of paper IDs from the Hive
        paper_fulltext table.

        Returns a dict mapping paper_id to full_text string. Papers not
        found in the table are omitted from the dict — callers should
        fall back to abstract text for missing entries.

        Parameters
        ----------
        paper_ids : list[str]
            List of paper IDs to fetch full text for.

        Returns
        -------
        dict[str, str]
            Mapping of paper_id to full_text string for papers that
            have full text available in the Hive table.
        """
        # retrieve full text for the given paper_ids from Hive
        for paper_id in paper_ids:
             # query Hive for full text of this paper_id
            full_text = self.hive_conn.query(f"SELECT full_text FROM paper_fulltext WHERE paper_id = '{paper_id}'")
            if full_text:
                return {paper_id: full_text}
            # if not found return empty dict to signal fallback to abstract
        return {}
            

    def _classify_sentences(self, sentences: list[str]) -> list[dict]:
        """
        Classify each sentence into one of the four extraction categories
        or discard it as non-informative.

        Runs a HuggingFace zero-shot or fine-tuned sentence classifier
        to assign each sentence to one of:
          - METHODOLOGY   : describes a technique or algorithmic step
          - LIMITATION    : describes a failure case, assumption, or constraint
          - CONTRIBUTION  : claims a novel contribution vs prior work
          - EVALUATION    : describes experimental setup, datasets, or metrics
          - OTHER         : not informative for extraction purposes

        Sentences classified as OTHER or below confidence_threshold are
        discarded. Returns only sentences with accepted classifications.

        Parameters
        ----------
        sentences : list[str]
            List of cleaned sentence strings from a single paper.

        Returns
        -------
        list[dict]
            Accepted sentence records, each with keys:
            - sentence (str): The sentence text
            - category (str): Assigned category label
            - confidence (float): Classification confidence score
        """
        for sentence in sentences:
            # tokenize and encode the sentence for the classifier
            inputs = self.classifier_tokenizer(sentence, return_tensors="pt", truncation=True)
            # run the classifier model to get category logits
            outputs = self.classifier_model(**inputs)
            logits = outputs.logits
            # convert logits to probabilities and determine predicted category
            probabilities = torch.softmax(logits, dim=1).detach().cpu().numpy()[0]
            category_idx = np.argmax(probabilities)
            confidence = probabilities[category_idx]
            category_label = self.classifier_model.config.id2label[category_idx]
            # filter by confidence threshold and discard OTHER category
            if confidence >= self.confidence_threshold and category_label != "OTHER":
                yield {
                    "sentence": sentence,
                    "category": category_label,
                    "confidence": confidence,
                }
            

    def _aggregate_by_category(self, classified_sentences: list[dict]) -> dict:
        """
        Group classified sentences by their category into the four
        extraction output lists.

        Sorts sentences within each category by confidence score
        descending, so the highest-confidence extractions appear first.
        Caps each category at a maximum of 5 sentences to keep outputs
        concise for dashboard display.

        Parameters
        ----------
        classified_sentences : list[dict]
            Classified sentence records as returned by _classify_sentences().

        Returns
        -------
        dict
            Grouped output with keys:
            - methodological_details (list[str])
            - limitations (list[str])
            - contributions (list[str])
            - evaluation_context (list[str])
            Each value is a list of sentence strings, sorted by
            confidence and capped at 5 items.
        """
        max_sentences_per_category = 5
        categories = {
            "METHODOLOGY": [],
            "LIMITATION": [],
            "CONTRIBUTION": [],
            "EVALUATION": [],
        }
        for record in classified_sentences:
            categories[record["category"]].append((record["sentence"], record["confidence"]))
        # sort each category by confidence and cap at max_sentences_per_category
        for cat in categories:
            categories[cat] = sorted(categories[cat], key=lambda x: x[1], reverse=True)[:max_sentences_per_category]
            categories[cat] = [s for s, _ in categories[cat]]  # keep only sentences
        return categories

    def _analyze_single_paper(self, paper: dict, fulltext_map: dict[str, str]) -> dict:
        """
        Run the full extraction pipeline for a single paper.

        Retrieves full text from fulltext_map (falls back to abstract),
        cleans the text, splits into sentences, classifies sentences,
        and aggregates into the four output categories.

        Parameters
        ----------
        paper : dict
            Paper dict containing paper_id and abstract fields.
        fulltext_map : dict[str, str]
            Mapping of paper_id to full_text as returned by _fetch_fulltext().

        Returns
        -------
        dict
            Annotation record for this paper with paper_id, has_fulltext,
            and the four extraction category lists.
        """
        paper_id = paper["paper_id"]
        full_text = fulltext_map.get(paper_id, paper["abstract"])
        has_fulltext = paper_id in fulltext_map
        cleaned_text = self.text_cleaner.clean(full_text)
        cleaned_sentences = self.tokenizer.split_sentences(cleaned_text)
        classified = list(self._classify_sentences(cleaned_sentences))
        aggregated = self._aggregate_by_category(classified)
        return {
            "paper_id": paper_id,
            "has_fulltext": has_fulltext,
            **aggregated,
        }