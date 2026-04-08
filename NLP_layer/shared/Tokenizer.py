"""
tokenizer.py
------------
Shared tokenization utilities used by both the NER pipeline and the
topic modeling module.

Wraps spaCy's pipeline to provide sentence splitting and token-level
processing on cleaned abstract text. spaCy model loading is expensive
(several seconds per load), so this module is designed to load the model
once and reuse it across all documents rather than reloading per call.

The tokenizer operates on text that has already been processed by
TextCleaner — it expects clean, well-formed input without LaTeX or
encoding artifacts.

Dependencies: spacy
Recommended model: en_core_web_sm (sufficient for sentence splitting)
                   en_core_web_trf (if transformer accuracy is needed)
"""

import spacy


class Tokenizer:
    """
    Wraps a spaCy pipeline for sentence splitting and tokenization.

    Loads the spaCy model once on instantiation and exposes methods
    for sentence splitting, token extraction, and batch processing.
    Designed to be instantiated once and reused across a full corpus
    to avoid repeated model loading overhead.

    Parameters
    ----------
    model_name : str
        Name of the spaCy model to load (default: 'en_core_web_sm').
        Use 'en_core_web_trf' for higher accuracy at greater cost.
    disable : list[str]
        spaCy pipeline components to disable for efficiency.
        Default disables ['ner', 'lemmatizer'] since this class is
        used for splitting only — NER is handled separately.
    """

    def __init__(self, model_name: str = "en_core_web_sm", disable: list[str] = None):
        pass

    def split_sentences(self, text: str) -> list[str]:
        """
        Split a cleaned abstract into individual sentences.

        Uses spaCy's dependency parser-based sentence boundary detection,
        which handles academic writing patterns better than simple
        punctuation splitting (e.g., correctly handles abbreviations
        like 'et al.' and 'Fig.').

        Parameters
        ----------
        text : str
            A single cleaned abstract string.

        Returns
        -------
        list[str]
            Ordered list of sentence strings extracted from the abstract.
            Empty or whitespace-only sentences are excluded.
        """
        pass

    def tokenize(self, text: str) -> list[str]:
        """
        Tokenize a cleaned abstract into a flat list of token strings.

        Returns surface-form tokens (not lemmatized) with punctuation
        tokens filtered out. Useful for vocabulary analysis and as input
        to models that expect pre-tokenized text.

        Parameters
        ----------
        text : str
            A single cleaned abstract string.

        Returns
        -------
        list[str]
            List of token strings with punctuation removed.
        """
        pass

    def tokenize_with_pos(self, text: str) -> list[tuple]:
        """
        Tokenize a cleaned abstract and return tokens with POS tags.

        Returns surface-form tokens paired with their coarse-grained
        part-of-speech tags (NOUN, VERB, ADJ, etc.). Useful for
        downstream filtering — for example, the NER pipeline may want
        to restrict candidate spans to noun phrases.

        Parameters
        ----------
        text : str
            A single cleaned abstract string.

        Returns
        -------
        list[tuple]
            List of (token_string, pos_tag) tuples.
        """
        pass

    def process_batch(self, texts: list[str], batch_size: int = 64) -> list[list[str]]:
        """
        Tokenize a list of cleaned abstracts into sentences using
        spaCy's pipe() for efficient batch processing.

        Uses spaCy's nlp.pipe() which streams documents through the
        pipeline in batches, significantly faster than calling
        split_sentences() in a loop for large corpora.

        Parameters
        ----------
        texts : list[str]
            List of cleaned abstract strings.
        batch_size : int
            Number of documents to process per spaCy batch (default 64).
            Increase for faster throughput on large corpora if memory allows.

        Returns
        -------
        list[list[str]]
            List of sentence lists, one per input abstract, in the same
            order as the input.
        """
        pass