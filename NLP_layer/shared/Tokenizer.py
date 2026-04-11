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
    def __init__(self, model_name: str = "en_core_web_sm", disable: list[str] = None):
        self.model_name = model_name
        self.disable = disable or ["ner", "lemmatizer"]
        self.nlp = spacy.load(self.model_name, disable=self.disable)

    def split_sentences(self, text: str) -> list[str]:
        doc = self.nlp(text)
        return [sent.text.strip() for sent in doc.sents if sent.text.strip()]

    def tokenize(self, text: str) -> list[str]:
        doc = self.nlp(text)
        return [token.text for token in doc if not token.is_punct and token.text.strip()]

    def tokenize_with_pos(self, text: str) -> list[tuple]:
        doc = self.nlp(text)
        return [(token.text, token.pos_) for token in doc if not token.is_punct]

    def process_batch(self, texts: list[str], batch_size: int = 64) -> list[list[str]]:
        results = []
        for doc in self.nlp.pipe(texts, batch_size=batch_size):
            results.append([sent.text.strip() for sent in doc.sents if sent.text.strip()])
        return results