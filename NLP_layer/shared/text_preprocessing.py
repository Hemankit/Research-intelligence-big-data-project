"""
text_cleaner.py
---------------
Shared text normalization utilities used by both the NER pipeline and
the topic modeling module before any NLP processing is applied.

Handles two categories of cleaning:
  1. LaTeX artifact removal  — strips macros, math environments, and
     formatting commands commonly found in arXiv abstracts.
  2. Unicode normalization   — normalizes accented characters, removes
     non-printable/control characters, and standardizes whitespace.

This module has no ML dependencies and no side effects — every function
is a pure string-in, string-out transformation. It should be the first
stage applied to any raw abstract before tokenization, embedding, or NER.

Dependencies: re (stdlib), unicodedata (stdlib)
"""

import re
import unicodedata


class TextCleaner:
    """Stateless text cleaner for academic paper abstracts."""

    def remove_latex_macros(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'\$\$.*?\$\$', ' ', text, flags=re.DOTALL)
        text = re.sub(r'\\\[.*?\\\]', ' ', text, flags=re.DOTALL)
        text = re.sub(r'\$[^$]*?\$', ' ', text)
        text = re.sub(r'\\\(.*?\\\)', ' ', text, flags=re.DOTALL)
        for cmd in ['textbf', 'textit', 'emph', 'underline', 'text']:
            text = re.sub(rf'\\{cmd}\{{([^}}]*)\}}', r'\1', text)
        text = re.sub(r'\\href\{[^}]*\}\{([^}]*)\}', r'\1', text)
        for cmd in ['cite', 'ref', 'label', 'footnote', 'url', 'includegraphics']:
            text = re.sub(rf'\\{cmd}\{{[^}}]*\}}', ' ', text)
        text = re.sub(r'\\[a-zA-Z]+\b\*?', ' ', text)
        text = re.sub(r'[{}]', '', text)
        return text

    def normalize_unicode(self, text: str) -> str:
        if not text:
            return ""
        text = unicodedata.normalize('NFC', text)
        text = ''.join(
            ch for ch in text
            if unicodedata.category(ch) not in ('Cc', 'Cf') or ch in ('\n', '\t', ' ')
        )
        return text

    def normalize_whitespace(self, text: str) -> str:
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text).strip()

    def remove_urls(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'https?://\S+', ' ', text)
        text = re.sub(r'www\.\S+', ' ', text)
        return text

    def clean(self, text: str) -> str:
        if not text:
            return ""
        text = self.remove_latex_macros(text)
        text = self.remove_urls(text)
        text = self.normalize_unicode(text)
        text = self.normalize_whitespace(text)
        return text

    def clean_batch(self, texts: list[str]) -> list[str]:
        return [self.clean(t) if t else "" for t in texts]