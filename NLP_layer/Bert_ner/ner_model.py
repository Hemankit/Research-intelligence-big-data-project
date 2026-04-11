"""
model.py
--------
Loads and wraps a fine-tuned BERT-based NER model from HuggingFace
Transformers for entity extraction from academic paper abstracts.

The model is expected to be fine-tuned on a scientific NER dataset
(e.g., SciERC or a custom annotated corpus) and capable of recognizing
three entity types:
  - METHOD   : ML techniques, algorithms, architectures (e.g., "BERT", "LSTM")
  - DATASET  : Benchmark datasets and corpora (e.g., "ImageNet", "SQuAD")
  - TASK     : Research tasks and problems (e.g., "machine translation", "NER")

Model loading is expensive. This class is designed to be instantiated
once and passed to the extractor — never reloaded per document.

Dependencies: transformers, torch
"""
from transformers import AutoTokenizer, AutoModelForTokenClassification
import torch
import torch.nn.functional as F
from tqdm import tqdm
import logging

logger = logging.getLogger(__name__)


class NERModel:
    def __init__(
        self,
        model_name_or_path: str,
        device: str = None,
        max_length: int = 512,
    ):
        self.model_name_or_path = model_name_or_path
        self.device             = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.max_length         = max_length
        self.tokenizer          = None
        self.model              = None

    def load(self):
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name_or_path)
            self.model     = AutoModelForTokenClassification.from_pretrained(
                self.model_name_or_path
            )
        except OSError as e:
            raise OSError(f"Failed to load model from '{self.model_name_or_path}': {e}")
        if self.device == "cuda" and not torch.cuda.is_available():
            raise RuntimeError("CUDA requested but not available.")
        self.model.to(self.device)
        self.model.eval()
        logger.info("NERModel loaded: %s on %s", self.model_name_or_path, self.device)

    def predict(self, text: str) -> list[dict]:
        inputs = self.tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            max_length=self.max_length,
            return_offsets_mapping=True,
        )
        offset_mapping = inputs.pop("offset_mapping")
        self.model.eval()
        with torch.no_grad():
            outputs = self.model(**{k: v.to(self.device) for k, v in inputs.items()})

        logits = outputs.logits[0]          # (seq_len, num_labels)
        probs  = F.softmax(logits, dim=-1)
        predicted_label_ids = logits.argmax(dim=-1)  # fixed: removed erroneous probs arg
        scores = probs.max(dim=-1).values

        predictions = []
        for token_id, label_id, score, offsets in zip(
            inputs["input_ids"][0],
            predicted_label_ids,
            scores,
            offset_mapping[0],
        ):
            predictions.append({
                "token":    self.tokenizer.decode([token_id.item()]),
                "label_id": label_id.item(),
                "score":    score.item(),
                "start":    offsets[0].item(),
                "end":      offsets[1].item(),
            })
        return predictions

    def predict_batch(self, texts: list[str], batch_size: int = 32) -> list[list[dict]]:
        all_predictions = []
        for i in tqdm(range(0, len(texts), batch_size), desc="NER batches", unit="batch"):
            batch_texts = texts[i:i + batch_size]
            inputs = self.tokenizer(
                batch_texts,
                return_tensors="pt",
                padding=True,
                truncation=True,
                max_length=self.max_length,
                return_offsets_mapping=True,
            )
            offset_mapping = inputs.pop("offset_mapping")
            self.model.eval()
            with torch.no_grad():
                outputs = self.model(**{k: v.to(self.device) for k, v in inputs.items()})

            logits = outputs.logits
            probs  = F.softmax(logits, dim=-1)
            predicted_label_ids = logits.argmax(dim=-1)
            scores = probs.max(dim=-1).values

            for idx in range(len(batch_texts)):
                predictions = [
                    {
                        "token":    self.tokenizer.decode([token_id.item()]),
                        "label_id": label_id.item(),
                        "score":    score.item(),
                        "start":    offsets[0].item(),
                        "end":      offsets[1].item(),
                    }
                    for token_id, label_id, score, offsets in zip(
                        inputs["input_ids"][idx],
                        predicted_label_ids[idx],
                        scores[idx],
                        offset_mapping[idx],
                    )
                ]
                all_predictions.append(predictions)
        return all_predictions

    @property
    def label_map(self) -> dict:
        if self.model is None:
            return {}
        return self.model.config.id2label

    def is_loaded(self) -> bool:
        return self.model is not None and self.tokenizer is not None