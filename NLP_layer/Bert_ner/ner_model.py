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
from tqdm import tqdm
import torch.nn.functional as F
import logging

class NERModel:
    """
    Wraps a HuggingFace BERT-based token classification model for
    scientific named entity recognition.

    Handles model and tokenizer loading, device placement (CPU/GPU),
    and raw inference on text inputs. Does not handle entity span
    aggregation or label post-processing — that is done in extractor.py.

    Parameters
    ----------
    model_name_or_path : str
        HuggingFace model hub name or local path to a fine-tuned NER model.
        Example: 'allenai/scibert_scivocab_cased' or a local checkpoint path.
    device : str
        Device to run inference on. 'cpu' by default. Pass 'cuda' if a
        GPU is available. If None, auto-detects based on torch availability.
    max_length : int
        Maximum token length for the tokenizer. Abstracts exceeding this
        length will be truncated. Default: 512 (BERT maximum).
    """

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
        """
        Load the tokenizer and model weights from HuggingFace hub or
        a local checkpoint path.

        Sets self.tokenizer and self.model on the instance. Moves the
        model to the configured device and sets it to eval mode.
        Should be called once after instantiation before any inference.

        Raises
        ------
        OSError
            If the model name or path cannot be resolved by HuggingFace.
        RuntimeError
            If CUDA is requested but not available.
        """
        # load the model
        try:
            tokenizer = AutoTokenizer.from_pretrained(self.model_name_or_path)
            model     = AutoModelForTokenClassification.from_pretrained(self.model_name_or_path)
        except OSError as e:
            raise OSError(f"Failed to load model from '{self.model_name_or_path}': {e}")
        # move to device
        if self.device == "cuda" and not torch.cuda.is_available():
            raise RuntimeError("CUDA requested but not available. Check your environment.")
        model.to(self.device)
        model.eval()
        # set instance variables
        self.tokenizer = tokenizer
        self.model     = model

    def predict(self, text: str) -> list[dict]:
        """
        Run token classification inference on a single text string.

        Tokenizes the input, runs a forward pass through the model,
        and returns raw per-token predictions with their logits and
        predicted label IDs. Does not perform span aggregation or
        label decoding — raw output is passed to extractor.py for
        post-processing.

        Parameters
        ----------
        text : str
            A single cleaned abstract string ready for inference.

        Returns
        -------
        list[dict]
            List of per-token prediction dicts, each containing:
            - token (str): The decoded token string
            - label_id (int): Predicted label index
            - score (float): Softmax confidence score for predicted label
            - start (int): Character start offset in original text
            - end (int): Character end offset in original text
        """
        inputs = self.tokenizer(
            text, return_tensors="pt", return_offsets_mapping=True) # adjust as needed
        self.model.eval()
        with torch.no_grad():
            outputs = self.model(**inputs.to(self.device))
        # process outputs to extract token-level predictions
        logits = outputs.logits[0]  # shape: (seq_len, num_labels)
        probs = F.softmax(logits, dim=-1) # shape: (seq_len,)
        predicted_label_ids = logits.argmax(probs, dim=-1) # shape: (seq_len,)
        scores = probs.max(dim=-1).values # shape: (seq_len,)
        predictions = [ {
        "token": self.tokenizer.decode(token_id),
        "label_id": label_id.item(),
        "score": score.item(),
        "start": offsets[0].item(),
        "end": offsets[1].item(),
        }
        # loop through each token in the input and pair it with its predicted label and score
        for token_id, label_id, score, offsets in zip(
        inputs["input_ids"][0], predicted_label_ids, scores, inputs["offset_mapping"][0]
    )
    ]
        return predictions
    
    def predict_batch(self, texts: list[str], batch_size: int = 32) -> list[list[dict]]:
        """
        Run inference on a batch of text strings.

        Processes texts in mini-batches to manage memory. More efficient
        than calling predict() in a loop when GPU inference is available,
        as it allows the model to process multiple abstracts per forward pass.

        Parameters
        ----------
        texts : list[str]
            List of cleaned abstract strings.
        batch_size : int
            Number of abstracts per inference batch (default: 32).
            Reduce if running out of memory on GPU.

        Returns
        -------
        list[list[dict]]
            List of per-token prediction lists, one per input text,
            in the same order as the input.
        """
        tqdm.write(f"Running NER inference on {len(texts)} abstracts with batch size {batch_size}...")
        all_predictions = []
        
        for i in tqdm(range(0, len(texts), batch_size), desc="NER batches", unit="batch"):
            batch_texts = texts[i:i+batch_size]
            
            # Tokenize entire batch with padding (crucial for batched inference)
            inputs = self.tokenizer(
                batch_texts,
                return_tensors="pt",
                padding=True,
                truncation=True,
                max_length=self.max_length,
                return_offsets_mapping=True
            )
            
            # Single forward pass for the entire batch
            self.model.eval()
            with torch.no_grad():
                outputs = self.model(**inputs.to(self.device))
            
            # Process outputs for each text in the batch
            logits = outputs.logits  # shape: (batch_size, seq_len, num_labels)
            probs = F.softmax(logits, dim=-1)
            predicted_label_ids = logits.argmax(dim=-1)  # shape: (batch_size, seq_len)
            scores = probs.max(dim=-1).values  # shape: (batch_size, seq_len)
            
            # Decode each text's predictions separately
            for idx in range(len(batch_texts)):
                predictions = [
                    {
                        "token": self.tokenizer.decode(token_id),
                        "label_id": label_id.item(),
                        "score": score.item(),
                        "start": offsets[0].item(),
                        "end": offsets[1].item(),
                    }
                    for token_id, label_id, score, offsets in zip(
                        inputs["input_ids"][idx],
                        predicted_label_ids[idx],
                        scores[idx],
                        inputs["offset_mapping"][idx]
                    )
                ]
                all_predictions.append(predictions)
        
        return all_predictions

    @property
    def label_map(self) -> dict:
        """
        Return the mapping from label IDs to label strings.

        Derived from the model's config.id2label. Used by extractor.py
        to decode predicted label IDs into entity type strings
        (e.g., {0: 'O', 1: 'B-METHOD', 2: 'I-METHOD', ...}).

        Returns
        -------
        dict
            Mapping of integer label IDs to BIO label strings.
        """
        if self.model is None:
            return {}
        return self.model.config.id2label

    def is_loaded(self) -> bool:
        """
        Return True if the model and tokenizer have been successfully loaded.

        Used as a guard in extractor.py to fail fast with a clear error
        if inference is attempted before load() has been called.

        Returns
        -------
        bool
            True if self.model and self.tokenizer are both initialized.
        """
        return self.model is not None and self.tokenizer is not None