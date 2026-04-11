"""
embedder.py
-----------
Generates dense sentence embeddings for the full abstract corpus using
a HuggingFace sentence transformer model.

Embeddings are the input to UMAP dimensionality reduction and are the
most computationally expensive step in the topic modeling pipeline.
This module isolates embedding generation so that results can be cached
to disk and reused across multiple clustering experiments without
recomputing embeddings each time.

Recommended model: 'all-MiniLM-L6-v2' — fast, lightweight, and produces
strong semantic embeddings for scientific text. For higher quality at
greater cost, consider 'allenai-specter' which is fine-tuned on
scientific paper abstracts specifically.

Dependencies: sentence-transformers, numpy
"""

import numpy as np
from sentence_transformers import SentenceTransformer
import os
import torch 

class Embedder:
    """
    Encodes a list of cleaned abstracts into dense vector embeddings
    using a sentence transformer model.

    Handles model loading, batched encoding, and optional caching of
    computed embeddings to disk so that UMAP and HDBSCAN experiments
    can be re-run without repeating the encoding step.

    Parameters
    ----------
    model_name : str
        HuggingFace sentence transformer model name or local path.
        Default: 'all-MiniLM-L6-v2'.
    batch_size : int
        Number of abstracts to encode per forward pass. Default: 64.
        Reduce if running out of memory; increase for GPU throughput.
    cache_path : str, optional
        Local or HDFS path to cache computed embeddings as a .npy file.
        If provided and the file exists, embeddings are loaded from cache
        instead of recomputed. If None, caching is disabled.
    device : str, optional
        Device for inference ('cpu' or 'cuda'). Auto-detected if None.
    """

    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        batch_size: int = 64,
        cache_path: str = None,
        device: str = None,
    ):
        self.model_name = model_name
        self.batch_size = batch_size
        self.cache_path = cache_path
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.model = None

    def load(self):
        """
        Load the sentence transformer model.

        Initializes the SentenceTransformer instance and moves it to
        the configured device. Should be called once before encode().

        Raises
        ------
        OSError
            If the model name or path cannot be resolved.
        """
        # load the model and raise os error if it fails (e.g., invalid model name or path)
        try:
            self.model = SentenceTransformer(self.model_name, device=self.device)
        except OSError as e:
            raise OSError(f"Failed to load model '{self.model_name}': {e}") 

    def encode(self, abstracts: list[str]) -> np.ndarray:
        """
        Encode a list of cleaned abstracts into a 2D embedding matrix.

        Primary entry point for embedding generation. If a cache path
        is configured and the cache file exists, embeddings are loaded
        from disk and returned immediately without re-encoding. Otherwise,
        encodes all abstracts and writes the result to cache if configured.

        Parameters
        ----------
        abstracts : list[str]
            List of cleaned abstract strings to encode. Order must match
            the paper_ids list returned by CorpusLoader.load() since
            embeddings[i] corresponds to paper_ids[i].

        Returns
        -------
        np.ndarray
            2D array of shape (n_papers, embedding_dim) where each row
            is the embedding vector for the corresponding abstract.
            
        Raises
        ------
        ValueError
            If abstracts is empty or if cached embeddings don't match input size.
        TypeError
            If abstracts is not a list.
        """
        # Validate input at API boundary
        if not isinstance(abstracts, list):
            raise TypeError(f"abstracts must be a list, got {type(abstracts)}")
        
        if not abstracts:
            raise ValueError("Cannot encode empty list of abstracts")
        
        n_papers = len(abstracts)
        
        # check cache first
        if self.cache_path and self.cache_exists():
            cached_embeddings = self.load_cache()
            
            # Validate cached embeddings match current input
            if not isinstance(cached_embeddings, np.ndarray):
                raise ValueError(
                    f"Cached embeddings are {type(cached_embeddings)}, expected np.ndarray"
                )
            
            if cached_embeddings.ndim != 2:
                raise ValueError(
                    f"Cached embeddings have {cached_embeddings.ndim} dimensions, expected 2D array. "
                    f"Shape: {cached_embeddings.shape}"
                )
            
            if cached_embeddings.shape[0] != n_papers:
                raise ValueError(
                    f"Cached embeddings have {cached_embeddings.shape[0]} rows, "
                    f"but provided {n_papers} abstracts. Cache may be stale or corrupted."
                )
            
            return cached_embeddings
        
        # encode in batches and save to cache if configured
        embeddings = self._encode_batched(abstracts)
        if self.cache_path:
            self.save_cache(embeddings)
        return embeddings

    def _encode_batched(self, abstracts: list[str]) -> np.ndarray:
        """
        Encode abstracts in mini-batches and concatenate results.

        Splits the corpus into batches of batch_size and encodes each
        batch sequentially. Logs progress per batch. Concatenates all
        batch outputs into a single (n_papers, embedding_dim) array.

        Parameters
        ----------
        abstracts : list[str]
            Full list of cleaned abstracts to encode.

        Returns
        -------
        np.ndarray
            Concatenated embedding matrix of shape (n_papers, embedding_dim).
            
        Raises
        ------
        ValueError
            If abstracts is empty or if batch embeddings have inconsistent shapes.
        """
        # Validate input
        if not abstracts:
            raise ValueError("Cannot encode empty list of abstracts")
        
        embedding_matrix = []
        expected_dim = None
        
        for i in range(0, len(abstracts), self.batch_size):
            batch = abstracts[i:i + self.batch_size]
            batch_embeddings = self.model.encode(batch, show_progress_bar=False)
            
            # Validate batch embeddings shape
            if not isinstance(batch_embeddings, np.ndarray):
                raise ValueError(f"model.encode() returned {type(batch_embeddings)}, expected np.ndarray")
            
            if batch_embeddings.ndim != 2:
                raise ValueError(
                    f"Batch embeddings have {batch_embeddings.ndim} dimensions, expected 2D array. "
                    f"Shape: {batch_embeddings.shape}"
                )
            
            if batch_embeddings.shape[0] != len(batch):
                raise ValueError(
                    f"Batch size mismatch: expected {len(batch)} embeddings, got {batch_embeddings.shape[0]}"
                )
            
            # Ensure consistent embedding dimension across batches
            if expected_dim is None:
                expected_dim = batch_embeddings.shape[1]
            elif batch_embeddings.shape[1] != expected_dim:
                raise ValueError(
                    f"Inconsistent embedding dimension: expected {expected_dim}, "
                    f"got {batch_embeddings.shape[1]} in batch {i // self.batch_size + 1}"
                )
            
            embedding_matrix.append(batch_embeddings)
            print(f"Encoded batch {i // self.batch_size + 1} / {(len(abstracts) - 1) // self.batch_size + 1}")
        
        # Concatenate all batches
        result = np.vstack(embedding_matrix)
        
        # Final validation: ensure output shape is (n_papers, embedding_dim)
        if result.shape[0] != len(abstracts):
            raise ValueError(
                f"Output shape mismatch: expected {len(abstracts)} rows, got {result.shape[0]}"
            )
        
        return result

    def save_cache(self, embeddings: np.ndarray):
        """
        Save a computed embedding matrix to the configured cache path.

        Writes the numpy array to disk as a .npy file. Creates parent
        directories if they do not exist. Logs the cache path and
        array shape on success.

        Parameters
        ----------
        embeddings : np.ndarray
            Embedding matrix to cache, shape (n_papers, embedding_dim).

        Raises
        ------
        ValueError
            If cache_path was not configured on instantiation.
        """
        if not self.cache_path:
            raise ValueError("cache_path is not configured")
        try:
            # create parent directories if they don't exist
            os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
            np.save(self.cache_path, embeddings)
        except Exception as e:
            raise ValueError(f"Failed to save embeddings to cache: {e}")

    def load_cache(self) -> np.ndarray:
        """
        Load a previously cached embedding matrix from disk.

        Parameters
        ----------
        None

        Returns
        -------
        np.ndarray
            Cached embedding matrix of shape (n_papers, embedding_dim).

        Raises
        ------
        FileNotFoundError
            If the cache file does not exist at the configured path.
        ValueError
            If cache_path was not configured on instantiation.
        """
        try:
            if not self.cache_path:
                raise ValueError("cache_path is not configured")
            return np.load(self.cache_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Cache file not found at {self.cache_path}")

    def cache_exists(self) -> bool:
        """
        Return True if a valid embedding cache file exists at cache_path.

        Used by encode() to decide whether to load from cache or
        recompute embeddings. Returns False if cache_path is not set.

        Returns
        -------
        bool
            True if the cache file exists and is non-empty.
        """
        if not self.cache_path:
            return False
        return os.path.exists(self.cache_path) and os.path.getsize(self.cache_path) > 0