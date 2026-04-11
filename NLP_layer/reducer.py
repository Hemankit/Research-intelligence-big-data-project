"""
reducer.py
----------
Wraps UMAP dimensionality reduction for use within the BERTopic pipeline.

Reduces high-dimensional sentence embeddings (typically 384 or 768 dims)
down to a lower-dimensional space before HDBSCAN clustering. Keeping UMAP
configuration isolated here makes it easy to tune hyperparameters
independently without modifying the core topic model or clusterer.

BERTopic uses UMAP in two distinct ways:
  1. For clustering: reduce to low dims (e.g. 5) to improve HDBSCAN density
     estimation. Dense clusters in high dimensions are hard for HDBSCAN
     to detect reliably.
  2. For visualization: reduce to 2D for the dashboard Landscape Map view.

Both configurations are supported here via separate methods.

Dependencies: umap-learn
"""

"""
reducer.py — UMAP dimensionality reduction wrapper.
"""
import umap
import numpy as np


class UMAPReducer:
    def __init__(
        self,
        n_components: int = 5,
        n_neighbors: int = 15,
        min_dist: float = 0.0,
        metric: str = "cosine",
        random_state: int = 42,
    ):
        self.n_components  = n_components
        self.n_neighbors   = n_neighbors
        self.min_dist      = min_dist
        self.metric        = metric
        self.random_state  = random_state
        self.umap_model    = None
        self.umap_model_2d = None

    def build(self) -> umap.UMAP:
        self.umap_model = umap.UMAP(
            n_components=self.n_components,
            n_neighbors=self.n_neighbors,
            min_dist=self.min_dist,
            metric=self.metric,
            random_state=self.random_state,
        )
        return self.umap_model

    def build_2d(self, n_neighbors: int = 20) -> umap.UMAP:
        self.umap_model_2d = umap.UMAP(
            n_components=2,
            n_neighbors=n_neighbors,
            min_dist=self.min_dist + 0.1,
            metric=self.metric,
            random_state=self.random_state,
        )
        return self.umap_model_2d

    def fit_transform(self, embeddings) -> np.ndarray:
        return self.build().fit_transform(embeddings)

    def fit_transform_2d(self, embeddings) -> np.ndarray:
        return self.build_2d().fit_transform(embeddings)