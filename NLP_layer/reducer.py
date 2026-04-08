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

import umap
import numpy as np

class UMAPReducer:
    """
    Configures and fits a UMAP model for dimensionality reduction of
    sentence embeddings prior to HDBSCAN clustering.

    Exposes separate configurations for clustering (low-dim) and
    visualization (2D) since the optimal hyperparameters differ
    between the two use cases.

    Parameters
    ----------
    n_components : int
        Number of dimensions to reduce to for clustering. Default: 5.
        BERTopic's documentation recommends 5 as a starting point.
    n_neighbors : int
        UMAP n_neighbors parameter. Controls the balance between
        local and global structure. Default: 15. Increase for larger
        corpora to capture broader topic structure.
    min_dist : float
        Minimum distance between points in the low-dimensional space.
        Default: 0.0. Lower values produce tighter clusters, which
        helps HDBSCAN find dense regions.
    metric : str
        Distance metric for UMAP. Default: 'cosine', appropriate for
        sentence embeddings which encode direction rather than magnitude.
    random_state : int
        Random seed for reproducibility. Default: 42.
    """

    def __init__(
        self,
        n_components: int = 5,
        n_neighbors: int = 15,
        min_dist: float = 0.0,
        metric: str = "cosine",
        random_state: int = 42,
    ):
        pass

    def build(self) -> umap.UMAP:
        """
        Instantiate and return a configured UMAP object for clustering.

        Returns a UMAP instance initialized with the parameters set
        on this reducer. This object is passed directly to BERTopic's
        umap_model parameter so BERTopic can manage fitting internally.

        Returns
        -------
        umap.UMAP
            Configured UMAP instance ready for BERTopic injection.
        """
        self.umap_model = umap.UMAP(
            n_components=self.n_components,
            n_neighbors=self.n_neighbors,
            min_dist=self.min_dist,
            metric=self.metric,
            random_state=self.random_state,
        )
        return self.umap_model

    def build_2d(self, n_neighbors: int = 15) -> umap.UMAP:
        """
        Instantiate a UMAP object configured for 2D visualization.

        Uses n_components=2 and a slightly higher min_dist than the
        clustering configuration to spread points out for readability
        on the dashboard Landscape Map. Other parameters are inherited
        from the instance configuration.

        Parameters
        ----------
        n_neighbors : int
            n_neighbors for the 2D projection. Can differ from the
            clustering configuration. Default: 15.

        Returns
        -------
        umap.UMAP
            UMAP instance configured for 2D visualization output.
        """
        self.umap_model_2d = umap.UMAP(
            n_components=2,
            n_neighbors=20,
            min_dist=self.min_dist + 0.1,  # increase min_dist for visualization to spread points out
            metric=self.metric,
            random_state=self.random_state,
        )
        return self.umap_model_2d

    def fit_transform(self, embeddings) -> "np.ndarray":
        """
        Fit a UMAP model on the provided embeddings and return the
        reduced representation.

        Convenience method for fitting UMAP outside of BERTopic —
        useful for generating 2D visualization coordinates independently
        of the clustering pipeline. Uses the clustering configuration
        (n_components set on instantiation) unless called via fit_transform_2d.

        Parameters
        ----------
        embeddings : np.ndarray
            2D embedding matrix of shape (n_papers, embedding_dim)
            as returned by Embedder.encode().

        Returns
        -------
        np.ndarray
            Reduced embedding matrix of shape (n_papers, n_components).
        """
        umap_model = self.build()
        return umap_model.fit_transform(embeddings)

    def fit_transform_2d(self, embeddings) -> "np.ndarray":
        """
        Fit a 2D UMAP model and return coordinates for visualization.

        Produces the (x, y) coordinate pairs used to render the
        dashboard Landscape Map. Each paper maps to one point in
        2D space, with proximity indicating topic similarity.

        Parameters
        ----------
        embeddings : np.ndarray
            2D embedding matrix of shape (n_papers, embedding_dim).

        Returns
        -------
        np.ndarray
            2D coordinate array of shape (n_papers, 2) where each row
            is the (x, y) position of a paper in the visualization space.
        """
        umap_model_2d = self.build_2d()
        return umap_model_2d.fit_transform(embeddings)