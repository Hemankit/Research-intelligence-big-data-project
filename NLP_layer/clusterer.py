"""
clusterer.py
------------
Wraps HDBSCAN clustering configuration for use within the BERTopic pipeline.
 
HDBSCAN is the clustering algorithm BERTopic uses to group papers into
topic clusters based on their UMAP-reduced embeddings. Isolating its
configuration here makes hyperparameter tuning straightforward — the two
parameters you will tune most are min_cluster_size and min_samples.
 
Key HDBSCAN behaviors to be aware of:
  - Papers that do not belong to any cluster are assigned label -1 (noise).
    In large academic corpora this is expected and acceptable.
  - min_cluster_size controls the minimum number of papers needed to form
    a topic. Too small → noisy micro-topics. Too large → over-merged topics.
  - min_samples controls how conservative cluster assignment is. Higher
    values produce more noise points but cleaner clusters.
 
Dependencies: hdbscan
"""
 
import hdbscan
import numpy as np
 
 
class HDBSCANClusterer:
    """
    Configures an HDBSCAN instance for injection into BERTopic.
 
    Exposes the most impactful hyperparameters as constructor arguments
    so they can be adjusted without modifying topic_model.py. Provides
    a build() method that returns a configured HDBSCAN object ready
    for BERTopic's hdbscan_model parameter.
 
    Parameters
    ----------
    min_cluster_size : int
        Minimum number of papers required to form a topic cluster.
        Default: 10. Increase for larger corpora (e.g., 50 for 200k papers)
        to avoid an excessive number of micro-topics.
    min_samples : int
        Number of samples in the neighborhood for a point to be considered
        a core point. Default: None (uses min_cluster_size value).
        Increase to make cluster assignment more conservative.
    metric : str
        Distance metric for HDBSCAN. Default: 'euclidean', appropriate
        for UMAP-reduced embeddings which no longer require cosine distance.
    cluster_selection_method : str
        Method for selecting flat clusters from the HDBSCAN hierarchy.
        'eom' (Excess of Mass, default) tends to find clusters of varying
        sizes. Use 'leaf' for more uniform cluster sizes.
    prediction_data : bool
        Whether to generate data structures for soft cluster assignment
        and approximate prediction on new points. Default: True.
        Required if you want to assign topic labels to new papers after
        fitting without re-fitting the full model.
    """
 
    def __init__(
        self,
        min_cluster_size: int = 10,
        min_samples: int = None,
        metric: str = "euclidean",
        cluster_selection_method: str = "eom",
        prediction_data: bool = True,
    ):
        self.min_cluster_size = min_cluster_size
        self.min_samples = min_samples
        self.metric = metric
        self.cluster_selection_method = cluster_selection_method
        self.prediction_data = prediction_data
 
    def build(self) -> hdbscan.HDBSCAN:
        """
        Instantiate and return a configured HDBSCAN object.
 
        Returns an HDBSCAN instance initialized with the parameters
        set on this clusterer. This object is passed directly to
        BERTopic's hdbscan_model parameter so BERTopic manages fitting.
 
        Returns
        -------
        hdbscan.HDBSCAN
            Configured HDBSCAN instance ready for BERTopic injection.
        """
        HDBSCAN_obj = hdbscan.HDBSCAN(
            min_cluster_size=self.min_cluster_size,
            min_samples=self.min_samples,
            metric=self.metric,
            cluster_selection_method=self.cluster_selection_method,
            prediction_data=self.prediction_data,
        )
        return HDBSCAN_obj
 
    def fit(self, reduced_embeddings) -> "np.ndarray":
        """
        Fit HDBSCAN on reduced embeddings and return cluster labels.
 
        Convenience method for fitting HDBSCAN outside of BERTopic —
        useful for experimentation and hyperparameter tuning independently
        of the full topic model pipeline.
 
        Parameters
        ----------
        reduced_embeddings : np.ndarray
            UMAP-reduced embedding matrix of shape (n_papers, n_components)
            as returned by UMAPReducer.fit_transform().
 
        Returns
        -------
        np.ndarray
            Integer cluster label array of shape (n_papers,).
            Papers assigned to noise receive label -1.
        """
        # build the HDBSCAN model using the configured parameters
        clusterer = self.build()
        # fit the model to the reduced embeddings and get cluster labels
        cluster_labels = clusterer.fit_predict(reduced_embeddings)
        return cluster_labels
 
    def noise_ratio(self, labels) -> float:
        """
        Compute the fraction of papers assigned to noise (label -1).
 
        Useful for evaluating the impact of hyperparameter choices.
        A very high noise ratio (>30%) suggests min_cluster_size or
        min_samples may be too large for the corpus. A very low noise
        ratio (<1%) may indicate over-clustering.
 
        Parameters
        ----------
        labels : np.ndarray
            Cluster label array as returned by fit().
 
        Returns
        -------
        float
            Fraction of papers with label -1, between 0.0 and 1.0.
        """
        return np.mean(labels == -1)
 
    def n_clusters(self, labels) -> int:
        """
        Return the number of clusters found (excluding noise label -1).
 
        Parameters
        ----------
        labels : np.ndarray
            Cluster label array as returned by fit().
 
        Returns
        -------
        int
            Number of distinct topic clusters discovered.
        """
        return len(set(labels)) - (1 if -1 in labels else 0)