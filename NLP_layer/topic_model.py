"""
topic_model.py
--------------
Core BERTopic wrapper that wires together the custom UMAP and HDBSCAN
instances from reducer.py and clusterer.py to discover latent research
clusters across the full abstract corpus.
 
BERTopic works in three stages internally:
  1. Embed documents (handled externally by embedder.py — pre-computed
     embeddings are passed in directly to skip recomputation)
  2. Reduce dimensionality with UMAP (injected from reducer.py)
  3. Cluster with HDBSCAN (injected from clusterer.py)
  4. Generate topic representations using c-TF-IDF over cluster documents
 
This module owns stages 2–4 and exposes the fitted topic model,
per-paper topic assignments, topic labels, and cluster metadata.
 
Dependencies: bertopic, reducer.py, clusterer.py
"""
 
from bertopic import BERTopic
from NLP_layer.reducer import UMAPReducer
from NLP_layer.clusterer import HDBSCANClusterer
 
 
class TopicModeler:
    """
    Fits a BERTopic model on the full abstract corpus using custom
    UMAP and HDBSCAN components from reducer.py and clusterer.py.
 
    Accepts pre-computed embeddings from Embedder so that the expensive
    encoding step is never repeated across clustering experiments. Exposes
    the fitted model, topic assignments, and topic metadata for downstream
    output and dashboard use.
 
    Parameters
    ----------
    reducer : UMAPReducer
        Configured UMAPReducer instance. Its build() method is called
        to produce the UMAP object injected into BERTopic.
    clusterer : HDBSCANClusterer
        Configured HDBSCANClusterer instance. Its build() method is called
        to produce the HDBSCAN object injected into BERTopic.
    top_n_words : int
        Number of words per topic to include in topic representations.
        Default: 10. These words form the human-readable topic labels.
    nr_topics : int or str, optional
        Number of topics to reduce to after initial fitting. Pass an
        integer to merge down to a fixed number, or 'auto' to let
        BERTopic decide. Default: None (no reduction).
    min_topic_size : int
        Minimum number of documents in a topic. Passed to BERTopic
        directly. Default: 10.
    """
 
    def __init__(
        self,
        reducer: UMAPReducer,
        clusterer: HDBSCANClusterer,
        top_n_words: int = 10,
        nr_topics=None,
        min_topic_size: int = 10,
    ):
        pass
 
    def fit(self, abstracts: list[str], embeddings) -> None:
        """
        Fit the BERTopic model on the full abstract corpus.
 
        Passes pre-computed embeddings directly to BERTopic to skip
        the internal embedding step. BERTopic then runs UMAP reduction,
        HDBSCAN clustering, and c-TF-IDF topic representation generation.
 
        Stores the fitted model and results on the instance for access
        via the property methods below.
 
        Parameters
        ----------
        abstracts : list[str]
            Full list of cleaned abstract strings. Must be in the same
            order as the rows in the embeddings matrix.
        embeddings : np.ndarray
            Pre-computed embedding matrix of shape (n_papers, embedding_dim)
            as returned by Embedder.encode().
        """
        # initalize umap reducer
        umap_model = self.reducer.build()
        # initalize hdbscan clusterer
        hdbscan_model = self.clusterer.build()
        # initialize BERTopic with the custom UMAP and HDBSCAN models
        self.model = BERTopic(
            umap_model=umap_model,
            hdbscan_model=hdbscan_model,
            top_n_words=self.top_n_words,
            nr_topics=self.nr_topics,
            min_topic_size=self.min_topic_size,
        )
        self.model.fit(abstracts, embeddings)

    def get_topic_assignments(self, paper_ids: list[str]) -> list[dict]:
        """
        Return per-paper topic assignments linked to paper IDs.
 
        Pairs the topic label assigned to each document (by BERTopic's
        internal fit) with its corresponding paper ID. Papers assigned
        to noise (topic -1) are included with label 'outlier'.
 
        Parameters
        ----------
        paper_ids : list[str]
            List of paper IDs in the same order as the abstracts passed
            to fit(). paper_ids[i] corresponds to topic assignment i.
 
        Returns
        -------
        list[dict]
            Per-paper assignment records, each with keys:
            - paper_id (str): Paper identifier
            - topic_id (int): BERTopic topic index (-1 for noise)
            - topic_label (str): Human-readable topic label or 'outlier'
            - probability (float): Assignment confidence score
        """
        assignment_records = []
        for i, (paper_id, topic_id) in enumerate(zip(paper_ids, self.model.topics_)):
            if topic_id == 1:
                topic_label = "outlier"
                probability = None # no topic representation for outliers, so confidence is undefined
            else:
                topic_words = self.model.get_topic(topic_id)
                topic_label = ", ".join([word for word, _ in topic_words]) # join top words into a label string e.g "deep learning, neural network, transformer"
                probability = self.model.probabilities_[i]
            assignment_records.append({
                "paper_id": paper_id,
                "topic_id": topic_id,
                "topic_label": topic_label,
                "probability": probability,
            })
        return assignment_records
 
    def get_topic_info(self) -> list[dict]:
        """
        Return metadata for all discovered topics.
 
        Wraps BERTopic's get_topic_info() output into a list of dicts
        for easy serialization. Includes topic size (number of papers),
        top keywords, and the auto-generated topic label.
 
        Returns
        -------
        list[dict]
            One record per topic with keys:
            - topic_id (int): BERTopic topic index
            - topic_label (str): Top keywords joined as a label string
            - size (int): Number of papers in this topic
            - keywords (list[str]): Top n words for this topic
        """
        metadata_records = []
        for topic in self.model.get_topic_info().itertuples():
            topic_id = topic.Topic
            if topic_id == -1:
                continue # skip the outlier topic
            topic_size = topic.Count
            topic_keywords = [word for word, score in self.model.get_topic(topic_id)] # get the top words for this topic as a list of strings
            topic_label = "_".join(topic.Name.split(", ")[:self.top_n_words]) # use the top n words as the topic label
            metadata_records.append({
                "topic_id": topic_id,
                "topic_label": topic_label,
                "size": topic_size,
                "keywords": topic_keywords,
            })
        return metadata_records
 
    def get_2d_coordinates(self, embeddings) -> list[dict]:
        """
        Generate 2D UMAP coordinates for the dashboard Landscape Map.
 
        Calls UMAPReducer.fit_transform_2d() on the full embeddings to
        produce (x, y) coordinates for visualization. Each paper maps
        to one point, colored by its topic assignment on the dashboard.
 
        Parameters
        ----------
        embeddings : np.ndarray
            Full embedding matrix of shape (n_papers, embedding_dim).
 
        Returns
        -------
        list[dict]
            Coordinate records, each with keys:
            - paper_id (str): Paper identifier (requires paper_ids set)
            - x (float): UMAP x coordinate
            - y (float): UMAP y coordinate
            - topic_id (int): Topic assignment for color coding
        """
        coord_records = []
        coords_2d = self.reducer.fit_transform_2d(embeddings) # get the 2D coordinates for all papers
        for i, (paper_id, topic_id) in enumerate(zip(self.model._documents, self.model.topics_)):
            coord_records.append({
                "paper_id": paper_id,
                "x": coords_2d[i, 0],
                "y": coords_2d[i, 1],
                "topic_id": topic_id,
            })
        return coord_records
 
    def save(self, path: str) -> None:
        """
        Serialize the fitted BERTopic model to disk.
 
        Uses BERTopic's built-in save() method to write the full model
        (including UMAP and HDBSCAN states) to the specified path.
        The saved model can be reloaded for inference on new papers
        without re-fitting.
 
        Parameters
        ----------
        path : str
            Local directory path where the model will be saved.
        """
        self.model.save(path)
 
    def load(self, path: str) -> None:
        """
        Load a previously saved BERTopic model from disk.
 
        Restores the fitted model state so that get_topic_assignments()
        and get_topic_info() can be called without re-fitting.
 
        Parameters
        ----------
        path : str
            Local directory path from which to load the saved model.
        """
        self.model = BERTopic.load(path)
 
    @property
    def is_fitted(self) -> bool:
        """
        Return True if the model has been successfully fitted.
 
        Used as a guard in get_topic_assignments() and get_topic_info()
        to fail fast with a clear error if called before fit().
 
        Returns
        -------
        bool
            True if the BERTopic model has been fitted.
        """
        return hasattr(self, "model") and self.model is not None