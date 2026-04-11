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
 
This module owns stages 2-4 and exposes the fitted topic model,
per-paper topic assignments, topic labels, and cluster metadata.
 
Dependencies: bertopic, reducer.py, clusterer.py
"""
 
"""
topic_model.py — BERTopic wrapper wiring UMAP + HDBSCAN.
"""
from bertopic import BERTopic
from NLP_layer.reducer import UMAPReducer
from NLP_layer.clusterer import HDBSCANClusterer


class TopicModeler:
    def __init__(
        self,
        reducer: UMAPReducer,
        clusterer: HDBSCANClusterer,
        top_n_words: int = 10,
        nr_topics=None,
        min_topic_size: int = 10,
    ):
        self.reducer = reducer
        self.clusterer = clusterer
        self.top_n_words = top_n_words
        self.nr_topics = nr_topics
        self.min_topic_size = min_topic_size
        self.model = None
        self._paper_ids = []  # stored at fit() time for get_2d_coordinates

    def fit(self, abstracts: list[str], embeddings) -> None:
        umap_model    = self.reducer.build()
        hdbscan_model = self.clusterer.build()
        self.model = BERTopic(
            umap_model=umap_model,
            hdbscan_model=hdbscan_model,
            top_n_words=self.top_n_words,
            nr_topics=self.nr_topics,
            min_topic_size=self.min_topic_size,
        )
        self.model.fit(abstracts, embeddings)

    def get_topic_assignments(self, paper_ids: list[str]) -> list[dict]:
        self._paper_ids = paper_ids
        assignment_records = []
        for i, (paper_id, topic_id) in enumerate(zip(paper_ids, self.model.topics_)):
            if topic_id == -1:  # BERTopic outlier label is -1, not 1
                topic_label = "outlier"
                probability = None
            else:
                topic_words = self.model.get_topic(topic_id)
                topic_label = ", ".join([word for word, _ in topic_words])
                probability = float(self.model.probabilities_[i])
            assignment_records.append({
                "paper_id":   paper_id,
                "topic_id":   int(topic_id),
                "topic_label": topic_label,
                "probability": probability,
            })
        return assignment_records

    def get_topic_info(self) -> list[dict]:
        metadata_records = []
        for topic in self.model.get_topic_info().itertuples():
            topic_id = topic.Topic
            if topic_id == -1:
                continue
            topic_keywords = [word for word, _ in self.model.get_topic(topic_id)]
            topic_label = "_".join(topic.Name.split(", ")[:self.top_n_words])
            metadata_records.append({
                "topic_id":    int(topic_id),
                "topic_label": topic_label,
                "size":        int(topic.Count),
                "keywords":    topic_keywords,
            })
        return metadata_records

    def get_2d_coordinates(self, embeddings, paper_ids: list[str] = None) -> list[dict]:
        ids = paper_ids or self._paper_ids
        coords_2d = self.reducer.fit_transform_2d(embeddings)
        coord_records = []
        for i, (paper_id, topic_id) in enumerate(zip(ids, self.model.topics_)):
            coord_records.append({
                "paper_id": paper_id,
                "x":        float(coords_2d[i, 0]),
                "y":        float(coords_2d[i, 1]),
                "topic_id": int(topic_id),
            })
        return coord_records

    def save(self, path: str) -> None:
        self.model.save(path)

    def load(self, path: str) -> None:
        self.model = BERTopic.load(path)

    @property
    def is_fitted(self) -> bool:
        return self.model is not None