"""
selector.py
-----------
Selection Engine for the Selective Full-Text Analysis Layer.

Given a user query, retrieves candidate papers from Elasticsearch
and Hive, scores them using the five signals defined in scorer.py,
and returns a curated subset of 10-50 high-value papers for targeted
full-text analysis.

The selection process runs entirely on-demand — it is triggered by
a user query from the dashboard and completes in seconds since it
operates on pre-computed metadata signals rather than raw text.

Selection pipeline:
  1. Query Elasticsearch for relevance candidates + raw ES scores
  2. Fetch enriched metadata (PageRank, UMAP coords) from Hive for candidates
  3. Score candidates on all five signals via scorer.py
  4. Iteratively select papers using composite scores with diversity reranking
  5. Return the final curated set with their scores and selection rationale

The diversity reranking in step 4 is iterative — after each paper is
selected, diversity scores are recomputed for remaining candidates to
reflect the current selection state. This greedy approach approximates
maximum marginal relevance (MMR) selection without the full MMR overhead.

Dependencies: scorer.py, elasticsearch (via ESClient), pyspark or direct
              Hive JDBC for metadata enrichment
"""

import logging
from elasticsearch.client import ESClient
from elasticsearch.mappings import PAPERS_INDEX
from .scorer import (
    score_relevance,
    score_influence,
    score_recency,
    score_representativeness,
    score_diversity,
    combine_scores,
)

logger = logging.getLogger(__name__)


class SelectionEngine:
    """
    Selects a curated subset of high-value papers for targeted full-text
    analysis based on a user query and composite signal scoring.

    Retrieves candidates from Elasticsearch, enriches with Hive metadata,
    scores on five signals, and returns the top-ranked diverse subset.

    Parameters
    ----------
    es_client : ESClient
        Connected ESClient instance for querying the papers index.
    hive_conn : object
        Hive connection or SparkSession for fetching enriched metadata
        (PageRank scores, UMAP coordinates) not stored in Elasticsearch.
    max_candidates : int
        Maximum number of Elasticsearch hits to consider before scoring.
        Default: 200. Higher values improve selection quality at the cost
        of scoring overhead.
    target_count : int
        Number of papers to return in the final curated set.
        Default: 20. Must be between 10 and 50 per the proposal spec.
    score_weights : dict[str, float], optional
        Custom signal weights forwarded to scorer.combine_scores().
        Default: None (use scorer defaults).
    """

    def __init__(
        self,
        es_client: ESClient,
        hive_conn,
        max_candidates: int = 200,
        target_count: int = 20,
        score_weights: dict = None,
    ):
        self.es_client = es_client
        self.hive_conn = hive_conn
        self.max_candidates = max_candidates
        self.target_count = target_count
        self.score_weights = score_weights or {}

    def select(self, query: str) -> list[dict]:
        """
        Run the full selection pipeline for a user query.

        Primary entry point called by run.py when a user submits a query.
        Orchestrates the full pipeline: retrieve → enrich → score → select.
        Returns a ranked list of selected paper dicts with scores attached.

        Parameters
        ----------
        query : str
            Natural language query from the dashboard, e.g.
            'emerging trends in graph neural networks'.

        Returns
        -------
        list[dict]
            Curated list of selected paper dicts, each containing all
            original paper fields plus:
            - composite_score (float): Final ranking score
            - signal_scores (dict): Individual signal scores for transparency
            - selection_reason (str): Human-readable rationale for selection
        """
        # query elasticsearch for candidates and raw scores
        candidates, es_scores = self._retrieve_candidates(query)
        # enrich candidates with Hive metadata
        enriched_candidates = self._enrich_with_hive(candidates)
        # score candidates on all signals
        scored_candidates = self._score_candidates(enriched_candidates, es_scores)
        # select top papers with diversity reranking
        selected_papers = self._select_with_diversity(scored_candidates)
        return selected_papers

    def _retrieve_candidates(self, query: str) -> tuple[list[dict], dict[str, float]]:
        """
        Query Elasticsearch for candidate papers and return raw ES scores.

        Constructs a multi-field Elasticsearch query targeting title,
        abstract, topic_cluster, methods, datasets, and tasks fields.
        Retrieves up to max_candidates hits and returns both the paper
        records and a mapping of paper_id to raw ES _score.

        Parameters
        ----------
        query : str
            User query string to search against Elasticsearch.

        Returns
        -------
        tuple[list[dict], dict[str, float]]
            - candidates: List of paper dicts from ES hits
            - es_scores: Dict mapping paper_id to raw Elasticsearch _score
        """
        # constructing the ES query with multi-match on relevant fields
        es_query = {
            "size": self.max_candidates,
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": [
                        "title", 
                        "abstract",  
                        "topic_cluster",
                        "methods",
                        "datasets",
                        "tasks",
                    ],
                    "type": "best_fields",
                    "operator": "or",
                }
            }
        }
        # execute the search query and extract candidates and their ES scores
        response = self.es_client.client.search(index=PAPERS_INDEX, body=es_query)
        hits = response.get("hits", {}).get("hits", [])
        candidates = [hit["_source"] for hit in hits]
        es_scores = {hit["_source"]["paper_id"]: hit["_score"] for hit in hits}
        return candidates, es_scores

    def _enrich_with_hive(self, candidates: list[dict]) -> list[dict]:
        """
        Enrich candidate paper records with fields not stored in Elasticsearch.

        Fetches PageRank scores from the pagerank_scores Hive table and
        attaches them to the candidate records. UMAP coordinates are already
        present in the Elasticsearch papers index so no additional fetch
        is needed for those.

        Parameters
        ----------
        candidates : list[dict]
            Candidate paper records from Elasticsearch, each containing
            at minimum a 'paper_id' field.

        Returns
        -------
        list[dict]
            Same candidate list with 'pagerank_score' field added to
            each record. Papers not found in the pagerank_scores table
            receive pagerank_score of 0.0.
        """
        # fetching pagerank scores from pagerank_scores Hive table for all candidate paper_ids
        paper_ids = [c["paper_id"] for c in candidates]
        # construct and execute Hive query to get pagerank scores for these paper_ids
        query = f"""SELECT paper_id, pagerank_score FROM pagerank_scores WHERE paper_id IN ({','.join(f"'{pid}'" for pid in paper_ids)})"""
        hive_results = self.hive_conn.execute(query).fetchall()
        pagerank_dict = {row.paper_id: row.pagerank_score for row in hive_results}
        # attach pagerank scores to candidate records, defaulting to 0.0 if not found
        for candidate in candidates:
            candidate["pagerank_score"] = pagerank_dict.get(candidate["paper_id"], 0.0)
        return candidates

    def _score_candidates(
        self, candidates: list[dict], es_scores: dict[str, float]
    ) -> list[dict]:
        """
        Compute all five signal scores for each candidate paper.

        Calls each scorer function from scorer.py and attaches the
        individual signal scores to each candidate dict for transparency
        and downstream use by _select_with_diversity().

        Parameters
        ----------
        candidates : list[dict]
            Enriched candidate paper records.
        es_scores : dict[str, float]
            Raw Elasticsearch scores from _retrieve_candidates().

        Returns
        -------
        list[dict]
            Same candidate list with a 'signal_scores' dict added to
            each record containing keys: relevance, influence, recency,
            representativeness, diversity (initial, pre-selection).
        """
        # compute all signal scores for each candidate and attach to the record
        for candidate in candidates:
            paper_id = candidate["paper_id"]
            signal_scores = {
                "relevance": score_relevance(candidate, es_scores.get(paper_id, 0.0)),
                "influence": score_influence(candidate),
                "recency": score_recency(candidate),
                "representativeness": score_representativeness(candidate),
                # initial diversity is computed before any papers are selected
                "diversity": score_diversity(candidate, selected_papers=[]),
            }
            candidate["signal_scores"] = signal_scores
        return candidates

    def _select_with_diversity(self, scored_candidates: list[dict]) -> list[dict]:
        """
        Greedily select target_count papers using iterative diversity reranking.

        Implements an approximation of Maximum Marginal Relevance (MMR):
        at each step, recomputes diversity scores based on already-selected
        papers, combines with other signals into a composite score, and
        selects the highest-scoring remaining candidate.

        This greedy approach ensures that selected papers cover multiple
        topic clusters and methodological approaches rather than
        clustering around a single highly-relevant topic.

        Parameters
        ----------
        scored_candidates : list[dict]
            Candidates with signal_scores attached by _score_candidates().

        Returns
        -------
        list[dict]
            Final selected papers in ranked order, each with
            'composite_score' and updated 'signal_scores' (including
            final diversity score) attached.
        """
        final_papers = []
        remaining_candidates = scored_candidates.copy()
        for rank in range(1, self.target_count + 1):
            # recompute diversity scores based on current selection
            for candidate in remaining_candidates:
                candidate["signal_scores"]["diversity"] = score_diversity(candidate, final_papers)
                # combine all signals into a composite score for ranking
                candidate["composite_score"] = combine_scores(candidate["signal_scores"], self.score_weights)
            # select the candidate with the highest composite score
            best_candidate = max(remaining_candidates, key=lambda c: c["composite_score"])
            best_candidate["selection_reason"] = self._assign_selection_reason(best_candidate, rank)
            final_papers.append(best_candidate)
            remaining_candidates.remove(best_candidate)
        return final_papers

    def _assign_selection_reason(self, paper: dict, rank: int) -> str:
        """
        Generate a short human-readable rationale for why a paper was selected.

        Used by the dashboard to explain selection decisions to the user.
        The rationale is based on which signal contributed most strongly
        to the paper's composite score, enriched with topic-specific details.

        Parameters
        ----------
        paper : dict
            Selected paper dict with signal_scores attached.
        rank : int
            Selection rank (1 = highest composite score).

        Returns
        -------
        str
            Short rationale string, e.g.:
            'Highly relevant to query and widely cited foundational work'
            'Recent emerging approach in graph neural networks'
            'Representative of the transformer architectures cluster'
        """
        scores = paper["signal_scores"]
        topic = paper.get("topic_cluster", "this research area")
    
        # find the dominant signal
        dominant = max(scores, key=scores.get)
    
        reasons = {
        "relevance": f"Highly relevant to your query and central to {topic}",
        "influence": f"Widely cited foundational work in {topic} "
                     f"(citation score: {scores['influence']:.2f})",
        "recency":   f"Recent emerging approach in {topic} "
                     f"published {paper.get('submitted_date', 'recently')}",
        "representativeness": f"Representative of the {topic} cluster",
        "diversity": f"Provides a distinct perspective not covered "
                     f"by other selected papers",
    }
    
        base_reason = reasons[dominant]
    
    # append rank context for top papers
        if rank == 1:
            return f"Top match — {base_reason}"
        return base_reason