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

Bugs fixed vs original:
  - All scorer functions expect list[dict] and return list[float].
    Original called them with single paper dicts and single floats.
    Fixed to score all candidates at once then attach per-paper scores.
  - combine_scores() expects five separate lists, not a dict.
    Fixed the call in _select_with_diversity().
  - score_diversity() argument renamed from selected_papers to selected_ids.
  - _enrich_with_hive() used hive_conn.execute() directly — pyhive
    requires a cursor. Fixed to use cursor.execute() + fetchall().
  - Imports updated from 'elasticsearch.*' to 'es.*' (folder was renamed).

Dependencies: scorer.py, es/client.py, es/mappings.py, pyhive
"""

import logging
from pyhive import hive as pyhive_hive

from es.client import ESClient
from es.mappings import PAPERS_INDEX
from .scorer import (
    score_relevance,
    score_influence,
    score_recency,
    score_representativeness,
    score_diversity,
    combine_scores,
)

logger = logging.getLogger(__name__)

HIVE_HOST = "localhost"
HIVE_PORT = 10000
HIVE_DATABASE = "research_intel"


def _get_hive_connection():
    """Open a pyhive connection to HiveServer2."""
    return pyhive_hive.Connection(
        host=HIVE_HOST,
        port=HIVE_PORT,
        database=HIVE_DATABASE,
        auth="NONE",
    )


class SelectionEngine:
    """
    Selects a curated subset of high-value papers for targeted full-text
    analysis based on a user query and composite signal scoring.

    Parameters
    ----------
    es_client : ESClient
        Connected ESClient instance for querying the papers index.
    hive_conn : pyhive.hive.Connection, optional
        Hive connection. If None, a new connection is created per call.
    max_candidates : int
        Maximum Elasticsearch hits to consider. Default: 200.
    target_count : int
        Number of papers to return. Default: 20.
    score_weights : dict[str, float], optional
        Custom signal weights. Default: None (use scorer defaults).
    """

    def __init__(
        self,
        es_client: ESClient,
        hive_conn=None,
        max_candidates: int = 200,
        target_count: int = 20,
        score_weights: dict = None,
    ):
        self.es_client     = es_client
        self.hive_conn     = hive_conn
        self.max_candidates = max_candidates
        self.target_count  = target_count
        self.score_weights = score_weights or {}

    def select(self, query: str) -> list[dict]:
        """
        Run the full selection pipeline for a user query.

        Parameters
        ----------
        query : str
            Natural language query from the dashboard.

        Returns
        -------
        list[dict]
            Curated list of selected paper dicts with composite_score,
            signal_scores, and selection_reason attached.
        """
        candidates, es_scores    = self._retrieve_candidates(query)
        enriched                 = self._enrich_with_hive(candidates)
        scored                   = self._score_candidates(enriched, es_scores)
        return self._select_with_diversity(scored)

    def _retrieve_candidates(self, query: str) -> tuple[list[dict], dict[str, float]]:
        """
        Query Elasticsearch for candidate papers and raw ES scores.

        Returns
        -------
        tuple[list[dict], dict[str, float]]
            (candidates, es_scores mapping paper_id -> raw _score)
        """
        es_query = {
            "size": self.max_candidates,
            "query": {
                "multi_match": {
                    "query":    query,
                    "fields":   ["title", "abstract", "topic_cluster",
                                 "methods", "datasets", "tasks"],
                    "type":     "best_fields",
                    "operator": "or",
                }
            }
        }
        response   = self.es_client.client.search(index=PAPERS_INDEX, body=es_query)
        hits       = response.get("hits", {}).get("hits", [])
        candidates = [hit["_source"] for hit in hits]
        es_scores  = {hit["_source"]["paper_id"]: hit["_score"] for hit in hits}
        logger.info("Retrieved %d candidates from Elasticsearch for query: '%s'", len(candidates), query)
        return candidates, es_scores

    def _enrich_with_hive(self, candidates: list[dict]) -> list[dict]:
        """
        Enrich candidates with PageRank scores from the Hive pagerank_scores table.

        Uses a pyhive cursor so connection management is explicit and correct.

        Parameters
        ----------
        candidates : list[dict]
            Candidate records from Elasticsearch.

        Returns
        -------
        list[dict]
            Same list with 'pagerank_score' field added to each record.
        """
        if not candidates:
            return candidates

        paper_ids = [c["paper_id"] for c in candidates]
        ids_str   = ",".join(f"'{pid}'" for pid in paper_ids)

        conn   = self.hive_conn or _get_hive_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT paper_id, pagerank_score FROM pagerank_scores "
                f"WHERE paper_id IN ({ids_str})"
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
            if self.hive_conn is None:
                conn.close()

        pagerank_map = {row[0]: row[1] for row in rows}
        for candidate in candidates:
            candidate["pagerank_score"] = pagerank_map.get(candidate["paper_id"], 0.15)
        return candidates

    def _score_candidates(
        self,
        candidates: list[dict],
        es_scores: dict[str, float],
    ) -> list[dict]:
        """
        Compute all five signal scores for each candidate paper.

        All scorer functions accept list[dict] and return list[float].
        Scores all candidates at once then attaches per-paper scores.

        Parameters
        ----------
        candidates : list[dict]
            Enriched candidate records.
        es_scores : dict[str, float]
            Raw Elasticsearch scores.

        Returns
        -------
        list[dict]
            Candidates with 'signal_scores' dict attached to each.
        """
        # Score all candidates at once — scorers expect list[dict] not single dicts
        rel  = score_relevance(candidates, es_scores)
        inf  = score_influence(candidates)
        rec  = score_recency(candidates)
        rep  = score_representativeness(candidates)
        div  = score_diversity(candidates, selected_ids=[])  # initial: nothing selected yet

        for i, candidate in enumerate(candidates):
            candidate["signal_scores"] = {
                "relevance":          rel[i],
                "influence":          inf[i],
                "recency":            rec[i],
                "representativeness": rep[i],
                "diversity":          div[i],
            }
        return candidates

    def _select_with_diversity(self, scored_candidates: list[dict]) -> list[dict]:
        """
        Greedily select target_count papers using iterative diversity reranking.

        Approximates Maximum Marginal Relevance (MMR): after each selection,
        recomputes diversity scores for remaining candidates and combines
        all signals into a fresh composite score.

        Parameters
        ----------
        scored_candidates : list[dict]
            Candidates with signal_scores attached.

        Returns
        -------
        list[dict]
            Final selected papers in ranked order with composite_score
            and updated signal_scores.
        """
        final_papers       = []
        remaining          = scored_candidates.copy()
        selected_ids       = []

        for rank in range(1, self.target_count + 1):
            if not remaining:
                break

            # Recompute diversity scores based on current selection state
            div = score_diversity(remaining, selected_ids=selected_ids)
            for j, candidate in enumerate(remaining):
                candidate["signal_scores"]["diversity"] = div[j]

            # Combine all signals into composite scores for this round
            s_rel  = [c["signal_scores"]["relevance"]          for c in remaining]
            s_inf  = [c["signal_scores"]["influence"]          for c in remaining]
            s_rec  = [c["signal_scores"]["recency"]            for c in remaining]
            s_rep  = [c["signal_scores"]["representativeness"] for c in remaining]
            s_div  = [c["signal_scores"]["diversity"]          for c in remaining]

            composite = combine_scores(
                s_rel, s_inf, s_rec, s_rep, s_div,
                weights=self.score_weights or None,
            )
            for j, candidate in enumerate(remaining):
                candidate["composite_score"] = composite[j]

            # Select highest-scoring candidate
            best = max(remaining, key=lambda c: c["composite_score"])
            best["selection_reason"] = self._assign_selection_reason(best, rank)
            final_papers.append(best)
            selected_ids.append(best["paper_id"])
            remaining.remove(best)

        logger.info("Selected %d papers from %d candidates", len(final_papers), len(scored_candidates))
        return final_papers

    def _assign_selection_reason(self, paper: dict, rank: int) -> str:
        """
        Generate a short human-readable rationale for why a paper was selected.

        Parameters
        ----------
        paper : dict
            Selected paper with signal_scores attached.
        rank : int
            Selection rank (1 = highest composite score).

        Returns
        -------
        str
            Short rationale string for dashboard display.
        """
        scores = paper.get("signal_scores", {})
        topic  = paper.get("topic_cluster", "this research area")

        dominant = max(scores, key=scores.get) if scores else "relevance"

        reasons = {
            "relevance":          f"Highly relevant to your query and central to {topic}",
            "influence":          f"Widely cited foundational work in {topic} "
                                  f"(citation score: {scores.get('influence', 0):.2f})",
            "recency":            f"Recent emerging approach in {topic} "
                                  f"published {paper.get('submitted_date', 'recently')}",
            "representativeness": f"Representative of the {topic} cluster",
            "diversity":          f"Provides a distinct perspective not covered by other selected papers",
        }

        base = reasons.get(dominant, f"Selected for relevance to the query in {topic}")
        return f"Top match — {base}" if rank == 1 else base