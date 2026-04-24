"""
organizer.py
------------
Groups selected and analyzed papers into meaningful reading categories
for the dashboard's guided reading workflow.

Takes the output of SelectionEngine and FullTextAnalyzer and organizes
papers into the four categories described in Section 2.5.3:
  1. Foundational works      — high influence, widely cited
  2. Representative methods  — central to major topic clusters
  3. Emerging approaches     — recent, rapidly growing topics
  4. Contrasting perspectives — different clusters addressing similar problems

This structured presentation enables researchers to navigate from
high-level exploration to targeted deep reading without being overwhelmed
by an unstructured list of papers.

Each paper is assigned to exactly one primary category based on its
signal scores and metadata. Papers that could fit multiple categories
are assigned to the most distinctive one to maximize coverage across
categories.

Categories:
  1. Foundational works      — high influence, widely cited
  2. Representative methods  — central to major topic clusters
  3. Emerging approaches     — recent, rapidly growing topics
  4. Contrasting perspectives — different clusters addressing similar problems

Bugs fixed vs original:
  - __init__ did `pass` — min_per_category and max_per_category were never
    stored. Fixed to assign self.min_per_category and self.max_per_category.
  - _assign_categories() called _is_contrasting() with assigned_clusters=set()
    for every paper — always an empty set, so every paper scored 1.0 for
    contrasting. Fixed to build assigned_clusters incrementally as papers
    are assigned to other categories, so contrasting truly means "novel cluster".

Dependencies: scorer.py (for signal score access), numpy
"""

import logging
import numpy as np

logger = logging.getLogger(__name__)

FOUNDATIONAL  = "foundational"
REPRESENTATIVE = "representative"
EMERGING      = "emerging"
CONTRASTING   = "contrasting"

CATEGORIES = [FOUNDATIONAL, REPRESENTATIVE, EMERGING, CONTRASTING]


class PaperOrganizer:
    """
    Organizes selected papers into four guided reading categories.

    Parameters
    ----------
    min_per_category : int
        Minimum papers per category. Default: 2.
    max_per_category : int
        Maximum papers per category. Default: 10.
    """

    def __init__(self, min_per_category: int = 2, max_per_category: int = 10):
        self.min_per_category = min_per_category
        self.max_per_category = max_per_category

    def organize(
        self,
        selected_papers: list[dict],
        annotations: list[dict],
    ) -> dict:
        """
        Assign selected papers to reading categories and merge annotations.

        Parameters
        ----------
        selected_papers : list[dict]
            Papers from SelectionEngine with composite_score and signal_scores.
        annotations : list[dict]
            Annotation records from FullTextAnalyzer, one per paper.

        Returns
        -------
        dict
            Reading guide with foundational, representative, emerging,
            contrasting keys. Each value is a list of paper dicts with
            annotations and category_reason merged in.
        """
        category_assignments = self._assign_categories(selected_papers)
        merged_papers        = self._merge_annotations(selected_papers, annotations)

        reading_guide = {cat: [] for cat in CATEGORIES}
        for paper in merged_papers:
            category = category_assignments.get(paper["paper_id"])
            if category:
                paper["category_reason"] = self._build_category_reason(paper, category)
                if len(reading_guide[category]) < self.max_per_category:
                    reading_guide[category].append(paper)

        logger.info(
            "Reading guide: foundational=%d, representative=%d, emerging=%d, contrasting=%d",
            len(reading_guide[FOUNDATIONAL]),
            len(reading_guide[REPRESENTATIVE]),
            len(reading_guide[EMERGING]),
            len(reading_guide[CONTRASTING]),
        )
        return reading_guide

    def _assign_categories(self, papers: list[dict]) -> dict[str, str]:
        """
        Assign each paper to its primary reading category.

        Processes categories in priority order: foundational → representative
        → emerging → contrasting. Builds assigned_clusters incrementally so
        the contrasting check genuinely detects novel clusters rather than
        always returning 1.0.

        Parameters
        ----------
        papers : list[dict]
            Selected papers with signal_scores attached.

        Returns
        -------
        dict[str, str]
            Mapping of paper_id to assigned category label.
        """
        assignments      = {}
        assigned_clusters = set()  # grows as papers are assigned

        for paper in papers:
            f = self._is_foundational(paper)
            r = self._is_representative(paper)
            e = self._is_emerging(paper)
            c = self._is_contrasting(paper, assigned_clusters)

            scores = {
                FOUNDATIONAL:  f,
                REPRESENTATIVE: r,
                EMERGING:      e,
                CONTRASTING:   c,
            }
            best_category = max(scores, key=scores.get)
            best_score    = scores[best_category]

            if best_score > 0.5:
                assignments[paper["paper_id"]] = best_category
                # Track cluster so future papers can be flagged as contrasting
                cluster_id = paper.get("topic_cluster_id")
                if cluster_id is not None and cluster_id != -1:
                    assigned_clusters.add(cluster_id)
            else:
                assignments[paper["paper_id"]] = None

        return assignments

    def _is_foundational(self, paper: dict) -> float:
        """High influence + high citation count → foundational."""
        influence      = paper.get("signal_scores", {}).get("influence", 0.0)
        citation_count = paper.get("citation_count", 0) or 0
        citation_score = min(1.0, np.log1p(citation_count) / 10)
        return 0.7 * influence + 0.3 * citation_score

    def _is_representative(self, paper: dict) -> float:
        """High cluster representativeness → representative."""
        return paper.get("signal_scores", {}).get("representativeness", 0.0)

    def _is_emerging(self, paper: dict) -> float:
        """High recency + reasonable relevance → emerging."""
        recency   = paper.get("signal_scores", {}).get("recency", 0.0)
        relevance = paper.get("signal_scores", {}).get("relevance", 0.0)
        return 0.7 * recency + 0.3 * relevance

    def _is_contrasting(self, paper: dict, assigned_clusters: set) -> float:
        """
        Paper belongs to a cluster not yet assigned → contrasting.

        Parameters
        ----------
        paper : dict
        assigned_clusters : set
            Clusters already assigned to foundational/representative/emerging.
            Grows as _assign_categories() processes each paper.
        """
        cluster_id = paper.get("topic_cluster_id")
        if cluster_id is None or cluster_id == -1:
            return 0.0
        return 1.0 if cluster_id not in assigned_clusters else 0.0

    def _merge_annotations(
        self,
        papers: list[dict],
        annotations: list[dict],
    ) -> list[dict]:
        """
        Merge FullTextAnalyzer annotation records into paper dicts.

        Parameters
        ----------
        papers : list[dict]
        annotations : list[dict]
            Each has paper_id plus the four extraction category lists.

        Returns
        -------
        list[dict]
            Paper dicts with annotation fields merged in.
        """
        annotation_map = {ann["paper_id"]: ann for ann in annotations}
        merged = []
        for paper in papers:
            ann = annotation_map.get(paper["paper_id"], {})
            merged.append({
                **paper,
                "has_fulltext":           ann.get("has_fulltext", False),
                "methodological_details": ann.get("methodological_details", []),
                "limitations":            ann.get("limitations", []),
                "contributions":          ann.get("contributions", []),
                "evaluation_context":     ann.get("evaluation_context", []),
            })
        return merged

    def _build_category_reason(self, paper: dict, category: str) -> str:
        """Generate a human-readable explanation for category assignment."""
        topic          = paper.get("topic_cluster", "this research area")
        date           = paper.get("submitted_date", "recently")
        citation_count = paper.get("citation_count", 0) or 0
        scores         = paper.get("signal_scores", {})

        if category == FOUNDATIONAL:
            return (
                f"Widely cited foundational work in {topic} "
                f"with {citation_count} citations and high influence score "
                f"({scores.get('influence', 0):.2f})"
            )
        elif category == REPRESENTATIVE:
            return (
                f"Central to the {topic} cluster — "
                f"closely aligned with the core research direction "
                f"(representativeness: {scores.get('representativeness', 0):.2f})"
            )
        elif category == EMERGING:
            return (
                f"Recent work in {topic} published {date} "
                f"with a strong recency signal "
                f"({scores.get('recency', 0):.2f})"
            )
        elif category == CONTRASTING:
            return (
                f"Offers a distinct perspective from a different "
                f"research cluster — broadens coverage beyond {topic}"
            )
        return f"Selected for relevance to the query in {topic}"