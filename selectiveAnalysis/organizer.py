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

Dependencies: scorer.py (for signal score access)
"""


import logging
import numpy as np

logger = logging.getLogger(__name__)

# Category label constants
FOUNDATIONAL      = "foundational"
REPRESENTATIVE    = "representative"
EMERGING          = "emerging"
CONTRASTING       = "contrasting"

CATEGORIES = [FOUNDATIONAL, REPRESENTATIVE, EMERGING, CONTRASTING]


class PaperOrganizer:
    """
    Organizes selected papers into four guided reading categories
    based on their signal scores and metadata.

    Designed to run after SelectionEngine.select() and
    FullTextAnalyzer.analyze() have completed. Takes the enriched
    paper records (with composite scores and annotations) and produces
    a structured reading guide for the dashboard.

    Parameters
    ----------
    min_per_category : int
        Minimum number of papers to include per category. If a category
        has fewer candidates than this threshold, the next-best papers
        from the scored pool are assigned to fill it. Default: 2.
    max_per_category : int
        Maximum number of papers per category. Default: 10.
    """

    def __init__(self, min_per_category: int = 2, max_per_category: int = 10):
        pass

    def organize(
        self,
        selected_papers: list[dict],
        annotations: list[dict],
    ) -> dict:
        """
        Assign selected papers to reading categories and merge annotations.

        Primary entry point called by run.py. Assigns each paper to a
        primary category, merges annotation records from FullTextAnalyzer,
        and returns a structured reading guide dict.

        Parameters
        ----------
        selected_papers : list[dict]
            Papers from SelectionEngine.select() with composite_score
            and signal_scores attached.
        annotations : list[dict]
            Annotation records from FullTextAnalyzer.analyze(), one per
            paper, keyed by paper_id.

        Returns
        -------
        dict
            Structured reading guide with keys for each category:
            - foundational (list[dict]): High-influence papers
            - representative (list[dict]): Cluster-central papers
            - emerging (list[dict]): Recent high-growth papers
            - contrasting (list[dict]): Diverse-cluster papers
            Each paper dict includes original fields, composite score,
            signal scores, annotations, and a category_reason string.
        """
        # assign the selected papers to their primary category based on signal scores
        category_assignments = self._assign_categories(selected_papers)
        # merge the FullTextAnalyzer annotations into the paper dicts
        merged_papers = self._merge_annotations(selected_papers, annotations)
        # build the final reading guide dict with category keys
        reading_guide = {category: [] for category in CATEGORIES}
        for paper in merged_papers:
            paper_id = paper["paper_id"]
            category = category_assignments.get(paper_id)
            if category:
                # generate a reason string for this paper's category assignment
                reason = self._build_category_reason(paper, category)
                paper["category_reason"] = reason
                reading_guide[category].append(paper)
        return reading_guide

    def _assign_categories(self, papers: list[dict]) -> dict[str, str]:
        """
        Assign each paper to its primary reading category.

        Uses a priority-based assignment: each paper is evaluated for
        all four categories and assigned to the one where it scores
        highest relative to its peers. Assignment is done in category
        priority order (foundational → representative → emerging →
        contrasting) to ensure the most distinctive papers get their
        most appropriate category.

        Parameters
        ----------
        papers : list[dict]
            Selected papers with signal_scores attached.

        Returns
        -------
        dict[str, str]
            Mapping of paper_id to assigned category label string.
        """
        for paper in papers:
            # check scores for each category and assign to the best one
            foundational_score = self._is_foundational(paper)
            representative_score = self._is_representative(paper)
            emerging_score = self._is_emerging(paper)
            contrasting_score = self._is_contrasting(paper, assigned_clusters=set())
            # determine the best category based on scores and thresholds
            max_score = max(foundational_score, representative_score, emerging_score, contrasting_score)
            if max_score == foundational_score and foundational_score > 0.5:
                paper["assigned_category"] = FOUNDATIONAL
            elif max_score == representative_score and representative_score > 0.5:
                paper["assigned_category"] = REPRESENTATIVE
            elif max_score == emerging_score and emerging_score > 0.5:
                paper["assigned_category"] = EMERGING
            elif max_score == contrasting_score and contrasting_score > 0.5:
                paper["assigned_category"] = CONTRASTING
            else:
                paper["assigned_category"] = None  # does not strongly fit any category
        return {paper["paper_id"]: paper["assigned_category"] for paper in papers}

    def _is_foundational(self, paper: dict) -> float:
        """
        Compute a foundational suitability score for a paper.

        A paper is foundational if it has high influence (PageRank)
        and high citation count. Returns a score in [0.0, 1.0] where
        higher means more suitable for the foundational category.

        Parameters
        ----------
        paper : dict
            Paper dict with signal_scores and citation_count fields.

        Returns
        -------
        float
            Foundational suitability score.
        """
        influence_score = paper.get("signal_scores", {}).get("influence", 0.0)
        citation_count = paper.get("citation_count", 0)
        # simple heuristic: combine influence and log-scaled citation count
        citation_score = min(1.0, np.log1p(citation_count) / 10)  # log scale with cap
        # weight influence more heavily than raw citations for foundational score
        foundational_score = 0.7 * influence_score + 0.3 * citation_score
        return foundational_score

    def _is_representative(self, paper: dict) -> float:
        """
        Compute a representative suitability score for a paper.

        A paper is representative if it has high cluster representativeness
        score (close to its BERTopic cluster centroid). Returns a score
        in [0.0, 1.0].

        Parameters
        ----------
        paper : dict
            Paper dict with signal_scores containing representativeness.

        Returns
        -------
        float
            Representative suitability score.
        """
        # representativeness_score is a signal computed by scorer.py based on distance to cluster centroid in embedding space
        representativeness_score = paper.get("signal_scores", {}).get("representativeness", 0.0)
        return representativeness_score

    def _is_emerging(self, paper: dict) -> float:
        """
        Compute an emerging suitability score for a paper.

        A paper is emerging if it has high recency score and reasonable
        relevance. Combines recency and relevance signals with recency
        weighted more heavily. Returns a score in [0.0, 1.0].

        Parameters
        ----------
        paper : dict
            Paper dict with signal_scores containing recency and relevance.

        Returns
        -------
        float
            Emerging suitability score.
        """
        recency_score = paper.get("signal_scores", {}).get("recency", 0.0)
        relevance_score = paper.get("signal_scores", {}).get("relevance", 0.0)
        # weight recency more heavily than relevance for emerging score
        emerging_score = 0.7 * recency_score + 0.3 * relevance_score
        return emerging_score

    def _is_contrasting(self, paper: dict, assigned_clusters: set) -> float:
        """
        Compute a contrasting suitability score for a paper.

        A paper is contrasting if it belongs to a topic cluster not yet
        well represented in the other categories. Returns a higher score
        for papers in underrepresented clusters.

        Parameters
        ----------
        paper : dict
            Paper dict with topic_cluster_id field.
        assigned_clusters : set
            Set of topic_cluster_ids already assigned to other categories.
            Papers in clusters not in this set score higher.

        Returns
        -------
        float
            Contrasting suitability score.
        """
        cluster_id = paper.get("topic_cluster_id")
        if cluster_id is None:
            return 0.0
        return 1.0 if cluster_id not in assigned_clusters else 0.0

    def _merge_annotations(
        self,
        papers: list[dict],
        annotations: list[dict],
    ) -> list[dict]:
        """
        Merge FullTextAnalyzer annotation records into paper dicts.

        Joins annotation records to their corresponding paper dicts
        by paper_id so each paper dict contains both its metadata
        and its extracted full-text insights in one place.

        Parameters
        ----------
        papers : list[dict]
            Selected paper dicts from SelectionEngine.
        annotations : list[dict]
            Annotation records from FullTextAnalyzer, each containing
            paper_id and extraction category lists.

        Returns
        -------
        list[dict]
            Paper dicts with annotation fields merged in. Papers without
            matching annotations receive empty lists for each category.
        """
        # build a lookup map from paper_id to annotation record for efficient merging
        annotation_map = {ann["paper_id"]: ann for ann in annotations}
        merged_papers = []
        # iterate over papers and merge in annotations based on paper_id
        for paper in papers:
            paper_id = paper["paper_id"]
            ann = annotation_map.get(paper_id, {})
            # create a new dict that combines the original paper fields with the annotation fields
            merged_paper = {
                **paper,
                "key_contributions": ann.get("key_contributions", []),
                "methodology": ann.get("methodology", []),
                "results": ann.get("results", []),
                "limitations": ann.get("limitations", []),
            }
            merged_papers.append(merged_paper)
        return merged_papers
    
    def _build_category_reason(self, paper: dict, category: str) -> str:
        """
        Generate a short explanation of why a paper was placed in its category.
 
        Produces a human-readable string for the dashboard that helps
        researchers understand the selection rationale at a glance.
 
        Parameters
        ----------
        paper : dict
            Paper dict with signal_scores and metadata fields.
        category : str
            The assigned category label (one of CATEGORIES).
 
        Returns
        -------
        str
            Short explanation string, e.g.:
            'Highly cited foundational work with strong influence score'
            'Central to the transformer architectures cluster'
            'Published recently with rapidly growing citation velocity'
            'Represents a distinct methodological perspective'
        """

        topic = paper.get("topic_cluster", "this research area")
        date = paper.get("submitted_date", "recently")
        citation_count = paper.get("citation_count", 0)
        scores = paper.get("signal_scores", {})

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
        else:
          return f"Selected for relevance to the query in {topic}"