"""
scorer.py
---------
Individual signal scoring functions used by the Selection Engine
to rank papers for targeted full-text analysis.

Each scorer computes a single normalized signal in the range [0.0, 1.0]
for a list of paper candidates. Keeping scorers as separate functions
makes it easy to tune, replace, or disable individual signals without
touching the composite ranking logic in selector.py.

The five signals defined here correspond directly to the selection
criteria described in the proposal (Section 2.5.1):
  1. Relevance       — match strength against Elasticsearch query
  2. Influence       — PageRank score from the citation graph
  3. Recency         — how recently the paper was published
  4. Representativeness — proximity to BERTopic cluster centroids
  5. Diversity       — penalizes redundancy across selected papers

All scorers accept a list of paper dicts and return a parallel list
of float scores. The paper dicts are expected to contain the enriched
fields written to Hive by spark_consolidate.py.

Dependencies: datetime (stdlib), math (stdlib)
"""

import math
from datetime import datetime, timezone


def score_relevance(papers: list[dict], es_scores: dict[str, float]) -> list[float]:
    """
    Score each paper by its Elasticsearch relevance score for the query.

    Elasticsearch returns a _score for each hit that reflects how well
    the document matches the query (BM25 by default). This function
    normalizes those scores to [0.0, 1.0] by dividing by the maximum
    score in the result set so all signals are on the same scale.

    Parameters
    ----------
    papers : list[dict]
        List of candidate paper dicts, each containing at minimum
        a 'paper_id' field.
    es_scores : dict[str, float]
        Mapping of paper_id to raw Elasticsearch _score as returned
        by the search query. Papers not present in es_scores receive
        a relevance score of 0.0.

    Returns
    -------
    list[float]
        Normalized relevance scores in [0.0, 1.0], one per paper,
        in the same order as the input list.
    """
    if not es_scores:
        return [0.0] * len(papers) # guard against empty score dict
    max_score = max(es_scores.values())
    if max_score == 0:
        return [0.0] * len(papers) # guard against zero max score
    return [es_scores.get(p["paper_id"], 0.0) / max_score for p in papers]

def score_influence(papers: list[dict]) -> list[float]:
    """
    Score each paper by its citation influence score from the
    research_intel.pagerank_scores Hive table.
 
    Scores in that table are computed by spark_pagerank.py using
    log-normalized citation counts — NOT iterative graph PageRank.
    S2ORC citation edges use SHA-1 internal corpus IDs on the cited_id
    side which do not match arXiv paper IDs, producing zero intra-corpus
    edges and making graph PageRank unusable. Instead, spark_pagerank.py
    uses the formula:
 
        raw   = log1p(citation_count)
        score = 0.15 + 0.85 * ((raw - min_raw) / (max_raw - min_raw))
 
    This means scores arrive already scaled to [0.15, 1.0]. This function
    rescales them to [0.0, 1.0] to match the other signals by treating
    0.15 as the known floor and the observed maximum as the ceiling.
    No additional log normalization is needed or should be applied here.
 
    Papers missing a pagerank_score field receive the floor value of 0.15
    before rescaling, which maps to 0.0 in the output.
 
    Parameters
    ----------
    papers : list[dict]
        List of candidate paper dicts, each optionally containing
        a 'pagerank_score' field as fetched from the pagerank_scores
        Hive table by selector._enrich_with_hive().
 
    Returns
    -------
    list[float]
        Rescaled influence scores in [0.0, 1.0], one per paper.
    """
    raw_scores = [p.get("pagerank_score", 0.15) for p in papers] # default to floor
    # Scores are already in [0.15, 1.0] from spark_pagerank.py
    # Rescale to [0.0, 1.0] to match other signals
    min_score = 0.15
    max_score = max(raw_scores) if raw_scores else 0.15 # guard against empty list
    score_range = max_score - min_score if max_score > min_score else 1.0 # avoid division by zero
    return [(s - min_score) / score_range for s in raw_scores] # rescale to [0.0, 1.0]


def score_recency(papers: list[dict], decay_days: int = 365) -> list[float]:
    """
    Score each paper by how recently it was published.

    Applies exponential decay so that papers published today score 1.0
    and scores decrease smoothly as papers get older. Papers older than
    decay_days are not zeroed out — they receive a small but nonzero
    score so foundational older papers are not completely excluded.

    Formula: score = exp(-days_since_publication / decay_days)

    Parameters
    ----------
    papers : list[dict]
        List of candidate paper dicts, each containing a 'submitted_date'
        field as an ISO date string or datetime object.
    decay_days : int
        Half-life for the exponential decay in days. Default: 365.
        Decrease for stronger recency bias, increase to weight older
        papers more fairly.

    Returns
    -------
    list[float]
        Recency scores in (0.0, 1.0], one per paper. Papers with
        missing or unparseable dates receive a score of 0.0.
    """
    now = datetime.now(timezone.utc)
    scores = []
    for p in papers:
        date_str = p.get("submitted_date")
        if not date_str:
            scores.append(0.0) # missing date
            continue
        try:
            pub_date = datetime.fromisoformat(date_str)
            days_old = (now - pub_date).days
            score = math.exp(-days_old / decay_days) if days_old >= 0 else 1.0
            scores.append(score)
        except ValueError:
            scores.append(0.0) # unparseable date
    return scores


def score_representativeness(papers: list[dict]) -> list[float]:
    """
    Score each paper by how representative it is of its topic cluster.

    Uses the BERTopic UMAP coordinates (umap_x, umap_y) stored in the
    papers table to estimate each paper's proximity to its cluster
    centroid. Papers near the centroid of their cluster are more
    representative of the cluster's core theme and are preferred
    over papers at the cluster periphery.

    For each topic_cluster_id, computes the centroid as the mean
    (umap_x, umap_y) across all papers in that cluster among the
    candidates, then scores each paper by its inverse distance to
    the centroid. Scores are normalized to [0.0, 1.0].

    Papers assigned to the outlier cluster (topic_cluster_id = -1)
    or missing UMAP coordinates receive a score of 0.0.

    Parameters
    ----------
    papers : list[dict]
        List of candidate paper dicts, each containing 'topic_cluster_id',
        'umap_x', and 'umap_y' fields.

    Returns
    -------
    list[float]
        Representativeness scores in [0.0, 1.0], one per paper.
    """
    # Group papers by cluster
    clusters = {}
    for p in papers:
        cluster_id = p.get("topic_cluster_id", -1)
        if cluster_id == -1:
            continue # skip outliers
        if cluster_id not in clusters:
            clusters[cluster_id] = []
        clusters[cluster_id].append(p)

    # Compute centroids
    centroids = {}
    for cluster_id, members in clusters.items():
        xs = [p.get("umap_x") for p in members if p.get("umap_x") is not None]
        ys = [p.get("umap_y") for p in members if p.get("umap_y") is not None]
        if xs and ys:
            centroids[cluster_id] = (sum(xs) / len(xs), sum(ys) / len(ys))

    # Score papers by inverse distance to centroid
    scores = []
    for p in papers:
        cluster_id = p.get("topic_cluster_id", -1)
        if cluster_id == -1 or cluster_id not in centroids:
            scores.append(0.0) # outliers or missing centroid
            continue
        centroid_x, centroid_y = centroids[cluster_id]
        x, y = p.get("umap_x"), p.get("umap_y")
        if x is None or y is None:
            scores.append(0.0) # missing coordinates
            continue
        distance = math.sqrt((x - centroid_x) ** 2 + (y - centroid_y) ** 2)
        scores.append(distance)

    # Normalize distances to [0.0, 1.0] and invert so closer to centroid is higher score
    if not scores:
        return [0.0] * len(papers) # guard against empty list
    max_distance = max(scores)
    min_distance = min(scores)
    distance_range = max_distance - min_distance if max_distance > min_distance else 1.0
    normalized_scores = [(max_distance - d) / distance_range for d in scores]
    return normalized_scores


def score_diversity(papers: list[dict], selected_ids: list[str]) -> list[float]:
    """
    Score each candidate paper by how much diversity it would add to
    the already-selected set.

    Penalizes candidates that belong to the same topic cluster as papers
    already in the selected set, encouraging coverage across multiple
    clusters and methodological approaches.

    A paper's diversity score is 1.0 if its topic cluster is not yet
    represented in selected_ids, and decreases progressively as more papers
    from the same cluster are already selected. The penalty uses an inverse
    formula: score = 1.0 / (1 + count), where count is the number of papers
    from that cluster already selected. This implements a soft diversity
    constraint rather than a hard cutoff.

    Example progression for papers from a cluster:
        0 papers selected from cluster: score = 1.0
        1 paper selected from cluster:  score = 0.5
        2 papers selected from cluster: score = 0.33
        3 papers selected from cluster: score = 0.25

    Used iteratively by selector.py — as papers are selected one by one,
    selected_ids grows and diversity scores are recomputed each round
    to reflect the current selection state.

    Parameters
    ----------
    papers : list[dict]
        List of candidate paper dicts, each containing a
        'topic_cluster_id' field.
    selected_ids : list[str]
        Paper IDs already selected in this round. Diversity scores
        are computed relative to the clusters represented by these papers.

    Returns
    -------
    list[float]
        Diversity scores in [0.0, 1.0], one per paper.
    """
    # Count how many papers from each cluster have already been selected
    cluster_counts = {}
    for s in papers:
        if s["paper_id"] in selected_ids:
            cluster_id = s.get("topic_cluster_id", -1)
            if cluster_id != -1:
                cluster_counts[cluster_id] = cluster_counts.get(cluster_id, 0) + 1
    
    # Score each candidate based on how many papers from its cluster are selected
    diversity_scores = []
    for p in papers:
        cluster_id = p.get("topic_cluster_id", -1)
        if cluster_id == -1:
            diversity_scores.append(0.0) # outliers get lowest diversity
            continue
        
        # Progressive penalty: score decreases as more papers from same cluster are selected
        count_from_cluster = cluster_counts.get(cluster_id, 0)
        score = 1.0 / (1.0 + count_from_cluster)
        diversity_scores.append(score)
    
    return diversity_scores


def combine_scores(
    relevance: list[float],
    influence: list[float],
    recency: list[float],
    representativeness: list[float],
    diversity: list[float],
    weights: dict[str, float] = None,
) -> list[float]:
    """
    Combine individual signal scores into a single composite ranking score.

    Computes a weighted sum of the five normalized signals. Default weights
    are tuned for a balanced selection that favors relevance and influence
    while still rewarding recency and diversity.

    Default weights:
        relevance:          0.35
        influence:          0.25
        recency:            0.15
        representativeness: 0.15
        diversity:          0.10

    Weights are normalized to sum to 1.0 if the provided weights do not
    already sum to 1.0, so callers can pass unnormalized preference values.

    Parameters
    ----------
    relevance : list[float]
        Normalized relevance scores from score_relevance().
    influence : list[float]
        Normalized influence scores from score_influence().
    recency : list[float]
        Normalized recency scores from score_recency().
    representativeness : list[float]
        Normalized representativeness scores from score_representativeness().
    diversity : list[float]
        Normalized diversity scores from score_diversity().
    weights : dict[str, float], optional
        Custom weight overrides. Keys must match signal names above.
        Missing keys use default values. Default: None (use defaults).

    Returns
    -------
    list[float]
        Composite scores in [0.0, 1.0], one per paper, in the same
        order as the input signal lists.
    """
    if weights is None:
        weights = {
            "relevance": 0.35,
            "influence": 0.25,
            "recency": 0.15,
            "representativeness": 0.15,
            "diversity": 0.10,
        }
    # Normalize weights to sum to 1.0
    total_weight = sum(weights.values())
    if total_weight != 1.0:
        weights = {k: v / total_weight for k, v in weights.items()}

    composite_scores = []
    for r, i, rec, rep, d in zip(relevance, influence, recency, representativeness, diversity):
        score = (
            r * weights.get("relevance", 0.0)
            + i * weights.get("influence", 0.0)
            + rec * weights.get("recency", 0.0)
            + rep * weights.get("representativeness", 0.0)
            + d * weights.get("diversity", 0.0)
        )
        composite_scores.append(score)
    return composite_scores