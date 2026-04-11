"""
run_bertopic.py — BERTopic pipeline entrypoint.

Orchestrates the full topic modeling workflow:
  1. Load paper abstracts from HDFS (all sources, deduplicated)
  2. Clean abstracts via TextCleaner
  3. Embed abstracts using sentence-transformers (cached to disk)
  4. Fit BERTopic model (UMAP + HDBSCAN + c-TF-IDF)
  5. Generate 2D UMAP coordinates for the Landscape Map
  6. Save assignments, topic metadata, and coordinates to HDFS
  7. Write per-paper topic assignments to HDFS so spark_consolidate.py
     can merge topic_cluster_id / topic_cluster / umap_x / umap_y
     back into the papers Hive table

HDFS output paths:
    /user/research-intelligence/raw/bertopic/<date>/assignments.jsonl
    /user/research-intelligence/raw/bertopic/<date>/topic_info.jsonl
    /user/research-intelligence/raw/bertopic/<date>/coordinates.jsonl

Per-paper record shape written for Spark merge:
    {
        "paper_id":        "2401.12345",
        "topic_cluster_id": 3,
        "topic_cluster":   "transformer_attention_language",
        "umap_x":          1.23,
        "umap_y":         -0.45
    }

Usage:
    python -m NLP_layer.run_bertopic \\
        --input_path /user/research-intelligence/raw \\
        --output_path /user/research-intelligence/raw/bertopic \\
        --embedding_cache ./cache/embeddings.npy \\
        --min_cluster_size 10 \\
        --sources arxiv

    # Re-run clustering without re-embedding (uses cache):
    python -m NLP_layer.run_bertopic \\
        --input_path /user/research-intelligence/raw \\
        --output_path /user/research-intelligence/raw/bertopic \\
        --embedding_cache ./cache/embeddings.npy \\
        --min_cluster_size 20
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from ingestion.hdfs_client import HDFSClient
from NLP_layer.loader import CorpusLoader
from NLP_layer.embedder import Embedder
from NLP_layer.reducer import UMAPReducer
from NLP_layer.clusterer import HDBSCANClusterer
from NLP_layer.topic_model import TopicModeler
from NLP_layer.shared.text_preprocessing import TextCleaner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("run_bertopic")


def save_to_hdfs(records: list[dict], hdfs: HDFSClient, category: str) -> str:
    """Write a list of records to HDFS under raw/bertopic/<category>/."""
    path = hdfs.write_json(records, source="bertopic", category=category)
    logger.info("Wrote %d records to HDFS: %s", len(records), path)
    return path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run BERTopic topic modeling pipeline over paper abstracts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full run — embed + cluster arXiv papers
  python -m NLP_layer.run_bertopic \\
      --input_path /user/research-intelligence/raw \\
      --output_path /user/research-intelligence/raw/bertopic \\
      --embedding_cache ./cache/embeddings.npy \\
      --sources arxiv

  # Re-cluster with different hyperparameters (skip re-embedding via cache)
  python -m NLP_layer.run_bertopic \\
      --input_path /user/research-intelligence/raw \\
      --output_path /user/research-intelligence/raw/bertopic \\
      --embedding_cache ./cache/embeddings.npy \\
      --min_cluster_size 20 \\
      --nr_topics 50
        """
    )
    parser.add_argument(
        "--input_path", required=True,
        help="Base HDFS path for raw ingested records"
    )
    parser.add_argument(
        "--output_path", required=True,
        help="Base HDFS path for BERTopic output"
    )
    parser.add_argument(
        "--embedding_cache", default="./cache/embeddings.npy",
        help="Local path to cache/load embeddings (default: ./cache/embeddings.npy)"
    )
    parser.add_argument(
        "--sources", default="arxiv,s2orc,openalex",
        help="Comma-separated sources to load (default: arxiv,s2orc,openalex)"
    )
    parser.add_argument(
        "--embedding_model", default="all-MiniLM-L6-v2",
        help="Sentence transformer model for embeddings (default: all-MiniLM-L6-v2)"
    )
    parser.add_argument(
        "--batch_size", type=int, default=64,
        help="Embedding batch size (default: 64)"
    )
    parser.add_argument(
        "--min_cluster_size", type=int, default=10,
        help="HDBSCAN min_cluster_size (default: 10)"
    )
    parser.add_argument(
        "--min_samples", type=int, default=None,
        help="HDBSCAN min_samples (default: same as min_cluster_size)"
    )
    parser.add_argument(
        "--nr_topics", default=None,
        help="Number of topics to reduce to after fitting. "
             "Integer or 'auto'. Default: no reduction."
    )
    parser.add_argument(
        "--n_components", type=int, default=5,
        help="UMAP n_components for clustering (default: 5)"
    )
    parser.add_argument(
        "--n_neighbors", type=int, default=15,
        help="UMAP n_neighbors (default: 15)"
    )
    parser.add_argument(
        "--top_n_words", type=int, default=10,
        help="Words per topic for labels (default: 10)"
    )
    parser.add_argument(
        "--min_abstract_length", type=int, default=100,
        help="Min abstract length in chars (default: 100)"
    )
    parser.add_argument(
        "--date_from", default=None,
        help="Load papers submitted from this date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--date_to", default=None,
        help="Load papers submitted up to this date (YYYY-MM-DD)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args    = parse_args()
    sources = [s.strip() for s in args.sources.split(",")]
    t0      = time.time()

    # Parse nr_topics
    nr_topics = None
    if args.nr_topics:
        nr_topics = int(args.nr_topics) if args.nr_topics.isdigit() else args.nr_topics

    logger.info("=" * 60)
    logger.info("BERTopic Pipeline starting")
    logger.info("  input_path       : %s", args.input_path)
    logger.info("  sources          : %s", sources)
    logger.info("  embedding_model  : %s", args.embedding_model)
    logger.info("  embedding_cache  : %s", args.embedding_cache)
    logger.info("  min_cluster_size : %d", args.min_cluster_size)
    logger.info("  nr_topics        : %s", nr_topics)
    logger.info("=" * 60)

    # ── Step 1: Load abstracts from HDFS ─────────────────────────────
    hdfs   = HDFSClient()
    loader = CorpusLoader(hdfs_client=hdfs, min_abstract_length=args.min_abstract_length)

    paper_ids, abstracts = loader.load(
        input_path=args.input_path,
        sources=sources,
        date_from=args.date_from,
        date_to=args.date_to,
    )
    logger.info("Loaded %d abstracts for topic modeling", len(abstracts))

    if len(abstracts) < 50:
        logger.error(
            "Too few abstracts (%d) to fit a meaningful topic model. "
            "Ingest more papers first.", len(abstracts)
        )
        raise SystemExit(1)

    # ── Step 2: Embed abstracts ───────────────────────────────────────
    # Embeddings are cached to disk so re-clustering experiments don't
    # repeat the expensive encoding step.
    Path(args.embedding_cache).parent.mkdir(parents=True, exist_ok=True)

    embedder = Embedder(
        model_name=args.embedding_model,
        batch_size=args.batch_size,
        cache_path=args.embedding_cache,
    )
    embedder.load()

    if embedder.cache_exists():
        logger.info("Loading embeddings from cache: %s", args.embedding_cache)
    else:
        logger.info("Computing embeddings (this may take several minutes on CPU)...")

    embeddings = embedder.encode(abstracts)
    logger.info("Embeddings shape: %s", embeddings.shape)

    # ── Step 3: Fit BERTopic ──────────────────────────────────────────
    reducer   = UMAPReducer(
        n_components=args.n_components,
        n_neighbors=args.n_neighbors,
    )
    clusterer = HDBSCANClusterer(
        min_cluster_size=args.min_cluster_size,
        min_samples=args.min_samples,
    )
    modeler = TopicModeler(
        reducer=reducer,
        clusterer=clusterer,
        top_n_words=args.top_n_words,
        nr_topics=nr_topics,
        min_topic_size=args.min_cluster_size,
    )

    logger.info("Fitting BERTopic model...")
    modeler.fit(abstracts, embeddings)
    logger.info("BERTopic fitting complete")

    # ── Step 4: Extract results ───────────────────────────────────────
    assignments  = modeler.get_topic_assignments(paper_ids)
    topic_info   = modeler.get_topic_info()
    coordinates  = modeler.get_2d_coordinates(embeddings, paper_ids)

    n_topics     = len(topic_info)
    n_outliers   = sum(1 for a in assignments if a["topic_id"] == -1)
    noise_ratio  = n_outliers / len(assignments) if assignments else 0

    logger.info("Topics discovered : %d", n_topics)
    logger.info("Outlier papers    : %d / %d (%.1f%%)",
                n_outliers, len(assignments), noise_ratio * 100)

    # ── Step 5: Write full outputs to HDFS ───────────────────────────
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    save_to_hdfs(assignments, hdfs, f"{today}/assignments")
    save_to_hdfs(topic_info,  hdfs, f"{today}/topic_info")
    save_to_hdfs(coordinates, hdfs, f"{today}/coordinates")

    # ── Step 6: Write flat per-paper records for Spark merge ─────────
    # spark_consolidate.py reads these to populate:
    #   papers.topic_cluster_id, papers.topic_cluster, papers.umap_x, papers.umap_y
    #
    # Build a lookup from paper_id → (x, y) from coordinates
    coord_lookup = {c["paper_id"]: (c["x"], c["y"]) for c in coordinates}

    spark_records = []
    for assignment in assignments:
        pid   = assignment["paper_id"]
        x, y  = coord_lookup.get(pid, (None, None))
        spark_records.append({
            "paper_id":         pid,
            "topic_cluster_id": assignment["topic_id"],
            "topic_cluster":    assignment["topic_label"],
            "umap_x":           x,
            "umap_y":           y,
        })

    save_to_hdfs(spark_records, hdfs, f"{today}/spark_merge")
    logger.info("Wrote %d spark_merge records for consolidation", len(spark_records))

    # ── Step 7: Print topic summary ───────────────────────────────────
    logger.info("=" * 60)
    logger.info("Top 10 topics by size:")
    for t in sorted(topic_info, key=lambda x: -x["size"])[:10]:
        logger.info(
            "  [%3d] %-40s  (%d papers)",
            t["topic_id"], t["topic_label"][:40], t["size"]
        )

    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info("BERTopic Pipeline complete in %.1fs", elapsed)
    logger.info("  Papers processed : %d", len(abstracts))
    logger.info("  Topics found     : %d", n_topics)
    logger.info("  Noise ratio      : %.1f%%", noise_ratio * 100)
    logger.info("=" * 60)
    logger.info(
        "Next step: re-run spark_consolidate.py to merge topic columns into Hive"
    )
