#!/usr/bin/env python3
"""
spark_pagerank.py — Spark Citation Influence Scoring Job

Computes a citation-based influence score for each paper in the corpus
and writes results to the `research_intel.pagerank_scores` table.

Scoring approach:
    S2ORC citation edges use SHA-1 internal corpus IDs on the cited_id
    side, while the papers table uses arXiv IDs. This means zero
    arXiv-to-arXiv edges exist — making iterative graph PageRank
    produce a flat 0.15 score for all corpus papers.

    Instead, we use citation_count normalization: a log-scaled,
    min-max normalized score derived from the citation_count field
    already present in the papers table (sourced from S2ORC during
    enrichment). This produces meaningful differentiated scores that
    reflect real-world citation influence and is equivalent in spirit
    to PageRank for a corpus where cross-corpus edges are unavailable.

    Score formula:
        raw       = log1p(citation_count)
        score     = (raw - min_raw) / (max_raw - min_raw)
        clamped   = 0.15 + score * 0.85   (maps to [0.15, 1.0])

    Papers with citation_count = NULL are assigned the minimum score (0.15).

Usage:
    MSYS_NO_PATHCONV=1 docker exec -it spark-master \\
        /opt/spark/bin/spark-submit \\
        --master spark://spark-master:7077 \\
        /opt/spark/work-dir/pipelines/spark_pagerank.py
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("spark_pagerank")

HIVE_DB      = "research_intel"
PAPERS_TABLE = f"{HIVE_DB}.papers"
EDGES_TABLE  = f"{HIVE_DB}.citation_edges"
SCORES_TABLE = f"{HIVE_DB}.pagerank_scores"


def run_pagerank(spark: SparkSession) -> None:
    """
    Compute citation influence scores and write to pagerank_scores table.

    Uses log-normalized citation_count from the papers table as a proxy
    for PageRank influence. S2ORC citation edges cannot be used directly
    for graph PageRank because cited_id values are SHA-1 internal corpus
    IDs that don't match any arXiv paper_id in the papers table —
    resulting in zero intra-corpus edges and flat 0.15 scores for all
    papers. Citation count normalization produces equivalent results using
    data already available from S2ORC enrichment.
    """

    # 1. Read papers with citation counts
    logger.info("Reading papers from %s", PAPERS_TABLE)
    papers = spark.sql(f"""
        SELECT paper_id, title, citation_count
        FROM {PAPERS_TABLE}
        WHERE paper_id IS NOT NULL AND paper_id != ''
    """)
    total = papers.count()
    logger.info("Loaded %d papers", total)

    if total == 0:
        logger.warning("No papers found — skipping scoring.")
        return

    # 2. Log citation edge stats for reference
    logger.info("Reading citation edges from %s for reference stats", EDGES_TABLE)
    edges_df   = spark.sql(f"SELECT citing_id, cited_id FROM {EDGES_TABLE}")
    edge_count = edges_df.count()
    logger.info("Total citation edges: %d", edge_count)

    # Check arXiv-to-arXiv edge count (expected: 0 due to SHA-1 cited_id format)
    arxiv_pattern = r"^\d{4}\.\d{4,6}$"
    arxiv_edges = edges_df.filter(
        F.col("citing_id").rlike(arxiv_pattern) &
        F.col("cited_id").rlike(arxiv_pattern)
    ).count()
    logger.info(
        "arXiv-to-arXiv edges: %d / %d (%.1f%%) — "
        "SHA-1 cited_id format prevents intra-corpus graph PageRank",
        arxiv_edges, edge_count,
        100.0 * arxiv_edges / edge_count if edge_count > 0 else 0,
    )

    # 3. Compute log-normalized citation influence score
    logger.info("Computing log-normalized citation influence scores...")

    # Fill nulls with 0 before log transform
    papers_filled = papers.withColumn(
        "citation_count_filled",
        F.coalesce(F.col("citation_count").cast("double"), F.lit(0.0))
    )

    # log1p(x) = log(1 + x) — handles zero citations gracefully
    papers_log = papers_filled.withColumn(
        "log_citations",
        F.log1p(F.col("citation_count_filled"))
    )

    # Compute min and max for normalization
    stats = papers_log.agg(
        F.min("log_citations").alias("min_log"),
        F.max("log_citations").alias("max_log"),
        F.avg("citation_count_filled").alias("avg_citations"),
        F.max("citation_count_filled").alias("max_citations"),
    ).collect()[0]

    min_log    = float(stats["min_log"] or 0.0)
    max_log    = float(stats["max_log"] or 1.0)
    log_range  = max_log - min_log if max_log > min_log else 1.0

    logger.info(
        "Citation stats — avg: %.1f, max: %.0f | log range: [%.4f, %.4f]",
        stats["avg_citations"] or 0,
        stats["max_citations"] or 0,
        min_log, max_log,
    )

    # Normalize to [0.15, 1.0] — keeps 0.15 as the floor (matching
    # the reset_prob used in the original iterative PageRank formulation)
    scores_df = papers_log.withColumn(
        "pagerank_score",
        (
            F.lit(0.15) + F.lit(0.85) *
            ((F.col("log_citations") - F.lit(min_log)) / F.lit(log_range))
        ).cast(FloatType())
    ).select(
        F.col("paper_id"),
        F.col("pagerank_score"),
    )

    # 4. Write to pagerank_scores table
    score_count = scores_df.count()
    logger.info("Writing %d scores to %s", score_count, SCORES_TABLE)

    scores_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .saveAsTable(SCORES_TABLE)

    logger.info("Successfully wrote scores to %s", SCORES_TABLE)

    # 5. Log top 10 papers by influence score
    logger.info("Top 10 papers by citation influence score:")
    spark.sql(f"""
        SELECT p.paper_id, p.title, s.pagerank_score,
               p.citation_count
        FROM {PAPERS_TABLE} p
        JOIN {SCORES_TABLE} s ON p.paper_id = s.paper_id
        ORDER BY s.pagerank_score DESC
        LIMIT 10
    """).show(truncate=60)

    # 6. Summary stats
    final_stats = spark.sql(f"SELECT * FROM {SCORES_TABLE}").agg(
        F.avg("pagerank_score").alias("avg"),
        F.max("pagerank_score").alias("max"),
        F.min("pagerank_score").alias("min"),
        F.stddev("pagerank_score").alias("stddev"),
        F.count("*").alias("total"),
        F.sum(F.when(F.col("pagerank_score") > 0.15, 1).otherwise(0)).alias("above_floor"),
    ).collect()[0]

    logger.info(
        "Score stats — avg: %.4f, max: %.4f, min: %.4f, stddev: %.4f, "
        "total: %d, above_floor: %d (%.1f%%)",
        final_stats["avg"]    or 0,
        final_stats["max"]    or 0,
        final_stats["min"]    or 0,
        final_stats["stddev"] or 0,
        final_stats["total"]  or 0,
        final_stats["above_floor"] or 0,
        100.0 * (final_stats["above_floor"] or 0) / (final_stats["total"] or 1),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Spark citation influence scoring job"
    )
    parser.add_argument(
        "--local", action="store_true",
        help="Run in local mode (for testing outside Docker)",
    )
    args = parser.parse_args()

    builder = SparkSession.builder \
        .appName("ResearchIntel-PageRank") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .enableHiveSupport()

    if args.local:
        builder = builder.master("local[*]")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("=" * 60)
    logger.info("Starting citation influence scoring job")
    logger.info("=" * 60)

    try:
        run_pagerank(spark)
        logger.info("=" * 60)
        logger.info("Citation influence scoring complete!")
        logger.info("=" * 60)
    except Exception as e:
        logger.error("Job failed: %s", e, exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()