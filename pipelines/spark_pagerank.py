#!/usr/bin/env python3
"""
spark_pagerank.py — Spark PageRank Job

Reads citation edges from the Hive `research_intel.citation_edges` table,
builds a citation graph, runs PageRank, and writes the scores back to
the `research_intel.papers` table.

Uses GraphFrames (the Python/DataFrame-based graph API) rather than
the Scala-only GraphX, since we're running PySpark.

Usage:
    # From the project root (with Docker running):
    MSYS_NO_PATHCONV=1 docker exec -it spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
        /opt/spark/work-dir/pipelines/spark_pagerank.py

    # Or run locally for testing:
    python pipelines/spark_pagerank.py --local
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

# Constants 

HIVE_DB = "research_intel"
PAPERS_TABLE = f"{HIVE_DB}.papers"
EDGES_TABLE = f"{HIVE_DB}.citation_edges"

# PageRank parameters
MAX_ITER = 20          # convergence iterations
RESET_PROB = 0.15      # damping factor = 1 - reset_prob = 0.85


# Build the graph and run PageRank

def run_pagerank(spark: SparkSession, max_iter: int = MAX_ITER, 
                 reset_prob: float = RESET_PROB) -> None:
    """
    Build a citation graph from Hive edges, run PageRank, and write
    scores back to the papers table.
    """
    # 1. Read edges 
    logger.info("Reading citation edges from %s", EDGES_TABLE)
    edges_df = spark.sql(f"SELECT citing_id, cited_id FROM {EDGES_TABLE}")
    edge_count = edges_df.count()
    logger.info("Loaded %d citation edges", edge_count)

    if edge_count == 0:
        logger.warning("No citation edges found — skipping PageRank.")
        return

    # 2. Build vertex list 
    # Vertices = all unique paper IDs that appear in either side of an edge
    # PLUS all papers in the papers table (so papers with no citations
    # still get a PageRank score)
    logger.info("Building vertex list...")
    
    citing_ids = edges_df.select(F.col("citing_id").alias("id"))
    cited_ids = edges_df.select(F.col("cited_id").alias("id"))
    paper_ids = spark.sql(f"SELECT paper_id AS id FROM {PAPERS_TABLE}")
    
    vertices = citing_ids.union(cited_ids).union(paper_ids) \
        .distinct() \
        .filter(F.col("id").isNotNull() & (F.col("id") != ""))
    
    vertex_count = vertices.count()
    logger.info("Graph has %d vertices and %d edges", vertex_count, edge_count)

    # 3. Rename edge columns for GraphFrames 
    # GraphFrames expects columns named 'src' and 'dst'
    edges_gf = edges_df.select(
        F.col("citing_id").alias("src"),
        F.col("cited_id").alias("dst"),
    )

    # 4. Run PageRank 
    # GraphFrames is not available: use native Spark DataFrame-based
    # iterative PageRank implementation instead
    logger.info(
        "Running PageRank (max_iter=%d, reset_prob=%.2f)...",
        max_iter, reset_prob
    )
    
    pagerank_scores = _iterative_pagerank(
        spark, vertices, edges_gf, max_iter, reset_prob
    )

    # 5. Write scores back to papers 
    logger.info("Updating papers table with PageRank scores...")
    
    # Write PageRank scores to a dedicated table (fast, small, no read-write conflict)
    # FastAPI will JOIN papers + pagerank_scores at query time
    scores_table = f"{HIVE_DB}.pagerank_scores"
    
    scores_df = pagerank_scores.select(
        F.col("id").alias("paper_id"),
        F.col("rank").cast(FloatType()).alias("pagerank_score"),
    ).filter(F.col("paper_id").isNotNull() & (F.col("paper_id") != ""))

    score_count = scores_df.count()
    logger.info("Writing %d PageRank scores to %s", score_count, scores_table)

    scores_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .saveAsTable(scores_table)

    logger.info("Successfully wrote PageRank scores to %s", scores_table)

    # 6. Log top papers 
    logger.info("Top 10 papers by PageRank:")
    final_df = spark.sql(f"""
        SELECT p.paper_id, p.title, s.pagerank_score
        FROM {PAPERS_TABLE} p
        JOIN {scores_table} s ON p.paper_id = s.paper_id
        ORDER BY s.pagerank_score DESC
        LIMIT 10
    """)
    final_df.show(truncate=60)

    # Stats
    stats = spark.sql(f"SELECT * FROM {scores_table}").agg(
        F.avg("pagerank_score").alias("avg_pagerank"),
        F.max("pagerank_score").alias("max_pagerank"),
        F.min("pagerank_score").alias("min_pagerank"),
        F.stddev("pagerank_score").alias("stddev_pagerank"),
        F.count("*").alias("total_scores"),
    ).collect()[0]

    logger.info(
        "PageRank stats — avg: %.6f, max: %.6f, min: %.6f, stddev: %.6f, total_scores: %d",
        stats["avg_pagerank"] or 0,
        stats["max_pagerank"] or 0,
        stats["min_pagerank"] or 0,
        stats["stddev_pagerank"] or 0,
        stats["total_scores"] or 0,
    )


def _iterative_pagerank(spark, vertices, edges, max_iter, reset_prob):
    """
    DataFrame-based iterative PageRank implementation.
    
    This avoids the need for the GraphFrames package, which requires
    a separate JAR dependency. Uses standard Spark DataFrame operations.
    
    Algorithm:
        1. Initialize all vertices with rank = 1.0
        2. For each iteration:
           a. Each vertex distributes its rank equally to all outgoing edges
           b. New rank = reset_prob + (1 - reset_prob) * sum(incoming contributions)
        3. Return final ranks
    """
    damping = 1.0 - reset_prob
    
    # Initialize ranks
    ranks = vertices.select("id").withColumn("rank", F.lit(1.0))
    
    # Compute out-degree for each vertex
    out_degrees = edges.groupBy("src").agg(
        F.count("*").alias("out_degree")
    )
    
    for iteration in range(max_iter):
        # Join ranks with edges to compute contributions
        # Each source vertex sends rank/out_degree to each destination
        contribs = edges.join(ranks, edges["src"] == ranks["id"], "inner") \
            .join(out_degrees, edges["src"] == out_degrees["src"], "inner") \
            .select(
                edges["dst"].alias("id"),
                (F.col("rank") / F.col("out_degree")).alias("contribution"),
            )
        
        # Sum contributions for each destination vertex
        incoming = contribs.groupBy("id").agg(
            F.sum("contribution").alias("total_contrib")
        )
        
        # Update ranks: reset_prob + damping * sum(contributions)
        # Use left join to keep vertices with no incoming edges
        new_ranks = vertices.join(incoming, "id", "left").select(
            F.col("id"),
            (F.lit(reset_prob) + damping * F.coalesce(F.col("total_contrib"), F.lit(0.0))).alias("rank"),
        )
        
        ranks = new_ranks
        
        if (iteration + 1) % 5 == 0:
            logger.info("  PageRank iteration %d/%d complete", iteration + 1, max_iter)
    
    logger.info("PageRank converged after %d iterations", max_iter)
    return ranks


# Main

def main():
    parser = argparse.ArgumentParser(description="Spark PageRank job")
    parser.add_argument(
        "--local", action="store_true",
        help="Run in local mode (for testing outside Docker)",
    )
    parser.add_argument(
        "--max-iter", type=int, default=MAX_ITER,
        help=f"PageRank iterations (default: {MAX_ITER})",
    )
    parser.add_argument(
        "--reset-prob", type=float, default=RESET_PROB,
        help=f"Reset probability / 1-damping (default: {RESET_PROB})",
    )
    args = parser.parse_args()

    # Build Spark session with Hive metastore connection
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
    logger.info("Starting PageRank job")
    logger.info("=" * 60)

    try:
        run_pagerank(spark, max_iter=args.max_iter, reset_prob=args.reset_prob)
        logger.info("=" * 60)
        logger.info("PageRank job complete!")
        logger.info("=" * 60)
    except Exception as e:
        logger.error("PageRank job failed: %s", e, exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()