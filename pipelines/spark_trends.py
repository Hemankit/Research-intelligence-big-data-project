#!/usr/bin/env python3
"""
spark_trends.py — Spark Trend Aggregation Job
===============================================
Reads from the Hive `research_intel.papers` table (and optionally
`pagerank_scores`), computes trend aggregations by category and month,
and writes results to the `research_intel.trends` table.

Usage:
    MSYS_NO_PATHCONV=1 docker exec -it spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/work-dir/pipelines/spark_trends.py
"""

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("spark_trends")

# Constants 

HIVE_DB = "research_intel"
PAPERS_TABLE = f"{HIVE_DB}.papers"
PAGERANK_TABLE = f"{HIVE_DB}.pagerank_scores"
TRENDS_TABLE = f"{HIVE_DB}.trends"


def run_trend_aggregation(spark: SparkSession) -> None:
    """
    Compute trend aggregations and write to the trends table.

    Aggregation dimensions:
        - primary_category  (e.g. cs.LG, cs.CL)
        - topic_cluster     (from BERTopic — null until BERTopic runs)
        - year_month        (derived from submitted_date)

    Metrics per bucket:
        - paper_count
        - avg_citation_count
        - avg_pagerank
    """
    # 1. Read papers 
    logger.info("Reading papers from %s", PAPERS_TABLE)
    papers = spark.sql(f"SELECT * FROM {PAPERS_TABLE}")
    total_papers = papers.count()
    logger.info("Loaded %d papers", total_papers)

    if total_papers == 0:
        logger.warning("No papers found — skipping trend aggregation.")
        return

    # 2. Join PageRank scores 
    logger.info("Joining PageRank scores from %s", PAGERANK_TABLE)
    try:
        pagerank = spark.sql(f"SELECT paper_id, pagerank_score FROM {PAGERANK_TABLE}")
        papers = papers.drop("pagerank_score").join(pagerank, on="paper_id", how="left")
        logger.info("PageRank scores joined successfully")
    except Exception as e:
        logger.warning("Could not join PageRank scores (table may not exist yet): %s", e)
        papers = papers.withColumn("pagerank_score", F.lit(None).cast(FloatType()))

    # 3. Compute year_month from submitted_date
    papers = papers.withColumn(
        "year_month",
        F.date_format(F.col("submitted_date"), "yyyy-MM")
    )

    # Filter out papers with no valid date
    papers_with_date = papers.filter(
        F.col("year_month").isNotNull() & (F.col("year_month") != "")
    )
    logger.info("Papers with valid dates: %d / %d", papers_with_date.count(), total_papers)

    # 4. Check if topic_cluster is populated 
    has_topics = False
    if "topic_cluster" in papers.columns:
        topic_count = papers.filter(F.col("topic_cluster").isNotNull()).count()
        has_topics = topic_count > 0
        logger.info("Papers with topic_cluster: %d", topic_count)

    if has_topics:
        logger.info("BERTopic data found — aggregating by category + topic + month")
    else:
        logger.info("No BERTopic data yet — aggregating by category + month only")

    # 5. Aggregate by category + month 
    logger.info("Computing category-level trends...")
    category_trends = papers_with_date.groupBy(
        "primary_category", "year_month"
    ).agg(
        F.count("*").alias("paper_count"),
        F.avg("citation_count").cast(FloatType()).alias("avg_citation_count"),
        F.avg("pagerank_score").cast(FloatType()).alias("avg_pagerank"),
    ).withColumn(
        "topic_cluster", F.lit(None).cast("string")
    ).withColumn(
        "topic_cluster_id", F.lit(None).cast("int")
    )

    # Select columns in the order matching the Hive table schema
    category_trends = category_trends.select(
        "primary_category",
        "topic_cluster",
        "topic_cluster_id",
        "year_month",
        "paper_count",
        "avg_citation_count",
        "avg_pagerank",
    )

    trend_count = category_trends.count()
    logger.info("Category-level trend rows: %d", trend_count)

    # 6. Aggregate by topic + month (only if BERTopic has run) 
    if has_topics:
        logger.info("Computing topic-level trends...")
        topic_trends = papers_with_date.filter(
            F.col("topic_cluster").isNotNull()
        ).groupBy(
            "primary_category", "topic_cluster", "topic_cluster_id", "year_month"
        ).agg(
            F.count("*").alias("paper_count"),
            F.avg("citation_count").cast(FloatType()).alias("avg_citation_count"),
            F.avg("pagerank_score").cast(FloatType()).alias("avg_pagerank"),
        )

        topic_trends = topic_trends.select(
            "primary_category",
            "topic_cluster",
            "topic_cluster_id",
            "year_month",
            "paper_count",
            "avg_citation_count",
            "avg_pagerank",
        )

        topic_count = topic_trends.count()
        logger.info("Topic-level trend rows: %d", topic_count)

        # Union both levels
        all_trends = category_trends.unionByName(topic_trends)
    else:
        all_trends = category_trends

    # 7. Write to Hive 
    final_count = all_trends.count()
    logger.info("Writing %d trend rows to %s", final_count, TRENDS_TABLE)

    all_trends.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .saveAsTable(TRENDS_TABLE)

    logger.info("Successfully wrote trends to %s", TRENDS_TABLE)

    # 8. Log sample output 
    logger.info("Sample trends (top 20 by paper_count):")
    spark.sql(f"""
        SELECT primary_category, topic_cluster, year_month, paper_count,
               avg_citation_count, avg_pagerank
        FROM {TRENDS_TABLE}
        ORDER BY paper_count DESC
        LIMIT 20
    """).show(truncate=40)

    # Summary stats
    stats = spark.sql(f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT primary_category) as categories,
            COUNT(DISTINCT topic_cluster) as topics,
            COUNT(DISTINCT year_month) as months,
            SUM(paper_count) as total_paper_refs,
            MIN(year_month) as earliest_month,
            MAX(year_month) as latest_month
        FROM {TRENDS_TABLE}
    """).collect()[0]

    logger.info(
        "Trend stats — rows: %d, categories: %d, topics: %d, months: %d, "
        "date range: %s to %s",
        stats["total_rows"],
        stats["categories"],
        stats["topics"],
        stats["months"],
        stats["earliest_month"],
        stats["latest_month"],
    )


# Main 

def main():
    parser = argparse.ArgumentParser(description="Spark trend aggregation job")
    parser.add_argument(
        "--local", action="store_true",
        help="Run in local mode (for testing outside Docker)",
    )
    args = parser.parse_args()

    builder = SparkSession.builder \
        .appName("ResearchIntel-TrendAggregation") \
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
    logger.info("Starting trend aggregation job")
    logger.info("=" * 60)

    try:
        run_trend_aggregation(spark)
        logger.info("=" * 60)
        logger.info("Trend aggregation complete!")
        logger.info("=" * 60)
    except Exception as e:
        logger.error("Trend aggregation failed: %s", e, exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()