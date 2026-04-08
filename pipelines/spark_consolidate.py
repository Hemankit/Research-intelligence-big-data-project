#!/usr/bin/env python3
"""
spark_consolidate.py — Spark Consolidation Job
================================================
Reads raw JSONL files from all three ingestion sources in HDFS
(arXiv, S2ORC, OpenAlex), deduplicates and merges by paper_id,
and writes the result as Parquet into the Hive `research_intel.papers` table.

HDFS input paths:
    /user/research-intelligence/raw/arxiv/{category}/{date}/*.jsonl
    /user/research-intelligence/raw/s2orc/{category}/{date}/*.jsonl
    /user/research-intelligence/raw/openalex/{category}/{date}/*.jsonl

Hive output:
    research_intel.papers  (partitioned by ingest_year_month)

Usage:
    # From the project root (with Docker running):
    docker exec -it spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/work-dir/pipelines/spark_consolidate.py

    # Or run locally for testing:
    python pipelines/spark_consolidate.py --local
"""

import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, TimestampType, DateType,
    ArrayType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("spark_consolidate")

# Constants 

HDFS_BASE        = "hdfs://namenode:9000/user/research-intelligence/raw"
HIVE_DB          = "research_intel"
HIVE_TABLE       = "papers"
FULLTEXT_TABLE   = "paper_fulltext"

# Schema for each source 
# We read as JSON with permissive mode: fields not present become null.

UNIFIED_SCHEMA = StructType([
    StructField("paper_id",            StringType(), True),
    StructField("title",               StringType(), True),
    StructField("abstract",            StringType(), True),
    StructField("authors",             ArrayType(StringType()), True),
    StructField("submitted_date",      StringType(), True),
    StructField("updated_date",        StringType(), True),
    StructField("primary_category",    StringType(), True),
    StructField("categories",          ArrayType(StringType()), True),
    StructField("citation_count",      IntegerType(), True),
    StructField("reference_count",     IntegerType(), True),
    StructField("influential_citation_count", IntegerType(), True),
    StructField("source",              StringType(), True),
    StructField("ingested_at",         StringType(), True),
])

# Schema for the separate paper_fulltext table (Option B).
# Kept narrow on purpose — only the fields needed for full-text workloads
# (BERTopic, NER, search indexing). Metadata lives in the papers table
# and is joined on paper_id when needed.
FULLTEXT_SCHEMA = StructType([
    StructField("paper_id",   StringType(), True),
    StructField("full_text",  StringType(), True),
    StructField("sections",   ArrayType(
        StructType([
            StructField("heading", StringType(), True),
            StructField("text",    StringType(), True),
        ])
    ), True),
    StructField("doi",        StringType(), True),
    StructField("ingested_at", StringType(), True),
])


# Helper: read + normalize each source 

def read_arxiv(spark: SparkSession) -> DataFrame:
    """Read arXiv JSONL and normalize to unified schema."""
    path = f"{HDFS_BASE}/arxiv/*/*/*"
    logger.info("Reading arXiv data from: %s", path)

    try:
        df = spark.read.json(path)
    except Exception as e:
        logger.warning("No arXiv data found or read error: %s", e)
        return spark.createDataFrame([], UNIFIED_SCHEMA)

    if df.rdd.isEmpty():
        return spark.createDataFrame([], UNIFIED_SCHEMA)

    return df.select(
        F.col("paper_id"),
        F.col("title"),
        F.col("abstract"),
        F.col("authors").cast(ArrayType(StringType())).alias("authors"),
        F.col("submitted_date"),
        F.col("updated_date"),
        F.col("primary_category"),
        F.col("all_categories").alias("categories"),
        F.lit(None).cast(IntegerType()).alias("citation_count"),
        F.lit(None).cast(IntegerType()).alias("reference_count"),
        F.lit(None).cast(IntegerType()).alias("influential_citation_count"),
        F.lit("arxiv").alias("source"),
        F.col("ingested_at"),
    )


def read_s2orc(spark: SparkSession) -> DataFrame:
    """Read S2ORC JSONL (paper records, not edges) and normalize.

    Reads only from s2orc_bulk/ — explicitly excludes s2orc_fulltext/
    (handled separately by consolidate_fulltext) and edges/ (handled by
    consolidate_edges) to avoid schema mismatches.
    """
    path = f"{HDFS_BASE}/s2orc/s2orc_bulk/*/*"
    logger.info("Reading S2ORC metadata from: %s", path)

    try:
        df = spark.read.json(path)
    except Exception as e:
        logger.warning("No S2ORC data found or read error: %s", e)
        return spark.createDataFrame([], UNIFIED_SCHEMA)

    if df.rdd.isEmpty():
        return spark.createDataFrame([], UNIFIED_SCHEMA)

    # Filter out edge records (they have citing_id/cited_id, not paper_id)
    if "citing_id" in df.columns:
        df = df.filter(F.col("citing_id").isNull())

    # S2ORC normalized fields from s2orc.py
    # Actual columns: abstract, authors, categories, citation_count, cited_id,
    # citing_id, date, doi, ingested_at, is_open_access, open_pdf, paper_id,
    # reference_count, s2_paper_id, source, title, venue
    return df.select(
        F.col("paper_id"),
        F.col("title"),
        F.col("abstract"),
        F.col("authors").cast(ArrayType(StringType())).alias("authors"),
        F.col("date").alias("submitted_date"),
        F.lit(None).cast(StringType()).alias("updated_date"),
        F.lit(None).cast(StringType()).alias("primary_category"),
        F.col("categories"),
        F.col("citation_count").cast(IntegerType()),
        F.col("reference_count").cast(IntegerType()),
        F.lit(None).cast(IntegerType()).alias("influential_citation_count"),
        F.lit("s2orc").alias("source"),
        F.col("ingested_at"),
    )


def read_openalex(spark: SparkSession) -> DataFrame:
    """Read OpenAlex JSONL and normalize."""
    path = f"{HDFS_BASE}/openalex/*/*/*"
    logger.info("Reading OpenAlex data from: %s", path)

    try:
        df = spark.read.json(path)
    except Exception as e:
        logger.warning("No OpenAlex data found or read error: %s", e)
        return spark.createDataFrame([], UNIFIED_SCHEMA)

    if df.rdd.isEmpty():
        return spark.createDataFrame([], UNIFIED_SCHEMA)

    return df.select(
        F.col("paper_id"),
        F.col("title"),
        F.col("abstract") if "abstract" in df.columns else F.lit(None).cast(StringType()).alias("abstract"),
        F.col("authors").cast(ArrayType(StringType())).alias("authors") if "authors" in df.columns else F.lit(None).cast(ArrayType(StringType())).alias("authors"),
        F.col("date").alias("submitted_date") if "date" in df.columns else F.lit(None).cast(StringType()).alias("submitted_date"),
        F.lit(None).cast(StringType()).alias("updated_date"),
        F.lit(None).cast(StringType()).alias("primary_category"),
        F.col("categories") if "categories" in df.columns else F.lit(None).cast(ArrayType(StringType())).alias("categories"),
        F.col("citation_count").cast(IntegerType()) if "citation_count" in df.columns else F.lit(None).cast(IntegerType()).alias("citation_count"),
        F.col("reference_count").cast(IntegerType()) if "reference_count" in df.columns else F.lit(None).cast(IntegerType()).alias("reference_count"),
        F.lit(None).cast(IntegerType()).alias("influential_citation_count"),
        F.lit("openalex").alias("source"),
        F.col("ingested_at"),
    )


# Merge logic 

def merge_sources(arxiv_df: DataFrame, s2orc_df: DataFrame, openalex_df: DataFrame) -> DataFrame:
    """
    Union all three sources, then deduplicate by paper_id.

    Priority for conflicting fields:
        1. arXiv  (most reliable metadata: title, abstract, dates, categories)
        2. S2ORC  (best for citations, references, influential counts)
        3. OpenAlex (supplementary citation data)

    For each paper_id, we take the first non-null value per column
    following the priority above.
    """
    # Add a priority column for ordering during dedup
    arxiv_df   = arxiv_df.withColumn("_priority", F.lit(1))
    s2orc_df   = s2orc_df.withColumn("_priority", F.lit(2))
    openalex_df = openalex_df.withColumn("_priority", F.lit(3))

    # Union all
    combined = arxiv_df.unionByName(s2orc_df, allowMissingColumns=True) \
                       .unionByName(openalex_df, allowMissingColumns=True)

    logger.info("Combined records before dedup: %d", combined.count())

    # Filter out records with no paper_id
    combined = combined.filter(F.col("paper_id").isNotNull() & (F.col("paper_id") != ""))

    # Columns to merge (exclude paper_id and _priority)
    merge_cols = [
        "title", "abstract", "authors", "submitted_date", "updated_date",
        "primary_category", "categories", "citation_count", "reference_count",
        "influential_citation_count", "source", "ingested_at",
    ]

    # For each paper_id, take the first non-null value per column
    # ordered by priority (arXiv=1 wins over S2ORC=2 wins over OpenAlex=3)
    merged = combined.groupBy("paper_id").agg(
        *[
            F.first(F.col(c), ignorenulls=True).alias(c)
            for c in merge_cols
        ]
    )

    # For citation counts, take the MAX across sources (most complete count wins)
    # Override the first() aggregation for numeric fields
    merged = combined.groupBy("paper_id").agg(
        F.first("title", ignorenulls=True).alias("title"),
        F.first("abstract", ignorenulls=True).alias("abstract"),
        F.first("authors", ignorenulls=True).alias("authors"),
        F.first("submitted_date", ignorenulls=True).alias("submitted_date"),
        F.first("updated_date", ignorenulls=True).alias("updated_date"),
        F.first("primary_category", ignorenulls=True).alias("primary_category"),
        F.first("categories", ignorenulls=True).alias("categories"),
        F.max("citation_count").alias("citation_count"),
        F.max("reference_count").alias("reference_count"),
        F.max("influential_citation_count").alias("influential_citation_count"),
        F.first("source", ignorenulls=True).alias("source"),
        F.min("ingested_at").alias("ingested_at"),  # earliest ingestion time
    )

    logger.info("Merged records after dedup: %d", merged.count())
    return merged


# Add Hive-compatible columns 

def prepare_for_hive(df: DataFrame) -> DataFrame:
    """
    Add placeholder columns for fields that will be populated later
    (BERTopic, GraphX PageRank, NER) and compute the partition key.
    """
    return df.select(
        # Core fields from ingestion
        F.col("paper_id"),
        F.col("title"),
        F.col("abstract"),
        F.col("authors"),
        F.to_date(F.col("submitted_date")).alias("submitted_date"),
        F.to_date(F.col("updated_date")).alias("updated_date"),
        F.col("primary_category"),
        F.col("categories"),

        # Enrichment fields
        F.col("citation_count"),
        F.col("reference_count"),
        F.col("influential_citation_count"),

        # BERTopic placeholders (filled by BERTopic job later)
        F.lit(None).cast(IntegerType()).alias("topic_cluster_id"),
        F.lit(None).cast(StringType()).alias("topic_cluster"),
        F.lit(None).cast(FloatType()).alias("umap_x"),
        F.lit(None).cast(FloatType()).alias("umap_y"),

        # PageRank scores live in separate pagerank_scores table and are joined in Spark Trends, so no pagerank_score column here
        
        # NER placeholders (filled by NER job later)
        F.lit(None).cast(ArrayType(StringType())).alias("methods"),
        F.lit(None).cast(ArrayType(StringType())).alias("datasets"),
        F.lit(None).cast(ArrayType(StringType())).alias("tasks"),

        # Metadata
        F.col("source"),
        F.to_timestamp(F.col("ingested_at")).alias("ingested_at"),

        # Partition key: YYYY-MM from submitted_date
        F.coalesce(
            F.date_format(F.to_date(F.col("submitted_date")), "yyyy-MM"),
            F.lit("unknown"),
        ).alias("ingest_year_month"),
    )


# Write to Hive 

def write_to_hive(df: DataFrame) -> None:
    """
    Write the consolidated DataFrame to the Hive papers table
    as Parquet, partitioned by ingest_year_month.

    Uses dynamic partition overwrite mode so only affected partitions
    are replaced — historical partitions stay intact.
    """
    table = f"{HIVE_DB}.{HIVE_TABLE}"
    logger.info("Writing %d records to Hive table: %s", df.count(), table)

    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("ingest_year_month") \
        .saveAsTable(table)

    logger.info("Successfully wrote to %s", table)


# Citation edges → Hive 

def consolidate_edges(spark: SparkSession) -> None:
    """
    Read S2ORC citation edge files and write to Hive citation_edges table.
    """
    path = f"{HDFS_BASE}/s2orc/edges/*/*"
    logger.info("Reading citation edges from: %s", path)

    try:
        edges_df = spark.read.json(path)
    except Exception as e:
        logger.warning("No citation edge data found: %s", e)
        return

    if edges_df.rdd.isEmpty():
        logger.info("No citation edges to consolidate.")
        return

    # Select only the fields we need
    edges_out = edges_df.select(
        F.col("citing_id").cast(StringType()),
        F.col("cited_id").cast(StringType()),
        F.lit("s2orc").alias("source"),
    ).filter(
        F.col("citing_id").isNotNull() & F.col("cited_id").isNotNull()
    ).dropDuplicates(["citing_id", "cited_id"])

    table = f"{HIVE_DB}.citation_edges"
    logger.info("Writing %d citation edges to: %s", edges_out.count(), table)

    edges_out.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .saveAsTable(table)

    logger.info("Successfully wrote citation edges to %s", table)


# Full-text table (Option B — separate from papers table) 

def read_s2orc_fulltext(spark: SparkSession) -> DataFrame:
    """
    Read S2ORC full-text JSONL records from s2orc_fulltext/ into a DataFrame
    matching FULLTEXT_SCHEMA.

    Records here were produced by s2orc_bulk_download.py chaining into
    S2ORCIngester corpus mode. Each record has full_text (the complete paper
    body) and sections (list of {heading, text} dicts).

    Authors, title, abstract, and citation counts are intentionally NOT
    read here — they live in the papers table and should be joined on
    paper_id rather than duplicated.
    """
    path = f"{HDFS_BASE}/s2orc/s2orc_fulltext/*/*"
    logger.info("Reading S2ORC full-text records from: %s", path)

    try:
        df = spark.read.json(path)
    except Exception as e:
        logger.warning("No S2ORC full-text data found or read error: %s", e)
        return spark.createDataFrame([], FULLTEXT_SCHEMA)

    if df.rdd.isEmpty():
        logger.info("S2ORC full-text path is empty — skipping.")
        return spark.createDataFrame([], FULLTEXT_SCHEMA)

    # Only keep records that actually have full_text content
    if "full_text" in df.columns:
        df = df.filter(
            F.col("full_text").isNotNull() & (F.col("full_text") != "")
        )

    # Build the select — handle optional columns defensively so the job
    # doesn't crash if a field is missing from older records.
    select_exprs = [F.col("paper_id")]

    select_exprs.append(
        F.col("full_text") if "full_text" in df.columns
        else F.lit(None).cast(StringType()).alias("full_text")
    )
    select_exprs.append(
        F.col("sections") if "sections" in df.columns
        else F.lit(None).cast(FULLTEXT_SCHEMA["sections"].dataType).alias("sections")
    )
    select_exprs.append(
        F.col("doi") if "doi" in df.columns
        else F.lit(None).cast(StringType()).alias("doi")
    )
    select_exprs.append(F.col("ingested_at"))

    return df.select(*select_exprs)


def consolidate_fulltext(spark: SparkSession) -> None:
    """
    Read S2ORC full-text records and write to the Hive paper_fulltext table.

    The paper_fulltext table is separate from papers (Option B) so that:
      - Metadata queries against papers stay fast (no giant text columns)
      - BERTopic, NER, and search indexing jobs can query just this table
      - spark_consolidate --skip-fulltext lets you run the metadata pipeline
        independently without waiting for full-text data

    Deduplicates on paper_id, keeping the record with the longest full_text
    (in case a paper was ingested from multiple shards).
    """
    df = read_s2orc_fulltext(spark)

    if df.rdd.isEmpty():
        logger.info("No full-text records to consolidate — skipping.")
        return

    # Deduplicate: if a paper appears in multiple shards, keep the longest
    # full_text (most complete parse). Use a window function to rank by length.
    from pyspark.sql.window import Window
    w = Window.partitionBy("paper_id").orderBy(
        F.length(F.col("full_text")).desc()
    )
    df_deduped = (
        df.filter(F.col("paper_id").isNotNull() & (F.col("paper_id") != ""))
          .withColumn("_rank", F.row_number().over(w))
          .filter(F.col("_rank") == 1)
          .drop("_rank")
    )

    table = f"{HIVE_DB}.{FULLTEXT_TABLE}"
    count = df_deduped.count()
    logger.info("Writing %d full-text records to Hive table: %s", count, table)

    df_deduped.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .saveAsTable(table)

    logger.info("Successfully wrote %d records to %s", count, table)


# Main 

def main():
    parser = argparse.ArgumentParser(description="Spark consolidation job")
    parser.add_argument(
        "--local", action="store_true",
        help="Run in local mode (for testing outside Docker)",
    )
    parser.add_argument(
        "--skip-edges", action="store_true",
        help="Skip citation edge consolidation",
    )
    parser.add_argument(
        "--skip-fulltext", action="store_true",
        help="Skip full-text consolidation into paper_fulltext table",
    )
    args = parser.parse_args()

    # Build Spark session
    builder = SparkSession.builder \
        .appName("ResearchIntel-Consolidation") \
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
    logger.info("Starting Research Intelligence consolidation job")
    logger.info("=" * 60)

    try:
        # 1. Read from all three sources
        arxiv_df   = read_arxiv(spark)
        s2orc_df   = read_s2orc(spark)
        openalex_df = read_openalex(spark)

        logger.info("Records read — arXiv: %d, S2ORC: %d, OpenAlex: %d",
                     arxiv_df.count(), s2orc_df.count(), openalex_df.count())

        # 2. Merge and deduplicate
        merged = merge_sources(arxiv_df, s2orc_df, openalex_df)

        # 3. Prepare for Hive (add placeholders, compute partition key)
        hive_ready = prepare_for_hive(merged)

        # 4. Write to Hive papers table
        write_to_hive(hive_ready)

        # 5. Consolidate citation edges
        if not args.skip_edges:
            consolidate_edges(spark)

        # 6. Consolidate full-text into separate paper_fulltext table (Option B)
        if not args.skip_fulltext:
            consolidate_fulltext(spark)

        logger.info("=" * 60)
        logger.info("Consolidation complete!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error("Consolidation job failed: %s", e, exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()