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
#
# Join strategy: paper_id is the primary join key (arXiv ID when available,
# S2ORC corpusid otherwise). arxiv_id and corpusid are stored as secondary
# keys so downstream jobs can join on whichever is available.
FULLTEXT_SCHEMA = StructType([
    StructField("paper_id",   StringType(), True),
    StructField("arxiv_id",   StringType(), True),
    StructField("corpusid",   StringType(), True),
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


# BERTopic output → papers table merge 

def read_bertopic_output(spark: SparkSession) -> DataFrame:
    """
    Read per-paper BERTopic spark_merge records written by run_bertopic.py.

    The spark_merge records live under:
        raw/bertopic/<date>/spark_merge/<date>/<timestamp>.jsonl

    We use a specific glob targeting the spark_merge subdirectory to avoid
    schema inference errors from mixing assignments/topic_info/coordinates
    files which have different schemas.
    """
    BERTOPIC_SCHEMA = StructType([
        StructField("paper_id",         StringType(),  True),
        StructField("topic_cluster_id", IntegerType(), True),
        StructField("topic_cluster",    StringType(),  True),
        StructField("umap_x",           FloatType(),   True),
        StructField("umap_y",           FloatType(),   True),
    ])

    # Try multiple glob depths to handle both normal and doubled date paths
    dfs = []
    for pattern in ("bertopic/*/spark_merge/*/*", "bertopic/*/spark_merge/*"):
        path = f"{HDFS_BASE}/{pattern}"
        logger.info("Trying BERTopic path: %s", path)
        try:
            df = spark.read.json(path)
            if not df.rdd.isEmpty() and "topic_cluster_id" in df.columns:
                dfs.append(df)
                logger.info("Found BERTopic spark_merge data at: %s", path)
        except Exception:
            pass

    if not dfs:
        logger.warning("No BERTopic spark_merge output found — skipping BERTopic merge.")
        return spark.createDataFrame([], BERTOPIC_SCHEMA)

    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    if combined.rdd.isEmpty():
        return spark.createDataFrame([], BERTOPIC_SCHEMA)

    result = combined.select(
        F.col("paper_id"),
        F.col("topic_cluster_id").cast(IntegerType()),
        F.col("topic_cluster").cast(StringType()),
        F.col("umap_x").cast(FloatType()),
        F.col("umap_y").cast(FloatType()),
    ).filter(
        F.col("paper_id").isNotNull() & F.col("topic_cluster_id").isNotNull()
    ).dropDuplicates(["paper_id"])

    count = result.count()
    logger.info("BERTopic output loaded: %d records", count)
    return result


def merge_bertopic_into_papers(spark: SparkSession) -> None:
    """
    Join BERTopic topic assignments into the papers Hive table, populating
    topic_cluster_id, topic_cluster, umap_x, and umap_y columns.

    Uses the same temp-path pattern as merge_ner_into_papers to avoid
    the Spark read→write same-table restriction.
    """
    bertopic_df = read_bertopic_output(spark)
    if bertopic_df.rdd.isEmpty():
        logger.info("No BERTopic data to merge — skipping.")
        return

    table = f"{HIVE_DB}.{HIVE_TABLE}"
    logger.info("Reading existing papers table for BERTopic merge: %s", table)

    try:
        papers_df = spark.table(table)
    except Exception as e:
        logger.warning("Could not read papers table: %s", e)
        return

    # Drop old placeholder BERTopic columns before joining
    for col in ("topic_cluster_id", "topic_cluster", "umap_x", "umap_y"):
        if col in papers_df.columns:
            papers_df = papers_df.drop(col)

    merged = papers_df.join(
        bertopic_df.select(
            "paper_id", "topic_cluster_id", "topic_cluster", "umap_x", "umap_y"
        ),
        on="paper_id",
        how="left",
    )

    # Materialize to temp path to break read→write cycle
    temp_path = "hdfs://namenode:9000/user/research-intelligence/tmp/papers_bertopic_merge"
    logger.info("Materializing merged DataFrame to temp path: %s", temp_path)
    merged.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("ingest_year_month") \
        .save(temp_path)

    count = spark.read.parquet(temp_path).count()
    logger.info("Writing %d papers with BERTopic columns back to %s", count, table)

    spark.read.parquet(temp_path).write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("ingest_year_month") \
        .saveAsTable(table)

    # Clean up temp path
    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.defaultFS", "hdfs://namenode:9000")
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        fs.delete(
            spark.sparkContext._jvm.org.apache.hadoop.fs.Path(temp_path), True
        )
        logger.info("Cleaned up temp path: %s", temp_path)
    except Exception as e:
        logger.warning("Could not clean up temp path %s: %s", temp_path, e)

    bertopic_count = bertopic_df.count()
    logger.info(
        "BERTopic merge complete — %d papers enriched with topic data", bertopic_count
    )


# NER output → papers table merge 

def read_ner_output(spark: SparkSession) -> DataFrame:
    """
    Read per-paper NER entity records written by ner_main.py.

    NER records are stored under raw/ner/ with a date-partitioned structure.
    Due to a known issue in save_results(), the path may be doubled:
        raw/ner/2026-04-09/2026-04-09/<timestamp>.jsonl
    We use a three-level glob (*/*/*) to handle both the normal two-level
    and the doubled three-level layouts.

    Returns a DataFrame with columns: paper_id, methods, datasets, tasks.
    Returns an empty DataFrame if no NER output exists yet.
    """
    NER_SCHEMA = StructType([
        StructField("paper_id", StringType(), True),
        StructField("methods",  ArrayType(StringType()), True),
        StructField("datasets", ArrayType(StringType()), True),
        StructField("tasks",    ArrayType(StringType()), True),
    ])

    # Try three-level glob first (handles doubled date path from save_results bug)
    # then fall back to two-level glob for correctly structured output
    dfs = []
    for glob_pattern in ("ner/*/*/*", "ner/*/*"):
        path = f"{HDFS_BASE}/{glob_pattern}"
        logger.info("Trying NER path: %s", path)
        try:
            df = spark.read.json(path)
            if not df.rdd.isEmpty():
                dfs.append(df)
                logger.info("Found NER data at: %s", path)
        except Exception:
            pass

    if not dfs:
        logger.warning("No NER output found at any path — skipping NER merge.")
        return spark.createDataFrame([], NER_SCHEMA)

    # Union all found DataFrames and deduplicate
    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    if combined.rdd.isEmpty():
        logger.info("NER output path is empty — skipping NER merge.")
        return spark.createDataFrame([], NER_SCHEMA)

    result = combined.select(
        F.col("paper_id"),
        F.col("methods").cast(ArrayType(StringType()))  if "methods"  in combined.columns
            else F.lit(None).cast(ArrayType(StringType())).alias("methods"),
        F.col("datasets").cast(ArrayType(StringType())) if "datasets" in combined.columns
            else F.lit(None).cast(ArrayType(StringType())).alias("datasets"),
        F.col("tasks").cast(ArrayType(StringType()))    if "tasks"    in combined.columns
            else F.lit(None).cast(ArrayType(StringType())).alias("tasks"),
    ).filter(F.col("paper_id").isNotNull()).dropDuplicates(["paper_id"])

    count = result.count()
    logger.info("NER output loaded: %d records", count)
    return result


def merge_ner_into_papers(spark: SparkSession) -> None:
    """
    Join NER entity output into the papers Hive table, populating
    the methods, datasets, and tasks columns.

    Spark cannot read from and overwrite the same table in one operation.
    We work around this by materializing the merged DataFrame to a temp
    HDFS path first, then overwriting the Hive table from that path.
    The temp path is cleaned up after a successful write.
    """
    ner_df = read_ner_output(spark)
    if ner_df.rdd.isEmpty():
        logger.info("No NER data to merge — skipping.")
        return

    table = f"{HIVE_DB}.{HIVE_TABLE}"
    logger.info("Reading existing papers table for NER merge: %s", table)

    try:
        papers_df = spark.table(table)
    except Exception as e:
        logger.warning("Could not read papers table: %s", e)
        return

    # Drop the old placeholder NER columns before joining
    for col in ("methods", "datasets", "tasks"):
        if col in papers_df.columns:
            papers_df = papers_df.drop(col)

    # Left join — papers without NER output get null arrays
    merged = papers_df.join(
        ner_df.select("paper_id", "methods", "datasets", "tasks"),
        on="paper_id",
        how="left",
    )

    # Step 1: materialize to a temp HDFS path to break the read→write cycle.
    # Spark refuses to overwrite a table it is currently reading from, so we
    # write the merged result to a staging location first.
    temp_path = "hdfs://namenode:9000/user/research-intelligence/tmp/papers_ner_merge"
    logger.info("Materializing merged DataFrame to temp path: %s", temp_path)
    merged.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("ingest_year_month") \
        .save(temp_path)

    # Step 2: read back from temp path and overwrite the Hive table.
    # The papers table is no longer in the query plan so Spark allows the write.
    count = spark.read.parquet(temp_path).count()
    logger.info("Writing %d papers with NER columns back to %s", count, table)

    spark.read.parquet(temp_path).write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("ingest_year_month") \
        .saveAsTable(table)

    # Step 3: clean up temp path
    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.defaultFS", "hdfs://namenode:9000")
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        fs.delete(
            spark.sparkContext._jvm.org.apache.hadoop.fs.Path(temp_path),
            True  # recursive
        )
        logger.info("Cleaned up temp path: %s", temp_path)
    except Exception as e:
        logger.warning("Could not clean up temp path %s: %s", temp_path, e)

    ner_count = ner_df.count()
    logger.info(
        "NER merge complete — %d papers enriched with entity data", ner_count
    )


# Full-text table (Option B — separate from papers table) 

def read_s2orc_fulltext(spark: SparkSession) -> DataFrame:
    """
    Read S2ORC full-text JSONL records from s2orc_fulltext/ into a DataFrame
    matching FULLTEXT_SCHEMA.

    Stores arxiv_id and corpusid as secondary join keys alongside paper_id
    so downstream jobs can join paper_fulltext to papers on whichever key
    is available. This handles the common case where full-text records are
    S2ORC-only papers with no arXiv ID — their paper_id is a numeric corpusid
    that doesn't exist in the papers table, making direct joins return zero rows.
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

    # Extract arxiv_id from paper_id: arXiv IDs match NNNN.NNNNN format.
    # corpusid is the numeric S2ORC ID — store it for S2ORC-only papers.
    arxiv_pattern = r"^\d{4}\.\d{4,6}$"

    select_exprs = [
        F.col("paper_id"),
        # arxiv_id: paper_id itself when it looks like an arXiv ID, else null
        F.when(
            F.col("paper_id").rlike(arxiv_pattern), F.col("paper_id")
        ).otherwise(F.lit(None)).alias("arxiv_id"),
        # corpusid: paper_id itself when it's a pure integer (S2ORC corpusid)
        F.when(
            ~F.col("paper_id").rlike(arxiv_pattern), F.col("paper_id")
        ).otherwise(F.lit(None)).alias("corpusid"),
    ]

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
    parser.add_argument(
        "--skip-ner", action="store_true",
        help="Skip NER merge into papers table (methods/datasets/tasks columns)",
    )
    parser.add_argument(
        "--skip-bertopic", action="store_true",
        help="Skip BERTopic merge into papers table (topic_cluster_id/topic_cluster/umap columns)",
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

        # 7. Merge NER output into papers table (methods/datasets/tasks columns)
        if not args.skip_ner:
            merge_ner_into_papers(spark)

        # 8. Merge BERTopic output into papers table (topic/umap columns)
        if not args.skip_bertopic:
            merge_bertopic_into_papers(spark)

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