-- ============================================================================
-- Research Intelligence Pipeline — Hive Schema DDL
-- CS585/DS504 Spring 2026, WPI
-- ============================================================================
-- Run via:  beeline -u jdbc:hive2://localhost:10000 -f hive_schema.hql
-- Or from Python:  cursor.execute(open('hive_schema.hql').read())
-- ============================================================================

CREATE DATABASE IF NOT EXISTS research_intel;
USE research_intel;

-- ────────────────────────────────────────────────────────────────────────────
-- 1. papers — consolidated record per paper (arXiv + S2ORC + OpenAlex)
--    Populated by: Spark consolidation job + BERTopic output merge
--    Queried by:   FastAPI /landscape, /entities, /knowledge-table
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS papers (
    paper_id            STRING      COMMENT 'arXiv ID format, e.g. 2603.24594',
    title               STRING,
    abstract            STRING,
    authors             ARRAY<STRING>,
    submitted_date      DATE        COMMENT 'Date first submitted to arXiv',
    updated_date        DATE        COMMENT 'Date of latest arXiv revision',
    primary_category    STRING      COMMENT 'arXiv primary category, e.g. cs.LG',
    categories          ARRAY<STRING> COMMENT 'All arXiv categories',

    -- S2ORC / OpenAlex enrichment
    citation_count      INT         COMMENT 'From S2ORC + OpenAlex',
    reference_count     INT         COMMENT 'Number of references in the paper',
    influential_citation_count INT  COMMENT 'From Semantic Scholar',

    -- BERTopic outputs
    topic_cluster_id    INT         COMMENT 'BERTopic cluster label (-1 = outlier)',
    topic_cluster       STRING      COMMENT 'Human-readable topic name from BERTopic',
    umap_x              FLOAT       COMMENT 'UMAP 2D x-coordinate for landscape map',
    umap_y              FLOAT       COMMENT 'UMAP 2D y-coordinate for landscape map',

    -- GraphX output
    pagerank_score      FLOAT       COMMENT 'PageRank from citation graph',

    -- NER outputs (populated on-demand or batch)
    methods             ARRAY<STRING> COMMENT 'Extracted method entities',
    datasets            ARRAY<STRING> COMMENT 'Extracted dataset entities',
    tasks               ARRAY<STRING> COMMENT 'Extracted task entities',

    -- Metadata
    source              STRING      COMMENT 'Primary ingestion source: arxiv|s2orc|openalex',
    ingested_at         TIMESTAMP   COMMENT 'When this record was first ingested'
)
COMMENT 'Consolidated paper records from all ingestion sources'
PARTITIONED BY (ingest_year_month STRING COMMENT 'Partition key: YYYY-MM')
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');


-- ────────────────────────────────────────────────────────────────────────────
-- 2. trends — pre-aggregated counts for the Trends Over Time chart
--    Populated by: Spark aggregation job (nightly via Airflow)
--    Queried by:   FastAPI /trends
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trends (
    primary_category    STRING      COMMENT 'arXiv category, e.g. cs.LG',
    topic_cluster       STRING      COMMENT 'BERTopic topic name',
    topic_cluster_id    INT         COMMENT 'BERTopic cluster label',
    year_month          STRING      COMMENT 'Aggregation bucket: YYYY-MM',
    paper_count         INT         COMMENT 'Number of papers in this bucket',
    avg_citation_count  FLOAT       COMMENT 'Mean citations for papers in bucket',
    avg_pagerank        FLOAT       COMMENT 'Mean PageRank for papers in bucket'
)
COMMENT 'Pre-aggregated trend metrics — Spark writes, dashboard reads'
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');


-- ────────────────────────────────────────────────────────────────────────────
-- 3. citation_edges — raw citation graph for Spark GraphX
--    Populated by: S2ORC ingestion (edges written to HDFS, loaded here)
--    Queried by:   Spark GraphX PageRank job
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS citation_edges (
    citing_id           STRING      COMMENT 'arXiv ID of the citing paper',
    cited_id            STRING      COMMENT 'arXiv ID of the cited paper',
    source              STRING      COMMENT 'Where the edge came from: s2orc|openalex'
)
COMMENT 'Citation graph edges for GraphX PageRank'
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');


-- ────────────────────────────────────────────────────────────────────────────
-- Verification queries (run after loading data to sanity-check)
-- ────────────────────────────────────────────────────────────────────────────
-- SHOW TABLES;
-- DESCRIBE FORMATTED papers;
-- SELECT COUNT(*) FROM papers;
-- SELECT COUNT(*) FROM citation_edges;
-- SELECT primary_category, year_month, SUM(paper_count) FROM trends GROUP BY primary_category, year_month LIMIT 20;
