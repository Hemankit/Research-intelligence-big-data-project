-- ============================================================================
-- Research Intelligence Pipeline — Hive Schema DDL
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
    -- NOTE: pagerank_score is NOT stored here — it lives in the separate
    -- pagerank_scores table (written by spark_pagerank.py) and is joined
    -- in at query time. spark_consolidate.py does not write this column.

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
-- 4. paper_fulltext — parsed full-text bodies from S2ORC bulk corpus
--    Populated by: spark_consolidate.py consolidate_fulltext()
--                  (reads from HDFS raw/s2orc/s2orc_fulltext/)
--    Queried by:   BERTopic job, NER pipeline, Elasticsearch indexer
--    NOTE: Kept separate from papers (Option B) so metadata queries stay
--    fast. Join on paper_id when full text is needed.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS paper_fulltext (
    paper_id            STRING      COMMENT 'arXiv ID or S2ORC corpusid — joins to papers.paper_id',
    full_text           STRING      COMMENT 'Complete parsed paper body (~45K chars avg)',
    sections            ARRAY<STRUCT<
                            heading: STRING,
                            text:    STRING
                        >>          COMMENT 'Section list with headings and body text',
    doi                 STRING      COMMENT 'DOI if available',
    ingested_at         STRING      COMMENT 'When this record was ingested from the bulk shard'
)
COMMENT 'Full-text paper bodies from S2ORC bulk corpus — separate from papers table for performance'
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');



-- ────────────────────────────────────────────────────────────────────────────
-- Verification queries (run after loading data to sanity-check)
-- ────────────────────────────────────────────────────────────────────────────
-- SHOW TABLES;
-- DESCRIBE FORMATTED papers;
-- DESCRIBE FORMATTED paper_fulltext;
-- SELECT COUNT(*) FROM papers;
-- SELECT COUNT(*) FROM paper_fulltext;
-- SELECT COUNT(*) FROM citation_edges;
-- SELECT primary_category, year_month, SUM(paper_count) FROM trends GROUP BY primary_category, year_month LIMIT 20;
-- Example join — papers metadata + full text:
-- SELECT p.paper_id, p.title, LENGTH(f.full_text) AS chars, SIZE(f.sections) AS num_sections
-- FROM papers p JOIN paper_fulltext f ON p.paper_id = f.paper_id LIMIT 10;
