# Research Intelligence — Big Data Pipeline

Extract insights from academic research sources at scale and display analytics in an interactive dashboard.

**Stack:** Hadoop/HDFS · Hive · Spark · FastAPI · Elasticsearch · Airflow · BERTopic · HuggingFace Transformers · spaCy

---

## Table of Contents

1. [Environment Setup](#1-environment-setup)
2. [Ingestion — arXiv](#2-ingestion--arxiv)
3. [Ingestion — S2ORC Metadata](#3-ingestion--s2orc-metadata)
4. [Ingestion — S2ORC Full Text](#4-ingestion--s2orc-full-text)
5. [Ingestion — OpenAlex](#5-ingestion--openalex)
6. [Spark Jobs](#6-spark-jobs)
7. [NLP Layer — NER](#7-nlp-layer--ner)
8. [NLP Layer — BERTopic](#8-nlp-layer--bertopic)
9. [Hive Schema](#9-hive-schema)
10. [FastAPI Backend](#10-fastapi-backend)
11. [Airflow DAG](#11-airflow-dag)
12. [Verification Queries](#12-verification-queries)

---

## 1. Environment Setup

\`\`\`bash
# Make setup script executable (run once)
chmod +x setup.sh

# Initialize Docker + HDFS environment (run once — Docker Desktop must be running)
./setup.sh

# Start the full stack
docker compose up -d

# Stop the stack when done
docker compose down

# Stop and remove all data volumes (WARNING: deletes all HDFS data)
docker compose down -v

# View running services and their ports
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
\`\`\`

# One-time setup for all required models; run before first use
python scripts/download_models.py

**Localhost ports:**
| Service | URL |
|---|---|
| HDFS NameNode UI | http://localhost:9870 |
| Spark Master UI | http://localhost:8080 |
| Airflow UI | http://localhost:8085 |
| Hive | localhost:10000 |
| Elasticsearch | http://localhost:9200 |
| FastAPI | http://localhost:8000 |

---

## 2. Ingestion: arXiv

Pulls paper metadata from the arXiv API into HDFS under `raw/arxiv/<category>/<date>/`.

\`\`\`bash
# Incremental: last 1 day (default)
python -m ingestion.arxiv

# Incremental: last N days
python -m ingestion.arxiv --lookback 7

# Incremental: specific categories
python -m ingestion.arxiv --lookback 1 --categories cs.LG cs.CL cs.CV cs.AI cs.IR

# Bulk historical ingest (caution: may trigger 429 rate limits from arXiv)
python -m ingestion.arxiv --bulk --max 1000 --batch-size 200

# Bulk: larger dataset
python -m ingestion.arxiv --bulk --max 5000 --batch-size 500
\`\`\`

---

## 3. Ingestion: S2ORC Metadata

Enriches existing arXiv paper IDs with citation metadata and edges from Semantic Scholar.

\`\`\`bash
# Bulk ingest by date range (metadata + abstracts, no full text)
MSYS_NO_PATHCONV=1 python -m ingestion.S2orc --ingest \
  --query "2023-01-01:2024-12-31" \
  --category s2orc_bulk \
  --batch-size 1000 \
  --max 10000

# Enrich specific arXiv IDs with S2ORC metadata + citation edges
MSYS_NO_PATHCONV=1 python -m ingestion.S2orc --enrich \
  --ids 2401.12345 2401.67890 \
  --category cs.LG

# Enrich all IDs from an existing HDFS file
MSYS_NO_PATHCONV=1 python -m ingestion.S2orc --enrich \
  --from-hdfs /user/research-intelligence/raw/s2orc/s2orc_bulk/2026-04-08/20260408_014236.jsonl \
  --category s2orc_bulk
\`\`\`

---

## 4. Ingestion: S2ORC Full Text

Downloads S2ORC bulk corpus shards (full parsed paper bodies with section structure).
Each shard is ~1 GB compressed (~400K papers). Raw shards stay on local disk; normalized records go to HDFS under `raw/s2orc/s2orc_fulltext/`.

\`\`\`bash
# List available shards without downloading (verifies API key access)
MSYS_NO_PATHCONV=1 python -m ingestion.s2orc_bulk_download --list-only

# Pilot run — 1 shard, first 100 records only (safe test)
MSYS_NO_PATHCONV=1 python -m ingestion.s2orc_bulk_download \
  --shards 1 \
  --local-dir ./s2orc_shards \
  --ingest \
  --max-per-shard 100

# Full single shard ingest with cleanup of raw .gz after ingest
MSYS_NO_PATHCONV=1 python -m ingestion.s2orc_bulk_download \
  --shards 1 \
  --local-dir ./s2orc_shards \
  --ingest \
  --category s2orc_fulltext \
  --cleanup

# Multi-shard run: 5 shards (~2M papers)
MSYS_NO_PATHCONV=1 python -m ingestion.s2orc_bulk_download \
  --shards 5 \
  --local-dir ./s2orc_shards \
  --ingest \
  --category s2orc_fulltext \
  --cleanup

# Resume from a specific shard index
MSYS_NO_PATHCONV=1 python -m ingestion.s2orc_bulk_download \
  --shards 3 \
  --start-shard 5 \
  --local-dir ./s2orc_shards \
  --ingest \
  --category s2orc_fulltext \
  --cleanup
\`\`\`

---

## 5. Ingestion" OpenAlex

Enriches arXiv IDs with author institutions, concept tags, and citation counts from OpenAlex.

\`\`\`bash
# Enrich specific arXiv IDs
MSYS_NO_PATHCONV=1 python -m ingestion.Openalex \
  --ids 2603.24594 2603.24587 2603.24580 2603.24567 2603.24562 \
  --category cs.LG

# Run full pipeline (arXiv + S2ORC + OpenAlex) via ingestion_main
MSYS_NO_PATHCONV=1 python -m pipelines.ingestion_main

# Full pipeline with config file
MSYS_NO_PATHCONV=1 python -m pipelines.ingestion_main --config config/ingestion.yaml

# Full pipeline: bulk historical mode
MSYS_NO_PATHCONV=1 python -m pipelines.ingestion_main --bulk

# Full pipeline: specific sources only
MSYS_NO_PATHCONV=1 python -m pipelines.ingestion_main --sources arxiv,s2orc

# Full pipeline: last 7 days, arXiv + OpenAlex only
MSYS_NO_PATHCONV=1 python -m pipelines.ingestion_main --lookback 7 --sources arxiv,openalex
\`\`\`

---

## 6. Spark Jobs

All Spark jobs run inside the `spark-master` container via `spark-submit`.

### Consolidation (run after every ingestion)
Reads all HDFS sources, deduplicates, merges NER and BERTopic outputs, writes to Hive.

\`\`\`bash
# Full consolidation (papers + edges + fulltext + NER merge + BERTopic merge)
MSYS_NO_PATHCONV=1 docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/pipelines/spark_consolidate.py

# Skip specific stages
MSYS_NO_PATHCONV=1 docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/pipelines/spark_consolidate.py \
  --skip-edges --skip-fulltext --skip-ner --skip-bertopic
\`\`\`

### PageRank
Computes citation graph PageRank scores. Run after consolidation.

\`\`\`bash
MSYS_NO_PATHCONV=1 docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/pipelines/spark_pagerank.py
\`\`\`

### Trends
Aggregates paper counts and citation metrics by category and month. Run after PageRank.

\`\`\`bash
MSYS_NO_PATHCONV=1 docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/pipelines/spark_trends.py
\`\`\`

### Recommended run order after ingestion
\`\`\`
spark_consolidate.py → spark_pagerank.py → spark_trends.py
\`\`\`

---

## 7. NLP Layer: NER

Extracts METHOD and TASK entities from paper abstracts or full-text sections using a BERT-based NER model.
Results are written to HDFS and merged into the `papers` Hive table via `spark_consolidate.py`.

\`\`\`bash
# Abstract NER: arXiv papers only (recommended, ~9 min on CPU)
MSYS_NO_PATHCONV=1 python -m NLP_layer.Bert_ner.ner_main \
  --model dslim/bert-base-NER \
  --input_path /user/research-intelligence/raw \
  --output_path /user/research-intelligence/raw/ner \
  --sources arxiv \
  --n_jobs 1 \
  --batch_size 32

# Abstract NER: all sources (arXiv + S2ORC)
MSYS_NO_PATHCONV=1 python -m NLP_layer.Bert_ner.ner_main \
  --model dslim/bert-base-NER \
  --input_path /user/research-intelligence/raw \
  --output_path /user/research-intelligence/raw/ner \
  --sources arxiv,s2orc \
  --n_jobs 1 \
  --batch_size 32

# Full-text NER: runs on s2orc_fulltext/ section text (slower)
MSYS_NO_PATHCONV=1 python -m NLP_layer.Bert_ner.ner_main \
  --model dslim/bert-base-NER \
  --input_path /user/research-intelligence/raw \
  --output_path /user/research-intelligence/raw/ner \
  --fulltext \
  --max_sections 5 \
  --n_jobs 1 \
  --batch_size 16
\`\`\`

> **After NER completes:** re-run `spark_consolidate.py` to merge `methods`/`tasks` columns into the `papers` table.

> **Note:** `--n_jobs 1` is required on CPU. Using `--n_jobs -2` causes each joblib worker
> to reload the 433MB model, making it 10-20x slower.

---

## 8. NLP Layer: BERTopic

Discovers latent research topic clusters across the abstract corpus using UMAP + HDBSCAN.
Embeddings are cached to disk so re-clustering experiments skip the encoding step.

\`\`\`bash
# Full run: arXiv papers, embed + cluster (~60s on CPU)
MSYS_NO_PATHCONV=1 python -m NLP_layer.run_bertopic \
  --input_path /user/research-intelligence/raw \
  --output_path /user/research-intelligence/raw/bertopic \
  --embedding_cache ./cache/embeddings.npy \
  --sources arxiv \
  --min_cluster_size 10

# All sources
MSYS_NO_PATHCONV=1 python -m NLP_layer.run_bertopic \
  --input_path /user/research-intelligence/raw \
  --output_path /user/research-intelligence/raw/bertopic \
  --embedding_cache ./cache/embeddings.npy \
  --sources arxiv,s2orc,openalex \
  --min_cluster_size 10

# Re-cluster with different hyperparameters (uses cached embeddings — fast)
MSYS_NO_PATHCONV=1 python -m NLP_layer.run_bertopic \
  --input_path /user/research-intelligence/raw \
  --output_path /user/research-intelligence/raw/bertopic \
  --embedding_cache ./cache/embeddings.npy \
  --sources arxiv \
  --min_cluster_size 20 \
  --nr_topics 50
\`\`\`

> **After BERTopic completes:** re-run `spark_consolidate.py` to merge
> `topic_cluster_id`, `topic_cluster`, `umap_x`, `umap_y` into the `papers` table.

---

## 9. Hive Schema

Creates all four Hive tables: `papers`, `trends`, `citation_edges`, `paper_fulltext`.

\`\`\`bash
# Copy schema file into the container
MSYS_NO_PATHCONV=1 docker cp docker/hive/hive_schema.hql hiveserver2:/tmp/hive_schema.hql

# Apply schema (safe to re-run which uses CREATE TABLE IF NOT EXISTS)
MSYS_NO_PATHCONV=1 docker exec -it hiveserver2 beeline \
  -u jdbc:hive2://localhost:10000 \
  -f /tmp/hive_schema.hql
\`\`\`

---

## 10. FastAPI Backend

Serves analytics queries from Hive and Elasticsearch to the React dashboard.

\`\`\`bash
# Install dependencies
pip install fastapi uvicorn pyhive thrift elasticsearch

# Run locally (connects to Docker services on localhost)
export HF_HUB_OFFLINE=1
python -m uvicorn api.app:app --port 8000
\`\`\`

**Endpoints:**
| Method | Path | Description |
|---|---|---|
| GET | /health | Connectivity check (Hive + Elasticsearch) |
| GET | /stats | Dashboard summary statistics |
| GET | /trends | Trend data by category/topic over time |
| GET | /papers | Paginated paper listing with filters |
| GET | /papers/top | Top papers by PageRank score |
| GET | /papers/{id} | Single paper detail with citation neighbors |
| GET | /search?q=... | Full-text search (Elasticsearch with Hive fallback) |
| GET | /landscape | UMAP coordinates for topic landscape map |
| GET | /citations/{id} | Citation neighborhood for a paper |

---

## 11. Airflow DAG

Scheduled ingestion pipeline that runs every 2 days at 03:00 UTC.
Task flow: `check_api_health → ingest_arxiv → enrich_s2orc → enrich_openalex`

\`\`\`bash
# Copy DAG to Airflow's dags folder
cp dags/ingestion_dag.py dags/

# Access Airflow UI (default credentials: admin / admin)
open http://localhost:8081

# Trigger a manual run via CLI
docker exec -it airflow-webserver airflow dags trigger research_intelligence_ingestion

# Check DAG status
docker exec -it airflow-webserver airflow dags list
\`\`\`

---

## 12. Verification Queries

Run these after each pipeline run to confirm data landed correctly.

\`\`\`bash
# Open Beeline
docker exec -it hiveserver2 beeline -u jdbc:hive2://localhost:10000
\`\`\`

\`\`\`sql
USE research_intel;

-- Table overview
SHOW TABLES;

-- Paper counts and enrichment coverage
SELECT
  COUNT(*) AS total_papers,
  COUNT(topic_cluster_id) AS with_topics,
  COUNT(methods) AS with_methods,
  SUM(CASE WHEN topic_cluster_id = -1 THEN 1 ELSE 0 END) AS outliers
FROM papers;

-- Sample papers with topic + NER columns populated
SELECT paper_id, title, topic_cluster, methods, tasks
FROM papers
WHERE SIZE(methods) > 0
LIMIT 5;

-- Citation edge count
SELECT COUNT(*) FROM citation_edges;

-- Full-text record count
SELECT COUNT(*) FROM paper_fulltext;

-- PageRank top 10 papers
SELECT p.paper_id, p.title, s.pagerank_score
FROM papers p
JOIN pagerank_scores s ON p.paper_id = s.paper_id
ORDER BY s.pagerank_score DESC
LIMIT 10;

-- Trends sample
SELECT primary_category, year_month, paper_count, avg_citation_count
FROM trends
ORDER BY paper_count DESC
LIMIT 20;

-- Papers joined with full text
SELECT p.paper_id, p.title, LENGTH(f.full_text) AS chars, SIZE(f.sections) AS num_sections
FROM papers p
JOIN paper_fulltext f ON p.paper_id = f.paper_id
LIMIT 10;
\`\`\`

### 13. Elasticsearch Indexing

# --- Prerequisites (one-time install) ---
pip install "elasticsearch>=8.0.0,<9.0.0" pyhive thrift

# --- Initial setup (run after spark_consolidate) ---

# Full reindex of both indices
python -m es.elasticsearch_run --mode full

# Full reindex of papers index only
python -m es.elasticsearch_run --mode full --index papers

# Full reindex of fulltext index only
python -m es.elasticsearch_run --mode full --index fulltext

# --- Incremental update (run after each ingestion + consolidation cycle) ---

python -m es.elasticsearch_run --mode incremental --year-month 2026-04

# --- Verification ---

# Check cluster health
curl http://localhost:9200/_cluster/health?pretty

# Count documents in each index
curl http://localhost:9200/research_intel_papers/_count?pretty
curl http://localhost:9200/research_intel_fulltext/_count?pretty

# List all indices
curl http://localhost:9200/_cat/indices?v

# Test a search query
curl -X GET "http://localhost:9200/research_intel_papers/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{"query": {"match": {"abstract": "large language models"}}, "size": 3, "_source": ["paper_id", "title", "topic_cluster"]}'