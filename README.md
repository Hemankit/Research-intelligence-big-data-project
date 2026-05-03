# Research Intelligence Pipeline

Extract insights from academic research at scale and explore them through an interactive analytics dashboard.

**Stack:** Hadoop/HDFS · Hive · Spark · FastAPI · Elasticsearch · Airflow · BERTopic · SciBERT · HuggingFace Transformers · spaCy · Next.js

**Corpus:** 23,084 papers · 114,233 citation edges · 257 topic clusters · 5,746 arXiv + 17,338 S2ORC

---

## Table of Contents

1. [Quick Start](#1-quick-start)
2. [Architecture Overview](#2-architecture-overview)
3. [Environment Setup](#3-environment-setup)
4. [Data Ingestion](#4-data-ingestion)
5. [Spark Pipeline](#5-spark-pipeline)
6. [NLP Layer — NER](#6-nlp-layer--ner)
7. [NLP Layer — BERTopic](#7-nlp-layer--bertopic)
8. [Elasticsearch Indexing](#8-elasticsearch-indexing)
9. [FastAPI Backend](#9-fastapi-backend)
10. [React / Next.js Dashboard](#10-react--nextjs-dashboard)
11. [Airflow DAG](#11-airflow-dag)
12. [Hive Schema](#12-hive-schema)
13. [New Machine Setup](#13-new-machine-setup)
14. [Verification Queries](#14-verification-queries)

---

## 1. Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/your-team/Research-intelligence-big-data-project
cd Research-intelligence-big-data-project

# 2. Fix line endings (Windows only — required for Hadoop)
sed -i 's/\r//' ./docker/hadoop/hadoop-env.sh

# 3. Start Docker stack
docker compose up -d

# 4. Download ML models (one-time, ~1.5GB)
python scripts/download_models.py

# 5. Install Python dependencies
pip install -r requirements.txt --user

# 6. Start FastAPI backend
export HF_HUB_OFFLINE=1
python -m uvicorn api.app:app --port 8000

# 7. Start the dashboard
cd frontend2/frontend
npm install --legacy-peer-deps
npm run dev
```

Open http://localhost:3000 to access the dashboard.

---

## 2. Architecture Overview

```
arXiv API ──────────────────────────────────────────┐
S2ORC API ──────────────────────────────────────────┤
OpenAlex API ───────────────────────────────────────┤
                                                     ▼
                                              HDFS (raw JSONL)
                                                     │
                         ┌───────────────────────────┤
                         │                           │
                    NER Pipeline              BERTopic Pipeline
                    (dslim/bert)          (MiniLM + UMAP + HDBSCAN)
                         │                           │
                         └───────────────────────────┤
                                                     ▼
                                          spark_consolidate.py
                                          (dedup · merge · enrich)
                                                     │
                                    ┌────────────────┴────────────────┐
                                    ▼                                 ▼
                           spark_pagerank.py                spark_trends.py
                           (influence scores)              (monthly aggregation)
                                    │                                 │
                                    └────────────────┬────────────────┘
                                                     ▼
                                              Apache Hive
                                         (5 tables · Parquet)
                                                     │
                                    ┌────────────────┴────────────────┐
                                    ▼                                 ▼
                             ES Indexer                          FastAPI
                          (23,083 papers)                    (19 endpoints)
                                    │                                 │
                                    └────────────────┬────────────────┘
                                                     ▼
                                        Next.js Dashboard (9 views)
```

**Localhost ports:**

| Service | URL |
|---|---|
| Dashboard | http://localhost:3000 |
| FastAPI | http://localhost:8000 |
| HDFS NameNode UI | http://localhost:9870 |
| Spark Master UI | http://localhost:8080 |
| Airflow UI | http://localhost:8085 |
| Elasticsearch | http://localhost:9200 |
| Hive | localhost:10000 |

---

## 3. Environment Setup

```bash
# Make setup script executable (run once)
chmod +x setup.sh

# Initialize Docker + HDFS environment (Docker Desktop must be running)
./setup.sh

# Start the full stack
docker compose up -d

# Stop the stack
docker compose down

# Stop and remove all data volumes (WARNING: deletes all HDFS data)
docker compose down -v

# View running services
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
```

### Fix Windows Line Endings (Windows only)

If namenode fails to start with `JAVA_HOME does not exist`, run from Git Bash:

```bash
sed -i 's/\r//' ./docker/hadoop/hadoop-env.sh
docker compose down && docker compose up -d
```

### Install Python Dependencies

```bash
pip install -r requirements.txt --user
pip install "elasticsearch>=8.0.0,<9.0.0" --user --force-reinstall
```

### Download ML Models (one-time, ~1.5GB)

```bash
python scripts/download_models.py
```

Downloads: `allenai/scibert_scivocab_cased` · `dslim/bert-base-NER` · `all-MiniLM-L6-v2` · `en_core_web_sm`

---

## 4. Data Ingestion

### arXiv

Pulls paper metadata into HDFS under `raw/arxiv/<category>/<date>/`.

```bash
# Incremental: last 1 day (default)
python -m ingestion.arxiv

# Incremental: last N days
python -m ingestion.arxiv --lookback 7

# Specific categories
python -m ingestion.arxiv --lookback 1 --categories cs.LG cs.CL cs.CV cs.AI cs.IR

# Bulk historical ingest
python -m ingestion.arxiv --bulk --max 5000 --batch-size 500
```

### S2ORC Metadata

Enriches arXiv paper IDs with citation metadata and edges from Semantic Scholar.

```bash
# Bulk ingest by date range
MSYS_NO_PATHCONV=1 python -m ingestion.S2orc --ingest \
  --query "2023-01-01:2024-12-31" \
  --category s2orc_bulk \
  --batch-size 1000 \
  --max 10000

# Enrich specific arXiv IDs
MSYS_NO_PATHCONV=1 python -m ingestion.S2orc --enrich \
  --ids 2401.12345 2401.67890 \
  --category cs.LG
```

### S2ORC Full Text

Downloads full parsed paper bodies with section structure (~1GB per shard).

```bash
# Pilot run — 1 shard, first 100 records (safe test)
MSYS_NO_PATHCONV=1 python -m ingestion.s2orc_bulk_download \
  --shards 1 --local-dir ./s2orc_shards --ingest --max-per-shard 100

# Full single shard ingest
MSYS_NO_PATHCONV=1 python -m ingestion.s2orc_bulk_download \
  --shards 1 --local-dir ./s2orc_shards --ingest --category s2orc_fulltext --cleanup

# Multi-shard run
MSYS_NO_PATHCONV=1 python -m ingestion.s2orc_bulk_download \
  --shards 5 --local-dir ./s2orc_shards --ingest --category s2orc_fulltext --cleanup
```

### OpenAlex

Enriches arXiv IDs with author institutions, concept tags, and citation counts.

> ⚠ OpenAlex has a 2-4 week indexing lag for recent papers. Infrastructure is ready but deferred.

```bash
MSYS_NO_PATHCONV=1 python -m ingestion.Openalex \
  --ids 2603.24594 2603.24587 \
  --category cs.LG
```

### Full Pipeline (all sources)

```bash
MSYS_NO_PATHCONV=1 python -m pipelines.ingestion_main
MSYS_NO_PATHCONV=1 python -m pipelines.ingestion_main --bulk
MSYS_NO_PATHCONV=1 python -m pipelines.ingestion_main --lookback 7 --sources arxiv,s2orc
```

---

## 5. Spark Pipeline

All Spark jobs run inside the `spark-master` container. **Required execution order after every ingestion cycle:**

```
spark_consolidate.py → spark_pagerank.py → spark_trends.py
```

### spark_consolidate.py

Reads all HDFS sources, deduplicates 51,336 raw records → 23,084 unique papers, merges NER and BERTopic outputs, writes all 5 Hive tables.

```bash
MSYS_NO_PATHCONV=1 docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/pipelines/spark_consolidate.py
```

If it fails with `LOCATION_ALREADY_EXISTS`, delete the HDFS location first (PowerShell):

```powershell
docker exec namenode hdfs dfs -rm -r /user/hive/warehouse/research_intel.db/papers
```

### spark_pagerank.py

Computes log-normalized citation influence scores for all papers. Formula: `0.15 + 0.85 × (log(1+citations) - min) / (max - min)` → maps to [0.15, 1.0].

```bash
MSYS_NO_PATHCONV=1 docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/pipelines/spark_pagerank.py
```

### spark_trends.py

Pre-aggregates paper counts and citation metrics by category and month. Produces 4,285 rows covering 28 months.

```bash
MSYS_NO_PATHCONV=1 docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/pipelines/spark_trends.py
```

---

## 6. NLP Layer — NER

Extracts METHOD and TASK entities from paper abstracts using `dslim/bert-base-NER`.

> Always use `--n_jobs 1`. Parallelization causes each worker to reload the 433MB model, making it 10-20x slower.

```bash
# Abstract NER: arXiv papers only (~15 min on CPU)
MSYS_NO_PATHCONV=1 python -m NLP_layer.Bert_ner.ner_main \
  --model dslim/bert-base-NER \
  --input_path /user/research-intelligence/raw \
  --output_path /user/research-intelligence/raw/ner \
  --sources arxiv \
  --n_jobs 1 \
  --batch_size 32

# Abstract NER: all sources
MSYS_NO_PATHCONV=1 python -m NLP_layer.Bert_ner.ner_main \
  --model dslim/bert-base-NER \
  --input_path /user/research-intelligence/raw \
  --output_path /user/research-intelligence/raw/ner \
  --sources arxiv,s2orc \
  --n_jobs 1 \
  --batch_size 32

# Full-text NER: runs on s2orc_fulltext section text
MSYS_NO_PATHCONV=1 python -m NLP_layer.Bert_ner.ner_main \
  --model dslim/bert-base-NER \
  --input_path /user/research-intelligence/raw \
  --output_path /user/research-intelligence/raw/ner \
  --fulltext \
  --max_sections 5 \
  --n_jobs 1 \
  --batch_size 16
```

After NER completes, re-run `spark_consolidate.py` to merge `methods`/`tasks` into the papers table.

---

## 7. NLP Layer — BERTopic

Discovers latent topic clusters using UMAP + HDBSCAN on sentence embeddings (`all-MiniLM-L6-v2`). Produces 257 clusters across 20,487 papers with UMAP x/y coordinates for visualization.

```bash
# Full run: arXiv papers (~60s on CPU)
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

# Re-cluster with cached embeddings (fast — skips encoding)
MSYS_NO_PATHCONV=1 python -m NLP_layer.run_bertopic \
  --input_path /user/research-intelligence/raw \
  --output_path /user/research-intelligence/raw/bertopic \
  --embedding_cache ./cache/embeddings.npy \
  --sources arxiv \
  --min_cluster_size 20 \
  --nr_topics 50
```

After BERTopic completes, re-run `spark_consolidate.py` to merge topic assignments into the papers table.

---

## 8. Elasticsearch Indexing

```bash
# Install correct ES client version (must match ES server 8.x)
pip install "elasticsearch>=8.0.0,<9.0.0" --user --force-reinstall

# Full reindex of both indices (papers + fulltext)
python -m es.elasticsearch_run --mode full

# Full reindex of papers only
python -m es.elasticsearch_run --mode full --index papers

# Full reindex of fulltext only
python -m es.elasticsearch_run --mode full --index fulltext

# Incremental update after ingestion
python -m es.elasticsearch_run --mode incremental --year-month 2026-04
```

### Verification

```bash
# Cluster health
curl http://localhost:9200/_cluster/health?pretty

# Document counts
curl http://localhost:9200/research_intel_papers/_count?pretty
curl http://localhost:9200/research_intel_fulltext/_count?pretty

# Test search
curl -X GET "http://localhost:9200/research_intel_papers/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{"query": {"match": {"abstract": "large language models"}}, "size": 3, "_source": ["paper_id", "title"]}'
```

---

## 9. FastAPI Backend

Serves 19 REST endpoints connecting the dashboard to Hive and Elasticsearch.

```bash
# Install dependencies
pip install fastapi uvicorn pyhive thrift thrift-sasl pure-sasl --user

# Start (offline mode prevents HuggingFace download attempts)
export HF_HUB_OFFLINE=1
python -m uvicorn api.app:app --port 8000
```

### API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Connectivity check (Hive + Elasticsearch) |
| GET | `/api/stats` | Corpus summary statistics (cached) |
| GET | `/api/trends` | Monthly trend data by category and topic |
| GET | `/api/papers/list` | Paginated paper browser with filters |
| GET | `/api/papers/search` | Full-text search via Elasticsearch |
| GET | `/api/papers/influential` | Top papers by PageRank score |
| GET | `/api/papers/{id}` | Full paper detail with NER and citations |
| GET | `/api/topics/landscape` | UMAP coordinates for scatter plot |
| GET | `/api/topics/clusters` | All BERTopic clusters with paper counts |
| GET | `/api/graph/citation/{id}` | Citation graph neighborhood |
| GET | `/api/graph/citation` | Citation edges with filters |
| GET | `/api/methods/adoption` | Method adoption trends over time |
| GET | `/api/entities/trending` | Top NER entities from Hive |
| GET | `/api/entities/trending/es` | Top NER entities via Elasticsearch scroll |
| GET | `/api/entities/timeline` | Entity mention counts over time |
| GET | `/api/pipeline/status` | Pipeline component health status |
| GET | `/analyze` | Curated Reading Path (SciBERT + ES + Hive) |
| GET | `/analyze/fresh` | Curated Reading Path (bypass cache) |
| POST | `/api/query` | Natural language query via analysis pipeline |

> ⚠ **Route order matters** in `api/app.py`: `/papers/list` → `/papers/influential` → `/papers/search` → `/papers/{paper_id}`. Do not reorder.

---

## 10. React / Next.js Dashboard

Built with Next.js 16, React, and shadcn/ui. Connects to FastAPI via a typed TypeScript API client.

```bash
cd frontend2/frontend
npm install --legacy-peer-deps
npm run dev
```

Dashboard available at http://localhost:3000.

### 9 Views

| View | Description |
|---|---|
| **Overview** | Stat cards, bar chart, top papers, NL query bar |
| **Trend Explorer** | Area chart by category, avg citations, top categories |
| **Topic Landscape** | UMAP scatter plot with ES search, cluster filter, point slider |
| **Knowledge Table** | Paper browser with source filter and ES search |
| **Paper Detail** | Citation graph, NER entities, related papers |
| **Graph Stats** | Corpus metrics and pipeline health |
| **Ingestion Log** | Papers grouped by month with source badges |
| **NER Pipeline** | Top methods/tasks bar charts, pipeline architecture |
| **Curated Reading Path** | 5-signal selection engine with SciBERT full-text analysis |

### Warm Up SciBERT Before Demo

SciBERT loads on first `/analyze` call (~30 seconds). Warm it up before presenting:

```bash
curl "http://localhost:8000/analyze?q=transformer+attention+mechanism"
```

---

## 11. Airflow DAG

Scheduled ingestion pipeline running every 2 days at 03:00 UTC.

Task flow: `check_api_health → ingest_arxiv → enrich_s2orc → enrich_openalex`

```bash
# Access Airflow UI (credentials: admin / admin)
open http://localhost:8085

# Trigger a manual run
docker exec -it airflow-webserver airflow dags trigger research_intelligence_ingestion

# Check DAG status
docker exec -it airflow-webserver airflow dags list
```

---

## 12. Hive Schema

Creates all 5 Hive tables: `papers`, `trends`, `citation_edges`, `paper_fulltext`, `pagerank_scores`.

```bash
# Copy schema into container
MSYS_NO_PATHCONV=1 docker cp docker/hive/hive_schema.hql hiveserver2:/tmp/hive_schema.hql

# Apply schema (safe to re-run — uses CREATE TABLE IF NOT EXISTS)
MSYS_NO_PATHCONV=1 docker exec -it hiveserver2 beeline \
  -u jdbc:hive2://localhost:10000 \
  -f /tmp/hive_schema.hql
```

### Hive Tables

| Table | Rows | Description |
|---|---|---|
| `papers` | 23,084 | Metadata, NER entities, BERTopic cluster, UMAP coords, PageRank. Partitioned by `ingest_year_month` |
| `citation_edges` | 114,233 | Directed citation links: `citing_id` (arXiv) → `cited_id` (SHA-1) |
| `pagerank_scores` | 23,084 | Log-normalized influence scores [0.15, 1.0] |
| `trends` | 4,285 | Pre-aggregated monthly metrics by category and topic |
| `paper_fulltext` | 495 | Full-text bodies with section structure (kept separate for query performance) |

---

## 13. New Machine Setup

To transfer the pipeline to a new machine without re-running ingestion:

### Export Docker Volumes (on source machine, PowerShell)

```powershell
docker run --rm -v research-intelligence-big-data-project_namenode_data:/data -v ${PWD}:/backup alpine tar czf /backup/vol_namenode.tar.gz -C /data .
docker run --rm -v research-intelligence-big-data-project_datanode_data:/data -v ${PWD}:/backup alpine tar czf /backup/vol_datanode.tar.gz -C /data .
docker run --rm -v research-intelligence-big-data-project_elasticsearch_data:/data -v ${PWD}:/backup alpine tar czf /backup/vol_elasticsearch.tar.gz -C /data .
docker run --rm -v research-intelligence-big-data-project_hive_metastore_db:/data -v ${PWD}:/backup alpine tar czf /backup/vol_hive_metastore.tar.gz -C /data .
docker run --rm -v research-intelligence-big-data-project_airflow_postgres_db:/data -v ${PWD}:/backup alpine tar czf /backup/vol_airflow_postgres.tar.gz -C /data .
docker run --rm -v research-intelligence-big-data-project_airflow_logs:/data -v ${PWD}:/backup alpine tar czf /backup/vol_airflow_logs.tar.gz -C /data .
```

### Restore on New Machine (PowerShell)

```powershell
# 1. Fix line endings
# Run in Git Bash: sed -i 's/\r//' ./docker/hadoop/hadoop-env.sh

# 2. Start stack once to create volumes
docker compose up -d
docker compose down

# 3. Restore all volumes
docker run --rm -v research-intelligence-big-data-project_namenode_data:/data -v ${PWD}:/backup alpine sh -c "cd /data && tar xzf /backup/vol_namenode.tar.gz"
docker run --rm -v research-intelligence-big-data-project_datanode_data:/data -v ${PWD}:/backup alpine sh -c "cd /data && tar xzf /backup/vol_datanode.tar.gz"
docker run --rm -v research-intelligence-big-data-project_elasticsearch_data:/data -v ${PWD}:/backup alpine sh -c "cd /data && tar xzf /backup/vol_elasticsearch.tar.gz"
docker run --rm -v research-intelligence-big-data-project_hive_metastore_db:/data -v ${PWD}:/backup alpine sh -c "cd /data && tar xzf /backup/vol_hive_metastore.tar.gz"
docker run --rm -v research-intelligence-big-data-project_airflow_postgres_db:/data -v ${PWD}:/backup alpine sh -c "cd /data && tar xzf /backup/vol_airflow_postgres.tar.gz"
docker run --rm -v research-intelligence-big-data-project_airflow_logs:/data -v ${PWD}:/backup alpine sh -c "cd /data && tar xzf /backup/vol_airflow_logs.tar.gz"

# 4. Start the stack
docker compose up -d

# 5. Verify
docker exec hiveserver2 hive -e "SELECT COUNT(*) FROM research_intel.papers;"
# Expected: 23084
```

---

## 14. Verification Queries

Run after each pipeline cycle to confirm data landed correctly.

```bash
docker exec -it hiveserver2 beeline -u jdbc:hive2://localhost:10000
```

```sql
USE research_intel;

-- Table overview
SHOW TABLES;

-- Paper counts and enrichment coverage
SELECT
  COUNT(*) AS total_papers,
  COUNT(topic_cluster_id) AS with_topics,
  COUNT(methods) AS with_ner,
  SUM(CASE WHEN topic_cluster_id = -1 THEN 1 ELSE 0 END) AS outliers
FROM papers;

-- Papers by source
SELECT source, COUNT(*) as cnt FROM papers GROUP BY source;

-- Citation edge count
SELECT COUNT(*) FROM citation_edges;

-- Full-text record count
SELECT COUNT(*) FROM paper_fulltext;

-- PageRank top 10
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

-- Papers with full text
SELECT p.paper_id, p.title, LENGTH(f.full_text) AS chars
FROM papers p
JOIN paper_fulltext f ON p.paper_id = f.paper_id
LIMIT 10;
```
