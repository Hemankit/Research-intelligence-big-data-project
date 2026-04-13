#!/usr/bin/env python3
"""
app.py — FastAPI Backend for Research Intelligence Dashboard
=============================================================
Serves analytics queries from Hive (structured data) and
Elasticsearch (full-text search) to the React dashboard.

Endpoints:
    GET /health              — Health check
    GET /trends              — Trend data by category/topic over time
    GET /papers              — Paginated paper listing with filters
    GET /papers/{paper_id}   — Single paper detail with PageRank
    GET /papers/top          — Top influential papers by PageRank
    GET /search              — Full-text search via Elasticsearch
    GET /landscape           — UMAP coordinates for topic landscape map
    GET /stats               — Summary statistics for the dashboard

Usage:
    # Install dependencies
    pip install fastapi uvicorn pyhive thrift elasticsearch

    # Run locally (connects to Docker services)
    uvicorn api.app:app --reload --port 8000

    # Or with Docker
    docker run -p 8000:8000 --network pipeline_net ...
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pyhive import hive
from elasticsearch import Elasticsearch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("api")

# Configuration 

HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT = int(os.getenv("HIVE_PORT", "10000"))
HIVE_DB = os.getenv("HIVE_DB", "research_intel")

ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = int(os.getenv("ES_PORT", "9200"))
ES_INDEX = os.getenv("ES_INDEX", "papers")


# Database helpers

def get_hive_connection():
    """Create a new Hive connection via PyHive (no authentication)."""
    return hive.connect(
        host=HIVE_HOST,
        port=HIVE_PORT,
        database=HIVE_DB,
        auth="NONE",
    )


def query_hive(sql: str, params: dict = None) -> list[dict]:
    """
    Execute a HiveQL query and return results as a list of dicts.
    Each dict maps column names to values.
    """
    conn = get_hive_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def get_es_client() -> Elasticsearch:
    """Create an Elasticsearch client."""
    return Elasticsearch(
        [{"host": ES_HOST, "port": ES_PORT, "scheme": "http"}]
    )


# App lifecycle 

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info("Starting Research Intelligence API")
    logger.info("Hive: %s:%d/%s", HIVE_HOST, HIVE_PORT, HIVE_DB)
    logger.info("Elasticsearch: %s:%d", ES_HOST, ES_PORT)
    yield
    logger.info("Shutting down API")


# FastAPI app 

app = FastAPI(
    title="Research Intelligence API",
    description="Analytics backend for the Research Intelligence Dashboard",
    version="0.1.0",
    lifespan=lifespan,
)

# Allow React dashboard to call the API from localhost
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Health check 

@app.get("/health")
def health_check():
    """Check connectivity to Hive and Elasticsearch."""
    status = {"api": "ok", "hive": "unknown", "elasticsearch": "unknown"}

    try:
        result = query_hive("SELECT 1")
        status["hive"] = "ok"
    except Exception as e:
        status["hive"] = f"error: {str(e)}"

    try:
        es = get_es_client()
        if es.ping():
            status["elasticsearch"] = "ok"
        else:
            status["elasticsearch"] = "unreachable"
    except Exception as e:
        status["elasticsearch"] = f"error: {str(e)}"

    return status


# Trends endpoint 

@app.get("/trends")
def get_trends(
    category: Optional[str] = Query(None, description="Filter by arXiv category (e.g. cs.LG)"),
    topic: Optional[str] = Query(None, description="Filter by topic cluster name"),
    start: Optional[str] = Query(None, description="Start month (YYYY-MM)"),
    end: Optional[str] = Query(None, description="End month (YYYY-MM)"),
):
    """
    Return trend data from the pre-aggregated trends table.
    Powers the Trend Explorer chart on the dashboard.
    """
    sql = """
        SELECT primary_category, topic_cluster, year_month,
               paper_count, avg_citation_count, avg_pagerank
        FROM trends
        WHERE 1=1
    """
    if category:
        sql += f" AND primary_category = '{category}'"
    if topic:
        sql += f" AND topic_cluster = '{topic}'"
    if start:
        sql += f" AND year_month >= '{start}'"
    if end:
        sql += f" AND year_month <= '{end}'"

    sql += " ORDER BY year_month ASC"

    try:
        results = query_hive(sql)
        return {"count": len(results), "trends": results}
    except Exception as e:
        logger.error("Trends query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# Papers listing 

@app.get("/papers")
def get_papers(
    category: Optional[str] = Query(None, description="Filter by primary_category"),
    source: Optional[str] = Query(None, description="Filter by source (arxiv, s2orc, openalex)"),
    limit: int = Query(20, ge=1, le=100, description="Results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    sort_by: str = Query("citation_count", description="Sort field"),
    order: str = Query("DESC", description="Sort order (ASC/DESC)"),
):
    """
    Return paginated paper listings with optional filters.
    Powers the Knowledge Table view on the dashboard.
    """
    # Validate sort order
    order = order.upper()
    if order not in ("ASC", "DESC"):
        order = "DESC"

    # Validate sort field to prevent injection
    allowed_sorts = {"citation_count", "submitted_date", "title", "paper_id"}
    if sort_by not in allowed_sorts:
        sort_by = "citation_count"

    sql = f"""
        SELECT p.paper_id, p.title, p.abstract, p.authors,
               p.submitted_date, p.primary_category, p.categories,
               p.citation_count, p.reference_count,
               p.topic_cluster, p.topic_cluster_id,
               s.pagerank_score,
               p.source
        FROM papers p
        LEFT JOIN pagerank_scores s ON p.paper_id = s.paper_id
        WHERE 1=1
    """
    if category:
        sql += f" AND p.primary_category = '{category}'"
    if source:
        sql += f" AND p.source = '{source}'"

    sql += f" ORDER BY {sort_by} {order} LIMIT {limit} OFFSET {offset}"

    try:
        results = query_hive(sql)
        return {"count": len(results), "offset": offset, "papers": results}
    except Exception as e:
        logger.error("Papers query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# Single paper detail

@app.get("/papers/top")
def get_top_papers(
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(10, ge=1, le=50, description="Number of top papers"),
):
    """
    Return the most influential papers ranked by PageRank score.
    Powers the Influential Papers section on the dashboard.
    """
    sql = f"""
        SELECT p.paper_id, p.title, p.abstract, p.authors,
               p.submitted_date, p.primary_category,
               p.citation_count,
               s.pagerank_score
        FROM papers p
        JOIN pagerank_scores s ON p.paper_id = s.paper_id
        WHERE s.pagerank_score IS NOT NULL
    """
    if category:
        sql += f" AND p.primary_category = '{category}'"

    sql += f" ORDER BY s.pagerank_score DESC LIMIT {limit}"

    try:
        results = query_hive(sql)
        return {"count": len(results), "papers": results}
    except Exception as e:
        logger.error("Top papers query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/papers/{paper_id}")
def get_paper_detail(paper_id: str):
    """
    Return full details for a single paper, including PageRank score
    and citation neighborhood.
    """
    # Paper details
    sql = f"""
        SELECT p.*, s.pagerank_score
        FROM papers p
        LEFT JOIN pagerank_scores s ON p.paper_id = s.paper_id
        WHERE p.paper_id = '{paper_id}'
    """

    try:
        results = query_hive(sql)
        if not results:
            raise HTTPException(status_code=404, detail=f"Paper {paper_id} not found")

        paper = results[0]

        # Get citation neighbors
        citing_sql = f"""
            SELECT citing_id FROM citation_edges WHERE cited_id = '{paper_id}'
        """
        cited_by_sql = f"""
            SELECT cited_id FROM citation_edges WHERE citing_id = '{paper_id}'
        """

        paper["cited_by"] = [r["citing_id"] for r in query_hive(citing_sql)]
        paper["references"] = [r["cited_id"] for r in query_hive(cited_by_sql)]
        paper["cited_by_count"] = len(paper["cited_by"])
        paper["references_count"] = len(paper["references"])

        return paper
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Paper detail query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# Search (Elasticsearch) 

@app.get("/search")
def search_papers(
    q: str = Query(..., description="Search query"),
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(20, ge=1, le=100, description="Max results"),
):
    """
    Full-text search over paper titles and abstracts via Elasticsearch.
    Falls back to Hive LIKE query if Elasticsearch is unavailable.
    """
    # Try Elasticsearch first
    try:
        es = get_es_client()
        if es.ping():
            body = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "multi_match": {
                                    "query": q,
                                    "fields": ["title^3", "abstract"],
                                    "type": "best_fields",
                                    "fuzziness": "AUTO",
                                }
                            }
                        ]
                    }
                },
                "size": limit,
            }

            if category:
                body["query"]["bool"]["filter"] = [
                    {"term": {"primary_category": category}}
                ]

            resp = es.search(index=ES_INDEX, body=body)
            hits = resp.get("hits", {}).get("hits", [])
            papers = [
                {**hit["_source"], "score": hit["_score"]}
                for hit in hits
            ]
            return {
                "count": len(papers),
                "source": "elasticsearch",
                "papers": papers,
            }
    except Exception as e:
        logger.warning("Elasticsearch unavailable, falling back to Hive: %s", e)

    # Fallback: Hive LIKE query
    safe_q = q.replace("'", "''")
    sql = f"""
        SELECT p.paper_id, p.title, p.abstract, p.authors,
               p.submitted_date, p.primary_category,
               p.citation_count,
               s.pagerank_score
        FROM papers p
        LEFT JOIN pagerank_scores s ON p.paper_id = s.paper_id
        WHERE (p.title LIKE '%{safe_q}%' OR p.abstract LIKE '%{safe_q}%')
    """
    if category:
        sql += f" AND p.primary_category = '{category}'"
    sql += f" LIMIT {limit}"

    try:
        results = query_hive(sql)
        return {
            "count": len(results),
            "source": "hive_fallback",
            "papers": results,
        }
    except Exception as e:
        logger.error("Search fallback failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# Landscape (UMAP coordinates) 

@app.get("/landscape")
def get_landscape(
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(5000, ge=1, le=10000, description="Max points"),
):
    """
    Return UMAP 2D coordinates for the topic landscape map.
    Only returns data after BERTopic has run.
    """
    sql = f"""
        SELECT p.paper_id, p.title, p.primary_category,
               p.topic_cluster, p.topic_cluster_id,
               p.umap_x, p.umap_y,
               s.pagerank_score
        FROM papers p
        LEFT JOIN pagerank_scores s ON p.paper_id = s.paper_id
        WHERE p.umap_x IS NOT NULL AND p.umap_y IS NOT NULL
    """
    if category:
        sql += f" AND p.primary_category = '{category}'"
    sql += f" LIMIT {limit}"

    try:
        results = query_hive(sql)
        return {
            "count": len(results),
            "note": "Empty until BERTopic has run" if not results else None,
            "points": results,
        }
    except Exception as e:
        logger.error("Landscape query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# Dashboard stats 

@app.get("/stats")
def get_stats():
    """
    Return summary statistics for the dashboard insight cards.
    """
    try:
        paper_stats = query_hive("""
            SELECT
                COUNT(*) as total_papers,
                COUNT(DISTINCT primary_category) as total_categories,
                MIN(submitted_date) as earliest_paper,
                MAX(submitted_date) as latest_paper,
                AVG(citation_count) as avg_citations
            FROM papers
        """)[0]

        edge_stats = query_hive("""
            SELECT COUNT(*) as total_edges
            FROM citation_edges
        """)[0]

        pagerank_stats = query_hive("""
            SELECT
                COUNT(*) as scored_papers,
                MAX(pagerank_score) as max_pagerank,
                AVG(pagerank_score) as avg_pagerank
            FROM pagerank_scores
        """)[0]

        trend_stats = query_hive("""
            SELECT
                COUNT(DISTINCT year_month) as months_covered,
                COUNT(DISTINCT topic_cluster) as topic_count
            FROM trends
        """)[0]

        return {
            "papers": paper_stats,
            "citations": edge_stats,
            "pagerank": pagerank_stats,
            "trends": trend_stats,
        }
    except Exception as e:
        logger.error("Stats query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# Citation neighborhood 

@app.get("/citations/{paper_id}")
def get_citations(
    paper_id: str,
    direction: str = Query("both", description="cited_by, references, or both"),
    limit: int = Query(50, ge=1, le=200),
):
    """
    Return the citation neighborhood for a paper.
    Supports the citation graph visualization.
    """
    result = {"paper_id": paper_id}

    try:
        if direction in ("cited_by", "both"):
            sql = f"""
                SELECT e.citing_id as paper_id, p.title, s.pagerank_score
                FROM citation_edges e
                LEFT JOIN papers p ON e.citing_id = p.paper_id
                LEFT JOIN pagerank_scores s ON e.citing_id = s.paper_id
                WHERE e.cited_id = '{paper_id}'
                LIMIT {limit}
            """
            result["cited_by"] = query_hive(sql)

        if direction in ("references", "both"):
            sql = f"""
                SELECT e.cited_id as paper_id, p.title, s.pagerank_score
                FROM citation_edges e
                LEFT JOIN papers p ON e.cited_id = p.paper_id
                LEFT JOIN pagerank_scores s ON e.cited_id = s.paper_id
                WHERE e.citing_id = '{paper_id}'
                LIMIT {limit}
            """
            result["references"] = query_hive(sql)

        return result
    except Exception as e:
        logger.error("Citations query failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# Run 

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)