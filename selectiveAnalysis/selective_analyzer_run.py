"""
selective_analyzer_run.py
-------------------------
Entry point for the Selective Full-Text Analysis Layer (Section 2.5).

Triggered on-demand by the FastAPI backend when a user submits a query
from the dashboard. Orchestrates the full selective analysis pipeline:

  1. Check cache  — return cached result immediately if available
  2. Select       — SelectionEngine identifies 10-50 high-value papers
  3. Analyze      — FullTextAnalyzer runs targeted NLP on selected papers
  4. Organize     — PaperOrganizer groups papers into reading categories
  5. Cache result — store for reuse on repeated queries
  6. Return       — structured reading guide to FastAPI

Usage from FastAPI (call once at startup, then per request):

    from selectiveAnalysis.selective_analyzer_run import build_pipeline, run_analysis

    pipeline = build_pipeline()
    result   = run_analysis(query="graph neural networks", pipeline=pipeline)

Dependencies: selector.py, fulltext_Analyzer.py, organizer.py, cache.py,
              es/client.py, NLP_layer/shared/
"""

import logging
import time

from pyhive import hive as pyhive_hive

from .selector import SelectionEngine
from .fulltext_Analyzer import FullTextAnalyzer
from .organizer import PaperOrganizer
from .cache import AnalysisCache
from es.client import ESClient
from NLP_layer.shared.text_preprocessing import TextCleaner
from NLP_layer.shared.Tokenizer import Tokenizer

logger = logging.getLogger(__name__)

# Hive connection parameters — matches es/indexer.py
HIVE_HOST     = "localhost"
HIVE_PORT     = 10000
HIVE_DATABASE = "research_intel"


def _get_hive_connection():
    """Open a pyhive connection to HiveServer2."""
    return pyhive_hive.Connection(
        host=HIVE_HOST,
        port=HIVE_PORT,
        database=HIVE_DATABASE,
        auth="NONE",
    )


def build_pipeline(
    es_host: str = None,
    cache_backend: str = "memory",
    cache_ttl: int = 21600,
    target_count: int = 20,
    score_weights: dict = None,
    classifier_model: str = "allenai/scibert_scivocab_cased",
    confidence_threshold: float = 0.75,
) -> dict:
    """
    Instantiate and return all pipeline components as a reusable dict.

    Called once at FastAPI application startup so that expensive
    initializations (model loading, ES connection, Hive connection)
    are not repeated per request.

    Parameters
    ----------
    es_host : str, optional
        Elasticsearch host. Reads ES_HOST env var if not provided.
        Default: http://localhost:9200.
    cache_backend : str
        'memory' or 'redis'. Default: 'memory'.
    cache_ttl : int
        Cache TTL in seconds. Default: 21600 (6 hours).
    target_count : int
        Papers to select per query. Default: 20.
    score_weights : dict, optional
        Custom signal weights for SelectionEngine.
    classifier_model : str
        HuggingFace model for FullTextAnalyzer sentence classification.
        Default: 'allenai/scibert_scivocab_cased'.
    confidence_threshold : float
        Sentence classification confidence threshold. Default: 0.75.

    Returns
    -------
    dict
        Pipeline component dict with keys:
        - selector (SelectionEngine)
        - analyzer (FullTextAnalyzer)
        - organizer (PaperOrganizer)
        - cache (AnalysisCache)
    """
    logger.info("Building selective analysis pipeline...")

    # Connect to Elasticsearch
    es_client = ESClient(host=es_host)
    es_client.connect()
    logger.info("Elasticsearch connected")

    # Connect to Hive — shared connection across pipeline components
    hive_conn = _get_hive_connection()
    logger.info("Hive connected")

    # Initialize shared NLP components
    text_cleaner = TextCleaner()
    tokenizer    = Tokenizer(
        model_name="en_core_web_sm",
        disable=["ner", "lemmatizer"],
    )

    # Build pipeline components
    selector = SelectionEngine(
        es_client=es_client,
        hive_conn=hive_conn,
        target_count=target_count,
        score_weights=score_weights,
    )

    analyzer = FullTextAnalyzer(
        hive_conn=hive_conn,
        text_cleaner=text_cleaner,
        tokenizer=tokenizer,
        model_name=classifier_model,
        confidence_threshold=confidence_threshold,
    )

    organizer = PaperOrganizer(
        min_per_category=2,
        max_per_category=10,
    )

    cache = AnalysisCache(
        backend=cache_backend,
        ttl_seconds=cache_ttl,
    )

    pipeline = {
        "selector": selector,
        "analyzer": analyzer,
        "organizer": organizer,
        "cache":    cache,
    }

    logger.info("Selective analysis pipeline ready")
    return pipeline


def run_analysis(query: str, pipeline: dict) -> dict:
    """
    Run the full selective analysis pipeline for a user query.

    Checks cache first. On a hit returns immediately. On a miss runs
    the full pipeline, caches the result, and returns it.

    Parameters
    ----------
    query : str
        Natural language query from the dashboard user.
    pipeline : dict
        Pipeline component dict from build_pipeline().

    Returns
    -------
    dict
        Structured result with keys:
        - query (str)
        - cached (bool)
        - selected_count (int)
        - duration_seconds (float): 0.0 if cached
        - reading_guide (dict): foundational/representative/emerging/contrasting
        - cache_stats (dict)
    """
    cache = pipeline["cache"]

    # Cache hit — return immediately
    cached_result = cache.get(query)
    if cached_result is not None:
        logger.info("Cache hit for query: '%s'", query)
        return {
            **cached_result,
            "cached": True,
            "duration_seconds": 0.0,
            "cache_stats": cache.stats(),
        }

    # Cache miss — run full pipeline
    logger.info("Cache miss — running full pipeline for query: '%s'", query)
    result = _run_pipeline_stages(query, pipeline)

    # Wrap and cache
    wrapped = {
        "query":           query,
        "cached":          False,
        "selected_count":  result["selected_count"],
        "duration_seconds": result["duration_seconds"],
        "reading_guide":   result["reading_guide"],
        "stage_timings":   result["stage_timings"],
    }
    cache.set(query, wrapped)

    return {
        **wrapped,
        "cache_stats": cache.stats(),
    }


def run_analysis_no_cache(query: str, pipeline: dict) -> dict:
    """
    Run the full selective analysis pipeline bypassing the cache.

    Invalidates any existing cached result for the query, runs fresh
    analysis, and caches the new result.

    Parameters
    ----------
    query : str
    pipeline : dict

    Returns
    -------
    dict
        Fresh analysis result with cached=False always.
    """
    cache = pipeline["cache"]
    cache.invalidate(query)
    logger.info("Cache invalidated — running fresh analysis for: '%s'", query)

    result = _run_pipeline_stages(query, pipeline)

    wrapped = {
        "query":            query,
        "cached":           False,
        "selected_count":   result["selected_count"],
        "duration_seconds": result["duration_seconds"],
        "reading_guide":    result["reading_guide"],
        "stage_timings":    result["stage_timings"],
    }
    cache.set(query, wrapped)

    return {
        **wrapped,
        "cache_stats": cache.stats(),
    }


def _run_pipeline_stages(query: str, pipeline: dict) -> dict:
    """
    Execute all three pipeline stages sequentially and return results.

    Stages:
      1. SelectionEngine.select()   — retrieve and score candidates
      2. FullTextAnalyzer.analyze() — targeted NLP on selected papers
      3. PaperOrganizer.organize()  — group into reading categories

    Parameters
    ----------
    query : str
    pipeline : dict

    Returns
    -------
    dict
        Raw pipeline output with keys: selected_papers, annotations,
        reading_guide, selected_count, duration_seconds, stage_timings.
    """
    selector  = pipeline["selector"]
    analyzer  = pipeline["analyzer"]
    organizer = pipeline["organizer"]
    timings   = {}

    # Stage 1 — Selection
    t0 = time.time()
    selected_papers = selector.select(query)
    timings["selection"] = round(time.time() - t0, 2)
    logger.info(
        "Stage 1 complete — selected %d papers in %.1fs",
        len(selected_papers), timings["selection"]
    )

    # Stage 2 — Full-text analysis
    t0 = time.time()
    annotations = analyzer.analyze(selected_papers)
    timings["analysis"] = round(time.time() - t0, 2)
    logger.info(
        "Stage 2 complete — analyzed %d papers in %.1fs",
        len(annotations), timings["analysis"]
    )

    # Stage 3 — Organization
    t0 = time.time()
    reading_guide = organizer.organize(selected_papers, annotations)
    timings["organization"] = round(time.time() - t0, 2)
    logger.info(
        "Stage 3 complete — organized papers in %.1fs",
        timings["organization"]
    )

    total_duration = sum(timings.values())
    logger.info("Pipeline complete in %.1fs for query: '%s'", total_duration, query)

    return {
        "selected_papers":  selected_papers,
        "annotations":      annotations,
        "reading_guide":    reading_guide,
        "selected_count":   len(selected_papers),
        "duration_seconds": total_duration,
        "stage_timings":    timings,
    }