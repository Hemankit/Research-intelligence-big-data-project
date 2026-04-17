"""
mappings.py
-----------
Elasticsearch index mappings and settings for the research intelligence pipeline.

Defines two indices:
  1. papers        — primary search index, maps all fields from the Hive papers
                     table. Powers the Search & Entity Explorer and Knowledge
                     Table dashboard views.
  2. paper_fulltext — full-text search index, maps content from the Hive
                      paper_fulltext table. Used for deep text search across
                      complete paper bodies.

Field type decisions are critical here — Elasticsearch cannot change a field's
type after documents have been indexed. The mapping must be applied before
any indexing begins.

Key mapping decisions reflected below:
  - title and abstract use 'text' (analyzed) for full-text search AND
    'keyword' (sub-field) for exact match and aggregations
  - paper_id, source, primary_category use 'keyword' only — never analyzed
  - authors, methods, datasets, tasks are 'keyword' arrays for faceted filtering
  - umap_x, umap_y are 'float' for the landscape map coordinates
  - submitted_date is 'date' for time-range filtering in trend queries
  - topic_cluster uses both 'text' and 'keyword' sub-field like title/abstract

Dependencies: none (pure dicts)
"""

# ── Index names ───────────────────────────────────────────────────────────────

PAPERS_INDEX    = "research_intel_papers"
FULLTEXT_INDEX  = "research_intel_fulltext"

# ── Index settings shared across both indices ─────────────────────────────────

BASE_SETTINGS = {
    "number_of_shards": 1,
    "number_of_replicas": 0,        # set to 1+ in production
    "max_result_window": 50000,     # allow deeper pagination for dashboard
    "analysis": {
        "analyzer": {
            "academic_text": {
                # Custom analyzer for scientific text — lowercases, removes
                # common stop words, and applies English stemming. Used for
                # abstract and full_text fields.
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "stop", "porter_stem"],
            }
        }
    },
}

# ── Papers index mapping ──────────────────────────────────────────────────────

def papers_mapping() -> dict:
    """
    Return the full index mapping for the papers index.

    Maps every field from the Hive papers table to its appropriate
    Elasticsearch field type. Fields used for full-text search get
    'text' type with the academic_text analyzer. Fields used for
    filtering, aggregation, or exact match get 'keyword' type.
    Several fields get both via a 'fields' sub-field definition.

    Returns
    -------
    dict
        Complete index definition with 'settings' and 'mappings' keys,
        ready to pass to ESClient.create_index().
    """
    return {
        "settings": BASE_SETTINGS,
        "mappings": {
            "properties": {
                # ── Core metadata ──────────────────────────────────────────
                "paper_id": {
                    "type": "keyword",
                    # Unique document ID — never analyzed
                },
                "title": {
                    "type": "text",
                    "analyzer": "academic_text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 512}
                    },
                    # 'text' for full-text search, 'keyword' sub-field for
                    # exact match and sorting in the Knowledge Table
                },
                "abstract": {
                    "type": "text",
                    "analyzer": "academic_text",
                    # Abstract is the primary full-text search field — no
                    # keyword sub-field needed since abstracts are never
                    # used for exact match or aggregation
                },
                "authors": {
                    "type": "keyword",
                    # Array of author name strings — keyword for faceted
                    # filtering by author in the Entity Explorer
                },
                "submitted_date": {
                    "type": "date",
                    "format": "yyyy-MM-dd||yyyy-MM||epoch_millis",
                    # Date field for time-range filtering and trend queries
                },
                "updated_date": {
                    "type": "date",
                    "format": "yyyy-MM-dd||yyyy-MM||epoch_millis",
                },
                "primary_category": {
                    "type": "keyword",
                    # arXiv category (e.g. 'cs.LG') — keyword for faceted filtering
                },
                "categories": {
                    "type": "keyword",
                    # All arXiv categories — keyword array for multi-value filtering
                },

                # ── Citation metrics ───────────────────────────────────────
                "citation_count": {
                    "type": "integer",
                    # Used for sorting by influence in Knowledge Table
                },
                "reference_count": {"type": "integer"},
                "influential_citation_count": {"type": "integer"},

                # ── BERTopic outputs ───────────────────────────────────────
                "topic_cluster_id": {
                    "type": "integer",
                    # Numeric cluster label for filtering by topic
                },
                "topic_cluster": {
                    "type": "text",
                    "analyzer": "academic_text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256}
                    },
                    # Human-readable topic name — text for search, keyword
                    # sub-field for aggregation in the Trend Explorer
                },
                "umap_x": {
                    "type": "float",
                    # 2D UMAP coordinate for the Landscape Map — not searched,
                    # only retrieved and passed to the frontend
                },
                "umap_y": {"type": "float"},

                # ── NER entity outputs ─────────────────────────────────────
                "methods": {
                    "type": "keyword",
                    # Extracted method entities — keyword array for faceted
                    # filtering in the Entity Explorer (e.g. filter by "BERT")
                },
                "datasets": {
                    "type": "keyword",
                    # Extracted dataset entities — same pattern as methods
                },
                "tasks": {
                    "type": "keyword",
                    # Extracted task entities
                },

                # ── Metadata ───────────────────────────────────────────────
                "source": {
                    "type": "keyword",
                    # Ingestion source: 'arxiv', 's2orc', 'openalex'
                },
                "ingested_at": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss||epoch_millis",
                },
            }
        },
    }


# ── paper_fulltext index mapping ──────────────────────────────────────────────

def fulltext_mapping() -> dict:
    """
    Return the full index mapping for the paper_fulltext index.

    Maps fields from the Hive paper_fulltext table. The primary field
    is full_text which receives the academic_text analyzer for deep
    content search. Sections are mapped as nested objects so individual
    section headings and bodies can be searched independently.

    Returns
    -------
    dict
        Complete index definition with 'settings' and 'mappings' keys,
        ready to pass to ESClient.create_index().
    """
    return {
        "settings": BASE_SETTINGS,
        "mappings": {
            "properties": {
                "paper_id": {"type": "keyword"},
                "arxiv_id": {"type": "keyword"},
                "corpusid":  {"type": "keyword"},
                "doi":       {"type": "keyword"},
                "full_text": {
                    "type": "text",
                    "analyzer": "academic_text",
                    # Primary full-text search field — analyzed with academic
                    # analyzer for stemming and stop word removal
                },
                "sections": {
                    "type": "nested",
                    # Nested type preserves the relationship between section
                    # heading and body text so you can search within a specific
                    # section (e.g. find papers where 'limitations' section
                    # mentions 'out-of-distribution')
                    "properties": {
                        "heading": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256}
                            },
                        },
                        "text": {
                            "type": "text",
                            "analyzer": "academic_text",
                        },
                    },
                },
                "ingested_at": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss||epoch_millis",
                },
            }
        },
    }