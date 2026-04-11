"""
Aggregator.py — corpus-level NER result aggregation.

Aggregates named entity extraction results across the full paper corpus.

After extractor.py has processed each document independently (in parallel),
aggregator.py collects all per-document results and consolidates them into
corpus-level structures suitable for storage and downstream Spark joins.

Responsibilities:
  - Deduplication: merges surface-form variants of the same entity
    (e.g., "BERT" and "bert" treated as the same method)
  - Frequency counting: tracks how many papers mention each entity
  - Inverted index: maps each entity to the list of paper IDs that mention it
  - Summary statistics: per-entity-type counts across the full corpus

The output of this module is what gets written to storage by ner_main.py and
later loaded by Spark for trend aggregation and entity timeline queries.

Dependencies: collections (stdlib)
"""
from collections import defaultdict


class EntityAggregator:

    def aggregate(self, extraction_results: list[dict]) -> dict:
        entity_index    = self._build_entity_index(extraction_results)
        frequency_table = self._build_frequency_table(extraction_results)
        summary_stats   = self._compute_summary_stats(extraction_results)
        return {
            "entity_index":    entity_index,
            "frequency_table": frequency_table,
            "summary_stats":   summary_stats,
        }

    def _normalize_entity_text(self, entity_text: str) -> str:
        entity_text = entity_text.lower()
        entity_text = " ".join(entity_text.split())
        entity_text = entity_text.strip(".,;:!?()[]{}\"'")
        return entity_text

    def _build_entity_index(self, extraction_results: list[dict]) -> dict:
        entity_index = {}
        for result in extraction_results:
            paper_id = result["paper_id"]
            for entity in result["entities"]:
                norm = self._normalize_entity_text(entity["entity_text"])
                if norm not in entity_index:
                    entity_index[norm] = {
                        "entity_type":    entity["entity_type"],
                        "paper_ids":      set(),
                        "total_mentions": 0,
                    }
                entity_index[norm]["paper_ids"].add(paper_id)
                entity_index[norm]["total_mentions"] += 1
        for data in entity_index.values():
            data["paper_ids"] = list(data["paper_ids"])
        return entity_index

    def _build_frequency_table(self, extraction_results: list[dict]) -> dict:
        freq = defaultdict(lambda: defaultdict(int))
        for result in extraction_results:
            for entity in result["entities"]:
                norm = self._normalize_entity_text(entity["entity_text"])
                freq[entity["entity_type"]][norm] += 1
        return {
            etype: [{"entity": e, "count": c}
                    for e, c in sorted(counts.items(), key=lambda x: -x[1])]
            for etype, counts in freq.items()
        }

    def _compute_summary_stats(self, extraction_results: list[dict]) -> dict:
        total_papers          = len(extraction_results)
        papers_with_entities  = 0
        total_entity_mentions = 0
        entities_per_type     = {"METHOD": set(), "DATASET": set(), "TASK": set()}

        for result in extraction_results:  # fixed: all logic indented inside loop
            counts = result["entity_counts"]
            total_for_paper = sum(counts.values())
            if total_for_paper > 0:
                papers_with_entities += 1
            total_entity_mentions += total_for_paper
            for entity in result["entities"]:
                etype = entity["entity_type"]
                norm  = self._normalize_entity_text(entity["entity_text"])
                entities_per_type.setdefault(etype, set()).add(norm)

        unique_per_type = {k: len(v) for k, v in entities_per_type.items()}
        return {
            "total_papers":          total_papers,
            "papers_with_entities":  papers_with_entities,
            "total_entity_mentions": total_entity_mentions,
            "unique_entities":       sum(unique_per_type.values()),
            "entities_per_type":     unique_per_type,
            "avg_entities_per_paper": (
                total_entity_mentions / total_papers if total_papers > 0 else 0.0
            ),
        }

    def to_records(self, aggregated: dict) -> list[dict]:
        return [
            {
                "entity_text":    entity_text,
                "entity_type":    data["entity_type"],
                "paper_count":    len(data["paper_ids"]),
                "total_mentions": data["total_mentions"],
                "paper_ids":      data["paper_ids"],
            }
            for entity_text, data in aggregated["entity_index"].items()
        ]