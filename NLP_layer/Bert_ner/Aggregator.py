"""
aggregator.py
-------------
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
    """
    Collects and consolidates per-document NER results into corpus-level
    entity records.

    Processes a list of extraction results as returned by EntityExtractor.extract()
    and builds frequency tables, inverted indices, and summary statistics
    across the full corpus.

    Designed to be called once after all parallel extraction jobs have
    completed, not during per-document processing.
    """

    def aggregate(self, extraction_results: list[dict]) -> dict:
        """
        Aggregate a list of per-document extraction results into
        corpus-level entity structures.

        Primary entry point. Iterates over all per-document results,
        normalizes entity surface forms, and builds the full set of
        aggregated outputs. Calls the private helper methods below
        to build each output structure.

        Parameters
        ----------
        extraction_results : list[dict]
            List of per-document result dicts as returned by
            EntityExtractor.extract(), each containing paper_id,
            source, entities, and entity_counts.

        Returns
        -------
        dict
            Aggregated corpus results with keys:
            - entity_index (dict): See _build_entity_index()
            - frequency_table (dict): See _build_frequency_table()
            - summary_stats (dict): See _compute_summary_stats()
        """
        entity_index = self._build_entity_index(extraction_results)
        frequency_table = self._build_frequency_table(extraction_results)
        summary_stats = self._compute_summary_stats(extraction_results)
        return {
            "entity_index": entity_index,
            "frequency_table": frequency_table,
            "summary_stats": summary_stats
        }

    def _normalize_entity_text(self, entity_text: str) -> str:
        """
        Normalize an entity surface form for deduplication.

        Applies lowercasing and strips leading/trailing punctuation
        and whitespace so that surface variants of the same entity
        (e.g., "BERT", "bert", "BERT.") are treated as identical
        when building frequency tables and the inverted index.

        Parameters
        ----------
        entity_text : str
            Raw entity surface form as extracted by EntityExtractor.

        Returns
        -------
        str
            Normalized entity string suitable for use as a dict key.
        """
        entity_text = entity_text.lower()
        entity_text = " ".join(entity_text.split())  # collapse irregular whitespace
        entity_text = entity_text.strip(".,;:!?()[]{}\"'")
        return entity_text

    def _build_entity_index(self, extraction_results: list[dict]) -> dict:
        """
        Build an inverted index mapping each entity to the papers that mention it.

        For each unique normalized entity string, records the list of
        paper IDs that contain at least one mention of that entity,
        along with the entity type and total mention count across all papers.

        Parameters
        ----------
        extraction_results : list[dict]
            Full list of per-document extraction results.

        Returns
        -------
        dict
            Inverted index structured as:
            {
                "bert": {
                    "entity_type": "METHOD",
                    "paper_ids": ["paper_001", "paper_042", ...],
                    "total_mentions": 17
                },
                ...
            }
        """
        entity_index = {}
        # Iterate over all per-document results and populate the entity index
        for result in extraction_results:
            paper_id = result["paper_id"]
            for entity in result["entities"]:
                normalized_text = self._normalize_entity_text(entity["entity_text"])
                if normalized_text not in entity_index:
                    entity_index[normalized_text] = {
                        "entity_type": entity["entity_type"],
                        "paper_ids": set(),
                        "total_mentions": 0
                    }
                entity_index[normalized_text]["paper_ids"].add(paper_id)
                entity_index[normalized_text]["total_mentions"] += 1

        # Convert paper_ids from set to list for JSON serialization
        for entity_data in entity_index.values():
            entity_data["paper_ids"] = list(entity_data["paper_ids"])

        return entity_index

    def _build_frequency_table(self, extraction_results: list[dict]) -> dict:
        """
        Build a frequency table of entity mentions grouped by entity type.

        Counts how many distinct papers mention each entity, separately
        for each entity type (METHOD, DATASET, TASK). Sorted by frequency
        descending within each type, making it easy to identify the most
        commonly mentioned methods and datasets across the corpus.

        Parameters
        ----------
        extraction_results : list[dict]
            Full list of per-document extraction results.

        Returns
        -------
        dict
            Frequency table structured as:
            {
                "METHOD":  [{"entity": "bert", "count": 142}, ...],
                "DATASET": [{"entity": "imagenet", "count": 87}, ...],
                "TASK":    [{"entity": "named entity recognition", "count": 53}, ...]
            }
        """
        frequency_table = defaultdict(lambda: defaultdict(int))
        for result in extraction_results:
            for entity in result["entities"]:
                normalized_text = self._normalize_entity_text(entity["entity_text"])
                entity_type = entity["entity_type"]
                frequency_table[entity_type][normalized_text] += 1

        # Convert to desired output format with sorting
        sorted_frequency_table = {}
        for entity_type, entities in frequency_table.items():
            sorted_entities = sorted(entities.items(), key=lambda x: x[1], reverse=True)
            sorted_frequency_table[entity_type] = [
                {"entity": entity, "count": count} for entity, count in sorted_entities
            ]

        return sorted_frequency_table

    def _compute_summary_stats(self, extraction_results: list[dict]) -> dict:
        """
        Compute high-level summary statistics across the full corpus.

        Provides a quick overview of extraction coverage and entity
        distribution — useful for validating pipeline output and
        diagnosing model quality issues before writing to storage.

        Parameters
        ----------
        extraction_results : list[dict]
            Full list of per-document extraction results.

        Returns
        -------
        dict
            Summary statistics with keys:
            - total_papers (int): Number of papers processed
            - papers_with_entities (int): Papers with at least one entity
            - total_entity_mentions (int): Total entity mentions across corpus
            - unique_entities (int): Number of unique normalized entity strings
            - entities_per_type (dict): Unique entity count per type
              e.g. {'METHOD': 412, 'DATASET': 98, 'TASK': 203}
            - avg_entities_per_paper (float): Mean entity count per paper
        """
        total_papers = len(extraction_results)
        papers_with_entities = 0
        total_entity_mentions = 0
        entities_per_type = {"METHOD": set(), "DATASET": set(), "TASK": set()}
        # Iterate over all results to compute stats
        for result in extraction_results:
            counts = result["entity_counts"]
            total_for_paper = sum(counts.values())

        if total_for_paper > 0:
            papers_with_entities += 1

        total_entity_mentions += total_for_paper
        # Collect unique normalized entities per type for corpus-level counts
        for entity in result["entities"]:
            entity_type = entity["entity_type"]
            normalized = self._normalize_entity_text(entity["entity_text"])
            entities_per_type[entity_type].add(normalized)
        # Convert sets to counts for unique entities per type
        unique_entities_per_type = {k: len(v) for k, v in entities_per_type.items()}
        return {
        "total_papers": total_papers,
        "papers_with_entities": papers_with_entities,
        "total_entity_mentions": total_entity_mentions,
        "unique_entities": sum(unique_entities_per_type.values()),
        "entities_per_type": unique_entities_per_type,
        "avg_entities_per_paper": total_entity_mentions / total_papers if total_papers > 0 else 0.0,
    }
 

    def to_records(self, aggregated: dict) -> list[dict]:
        """
        Flatten the aggregated entity index into a list of records
        suitable for writing to storage or loading into a DataFrame.

        Converts the nested entity_index structure into a flat list
        where each record represents one unique entity and its corpus-level
        statistics. This format is what gets written by run.py and later
        joined by Spark.

        Parameters
        ----------
        aggregated : dict
            The full aggregated output as returned by aggregate().

        Returns
        -------
        list[dict]
            Flat list of entity records, each with keys:
            - entity_text (str): Normalized entity string
            - entity_type (str): METHOD, DATASET, or TASK
            - paper_count (int): Number of papers mentioning this entity
            - total_mentions (int): Total mention count across all papers
            - paper_ids (list[str]): List of paper IDs mentioning this entity
        """
        records = []
        for entity_text, data in aggregated["entity_index"].items():
            record = {
                "entity_text": entity_text,
                "entity_type": data["entity_type"],
                "paper_count": len(data["paper_ids"]),
                "total_mentions": data["total_mentions"],
                "paper_ids": data["paper_ids"]
            }
            records.append(record)
        return records