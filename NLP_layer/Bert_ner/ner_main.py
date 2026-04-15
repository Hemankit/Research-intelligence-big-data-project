"""
ner_main.py — NER pipeline entrypoint.

When n_jobs=1: uses EntityExtractor.extract_batch() — single process,
batched BERT inference. Fastest option on CPU — one model in memory,
papers processed in batches through a single forward pass.

When n_jobs != 1: uses joblib parallelization via ParallelExecutor.
WARNING: with large models (433MB+) on CPU, joblib spawns worker
processes that each reload the full model, which is extremely slow.
Only use n_jobs > 1 if you have a GPU or a very small model.

Output written to HDFS:
    /user/research-intelligence/raw/ner/<date>/<timestamp>.jsonl

Each record shape:
    {"paper_id": "2401.12345", "methods": [...], "datasets": [...], "tasks": [...]}

Full-text mode (--fulltext):
    Reads from raw/s2orc/s2orc_fulltext/ instead of abstracts.
    Each paper's full_text is sentence-split by spaCy and fed to NER
    section by section so BERT never exceeds its 512-token limit.
    Use --max_sections to cap how many sections per paper are processed
    (default: 5) — introduction + methods sections are most entity-rich.

    Example:
        python -m NLP_layer.Bert_ner.ner_main \\
            --model dslim/bert-base-NER \\
            --input_path /user/research-intelligence/raw \\
            --output_path /user/research-intelligence/raw/ner \\
            --fulltext \\
            --max_sections 5 \\
            --n_jobs 1 \\
            --batch_size 16
"""
import argparse
import logging
import os
from datetime import datetime, timezone
from tqdm import tqdm

from ingestion.hdfs_client import HDFSClient
from .ner_model import NERModel
from .extractor import EntityExtractor
from .Aggregator import EntityAggregator
from NLP_layer.shared.text_preprocessing import TextCleaner
from NLP_layer.shared.Tokenizer import Tokenizer
from NLP_layer.shared.parallelization import ParallelExecutor

logger = logging.getLogger(__name__)


def load_papers(input_path: str, sources: list[str] = None) -> list[dict]:
    """Load papers from abstract sources (arxiv, s2orc_bulk, openalex)."""
    if sources is None:
        sources = ["arxiv", "s2orc", "openalex"]
    hdfs_client = HDFSClient()
    all_papers  = []
    for source in sources:
        source_path = f"{input_path}/{source}"
        categories  = hdfs_client.list_directory(source_path)
        if not categories:
            logger.warning("No categories found for source %s", source)
            continue
        for category in categories:
            if category in ("edges", "s2orc_fulltext"):
                continue
            category_path = f"{source_path}/{category}"
            dates = hdfs_client.list_directory(category_path)
            for date in dates:
                date_path   = f"{category_path}/{date}"
                files       = hdfs_client.list_directory(date_path)
                jsonl_files = [f for f in files if f.endswith(".jsonl")]
                for jsonl_file in jsonl_files:
                    file_path = f"{date_path}/{jsonl_file}"
                    try:
                        records = hdfs_client.read_json(file_path)
                        all_papers.extend(records)
                        logger.info("Loaded %d records from %s", len(records), file_path)
                    except Exception as e:
                        logger.error("Failed to read %s: %s", file_path, e)
    logger.info("Total papers loaded: %d", len(all_papers))
    return all_papers


def load_fulltext_papers(input_path: str, max_sections: int = 5) -> list[dict]:
    """
    Load papers from the S2ORC full-text corpus for full-text NER.

    Reads from raw/s2orc/s2orc_fulltext/ which contains records produced by
    s2orc_bulk_download.py. Each record has a full_text string and a
    sections list of {heading, text} dicts.

    Strategy: rather than feeding the entire full_text (45K+ chars) to
    BERT at once — which would be truncated to 512 tokens — we extract
    the most entity-rich sections and run NER on each independently.
    Sections are prioritized by those most likely to contain METHOD,
    DATASET, and TASK mentions (introduction, methodology, experiments).

    Parameters
    ----------
    input_path : str
        Base HDFS path, e.g. /user/research-intelligence/raw
    max_sections : int
        Maximum number of sections to process per paper (default: 5).
        Introduction + methods sections are most entity-rich. Capping
        at 5 keeps runtime manageable on CPU.

    Returns
    -------
    list[dict]
        Paper records with 'abstract' field set to the concatenated
        section texts for NER processing. paper_id and source are
        preserved for downstream aggregation.
    """
    hdfs_client = HDFSClient()
    fulltext_path = f"{input_path}/s2orc/s2orc_fulltext"
    all_papers = []

    dates = hdfs_client.list_directory(fulltext_path)
    if not dates:
        logger.warning("No full-text data found at %s", fulltext_path)
        return []

    for date in dates:
        date_path = f"{fulltext_path}/{date}"
        files     = hdfs_client.list_directory(date_path)
        jsonl_files = [f for f in files if f.endswith(".jsonl")]
        for jsonl_file in jsonl_files:
            file_path = f"{date_path}/{jsonl_file}"
            try:
                records = hdfs_client.read_json(file_path)
                for rec in records:
                    pid      = rec.get("paper_id")
                    sections = rec.get("sections") or []
                    fulltext = rec.get("full_text") or ""

                    if not pid:
                        continue

                    # Priority keywords for section selection —
                    # these sections contain the most entity mentions
                    PRIORITY_KEYWORDS = {
                        "introduction", "method", "methodology",
                        "experiment", "approach", "model", "dataset",
                        "evaluation", "results", "system", "framework",
                    }

                    # Score sections: prioritized ones first, then others
                    def section_priority(sec):
                        heading = (sec.get("heading") or "").lower()
                        return 0 if any(k in heading for k in PRIORITY_KEYWORDS) else 1

                    sorted_sections = sorted(sections, key=section_priority)
                    top_sections    = sorted_sections[:max_sections]

                    if top_sections:
                        # Use section texts as the NER input
                        text_for_ner = " ".join(
                            s.get("text", "") for s in top_sections
                            if s.get("text")
                        )
                    elif fulltext:
                        # Fallback: use first 2000 chars of full_text if no sections
                        text_for_ner = fulltext[:2000]
                    else:
                        continue

                    all_papers.append({
                        "paper_id": pid,
                        "source":   "s2orc_fulltext",
                        "abstract": text_for_ner,  # NER pipeline reads 'abstract' field
                    })
                logger.info("Loaded %d full-text records from %s", len(records), file_path)
            except Exception as e:
                logger.error("Failed to read %s: %s", file_path, e)

    logger.info("Total full-text papers loaded: %d", len(all_papers))
    return all_papers


def clean_abstracts(papers: list[dict]) -> list[dict]:
    """
    Clean and sentence-split the abstract field of each paper record.

    Two-stage processing:
      1. TextCleaner  — removes LaTeX macros, URLs, normalizes Unicode
      2. spaCy Tokenizer — sentence-splits the cleaned abstract and
         rejoins sentences with spaces. This ensures clean sentence
         boundaries and handles abbreviations like 'et al.' and 'Fig.'
         better than naive splitting. Important for NER: BERT processes
         up to 512 tokens, so well-segmented input improves entity span
         detection near sentence boundaries.

    Papers with abstracts shorter than 50 chars after cleaning are
    flagged with skip_ner=True so the extractor skips them.
    """
    cleaner   = TextCleaner()
    tokenizer = Tokenizer(
        model_name="en_core_web_sm",
        disable=["ner", "lemmatizer"],
    )
    logger.info("clean_abstracts: cleaning and sentence-splitting %d abstracts", len(papers))

    for paper in papers:
        raw     = paper.get("abstract") or ""
        cleaned = cleaner.clean(raw)

        # Sentence-split with spaCy and rejoin — normalizes boundaries
        if cleaned.strip():
            sentences = tokenizer.split_sentences(cleaned)
            cleaned   = " ".join(sentences) if sentences else cleaned

        paper["abstract"] = cleaned
        paper["skip_ner"] = len(cleaned.strip()) < 50

    skipped = sum(1 for p in papers if p["skip_ner"])
    logger.info(
        "clean_abstracts: %d papers, %d skipped (too short after cleaning)",
        len(papers), skipped
    )
    return papers


def run_extraction_batched(
    papers: list[dict],
    model: NERModel,
    batch_size: int = 32,
) -> list[dict]:
    """
    Single-process batched inference — loads model once, processes in batches.
    Fastest option on CPU. Use when n_jobs=1.
    """
    papers_to_process = [p for p in papers if not p.get("skip_ner", False)]
    logger.info(
        "Running batched NER on %d papers (batch_size=%d, single process)",
        len(papers_to_process), batch_size,
    )
    extractor = EntityExtractor(model)
    results   = []
    total     = len(papers_to_process)

    for i in tqdm(range(0, total, batch_size), desc="NER batches", unit="batch"):
        batch        = papers_to_process[i:i + batch_size]
        batch_results = extractor.extract_batch(batch, batch_size=batch_size)
        results.extend(batch_results)

    return results


def run_extraction_parallel(
    papers: list[dict],
    model: NERModel,
    n_jobs: int = -2,
    chunk_size: int = 1000,
) -> list[dict]:
    """
    Parallel extraction via joblib. Fast only with GPU or small models.
    Avoid on CPU with large models — each worker reloads model weights.
    """
    extractor         = EntityExtractor(model)
    papers_to_process = [p for p in papers if not p.get("skip_ner", False)]
    logger.info(
        "Running parallel NER on %d papers (n_jobs=%d, chunk_size=%d)",
        len(papers_to_process), n_jobs, chunk_size,
    )
    executor = ParallelExecutor(n_jobs=n_jobs)
    return executor.run_in_chunks(
        func=extractor.extract,
        items=papers_to_process,
        chunk_size=chunk_size,
    )


def save_results(aggregated_records: list[dict], output_path: str):
    hdfs  = HDFSClient()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Group entity records by paper_id into methods/datasets/tasks lists
    paper_entities: dict[str, dict] = {}
    for rec in aggregated_records:
        for pid in rec["paper_ids"]:
            if pid not in paper_entities:
                paper_entities[pid] = {
                    "paper_id": pid,
                    "methods":  [],
                    "datasets": [],
                    "tasks":    [],
                }
            etype = rec["entity_type"]
            text  = rec["entity_text"]
            if etype == "METHOD":
                paper_entities[pid]["methods"].append(text)
            elif etype == "DATASET":
                paper_entities[pid]["datasets"].append(text)
            elif etype == "TASK":
                paper_entities[pid]["tasks"].append(text)

    per_paper_records = list(paper_entities.values())
    hdfs_path = hdfs.write_json(per_paper_records, source="ner", category=today)
    logger.info(
        "Wrote %d per-paper NER records to HDFS: %s",
        len(per_paper_records), hdfs_path,
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run BERT NER pipeline over paper abstracts or full text",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Abstract NER (default — fast, recommended)
  python -m NLP_layer.Bert_ner.ner_main \\
      --model dslim/bert-base-NER \\
      --input_path /user/research-intelligence/raw \\
      --output_path /user/research-intelligence/raw/ner \\
      --sources arxiv --n_jobs 1 --batch_size 32

  # Full-text NER (slower — processes section text from s2orc_fulltext/)
  python -m NLP_layer.Bert_ner.ner_main \\
      --model dslim/bert-base-NER \\
      --input_path /user/research-intelligence/raw \\
      --output_path /user/research-intelligence/raw/ner \\
      --fulltext --max_sections 5 --n_jobs 1 --batch_size 16
        """
    )
    parser.add_argument("--model",        required=True)
    parser.add_argument("--input_path",   required=True)
    parser.add_argument("--output_path",  required=True)
    parser.add_argument("--sources",      default="arxiv,s2orc,openalex",
                        help="Comma-separated sources for abstract NER (ignored when --fulltext)")
    parser.add_argument("--fulltext",     action="store_true", default=False,
                        help="Run NER on full-text sections from s2orc_fulltext/ instead of abstracts")
    parser.add_argument("--max_sections", type=int, default=5,
                        help="Max sections per paper in full-text mode (default: 5)")
    parser.add_argument("--n_jobs",       type=int, default=1,
                        help="Workers. Use 1 (default) for CPU — avoids model reload overhead.")
    parser.add_argument("--chunk_size",   type=int, default=1000)
    parser.add_argument("--batch_size",   type=int, default=32,
                        help="Papers per BERT forward pass when n_jobs=1 (default: 32). "
                             "Use 16 for full-text mode — sections are longer than abstracts.")
    parser.add_argument("--confidence",   type=float, default=0.80)
    return parser.parse_args()


if __name__ == "__main__":
    import time
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    args = parse_args()
    t0   = time.time()

    logger.info("=" * 60)
    logger.info("NER Pipeline starting")
    logger.info("  model        : %s", args.model)
    logger.info("  input_path   : %s", args.input_path)
    logger.info("  mode         : %s", "FULL-TEXT" if args.fulltext else "ABSTRACT")
    if args.fulltext:
        logger.info("  max_sections : %d", args.max_sections)
    else:
        sources = [s.strip() for s in args.sources.split(",")]
        logger.info("  sources      : %s", sources)
    logger.info("  n_jobs       : %d", args.n_jobs)
    logger.info("  batch_size   : %d", args.batch_size)
    logger.info("=" * 60)

    # Load papers — abstract mode or full-text mode
    if args.fulltext:
        papers = load_fulltext_papers(args.input_path, max_sections=args.max_sections)
    else:
        sources = [s.strip() for s in args.sources.split(",")]
        papers  = load_papers(args.input_path, sources)

    papers = clean_abstracts(papers)

    model = NERModel(args.model)
    model.load()

    if args.n_jobs == 1:
        results = run_extraction_batched(papers, model, batch_size=args.batch_size)
    else:
        results = run_extraction_parallel(
            papers, model, n_jobs=args.n_jobs, chunk_size=args.chunk_size
        )

    aggregator = EntityAggregator()
    aggregated = aggregator.aggregate(results)
    records    = aggregator.to_records(aggregated)

    save_results(records, args.output_path)

    elapsed = time.time() - t0
    stats   = aggregated["summary_stats"]
    logger.info("=" * 60)
    logger.info("NER Pipeline complete in %.1fs", elapsed)
    logger.info("  Papers processed    : %d", stats["total_papers"])
    logger.info("  Papers with entities: %d", stats["papers_with_entities"])
    logger.info("  Total mentions      : %d", stats["total_entity_mentions"])
    logger.info("  Unique entities     : %d", stats["unique_entities"])
    logger.info("  Per type            : %s", stats["entities_per_type"])
    logger.info("=" * 60)



def load_papers(input_path: str, sources: list[str] = None) -> list[dict]:
    if sources is None:
        sources = ["arxiv", "s2orc", "openalex"]
    hdfs_client = HDFSClient()
    all_papers  = []
    for source in sources:
        source_path = f"{input_path}/{source}"
        categories  = hdfs_client.list_directory(source_path)
        if not categories:
            logger.warning("No categories found for source %s", source)
            continue
        for category in categories:
            if category in ("edges", "s2orc_fulltext"):
                continue
            category_path = f"{source_path}/{category}"
            dates = hdfs_client.list_directory(category_path)
            for date in dates:
                date_path   = f"{category_path}/{date}"
                files       = hdfs_client.list_directory(date_path)
                jsonl_files = [f for f in files if f.endswith(".jsonl")]
                for jsonl_file in jsonl_files:
                    file_path = f"{date_path}/{jsonl_file}"
                    try:
                        records = hdfs_client.read_json(file_path)
                        all_papers.extend(records)
                        logger.info("Loaded %d records from %s", len(records), file_path)
                    except Exception as e:
                        logger.error("Failed to read %s: %s", file_path, e)
    logger.info("Total papers loaded: %d", len(all_papers))
    return all_papers


def clean_abstracts(papers: list[dict]) -> list[dict]:
    """
    Clean and sentence-split the abstract field of each paper record.

    Two-stage processing:
      1. TextCleaner  — removes LaTeX macros, URLs, normalizes Unicode
      2. spaCy Tokenizer — sentence-splits the cleaned abstract and
         rejoins sentences with spaces. This ensures clean sentence
         boundaries and handles abbreviations like 'et al.' and 'Fig.'
         better than naive splitting. Important for NER: BERT processes
         up to 512 tokens, so well-segmented input improves entity span
         detection near sentence boundaries.

    Papers with abstracts shorter than 50 chars after cleaning are
    flagged with skip_ner=True so the extractor skips them.
    """
    cleaner   = TextCleaner()
    tokenizer = Tokenizer(
        model_name="en_core_web_sm",
        disable=["ner", "lemmatizer"],
    )
    logger.info("clean_abstracts: cleaning and sentence-splitting %d abstracts", len(papers))

    for paper in papers:
        raw     = paper.get("abstract") or ""
        cleaned = cleaner.clean(raw)

        # Sentence-split with spaCy and rejoin — normalizes boundaries
        if cleaned.strip():
            sentences = tokenizer.split_sentences(cleaned)
            cleaned   = " ".join(sentences) if sentences else cleaned

        paper["abstract"] = cleaned
        paper["skip_ner"] = len(cleaned.strip()) < 50

    skipped = sum(1 for p in papers if p["skip_ner"])
    logger.info(
        "clean_abstracts: %d papers, %d skipped (too short after cleaning)",
        len(papers), skipped
    )
    return papers


def run_extraction_batched(
    papers: list[dict],
    model: NERModel,
    batch_size: int = 32,
) -> list[dict]:
    """
    Single-process batched inference — loads model once, processes in batches.
    Fastest option on CPU. Use when n_jobs=1.
    """
    papers_to_process = [p for p in papers if not p.get("skip_ner", False)]
    logger.info(
        "Running batched NER on %d papers (batch_size=%d, single process)",
        len(papers_to_process), batch_size,
    )
    extractor = EntityExtractor(model)
    results   = []
    total     = len(papers_to_process)

    for i in tqdm(range(0, total, batch_size), desc="NER batches", unit="batch"):
        batch        = papers_to_process[i:i + batch_size]
        batch_results = extractor.extract_batch(batch, batch_size=batch_size)
        results.extend(batch_results)

    return results


def run_extraction_parallel(
    papers: list[dict],
    model: NERModel,
    n_jobs: int = -2,
    chunk_size: int = 1000,
) -> list[dict]:
    """
    Parallel extraction via joblib. Fast only with GPU or small models.
    Avoid on CPU with large models — each worker reloads model weights.
    """
    extractor         = EntityExtractor(model)
    papers_to_process = [p for p in papers if not p.get("skip_ner", False)]
    logger.info(
        "Running parallel NER on %d papers (n_jobs=%d, chunk_size=%d)",
        len(papers_to_process), n_jobs, chunk_size,
    )
    executor = ParallelExecutor(n_jobs=n_jobs)
    return executor.run_in_chunks(
        func=extractor.extract,
        items=papers_to_process,
        chunk_size=chunk_size,
    )


def save_results(aggregated_records: list[dict], output_path: str):
    hdfs  = HDFSClient()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Group entity records by paper_id into methods/datasets/tasks lists
    paper_entities: dict[str, dict] = {}
    for rec in aggregated_records:
        for pid in rec["paper_ids"]:
            if pid not in paper_entities:
                paper_entities[pid] = {
                    "paper_id": pid,
                    "methods":  [],
                    "datasets": [],
                    "tasks":    [],
                }
            etype = rec["entity_type"]
            text  = rec["entity_text"]
            if etype == "METHOD":
                paper_entities[pid]["methods"].append(text)
            elif etype == "DATASET":
                paper_entities[pid]["datasets"].append(text)
            elif etype == "TASK":
                paper_entities[pid]["tasks"].append(text)

    per_paper_records = list(paper_entities.values())
    # write_json routes to: {base_path}/raw/{source}/{category}/{date}/{timestamp}.jsonl
    # Pass source="ner" and category=today so the final path is:
    #   raw/ner/{today}/{timestamp}.jsonl  (single date level, no doubling)
    hdfs_path = hdfs.write_json(per_paper_records, source="ner", category=today)
    logger.info(
        "Wrote %d per-paper NER records to HDFS: %s",
        len(per_paper_records), hdfs_path,
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Run BERT NER pipeline over paper abstracts")
    parser.add_argument("--model",       required=True)
    parser.add_argument("--input_path",  required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--sources",     default="arxiv,s2orc,openalex")
    parser.add_argument("--n_jobs",      type=int,   default=1,
                        help="Workers. Use 1 (default) for CPU — avoids model reload overhead.")
    parser.add_argument("--chunk_size",  type=int,   default=1000)
    parser.add_argument("--batch_size",  type=int,   default=32,
                        help="Abstracts per BERT forward pass when n_jobs=1 (default: 32)")
    parser.add_argument("--confidence",  type=float, default=0.80)
    return parser.parse_args()


if __name__ == "__main__":
    import time
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    args    = parse_args()
    sources = [s.strip() for s in args.sources.split(",")]
    t0      = time.time()

    logger.info("=" * 60)
    logger.info("NER Pipeline starting")
    logger.info("  model      : %s", args.model)
    logger.info("  input_path : %s", args.input_path)
    logger.info("  sources    : %s", sources)
    logger.info("  n_jobs     : %d", args.n_jobs)
    logger.info("  batch_size : %d", args.batch_size)
    logger.info("=" * 60)

    papers = load_papers(args.input_path, sources)
    papers = clean_abstracts(papers)

    model = NERModel(args.model)
    model.load()

    # Use batched single-process inference when n_jobs=1 (default, recommended for CPU)
    # Fall back to joblib parallelization only when n_jobs != 1
    if args.n_jobs == 1:
        results = run_extraction_batched(papers, model, batch_size=args.batch_size)
    else:
        results = run_extraction_parallel(
            papers, model, n_jobs=args.n_jobs, chunk_size=args.chunk_size
        )

    aggregator = EntityAggregator()
    aggregated = aggregator.aggregate(results)
    records    = aggregator.to_records(aggregated)

    save_results(records, args.output_path)

    elapsed = time.time() - t0
    stats   = aggregated["summary_stats"]
    logger.info("=" * 60)
    logger.info("NER Pipeline complete in %.1fs", elapsed)
    logger.info("  Papers processed    : %d", stats["total_papers"])
    logger.info("  Papers with entities: %d", stats["papers_with_entities"])
    logger.info("  Total mentions      : %d", stats["total_entity_mentions"])
    logger.info("  Unique entities     : %d", stats["unique_entities"])
    logger.info("  Per type            : %s", stats["entities_per_type"])
    logger.info("=" * 60)