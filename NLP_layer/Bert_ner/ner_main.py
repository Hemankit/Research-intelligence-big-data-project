"""
run.py
------
Entrypoint for the NER pipeline.

Orchestrates the full per-document entity extraction workflow across
the complete abstract corpus using joblib parallelization:

  1. Load all paper records from HDFS (all three sources combined)
  2. Clean abstracts via shared TextCleaner
  3. Load the BERT-based NER model once
  4. Fan out EntityExtractor.extract() across CPU cores via ParallelExecutor
  5. Collect results and pass to EntityAggregator
  6. Write aggregated entity records to storage

The NER model is loaded once in the main process before joblib workers
are spawned. Each worker receives a copy of the model via joblib's
process forking — this avoids the overhead of reloading model weights
in every worker process.

Usage:
    python run.py --model allenai/scibert_scivocab_cased
                  --input_path /user/research-intelligence/raw
                  --output_path /user/research-intelligence/ner
                  --n_jobs -2

Dependencies: ner_pipeline/model.py, ner_pipeline/extractor.py,
              ner_pipeline/aggregator.py, shared/text_cleaner.py,
              shared/parallel.py
"""

import logging
from ingestion.hdfs_client import HDFSClient
from .ner_model import NERModel
from .extractor import EntityExtractor
from .Aggregator import EntityAggregator
from NLP_layer.shared.text_preprocessing import TextCleaner
from NLP_layer.shared.parallelization import ParallelExecutor

logger = logging.getLogger(__name__)


def load_papers(input_path: str, sources: list[str] = None) -> list[dict]:
    """
    Load and merge paper records from HDFS across all ingested sources.

    Reads JSONL files from the partitioned HDFS directory structure
    written by the ingestion layer. Merges records from arXiv, S2ORC,
    and OpenAlex into a single flat list. Optionally filters to a
    subset of sources.

    Parameters
    ----------
    input_path : str
        Base HDFS path under which raw ingested records are stored,
        e.g. '/user/research-intelligence/raw'.
    sources : list[str], optional
        List of sources to load. Defaults to ['arxiv', 's2orc', 'openalex'].
        Pass a subset to load from specific sources only.

    Returns
    -------
    list[dict]
        Flat list of paper records from all requested sources,
        each containing at minimum paper_id, abstract, and source.
    """
    if sources is None:
        sources = ['arxiv', 's2orc', 'openalex']

    hdfs_client = HDFSClient()
    all_papers = []

    logger.info("Loading papers from HDFS: %s", input_path)
    logger.info("Sources: %s", sources)

    for source in sources:
        source_path = f"{input_path}/{source}"
        logger.info("Processing source: %s", source)

        # List all category directories for this source
        categories = hdfs_client.list_directory(source_path)
        if not categories:
            logger.warning("No categories found for source %s at %s", source, source_path)
            continue

        for category in categories:
            category_path = f"{source_path}/{category}"

            # List all date directories within this category
            dates = hdfs_client.list_directory(category_path)
            if not dates:
                logger.warning("No dates found for category %s/%s", source, category)
                continue

            for date in dates:
                date_path = f"{category_path}/{date}"

                # List all JSONL files within this date directory
                files = hdfs_client.list_directory(date_path)
                jsonl_files = [f for f in files if f.endswith('.jsonl')]

                if not jsonl_files:
                    logger.warning("No JSONL files found in %s", date_path)
                    continue

                # Read each JSONL file and accumulate records
                for jsonl_file in jsonl_files:
                    file_path = f"{date_path}/{jsonl_file}"
                    try:
                        records = hdfs_client.read_json(file_path)
                        all_papers.extend(records)
                        logger.info(
                            "Loaded %d records from %s/%s/%s/%s",
                            len(records), source, category, date, jsonl_file
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to read %s: %s",
                            file_path, e
                        )

    logger.info("Total papers loaded: %d", len(all_papers))
    return all_papers


def clean_abstracts(papers: list[dict]) -> list[dict]:
    """
    Apply TextCleaner to the abstract field of each paper record.

    Runs the full cleaning pipeline (LaTeX removal, Unicode normalization,
    whitespace normalization) on each abstract in place. Papers with
    missing or empty abstracts after cleaning are flagged with a
    'skip_ner' key so the extractor can skip them without erroring.

    Parameters
    ----------
    papers : list[dict]
        List of raw paper records with an 'abstract' field.

    Returns
    -------
    list[dict]
        Same list with 'abstract' fields cleaned in place and
        'skip_ner' (bool) added to each record.
    """
    pass


def run_extraction(
    papers: list[dict],
    model: NERModel,
    n_jobs: int = -2,
    chunk_size: int = 1000,
) -> list[dict]:
    """
    Parallelize EntityExtractor.extract() across all paper records
    using joblib via ParallelExecutor.

    Instantiates EntityExtractor once with the loaded model, then
    fans out extract() calls across CPU cores in chunks. Papers flagged
    with 'skip_ner' are excluded before dispatch and given empty results.

    Parameters
    ----------
    papers : list[dict]
        Cleaned paper records ready for NER inference.
    model : NERModel
        A loaded NERModel instance to pass to EntityExtractor.
    n_jobs : int
        Number of CPU cores to use (default: -2, all but one).
    chunk_size : int
        Number of papers per joblib chunk (default: 1000).

    Returns
    -------
    list[dict]
        List of per-document extraction results as returned by
        EntityExtractor.extract(), one per input paper.
    """
    # instantiate extractor once to load model weights into memory
    extractor = EntityExtractor(model)
    # filter out papers to skip before parallel processing
    papers_to_process = [paper for paper in papers if not paper.get('skip_ner', False)]
    logger.info("Running NER extraction on %d papers with n_jobs=%d and chunk_size=%d",
                len(papers_to_process), n_jobs, chunk_size)
    # use ParallelExecutor to run extraction in parallel across papers in chunks
    parallel_executor = ParallelExecutor(n_jobs=n_jobs)
    extraction_results = parallel_executor.run_in_chunks(
        func=extractor.extract,
        items=papers_to_process,
        chunk_size=chunk_size
    )
    return extraction_results


def save_results(aggregated_records: list[dict], output_path: str):
    """
    Write aggregated entity records to storage.

    Serializes the flat entity records produced by EntityAggregator.to_records()
    to JSONL format and writes them to the specified output path on HDFS.
    Output is partitioned by entity type for efficient downstream querying.

    Parameters
    ----------
    aggregated_records : list[dict]
        Flat entity records as returned by EntityAggregator.to_records().
    output_path : str
        Base HDFS output path. Files are written under
        output_path/ner/<entity_type>/<date>/.
    """
    pass


def parse_args():
    """
    Parse command-line arguments for the NER pipeline run.

    Supports the following flags:
    --model        : HuggingFace model name or local path (required)
    --input_path   : Base HDFS path for raw ingested records (required)
    --output_path  : Base HDFS path for NER output (required)
    --sources      : Comma-separated sources to load, default 'arxiv,s2orc,openalex'
    --n_jobs       : Number of parallel workers, default -2
    --chunk_size   : Papers per joblib chunk, default 1000
    --confidence   : Entity confidence threshold, default 0.80

    Returns
    -------
    argparse.Namespace
        Parsed arguments object.
    """
    pass


if __name__ == "__main__":
    """
    CLI entry point. Executes the full NER pipeline:
    parse args → load papers → clean abstracts → load model →
    run parallel extraction → aggregate → save results.

    Logs total papers processed, entities extracted per type,
    and wall-clock time on completion.
    """
    pass