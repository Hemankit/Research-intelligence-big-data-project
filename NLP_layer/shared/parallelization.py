"""
parallel.py
-----------
Shared joblib parallelization utilities used across the NLP layer.

Provides reusable helpers for running per-document NLP tasks (text
cleaning, NER inference) across CPU cores using joblib's Parallel and
delayed interfaces. Also handles corpus chunking and progress tracking
so that neither the NER pipeline nor topic modeling need to reimplement
these patterns independently.

The NER pipeline is the primary consumer of this module since each
document is processed independently — a pattern that is trivially
parallelizable. Topic modeling (BERTopic) handles its own internal
batching and does not rely on this module for its core computation.

Dependencies: joblib, tqdm
"""

from joblib import Parallel, delayed
from tqdm import tqdm


class ParallelExecutor:
    """
    Configures and runs joblib-based parallel execution for per-document
    NLP tasks across CPU cores.

    Wraps joblib.Parallel with sensible defaults for NLP workloads and
    adds corpus chunking and progress tracking on top.

    Parameters
    ----------
    n_jobs : int
        Number of CPU cores to use. -1 uses all available cores.
        -2 uses all cores minus one, leaving one free for the OS.
        Default: -2.
    backend : str
        joblib backend to use. 'loky' (default) is recommended for
        CPU-bound NLP tasks. Use 'threading' only if the workload
        releases the GIL (e.g., some numpy operations).
    verbose : int
        joblib verbosity level (0 = silent, 10 = detailed). Default: 0.
        Progress is tracked separately via tqdm.
    """

    def __init__(self, n_jobs: int = -2, backend: str = "loky", verbose: int = 0):
        self.n_jobs = n_jobs
        self.backend = backend
        self.verbose = verbose

    def run(self, func, items: list, **kwargs) -> list:
        """
        Apply a function to each item in a list in parallel.

        The simplest entry point — applies func(item) to every item
        using joblib.Parallel. Additional keyword arguments are forwarded
        to func as fixed parameters.

        Parameters
        ----------
        func : callable
            Function to apply to each item. Must be picklable (top-level
            function or a class method) for loky backend compatibility.
        items : list
            List of inputs to process — typically a list of abstract
            strings or paper dicts.
        **kwargs : dict
            Fixed keyword arguments forwarded to func on every call.

        Returns
        -------
        list
            Results in the same order as the input items list.
        """
        results = Parallel(n_jobs=self.n_jobs, backend=self.backend, verbose=self.verbose)(
            delayed(func)(item, **kwargs) for item in items
        )
        return results 

    def run_with_progress(self, func, items: list, desc: str = "Processing", **kwargs) -> list:
        """
        Apply a function to each item in parallel with a tqdm progress bar.

        Wraps run() with a tqdm progress bar so long-running NLP jobs
        (e.g., NER over 100k abstracts) show incremental progress rather
        than appearing to hang. The progress bar updates per completed item.

        Parameters
        ----------
        func : callable
            Function to apply to each item.
        items : list
            List of inputs to process.
        desc : str
            Label shown on the tqdm progress bar (default: 'Processing').
        **kwargs : dict
            Fixed keyword arguments forwarded to func on every call.

        Returns
        -------
        list
            Results in the same order as the input items list.
        """
        results = Parallel(n_jobs=self.n_jobs, backend=self.backend, return_as="generator")(
        delayed(func)(item, **kwargs) for item in items
    )
        return list(tqdm(results, total=len(items), desc=desc, unit="item"))


    def run_in_chunks(self, func, items: list, chunk_size: int = 1000, **kwargs) -> list:
        """
        Apply a function to items in parallel, processing in chunks.

        Splits the input list into chunks of chunk_size before dispatching
        to joblib. Useful when each item is lightweight but the full list
        is too large to hold all intermediate results in memory at once
        (e.g., processing 200k abstracts where each NER output is large).

        Chunks are processed sequentially while items within each chunk
        are processed in parallel.

        Parameters
        ----------
        func : callable
            Function to apply to each item.
        items : list
            Full list of inputs to process.
        chunk_size : int
            Number of items per chunk (default: 1000).
        **kwargs : dict
            Fixed keyword arguments forwarded to func on every call.

        Returns
        -------
        list
            Flattened results list in the same order as the input.
        """
        chunks = chunk_list(items, chunk_size)  # produce all chunks upfront
        results = []
        for chunk in chunks:
            chunk_results = self.run(func, chunk, **kwargs)
            results.extend(chunk_results)
        return results


def chunk_list(items: list, chunk_size: int) -> list[list]:
    """
    Split a list into sequential chunks of a given size.

    Standalone utility function used by ParallelExecutor.run_in_chunks()
    and available for direct use by other modules that need to batch
    a corpus without running parallel execution.

    Parameters
    ----------
    items : list
        The list to split.
    chunk_size : int
        Maximum number of items per chunk. The final chunk may be
        smaller if len(items) is not divisible by chunk_size.

    Returns
    -------
    list[list]
        List of chunks, each a sublist of the original.
    """
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def get_n_jobs(reserve_cores: int = 1) -> int:
    """
    Determine a safe number of parallel jobs based on available CPU cores.

    Returns the total number of logical CPU cores minus reserve_cores,
    with a minimum of 1. Provides a sensible default for NLP workloads
    that want to leave some cores free for the OS and other processes.

    Parameters
    ----------
    reserve_cores : int
        Number of cores to leave unused (default: 1).

    Returns
    -------
    int
        Recommended number of parallel jobs.
    """
    pass