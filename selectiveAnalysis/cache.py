"""
cache.py
--------
Caches selective analysis results to avoid reprocessing the same query.

Since the Selective Full-Text Analysis Layer is triggered on-demand by
user queries, the same or similar queries may be submitted repeatedly
from the dashboard. Caching results avoids redundant Elasticsearch queries,
Hive reads, and NLP inference for queries that have already been processed.

Cache keys are derived from the normalized query string. Results are stored
with a configurable TTL (time-to-live) after which they expire and the
analysis is rerun to reflect newly indexed papers.

Two cache backends are supported:
  - In-memory (default): Simple dict-based cache, fast but not persistent
    across process restarts. Suitable for development and single-instance
    deployments.
  - Redis: Persistent, shareable across multiple FastAPI worker processes.
    Recommended for production where multiple dashboard users may submit
    similar queries concurrently.

Dependencies: hashlib (stdlib), time (stdlib), json (stdlib)
              redis (optional, for Redis backend)
"""

import hashlib
import json
import logging
import os
import time
import redis

logger = logging.getLogger(__name__)

# Default TTL for cached results in seconds (6 hours)
DEFAULT_TTL_SECONDS = 6 * 60 * 60


class AnalysisCache:
    """
    Caches selective full-text analysis results keyed by normalized query.

    Supports in-memory and Redis backends. The backend is selected at
    instantiation time via the backend parameter.

    Parameters
    ----------
    backend : str
        Cache backend to use. 'memory' for in-process dict cache,
        'redis' for Redis-backed persistent cache. Default: 'memory'.
    ttl_seconds : int
        Time-to-live for cached results in seconds. Results older than
        this are considered stale and trigger a fresh analysis.
        Default: 21600 (6 hours).
    redis_url : str, optional
        Redis connection URL, e.g. 'redis://localhost:6379/0'.
        Required when backend='redis'. Reads REDIS_URL env var if not
        passed explicitly.
    max_memory_entries : int
        Maximum number of entries in the in-memory cache before the
        oldest entries are evicted. Default: 100. Has no effect for
        the Redis backend.
    """

    def __init__(self, backend="memory", ttl_seconds=DEFAULT_TTL_SECONDS,
             redis_url=None, max_memory_entries=100):
      self.backend = backend
      self.ttl_seconds = ttl_seconds
      self.max_memory_entries = max_memory_entries
      self._cache = {}          # was `cache = {}` inside get()
      self._hit_count = 0
      self._miss_count = 0
      self._redis = None
      if backend == "redis":    # was `redis.Redis.from_url(...)` inside get()
        self._redis = redis.Redis.from_url(
            redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        )

    def get(self, query: str) -> dict | None:
        """
        Retrieve cached analysis results for a query if available and fresh.

        Returns None if the query has not been cached or if the cached
        result has exceeded its TTL. Callers should run fresh analysis
        when None is returned.

        Parameters
        ----------
        query : str
            The user query string. Normalized before hashing so minor
            variations (extra whitespace, case differences) hit the
            same cache entry.

        Returns
        -------
        dict | None
            Cached analysis result dict as returned by run.py's
            run_analysis() function, or None if not cached / expired.
        """
        key = self._make_key(query) # define key instead of empty dict to avoid losing everything with each new call

        if self.backend == "memory":
          # see if the key exists in memory cache
          entry = self._cache.get(key)
          # if it exists and is not expired, return the cached result
          if entry and not self._is_expired(entry["cached_at"]):
            self._hit_count += 1
            logger.info(f"Cache hit for query: '{query}'")
            return entry["result"]
          # if it exists but is expired, remove it from cache
          if entry:
            logger.info(f"Cache expired for query: '{query}'")
            del self._cache[key]  # remove expired entry
        
        elif self.backend == "redis":
           cached_data = self._redis.get(key)
           if cached_data:
             entry = json.loads(cached_data)
             if not self._is_expired(entry["cached_at"]):
               self._hit_count += 1
               logger.info(f"Cache hit for query: '{query}'")
               return entry["result"]
             else:
               logger.info(f"Cache expired for query: '{query}'")
               self._redis.delete(key)  # remove expired entry
        self._miss_count += 1
        logger.info(f"Cache miss for query: '{query}'")
        return None

            

    def set(self, query: str, result: dict) -> None:
        """
        Store analysis results for a query in the cache.

        Stores the result with the current timestamp so TTL can be
        enforced on retrieval. For the memory backend, evicts the
        oldest entry if max_memory_entries would be exceeded.

        Parameters
        ----------
        query : str
            The user query string. Normalized before hashing.
        result : dict
            Analysis result dict to cache. Must be JSON-serializable
            for the Redis backend.
        """
        # Create the cache entry with the result and current timestamp
        key = self._make_key(query)
        entry = {"result": result, "cached_at": time.time()}
        # check max_memory_entries for memory backend and evict oldest if needed, then store the new entry
        if self.backend == "memory":
          if len(self._cache) >= self.max_memory_entries:
            self._evict_oldest()
          self._cache[key] = entry
          logger.info(f"Cached result for query: '{query}' (memory backend)")
        # For Redis, set the key with the JSON-serialized entry
        elif self.backend == "redis":
          self._redis.set(key, json.dumps(entry))
          logger.info(f"Cached result for query: '{query}' (Redis backend)")

    def invalidate(self, query: str) -> None:
        """
        Remove a specific query's cached result before its TTL expires.

        Useful for forcing a fresh analysis after new papers have been
        indexed that are relevant to the query.

        Parameters
        ----------
        query : str
            The user query string to invalidate.
        """
        # Generate the cache key for the query before TTL expiration and remove it from cache
        key = self._make_key(query)
        if self.backend == "memory":
          if key in self._cache:
            del self._cache[key]
            logger.info(f"Invalidated cache for query: '{query}' (memory backend)")
        elif self.backend == "redis":
          self._redis.delete(key)
          logger.info(f"Invalidated cache for query: '{query}' (Redis backend)")


    def clear(self) -> None:
        """
        Remove all cached results.

        Used during testing or when a full reindex has changed the
        underlying data enough that all cached results are stale.
        """
        if self.backend == "memory":
          self._cache.clear()
          logger.info("Cleared all cache entries (memory backend)")
        elif self.backend == "redis":
          # Assuming all keys are prefixed with 'analysis:' for namespacing
          for key in self._redis.scan_iter("analysis:*"):
            self._redis.delete(key)
          logger.info("Cleared all cache entries (Redis backend)")

    def stats(self) -> dict:
        """
        Return cache statistics for monitoring and debugging.

        Returns
        -------
        dict
            Statistics with keys:
            - backend (str): Active backend name
            - total_entries (int): Number of cached queries
            - hit_count (int): Total cache hits since instantiation
            - miss_count (int): Total cache misses since instantiation
            - hit_rate (float): hit_count / (hit_count + miss_count)
            - oldest_entry_age_seconds (float): Age of oldest cache entry
        """
        stats = {
            "backend": self.backend,
            "hit_count": self._hit_count,
            "miss_count": self._miss_count,
            "total_entries": len(self._cache) if self.backend == "memory" else self._redis.dbsize(),
            "hit_rate": self._hit_count / (self._hit_count + self._miss_count) if (self._hit_count + self._miss_count) > 0 else 0.0,
            "oldest_entry_age_seconds": time.time() - min((entry["cached_at"] for entry in self._cache.values()), default=time.time()) if self.backend == "memory" else None
        
        }
        return stats

    def _make_key(self, query: str) -> str:
        """
        Normalize a query string and hash it into a cache key.

        Normalization: lowercase, strip whitespace, collapse internal
        spaces. Hashing: MD5 of the normalized string for compact keys.
        Prefixed with 'analysis:' to namespace keys in Redis.

        Parameters
        ----------
        query : str
            Raw query string from the user.

        Returns
        -------
        str
            Cache key string, e.g. 'analysis:a3f2c1d4e5b6...'.
        """
        normalized = " ".join(query.lower().strip().split())
        key_hash = hashlib.md5(normalized.encode("utf-8")).hexdigest()
        return f"analysis:{key_hash}"

    def _is_expired(self, cached_at: float) -> bool:
        """
        Return True if a cached entry has exceeded its TTL.

        Parameters
        ----------
        cached_at : float
            Unix timestamp (from time.time()) when the entry was cached.

        Returns
        -------
        bool
            True if (current_time - cached_at) > ttl_seconds.
        """
        current_time = time.time()
        return (current_time - cached_at) > self.ttl_seconds

    def _evict_oldest(self) -> None:
        """
        Evict the oldest entry from the in-memory cache.

        Called when max_memory_entries would be exceeded. Finds the
        entry with the smallest cached_at timestamp and removes it.
        Only applies to the memory backend.
        """
        if self.backend == "memory":
            # Find the key of the oldest entry based on cached_at timestamp
            oldest_key = min(self._cache, key=lambda k: self._cache[k]["cached_at"], default=None)
            # Remove the oldest entry from the cache
            if oldest_key:
                del self._cache[oldest_key]
                logger.info(f"Evicted oldest cache entry: '{oldest_key}' (memory backend)")