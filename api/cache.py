"""
Caching utilities for API endpoints.
"""

import hashlib
import json
import time
from typing import Any, Callable, Dict
from functools import wraps
import logging

logger = logging.getLogger(__name__)

# In-memory cache with TTL
_cache: Dict[str, Dict[str, Any]] = {}
CACHE_TTL = 300  # 5 minutes in seconds


def _generate_cache_key(func_name: str, *args, **kwargs) -> str:
    """Generate a cache key from function name and arguments."""
    # Remove 'db' from kwargs as it's not serializable and not needed for cache key
    cache_kwargs = {k: v for k, v in kwargs.items() if k != "db"}

    # Create a stable string representation
    key_data = {
        "func": func_name,
        "args": args,
        "kwargs": cache_kwargs,
    }
    key_str = json.dumps(key_data, sort_keys=True, default=str)
    return hashlib.md5(key_str.encode()).hexdigest()


def cached(ttl: int = CACHE_TTL):
    """
    Decorator to cache API endpoint responses.

    Args:
        ttl: Time to live in seconds (default: 5 minutes)
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            cache_key = _generate_cache_key(func.__name__, *args, **kwargs)

            # Check cache
            if cache_key in _cache:
                entry = _cache[cache_key]
                if time.time() - entry["timestamp"] < ttl:
                    logger.debug(f"Cache HIT for {func.__name__}")
                    return entry["data"]
                else:
                    # Expired, remove from cache
                    del _cache[cache_key]
                    logger.debug(f"Cache EXPIRED for {func.__name__}")

            # Cache miss, execute function
            logger.debug(f"Cache MISS for {func.__name__}")
            result = await func(*args, **kwargs)

            # Store in cache
            _cache[cache_key] = {
                "data": result,
                "timestamp": time.time(),
            }

            return result

        return wrapper

    return decorator


def clear_cache():
    """Clear all cached entries."""
    global _cache
    _cache.clear()
    logger.info("Cache cleared")


def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics."""
    now = time.time()
    active_entries = sum(
        1 for entry in _cache.values() if now - entry["timestamp"] < CACHE_TTL
    )
    expired_entries = len(_cache) - active_entries

    return {
        "total_entries": len(_cache),
        "active_entries": active_entries,
        "expired_entries": expired_entries,
        "ttl_seconds": CACHE_TTL,
    }
