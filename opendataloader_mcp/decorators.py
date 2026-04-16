import time
import hashlib
from functools import wraps
from typing import Callable, Dict, Any
from datetime import datetime

from .config import logger, MAX_RETRIES, RETRY_BACKOFF, CACHE_SIZE
import json


_pdf_cache: Dict[str, Dict[str, Any]] = {}
_cache_metadata: Dict[str, Dict[str, Any]] = {}


def retry_operation(max_retries: int = MAX_RETRIES, backoff: float = RETRY_BACKOFF):
    """Decorator to retry failed operations with exponential backoff."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    logger.info(f"Attempt {attempt + 1}/{max_retries} for {func.__name__}")
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_retries - 1:
                        wait_time = backoff * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"{func.__name__} failed: {str(e)}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"{func.__name__} failed after {max_retries} attempts: {str(e)}")
            raise last_error
        return wrapper
    return decorator



def track_metrics(func: Callable) -> Callable:
    """Decorator to track performance metrics."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            
            # Add metrics to result if JSON
            if isinstance(result, str):
                try:
                    data = json.loads(result)
                    if isinstance(data, dict):
                        data["_metrics"] = {
                            "execution_time_ms": round(elapsed * 1000, 2),
                            "timestamp": datetime.now().isoformat(),
                            "function": func.__name__
                        }
                        return json.dumps(data, ensure_ascii=False, indent=2)
                except:
                    pass
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"{func.__name__} failed in {elapsed:.2f}s: {str(e)}")
            raise
    return wrapper



def cache_result(func: Callable) -> Callable:
    """Decorator to cache PDF parsing results."""
    @wraps(func)
    def wrapper(source: str, format: str = "markdown", *args, **kwargs):
        # Create cache key
        cache_key = f"{source}_{format}_{str(args)}_{str(sorted(kwargs.items()))}"
        cache_hash = hashlib.md5(cache_key.encode()).hexdigest()
        
        # Check cache
        if cache_hash in _pdf_cache:
            logger.info(f"Cache hit for {source} with format {format}")
            return _pdf_cache[cache_hash]
        
        # Process and cache
        result = func(source, format, *args, **kwargs)
        
        # Store in cache (limit to CACHE_SIZE)
        if len(_pdf_cache) >= CACHE_SIZE:
            # Remove oldest entry
            oldest_key = min(_cache_metadata.keys(), key=lambda k: _cache_metadata[k]['timestamp'])
            del _pdf_cache[oldest_key]
            del _cache_metadata[oldest_key]
            logger.info(f"Cache evicted oldest entry")
        
        _pdf_cache[cache_hash] = result
        _cache_metadata[cache_hash] = {'timestamp': time.time(), 'source': source}
        logger.info(f"Cached result for {source}")
        
        return result
    return wrapper



def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics."""
    return {
        "cached_items": len(_pdf_cache),
        "capacity": CACHE_SIZE,
        "utilization": f"{(len(_pdf_cache) / CACHE_SIZE * 100):.1f}%"
    }

def clear_cache_storage() -> int:
    """Clear all cached items and return count."""
    global _pdf_cache, _cache_metadata
    cache_size = len(_pdf_cache)
    _pdf_cache.clear()
    _cache_metadata.clear()
    logger.info(f"Cache cleared - freed {cache_size} entries")
    return cache_size
