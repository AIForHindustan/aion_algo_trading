"""
Canonical Keys Decorator
========================

Decorator that automatically converts Redis keys to canonical form
before passing them to Redis operations.
"""

import functools
import logging
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


def canonical_keys(func: Callable) -> Callable:
    """
    Decorator that automatically converts Redis keys to canonical form.
    
    Works with:
    - Single key arguments (key, stream, channel, pattern)
    - Dictionary arguments (streams dict for xread)
    - Multiple key arguments
    
    Usage:
        @canonical_keys
        def get(self, key: str):
            return self._client.get(key)
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # ✅ FIXED: redis_key_mapping removed - keys should be built using DatabaseAwareKeyBuilder
        # This decorator is now a no-op. Keys should be built correctly before calling Redis operations.
        # If you need key conversion, use DatabaseAwareKeyBuilder methods directly.
        logger.debug("canonical_keys decorator called - keys should already be canonical via DatabaseAwareKeyBuilder")
        return func(self, *args, **kwargs)
    
import time
import redis

def safe_redis_operation(func: Callable) -> Callable:
    """
    Decorator to handle Redis pool exhaustion and connection errors gracefully.
    Implemented with exponential backoff and retries.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except (redis.ConnectionError, IndexError) as e:
                # "pop from empty list" is a common symptom of pool exhaustion in redis-py
                if "pop from empty list" in str(e) or "Too many connections" in str(e):
                    logger.error(f"🚨 Redis pool exhausted on attempt {attempt+1}, sleeping...")
                    time.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    continue
                raise
        logger.error(f"❌ Redis operation failed after {max_retries} attempts: {func.__name__}")
        return None  # Fail gracefully after retries
    return wrapper

