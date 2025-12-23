"""
Unified Tick Storage - Single Source of Truth for Tick Storage

CRITICAL: This module ELIMINATES the tick key universe split by enforcing
standards-compliant keys ONLY. All tick storage operations MUST use this module.

Key Standards:
- Hash: ticks:{canonical_symbol} (plural, standards-compliant)
- Stream: ticks:stream:{canonical_symbol} OR ticks:intraday:processed (global)
- NO legacy keys: tick:{symbol} (singular) is FORBIDDEN

This ensures:
- Consistent key format across all components
- No split-brain scenarios (some code writes tick:*, other reads ticks:*)
- Single source of truth for tick data
"""

import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from shared_core.redis_clients.redis_client import RedisClientFactory
from shared_core.redis_clients.redis_key_standards import (
    DatabaseAwareKeyBuilder,
    RedisKeyStandards,
    RedisDatabase,
)

logger = logging.getLogger(__name__)


class UnifiedTickStorage:
    """
    Unified Tick Storage - Single source of truth for tick storage operations.
    
    CRITICAL: Uses ONLY standards-compliant keys:
    - ticks:{canonical_symbol} (hash) - NOT tick:{symbol}
    - ticks:stream:{canonical_symbol} (stream) - per-symbol stream
    - ticks:intraday:processed (stream) - global processed stream
    
    This eliminates the tick key universe split where some code writes
    tick:* (singular) and other code reads ticks:* (plural).
    """
    
    def __init__(self, redis_client=None, db: int = 1):
        """
        Initialize Unified Tick Storage.
        
        Args:
            redis_client: Optional Redis client. If None, creates one for DB1.
            db: Redis database number (default: 1 for live data)
        """
        # ✅ FIXED: Use DatabaseAwareKeyBuilder directly (no instance needed)
        from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
        self.db = db
        
        if redis_client:
            self.redis_client = redis_client
        else:
            try:
                self.redis_client = RedisClientFactory.get_trading_client()
                logger.info(f"✅ UnifiedTickStorage initialized with DB {db}")
            except Exception as e:
                logger.error(f"❌ Failed to initialize UnifiedTickStorage: {e}")
                raise
    
    def store_tick(self, symbol: str, tick_data: Dict[str, Any], 
                   ttl: Optional[int] = 3600, 
                   publish_to_stream: bool = True) -> bool:
        """
        Store tick data using ONLY standards-compliant keys.
        
        CRITICAL: This method uses:
        - ticks:{canonical_symbol} (hash) - NOT tick:{symbol}
        - ticks:stream:{canonical_symbol} (stream) - per-symbol stream
        - ticks:intraday:processed (stream) - global processed stream
        
        Args:
            symbol: Trading symbol (will be canonicalized)
            tick_data: Tick data dictionary
            ttl: Time to live in seconds for hash (default: 3600 = 1 hour)
            publish_to_stream: If True, also publish to streams (default: True)
            
        Returns:
            True if stored successfully, False otherwise
        """
        try:
            # ✅ SOURCE OF TRUTH: Canonicalize symbol
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            
            # ✅ STANDARDS-COMPLIANT: Use live_ticks_hash() method
            tick_hash_key = DatabaseAwareKeyBuilder.live_ticks_hash(canonical_symbol)
            
            # Verify database assignment
            db_assigned = DatabaseAwareKeyBuilder.get_database(tick_hash_key)
            if db_assigned != RedisDatabase.DB1_LIVE_DATA:
                logger.warning(
                    f"⚠️ Tick hash key {tick_hash_key} assigned to wrong DB: "
                    f"{db_assigned} (expected DB1)"
                )
            
            # ✅ FIXED: Sanitize tick_data for Redis storage (convert ALL to strings)
            sanitized_data = {}
            for key, value in tick_data.items():
                if value is None:
                    continue  # Skip None values
                # Handle numpy types first
                if hasattr(value, 'item'):  # numpy scalar
                    value = value.item()
                # Handle complex types
                if isinstance(value, (dict, list)):
                    sanitized_data[key] = json.dumps(value)
                else:
                    # Convert EVERYTHING to string (Redis hset mapping requires strings)
                    sanitized_data[key] = str(value)
            
            # Store in hash (standards-compliant key)
            if sanitized_data:
                self.redis_client.hset(tick_hash_key, mapping=sanitized_data)
                if ttl:
                    self.redis_client.expire(tick_hash_key, ttl)
            
            logger.debug(f"✅ Stored tick in hash: {tick_hash_key}")
            
            # Publish to streams if requested
            if publish_to_stream:
                # ✅ STANDARDS-COMPLIANT: Use intraday_processed_stream(symbol) per spec
                stream_key = DatabaseAwareKeyBuilder.intraday_processed_stream(canonical_symbol)
                try:
                    # Convert tick_data to stream format (all values must be strings)
                    stream_data = {}
                    for key, value in tick_data.items():
                        if isinstance(value, (dict, list)):
                            stream_data[key] = json.dumps(value)
                        else:
                            stream_data[key] = str(value)
                    
                    self.redis_client.xadd(
                        stream_key,
                        stream_data,
                        maxlen=10000,  # Keep last 10k ticks per symbol
                        approximate=True
                    )
                    logger.debug(f"✅ Published tick to per-symbol processed stream: {stream_key}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to publish to per-symbol processed stream {stream_key}: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to store tick for {symbol}: {e}")
            return False
    
    def get_tick(self, symbol: str) -> Dict[str, Any]:
        """
        Get tick data using ONLY standards-compliant keys.
        
        CRITICAL: This method reads from:
        - ticks:{canonical_symbol} (hash) - NOT tick:{symbol}
        
        Args:
            symbol: Trading symbol (will be canonicalized)
            
        Returns:
            Dictionary of tick data, or empty dict if not found
        """
        try:
            # ✅ SOURCE OF TRUTH: Canonicalize symbol
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            
            # ✅ STANDARDS-COMPLIANT: Use live_ticks_hash() method
            tick_hash_key = DatabaseAwareKeyBuilder.live_ticks_hash(canonical_symbol)
            
            # Verify database assignment
            db_assigned = DatabaseAwareKeyBuilder.get_database(tick_hash_key)
            if db_assigned != RedisDatabase.DB1_LIVE_DATA:
                logger.warning(
                    f"⚠️ Tick hash key {tick_hash_key} assigned to wrong DB: "
                    f"{db_assigned} (expected DB1)"
                )
            
            # Read from hash (standards-compliant key)
            tick_data_raw = self.redis_client.hgetall(tick_hash_key)
            
            if tick_data_raw:
                logger.debug(f"✅ Retrieved tick from hash: {tick_hash_key}")
                # ✅ FIX: Normalize field names for conflict resolution
                from shared_core.redis_clients import normalize_redis_tick_data
                # Decode bytes if needed (Redis returns bytes)
                if isinstance(list(tick_data_raw.values())[0] if tick_data_raw else None, bytes):
                    tick_data_raw = {
                        k.decode('utf-8') if isinstance(k, bytes) else k:
                        v.decode('utf-8') if isinstance(v, bytes) else v
                        for k, v in tick_data_raw.items()
                    }
                return normalize_redis_tick_data(tick_data_raw)
            else:
                logger.debug(f"ℹ️ No tick data found for {canonical_symbol} at {tick_hash_key}")
                return {}
                
        except Exception as e:
            logger.error(f"❌ Failed to get tick for {symbol}: {e}")
            return {}
    
    def get_tick_from_stream(self, symbol: str, count: int = 1) -> List[Dict[str, Any]]:
        """
        Get recent ticks from per-symbol stream.
        
        Args:
            symbol: Trading symbol (will be canonicalized)
            count: Number of recent ticks to retrieve (default: 1)
            
        Returns:
            List of tick dictionaries (most recent first)
        """
        try:
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            stream_key = DatabaseAwareKeyBuilder.live_ticks_stream(canonical_symbol)
            
            # Read from stream (most recent first)
            messages = self.redis_client.xrevrange(stream_key, count=count)
            
            ticks = []
            from shared_core.redis_clients import normalize_redis_tick_data
            for msg_id, data in messages:
                # Convert stream data back to dict
                tick = {}
                for key, value in data.items():
                    # Decode bytes if needed
                    if isinstance(key, bytes):
                        key = key.decode('utf-8')
                    if isinstance(value, bytes):
                        value = value.decode('utf-8')
                    try:
                        # Try to parse JSON values
                        tick[key] = json.loads(value)
                    except (json.JSONDecodeError, TypeError):
                        tick[key] = value
                # ✅ FIX: Normalize field names for conflict resolution
                normalized_tick = normalize_redis_tick_data(tick)
                ticks.append(normalized_tick)
            
            return ticks
            
        except Exception as e:
            logger.error(f"❌ Failed to get ticks from stream for {symbol}: {e}")
            return []
    
    def exists(self, symbol: str) -> bool:
        """
        Check if tick data exists for symbol.
        
        Args:
            symbol: Trading symbol (will be canonicalized)
            
        Returns:
            True if tick data exists, False otherwise
        """
        try:
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            tick_hash_key = DatabaseAwareKeyBuilder.live_ticks_hash(canonical_symbol)
            return self.redis_client.exists(tick_hash_key) > 0
        except Exception as e:
            logger.error(f"❌ Failed to check tick existence for {symbol}: {e}")
            return False


# Global singleton instance
_unified_tick_storage = None


def get_unified_tick_storage(redis_client=None, db: int = 1) -> UnifiedTickStorage:
    """
    Get singleton UnifiedTickStorage instance.
    
    Args:
        redis_client: Optional Redis client
        db: Redis database number (default: 1)
        
    Returns:
        UnifiedTickStorage instance
    """
    global _unified_tick_storage
    if _unified_tick_storage is None:
        _unified_tick_storage = UnifiedTickStorage(redis_client=redis_client, db=db)
    return _unified_tick_storage

