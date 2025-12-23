"""
Redis Clients Module
====================

Provides Redis client utilities, key standards, and storage helpers.
"""

from .redis_key_standards import (
    RedisKeyStandards,
    DatabaseAwareKeyBuilder,
    RedisDatabase,
    ConnectionType,
)
# ✅ FIXED: redis_key_mapping removed - use redis_field_utils instead
from .redis_field_utils import (
    normalize_redis_tick_data,
    map_tick_for_pattern,
    map_tick_for_scanner,
    map_tick_for_dashboard,
    resolve_redis_key,
    get_redis_key_fields,
)
# ✅ REMOVED: CanonicalKeyBuilder - use DatabaseAwareKeyBuilder instead
# ✅ REMOVED: CanonicalRedisClient - use UnifiedRedisManager instead
from .decorators import (
    canonical_keys,
)

# Connection managers
from .connection_manager import RedisConnectionManager

# Async connection pool (redis-py 7.x native asyncio)
from .async_connection_pool import (
    AsyncRedisPool,
    get_realtime_client,
    get_system_client,
    get_analytics_client,
    get_validator_client,
)

# Export redis_manager singleton from redis_client
try:
    from .redis_client import redis_manager
except ImportError:
    redis_manager = None

# Export UnifiedRedisManager (preferred interface) and RedisManager82 for backward compatibility
try:
    from .redis_client import UnifiedRedisManager, RedisManager82, get_redis_client
except ImportError:
    UnifiedRedisManager = None
    RedisManager82 = None
    get_redis_client = None

__all__ = [
    # Key standards
    "RedisKeyStandards",
    "DatabaseAwareKeyBuilder",  # ✅ Preferred - use static methods directly
    "RedisDatabase",
    "ConnectionType",
    # ✅ FIXED: redis_key_mapping removed - use redis_field_utils instead
    "normalize_redis_tick_data",
    "map_tick_for_pattern",
    "map_tick_for_scanner",
    "map_tick_for_dashboard",
    "resolve_redis_key",
    "get_redis_key_fields",
    # ✅ REMOVED: CanonicalRedisClient - use UnifiedRedisManager instead
    # Decorators
    "canonical_keys",
    # Connection managers (sync)
    "RedisConnectionManager",
    # Async connection pool
    "AsyncRedisPool",
    "get_realtime_client",
    "get_system_client",
    "get_analytics_client",
    "get_validator_client",
    # Redis managers
    "redis_manager",
    "UnifiedRedisManager",  # Preferred interface
    "RedisManager82",  # Backward compatibility
    "get_redis_client",
]
