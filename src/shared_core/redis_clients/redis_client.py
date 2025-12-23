"""Redis wrapper with auto-reconnection and thread-safe fallback

Features:
- Automatic reconnection on connection loss
- Retry logic with exponential backoff (RESP3 + redis-py built-in retry)
- Health checks and connection monitoring
- Graceful fallback to in-memory storage
- Thread-safe operations
- Queue for failed operations during disconnection
- Safe JSON serialization for all data types
- Circuit breaker for fast-fail during outages
- Proper connection pooling to prevent connection storms

Production benefits:
- Self-healing connections
- No manual intervention needed
- Zero message loss with retry queue
- Seamless failover
- RESP3 protocol for Redis 8.x compatibility
- XADD trim & group auto-create for Streams memory stability

⚠️ TROUBLESHOOTING & DOCUMENTATION:
- Read `redis_files/redis_document.md` for architecture overview and troubleshooting guide
- Always read method docstrings in this file before making assumptions
- Method signatures, parameters, return values, and data formats are documented in docstrings
- Storage locations (database, stream keys) are documented in each streaming method's docstring
- Don't assume method behavior - check docstrings for exact implementation details
"""

from __future__ import annotations

# ✅ CRITICAL: Set up path FIRST before any imports that depend on project structure
import sys
from pathlib import Path
_redis_file_path = Path(__file__).resolve()
_redis_files_dir = _redis_file_path.parent
_parent_root = _redis_files_dir.parent  # aion_algo_trading
if str(_parent_root) not in sys.path:
    sys.path.insert(0, str(_parent_root))

import time
import os
import json
import random
import redis
from redis.connection import ConnectionPool
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from redis.exceptions import (
    BusyLoadingError,
    ConnectionError,
    TimeoutError as RedisTimeoutError,
    AuthenticationError,
    ResponseError,
)
import logging
from threading import Lock, Thread
import threading
from collections import deque, defaultdict
from datetime import datetime, date, timedelta
from decimal import Decimal
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Callable, Iterable, Tuple, TypeVar, Union

# Hiredis optimization (C-level parser)
try:
    import hiredis
    from redis.connection import HiredisParser
    HIREDIS_AVAILABLE = True
except ImportError:
    HIREDIS_AVAILABLE = False
    HiredisParser = None

from shared_core.utils.time_utils import INDIAN_TIME_PARSER
from shared_core.config_utils.timestamp_normalizer import TimestampNormalizer
from shared_core.config_utils.yaml_field_loader import (
    get_field_mapping_manager,
    resolve_session_field,
)
from shared_core.redis_clients.redis_key_standards import (
    DatabaseAwareKeyBuilder,
    RedisKeyStandards,
)

from shared_core.redis_clients.decorators import safe_redis_operation
from shared_core.redis_clients.connection_manager import RedisConnectionManager
try:
    from shared_core.config.worker_config import PROCESS_POOL_CONFIG
except ImportError:
    PROCESS_POOL_CONFIG = {}

try:
    import numpy as np

    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    np = None

# Circuit breaker implementation (consolidated)
import time
from enum import Enum

class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "CLOSED"      # Normal operation
    OPEN = "OPEN"          # Circuit is open, requests blocked
    HALF_OPEN = "HALF_OPEN"  # Testing if service is back

class CircuitBreaker:
    """Circuit Breaker implementation for preventing cascading failures"""
    
    def __init__(
        self, 
        name: str = "redis",
        failure_threshold: int = 3, 
        reset_timeout: int = 30,
        success_threshold: int = 2,
        timeout: Optional[float] = None
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.success_threshold = success_threshold
        self.timeout = timeout
        
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
    
    def should_allow_request(self) -> bool:
        """Check if request should be allowed based on circuit state"""
        if self.state == CircuitState.CLOSED:
            return True
        elif self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time >= self.reset_timeout:
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
                return True
            return False
        elif self.state == CircuitState.HALF_OPEN:
            return True
        return False
    
    def record_success(self):
        """Record a successful operation"""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                logger.info(f"Circuit breaker {self.name} closed after {self.success_count} successes")
        elif self.state == CircuitState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)
    
    def record_failure(self, exception: Exception):
        """Record a failed operation"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
    
    def get_state(self) -> str:
        """Get current circuit breaker state"""
        return self.state.value

# Singleton circuit breaker for Redis (shared across all client instances)
# Best practice: Circuit breakers should be shared for shared infrastructure resources
# This ensures consistent failure detection across all Redis client instances
_shared_redis_circuit_breaker: Optional[CircuitBreaker] = None
_shared_circuit_breaker_lock = Lock()

def redis_circuit_breaker() -> CircuitBreaker:
    """
    Get or create the shared singleton circuit breaker for Redis operations.
    
    Best Practice (Redis 8.2):
    - Circuit breakers should be shared/singleton for shared infrastructure
    - All clients share the same failure state, ensuring consistent behavior
    - When Redis goes down, all clients immediately benefit from the open circuit
    - Prevents per-instance discovery delays and inconsistent state
    """
    global _shared_redis_circuit_breaker
    if _shared_redis_circuit_breaker is None:
        with _shared_circuit_breaker_lock:
            # Double-check pattern
            if _shared_redis_circuit_breaker is None:
                _shared_redis_circuit_breaker = CircuitBreaker(
                    name="redis",
                    failure_threshold=3,
                    reset_timeout=30,
                    success_threshold=2
                )
                logger.info("Created shared singleton circuit breaker for Redis")
    return _shared_redis_circuit_breaker

CIRCUIT_BREAKER_AVAILABLE = True

# Configure logging
logger = logging.getLogger(__name__)

# ============================================================================
# Enhanced Redis Data Retriever with Improved Fallback Logic
# ============================================================================

class RedisDataRetriever:
    """
    Enhanced retrieval class with comprehensive fallback logic and debugging.
    
    Provides better key variant generation and detailed failure logging.
    """
    
    def __init__(self, redis_db1, redis_db2):
        self.db1 = redis_db1
        self.db2 = redis_db2
        # ✅ FIXED: DatabaseAwareKeyBuilder is static, no instance needed
        from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
        self.logger = logging.getLogger(__name__)

    def get_indicator_with_fallback(self, symbol: str, indicator: str, max_retries: int = 3) -> Optional[Any]:
        """
        Enhanced retrieval with better fallback logic and debugging
        
        Args:
            symbol: Trading symbol
            indicator: Indicator name (e.g., 'rsi', 'atr', 'gamma')
            max_retries: Maximum number of retry attempts
            
        Returns:
            Indicator value if found, None otherwise
        """
        self.logger.info(f"🔍 [RETRIEVAL] Looking for {indicator} for {symbol}")
        
        # Get ALL possible variants (not just 4)
        all_variants = self._get_all_possible_variants(symbol, indicator)
        self.logger.info(f"   Variants to try: {all_variants}")
        
        for attempt in range(max_retries):
            for key in all_variants:
                try:
                    # Try DB1 first (live data)
                    data = self.db1.get(key)
                    if data:
                        self.logger.info(f"✅ [RETRIEVAL] Found {indicator} at key: {key}")
                        return data.decode('utf-8') if isinstance(data, bytes) else data
                    
                    # Try DB2 (analytics)
                    data = self.db2.get(key)
                    if data:
                        self.logger.info(f"✅ [RETRIEVAL] Found {indicator} at key: {key} (DB2)")
                        return data.decode('utf-8') if isinstance(data, bytes) else data
                        
                except Exception as e:
                    self.logger.warning(f"   Retrieval error for {key}: {e}")
            
            # Wait before retry
            if attempt < max_retries - 1:
                import time
                time.sleep(0.1 * (attempt + 1))
        
        # Log detailed debug info
        self._log_retrieval_failure(symbol, indicator, all_variants)
        return None

    def _get_all_possible_variants(self, symbol: str, indicator: str) -> List[str]:
        """Generate comprehensive list of key variants"""
        variants = set()
        
        # Get standard variants
        from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
        standard_variants = RedisKeyStandards.get_indicator_symbol_variants(symbol)
        variants.update(standard_variants)
        
        # Add raw symbol
        variants.add(symbol)
        variants.add(symbol.upper())
        variants.add(symbol.lower())
        
        # Generate keys for each variant
        keys = set()
        for variant in variants:
            # DB1 keys using canonical builder for all categories
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            
            # Try all categories using DatabaseAwareKeyBuilder (source of truth)
            for category in ['ta', 'greeks', 'volume', 'custom']:
                keys.add(DatabaseAwareKeyBuilder.live_indicator(variant, indicator, category))
            
            # Also try without category (auto-categorizes)
            keys.add(DatabaseAwareKeyBuilder.live_indicator(variant, indicator))
            
            # Legacy keys (for backward compatibility with old data)
            keys.add(f"analysis_cache:indicators:{variant}:{indicator}")
        
        return list(keys)

    def _log_retrieval_failure(self, symbol: str, indicator: str, tried_variants: List[str]):
        """Log detailed failure information"""
        self.logger.warning(f"❌ [RETRIEVAL_DEBUG] NOT FOUND {indicator} for {symbol}")
        self.logger.warning(f"   Tried {len(tried_variants)} variants: {tried_variants[:10]}...")  # First 10 only
        
        # Check what keys actually exist for this symbol
        existing_keys = self._find_existing_keys_for_symbol(symbol)
        if existing_keys:
            self.logger.info(f"   Existing keys for {symbol}: {existing_keys[:10]}")  # First 10
        else:
            self.logger.warning(f"   No keys found for {symbol} at all!")

    def _find_existing_keys_for_symbol(self, symbol: str) -> List[str]:
        """Find what keys actually exist for this symbol (use sparingly)"""
        existing = []
        
        # Check a few common patterns (be very selective to avoid performance issues)
        patterns_to_check = [
            f"*{symbol}*",
            f"*{symbol.upper()}*", 
            f"*{symbol.lower()}*",
        ]
        
        for pattern in patterns_to_check:
            try:
                # DB1 check
                keys = self.db1.keys(pattern)
                existing.extend([k.decode('utf-8') if isinstance(k, bytes) else k for k in keys])
                
                # DB2 check  
                keys = self.db2.keys(pattern)
                existing.extend([k.decode('utf-8') if isinstance(k, bytes) else k for k in keys])
            except Exception:
                continue
                
        return existing


class SafeJSONEncoder(json.JSONEncoder):
    """Handles all non-serializable types at source"""

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif NUMPY_AVAILABLE and isinstance(obj, np.ndarray):
            return obj.tolist()
        elif NUMPY_AVAILABLE and isinstance(obj, (np.int64, np.int32)):
            return int(obj)
        elif NUMPY_AVAILABLE and isinstance(obj, (np.float64, np.float32)):
            return float(obj)
        elif hasattr(obj, "__dict__"):
            return obj.__dict__
        return super().default(obj)

# ---------------------------------------------------------------------------
# Base connection configuration (Redis 8.x)
# ---------------------------------------------------------------------------

REDIS_8_CONFIG: Dict[str, Any] = {
    "host": os.environ.get("REDIS_HOST", "localhost"),
    "port": int(os.environ.get("REDIS_PORT", 6379)),
    "password": os.environ.get("REDIS_PASSWORD") or None,
    "decode_responses": True,  # Decode to strings for easier processing
    "socket_connect_timeout": 5,
    # "socket_keepalive": True,  # REMOVED - Deprecated in Redis 8.2
    "retry_on_timeout": True,
    "max_connections": 400,  # Increased for high-frequency trading data (crawler + scanner + volume operations)         
    "health_check_interval": 30,  # Redis 8.0 enhancement
    "socket_timeout": 10,  # ✅ Matched to Zerodha intraday crawler connection_timeout (10s)
    # Zerodha WebSocket uses 10-15s connection timeout for high-frequency tick data
    # This prevents premature timeouts during tick bursts while maintaining responsiveness
}

# Redis 8.2 low-latency optimizations for tick/crawler workload
REDIS_8_LOW_LATENCY_CONFIG: Dict[str, Any] = {
    # Persistence (avoid per-write fsync stalls)
    "appendonly": True,
    "appendfsync": "everysec",
    "no-appendfsync-on-rewrite": True,
    "rdb-save-incremental-fsync": True,
    "save": "",  # Disable RDB saves if AOF is sufficient
    
    # Latency monitoring (8.x adds per-command percentiles)
    "latency-tracking": True,
    "latency-monitor-threshold": 100,  # Record spikes >=100ms
    
    # Networking / event loop
    "tcp-keepalive": 60,
    "tcp-backlog": 511,
    
    # I/O threads (helpful for read-heavy Pub/Sub / Streams)
    "io-threads": 4,  # min(CPU cores/2, 4)
    "io-threads-do-reads": True,  # Only reads (writes remain single-threaded)
    
    # Memory & defrag (avoid stop-the-world malloc)
    "maxmemory": "4gb",
    "maxmemory-policy": "allkeys-lru",
    "activedefrag": True,
    "active-defrag-ignore-bytes": 1048576,
    "active-defrag-threshold-lower": 10,
    "active-defrag-threshold-upper": 100,
    
    # Lazy frees (reduce big-key delete stalls)
    "lazyfree-lazy-eviction": True,
    "lazyfree-lazy-expire": True,
    "lazyfree-lazy-server-del": True,
    
    # Pub/Sub buffers (protect server under bursts)
    "client-output-buffer-limit": "pubsub 64mb 16mb 60",
    "client-output-buffer-limit-normal": "0 0 0",
    
    # Streams (prefer over LPUSH/LTRIM; cheap trimming)
    # ✅ Redis 8.2: Optimized for high-frequency tick data
    "stream-node-max-bytes": 4096,
    "stream-node-max-entries": 100,
    # Note: maxlen and approximate=True are set per-stream in code:
    # - ticks:intraday:processed: maxlen=10000, approximate=True
    # - patterns streams: maxlen=5000, approximate=True
    # - alerts:stream: maxlen=1000, approximate=True
    
    # Server loop
    "hz": 10,  # Default; avoid cranking up unless measured
}

# Optional global feature flags (e.g., client tracking, RedisJSON support)
REDIS_8_FEATURE_FLAGS: Dict[str, bool] = {
    "client_tracking": True,
    "use_json": False,  # set to True when RedisJSON module is loaded
}

# ---------------------------------------------------------------------------
# Database segmentation - IMPORTED FROM redis_config.py (single source of truth)
# ---------------------------------------------------------------------------

# Import REDIS_DATABASES from redis_config.py to avoid duplication
# DB 0: user_database (credentials, trades, order data)
# DB 1: market_data (historical + live ticks, OHLC, indicators, Greeks)
# DB 2: analytics_validation (pattern performance, validation results)
from shared_core.redis_clients.redis_config import REDIS_DATABASES


# ---------------------------------------------------------------------------
# Redis 8.0 configuration helper
# ---------------------------------------------------------------------------

class Redis8Config:
    """Unified Redis configuration with feature awareness."""

    def __init__(self, environment: Optional[str] = None):
        self.environment = environment or os.environ.get("ENVIRONMENT", "dev")
        self.base_config = REDIS_8_CONFIG.copy()
        self.global_features = REDIS_8_FEATURE_FLAGS.copy()

        # Environment tuning
        if self.environment == "prod":
            self.base_config.update({"max_connections": 50, "health_check_interval": 60})
        elif self.environment == "dev":
            self.base_config.update({"max_connections": 10, "health_check_interval": 10})

    def apply_low_latency_optimizations(self, redis_client) -> bool:
        """
        Apply Redis 8.2 low-latency optimizations for tick/crawler workload.
        
        Args:
            redis_client: Connected Redis client instance
            
        Returns:
            bool: True if optimizations applied successfully
        """
        try:
            import redis
            
            # Apply low-latency configurations
            for config_key, config_value in REDIS_8_LOW_LATENCY_CONFIG.items():
                try:
                    if isinstance(config_value, str) and config_value == "":
                        # Handle empty string values (like save "")
                        redis_client.config_set(config_key, "")
                    elif isinstance(config_value, bool):
                        # Convert boolean to string
                        redis_client.config_set(config_key, "yes" if config_value else "no")
                    else:
                        redis_client.config_set(config_key, config_value)
                except redis.exceptions.ResponseError as e:
                    # Some configs might not be available or require restart
                    print(f"⚠️  Config {config_key}={config_value} failed: {e}")
                    continue
            
            print("✅ Redis 8.2 low-latency optimizations applied successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Failed to apply Redis optimizations: {e}")
            return False

    def _resolve_db_number(self, db_name: str) -> int:
        for number, spec in REDIS_DATABASES.items():
            if spec["name"] == db_name:
                return number
        raise KeyError(f"Unknown Redis DB name: {db_name}")

    def get_db_config(self, db_name: str) -> Dict[str, Any]:
        """Return base connection kwargs for a named database."""
        config = self.base_config.copy()
        config["db"] = self._resolve_db_number(db_name)
        return config

    def get_db_features(self, db_name: str) -> Dict[str, bool]:
        """Return feature flags (client tracking / JSON) for a named database."""
        db_num = self._resolve_db_number(db_name)
        spec = REDIS_DATABASES[db_num]
        return {
            "client_tracking": bool(spec.get("client_tracking", False)),
            "use_json": bool(spec.get("use_json", False)),
        }

# ---------------------------------------------------------------------------
# Compatibility helpers (legacy API)
# ---------------------------------------------------------------------------

def get_redis_config(environment: Optional[str] = None) -> Dict[str, Any]:
    """
    Backwards-compatible accessor that returns the base connection configuration.
    (Note: client-tracking must be enabled explicitly by the caller.)
    """
    cfg = Redis8Config(environment)
    return cfg.base_config.copy()


def get_database_for_data_type(data_type: str) -> int:
    """Map a logical data type to its Redis database number."""
    for db_num, spec in REDIS_DATABASES.items():
        if data_type in spec.get("data_types", []):
            return db_num
    return 0


def get_ttl_for_data_type(data_type: str) -> int:
    """Return TTL (seconds) for a given data type."""
    for spec in REDIS_DATABASES.values():
        if data_type in spec.get("data_types", []):
            return spec.get("ttl", 3600)
    return 3600


def apply_redis_low_latency_optimizations(redis_client) -> bool:
    """
    Convenience function to apply Redis 8.2 low-latency optimizations.
    
    Args:
        redis_client: Connected Redis client instance
        
    Returns:
        bool: True if optimizations applied successfully
    """
    config = Redis8Config()
    return config.apply_low_latency_optimizations(redis_client)


# ---------------------------------------------------------------------------
# Connection Pool Configuration - Process-Specific Pools
# ---------------------------------------------------------------------------
# Process Pool Configuration - IMPORTED FROM redis_config.py (single source of truth)
# ---------------------------------------------------------------------------

from shared_core.redis_clients.redis_config import PROCESS_POOL_CONFIG, REDIS_STREAM_MAXLEN_CONFIG, REDIS_8_CONFIG_STRING

# ⚠️ DEPRECATED FUNCTIONS REMOVED:
# - create_process_specific_pool() -> Use UnifiedRedisManager.get_client()
# - get_redis_connection_pool() -> Use UnifiedRedisManager.get_client()
# - get_redis_client_from_pool() -> Use UnifiedRedisManager.get_client()
#
# Migration guide:
# - Old: get_redis_client_from_pool(db=1)
# - New: UnifiedRedisManager.get_client(process_name="your_process", db=1)


# ---------------------------------------------------------------------------
# Redis 8.2 Optimization Settings
# ---------------------------------------------------------------------------

# ✅ RedisStorage batching configuration (migrated from Dragonfly)
# Used by RedisStorage.queue_tick_for_storage() for optimized tick storage
REDIS_STORAGE_BATCH_CONFIG: Dict[str, Any] = {
    "max_batch_size": 50,        # Flush after 50 ticks
    "flush_interval": 0.1,       # Or flush every 100ms
    "tick_ttl": 300,             # 5 minutes TTL for tick data
    "latest_tick_ttl": 60,       # 1 minute TTL for latest tick metadata
}

# ---------------------------------------------------------------------------
# Stream Max Length Configuration - IMPORTED FROM redis_config.py (single source of truth)
# ---------------------------------------------------------------------------

DEFAULT_STRING_REDIS_CONFIG: Dict[str, Any] = REDIS_8_CONFIG_STRING.copy()

# ✅ Redis 8.2 hset mapping optimization
# All hash operations use hset with mapping parameter for batch updates
# Example: pipe.hset(f"ticks:{symbol}", mapping=tick_hash_data)
# This reduces round-trips from N operations to 1 operation per hash
REDIS_HSET_MAPPING_ENABLED: bool = True  # Always enabled for Redis 8.2


class RedisConfig(Redis8Config):
    """
    Legacy wrapper retained for import compatibility.
    Exposes a ``databases`` property (name -> db number) matching prior usage.
    """

    def __init__(self, environment: Optional[str] = None):
        super().__init__(environment)
        self.databases = {spec["name"]: db for db, spec in REDIS_DATABASES.items()}


# ---------------------------------------------------------------------------
# AION analytics channels (replaces legacy DeepSeek exports)
# ---------------------------------------------------------------------------

AION_CHANNELS: Dict[str, str] = {
    "correlations": "aion.correlations",
    "divergences": "aion.divergences",
    "validations": "aion.validated",
    "alerts": "aion.alerts",
    "monte_carlo": "aion.monte_carlo",
    "granger": "aion.granger",
    "dbscan": "aion.dbscan",
}


class RobustRedisClient:
    """Consolidated Redis client with circuit breaker"""

    def __init__(
        self,
        host="localhost",
        port=6379,
        db=0,
        password=None,
        decode_responses=True,
        max_retries=5,
        retry_delay=1.0,
        health_check_interval=30,
        config=None,
        circuit_breaker_enabled=True,
    ):
        # Allow config override
        if config:
            host = config.get("redis_host", host)
            port = config.get("redis_port", port)
            db = config.get("redis_db", db)
            password = config.get("redis_password", password)
            circuit_breaker_enabled = config.get(
                "circuit_breaker_enabled", circuit_breaker_enabled
            )

        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.decode_responses = decode_responses
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.health_check_interval = health_check_interval
        
        # Get max_connections from config, default to 50 (reduced to prevent connection exhaustion)
        # Priority: config dict > centralized redis_config > default
        self.max_connections = 50
        if config:
            # Check for both redis_max_connections and max_connections keys
            self.max_connections = int(config.get("redis_max_connections") or config.get("max_connections", self.max_connections))
        else:
            # Try to get from centralized config if no config provided
            try:
                # Use local get_redis_config() function (consolidated from redis_config.py)
                redis_config = get_redis_config()
                if "max_connections" in redis_config:
                    self.max_connections = int(redis_config["max_connections"])
            except Exception:
                pass  # Use default if config unavailable

        # ✅ FIXED: Separate Redis clients per database
        self.clients = {}
        self.connection_wrappers: Dict[int, Optional[Any]] = {}  # Not using ConnectionPooledRedisClient anymore (Any is a placeholder)
        self.pubsub = None
        self.is_connected = False
        self.connection_lock = Lock()
        self.last_health_check = 0
        
        # Initialize redis_pool to None to prevent cleanup errors
        self.redis_pool = None
        self.redis_client = None
        
        # Initialize clients for all databases
        self._initialize_clients()

        # Circuit breaker integration
        self.circuit_breaker_enabled = (
            circuit_breaker_enabled and CIRCUIT_BREAKER_AVAILABLE
        )
        if self.circuit_breaker_enabled:
            self.circuit_breaker = redis_circuit_breaker()
            logger.info("Circuit breaker enabled for Redis operations")
        else:
            self.circuit_breaker = None
            if circuit_breaker_enabled and not CIRCUIT_BREAKER_AVAILABLE:
                logger.warning("Circuit breaker requested but not available")

        # Fallback storage (when Redis is down)
        self.fallback_data = {}
        self.fallback_expirations = {}
        self.fallback_lock = Lock()

        # Queue for failed operations
        self.retry_queue = deque(maxlen=1000)  # Max 1000 pending operations

        # Stats
        self.stats = {
            "reconnections": 0,
            "failed_operations": 0,
            "queued_operations": 0,
            "successful_operations": 0,
        }

        # Add cumulative data tracker for session-based storage
        self.cumulative_tracker = CumulativeDataTracker(self)
        self._fallback_session = datetime.now().strftime("%Y-%m-%d")

        # Initial connection
        logger.info("Initializing Redis connection...")
        self._connect()
    
    # REMOVED: _init_redis_calculations() - was initializing unused RedisCalculations
    # Purpose: Was intended to provide in-server calculations via Lua for performance
    # Status: Never actually used - HybridCalculations handles all calculations
    # Impact: Removed to eliminate dead code and initialization overhead

        # REMOVED: _init_redis_calculations() - was initializing unused RedisCalculations
    # Purpose: Was intended to provide in-server calculations via Lua for performance
    # Status: Never actually used - HybridCalculations handles all calculations
    # Impact: Removed to eliminate dead code and initialization overhead

    @staticmethod
    def _normalize_legacy_key(key: str) -> str:
        """Normalize legacy key patterns using DatabaseAwareKeyBuilder."""
        if not isinstance(key, str):
            return key
        
        if key.startswith("volume_state:"):
            _, token = key.split(":", 1)
            return DatabaseAwareKeyBuilder.live_volume_state(token)
        
        if key.startswith(("volume_averages:", "baseline:")):
            prefix, _, symbol = key.partition(":")
            if symbol:
                return DatabaseAwareKeyBuilder.live_volume_baseline(symbol)
            return key
        
        if key.startswith("volume_profile:"):
            parts = key.split(":", 2)
            if len(parts) >= 2:
                symbol = parts[1]
                profile_meta = parts[2] if len(parts) == 3 else ""
                meta_parts = profile_meta.split(":") if profile_meta else []
                profile_type = meta_parts[0] if meta_parts else "default"
                profile_date = ":".join(meta_parts[1:]) if len(meta_parts) > 1 else None
                return DatabaseAwareKeyBuilder.live_volume_profile(symbol, profile_type, profile_date or None)
            return key
        
        if key.startswith("straddle_volume:"):
            _, remainder = key.split(":", 1)
            underlying, _, date = remainder.partition(":")
            if underlying and date:
                return DatabaseAwareKeyBuilder.live_volume_straddle(underlying, date)
            return key
        
        if key.startswith("indicators:"):
            parts = key.split(":", 3)
            if len(parts) >= 3:
                symbol = parts[1]
                indicator = parts[2]
                category = RedisKeyStandards.categorize_indicator(indicator)
                return DatabaseAwareKeyBuilder.live_indicator(symbol, indicator, category)
            return key
        
        if key.startswith("realtime:"):
            _, symbol = key.split(":", 1)
            if symbol:
                return DatabaseAwareKeyBuilder.live_ticks_stream(symbol)
            return key
        
        return key

    def _transform_key(self, key: str) -> str:
        """Transform old key patterns to new unified structure"""
        return self._normalize_legacy_key(key)

    def _categorize_indicator(self, indicator: str) -> str:
        """Categorize indicators using RedisKeyStandards single source of truth."""
        try:
            return RedisKeyStandards.categorize_indicator(indicator)
        except AttributeError:
            indicator_lower = (indicator or "").lower()
            if 'volume' in indicator_lower:
                return 'volume'
            return 'custom'

    @property
    def current_session(self) -> str:
        if getattr(self, "cumulative_tracker", None):
            return self.cumulative_tracker.current_session
        return getattr(self, "_fallback_session", datetime.now().strftime("%Y-%m-%d"))

    @current_session.setter
    def current_session(self, value: str) -> None:
        if getattr(self, "cumulative_tracker", None):
            self.cumulative_tracker.current_session = value
        self._fallback_session = value

    @property
    def redis(self):
        """Backward compatibility property exposing the underlying redis client"""
        return self.redis_client

    def _initialize_clients(self):
        """Initialize separate Redis clients for each database using process-specific pools"""
        try:
            # Detect process name for process-specific pools
            process_name = "redis_client"
            logger.info(f"📡 Initializing Redis clients for process: {process_name}")
            
            # Initialize clients for all databases defined in REDIS_DATABASES
            for db_num in REDIS_DATABASES.keys():
                try:
                    # Use UnifiedRedisManager.get_client() for process-specific pools
                    client = UnifiedRedisManager.get_client(
                        process_name=process_name,
                        db=db_num,
                        host=REDIS_8_CONFIG.get("host", "localhost"),
                        port=REDIS_8_CONFIG.get("port", 6379),
                        password=REDIS_8_CONFIG.get("password"),
                        decode_responses=REDIS_8_CONFIG.get("decode_responses", True)
                    )
                    # Test connection
                    client.ping()
                    self.connection_wrappers[db_num] = None  # Not using wrapper anymore
                    self.clients[db_num] = client
                    logger.info(
                        f"✅ Redis client initialized for DB {db_num} using process-specific pool ({process_name})"
                    )
                except Exception as e:
                    logger.warning(f"⚠️ Failed to initialize client for DB {db_num}: {e}")
                    self.connection_wrappers[db_num] = None
                    self.clients[db_num] = None

            # Set primary client (DB 1 is the default for unified structure)
            self.redis_client = self.clients.get(1) or self.clients.get(0) or next(
                (client for client in self.clients.values() if client is not None),
                None,
            )

            if self.redis_client:
                self.pubsub = self.redis_client.pubsub()
                self.is_connected = True
                logger.info("✅ Redis clients initialized successfully")
            else:
                logger.error("❌ No Redis clients could be initialized")

        except Exception as e:
            logger.error(f"❌ Failed to initialize Redis clients: {e}")

    # ============ Database Segmentation Methods ============

    def get_client(self, db_num=0):
        """Get Redis client for specific database"""
        return self.clients.get(db_num, self.clients.get(0))

    def _get_function_client(self):
        """Return primary client suitable for Redis function execution."""
        client = self.redis_client or self.get_client(0)
        if client is None:
            raise RuntimeError("No Redis client available for Redis functions")
        return client

    def fcall(self, function_name, num_keys, *keys_and_args):
        """Proxy Redis FUNCTION CALL to the primary client."""
        client = self._get_function_client()
        return client.fcall(function_name, num_keys, *keys_and_args)

    def function_load(self, script, replace=False):
        """Proxy FUNCTION LOAD to the primary client."""
        client = self._get_function_client()
        if replace:
            return client.function_load(script, replace=True)
        return client.function_load(script)

    def publish_to_stream(self, stream_name, data, maxlen=10000):
        """
        Publish to Redis Stream instead of simple SET.
        
        Storage Location:
        - Database: DB 1 (realtime) - via get_database_for_data_type("stream_data")
        - Stream Key: stream_name (as provided)
        
        Data Format/Signature:
        - Input: data (dict, list, str, bytes, or other serializable type)
        - Stored Format: Redis Stream entry with fields:
          {
            "data": <JSON string of input data>,
            "timestamp": <current timestamp in milliseconds>
          }
        - Serialization: dict/list are JSON-encoded using SafeJSONEncoder
        - Other types are converted to string
        
        Args:
            stream_name: Redis stream key name
            data: Data to publish (will be JSON-serialized if dict/list)
            maxlen: Maximum stream length before trimming (default: 10000)
            
        Returns:
            Stream entry ID (string) on success, False on failure
        """
        try:
            # Get the appropriate client for the stream
            db_num = self.get_database_for_data_type("stream_data")
            client = self.get_client(db_num)
            if not client:
                logger.error(f"No client available for stream {stream_name}")
                return False
            
            # Serialize data if needed
            if isinstance(data, (dict, list)):
                data = json.dumps(data, cls=SafeJSONEncoder)
            elif not isinstance(data, (str, bytes)):
                data = str(data)
            
            # Add to stream with memory-efficient settings
            result = client.xadd(
                stream_name,
                {"data": data, "timestamp": int(time.time() * 1000)},
                maxlen=maxlen,
                approximate=True  # Memory efficient
            )
            self.stats["successful_operations"] += 1
            return result
            
        except Exception as e:
            logger.error(f"Failed to publish to stream {stream_name}: {e}")
            self.stats["failed_operations"] += 1
            return False

    def produce_ticks_batch_sync(
        self, 
        ticks: List[Dict[str, Any]],
        stream_name: str = "ticks:intraday:processed",
        maxlen: int = 10000,
        approximate: bool = True
    ) -> Dict[str, Any]:
        """
        High-throughput tick production with batching (storage only, no calculations).
        Uses pipelines for efficient batch writes to Redis streams.
        
        ✅ STORAGE ONLY: This method only stores ticks, no calculations are performed.
        Calculations should be done in calculations.py via HybridCalculations.
        
        Storage Location:
        - Database: DB 1 (realtime) - via get_database_for_data_type("stream_data")
        - Stream Keys:
          * Single symbol: "ticks:intraday:{symbol}" (e.g., "ticks:intraday:NSE:RELIANCE")
          * Multiple symbols: stream_name (default: "ticks:intraday:processed")
        
        Data Format/Signature:
        - Input: List of tick dictionaries with fields:
          {
            "symbol": str,                    # Required: Trading symbol
            "price" or "last_price": float,   # Price field (prefers "price", falls back to "last_price")
            "volume" or "traded_quantity" or "bucket_incremental_volume": int,  # Volume field
            "timestamp" or "exchange_timestamp_ms": int,  # Timestamp in milliseconds
            "high": float (optional),          # High price
            "low": float (optional),           # Low price
            "open": float (optional),          # Open price
            "close": float (optional),        # Close price
            "instrument_token": int (optional), # Instrument token
            "bucket_incremental_volume": int (optional),  # Incremental volume
            "bucket_cumulative_volume": int (optional)   # Cumulative volume
          }
        - Stored Format: Redis Stream entry with fields (all values converted to strings):
          {
            "symbol": "<symbol>",
            "price": "<price>",
            "volume": "<volume>",
            "timestamp": "<timestamp_ms>",
            ... (all other fields from input tick, converted to strings)
          }
        - Field Mapping: All tick fields are preserved and converted to strings for Redis streams
        
        Args:
            ticks: List of tick dictionaries (see Data Format above)
            stream_name: Stream name (defaults to "ticks:intraday:processed")
            maxlen: Maximum stream length (default: 10000)
            approximate: Use approximate trimming (default: True, faster)
            
        Returns:
            Dict with ticks_produced, batch_duration_ms, throughput_tps, symbols
        """
        import time
        start_time = time.perf_counter()
        
        # Get the appropriate client for streams (DB 1 for realtime)
        db_num = self.get_database_for_data_type("stream_data")
        client = self.get_client(db_num)
        if not client:
            logger.error(f"No client available for stream {stream_name}")
            return {
                'ticks_produced': 0,
                'batch_duration_ms': 0,
                'throughput_tps': 0,
                'symbols': []
            }
        
        # Group ticks by symbol for efficient streaming
        ticks_by_symbol = {}
        for tick in ticks:
            symbol = tick.get('symbol', 'UNKNOWN')
            if symbol not in ticks_by_symbol:
                ticks_by_symbol[symbol] = []
            ticks_by_symbol[symbol].append(tick)
        
        # Use pipelines for each symbol (batched writes)
        total_written = 0
        for symbol, symbol_ticks in ticks_by_symbol.items():
            # ✅ FIXED: Use DatabaseAwareKeyBuilder for consistent key generation
            if len(ticks_by_symbol) == 1:
                stream_key = DatabaseAwareKeyBuilder.live_ticks_stream(symbol)
            else:
                stream_key = stream_name
            
            # Batch writes using pipeline
            with client.pipeline(transaction=False) as pipe:
                for tick in symbol_ticks:
                    fields = {
                        'symbol': str(tick.get('symbol', 'UNKNOWN')),
                        'price': str(tick.get('price', tick.get('last_price', 0))),
                        'volume': str(tick.get('volume', tick.get('traded_quantity', tick.get('bucket_incremental_volume', 0)))),
                        'timestamp': str(tick.get('timestamp', tick.get('exchange_timestamp_ms', time.time() * 1000)))
                    }
                    # Add optional fields
                    if 'high' in tick:
                        fields['high'] = str(tick['high'])
                    if 'low' in tick:
                        fields['low'] = str(tick['low'])
                    if 'open' in tick:
                        fields['open'] = str(tick['open'])
                    if 'close' in tick:
                        fields['close'] = str(tick['close'])
                    if 'instrument_token' in tick:
                        fields['instrument_token'] = str(tick['instrument_token'])
                    if 'bucket_incremental_volume' in tick:
                        fields['bucket_incremental_volume'] = str(tick['bucket_incremental_volume'])
                    if 'bucket_cumulative_volume' in tick:
                        fields['bucket_cumulative_volume'] = str(tick['bucket_cumulative_volume'])
                    
                    pipe.xadd(
                        stream_key,
                        fields,
                        maxlen=maxlen,
                        approximate=approximate
                    )
                
                # Execute pipeline (all writes in one round-trip)
                results = pipe.execute()
                total_written += len([r for r in results if r])
        
        duration = time.perf_counter() - start_time
        return {
            'ticks_produced': total_written,
            'batch_duration_ms': duration * 1000,
            'throughput_tps': total_written / duration if duration > 0 else 0,
            'symbols': list(ticks_by_symbol.keys())
        }

    def read_from_stream(self, stream_name, count=100, start_id="0"):
        """
        Read from Redis Stream.
        
        Storage Location:
        - Database: DB 1 (realtime) - via get_database_for_data_type("stream_data")
        - Stream Key: stream_name (as provided)
        
        Data Format/Signature:
        - Returns: List of tuples (message_id, fields_dict)
        - Each message contains fields as stored (string values)
        - For publish_to_stream(): fields = {"data": "<JSON string>", "timestamp": "<ms>"}
        - For produce_ticks_batch_sync(): fields = {"symbol": "...", "price": "...", "volume": "...", ...}
        
        Args:
            stream_name: Redis stream key name
            count: Maximum number of messages to read (default: 100)
            start_id: Starting message ID (default: "0" for beginning)
            
        Returns:
            List of (message_id, fields_dict) tuples, or empty list on error
        """
        try:
            db_num = self.get_database_for_data_type("stream_data")
            client = self.get_client(db_num)
            if not client:
                logger.error(f"No client available for stream {stream_name}")
                return []
            
            # Read from stream
            messages = client.xread({stream_name: start_id}, count=count, block=0)
            return messages
            
        except Exception as e:
            logger.error(f"Failed to read from stream {stream_name}: {e}")
            return []

    def switch_database(self, db_num):
        """Switch to specific Redis database safely.

        Uses the SELECT command on the active connection when possible. If that
        fails (e.g., due to connection pool semantics), rebuilds the connection
        pool with the requested DB.
        """
        if not isinstance(db_num, int) or db_num < 0:
            logger.error(f"Invalid database number: {db_num}")
            return False

        # Fast path: try SELECT on current connection
        if self.is_connected and self.redis_client:
            try:
                # Execute raw SELECT command (redis-py does not expose .select())
                self.redis_client.execute_command("SELECT", db_num)
                self.db = db_num
                return True
            except Exception as e:
                logger.warning(f"Direct SELECT failed for DB {db_num}, rebuilding pool: {e}")

        # Rebuild connection pool bound to the requested DB
        try:
            with self.connection_lock:
                # Use UnifiedRedisManager.get_client() to get a client for the requested DB
                client = UnifiedRedisManager.get_client(
                    process_name="redis_client",
                    db=db_num,
                    host=REDIS_8_CONFIG.get("host", "localhost"),
                    port=REDIS_8_CONFIG.get("port", 6379),
                    password=REDIS_8_CONFIG.get("password"),
                    decode_responses=REDIS_8_CONFIG.get("decode_responses", True)
                )
                client.ping()
                self.redis_client = client
                self.connection_wrappers[db_num] = None  # Not using wrapper anymore
                self.clients[db_num] = client
                self.db = db_num
                self.is_connected = True
                self.pubsub = self.redis_client.pubsub()
                return True
        except Exception as e:
            logger.error(f"Failed to switch to database {db_num}: {e}")
            return False

    def store_by_data_type(self, data_type, key, value, ttl=None):
        """Store data in appropriate database based on type using separate clients or streams"""
        db_num = self.get_database_for_data_type(data_type)
        if ttl is None:
            ttl = self.get_ttl_for_data_type(data_type)

        # Check if this data type should use streams
        if data_type in ["stream_data", "ticks_stream", "alerts_stream", "patterns_stream"]:
            # Use Redis Streams for real-time data
            stream_name = f"stream:{data_type}"
            return self.publish_to_stream(stream_name, value, maxlen=10000)
        
        # Use traditional key-value storage for other data types
        client = self.get_client(db_num)
        if not client:
            logger.error(f"No client available for database {db_num}")
            return False

        try:
            # Serialize value if it's a dictionary or other complex type
            if isinstance(value, (dict, list)):
                # Handle dictionaries that may contain binary data
                if isinstance(value, dict):
                    # Convert binary data to base64 for JSON serialization
                    serializable_value = {}
                    for k, v in value.items():
                        if isinstance(v, bytes):
                            import base64
                            serializable_value[k] = base64.b64encode(v).decode("utf-8")
                        else:
                            serializable_value[k] = v
                    value = json.dumps(serializable_value, cls=SafeJSONEncoder)
                else:
                    value = json.dumps(value, cls=SafeJSONEncoder)
            elif not isinstance(value, (bytes, str, int, float)):
                value = str(value)

            # Store using the appropriate client with expiration
            if ttl and ttl > 0:
                result = client.setex(key, ttl, value)
            else:
                result = client.set(key, value)
            self.stats["successful_operations"] += 1
            return result
            
        except Exception as e:
            logger.error(f"Failed to store in database {db_num}: {e}")
            self.stats["failed_operations"] += 1
            return False

    def batch_store_by_data_type(self, operations: List[Tuple[str, str, Any, Optional[int]]]) -> Dict[str, bool]:
        """
        Batch store multiple operations using Redis pipeline for optimal performance.
        
        This is optimized for Redis 8.2 and uses connection pooling. All operations
        are executed in a single round trip to Redis.
        
        Args:
            operations: List of tuples (data_type, key, value, ttl)
                - data_type: Type of data (determines database)
                - key: Redis key
                - value: Value to store (dict/list will be JSON serialized)
                - ttl: Optional TTL in seconds (defaults to data_type TTL if None)
        
        Returns:
            Dict mapping key to success status
        
        Example:
            operations = [
                ("analysis_cache", "indicators:SYMBOL:rsi", "45.5", 3600),
                ("analysis_cache", "indicators:SYMBOL:atr", "2.3", 3600),
            ]
            results = redis_client.batch_store_by_data_type(operations)
        """
        if not operations:
            return {}
        
        # Group operations by database number
        db_operations: Dict[int, List[Tuple[str, Any, Optional[int]]]] = {}
        key_to_result = {}
        
        for data_type, key, value, ttl in operations:
            db_num = self.get_database_for_data_type(data_type)
            if ttl is None:
                ttl = self.get_ttl_for_data_type(data_type)
            
            if db_num not in db_operations:
                db_operations[db_num] = []
            
            # Serialize value
            serialized_value = value
            if isinstance(value, (dict, list)):
                if isinstance(value, dict):
                    serializable_value = {}
                    for k, v in value.items():
                        if isinstance(v, bytes):
                            import base64
                            serializable_value[k] = base64.b64encode(v).decode("utf-8")
                        else:
                            serializable_value[k] = v
                    serialized_value = json.dumps(serializable_value, cls=SafeJSONEncoder)
                else:
                    serialized_value = json.dumps(value, cls=SafeJSONEncoder)
            elif not isinstance(serialized_value, (bytes, str, int, float)):
                serialized_value = str(value)
            
            db_operations[db_num].append((key, serialized_value, ttl))
            key_to_result[key] = False  # Initialize as failed
        
        # Execute pipeline for each database
        for db_num, ops in db_operations.items():
            client = self.get_client(db_num)
            if not client:
                logger.error(f"No client available for database {db_num}")
                continue
            
            try:
                # Create pipeline for this database
                pipe = client.pipeline(transaction=False)
                
                # Add all operations to pipeline
                for key, value, ttl in ops:
                    if ttl and ttl > 0:
                        pipe.setex(key, ttl, value)
                    else:
                        pipe.set(key, value)
                
                # Execute pipeline (all operations in one round trip)
                results = pipe.execute()
                
                # Update success status
                for i, key in enumerate([op[0] for op in ops]):
                    key_to_result[key] = bool(results[i] if i < len(results) else False)
                
                self.stats["successful_operations"] += sum(1 for r in results if r)
                
            except Exception as e:
                logger.error(f"Pipeline execution failed for database {db_num}: {e}")
                self.stats["failed_operations"] += len(ops)
                # Fallback to individual operations
                for key, value, ttl in ops:
                    try:
                        if ttl and ttl > 0:
                            result = client.setex(key, ttl, value)
                        else:
                            result = client.set(key, value)
                        key_to_result[key] = bool(result)
                    except Exception as fallback_error:
                        logger.warning(f"Fallback operation failed for key {key}: {fallback_error}")
        
        return key_to_result

    def retrieve_by_data_type(self, key, data_type):
        """Retrieve data from appropriate database based on type using separate clients"""
        db_num = self.get_database_for_data_type(data_type)
        
        # Get the appropriate client for this database
        client = self.get_client(db_num)
        if not client:
            logger.error(f"No client available for database {db_num}")
            return None

        try:
            result = client.get(key)
            self.stats["successful_operations"] += 1
            return result
        except Exception as e:
            logger.error(f"Failed to retrieve from database {db_num}: {e}")
            self.stats["failed_operations"] += 1
            return None

    # Compatibility shim for legacy callers (e.g., alert_validator)
    def select(self, db_num):  # noqa: D401
        """Alias for switch_database for compatibility with redis-py `.select()` usage."""
        return self.switch_database(db_num)

    # ============ Stream Operations (XADD) ============
    def xadd(self, name, fields, id="*", maxlen=None, approximate=True):
        """
        Add entry to a Redis Stream with auto-reconnection and fallback queue.
        
        Storage Location:
        - Database: DB 0 (system) - uses primary redis_client
        - Stream Key: name (as provided)
        
        Data Format/Signature:
        - Input: fields (dict) with field->value pairs
        - Value Types: str, bytes, int, float (or will be JSON-encoded/stringified)
        - Stored Format: Redis Stream entry with fields as-is (values converted to strings if needed)
        - Field names and values are preserved exactly as provided
        
        Args:
            name: Redis stream key name
            fields: Dictionary of field->value pairs (values: str/bytes/int/float)
            id: Message ID (default: "*" for auto-generated)
            maxlen: Maximum stream length before trimming (optional)
            approximate: Use approximate trimming if maxlen set (default: True)
            
        Returns:
            Stream entry ID (string) on success, None on failure (queued for retry)
        """
        self._ensure_connection()
        if self.is_connected and self.redis_client:
            try:
                # Ensure all values are strings/bytes
                safe_fields = {}
                for k, v in (fields or {}).items():
                    if isinstance(v, (bytes, str, int, float)):
                        safe_fields[k] = v
                    else:
                        try:
                            safe_fields[k] = json.dumps(v, cls=SafeJSONEncoder)
                        except Exception:
                            safe_fields[k] = str(v)

                return self.redis_client.xadd(
                    name, safe_fields, id=id, maxlen=maxlen, approximate=approximate
                )
            except Exception as e:
                self.is_connected = False
                # Queue operation for retry
                self._queue_operation(
                    "xadd", name, fields, id=id, maxlen=maxlen, approximate=approximate
                )
                return None
        # No connected Redis; queue for later
        self._queue_operation(
            "xadd", name, fields, id=id, maxlen=maxlen, approximate=approximate
        )
        return None

    # ============ Specialized Methods for Each Segment ============

    def store_asset_price(self, symbol, price_data, asset_class="equity_cash"):
        """Store asset last_price in DB 1 (realtime) via store_by_data_type routing"""
        key = f"last_price:{asset_class}:{symbol}"
        # Map asset_class to correct data type
        data_type_map = {
            "equity_cash": "equity_cash_prices",
            "futures": "futures_prices", 
            "options": "options_prices",
            "commodity": "commodity_prices"
        }
        data_type = data_type_map.get(asset_class, "equity_cash_prices")
        return self.store_by_data_type(data_type, key, json.dumps(price_data))

    def store_premarket_volume(self, symbol, bucket_incremental_volume):
        """Store premarket bucket_incremental_volume in DB 1 (realtime) via store_by_data_type routing - Critical for 9:00-9:30 AM manipulation detection"""
        # Resolve token to symbol BEFORE storing
        # ✅ Delegate to cumulative_tracker if available
        if hasattr(self, 'cumulative_tracker') and self.cumulative_tracker:
            symbol = self.cumulative_tracker._resolve_token_to_symbol_for_storage(symbol)
        else:
            symbol_str = str(symbol) if symbol else ""
            if not symbol_str or symbol_str.startswith("UNKNOWN_") or (symbol_str.isdigit() and ":" not in symbol_str):
                symbol = None
        
        # ✅ FIX: Guard against None/UNKNOWN symbols to prevent invalid keys
        if not symbol or (isinstance(symbol, str) and symbol.startswith("UNKNOWN_")):
            logger.warning(f"⚠️ Skipping premarket volume storage - symbol resolution failed")
            return False
        
        key = f"premarket:{symbol}:{int(time.time())}"
        return self.store_by_data_type("premarket_trades", key, json.dumps(bucket_incremental_volume))

    # Order flow methods removed

    def _normalize_timestamp(self, value):
        """Normalize different timestamp formats to float seconds."""
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, datetime):
            return value.timestamp()

        if isinstance(value, str):
            stripped = value.strip()
            # Support direct float strings
            try:
                return float(stripped)
            except ValueError:
                pass

            # Attempt HH:MM[:SS] parsing relative to today (system timezone)
            time_formats = ["%H:%M:%S", "%H:%M"]
            for fmt in time_formats:
                try:
                    time_obj = datetime.strptime(stripped, fmt).time()
                    combined = datetime.combine(date.today(), time_obj)
                    return combined.timestamp()
                except ValueError:
                    continue

        return None

    def store_continuous_data(self, symbol, market_data):
        """Store 5s continuous data in DB 1 (realtime) via store_by_data_type routing"""
        # Resolve token to symbol BEFORE storing
        # ✅ Delegate to cumulative_tracker if available
        if hasattr(self, 'cumulative_tracker') and self.cumulative_tracker:
            symbol = self.cumulative_tracker._resolve_token_to_symbol_for_storage(symbol)
        else:
            symbol_str = str(symbol) if symbol else ""
            if not symbol_str or symbol_str.startswith("UNKNOWN_") or (symbol_str.isdigit() and ":" not in symbol_str):
                symbol = None
        key = f"continuous:{symbol}:{int(time.time())}"
        return self.store_by_data_type("5s_ticks", key, json.dumps(market_data))

    def store_ticks_optimized(self, symbol, ticks):
        """Normalize tick payloads, store them, and update bucket_incremental_volume buckets."""
        if not symbol or not ticks:
            return False
        
        # Resolve token to symbol BEFORE storing
        # CRITICAL: This will return None if resolution fails (prevents UNKNOWN keys)
        # ✅ Delegate to cumulative_tracker if available, otherwise use simple fallback
        if hasattr(self, 'cumulative_tracker') and self.cumulative_tracker:
            symbol = self.cumulative_tracker._resolve_token_to_symbol_for_storage(symbol)
        else:
            # Simple fallback: if symbol already contains ':', it's already resolved
            symbol_str = str(symbol) if symbol else ""
            if not symbol_str or symbol_str.startswith("UNKNOWN_"):
                symbol = None
            elif ":" not in symbol_str and (symbol_str.isdigit() or symbol_str.startswith("TOKEN_")):
                # Looks like a token, but no resolver available - skip storage
                logger.warning(f"⚠️ Cannot resolve token {symbol_str} - no cumulative_tracker available")
                symbol = None
        
        # If resolution failed, skip storage to prevent UNKNOWN keys in Redis
        if not symbol or symbol.startswith("UNKNOWN_"):
            logger.warning(f"⚠️ Skipping ticks storage - symbol resolution failed for input symbol")
            return False

        optimized_ticks = []
        for tick in ticks:
            exchange_timestamp_raw = tick.get("exchange_timestamp")
            exchange_timestamp_epoch = (
                TimestampNormalizer.to_epoch_ms(exchange_timestamp_raw)
                if exchange_timestamp_raw
                else None
            )

            timestamp_ns_raw = tick.get("timestamp_ns")
            timestamp_ns_epoch = (
                TimestampNormalizer.to_epoch_ms(timestamp_ns_raw)
                if timestamp_ns_raw
                else None
            )

            timestamp_raw = tick.get("timestamp")
            timestamp_epoch = (
                TimestampNormalizer.to_epoch_ms(timestamp_raw) if timestamp_raw else None
            )

            optimized_tick = {
                "symbol": symbol,
                "last_price": tick.get("last_price"),
                "bucket_incremental_volume": tick.get("bucket_incremental_volume"),
                "exchange_timestamp": exchange_timestamp_raw or "",
                "exchange_timestamp_ms": exchange_timestamp_epoch or 0,
                "timestamp_ns": timestamp_ns_raw or "",
                "timestamp_ns_ms": timestamp_ns_epoch or 0,
                "timestamp": timestamp_raw or "",
                "timestamp_ms": timestamp_epoch or 0,
            }

            bucket_incremental_volume = tick.get("bucket_incremental_volume")
            if bucket_incremental_volume is None:
                bucket_incremental_volume = tick.get("bucket_incremental_volume")
            if bucket_incremental_volume is None:
                bucket_incremental_volume = 0

            bucket_cumulative_volume = tick.get("bucket_cumulative_volume")
            if bucket_cumulative_volume is None:
                bucket_cumulative_volume = tick.get("zerodha_cumulative_volume")
            if bucket_cumulative_volume is None:
                bucket_cumulative_volume = tick.get("bucket_incremental_volume")

            optimized_tick["bucket_incremental_volume"] = bucket_incremental_volume
            if bucket_cumulative_volume is not None:
                optimized_tick["bucket_cumulative_volume"] = bucket_cumulative_volume
                if "zerodha_cumulative_volume" not in tick:
                    optimized_tick["zerodha_cumulative_volume"] = bucket_cumulative_volume
            if "zerodha_last_traded_quantity" in tick:
                optimized_tick["zerodha_last_traded_quantity"] = tick.get(
                    "zerodha_last_traded_quantity"
                )

            for field in ("bid", "ask", "open", "high", "low"):
                if field in tick:
                    optimized_tick[field] = tick[field]

            optimized_ticks.append(optimized_tick)

            bucket_timestamp_candidate = None
            for candidate in (
                exchange_timestamp_epoch,
                tick.get("timestamp_epoch"),
                timestamp_epoch,
                timestamp_ns_epoch,
                tick.get("exchange_timestamp_epoch"),
                exchange_timestamp_raw,
                timestamp_ns_raw,
                timestamp_raw,
            ):
                if candidate is not None:
                    bucket_timestamp_candidate = candidate
                    break

            last_price = tick.get("last_price")
            try:
                # symbol is already resolved above, pass it as-is
                self.update_volume_buckets(
                    symbol=symbol,
                    bucket_incremental_volume=bucket_incremental_volume,
                    timestamp=bucket_timestamp_candidate or time.time(),
                    bucket_cumulative_volume=bucket_cumulative_volume,
                    last_price=last_price,
                )
            except Exception as bucket_err:
                logger.warning(
                    "Failed to update bucket_incremental_volume buckets from store_ticks_optimized for %s: %s",
                    symbol,
                    bucket_err,
                )

        key = f"ticks_optimized:{symbol}"
        return self.store_by_data_type(
            "ticks_optimized", key, json.dumps(optimized_ticks, cls=SafeJSONEncoder)
        )

    def store_cumulative_volume(self, symbol, bucket_incremental_volume):
        """Store cumulative bucket_incremental_volume in DB 2 (analytics) via store_by_data_type routing"""
        # Resolve token to symbol BEFORE storing
        # ✅ Delegate to cumulative_tracker if available
        if hasattr(self, 'cumulative_tracker') and self.cumulative_tracker:
            symbol = self.cumulative_tracker._resolve_token_to_symbol_for_storage(symbol)
        else:
            symbol_str = str(symbol) if symbol else ""
            if not symbol_str or symbol_str.startswith("UNKNOWN_") or (symbol_str.isdigit() and ":" not in symbol_str):
                symbol = None
        key = f"cumulative:{symbol}"
        return self.store_by_data_type("daily_cumulative", key, json.dumps(bucket_incremental_volume))

    def store_alert(self, alert_id, alert_data):
        """Store alert in DB 1 (realtime) via store_by_data_type routing"""
        key = f"alert:{alert_id}"
        return self.store_by_data_type("pattern_alerts", key, json.dumps(alert_data))

    def store_validation_result(self, alert_id, validation_data):
        """Store validation result in DB 1 (realtime) via store_by_data_type routing"""
        key = f"validation:{alert_id}"
        return self.store_by_data_type("pattern_alerts", key, json.dumps(validation_data))

    def store_news_sentiment(self, symbol, news_data):
        """Store news sentiment data in DB 1 (realtime) via store_by_data_type routing"""
        key = f"news:{symbol}:{int(time.time())}"
        return self.store_by_data_type("news_data", key, json.dumps(news_data))

    # ============ Redis Streams Methods ============

    def publish_tick_stream(self, symbol, tick_data):
        """
        Publish tick data to Redis Stream.
        
        Storage Location:
        - Database: DB 1 (realtime) - via publish_to_stream() -> get_database_for_data_type("stream_data")
        - Stream Key: "ticks:{symbol}" (e.g., "ticks:NSE:RELIANCE")
        - Max Length: 50000 entries (auto-trimmed)
        
        Data Format/Signature:
        - Input: tick_data (dict, list, str, or other serializable type)
        - Stored Format: Redis Stream entry with fields:
          {
            "data": <JSON string of tick_data>,
            "timestamp": <current timestamp in milliseconds>
          }
        - See publish_to_stream() for full data format details
        
        Args:
            symbol: Trading symbol (e.g., "NSE:RELIANCE")
            tick_data: Tick data dictionary or serializable object
            
        Returns:
            Stream entry ID (string) on success, False on failure
        """
        # ✅ SOURCE OF TRUTH: Canonicalize symbol and use DatabaseAwareKeyBuilder static method
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        stream_name = DatabaseAwareKeyBuilder.live_ticks_stream(canonical_symbol)
        
        return self.publish_to_stream(stream_name, tick_data, maxlen=50000)

    def publish_alert_stream(self, alert_data):
        """
        Publish alert data to Redis Stream.
        
        Storage Location:
        - Database: DB 1 (realtime) - via publish_to_stream() -> get_database_for_data_type("stream_data")
        - Stream Key: "alerts:system"
        - Max Length: 10000 entries (auto-trimmed)
        
        Data Format/Signature:
        - Input: alert_data (dict, list, str, or other serializable type)
        - Stored Format: Redis Stream entry with fields:
          {
            "data": <JSON string of alert_data>,
            "timestamp": <current timestamp in milliseconds>
          }
        - See publish_to_stream() for full data format details
        
        Args:
            alert_data: Alert data dictionary or serializable object
            
        Returns:
            Stream entry ID (string) on success, False on failure
        """
        stream_name = "alerts:system"
        return self.publish_to_stream(stream_name, alert_data, maxlen=10000)

    def publish_pattern_stream(self, symbol, pattern_data):
        """
        Publish pattern detection data to Redis Stream.
        
        Storage Location:
        - Database: DB 1 (realtime) - via publish_to_stream() -> get_database_for_data_type("stream_data")
        - Stream Key: "patterns:{symbol}" (e.g., "patterns:NSE:RELIANCE")
        - Max Length: 5000 entries (auto-trimmed)
        
        Data Format/Signature:
        - Input: pattern_data (dict, list, str, or other serializable type)
        - Stored Format: Redis Stream entry with fields:
          {
            "data": <JSON string of pattern_data>,
            "timestamp": <current timestamp in milliseconds>
          }
        - See publish_to_stream() for full data format details
        
        Args:
            symbol: Trading symbol (e.g., "NSE:RELIANCE")
            pattern_data: Pattern detection data dictionary or serializable object
            
        Returns:
            Stream entry ID (string) on success, False on failure
        """
        stream_name = f"patterns:{symbol}"
        return self.publish_to_stream(stream_name, pattern_data, maxlen=5000)

    def read_tick_stream(self, symbol, count=100, start_id="0"):
        """
        Read from tick stream.
        
        Storage Location:
        - Database: DB 1 (realtime) - via read_from_stream() -> get_database_for_data_type("stream_data")
        - Stream Key: "ticks:{symbol}" (e.g., "ticks:NSE:RELIANCE")
        
        Data Format/Signature:
        - Returns: List of tuples (message_id, fields_dict)
        - Fields format (from publish_tick_stream()):
          {
            "data": "<JSON string of tick_data>",
            "timestamp": "<timestamp in milliseconds>"
          }
        - See read_from_stream() for full return format details
        
        Args:
            symbol: Trading symbol (e.g., "NSE:RELIANCE")
            count: Maximum number of messages to read (default: 100)
            start_id: Starting message ID (default: "0" for beginning)
            
        Returns:
            List of (message_id, fields_dict) tuples, or empty list on error
        """
        # ✅ SOURCE OF TRUTH: Canonicalize symbol and use DatabaseAwareKeyBuilder static method
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        stream_name = DatabaseAwareKeyBuilder.live_ticks_stream(canonical_symbol)
        return self.read_from_stream(stream_name, count, start_id)

    def read_alert_stream(self, count=100, start_id="0"):
        """
        Read from alert stream.
        
        Storage Location:
        - Database: DB 1 (realtime) - via read_from_stream() -> get_database_for_data_type("stream_data")
        - Stream Key: "alerts:system"
        
        Data Format/Signature:
        - Returns: List of tuples (message_id, fields_dict)
        - Fields format (from publish_alert_stream()):
          {
            "data": "<JSON string of alert_data>",
            "timestamp": "<timestamp in milliseconds>"
          }
        - See read_from_stream() for full return format details
        
        Args:
            count: Maximum number of messages to read (default: 100)
            start_id: Starting message ID (default: "0" for beginning)
            
        Returns:
            List of (message_id, fields_dict) tuples, or empty list on error
        """
        stream_name = "alerts:system"
        return self.read_from_stream(stream_name, count, start_id)

    def read_pattern_stream(self, symbol, count=100, start_id="0"):
        """
        Read from pattern stream.
        
        Storage Location:
        - Database: DB 1 (realtime) - via read_from_stream() -> get_database_for_data_type("stream_data")
        - Stream Key: "patterns:{symbol}" (e.g., "patterns:NSE:RELIANCE")
        
        Data Format/Signature:
        - Returns: List of tuples (message_id, fields_dict)
        - Fields format (from publish_pattern_stream()):
          {
            "data": "<JSON string of pattern_data>",
            "timestamp": "<timestamp in milliseconds>"
          }
        - See read_from_stream() for full return format details
        
        Args:
            symbol: Trading symbol (e.g., "NSE:RELIANCE")
            count: Maximum number of messages to read (default: 100)
            start_id: Starting message ID (default: "0" for beginning)
            
        Returns:
            List of (message_id, fields_dict) tuples, or empty list on error
        """
        stream_name = f"patterns:{symbol}"
        return self.read_from_stream(stream_name, count, start_id)

    # ============ Cumulative Data Tracking Methods ============

    def update_cumulative_data(self, symbol, last_price, bucket_incremental_volume, timestamp=None, depth_data=None, bucket_cumulative_volume=None):
        """
        Update cumulative data with proper cumulative volume handling
        
        If bucket_cumulative_volume is not provided, attempt to extract from:
        1. depth_data (if provided)
        2. Redis session data
        3. Leave as None (cumulative fields won't be updated)
        """
        # Try to extract cumulative from depth_data if not provided
        if bucket_cumulative_volume is None and depth_data is not None:
            bucket_cumulative_volume = depth_data.get('zerodha_cumulative_volume') or depth_data.get('bucket_cumulative_volume')
        
        return self.cumulative_tracker.update_symbol_data(
            symbol, last_price, bucket_incremental_volume, timestamp, depth_data, bucket_cumulative_volume
        )

    def update_symbol_data_direct(
        self,
        symbol,
        bucket_cumulative_volume,
        last_price,
        timestamp,
        high=None,
        low=None,
        open_price=None,
        close_price=None,
        depth_data=None,
    ):
        """Optimized cumulative update when upstream provides direct totals."""
        return self.cumulative_tracker.update_symbol_data_direct(
            symbol,
            bucket_cumulative_volume,
            last_price,
            timestamp,
            high=high,
            low=low,
            open_price=open_price,
            close_price=close_price,
            depth_data=depth_data,
        )

    def get_cumulative_data(self, symbol, session_date=None):
        """Get cumulative data for a symbol"""
        return self.cumulative_tracker.get_cumulative_data(symbol, session_date)

    def get_session_price_movement(self, symbol, minutes_back=30):
        """Get last_price movement analysis for recent period"""
        return self.cumulative_tracker.get_session_price_movement(symbol, minutes_back)

    def get_pattern_data(self, symbol, time_of_day=None, session_date=None):
        """Get pattern data for a symbol based on time of day"""
        return self.cumulative_tracker.get_pattern_data(
            symbol, time_of_day, session_date
        )

    def _extract_base_symbol(self, symbol: str) -> str:
        """Extract base symbol from F&O contract names"""
        # Ensure symbol is a string (may be int like instrument_token)
        if not isinstance(symbol, str):
            symbol = str(symbol)
        # Remove exchange prefix if present
        if ":" in symbol:
            symbol = symbol.split(":", 1)[1]
        
        import re
        is_option = bool(re.search(r"\d+(CE|PE)$", symbol))
        is_future = symbol.endswith("FUT") or bool(re.search(r"\d{2}[A-Z]{3}", symbol))

        if is_future or is_option:
            base_symbol = symbol

            expiry_match = re.search(r"\d{2}[A-Z]{3}", base_symbol)
            if expiry_match:
                base_symbol = base_symbol.replace(expiry_match.group(), "")

            if base_symbol.endswith("FUT"):
                base_symbol = base_symbol[:-3]
            elif is_option:
                base_symbol = re.sub(r"\d+(CE|PE)$", "", base_symbol)

            return base_symbol

        return symbol

    # ✅ REMOVED: get_rolling_window_buckets - session and bucket window logic removed
    # ✅ SESSION-BASED: Buckets are now retrieved via CumulativeDataTracker.get_time_buckets()
    # Buckets stored in DB1 as: bucket_incremental_volume:history:{resolution}:{symbol}
    # Use CumulativeDataTracker for proper session-based bucket retrieval

    def get_time_buckets_enhanced(self, symbol: str, lookback_minutes: int = 60) -> List[Dict[str, Any]]:
        """
        Enhanced bucket retrieval that falls back to 30-day OHLC data.
        """
        try:
            # Bucket data is in DB 0 (system), not DB 5
            self.redis_client.select(0)
            
            # ✅ FIXED: get_time_buckets is in CumulativeDataTracker, not RobustRedisClient
            buckets = self.cumulative_tracker.get_time_buckets(symbol, lookback_minutes=lookback_minutes)
            
            # ✅ SINGLE SOURCE OF TRUTH: Accept any available bucket data
            # Don't require specific number of buckets - use whatever real data is available
            # This prevents "NO_BUCKET_DATA" when sparse but valid data exists
            required_buckets = 1  # Accept any bucket data
            
            if len(buckets) >= required_buckets:
                logger.info(
                    f"✅ [BUCKET_DATA] {symbol} - Using {len(buckets)} real-time buckets"
                )
                return buckets

            # Switch to DB 5 for OHLC fallback
            self.redis_client.select(5)
            ohlc_buckets = self._get_ohlc_buckets_from_30day(symbol, lookback_minutes)
            if ohlc_buckets:
                logger.info(
                    f"📦 [OHLC_FALLBACK] {symbol} - Using {len(ohlc_buckets)} OHLC data points"
                )
                return ohlc_buckets

            logger.info(
                f"ℹ️ [NO_BUCKET_DATA] {symbol} - No real bucket data available, using 4 consolidated bucket system"
            )
            # ✅ SINGLE SOURCE OF TRUTH: Do NOT generate synthetic data
            # Synthetic data prevents pattern detection and violates data integrity
            # Return empty data instead of fake data
            return []
        except Exception as exc:
            logger.error(f"❌ [BUCKET_LOAD_ERROR] {symbol}: {exc}")
            return []

    def get_time_buckets_robust(self, symbol: str, lookback_minutes: int = 60) -> List[Dict[str, Any]]:
        """Guaranteed time-bucket loading with layered fallbacks."""
        try:
            if hasattr(self, "get_time_buckets_enhanced"):
                buckets = self.get_time_buckets_enhanced(symbol, lookback_minutes)
            else:
                # ✅ FIXED: get_time_buckets is in CumulativeDataTracker, not RobustRedisClient
                buckets = self.cumulative_tracker.get_time_buckets(symbol, lookback_minutes=lookback_minutes)
            if buckets:
                return buckets

            buckets = self._get_ohlc_buckets(symbol, lookback_minutes)
            if buckets:
                return buckets

            buckets = self._get_tick_buckets(symbol, lookback_minutes)
            if buckets:
                return buckets

            # ✅ SINGLE SOURCE OF TRUTH: Do NOT generate synthetic data
            # Synthetic data prevents pattern detection and violates data integrity
            # Return empty data instead of fake data
            return []
        except Exception as e:
            logger.error(f"❌ [BUCKET_LOAD_FAILED] {symbol}: {e}")
            return []

    def _get_ohlc_buckets(self, symbol: str, lookback_minutes: int) -> List[Dict[str, Any]]:
        """Load OHLC buckets from Redis Time Series fallbacks."""
        hourly_limit = max(int(lookback_minutes / 5) + 1, 12)
        hourly_series = self._load_ohlc_series(symbol, hourly_limit, interval="1h")
        if hourly_series:
            buckets = self._convert_ohlc_to_buckets(symbol, hourly_series, lookback_minutes)
            if buckets:
                return buckets

        daily_series = self._load_ohlc_series(symbol, 30, interval="1d")
        if daily_series:
            return self._convert_ohlc_to_buckets(symbol, daily_series, lookback_minutes)

        return []

    def _load_ohlc_series(
        self, symbol: str, limit: int, interval: str = "1d"
    ) -> List[Dict[str, Any]]:
        client = self.redis_client
        if not client or limit <= 0:
            return []

        symbol_key = RedisKeyStandards.canonical_symbol(symbol)
        if interval == "1h":
            # ✅ FIXED: Use DatabaseAwareKeyBuilder static method for OHLC hourly (DB1)
            zset_key = DatabaseAwareKeyBuilder.live_ohlc_timeseries(symbol_key, "1h")
        else:
            # ✅ FIXED: Use DatabaseAwareKeyBuilder static method for OHLC daily (DB1)
            zset_key = DatabaseAwareKeyBuilder.live_ohlc_daily(symbol_key)

        try:
            entries = client.zrevrange(zset_key, 0, max(limit - 1, 0), withscores=True)
        except Exception as exc:
            return []

        series: List[Dict[str, Any]] = []
        for payload, score in reversed(entries or []):
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            try:
                data = json.loads(payload)
            except Exception:
                continue
            data["timestamp"] = int(score)
            series.append(data)
        return series

    def _convert_ohlc_to_buckets(
        self, symbol: str, ohlc_data: List[Dict[str, Any]], lookback_minutes: int = 60
    ) -> List[Dict[str, Any]]:
        """Convert 30-day OHLC list into simple bucket format using recent entries."""
        if not ohlc_data:
            return []

        buckets: List[Dict[str, Any]] = []
        now_ts = time.time()
        recent = ohlc_data[-5:]
        for idx, day in enumerate(reversed(recent)):
            try:
                timestamp_value = day.get("timestamp") or day.get("t")
                if not timestamp_value and day.get("d"):
                    try:
                        dt = datetime.fromisoformat(f"{day['d']}T00:00:00")
                        timestamp_value = int(dt.timestamp())
                    except Exception:
                        timestamp_value = None
                if timestamp_value:
                    bucket_ts = (
                        timestamp_value / 1000.0
                        if isinstance(timestamp_value, (int, float)) and timestamp_value > 1e12
                        else float(timestamp_value)
                    )
                else:
                    bucket_ts = now_ts - (idx * 86400)

                open_price = day.get("open", day.get("o"))
                high = day.get("high", day.get("h"))
                low = day.get("low", day.get("l"))
                close_price = day.get("close", day.get("c"))
                bucket_incremental_volume = day.get("bucket_incremental_volume", day.get("v", 0))

                bucket = {
                    "symbol": symbol,
                    "timestamp": bucket_ts,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close_price,
                    "bucket_incremental_volume": bucket_incremental_volume,
                    "source": "30day_ohlc",
                }
                buckets.append(bucket)
            except Exception:
                continue

        if lookback_minutes and len(buckets) > lookback_minutes:
            buckets = buckets[:lookback_minutes]
        return buckets

    def _get_ohlc_buckets_from_30day(self, symbol: str, lookback_minutes: int) -> List[Dict[str, Any]]:
        """Convert 30-day OHLC snapshots into synthetic intraday buckets."""
        daily_series = self._load_ohlc_series(symbol, 30, interval="1d")
        if not daily_series:
            return []

        ohlc_series: List[Dict[str, Any]] = []
        for entry in daily_series:
            try:
                open_price = entry.get("open", entry.get("o"))
                high = entry.get("high", entry.get("h"))
                low = entry.get("low", entry.get("l"))
                close_price = entry.get("close", entry.get("c"))
                bucket_incremental_volume = entry.get("bucket_incremental_volume", entry.get("v", 0))
                date_str = entry.get("d")
                if not date_str:
                    ts = entry.get("timestamp")
                    if ts and ts > 1e12:
                        ts = ts / 1000.0
                    if ts:
                        date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d 00:00:00")
                if not date_str:
                    continue
                ohlc_series.append(
                    {
                        "date": date_str,
                        "open": open_price,
                        "high": high,
                        "low": low,
                        "close": close_price,
                        "bucket_incremental_volume": bucket_incremental_volume,
                    }
                )
            except Exception:
                continue

        if not ohlc_series:
            return []

        simple_buckets = self._convert_ohlc_to_buckets(symbol, ohlc_series, lookback_minutes)
        if simple_buckets:
            logger.info(
                f"✅ [OHLC_SIMPLE] {symbol} - Generated {len(simple_buckets)} simple buckets from 30-day data"
            )
            return simple_buckets

        recent_days = ohlc_series[-10:]
        if not recent_days:
            return []

        bucket_minutes = [0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330]
        buckets: List[Dict[str, Any]] = []

        per_day_limit = max(1, lookback_minutes // max(len(bucket_minutes), 1))

        for idx, day in enumerate(recent_days):
            date_str = day.get("date")
            try:
                base_dt = datetime.fromisoformat(f"{date_str} 09:15:00") if date_str else datetime.now() - timedelta(days=len(recent_days) - idx)
            except ValueError:
                base_dt = datetime.now() - timedelta(days=len(recent_days) - idx)

            daily_volume = float(day.get("bucket_incremental_volume", 0) or 0.0)
            per_bucket_volume = daily_volume / len(bucket_minutes) if daily_volume else 0.0

            day_added = 0
            for offset in bucket_minutes:
                bucket_time = base_dt + timedelta(minutes=offset)
                bucket = {
                    "symbol": symbol,
                    "session_date": bucket_time.strftime("%Y-%m-%d"),
                    "first_timestamp": bucket_time.timestamp(),
                    "last_timestamp": bucket_time.timestamp(),
                    "open": day.get("open"),
                    "high": day.get("high"),
                    "low": day.get("low"),
                    "close": day.get("close"),
                    "bucket_incremental_volume": per_bucket_volume,
                    "source": "30day_ohlc",
                }
                buckets.append(bucket)
                day_added += 1
                if day_added >= per_day_limit or len(buckets) >= lookback_minutes:
                    break
            if len(buckets) >= lookback_minutes:
                break

        if buckets:
            logger.info(
                f"📊 [OHLC_CONVERSION] {symbol} - Created {len(buckets)} buckets from 30-day OHLC data"
            )
        return buckets

    def _get_tick_buckets(self, symbol: str, lookback_minutes: int) -> List[Dict[str, Any]]:
        """Build buckets from raw tick data if available."""
        client = self.redis_client
        if not client:
            return []

        # ✅ SOURCE OF TRUTH: Canonicalize symbol and use DatabaseAwareKeyBuilder static method
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        tick_key = DatabaseAwareKeyBuilder.live_ticks_hash(canonical_symbol)
        try:
            recent_ticks = client.lrange(tick_key, -2000, -1)
        except Exception as e:
            return []

        if not recent_ticks:
            return []

        buckets = self._bucket_ticks(recent_ticks, lookback_minutes)
        if buckets:
            logger.info(f"✅ [TICK_DATA] {symbol} - Created {len(buckets)} buckets from ticks")
        return buckets

    def _bucket_ticks(self, ticks: List[Any], lookback_minutes: int) -> List[Dict[str, Any]]:
        """Convert raw ticks into minute buckets."""
        if not ticks:
            return []

        min_timestamp = time.time() - (lookback_minutes * 60)
        bucket_map: Dict[int, Dict[str, Any]] = {}

        for raw_tick in ticks:
            try:
                if isinstance(raw_tick, bytes):
                    raw_tick = raw_tick.decode("utf-8")
                if isinstance(raw_tick, str):
                    tick = json.loads(raw_tick)
                elif isinstance(raw_tick, dict):
                    tick = raw_tick
                else:
                    continue
            except Exception:
                continue

            ts = (
                tick.get("timestamp_epoch")
                or tick.get("timestamp_ms")
                or tick.get("timestamp")
                or tick.get("time")
            )
            if ts is None:
                continue
            try:
                ts = float(ts)
            except (TypeError, ValueError):
                continue
            if ts > 1e12:  # Convert ms to seconds
                ts = ts / 1000.0
            if ts < min_timestamp:
                continue

            last_price = (
                tick.get("last_price")
                or tick.get("close")
                or tick.get("last_price")
                or tick.get("last_price")
            )
            last_price = self._safe_float(last_price)
            if last_price <= 0:
                continue

            bucket_incremental_volume = (
                tick.get("bucket_incremental_volume")
                or tick.get("zerodha_last_traded_quantity")
                or tick.get("quantity")
                or tick.get("trade_quantity")
            )
            bucket_incremental_volume = self._safe_float(bucket_incremental_volume)

            bucket_start = int(ts // 60) * 60
            bucket = bucket_map.get(bucket_start)
            if not bucket:
                bucket = {
                    "timestamp": bucket_start,
                    "first_timestamp": bucket_start,
                    "last_timestamp": bucket_start,
                    "open": last_price,
                    "high": last_price,
                    "low": last_price,
                    "close": last_price,
                    "bucket_incremental_volume": bucket_incremental_volume,
                }
                bucket_map[bucket_start] = bucket
            else:
                bucket["high"] = max(bucket["high"], last_price)
                bucket["low"] = min(bucket["low"], last_price)
                bucket["close"] = last_price
                bucket["last_timestamp"] = bucket_start
                bucket["bucket_incremental_volume"] += bucket_incremental_volume

        if not bucket_map:
            return []

        ordered = []
        for ts in sorted(bucket_map.keys())[-lookback_minutes:]:
            ordered.append(bucket_map[ts])
        return ordered

    def _get_last_price(self, symbol: str) -> Optional[float]:
        """Fetch the most recent known last_price for a symbol.
        
        ✅ CRITICAL: Checks multiple sources including ticks stream (primary source)
        """
        # ✅ PRIMARY: Check ticks stream (most reliable source for real-time prices)
        try:
            # ✅ SOURCE OF TRUTH: Canonicalize symbol and use DatabaseAwareKeyBuilder static method
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            stream_key = DatabaseAwareKeyBuilder.live_ticks_stream(canonical_symbol)
            # Try to get last message from stream (most recent tick)
            messages = self.redis_client.xrevrange(stream_key, count=1)
            if messages:
                msg_id, fields = messages[0]
                # Extract data field (contains JSON with last_price)
                data_field = fields.get('data') or fields.get(b'data')
                if data_field:
                    # Handle both bytes and string
                    if isinstance(data_field, bytes):
                        data_field = data_field.decode('utf-8')
                    
                    # Parse JSON to extract last_price
                    try:
                        import json
                        tick_data = json.loads(data_field)
                        price = tick_data.get('last_price') or tick_data.get('price') or tick_data.get('close')
                        if price is not None:
                            return self._safe_float(price)
                    except (json.JSONDecodeError, TypeError, KeyError):
                        pass
        except Exception:
            pass  # Fall through to other methods
        
        # Fallback 1: Check direct keys
        candidate_keys = [
            f"last_price:{symbol}",
            f"latest_price:{symbol}",
            f"market_data:last_price:{symbol}",
        ]

        for key in candidate_keys:
            try:
                value = self.get(key)
                if value:
                    return self._safe_float(value)
            except Exception:
                continue

        # Fallback 2: Check session data
        try:
            session_key = f"session:{symbol}:{datetime.now().strftime('%Y-%m-%d')}"
            session_data = self.get(session_key)
            if session_data:
                if isinstance(session_data, (bytes, str)):
                    session_data = json.loads(
                        session_data.decode("utf-8") if isinstance(session_data, bytes) else session_data
                    )
                if isinstance(session_data, dict):
                    time_buckets = session_data.get("time_buckets") or {}
                    if isinstance(time_buckets, dict) and time_buckets:
                        last_bucket = time_buckets.get(sorted(time_buckets.keys())[-1])
                        if isinstance(last_bucket, dict):
                            last_price = (
                                last_bucket.get("close")
                                or last_bucket.get("last_price")
                                or last_bucket.get("last_price")
                            )
                            if last_price:
                                return self._safe_float(last_price)
                    last_price = (
                        session_data.get("last_price")
                        or session_data.get("close")
                        or session_data.get("last_price")
                    )
                    if last_price:
                        return self._safe_float(last_price)
        except Exception:
            pass

        return None

    def get_last_price(self, symbol: str) -> Optional[float]:
        """Public wrapper to fetch the most recent last_price for a symbol."""
        try:
            return self._get_last_price(symbol)
        except Exception as e:
            return None

    def _get_buckets_by_pattern(self, symbol, pattern, start_time, bucket_size):
        """Get buckets using key pattern scanning"""
        try:
            # Get all keys matching pattern
            keys = self.redis_client.keys(pattern)
            if not keys:
                return []
            
            
            buckets = []
            for key in keys:
                try:
                    # Try different data structure access methods
                    bucket_data = self._extract_bucket_data(key, start_time)
                    if bucket_data:
                        buckets.append(bucket_data)
                except Exception as e:
                    continue
            
            return buckets
            
        except Exception as e:
            logger.error(f"Error in _get_buckets_by_pattern: {e}")
            return []

    def _extract_bucket_data(self, key, start_time):
        """Extract bucket data from Redis key using multiple methods"""
        try:
            # Method 1: Try as Hash first (most common)
            bucket_data = self.redis_client.hgetall(key)
            if bucket_data:
                return self._parse_hash_bucket(key, bucket_data, start_time)
            
            # Method 2: Try as String (JSON)
            str_data = self.redis_client.get(key)
            if str_data:
                return self._parse_string_bucket(key, str_data, start_time)
            
            # Method 3: Try as Sorted Set
            try:
                zset_data = self.redis_client.zrange(key, 0, -1, withscores=True)
                if zset_data:
                    return self._parse_zset_bucket(key, zset_data, start_time)
            except:
                pass
            
            return None
            
        except Exception as e:
            return None

    def _parse_hash_bucket(self, key, bucket_data, start_time):
        """Parse bucket data from Redis hash"""
        try:
            # Extract timestamp from key (common patterns)
            timestamp = self._extract_timestamp_from_key(key)
            if not timestamp:
                return None
            
            # More lenient time filtering - include buckets from today and recent days
            from datetime import datetime, timedelta
            bucket_dt = datetime.fromtimestamp(timestamp)
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Include buckets from today or recent days (within last 7 days)
            if bucket_dt < today - timedelta(days=7):
                return None
            
            # Standardize bucket_incremental_volume fields
            standardized_data = self._standardize_bucket_data(bucket_data, timestamp)
            return standardized_data
            
        except Exception as e:
            return None

    def _extract_timestamp_from_key(self, key):
        """Extract timestamp from various key patterns"""
        try:
            # Common timestamp patterns in keys:
            # volume_buckets:NSE:RELIANCE:1718902800
            # buckets:NSE:RELIANCE:20240620103000
            # ts:NSE:RELIANCE:1718902860
            
            key_parts = key.split(':')
            
            # Pattern 1: Unix timestamp at end
            if key_parts[-1].isdigit():
                timestamp = int(key_parts[-1])
                # Check if it's Unix timestamp (reasonable range)
                if 1600000000 < timestamp < 2000000000:  # 2020-2033
                    return float(timestamp)
                # Check if it's formatted datetime (YYYYMMDDHHMMSS)
                elif len(key_parts[-1]) == 14:
                    from datetime import datetime
                    dt = datetime.strptime(key_parts[-1], '%Y%m%d%H%M%S')
                    return dt.timestamp()
            
            # Pattern 2: Try second-to-last part
            if len(key_parts) >= 2 and key_parts[-2].isdigit():
                timestamp = int(key_parts[-2])
                if 1600000000 < timestamp < 2000000000:
                    return float(timestamp)
            
            # Pattern 3: Extract from bucket_incremental_volume:bucket_incremental_volume:bucket:SYMBOL:buckets:HOUR:MINUTE
            if 'bucket_incremental_volume:bucket_incremental_volume:bucket:' in key and 'buckets:' in key:
                try:
                    # Extract hour and minute from key
                    parts = key.split(':')
                    if len(parts) >= 6:
                        hour = int(parts[-2])
                        minute = int(parts[-1]) * 5  # Convert bucket to minute
                        from datetime import datetime
                        # Use current date with extracted time
                        dt = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
                        return dt.timestamp()
                except:
                    pass
            
            # Pattern 4: Extract from bucket:NSE:SYMBOL:YYYY-MM-DD:HH:MM
            if key.startswith('bucket:NSE:') and len(key_parts) >= 6:
                try:
                    date_part = key_parts[3]  # YYYY-MM-DD
                    hour_part = key_parts[4]  # HH
                    minute_part = key_parts[5]  # MM
                    
                    from datetime import datetime
                    dt = datetime.strptime(f"{date_part} {hour_part}:{minute_part}", "%Y-%m-%d %H:%M")
                    return dt.timestamp()
                except:
                    pass
            
            return None
            
        except Exception as e:
            return None

    def _standardize_bucket_data(self, raw_data, timestamp):
        """Standardize bucket data to common format"""
        standardized = {'timestamp': timestamp}
        
        # Price fields
        price_fields = ['open', 'high', 'low', 'close', 'last_price', 'last_price']
        for field in price_fields:
            if field in raw_data:
                standardized[field] = self._safe_float(raw_data[field])
        
        # Volume fields - Use canonical field names from optimized_field_mapping.yaml
        # Only use the standard field name to avoid confusion
        if 'zerodha_cumulative_volume' in raw_data:
            standardized['zerodha_cumulative_volume'] = self._safe_float(raw_data['zerodha_cumulative_volume'])
        elif 'bucket_cumulative_volume' in raw_data:
            standardized['zerodha_cumulative_volume'] = self._safe_float(raw_data['bucket_cumulative_volume'])
        
        # If no bucket_incremental_volume found, try to calculate from other fields
        if 'zerodha_cumulative_volume' not in standardized:
            if 'buy_volume' in raw_data and 'sell_volume' in raw_data:
                buy_vol = self._safe_float(raw_data.get('buy_volume', 0))
                sell_vol = self._safe_float(raw_data.get('sell_volume', 0))
                standardized['zerodha_cumulative_volume'] = buy_vol + sell_vol
        
        # Ensure we have at least basic fields
        if 'close' not in standardized and 'last_price' in standardized:
            standardized['close'] = standardized['last_price']
        
        return standardized

    def _safe_float(self, value) -> float:
        """Safely convert to float"""
        try:
            if value is None:
                return float(0.0)
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _parse_string_bucket(self, key, str_data, start_time):
        """Parse bucket data from JSON string"""
        try:
            import json
            data = json.loads(str_data)
            timestamp = self._extract_timestamp_from_key(key)
            if timestamp:
                return self._standardize_bucket_data(data, timestamp)
            return None
        except Exception as e:
            return None

    def _parse_zset_bucket(self, key, zset_data, start_time):
        """Parse bucket data from sorted set"""
        try:
            # For sorted sets, use the score as timestamp
            if zset_data:
                timestamp, data = zset_data[0]
                return self._standardize_bucket_data(data, float(timestamp))
            return None
        except Exception as e:
            return None

    def get_ohlc_buckets(self, symbol, count=10, session_date=None):
        """Convenience: return most recent N OHLC buckets for a symbol.

        Args:
            symbol (str): instrument symbol
            count (int): number of recent buckets to return
            session_date (str|None): YYYY-MM-DD; defaults to current session
        Returns:
            list[dict]: recent bucket dicts sorted by first_timestamp ascending within the slice
        """
        # ✅ FIXED: get_time_buckets is in CumulativeDataTracker, not RobustRedisClient
        buckets = self.cumulative_tracker.get_time_buckets(symbol, session_date=session_date)
        if not buckets:
            return []
        return buckets[-count:]

    def get_index(self, index_name):
        """Get normalized index snapshot from Redis, parsed as dict.

        Accepts aliases like 'nifty50', 'nifty_50', 'bank_nifty', 'niftybank',
        'giftnifty', 'sgx_nifty', 'indiavix', 'india_vix'.
        Returns parsed JSON dict or None.
        """
        try:
            name = (index_name or "").lower().replace(" ", "")
            alias_map = {
                "nifty50": "nifty50",
                "nifty_50": "nifty50",
                "niftybank": "niftybank",
                "banknifty": "niftybank",
                "bank_nifty": "niftybank",
                "giftnifty": "giftnifty",
                "sgxnifty": "giftnifty",
                "sgx_nifty": "giftnifty",
                "indiavix": "indiavix",
                "india_vix": "indiavix",
            }
            norm = alias_map.get(name, None)
            if not norm:
                logger.warning(f"Unknown index requested: {index_name}")
                return None
            keys = [
                f"index_data:{norm}",
                f"indices:{norm}",
                f"market:indices:{norm}",
            ]
            for key in keys:
                raw = self.get(key)
                if not raw:
                    continue
                try:
                    return json.loads(raw)
                except Exception:
                    # Sometimes value may already be a dict
                    if isinstance(raw, dict):
                        return raw
                    continue
            return None
        except Exception as e:
            return None

    def cleanup_old_sessions(self, days_to_keep=7):
        """Clean up old session data"""
        return self.cumulative_tracker.cleanup_old_sessions(days_to_keep)

    # ============ Enhanced Order Book Methods ============

    def store_order_book_snapshot(
        self, symbol, order_book_data, last_price, bucket_incremental_volume, timestamp=None
    ):
        """Store complete order book snapshot with last_price/bucket_incremental_volume context"""
        if timestamp is None:
            timestamp = time.time()

        snapshot = {
            "symbol": symbol,
            "timestamp": timestamp,
            "last_price": last_price,
            "bucket_incremental_volume": bucket_incremental_volume,
            "depth": order_book_data,
            "session_date": date.today().isoformat(),
        }

        # Store in cumulative tracker for session tracking
        # Extract cumulative from order_book_data if available
        bucket_cumulative_volume = None
        if order_book_data is not None:
            bucket_cumulative_volume = order_book_data.get('zerodha_cumulative_volume') or order_book_data.get('bucket_cumulative_volume')

        self.cumulative_tracker.update_symbol_data(
            symbol, last_price, bucket_incremental_volume, timestamp, order_book_data, bucket_cumulative_volume
        )

        # Also store as individual snapshot for immediate analysis
        key = f"orderbook:{symbol}:{int(timestamp)}"
        return self.store_by_data_type(
            "5s_ticks", key, json.dumps(snapshot, cls=SafeJSONEncoder)
        )

    def get_order_book_history(self, symbol, minutes_back=5):
        """Get order book history for pattern analysis"""
        # ✅ FIXED: get_time_buckets is in CumulativeDataTracker, not RobustRedisClient
        buckets = self.cumulative_tracker.get_time_buckets(symbol)

        if not buckets:
            return []

        # Get recent buckets
        current_time = time.time()
        recent_cutoff = current_time - (minutes_back * 60)

        recent_history = []
        for bucket in buckets:
            if bucket["last_timestamp"] >= recent_cutoff:
                # Extract order book snapshots from bucket
                if "order_book_snapshots" in bucket:
                    recent_history.extend(bucket["order_book_snapshots"])

        # Sort by timestamp
        recent_history.sort(key=lambda x: x["timestamp"])
        return recent_history

    # ============ Basic Hash Helpers ============
    def hget(self, name, key):
        try:
            self._ensure_connection()
            if self.redis_client:
                # Transform key to new unified structure
                name = self._transform_key(name)
                return self.redis_client.hget(name, key)
        except Exception as e:
            pass
        return None

    def hset(self, name, key=None, value=None, mapping=None, items=None):
        """
        Set key to value within hash name.
        
        Matches redis-py 7.0.1 signature:
        hset(name: str, key: Optional[str] = None, value: Optional[str] = None, 
             mapping: Optional[dict] = None, items: Optional[list] = None)
        
        Args:
            name: Hash name
            key: Field name (optional if using mapping or items)
            value: Field value (optional if using mapping or items)
            mapping: Dict of key/value pairs for batch update (Redis 8.2 optimization)
            items: List of key/value pairs (alternative to mapping)
        
        Returns:
            Number of fields added, or 0 on failure
        """
        try:
            self._ensure_connection()
            if self.redis_client:
                # Transform key to new unified structure
                name = self._transform_key(name)
                # Support all parameter combinations matching redis-py 7.0.1
                if mapping is not None:
                    return self.redis_client.hset(name, mapping=mapping)
                elif items is not None:
                    return self.redis_client.hset(name, items=items)
                elif key is not None and value is not None:
                    return self.redis_client.hset(name, key, value)
                else:
                    # Invalid call - need at least key+value, mapping, or items
                    logger.warning(f"hset called with invalid parameters: name={name}, key={key}, value={value}, mapping={mapping}, items={items}")
                    return 0
        except Exception as e:
            logger.error(f"hset failed for {name}: {e}")
            return 0

    # ============ Volume & Indicator Methods (Unified interface) ============
    
    def store_volume_state(self, token: str, data: dict):
        """Store volume state - new structure: vol:state:{token}"""
        storage = UnifiedDataStorage(redis_client=self)
        storage.store_volume_state(token, data)
    
    def get_volume_state(self, token: str) -> dict:
        """Get volume state from new structure"""
        storage = UnifiedDataStorage(redis_client=self)
        return storage.get_volume_state(token)
    
    def store_volume_baseline(self, symbol: str, data: dict, ttl: int = 86400):
        """Store volume baseline - new structure: vol:baseline:{symbol}"""
        storage = UnifiedDataStorage(redis_client=self)
        storage.store_volume_baseline(symbol, data, ttl=ttl)
    
    def store_volume_profile(self, symbol: str, period: str, data: dict, ttl: int = 57600):
        """Store volume profile - new structure: vol:profile:{symbol}:{period}"""
        storage = UnifiedDataStorage(redis_client=self)
        storage.store_volume_profile(symbol, period, data, ttl=ttl)
    
    def store_straddle_volume(self, underlying: str, date: str, data: dict, ttl: int = 57600):
        """Store straddle volume - new structure: vol:straddle:{underlying}:{date}"""
        storage = UnifiedDataStorage(redis_client=self)
        storage.store_straddle_volume(underlying, date, data, ttl=ttl)
    
    def store_indicator(self, symbol: str, indicator: str, value, ttl: int = 3600):
        """Store indicator via UnifiedDataStorage."""
        storage = UnifiedDataStorage(redis_client=self)
        storage.store_indicators(symbol, {indicator: value}, ttl=ttl)
    
    def get_indicator(self, symbol: str, indicator: str) -> Optional[str]:
        """Fetch indicator via UnifiedDataStorage (includes greek fallbacks)."""
        storage = UnifiedDataStorage(redis_client=self)
        indicators = storage.get_indicators(symbol, indicator_names=[indicator])
        if indicator in indicators:
            return indicators[indicator]
        greeks = storage.get_greeks(symbol)
        return greeks.get(indicator)

    def get_session_order_book_evolution(self, symbol, session_date=None):
        """Get complete order book evolution for a trading session"""
        # ✅ FIXED: get_time_buckets is in CumulativeDataTracker, not RobustRedisClient
        buckets = self.cumulative_tracker.get_time_buckets(symbol, session_date)

        evolution = {
            "symbol": symbol,
            "session_date": session_date or date.today().isoformat(),
            "total_snapshots": 0,
            "time_buckets": len(buckets),
            "order_book_snapshots": [],
        }

        for bucket in buckets:
            if "order_book_snapshots" in bucket:
                evolution["order_book_snapshots"].extend(bucket["order_book_snapshots"])
                evolution["total_snapshots"] += len(bucket["order_book_snapshots"])

        # Sort chronologically
        evolution["order_book_snapshots"].sort(key=lambda x: x["timestamp"])

        return evolution

    def _connect(self):
        """Establish Redis connection with retry logic - now handled by _initialize_clients"""
        # Connection is now handled by _initialize_clients during initialization
        # This method is kept for compatibility but the actual connection logic
        # is in _initialize_clients which creates separate clients per database
        
        if self.is_connected:
            logger.info("✅ Redis clients already connected")
            return True
        else:
            # ✅ FIX: Only warn if clients are actually None, not just not connected yet
            # _initialize_clients() sets is_connected after successful initialization
            if not self.redis_client and not any(self.clients.values()):
                logger.warning("⚠️ Redis clients not properly initialized")
            return False

    def _ensure_connection(self):
        """Check and restore connection if needed"""
        if (
            not self.is_connected
            or time.time() - self.last_health_check > self.health_check_interval
        ):
            try:
                if self.redis_client:
                    self.redis_client.ping()
                    self.last_health_check = time.time()
                    if not self.is_connected:
                        self.is_connected = True
                        logger.info("✅ Redis connection restored")
                else:
                    self._connect()
            except:
                if self.is_connected:
                    logger.warning(
                        "⚠️ Redis connection lost. Attempting reconnection..."
                    )
                    self.is_connected = False
                self._connect()

    def _process_retry_queue(self):
        """Process queued operations after reconnection"""
        processed = 0
        while self.retry_queue and self.is_connected:
            try:
                operation = self.retry_queue.popleft()
                method = operation["method"]
                args = operation["args"]
                kwargs = operation["kwargs"]

                # Retry the operation using the actual method
                getattr(self, method)(*args, **kwargs)
                processed += 1

            except Exception as e:
                logger.error(f"Failed to process queued operation: {e}")
                # Re-queue the failed operation
                self.retry_queue.appendleft(operation)
                break

        if processed > 0:
            logger.info(f"✅ Processed {processed} queued operations")

    def _queue_operation(self, method, *args, **kwargs):
        """Queue operation for retry when connection is restored"""
        self.retry_queue.append(
            {"method": method, "args": args, "kwargs": kwargs, "timestamp": time.time()}
        )
        self.stats["queued_operations"] += 1

    def close(self):
        """Close Redis connection and connection pool"""
        with self.connection_lock:
            if self.redis_client:
                try:
                    self.redis_client.close()
                except Exception as e:
                    logger.error(f"Error closing Redis client: {e}")
                finally:
                    self.redis_client = None

            if getattr(self, "connection_wrappers", None):
                for db_num, wrapper in list(self.connection_wrappers.items()):
                    try:
                        if wrapper:
                            wrapper.close()
                    except Exception as e:
                        logger.error(
                            f"Error closing Redis connection pool wrapper for DB {db_num}: {e}"
                        )
                    finally:
                        self.connection_wrappers[db_num] = None

            self.is_connected = False
            logger.info("Redis connection and pool closed")

    # ============ Redis Operations with Auto-Reconnection ============

    def ping(self):
        """Test connection with auto-reconnection"""
        self._ensure_connection()
        if self.is_connected:
            try:
                return self.redis_client.ping()
            except:
                self.is_connected = False
                self._connect()
                if self.is_connected:
                    return self.redis_client.ping()
        return False

    def get(self, key):
        """Get value with fallback and circuit breaker protection"""
        # Transform key to new unified structure
        key = self._transform_key(key)
        
        if self.circuit_breaker_enabled and self.circuit_breaker:
            if not self.circuit_breaker.should_allow_request():
                logger.warning(f"Circuit breaker open for Redis GET operation: {key}")
                return self._get_fallback(key)

        self._ensure_connection()

        if self.is_connected:
            try:
                result = self.redis_client.get(key)
                self.stats["successful_operations"] += 1
                # Record success for circuit breaker
                if self.circuit_breaker_enabled and self.circuit_breaker:
                    self.circuit_breaker.record_success()
                return result
            except Exception as e:
                self.is_connected = False
                # Record failure for circuit breaker
                if self.circuit_breaker_enabled and self.circuit_breaker:
                    self.circuit_breaker.record_failure(e)
                # Queue for retry
                self._queue_operation("get", key)

        # Fallback to local storage
        with self.fallback_lock:
            self._clean_expired_fallback(key)
            return self.fallback_data.get(key)

    def set(self, key, value, ex=None):
        """Set value with fallback and circuit breaker protection"""
        # Transform key to new unified structure
        key = self._transform_key(key)
        
        if self.circuit_breaker_enabled and self.circuit_breaker:
            if not self.circuit_breaker.should_allow_request():
                logger.warning(f"Circuit breaker open for Redis SET operation: {key}")
                return self._set_fallback(key, value, ex)

        self._ensure_connection()

        success = False
        if self.is_connected:
            try:
                success = self.redis_client.set(key, value, ex=ex)
                self.stats["successful_operations"] += 1
                # Record success for circuit breaker
                if self.circuit_breaker_enabled and self.circuit_breaker:
                    self.circuit_breaker.record_success()
            except Exception as e:
                self.is_connected = False
                # Record failure for circuit breaker
                if self.circuit_breaker_enabled and self.circuit_breaker:
                    self.circuit_breaker.record_failure(e)
                self._queue_operation("set", key, value, ex=ex)

        # Always update fallback storage
        with self.fallback_lock:
            self.fallback_data[key] = value
            if ex:
                self.fallback_expirations[key] = time.time() + ex

        return success or True  # Return True even if using fallback

    def setex(self, key, time_seconds, value):
        """Set value with TTL (compat shim around set with ex)."""
        # Reuse set() which already handles fallback and retry queue
        return self.set(key, value, ex=time_seconds)


    def zrevrangebyscore(
        self, key, max_score, min_score, withscores=False, start=None, num=None
    ):
        """Fetch from sorted set by score (reverse). Returns [] on fallback."""
        self._ensure_connection()
        try:
            if self.is_connected:
                return self.redis_client.zrevrangebyscore(
                    key,
                    max_score,
                    min_score,
                    withscores=withscores,
                    start=start,
                    num=num,
                )
        except Exception as e:
            self.is_connected = False
        # No sensible fallback for sorted sets; return empty
        return []

    def zadd(self, key, mapping):
        """Add items to sorted set. Returns count of added items."""
        self._ensure_connection()
        try:
            if self.is_connected:
                return self.redis_client.zadd(key, mapping)
        except Exception as e:
            self.is_connected = False
        return 0

    def zcard(self, key):
        """Get cardinality of sorted set. Returns 0 on fallback."""
        self._ensure_connection()
        try:
            if self.is_connected:
                return self.redis_client.zcard(key)
        except Exception as e:
            self.is_connected = False
        return 0

    def expire(self, key, time):
        """Set expiry on key. Returns True on success."""
        self._ensure_connection()
        try:
            if self.is_connected:
                return self.redis_client.expire(key, time)
        except Exception as e:
            self.is_connected = False
        return False

    def publish(self, channel, message):
        """Publish message with retry queue and circuit breaker protection"""
        if self.circuit_breaker_enabled and self.circuit_breaker:
            if not self.circuit_breaker.should_allow_request():
                logger.warning(
                    f"Circuit breaker open for Redis PUBLISH operation: {channel}"
                )
                return 0

        self._ensure_connection()

        if self.is_connected:
            try:
                result = self.redis_client.publish(channel, message)
                self.stats["successful_operations"] += 1
                # Record success for circuit breaker
                if self.circuit_breaker_enabled and self.circuit_breaker:
                    self.circuit_breaker.record_success()
                return result
            except Exception as e:
                self.is_connected = False
                # Record failure for circuit breaker
                if self.circuit_breaker_enabled and self.circuit_breaker:
                    self.circuit_breaker.record_failure(e)
                self._queue_operation("publish", channel, message)

        # Fallback: return 0 subscribers for publish operations
        return 0

    def publish_aion_signal(self, signal_type, data, symbol=None):
        """Publish AION research signals into the pattern-detection datastore."""
        if signal_type not in AION_CHANNELS:
            logger.error(f"Unknown AION signal type: {signal_type}")
            return False

        # Store in pattern detection database instead of publishing
        key = f"aion:{signal_type}:{symbol or 'global'}:{int(time.time())}"
        enriched_data = {
            "timestamp": time.time(),
            "signal_type": signal_type,
            "symbol": symbol,
            "data": data,
        }
        
        return self.store_by_data_type("analysis_cache", key, json.dumps(enriched_data))

    def subscribe(self, *channels):
        """Subscribe to channels with reconnection"""
        self._ensure_connection()

        if self.is_connected and self.pubsub:
            try:
                self.pubsub.subscribe(*channels)
                return True
            except Exception as e:
                logger.error(f"Subscribe failed: {e}")
                self.is_connected = False
        return False

    def keys(self, pattern="*"):
        """Get keys with fallback"""
        self._ensure_connection()

        if self.is_connected:
            try:
                # Prefer SCAN to avoid blocking Redis when pattern is broad
                try:
                    iterator = self.redis_client.scan_iter(match=pattern, count=500)
                    result = list(iterator)
                except Exception:
                    result = self.redis_client.keys(pattern)
                self.stats["successful_operations"] += 1
                return result
            except Exception as e:
                self.is_connected = False

        # Fallback to local storage
        with self.fallback_lock:
            if pattern == "*":
                return list(self.fallback_data.keys())
            return [k for k in self.fallback_data if pattern.replace("*", "") in k]

    def delete(self, *keys):
        """Delete keys with fallback"""
        self._ensure_connection()

        deleted = 0
        if self.is_connected:
            try:
                deleted = self.redis_client.delete(*keys)
                self.stats["successful_operations"] += 1
            except Exception as e:
                self.is_connected = False

        # Also delete from fallback
        with self.fallback_lock:
            for key in keys:
                if key in self.fallback_data:
                    del self.fallback_data[key]
                    deleted += 1
                if key in self.fallback_expirations:
                    del self.fallback_expirations[key]

        return deleted

    def lpush(self, key, *values):
        """Push values to the left of a list with enhanced error handling and fallback storage.
        
        Supports both single value and multiple values:
        - lpush(key, value) - single value
        - lpush(key, *values) - multiple values
        """
        if not values:
            return 0
        
        self._ensure_connection()

        # Serialize values if needed
        serialized_values = []
        for v in values:
            if not isinstance(v, str):
                try:
                    v = json.dumps(v, cls=SafeJSONEncoder)
                except Exception:
                    v = str(v)
            serialized_values.append(v)

        if self.is_connected:
            try:
                result = self.redis_client.lpush(key, *serialized_values)
                self.stats["successful_operations"] += 1
                return result
            except Exception as e:
                logger.error(f"Error in lpush for key {key}: {e}")
                self.is_connected = False
                
                # Fallback: try to push values one by one
                success_count = 0
                for value in serialized_values:
                    try:
                        if self.is_connected:
                            self.redis_client.lpush(key, value)
                            success_count += 1
                        else:
                            break
                    except Exception:
                        logger.warning(f"Failed to push individual value to {key}")
                        break

        # Fallback to local storage if Redis unavailable
        with self.fallback_lock:
            if key not in self.fallback_data:
                self.fallback_data[key] = []
            for value in reversed(serialized_values):  # lpush adds to front
                self.fallback_data[key].insert(0, value)
            return len(self.fallback_data[key])

    # ============ Implementation Methods for Retry Queue ============

    def _set_impl(self, key, value, ex=None):
        """Internal set implementation for retry"""
        if self.is_connected:
            self.redis_client.set(key, value, ex=ex)

    def _publish_impl(self, channel, message):
        """Internal publish implementation for retry"""
        if self.is_connected:
            self.redis_client.publish(channel, message)

    # ============ Fallback Helper Methods ============

    def _clean_expired_fallback(self, key):
        """Clean expired keys in fallback storage"""
        if key in self.fallback_expirations:
            if time.time() > self.fallback_expirations[key]:
                del self.fallback_data[key]
                del self.fallback_expirations[key]

    def get_stats(self):
        """Get connection statistics"""
        return {
            "is_connected": self.is_connected,
            "reconnections": self.stats["reconnections"],
            "failed_operations": self.stats["failed_operations"],
            "queued_operations": len(self.retry_queue),
            "total_queued": self.stats["queued_operations"],
            "successful_operations": self.stats["successful_operations"],
            "fallback_keys": len(self.fallback_data),
        }

    # ------------------------------------------------------------------
    # Session utilities for bucket_incremental_volume tracking
    # ------------------------------------------------------------------
    def reset_volume_session(self, symbols=None):
        """Reset cumulative bucket_incremental_volume tracking keys at market open.

        If symbols is None, clears all keys matching 'last_cumulative:*'.
        Otherwise clears only specified symbols.
        """
        try:
            if symbols is None:
                keys = self.redis_client.keys("last_cumulative:*") if self.redis_client else []
            else:
                keys = [f"last_cumulative:{symbol}" for symbol in symbols]
            if keys:
                self.redis_client.delete(*keys)
            logger.info(f"Reset bucket_incremental_volume session tracking for {len(keys)} symbols")
        except Exception as e:
            logger.warning(f"Error resetting volume session: {e}")

    def validate_volume_consistency(self, symbol: str):
        """Debug helper to check incremental vs cumulative consistency for a symbol."""
        try:
            from datetime import datetime

            # Last cumulative used by data pipeline ingress
            last_cumulative_key = f"last_cumulative:{symbol}"
            last_cumulative = self.redis_client.get(last_cumulative_key) if self.redis_client else None

            # Latest 1min bucket: pick highest minute bucket index for current hour
            now = datetime.now()
            hour = now.hour
            pattern = f"bucket_incremental_volume:bucket_incremental_volume:bucket1:{symbol}:buckets:{hour}:*"
            bucket_inc = None
            if self.redis_client:
                try:
                    keys = self.redis_client.keys(pattern)
                    def _idx(k: str) -> int:
                        try:
                            return int(k.split(":")[-1])
                        except Exception:
                            return -1
                    keys = sorted(keys, key=_idx)
                    if keys:
                        latest_key = keys[-1]
                        data = self.redis_client.hgetall(latest_key)
                        if isinstance(data, dict):
                            if "bucket_incremental_volume" in data:
                                try:
                                    bucket_inc = int(data.get("bucket_incremental_volume", 0))
                                except Exception:
                                    bucket_inc = None
                            elif "bucket_incremental_volume" in data:
                                try:
                                    bucket_inc = int(data.get("bucket_incremental_volume", 0))
                                except Exception:
                                    bucket_inc = None
                except Exception:
                    pass

            # Daily cumulative hash - ✅ USE DatabaseAwareKeyBuilder
            date_str = now.strftime('%Y-%m-%d')
            daily_key = DatabaseAwareKeyBuilder.live_bucket_daily(symbol, date_str)
            daily_cum = None
            try:
                dd = self.redis_client.hgetall(daily_key) if self.redis_client else None
                if isinstance(dd, dict):
                    if "zerodha_cumulative_volume" in dd:
                        try:
                            daily_cum = int(dd.get("zerodha_cumulative_volume", 0))
                        except Exception:
                            daily_cum = None
                    elif "bucket_cumulative_volume" in dd:
                        try:
                            daily_cum = int(dd.get("bucket_cumulative_volume", 0))
                        except Exception:
                            daily_cum = None
            except Exception:
                pass

            result = {
                "last_cumulative": int(last_cumulative) if last_cumulative is not None else None,
                "bucket_incremental": bucket_inc,
                "daily_cumulative": daily_cum,
            }
            logger.info(
                f"[VOLUME_VALIDATE] {symbol} last_cum={result['last_cumulative']} bucket_inc={result['bucket_incremental']} daily_cum={result['daily_cumulative']}"
            )
            return result
        except Exception as e:
            return None

    def pipeline(self):
        """Create a Redis pipeline with automatic reconnection support"""

        class RobustPipeline:
            def __init__(self, parent_client):
                self.parent_client = parent_client
                self.commands = []
                self.redis_pipeline = None

            def _ensure_pipeline(self):
                """Ensure we have a valid pipeline connection"""
                if not self.parent_client.is_connected:
                    self.parent_client._ensure_connection()

                if self.parent_client.is_connected and self.redis_pipeline is None:
                    try:
                        self.redis_pipeline = self.parent_client.redis_client.pipeline()
                    except Exception as e:
                        logger.warning(f"Failed to create pipeline: {e}")
                        return False
                return self.redis_pipeline is not None

            def set(self, key, value, ex=None):
                """Add SET command to pipeline"""
                self.commands.append(("set", key, value, ex))
                return self

            def get(self, key):
                """Add GET command to pipeline"""
                self.commands.append(("get", key))
                return self

            def delete(self, *keys):
                """Add DELETE command to pipeline"""
                self.commands.append(("delete", keys))
                return self

            def publish(self, channel, message):
                """Add PUBLISH command to pipeline"""
                self.commands.append(("publish", channel, message))
                return self

            def zadd(self, key, mapping):
                """Add ZADD command to pipeline"""
                self.commands.append(("zadd", key, mapping))
                return self

            def expire(self, key, time):
                """Add EXPIRE command to pipeline"""
                self.commands.append(("expire", key, time))
                return self

            def execute(self):
                """Execute all commands in the pipeline"""
                if not self.commands:
                    return []

                if not self._ensure_pipeline():
                    # Fallback: execute commands individually
                    logger.warning(
                        "Pipeline unavailable, executing commands individually"
                    )
                    results = []
                    for cmd in self.commands:
                        try:
                            method_name = cmd[0]
                            args = cmd[1:]
                            method = getattr(self.parent_client, method_name)
                            result = method(*args)
                            results.append(result)
                        except Exception as e:
                            logger.error(f"Pipeline fallback command failed: {e}")
                            results.append(None)
                    self.commands = []
                    return results

                # Execute pipeline
                try:
                    # Build pipeline commands
                    if self.redis_pipeline is None:
                        logger.warning("Redis pipeline not available, creating new one")
                        self.redis_pipeline = self.parent_client.redis_client.pipeline() if self.parent_client.redis_client else None
                    for cmd in self.commands:
                        method_name = cmd[0]
                        args = cmd[1:]
                        if self.redis_pipeline is None:
                            logger.warning("Redis pipeline not available, creating new one")
                            self.redis_pipeline = self.parent_client.redis_client.pipeline() if self.parent_client.redis_client else None

                        if method_name == "set":
                            key, value, ex = args
                            if ex:
                                self.redis_pipeline.set(key, value, ex=ex)
                            else:
                                self.redis_pipeline.set(key, value)
                        elif method_name == "get":
                            self.redis_pipeline.get(args[0])
                        elif method_name == "delete":
                            self.redis_pipeline.delete(*args[0])
                        elif method_name == "publish":
                            self.redis_pipeline.publish(*args)
                        elif method_name == "zadd":
                            self.redis_pipeline.zadd(*args)
                        elif method_name == "expire":
                            self.redis_pipeline.expire(*args)

                    # Execute and return results
                    results = self.redis_pipeline.execute()
                    self.parent_client.stats["successful_operations"] += len(
                        self.commands
                    )
                    self.commands = []
                    return results

                except Exception as e:
                    logger.error(f"Pipeline execution failed: {e}")
                    self.parent_client.stats["failed_operations"] += 1

                    # Fallback to individual execution
                    logger.warning("Falling back to individual command execution")
                    results = []
                    for cmd in self.commands:
                        try:
                            method_name = cmd[0]
                            args = cmd[1:]
                            method = getattr(self.parent_client, method_name)
                            result = method(*args)
                            results.append(result)
                        except Exception as e2:
                            logger.error(f"Fallback command failed: {e2}")
                            results.append(None)

                    self.commands = []
                    return results

        return RobustPipeline(self)

    def update_volume_buckets(
        self,
        symbol,
        bucket_incremental_volume,
        timestamp,
        bucket_cumulative_volume=None,
        last_price=None,
    ):
        """Store only incremental bucket_incremental_volume in intraday buckets and mirror cumulative bucket_incremental_volume daily."""
        if not symbol or not (self.is_connected and self.redis_client):
            return False
        
        # Resolve token to symbol BEFORE storing volume buckets
        # CRITICAL: This will return None if resolution fails (prevents UNKNOWN keys)
        # ✅ Delegate to cumulative_tracker if available
        if hasattr(self, 'cumulative_tracker') and self.cumulative_tracker:
            symbol = self.cumulative_tracker._resolve_token_to_symbol_for_storage(symbol)
        else:
            # Simple fallback
            symbol_str = str(symbol) if symbol else ""
            if not symbol_str or symbol_str.startswith("UNKNOWN_") or (symbol_str.isdigit() and ":" not in symbol_str):
                symbol = None
        
        # If resolution failed, skip storage to prevent UNKNOWN keys in Redis
        if not symbol or symbol.startswith("UNKNOWN_"):
            logger.warning(f"⚠️ Skipping volume bucket storage - symbol resolution failed for input: {symbol}")
            return False

        try:
            if isinstance(bucket_incremental_volume, dict):
                volume_payload = bucket_incremental_volume
                bucket_incremental_volume = volume_payload.get("bucket_incremental_volume")
                timestamp = volume_payload.get("timestamp", timestamp)
                bucket_cumulative_volume = volume_payload.get("bucket_cumulative_volume", bucket_cumulative_volume)
                last_price = volume_payload.get("last_price", volume_payload.get("last_traded_price", last_price))

            import pytz
            from datetime import datetime

            def _to_int(value, default=None):
                if value is None:
                    return default
                try:
                    return int(float(value))
                except (TypeError, ValueError):
                    return default

            def _to_float(value):
                if value is None:
                    return None
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None

            volume_int = _to_int(bucket_incremental_volume)
            if volume_int is None:
                logger.error("No incremental bucket_incremental_volume for %s", symbol)
                return False
            if volume_int < 0:
                volume_int = 0

            cumulative_int = _to_int(bucket_cumulative_volume)
            if (
                cumulative_int is not None
                and cumulative_int > 0
                and volume_int > cumulative_int * 0.1
            ):
                logger.warning(
                    "Potential cumulative bucket_incremental_volume used as incremental for %s: %s vs cumulative %s",
                    symbol,
                    volume_int,
                    cumulative_int,
                )

            ist = pytz.timezone("Asia/Kolkata")

            if isinstance(timestamp, datetime):
                ts = timestamp
                if ts.tzinfo is None:
                    ts = ist.localize(ts)
                else:
                    ts = ts.astimezone(ist)
            else:
                epoch_ms = TimestampNormalizer.to_epoch_ms(timestamp)
                ts = datetime.fromtimestamp(epoch_ms / 1000.0, tz=ist)

            price_float = _to_float(last_price)
            bucket_iso_ts = ts.isoformat()

            # ✅ UPDATED: Use shorter canonical volume bucket prefixes
            resolutions = {
                "1min": {
                    "size": 1,
                    "prefix": "vol:bucket:1min",
                    "history": "bucket_incremental_volume:history:1min",
                    "history_len": 6000,
                },
                "2min": {
                    "size": 2,
                    "prefix": "vol:bucket:2min",
                    "history": "bucket_incremental_volume:history:2min",
                    "history_len": 4000,
                },
                "5min": {
                    "size": 5,
                    "prefix": "vol:bucket:5min",
                    "history": "bucket_incremental_volume:history:5min",
                    "history_len": 4000,
                },
                "10min": {
                    "size": 10,
                    "prefix": "vol:bucket:10min",
                    "history": "bucket_incremental_volume:history:10min",
                    "history_len": 3000,
                },
                "15min": {
                    "size": 15,
                    "prefix": "vol:bucket:15min",
                    "history": "bucket_incremental_volume:history:15min",
                    "history_len": 2000,
                },
                "30min": {
                    "size": 30,
                    "prefix": "vol:bucket:30min",
                    "history": "bucket_incremental_volume:history:30min",
                    "history_len": 1000,
                },
                "45min": {
                    "size": 45,
                    "prefix": "vol:bucket:45min",
                    "history": "bucket_incremental_volume:history:45min",
                    "history_len": 800,
                },
                "60min": {
                    "size": 60,
                    "prefix": "vol:bucket:60min",
                    "history": "bucket_incremental_volume:history:60min",
                    "history_len": 600,
                },
            }

            pipe = self.redis_client.pipeline(transaction=False)
            hour = ts.hour

            for label, cfg in resolutions.items():
                bucket_index = ts.minute // cfg["size"]
                bucket_key = f"{cfg['prefix']}:{symbol}:buckets:{hour}:{bucket_index}"

                pipe.hincrby(bucket_key, SESSION_FIELD_BUCKET_INC, volume_int)
                pipe.hincrby(bucket_key, "bucket_incremental_volume", volume_int)
                pipe.hincrby(bucket_key, "count", 1)
                pipe.hsetnx(bucket_key, "first_timestamp", bucket_iso_ts)
                pipe.hset(bucket_key, "last_timestamp", bucket_iso_ts)
                if price_float is not None:
                    pipe.hset(bucket_key, "last_price", price_float)
                    pipe.hset(bucket_key, "close", price_float)
                pipe.expire(bucket_key, 86400)

                history_key = f"{cfg['history']}:{label}:{symbol}"
                history_entry = {
                    "timestamp": bucket_iso_ts,
                    SESSION_FIELD_BUCKET_INC: volume_int,
                    "hour": hour,
                    "bucket_index": bucket_index,
                }
                if cumulative_int is not None:
                    history_entry[SESSION_FIELD_ZERODHA_CUM] = cumulative_int
                if price_float is not None:
                    history_entry["last_price"] = price_float

                pipe.lpush(history_key, json.dumps(history_entry))
                pipe.ltrim(history_key, 0, cfg["history_len"] - 1)
                pipe.expire(history_key, 7 * 86400)

            if cumulative_int is not None:
                date_str = ts.strftime("%Y-%m-%d")
                # ✅ USE DatabaseAwareKeyBuilder for canonical key
                daily_key = DatabaseAwareKeyBuilder.live_bucket_daily(symbol, date_str)
                pipe.hset(daily_key, SESSION_FIELD_ZERODHA_CUM, cumulative_int)
                pipe.hset(daily_key, "bucket_cumulative_volume", cumulative_int)
                pipe.expire(daily_key, 7 * 86400)

            pipe.execute()
            return True

        except Exception as exc:
            return False

    def get_time_based_volume(self, symbol, target_time):
        """Get historical bucket_incremental_volume for specific time"""
        try:
            import pytz
            from datetime import datetime

            ist = pytz.timezone("Asia/Kolkata")
            if isinstance(target_time, (int, float)):
                target_time = datetime.fromtimestamp(target_time)
            if target_time.tzinfo is None:
                target_time = ist.localize(target_time)
            else:
                target_time = target_time.astimezone(ist)

            hour = target_time.hour
            minute_bucket = target_time.minute // 5
            # ✅ USE DatabaseAwareKeyBuilder for canonical key
            bucket_key = DatabaseAwareKeyBuilder.live_bucket_timeslot(symbol, hour, minute_bucket)

            if self.is_connected and self.redis_client:
                data = self.redis_client.hgetall(bucket_key)
                if data and b"bucket_incremental_volume" in data and b"count" in data:
                    bucket_incremental_volume = int(data[b"bucket_incremental_volume"])
                    count = int(data[b"count"])
                    return bucket_incremental_volume / count if count > 0 else None

            return None

        except Exception as e:
            print(f"Time-based bucket_incremental_volume error: {e}")
            return None



    def normalize_volume_field(self, data, field_name='bucket_incremental_volume'):
        """Normalize bucket_incremental_volume field access with backward compatibility"""
        if field_name in data:
            return float(data[field_name] or 0)
        
        # Try legacy field names
        legacy_fields = ['bucket_incremental_volume', 'vol', 'bucket_incremental_volume', 'bucket_incremental_volume', 'zerodha_cumulative_volume', 'zerodha_last_traded_quantity', 'quantity']
        for legacy_field in legacy_fields:
            if legacy_field in data:
                return float(data[legacy_field] or 0)
        
        return 0.0
    
    def info(self, section=None):
        """Get Redis server information"""
        try:
            if section:
                return self.redis_client.info(section)
            return self.redis_client.info()
        except Exception as e:
            logger.error(f"Failed to get Redis info: {e}")
            return {}
    
    def xinfo_stream(self, name):
        """Get stream information"""
        try:
            return self.redis_client.xinfo_stream(name)
        except Exception as e:
            logger.error(f"Failed to get stream info for {name}: {e}")
            return {}
    
    def hgetall(self, name):
        """Get all fields and values from a hash"""
        try:
            # Transform key to new unified structure
            name = self._transform_key(name)
            return self.redis_client.hgetall(name)
        except Exception as e:
            logger.error(f"Failed to get hash {name}: {e}")
            return {}


class CumulativeDataTracker:
    """Specialized tracker for cumulative bucket_incremental_volume and last_price movements with session-based storage"""

    def __init__(self, redis_client):
        self.redis = redis_client
        self.price_history = defaultdict(list)
        self.volume_history = defaultdict(list)
        self.lock = Lock()

        # Session management
        self.current_session = self._get_current_session_date()
        self.session_data = defaultdict(self._create_symbol_session)
    
    @property
    def redis_client(self):
        """Backward compatibility property that points to redis attribute"""
        return self.redis
    
    def _extract_base_symbol(self, symbol: str) -> str:
        """Extract base symbol from F&O contract names"""
        # Ensure symbol is a string (may be int like instrument_token)
        if not isinstance(symbol, str):
            symbol = str(symbol)
        # Remove exchange prefix if present
        if ":" in symbol:
            symbol = symbol.split(":", 1)[1]
        
        import re
        is_option = bool(re.search(r"\d+(CE|PE)$", symbol))
        is_future = symbol.endswith("FUT") or bool(re.search(r"\d{2}[A-Z]{3}", symbol))

        if is_future or is_option:
            expiry_match = re.search(r"\d{2}[A-Z]{3}", symbol)
            if expiry_match:
                symbol = symbol.replace(expiry_match.group(), "")

            if symbol.endswith("FUT"):
                symbol = symbol[:-3]

            if is_option:
                symbol = re.sub(r"\d+(CE|PE)$", "", symbol)

        return symbol.upper()
    
    def validate_volume_data(self, symbol, incremental_volume, cumulative_volume=None):
        """
        Validate that volume data makes logical sense.
        
        Args:
            symbol: Trading symbol for logging
            incremental_volume: Incremental volume to validate
            cumulative_volume: Cumulative volume (optional, for validation)
        
        Returns:
            Validated incremental_volume (may be corrected to 0 if negative)
        """
        if cumulative_volume is not None and incremental_volume > cumulative_volume:
            logger.warning(
                f"Volume validation failed for {symbol}: "
                f"incremental({incremental_volume}) > cumulative({cumulative_volume})"
            )
            # Don't throw exception, just log - system should be resilient to bad data
        
        if incremental_volume < 0:
            logger.warning(f"Negative incremental volume for {symbol}: {incremental_volume}")
            incremental_volume = 0  # Reset to 0
        
        return incremental_volume
    
    def get_time_buckets(self, symbol, session_date=None, hour=None, lookback_minutes=60, start_time=None, end_time=None, use_history_lists=True):
        """Get time buckets for a symbol - consolidated efficient version.
        
        Args:
            symbol: Trading symbol
            session_date: Session date (defaults to current session)
            hour: Optional hour filter
            lookback_minutes: Lookback window in minutes (for compatibility)
            start_time: Optional start timestamp filter (numeric epoch)
            end_time: Optional end timestamp filter (numeric epoch)
            use_history_lists: Use efficient history lists when available (default True)
        
        Returns:
            List of bucket data dictionaries, sorted by timestamp
        """
        if session_date is None:
            session_date = self.current_session

        try:
            # Extract base symbol for F&O contracts
            base_symbol = self._extract_base_symbol(symbol)
            
            buckets = []
            
            # Use history lists for fast retrieval (already maintained during bucket creation)
            if use_history_lists:
                logger.info(f"🔍 [BUCKET_DEBUG] {symbol} -> {base_symbol} - Using history lists")
                resolutions_to_check = ["1min", "2min", "5min", "10min"]
                
                for resolution in resolutions_to_check:
                    try:
                        # Direct key lookup from history list
                        history_key = f"bucket_incremental_volume:history:{resolution}:{symbol}"
                        history_data = self.redis.lrange(history_key, 0, -1)
                        
                        if history_data:
                            logger.info(f"🔍 [BUCKET_DEBUG] Found history list: {history_key} with {len(history_data)} buckets")
                            
                            # Parse history entries and construct bucket keys
                            for entry_str in history_data:
                                try:
                                    entry = json.loads(entry_str)
                                    
                                    # Filter by date using timestamp from history entry if available
                                    if start_time or end_time:
                                        entry_timestamp = None
                                        
                                        # Try to get timestamp from history entry
                                        if 'timestamp' in entry:
                                            entry_timestamp_str = entry['timestamp']
                                            try:
                                                from datetime import datetime
                                                if isinstance(entry_timestamp_str, str):
                                                    if 'T' in entry_timestamp_str:
                                                        entry_dt = datetime.fromisoformat(entry_timestamp_str.replace('Z', '+00:00'))
                                                    else:
                                                        entry_dt = datetime.strptime(entry_timestamp_str.split()[0], '%Y-%m-%d')
                                                    entry_timestamp = entry_dt.timestamp()
                                                else:
                                                    entry_timestamp = float(entry_timestamp_str)
                                            except (ValueError, TypeError):
                                                pass
                                        
                                        # If we have time filters and timestamp, check them
                                        if entry_timestamp:
                                            if start_time and entry_timestamp < start_time:
                                                continue
                                            if end_time and entry_timestamp > end_time:
                                                continue
                                    
                                    hr = entry.get('hour', 0)
                                    bucket_idx = entry.get('bucket_index', 0)
                                    
                                    # ✅ UPDATED: Use shorter canonical volume bucket prefixes
                                    prefix = f"vol:bucket:{resolution}"
                                    
                                    # Construct the actual bucket key
                                    bucket_key = f"{prefix}:{symbol}:{hr:02d}:{bucket_idx:02d}"
                                    
                                    # Get bucket data directly (fast HGETALL)
                                    hash_data = self.redis.hgetall(bucket_key)
                                    if hash_data and b"bucket_incremental_volume" in hash_data:
                                        bucket_timestamp = float(hash_data.get(b"first_timestamp", 0))
                                        
                                        # Apply time filtering if provided (double-check with bucket timestamp)
                                        if start_time and bucket_timestamp < start_time:
                                            continue
                                        if end_time and bucket_timestamp > end_time:
                                            continue
                                        
                                        bucket_data = {
                                            "symbol": symbol,
                                            "session_date": session_date,
                                            "bucket_incremental_volume": int(hash_data.get(b"bucket_incremental_volume", 0)),
                                            "zerodha_cumulative_volume": int(hash_data.get(b"zerodha_cumulative_volume", 0)),
                                            "count": int(hash_data.get(b"count", 0)),
                                            "first_timestamp": bucket_timestamp,
                                            "last_timestamp": float(hash_data.get(b"last_timestamp", 0)),
                                            "last_price": float(hash_data.get(b"last_price", 0))
                                        }
                                        buckets.append(bucket_data)
                                except Exception as e:
                                    continue
                    except Exception as e:
                        # Fall through to pattern matching
                        pass
            
            # ❌ REMOVED: Pattern matching fallback - FORBIDDEN per Redis key standards
            # ✅ STANDARDIZED: Return empty buckets instead of pattern matching
            # If history lists aren't found, buckets should be created by update_volume_buckets() 
            # with proper symbol resolution (no UNKNOWN keys)
            if not buckets:
                # Return empty list - pattern matching is FORBIDDEN
                # Buckets will be created as new ticks arrive with proper symbol resolution
                return []
            
            # Removed all pattern matching code - violates Redis key standards
            # Historical note: Previous code used .keys() with patterns, which is O(N) blocking
            # New code relies on history lists (direct lookups) or returns empty (wait for new data)
            
            # Return buckets found from history lists (if any)
            return buckets

        except Exception as e:
            logger.error(f"Failed to get time buckets for {symbol}: {e}")
            return []

    def _get_current_session_date(self, timestamp=None):
        """Get current trading session date with timezone awareness"""
        if timestamp:
            return date.fromtimestamp(timestamp).isoformat()
        return date.today().isoformat()
    
    def _validate_session_date(self, symbol, session_date):
        """Validate session date and handle timezone corrections"""
        try:
            current_date = date.today().isoformat()
            
            # If session date is different from current date, check if it's a valid trading day
            if session_date != current_date:
                # Check if it's a weekend or holiday (basic check)
                from datetime import datetime
                session_datetime = datetime.fromisoformat(session_date)
                
                # Weekend check (Saturday = 5, Sunday = 6)
                if session_datetime.weekday() >= 5:
                    logger.warning(f"Session date {session_date} is weekend for {symbol}, using current date")
                    return current_date
                
                # If session date is in the future, use current date
                if session_datetime.date() > date.today():
                    logger.warning(f"Session date {session_date} is in future for {symbol}, using current date")
                    return current_date
                    
            return session_date
        except Exception as e:
            logger.error(f"Session date validation failed for {symbol}: {e}")
            return date.today().isoformat()

    def _resolve_token_to_symbol(self, symbol):
        """Resolve token to proper symbol format"""
        try:
            symbol_str = str(symbol)
            # Check if symbol needs resolution
            # Handle: "TOKEN_12345", "UNKNOWN_12345", or "12345"
            token_str = None
            if symbol_str.startswith("TOKEN_"):
                token_str = symbol_str.replace("TOKEN_", "")
            elif symbol_str.startswith("UNKNOWN_"):
                token_str = symbol_str.replace("UNKNOWN_", "")
            elif symbol_str.isdigit():
                token_str = symbol_str
            
            if token_str:
                try:
                    token = int(token_str)
                    # ✅ Use InstrumentMapper - loads from token_lookup_enriched.json (expected 250K+ instruments)
                    # This is the same file used by metadata_resolver and covers all intraday_crawler tokens
                    # from crawlers/binary_crawler1/binary_crawler1.json (246 tokens)
                    from zerodha.crawlers.hot_token_mapper import get_hot_token_mapper
                    instrument_mapper = get_hot_token_mapper()
                    resolved = instrument_mapper.token_to_symbol(token)
                    
                    # Only return if it's not still UNKNOWN
                    if resolved and not resolved.startswith("UNKNOWN_"):
                        return resolved
                except (ValueError, ImportError) as e:
                    pass
        except Exception as e:
            pass

        return None

    def _create_symbol_session(self):
        """Create session data structure for each symbol"""
        return {
            "bucket_cumulative_volume": 0,
            "first_price": None,
            "last_price": None,
            "high": float("-inf"),
            "low": float("inf"),
            "update_count": 0,
            "first_update": None,
            "last_update": None,
            "session_date": self.current_session,
            "time_buckets": defaultdict(dict),  # hour:minute_bucket -> data
        }

    def _normalize_symbol(self, symbol):
        symbol_str = str(symbol)
        resolved_symbol = self._resolve_token_to_symbol(symbol_str)
        return resolved_symbol or symbol_str

    # ✅ REMOVED: _get_session_data - session logic removed

    # ✅ REMOVED: _persist_session_data - session logic removed

    def update_symbol_data(
        self, symbol, last_price, bucket_incremental_volume, timestamp=None, depth_data=None, bucket_cumulative_volume=None
    ):
        """
        Update symbol session data with volume information.
        Args:
        ---------
        symbol: Trading symbol
        last_price: Last traded price
        bucket_incremental_volume: Incremental volume for the bucket
        timestamp: Event timestamp
        depth_data: Order book depth data
        bucket_cumulative_volume: Cumulative volume (optional, will be fetched if not provided)
        """
        if timestamp is None:
            timestamp = time.time()

        symbol = self._normalize_symbol(symbol)

        # Validate volume data
        bucket_incremental_volume = self.validate_volume_data(
            symbol, bucket_incremental_volume, bucket_cumulative_volume
        )

        # ✅ REMOVED: Session logic - session data management removed
        # Store last_price and bucket_incremental_volume in local history only
        with self.lock:
            self.price_history[symbol].append((timestamp, last_price))
            self.volume_history[symbol].append((timestamp, bucket_incremental_volume))

            # Keep only recent history to prevent memory bloat
            if len(self.price_history[symbol]) > 1000:
                self.price_history[symbol] = self.price_history[symbol][-500:]
            if len(self.volume_history[symbol]) > 1000:
                self.volume_history[symbol] = self.volume_history[symbol][-500:]

            # ✅ REMOVED: Session persistence and time bucket storage
            return True

    def _store_time_bucket(self, symbol, last_price, bucket_incremental_volume, timestamp, depth_data=None):
        """Store data in time buckets for pattern analysis with order book data"""
        # ✅ REMOVED: Session and bucket window logic - method disabled
        return

    def get_cumulative_data(self, symbol, session_date=None):
        """Get cumulative data for a symbol with in-memory fallback and field validation"""
        if session_date is None:
            session_date = self.current_session

        # ✅ REMOVED: Session logic - in-memory session data removed

        # Otherwise, fetch from Redis using standardized key builder
        try:
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            session_key = DatabaseAwareKeyBuilder.live_session(symbol, session_date)
            data = self.redis.get(session_key)
            if data:
                parsed_data = json.loads(data)
                return self._validate_and_normalize_session_data(symbol, parsed_data)
            return None
        except Exception as e:
            logger.error(f"Error fetching cumulative data for {symbol}: {e}")
            return None
    
    def store_time_bucket(self, symbol, bucket_data):
        """Store a time bucket for a symbol"""
        try:
            # Create bucket key
            session_date = self.current_session
            hour = bucket_data.get("hour", 0)
            minute = bucket_data.get("minute", 0)
            # ✅ UPDATED: Use shorter canonical volume bucket pattern
            bucket_key = f"vol:bucket:{symbol}:{session_date}:{hour:02d}:{minute:02d}"
            
            # Store bucket data as JSON
            self.redis.setex(bucket_key, 86400, json.dumps(bucket_data))  # 24 hour TTL
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to store time bucket for {symbol}: {e}")
            return False
    
    # ✅ REMOVED: _validate_and_normalize_session_data - session logic removed

    def update_symbol_data_direct(
        self,
        symbol,
        bucket_cumulative_volume,
        last_price,
        timestamp,
        high=None,
        low=None,
        open_price=None,
        close_price=None,
        depth_data=None,
    ):
        """Store raw cumulative bucket_incremental_volume as-is from Zerodha WebSocket (no calculations)"""
        if timestamp is None:
            timestamp = time.time()

        symbol = self._normalize_symbol(symbol)

        # ✅ REMOVED: Session logic - session data management removed
        with self.lock:
            # Store last_price and bucket_cumulative_volume in local history only
            self.price_history[symbol].append((timestamp, last_price))
            self.volume_history[symbol].append((timestamp, bucket_cumulative_volume))
            
            # Keep only recent history to prevent memory bloat
            if len(self.price_history[symbol]) > 1000:
                self.price_history[symbol] = self.price_history[symbol][-500:]
            if len(self.volume_history[symbol]) > 1000:
                self.volume_history[symbol] = self.volume_history[symbol][-500:]

            # ✅ REMOVED: Session persistence and time bucket storage
            return bucket_cumulative_volume

    def _store_time_bucket_direct(
        self, symbol, bucket_cumulative_volume, timestamp, last_price, previous_cumulative=None, depth_data=None
    ):
        """Store raw cumulative bucket_incremental_volume in time buckets (no calculations)"""
        if bucket_cumulative_volume <= 0:
            return
        
        # ✅ FIXED: Calculate incremental volume from cumulative
        # ✅ REMOVED: Session logic - session data lookup removed
        if previous_cumulative is None:
            previous_cumulative = 0  # Default fallback
        
        # Calculate incremental: current - previous (only if current > previous)
        if bucket_cumulative_volume > previous_cumulative:
            bucket_incremental_volume = bucket_cumulative_volume - previous_cumulative
        else:
            # Session reset or data anomaly - use 0 to avoid negative increments
            bucket_incremental_volume = 0
        
        # Only store if there's actual incremental volume
        if bucket_incremental_volume > 0:
            self._store_time_bucket(symbol, last_price, bucket_incremental_volume, timestamp, depth_data)

    def get_session_price_movement(self, symbol, minutes_back=30):
        """Get last_price movement analysis for recent period"""
        if symbol not in self.price_history:
            return None

        current_time = time.time()
        lookback_time = current_time - (minutes_back * 60)

        # Get recent last_price data
        recent_prices = [
            (ts, last_price)
            for ts, last_price in self.price_history[symbol]
            if ts >= lookback_time
        ]

        if len(recent_prices) < 2:
            return None

        start_price = recent_prices[0][1]
        end_price = recent_prices[-1][1]
        high = max(last_price for _, last_price in recent_prices)
        low = min(last_price for _, last_price in recent_prices)

        return {
            "symbol": symbol,
            "period_minutes": minutes_back,
            "start_price": start_price,
            "end_price": end_price,
            "high": high,
            "low": low,
            "price_change": end_price - start_price,
            "price_change_pct": ((end_price - start_price) / start_price) * 100
            if start_price > 0
            else 0,
            "data_points": len(recent_prices),
        }

    def get_pattern_data(self, symbol, time_of_day=None, session_date=None):
        """
        Get pattern data for a symbol based on time of day
        Returns aggregated data for pattern recognition
        """
        buckets = self.get_time_buckets(symbol, session_date)

        if not buckets:
            return None

        # Filter by time of day if specified (e.g., "10:30")
        if time_of_day:
            try:
                hour, minute = map(int, time_of_day.split(":"))
                minute_bucket = minute // 5 * 5
                filtered = [
                    b
                    for b in buckets
                    if b["hour"] == hour and b["minute_bucket"] == minute_bucket
                ]
                buckets = filtered if filtered else buckets
            except ValueError:
                pass  # Invalid time format, use all buckets

        if not buckets:
            return None

        # Calculate pattern metrics
        bucket_incremental_volume = sum(b["bucket_incremental_volume"] for b in buckets)
        avg_price = sum(b["close"] for b in buckets) / len(buckets)
        price_volatility = max(b["high"] for b in buckets) - min(
            b["low"] for b in buckets
        )
        volume_volatility = (
            np.std([b["bucket_incremental_volume"] for b in buckets])
            if NUMPY_AVAILABLE and len(buckets) > 1
            else 0
        )

        return {
            "symbol": symbol,
            "session_date": session_date or self.current_session,
            "bucket_incremental_volume": bucket_incremental_volume,
            "average_price": avg_price,
            "price_volatility": price_volatility,
            "volume_volatility": volume_volatility,
            "time_buckets": buckets,
            "bucket_count": len(buckets),
            "price_trend": "UP"
            if buckets[-1]["close"] > buckets[0]["open"]
            else "DOWN",
            "volume_trend": "HIGH"
            if bucket_incremental_volume > sum(b["bucket_incremental_volume"] for b in buckets[:-1])
            else "NORMAL",
        }

    def cleanup_old_sessions(self, days_to_keep=7):
        """Clean up old session data"""
        try:
            cutoff_date = date.today() - timedelta(days=days_to_keep)
            cutoff_iso = cutoff_date.isoformat()

            # Clean up old session keys
            session_pattern = "session:*:*"
            session_keys = self.redis.keys(session_pattern)

            for key in session_keys:
                try:
                    key_parts = key.split(":")
                    if len(key_parts) >= 3:
                        session_date = key_parts[2]
                        if session_date < cutoff_iso:
                            self.redis.delete(key)
                except:
                    pass

            # Clean up old bucket keys - ✅ UPDATED: Match new canonical pattern
            bucket_pattern = "vol:bucket:*:*:*:*"
            bucket_keys = self.redis.keys(bucket_pattern)

            for key in bucket_keys:
                try:
                    key_parts = key.split(":")
                    if len(key_parts) >= 4:
                        session_date = key_parts[2]
                        if session_date < cutoff_iso:
                            self.redis.delete(key)
                except:
                    pass

            logger.info(f"Cleaned up sessions older than {cutoff_iso}")

        except Exception as e:
            logger.error(f"Failed to cleanup old sessions: {e}")


# ============================================
# Modern Redis Client with RESP3, Retry/Backoff, Circuit Breaker
# ============================================

T = TypeVar("T")

# Ensure os and threading are imported (needed for REDIS_DEFAULTS and _pool_lock)
# These are already imported at module top, but ensure availability here
import os  # Already imported, but explicit for clarity
import threading  # Already imported, but explicit for clarity

# -------- Defaults tuned for Redis 8.2 and high-throughput intraday workloads --------
REDIS_DEFAULTS = {
    "host": os.getenv("REDIS_HOST", "127.0.0.1"),
    "port": int(os.getenv("REDIS_PORT", "6379")),
    "db": int(os.getenv("REDIS_DB_DEFAULT", "0")),
    "username": os.getenv("REDIS_USERNAME"),
    "password": os.getenv("REDIS_PASSWORD"),
    "ssl": os.getenv("REDIS_SSL", "false").lower() == "true",
    # Timeouts (seconds): matched to Zerodha WebSocket configuration
    # Zerodha intraday crawler uses 10s connection_timeout, standard client uses 15s
    "socket_connect_timeout": float(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "5.0")),
    "socket_timeout": float(os.getenv("REDIS_SOCKET_TIMEOUT", "10.0")),  # ✅ Matched to Zerodha (10s for intraday)
    # RESP3 for richer types & better INFO/Streams parsing issues on Redis 8.x
    "protocol": int(os.getenv("REDIS_PROTOCOL", "3")),
    # Health/probing
    "health_check_interval": int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "15")),
    "client_name": os.getenv("REDIS_CLIENT_NAME", "intraday-core"),
    # Pool sizing: bursty tick traffic; tweak for your cores / process count
    "max_connections": int(os.getenv("REDIS_MAX_CONNECTIONS", "256")),
    # Retry helpers (first-class in redis-py)
    "retry": Retry(ExponentialBackoff(cap=3.0, base=0.05), retries=int(os.getenv("REDIS_RETRIES", "5"))),
    "retry_on_error": (
        ConnectionError,
        RedisTimeoutError,
        BusyLoadingError,
    ),
    "retry_on_timeout": True,
    # Keepalive - REMOVED: deprecated in Redis 8.2
    # "socket_keepalive": True,
    # decode_responses: default True; auto-decode bytes to strings for all consumers
    "decode_responses": True,
}

# ---------------- Singletons ----------------
_pool_lock = threading.Lock()
_modern_pool: Optional[redis.ConnectionPool] = None
_modern_sync_client: Optional[redis.Redis] = None

# ---------------- Circuit Breaker ----------------
@dataclass
class Circuit:
    threshold: int = int(os.getenv("REDIS_CB_THRESHOLD", "8"))
    cooldown: float = float(os.getenv("REDIS_CB_COOLDOWN_SEC", "5.0"))
    failures: int = 0
    opened_at: float = 0.0

    def allow(self) -> bool:
        """Why: reject fast when Redis is flapping; half-open after cooldown"""
        if self.opened_at <= 0:
            return True
        if (time.time() - self.opened_at) >= self.cooldown:
            return True
        return False

    def mark_failure(self) -> None:
        self.failures += 1
        if self.failures >= self.threshold:
            self.opened_at = time.time()

    def mark_success(self) -> None:
        self.failures = 0
        self.opened_at = 0.0

_modern_cb = Circuit()

def _build_modern_pool(overrides: dict | None = None) -> redis.ConnectionPool:
    """Build modern Redis connection pool with RESP3 and retry/backoff"""
    cfg = REDIS_DEFAULTS.copy()
    if overrides:
        cfg.update(overrides)
    # Note: redis.ConnectionPool doesn't support all kwargs; filter unsupported ones
    pool_kwargs = {
        "host": cfg["host"],
        "port": cfg["port"],
        "db": cfg["db"],
        "username": cfg.get("username"),
        "password": cfg.get("password"),
        "socket_connect_timeout": cfg["socket_connect_timeout"],
        "socket_timeout": cfg["socket_timeout"],
        "max_connections": cfg["max_connections"],
        # "socket_keepalive": cfg["socket_keepalive"],  # REMOVED - Deprecated in Redis 8.2
        "health_check_interval": cfg["health_check_interval"],
        "client_name": cfg["client_name"],
    }
    # Remove None values
    pool_kwargs = {k: v for k, v in pool_kwargs.items() if v is not None}
    
    # SSL and protocol are handled at connection level, not pool level
    pool = redis.ConnectionPool(**pool_kwargs)
    return pool

def get_sync_modern(overrides: dict | None = None) -> redis.Redis:
    """
    Return a process-wide Redis client with robust retry/backoff and pooling.
    Note: decode_responses belongs to client, not pool. Each call with different decode_responses
    may need a separate client instance.
    """
    cfg = REDIS_DEFAULTS.copy()
    if overrides:
        cfg.update(overrides)
    
    # Build pool without decode_responses (pool doesn't support it)
    pool_overrides = {k: v for k, v in (overrides or {}).items() if k != "decode_responses"}
    global _modern_pool
    with _pool_lock:
        if _modern_pool is None or (overrides and any(k in pool_overrides for k in ["db", "host", "port"])):
            _modern_pool = _build_modern_pool(pool_overrides)
    
    # Return client with decode_responses set appropriately
    # SSL and protocol are set at client level if supported
    client_kwargs = {
        "connection_pool": _modern_pool,
        "retry": cfg["retry"],
        "retry_on_error": cfg["retry_on_error"],
        "retry_on_timeout": cfg["retry_on_timeout"],
        "decode_responses": bool(cfg.get("decode_responses", False)),
    }
    # Add protocol if supported (RESP3) - some redis-py versions support it
    if cfg.get("protocol"):
        try:
            client_kwargs["protocol"] = cfg["protocol"]
        except Exception:
            pass  # Ignore if protocol not supported in this redis-py version
    
    return redis.Redis(**client_kwargs)  # type: ignore[return-value]

# ---------------- Retry helper for idempotent ops ----------------
def retryable(fn: Callable[..., T]) -> Callable[..., T]:
    """Adds CB + lightweight retry for idempotent calls (GET, HGET, etc.)."""
    def wrapper(*args: Any, **kwargs: Any) -> T:
        attempts = 0
        last_err: Optional[Exception] = None
        while attempts < 3:
            if not _modern_cb.allow():
                raise ConnectionError("Redis circuit open; fast-failing calls")
            try:
                res = fn(*args, **kwargs)
                _modern_cb.mark_success()
                return res
            except (ConnectionError, RedisTimeoutError, BusyLoadingError) as e:
                last_err = e
                attempts += 1
                _modern_cb.mark_failure()
                time.sleep(0.05 * attempts)  # small extra delay on top of redis-py retry
        # bubble the last error with context
        raise last_err or ConnectionError("Unknown Redis error")
    return wrapper

# ---------------- Stream / PubSub helpers ----------------
@retryable
def xadd_safe(
    key: str,
    fields: dict[str, Any],
    maxlen: Optional[int] = None,
    approximate: bool = True,
    trim_strategy: str = "MAXLEN",
) -> str:
    """
    Safe XADD with optional stream trimming.
    Why: prevent unbounded growth causing memory pressure and slow restarts.
    
    Storage Location:
    - Database: DB 0 (system) - uses get_sync_modern() default connection
    - Stream Key: key (as provided)
    
    Data Format/Signature:
    - Input: fields (dict) with field->value pairs
    - Value Types: Any (will be converted to strings by Redis)
    - Stored Format: Redis Stream entry with fields as-is (all values as strings)
    - Field names and values are preserved exactly as provided
    
    Args:
        key: Redis stream key name
        fields: Dictionary of field->value pairs
        maxlen: Maximum stream length before trimming (optional)
        approximate: Use approximate trimming if maxlen set (default: True)
        trim_strategy: Trimming strategy (default: "MAXLEN")
        
    Returns:
        Stream entry ID (string)
    """
    r = get_sync_modern()
    kwargs: dict[str, Any] = {}
    if maxlen:
        kwargs["maxlen"] = maxlen
        kwargs["approximate"] = approximate
        kwargs["trim_strategy"] = trim_strategy
    return r.xadd(key, fields, **kwargs)  # type: ignore[return-value]

@retryable
def create_consumer_group_if_needed(stream: str, group: str, mkstream: bool = True) -> None:
    """Idempotent create of consumer group."""
    r = get_sync_modern()
    try:
        r.xgroup_create(name=stream, groupname=group, id="0", mkstream=mkstream)
    except ResponseError as e:
        # BUSYGROUP is fine: group exists
        if "BUSYGROUP" not in str(e):
            raise

# ============================================================================
# Redis Connection Managers (from redis_manager.py)
# ============================================================================

class RedisManager82:
    """
    ⚠️ DEPRECATED: Use UnifiedRedisManager instead.
    
    Migration:
        OLD: RedisManager82.get_client(process_name="x", db=1)
        NEW: UnifiedRedisManager.get_client(process_name="x", db=1)
    
    Features:
    - Process-specific connection pools
    - Redis 8.2 health check intervals
    - Automatic client naming (process_PID_timestamp)
    - Connection cleanup methods
    - Thread-safe pool management
    """
    
    _pools: Dict[str, Dict[int, redis.ConnectionPool]] = {}  # process_name -> {db -> pool}
    _lock = threading.Lock()
    
    @classmethod
    def get_client(
        cls, 
        process_name: str, 
        db: int = 0,
        max_connections: int = None,  # ✅ SOLUTION 4: Default to None to force config lookup
        host: str = 'localhost',
        port: int = 6379,
        password: Optional[str] = None,
        decode_responses: bool = True
    ) -> redis.Redis:
        """
        Get a Redis client with process-specific connection pool.
        
        ✅ SOLUTION 4: Uses PROCESS_POOL_CONFIG for optimal connection pool sizes.
        Connection pools are cached per process+db to enable connection reuse.
        
        Args:
            process_name: Unique identifier for the process
            db: Redis database number (default: 0)
            max_connections: Max connections for this pool (if None, uses PROCESS_POOL_CONFIG)
            host: Redis host (default: 'localhost')
            port: Redis port (default: 6379)
            password: Redis password (optional)
            decode_responses: If True, decode responses to strings (default: True)
            
        Returns:
            redis.Redis: Redis client instance with process-specific pool
        """
        # Check for environment or config overrides
        host = os.getenv('REDIS_HOST', host)
        port = int(os.getenv('REDIS_PORT', port))
        password = os.getenv('REDIS_PASSWORD') or password
        
        # ✅ SOLUTION 4: Get optimized pool size from PROCESS_POOL_CONFIG
        # Priority: PROCESS_POOL_CONFIG > provided max_connections > default (10)
        if max_connections is None:
            try:
                # ✅ PROCESS_POOL_CONFIG imported from redis_config.py (single source of truth)
                max_connections = PROCESS_POOL_CONFIG.get(
                    process_name, 
                    PROCESS_POOL_CONFIG.get('default', 10)
                )
            except Exception:
                max_connections = 10  # Fallback default
        else:
            # If max_connections provided, still check config for override
            try:
                # ✅ PROCESS_POOL_CONFIG imported from redis_config.py (single source of truth)
                config_max = PROCESS_POOL_CONFIG.get(process_name)
                if config_max is not None:
                    max_connections = config_max  # Use config value (overrides provided)
            except Exception:
                pass  # Use provided max_connections if config lookup fails
        
        # Thread-safe pool creation/caching
        with cls._lock:
            if process_name not in cls._pools:
                cls._pools[process_name] = {}
            
            if db not in cls._pools[process_name]:
                # ✅ FIXED: redis-py 7.1+ with hiredis 3.x auto-detects hiredis parser
                # No need to set parser_class - causes socket_read_size error
                try:
                    import hiredis
                    logger.info(f"✅ hiredis {getattr(hiredis, '__version__', 'unknown')} available (auto-detected, db={db})")
                except ImportError:
                    logger.debug(f"⚠️ hiredis not available, using default parser (db={db})")

                # Create process-specific pool for this db
                pool_config = {
                    'host': host,
                    'port': port,
                    'db': db,
                    'password': password,
                    'max_connections': max_connections,
                    # 'socket_keepalive': True,  # REMOVED - Deprecated in Redis 8.2
                    'retry_on_timeout': True,
                    'health_check_interval': 30,  # Redis 8.2 enhancement
                    'socket_timeout': 10,
                    'socket_connect_timeout': 5,
                }
                
                # Remove None values
                pool_config = {k: v for k, v in pool_config.items() if v is not None}
                
                pool = ConnectionPool(**pool_config)
                cls._pools[process_name][db] = pool
        
        # Get cached pool
        pool = cls._pools[process_name][db]
        
        # Create client with connection pool
        client = redis.Redis(
            connection_pool=pool,
            decode_responses=decode_responses
        )
        
        # Set descriptive client name (process_PID) for monitoring
        try:
            client_id = f"{process_name}_{os.getpid()}"
            client.client_setname(client_id)
        except Exception:
            pass  # Ignore errors setting name
        
        return client
    
    @classmethod
    def cleanup(cls, process_name: Optional[str] = None):
        """
        Cleanup connection pools.
        
        Args:
            process_name: If provided, cleanup only this process's pools.
                         If None, cleanup all pools.
        """
        with cls._lock:
            if process_name:
                # Cleanup specific process
                if process_name in cls._pools:
                    for pool in cls._pools[process_name].values():
                        try:
                            pool.disconnect()
                        except Exception:
                            pass
                    del cls._pools[process_name]
            else:
                # Cleanup all processes
                for process_pools in cls._pools.values():
                    for pool in process_pools.values():
                        try:
                            pool.disconnect()
                        except Exception:
                            pass
                cls._pools.clear()
    
    @classmethod
    def get_pool_stats(cls) -> Dict:
        """Get statistics about connection pools"""
        with cls._lock:
            stats = {}
            for process_name, db_pools in cls._pools.items():
                stats[process_name] = {
                    'databases': list(db_pools.keys()),
                    'total_pools': len(db_pools),
                    'total_connections': sum(
                        pool.connection_kwargs.get('max_connections', 0) 
                        for pool in db_pools.values()
                    )
                }
            return stats


class UnifiedRedisManager:
    """
    ✅ FIXED: Process-wide connection pool sharing
    All components in a worker process share ONE pool
    """
    
    # ✅ Process-level pool registry (shared across all instances in same process)
    _process_pools: Dict[str, redis.ConnectionPool] = {}
    _redis_clients: Dict[str, redis.Redis] = {}
    _pool_lock = threading.Lock()
    
    @classmethod
    def get_client(cls, 
                   process_name: Optional[str] = None,
                   **kwargs) -> redis.Redis:
        """
        Centralized entry point for Redis clients with process-wide pooling.
        """
        # Merge with base Redis 8 configuration
        db = kwargs.get('db', 1)
        
        base_config = DEFAULT_STRING_REDIS_CONFIG.copy()
        base_config.update(kwargs)
        
        # Enforce critical settings
        base_config['db'] = db
        if 'decode_responses' not in base_config:
            base_config['decode_responses'] = True

        socket_keepalive_options = base_config.pop('socket_keepalive_options', None)
        base_config.pop('socket_keepalive', None)
        
        # Force hiredis if available
        if HIREDIS_AVAILABLE:
            base_config['parser_class'] = HiredisParser
        elif 'parser_class' in base_config:
            base_config.pop('parser_class')

        # Respect PROCESS_POOL_CONFIG for max connections (string connection)
        pool_limits = PROCESS_POOL_CONFIG.get(process_name or 'default', PROCESS_POOL_CONFIG.get('default'))
        if isinstance(pool_limits, dict):
            pool_max = pool_limits.get('string') or pool_limits.get('max')
        else:
            pool_max = pool_limits
        if pool_max is not None and 'max_connections' not in kwargs:
            base_config['max_connections'] = pool_max
        
        # Build pool key based on connection parameters (NOT process_name)
        unix_socket_path = base_config.get('unix_socket_path')
        unix_socket_path_for_key = unix_socket_path if unix_socket_path else ''
        pool_key = f"{base_config.get('host', 'localhost')}:{base_config.get('port', 6379)}:{db}:{unix_socket_path_for_key}"
        
        with cls._pool_lock:
            # Get or create process-wide pool
            if pool_key not in cls._process_pools:
                pool_creation_kwargs = base_config.copy()
                
                # Handle Unix sockets
                if unix_socket_path and os.path.exists(unix_socket_path):
                    pool_creation_kwargs['path'] = unix_socket_path
                    pool_creation_kwargs['connection_class'] = redis.connection.UnixDomainSocketConnection
                    pool_creation_kwargs.pop('host', None)
                    pool_creation_kwargs.pop('port', None)
                    pool_creation_kwargs.pop('unix_socket_path', None)
                else:
                    if unix_socket_path:
                        logging.getLogger(__name__).warning(
                            f"⚠️ Unix socket not found at {unix_socket_path}, falling back to TCP {base_config.get('host', 'localhost')}:{base_config.get('port', 6379)}"
                        )
                    pool_creation_kwargs['connection_class'] = redis.connection.Connection
                    pool_creation_kwargs.pop('unix_socket_path', None)

                cls._process_pools[pool_key] = redis.ConnectionPool(**pool_creation_kwargs)
                logging.getLogger(__name__).info(
                    f"✅ Created process-wide Redis pool: {pool_key} "
                    f"(max_connections={pool_creation_kwargs.get('max_connections', 50)})"
                )
            
            pool = cls._process_pools[pool_key]
        
        client_key = f"{pool_key}:{process_name or 'default'}:{base_config.get('decode_responses', True)}"
        if client_key not in cls._redis_clients:
            cls._redis_clients[client_key] = redis.Redis(connection_pool=pool)
        return cls._redis_clients[client_key]
    
    @classmethod
    def cleanup(cls):
        """Cleanup all connection pools in this process."""
        with cls._pool_lock:
            for pool in cls._process_pools.values():
                try:
                    pool.disconnect()
                except Exception:
                    pass
            cls._process_pools.clear()
    
    @staticmethod
    def get_pool_stats() -> Dict:
        """Get pool statistics from the connection manager."""
        manager = UnifiedRedisManager._get_manager()
        return manager.get_stats()


class RedisSingletonManager:
    """Compatibility singleton that mirrors the legacy redis_manager behavior."""

    def __init__(self):
        self._client: Optional[redis.Redis] = None
        self._lock = Lock()
        self._max_connections = int(os.getenv("REDIS_MAX_CONNECTIONS", "50"))

    def _initialize(self):
        if self._client:
            return
        with self._lock:
            if self._client:
                return

            host = os.getenv("REDIS_HOST", "localhost")
            port = int(os.getenv("REDIS_PORT", 6379))
            password = os.getenv("REDIS_PASSWORD") or None

            self._client = UnifiedRedisManager.get_client(
                process_name="redis_manager_singleton",
                db=1,
                host=host,
                port=port,
                password=password,
                max_connections=self._max_connections,
                decode_responses=True,
            )

            try:
                client_id = f"redis_connection_manager_{os.getpid()}"
                self._client.client_setname(client_id)
            except Exception:
                pass

    def get_client(self) -> redis.Redis:
        if not self._client:
            self._initialize()
        return self._client

    def get_connection_info(self) -> Dict[str, Any]:
        client = self.get_client()
        try:
            info = client.info("clients")
            pool = client.connection_pool
            available = len(getattr(pool, "_available_connections", [])) if hasattr(pool, "_available_connections") else 0
            created = getattr(pool, "_created_connections", 0)
            return {
                "connected_clients": info.get("connected_clients", 0),
                "blocked_clients": info.get("blocked_clients", 0),
                "max_connections": self._max_connections,
                "pool_size": pool.max_connections,
                "pool_available": available,
                "pool_created": created,
            }
        except Exception as exc:
            return {"error": f"Could not fetch connection info: {exc}"}

    def cleanup(self):
        if self._client:
            try:
                self._client.connection_pool.disconnect()
            except Exception:
                pass
            finally:
                self._client = None


# GLOBAL INSTANCE - everyone uses this singleton
redis_manager = RedisSingletonManager()

# GLOBAL INSTANCE - every file uses this singleton for DB 1 access when needed
try:
    redis_manager.get_client()
except Exception as e:
    logger.warning(f"⚠️ Failed to initialize Redis singleton client: {e}")

# ============================================================================
# Compatibility Functions
# ============================================================================

def get_redis_client(
    process_name: str = "default",
    *,
    db: Optional[int] = None,
    decode_responses: bool = True,
    **redis_kwargs: Any,
) -> redis.Redis:
    """
    ✅ CENTRALIZED POOLING: All clients now use UnifiedRedisManager.
    No more unbounded connection creation.
    
    Args:
        process_name: Identifier for the caller (for pool accounting).
        db: Redis database number (defaults to 1 if not provided).
        decode_responses: Whether to decode bytes to str automatically.
        **redis_kwargs: Additional overrides (host, port, unix_socket_path, etc.).
    """
    # Force DB 1 if none specified (standard for AION data)
    db_num = db if db is not None else 1
    
    # Use UnifiedRedisManager to ensure process-level pooling
    # This prevents connection exhaustion under high load
    return UnifiedRedisManager.get_client(
        process_name=process_name,
        db=db_num,
        decode_responses=decode_responses,
        **redis_kwargs,
    )


def publish_to_redis(stream_name: str, data: Dict[str, Any], maxlen: int = 10000) -> Optional[str]:
    """
    Compatibility function to publish data to Redis stream.
    
    Args:
        stream_name: Name of the Redis stream
        data: Data dictionary to publish
        maxlen: Maximum length of the stream (default: 10000)
    
    Returns:
        Stream entry ID if successful, None otherwise
    """
    try:
        client = get_redis_client(process_name="redis_stream_publisher", db=1)
        return client.xadd(stream_name, data, maxlen=maxlen)
    except Exception as e:
        logger.error(f"Failed to publish to Redis stream {stream_name}: {e}")
        return None

# ============================================================================
# RedisClientFactory - Compatibility wrapper for optimized_main.py
# ============================================================================

class RedisClientFactory:
    """
    Factory class for getting Redis clients for different databases.
    Provides compatibility interface for optimized_main.py and other dashboard code.
    """
    
    @staticmethod
    def get_system_client(process_name: str = "dashboard") -> redis.Redis:
        """
        Get Redis client for system database (DB 0).
        
        Used for:
        - User accounts and authentication
        - System configuration
        - Forward validation results
        
        Args:
            process_name: Process identifier for connection pool
            
        Returns:
            redis.Redis: Redis client connected to DB 0
        """
        return UnifiedRedisManager.get_client(process_name=process_name, db=0)
    
    @staticmethod
    def get_trading_client(process_name: str = "dashboard") -> redis.Redis:
        """
        Get Redis client for trading database (DB 1).
        
        Used for:
        - Live ticks, OHLC data
        - Volume data, indicators
        - Alerts stream
        - Real-time trading operations
        
        Args:
            process_name: Process identifier for connection pool
            
        Returns:
            redis.Redis: Redis client connected to DB 1
        """
        return UnifiedRedisManager.get_client(process_name=process_name, db=1)
    
    @staticmethod
    def get_analytics_client(process_name: str = "dashboard") -> redis.Redis:
        """
        Get Redis client for analytics database (DB 2).
        
        Used for:
        - Pattern metrics
        - Historical analytics
        - Volume profile data
        
        Args:
            process_name: Process identifier for connection pool
            
        Returns:
            redis.Redis: Redis client connected to DB 2
        """
        return UnifiedRedisManager.get_client(process_name=process_name, db=2)
    
