"""
Redis 8.0 Configuration - DUAL CONNECTION VERSION
=======================

Dual connection support: 
- String Redis (decode_responses=True) for processed data, indicators, prices
- Binary Redis (decode_responses=False) for raw ticks, pickled objects, state

Single source of truth for Redis connection settings, database segmentation,
and feature flags (client tracking, RedisJSON readiness). updated on 2025-12-19
"""

from __future__ import annotations

import os
import time
import threading
import pickle
import json
from typing import Any, Dict, Optional, Union, Tuple, List
import redis
from redis.connection import ConnectionPool


# ---------------------------------------------------------------------------
# Dual Base connection configuration (Redis 8.x)
# ---------------------------------------------------------------------------

# STRING Redis - for everything that needs UTF-8 decoding (processed data)
REDIS_8_CONFIG_STRING: Dict[str, Any] = {
    "host": os.environ.get("REDIS_HOST", "localhost"),
    "port": int(os.environ.get("REDIS_PORT", 6379)),
    "password": os.environ.get("REDIS_PASSWORD") or None,
    "decode_responses": True,  # Decode to strings for easier processing
    "encoding": "utf-8",
    "socket_connect_timeout": 5,
    "socket_keepalive": True,
    "retry_on_timeout": True,
    "max_connections": 50,  # Increased for high-frequency trading data
    "health_check_interval": 30,  # Redis 8.0 enhancement
    "socket_timeout": 2,  # Reduced from default for faster response
    # charset removed - not supported in redis-py 5.x+, encoding is sufficient
    "encoding_errors": "strict",
}

# BINARY Redis - for everything that needs raw binary data (ticks, pickled objects)
REDIS_8_CONFIG_BINARY: Dict[str, Any] = {
    "host": os.environ.get("REDIS_HOST", "localhost"),
    "port": int(os.environ.get("REDIS_PORT", 6379)),
    "password": os.environ.get("REDIS_PASSWORD") or None,
    "decode_responses": False,  # Do not decode responses
    "encoding": "utf-8",  # Encoding for internal operations
    "socket_connect_timeout": 5,
    "socket_keepalive": True,
    "retry_on_timeout": True,
    "max_connections": 50,  # Increased for high-frequency trading data
    "health_check_interval": 30,  # Redis 8.0 enhancement
    "socket_timeout": 2,  # Reduced from default for faster response
    # charset removed - not supported in redis-py 5.x+, encoding is sufficient
    "encoding_errors": "strict",
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
    "stream-node-max-bytes": 4096,
    "stream-node-max-entries": 100,
    
    # Server loop
    "hz": 10,  # Default; avoid cranking up unless measured
}

# Optional global feature flags (e.g., client tracking, RedisJSON support)
REDIS_8_FEATURE_FLAGS: Dict[str, bool] = {
    "client_tracking": True,
    "use_json": False,  # set to True when RedisJSON module is loaded
    "dual_connections": True,  # NEW: Flag indicating we're using dual connections
}

# ---------------------------------------------------------------------------
# Database segmentation (SINGLE SOURCE OF TRUTH)
# 3 databases - simplified for clarity and performance
# Now specifies which connection type to use for each data type
# ---------------------------------------------------------------------------

REDIS_DATABASES: Dict[int, Dict[str, Any]] = {
    # DB 0: User Database
    # User credentials, trades, order data, session management
    # TTL: 90 days (data deleted if no login for 90 days)
    # DB 0: User Database - DUAL connections
    0: {
        "name": "user_database",
        "ttl": 7_776_000,  # 90 days
        "connection_type": "dual",  # BOTH string and binary
        "data_types": {
            "string": [  # JSON string data
                # User management
                "user_credentials",
                "user_sessions",
                "user_preferences",
                "user_config",
                # Trading data
                "order_rejections",
                "order_history",
                "trade_history",
                "positions",
                "holdings",
                # Configuration
                "user_config",
                "system_config",
                "alert_preferences",
                "watchlist",
                "portfolio",
                "strategy_config",
                "notifications",
                "subscription",
                "license",
                # Benchmark data
                "bench_data",
            ],
            "binary": [  # Binary/secure data
                # Secure tokens
                "broker_tokens",
                "api_keys",
                "encrypted_data",
                # Binary objects
                "pickled_objects",
                "secure_cache",
            ]
        },
        "client_tracking": False,
        "use_json": True,
    },
    # DB 1: Historical + Live Data - DUAL connections
    1: {
        "name": "market_data",
        "ttl": 57_600,  # 16 hours for intraday data
        "connection_type": "dual",  # BOTH string and binary
        "data_types": {
            "string": [  # Processed data (use string Redis)
                # OHLC data
                "ohlc_latest",
                "ohlc_daily",
                "ohlc_hourly",
                "ohlc_stats",
                # Prices
                "equity_prices",
                "futures_prices",
                "options_prices",
                "commodity_prices",
                # Volume data
                "bucket_incremental_volume",
                "bucket_cumulative_volume",
                "volume_state",
                "volume_baseline",
                "volume_profile",
                "straddle_volume",
                # Indicators & Greeks
                "indicators_cache",
                "greeks",
                "edge_indicators",
                # Microstructure
                "depth_analysis",
                "flow_data",
                "microstructure_metrics",
                # Alerts & Patterns (live)
                "alerts_stream",
                "patterns_stream",
                "pattern_candidates",
                # Latest ticks (processed)
                "live_ticks_latest",
            ],
            "binary": [  # Raw data (use binary Redis)
                # Raw tick data streams
                "ticks_stream_raw",
                "ticks:intraday:raw",
                "ticks:realtime_raw",
                # Binary objects, classifiers, state
                "classifier_state",
                "model_state",
                "pickled_objects",
                "binary_cache",
            ]
        },
        "client_tracking": True,
        "use_json": True,
    },
    # DB 2: Analytics & Alert Validation - STRING connection
    2: {
        "name": "analytics_validation",
        "ttl": 604800,  # 7 days for analytics persistence
        "connection_type": "string",  # All analytics are string
        "data_types": [
            # Pattern analytics
            "pattern_performance",
            "pattern_metrics",
            "signal_quality",
            # Validation
            "validation_results",
            "forward_validation",
            "validator_metadata",
            # Historical analytics
            "performance_data",
            "metrics_cache",
            "analytics_data",
        ],
        "client_tracking": False,
        "use_json": True,
    },
}

# ---------------------------------------------------------------------------
# Stream Max Length Configuration (Source of Truth)
# Prevents unbounded stream growth while maintaining performance
# ---------------------------------------------------------------------------

REDIS_STREAM_MAXLEN_CONFIG: Dict[str, int] = {
    # Tick data streams
    "ticks:intraday:processed": 2_000_000,    # Keep 10 days of data for recovery (~2M ticks)
    "ticks:stream": 100_000,                   # Per-symbol tick stream limit
    
    # Pattern streams
    "patterns:global": 5_000,                  # Keep last 5k patterns globally
    "patterns:{symbol}": 5_000,                # Keep last 5k patterns per symbol
    
    # Indicator streams
    "indicators:{indicator_type}:{symbol}": 5_000,   # Keep last 5k indicator updates
    "indicators:{indicator_type}:global": 5_000,     # Keep last 5k global indicator updates
    
    # Alert stream
    "alerts:stream": 1_000,                    # Keep last 1k alerts (Tier 1 performance)
}


# ---------------------------------------------------------------------------
# Dual connection Redis 8.0 configuration helper
# ---------------------------------------------------------------------------

class Redis8Config:
    """Unified Redis configuration with feature awareness."""

    def __init__(self, environment: Optional[str] = None):
        self.environment = environment or os.environ.get("ENVIRONMENT", "dev")
        self.base_string_config = REDIS_8_CONFIG_STRING.copy()
        self.base_binary_config = REDIS_8_CONFIG_BINARY.copy()
        self.global_features = REDIS_8_FEATURE_FLAGS.copy()

        # Environment tuning
        if self.environment == "prod":
            self.base_string_config.update({"max_connections": 100, "health_check_interval": 60})
            self.base_binary_config.update({"max_connections": 100, "health_check_interval": 60})
        elif self.environment == "dev":
            self.base_string_config.update({"max_connections": 20, "health_check_interval": 10})
            self.base_binary_config.update({"max_connections": 20, "health_check_interval": 10})


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

    def get_string_db_config(self, db_name: str) -> Dict[str, Any]:
        """Rerurn STRING connection kwareds for a named database."""
        config = self.base_string_config.copy()
        config["db"] = self._resolve_db_number(db_name)
        return config

    def get_binary_db_config(self, db_name: str) -> Dict[str, Any]:
        """Return BINARY connection kwargs for a named database."""
        config = self.base_binary_config.copy()
        config["db"] = self._resolve_db_number(db_name)
        return config

    def get_db_config(self, db_name: str, connection_type: str = "string") -> Dict[str, Any]:
        """
        Return connection kwargs for a named database with specified connection type.
        
        Args:
            db_name: Database name
            connection_type: "string" or "binary"
            
        Returns:
            Dict with connection configuration
        """
        if connection_type == "string":
            return self.get_string_db_config(db_name)
        elif connection_type == "binary":
            return self.get_binary_db_config(db_name)
        else:
            raise ValueError(f"Invalid connection_type: {connection_type}. Use 'string' or 'binary'.")

    def get_db_features(self, db_name: str) -> Dict[str, bool]:
        """Return feature flags for a named database."""
        db_num = self._resolve_db_number(db_name)
        spec = REDIS_DATABASES[db_num]
        return {
            "client_tracking": bool(spec.get("client_tracking", False)),
            "use_json": bool(spec.get("use_json", False)),
            "dual_connections": spec.get("connection_type", "string") == "dual",
        }
    
    def get_connection_type_for_data(self, data_type: str) -> str:
        """
        Determine which connection type to use for a specific data type.
        
        Returns:
            "string" or "binary"
        """
        for db_num, spec in REDIS_DATABASES.items():
            if spec.get("connection_type") == "dual":
                # Check string data types
                if "data_types" in spec and isinstance(spec["data_types"], dict):
                    if data_type in spec["data_types"].get("string", []):
                        return "string"
                    if data_type in spec["data_types"].get("binary", []):
                        return "binary"
            elif "data_types" in spec and data_type in spec["data_types"]:
                return spec.get("connection_type", "string")
        
        # Default to string for unknown data types
        return "string"


# ---------------------------------------------------------------------------
# DUAL Connection Pools - Process-Specific Pools for BOTH string and binary
# ---------------------------------------------------------------------------

# Process-specific pool size configurations for DUAL connections
PROCESS_POOL_CONFIG: Dict[str, Dict[str, int]] = {
    # Format: {process_name: {"string": size, "binary": size}}
    
    # High-frequency processes
    "intraday_scanner": {"string": 80, "binary": 20},      # Mostly string operations
    "websocket_parser": {"string": 30, "binary": 20},       # Needs both (binary for raw, string for processed)
    "intraday_crawler": {"string": 40, "binary": 10},       # Mostly computed data (string)
    "crawler": {"string": 40, "binary": 10},                # Same as intraday_crawler
    
    # Moderate processes
    "stream_consumer": {"string": 20, "binary": 10},
    "data_pipeline": {"string": 20, "binary": 10},
    "redis_storage": {"string": 25, "binary": 5},           # Mostly string (batching operations)
    "unified_data_storage": {"string": 40, "binary": 10},   # Indicators/Greeks - mostly string
    "validator": {"string": 15, "binary": 5},
    "alert_validator": {"string": 15, "binary": 5},
    "dashboard": {"string": 12, "binary": 3},               # Mostly read operations (string)
    "volume_computation_manager": {"string": 15, "binary": 5},
    "pattern_detector": {"string": 25, "binary": 5},        # Pattern detection
    
    # Low-frequency processes
    "vix_utils": {"string": 8, "binary": 2},
    "vix_utils_fallback": {"string": 8, "binary": 2},
    "pattern_health_check": {"string": 8, "binary": 2},
    "alert_stream": {"string": 8, "binary": 2},
    "redis_instrument_cache": {"string": 8, "binary": 2},
    "historical_queries": {"string": 8, "binary": 2},
    "redis_gateway": {"string": 8, "binary": 2},
    "ngrok": {"string": 4, "binary": 1},
    "cleanup": {"string": 4, "binary": 1},
    "monitor": {"string": 4, "binary": 1},
    "gift_nifty_crawler": {"string": 4, "binary": 1},
    "gift_nifty_gap": {"string": 4, "binary": 1},
    "sharpe_dashboard": {"string": 4, "binary": 1},
    
    # Default fallback
    "default": {"string": 20, "binary": 10},
}

# ---------------------------------------------------------------------------
# DUAL Redis Client Factory Functions
# ---------------------------------------------------------------------------

def get_dual_redis_clients(process_name: str = "default", db_name: str = "market_data") -> Tuple[redis.Redis, redis.Redis]:
    """
    Get both string and binary Redis clients for a process.
    
    Args:
        process_name: Name of the process (for pool sizing)
        db_name: Name of the database ("user_database", "market_data", "analytics_validation")
        
    Returns:
        Tuple of (string_redis_client, binary_redis_client)
    """
    config = Redis8Config()
    
    # Get process-specific pool sizes
    pool_config = PROCESS_POOL_CONFIG.get(process_name, PROCESS_POOL_CONFIG["default"])
    string_pool_size = pool_config.get("string", 20)
    binary_pool_size = pool_config.get("binary", 10)
    
    # Get database configurations
    string_config = config.get_string_db_config(db_name)
    binary_config = config.get_binary_db_config(db_name)
    
    # Apply process-specific pool sizes
    string_config["max_connections"] = string_pool_size
    binary_config["max_connections"] = binary_pool_size
    
    # Create connection pools
    string_pool = ConnectionPool(**string_config)
    binary_pool = ConnectionPool(**binary_config)
    
    # Create clients
    string_client = redis.Redis(connection_pool=string_pool)
    binary_client = redis.Redis(connection_pool=binary_pool)
    
    return string_client, binary_client

def get_redis_client(process_name: str = "default", db_name: str = "market_data", 
                    connection_type: str = "string") -> redis.Redis:
    """
    Get a single Redis client (string or binary).
    
    Args:
        process_name: Name of the process
        db_name: Name of the database
        connection_type: "string" or "binary"
        
    Returns:
        Redis client instance
    """
    if connection_type not in ["string", "binary"]:
        raise ValueError(f"connection_type must be 'string' or 'binary', got {connection_type}")
    
    config = Redis8Config()
    
    # Get process-specific pool size
    pool_config = PROCESS_POOL_CONFIG.get(process_name, PROCESS_POOL_CONFIG["default"])
    pool_size = pool_config.get(connection_type, 20 if connection_type == "string" else 10)
    
    # Get database configuration
    if connection_type == "string":
        db_config = config.get_string_db_config(db_name)
    else:
        db_config = config.get_binary_db_config(db_name)
    
    # Apply process-specific pool size
    db_config["max_connections"] = pool_size
    
    # Create connection pool and client
    pool = ConnectionPool(**db_config)
    client = redis.Redis(connection_pool=pool)
    
    return client

def get_redis_client_for_data(data_type: str, process_name: str = "default") -> redis.Redis:
    """
    Get appropriate Redis client based on data type.
    
    Args:
        data_type: Type of data being stored/retrieved
        process_name: Name of the process
        
    Returns:
        Redis client (string or binary based on data type)
    """
    config = Redis8Config()
    connection_type = config.get_connection_type_for_data(data_type)
    
    # Determine which database this data type belongs to
    db_name = "market_data"  # Default
    for db_num, spec in REDIS_DATABASES.items():
        if spec.get("connection_type") == "dual":
            if "data_types" in spec and isinstance(spec["data_types"], dict):
                if (data_type in spec["data_types"].get("string", []) or 
                    data_type in spec["data_types"].get("binary", [])):
                    db_name = spec["name"]
                    break
        elif "data_types" in spec and data_type in spec["data_types"]:
            db_name = spec["name"]
            break
    
    return get_redis_client(process_name, db_name, connection_type)


# ---------------------------------------------------------------------------
# Helper functions for specific use cases
# ---------------------------------------------------------------------------

# Add these helper functions to redis_config.py:

def get_user_redis(connection_type: str = "string") -> redis.Redis:
    """Quick access to user database Redis client."""
    return get_redis_client("default", "user_database", connection_type)

def save_user_config(user_id: str, config_key: str, config_data: Any):
    """Save user configuration to DB0 string Redis."""
    string_redis = get_user_redis("string")
    key = f"user:{user_id}:config:{config_key}"
    string_redis.set(key, json.dumps(config_data))

def load_user_config(user_id: str, config_key: str) -> Any:
    """Load user configuration from DB0 string Redis."""
    string_redis = get_user_redis("string")
    key = f"user:{user_id}:config:{config_key}"
    data = string_redis.get(key)
    return json.loads(data) if data else None

def save_broker_token(broker_name: str, user_id: str, token_data: Any):
    """Save broker token to DB0 binary Redis (secure)."""
    binary_redis = get_user_redis("binary")
    key = f"broker:token:{broker_name}:{user_id}"
    pickled_data = pickle.dumps(token_data)
    binary_redis.set(key, pickled_data)

def load_broker_token(broker_name: str, user_id: str) -> Any:
    """Load broker token from DB0 binary Redis."""
    binary_redis = get_user_redis("binary")
    key = f"broker:token:{broker_name}:{user_id}"
    data = binary_redis.get(key)
    return pickle.loads(data) if data and isinstance(data, bytes) else None

def save_order(user_id: str, order_id: str, order_data: Dict[str, Any]):
    """Save order to DB0 string Redis."""
    string_redis = get_user_redis("string")
    key = f"order:{user_id}:{order_id}"
    string_redis.hset(key, mapping={
        k: json.dumps(v) if not isinstance(v, str) else v
        for k, v in order_data.items()
    })

def get_user_orders(user_id: str) -> List[Dict[str, Any]]:
    """Get all orders for a user from DB0."""
    string_redis = get_user_redis("string")
    pattern = f"order:{user_id}:*"
    keys = string_redis.keys(pattern)
    orders = []
    for key in keys if isinstance(keys, list) else []:
        order_data = string_redis.hgetall(key)
        orders.append({
            k: json.loads(v) if v.startswith('{') or v.startswith('[') else v
            for k, v in order_data.items() if isinstance(order_data, dict) and isinstance(v, str) and isinstance(k, str)
        })
    return orders

def get_user_redis(connection_type: str = "string") -> redis.Redis:
    """Quick access to user database Redis client."""
    return get_redis_client("default", "user_database", connection_type)

def get_string_redis(db_name: str = "market_data") -> redis.Redis:
    """Quick access to string Redis client."""
    return get_redis_client("default", db_name, "string")


def get_binary_redis(db_name: str = "market_data") -> redis.Redis:
    """Quick access to binary Redis client."""
    return get_redis_client("default", db_name, "binary")


def save_pickled_object(key: str, obj: Any, db_name: str = "market_data"):
    """Save a pickled object to binary Redis."""
    binary_client = get_binary_redis(db_name)
    pickled_data = pickle.dumps(obj)
    binary_client.set(key, pickled_data)


def load_pickled_object(key: str, db_name: str = "market_data") -> Any:
    """Load a pickled object from binary Redis."""
    binary_client = get_binary_redis(db_name)
    data = binary_client.get(key)
    return pickle.loads(data) if data and isinstance(data, bytes) else None


def save_json_object(key: str, obj: Any, db_name: str = "market_data"):
    """Save a JSON object to string Redis."""
    string_client = get_string_redis(db_name)
    json_data = json.dumps(obj)
    string_client.set(key, json_data)


def load_json_object(key: str, db_name: str = "market_data") -> Any:
    """Load a JSON object from string Redis."""
    string_client = get_string_redis(db_name)
    data = string_client.get(key)
    return json.loads(data) if data else None

# ---------------------------------------------------------------------------
# Compatibility helpers (legacy API - maintain backward compatibility)
# ---------------------------------------------------------------------------

def get_redis_config(environment: Optional[str] = None) -> Dict[str, Any]:
    """
    Backwards-compatible accessor that returns the STRING connection configuration.
    """
    cfg = Redis8Config(environment)
    return cfg.base_string_config.copy()


def get_database_for_data_type(data_type: str) -> int:
    """Map a logical data type to its Redis database number."""
    for db_num, spec in REDIS_DATABASES.items():
        if "data_types" in spec:
            if isinstance(spec["data_types"], dict):
                # Check both string and binary data types
                if (data_type in spec["data_types"].get("string", []) or 
                    data_type in spec["data_types"].get("binary", [])):
                    return db_num
            elif data_type in spec["data_types"]:
                return db_num
    return 0


def get_ttl_for_data_type(data_type: str) -> int:
    """Return TTL (seconds) for a given data type."""
    for spec in REDIS_DATABASES.values():
        if "data_types" in spec:
            if isinstance(spec["data_types"], dict):
                if (data_type in spec["data_types"].get("string", []) or 
                    data_type in spec["data_types"].get("binary", [])):
                    return spec.get("ttl", 3600)
            elif data_type in spec["data_types"]:
                return spec.get("ttl", 3600)
    return 3600


def apply_redis_low_latency_optimizations(redis_client) -> bool:
    """
    Convenience function to apply Redis 8.2 low-latency optimizations.
    """
    config = Redis8Config()
    return config.apply_low_latency_optimizations(redis_client)


class RedisConfig(Redis8Config):
    """
    Legacy wrapper retained for import compatibility.
    Exposes a ``databases`` property (name -> db number) matching prior usage.
    """

    def __init__(self, environment: Optional[str] = None):
        super().__init__(environment)
        self.databases = {spec["name"]: db for db, spec in REDIS_DATABASES.items()}


# ---------------------------------------------------------------------------
# Global instances for easy import
# ---------------------------------------------------------------------------

# Global Redis config instance
redis_config = Redis8Config()

# Pre-configured clients for common use cases
try:
    # Initialize on import (lazy initialization in real usage)
    string_redis = get_string_redis("market_data")
    binary_redis = get_binary_redis("market_data")
    print("✅ DUAL Redis connections configured successfully!")
except Exception as e:
    print(f"⚠️  Redis initialization warning: {e}")
    # Fallback to None if Redis is not running
    string_redis = None
    binary_redis = None


# ---------------------------------------------------------------------------
# AION analytics channels
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
