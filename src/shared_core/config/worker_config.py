# ✅ Centralized M4 configuration - SINGLE SOURCE OF TRUTH

PROCESS_POOL_CONFIG = {
    # ✅ REDIS: Process-wide pool (shared by ALL components in worker)
    'redis': {
        'host': 'localhost',
        'port': 6379,
        'unix_socket_path': '/tmp/redis_trading.sock',  # ✅ M4: 40% faster
        'db': 1,
        'max_connections': 50,  # ✅ HARD LIMIT: 50 connections per WORKER PROCESS
        'decode_responses': True,  # ✅ ENFORCE True globally
        'socket_timeout': 0.1,
        'socket_connect_timeout': 0.3,
        # 'socket_keepalive': True,  # REMOVED - Deprecated in Redis 8.2
        'health_check_interval': 5,
        'retry_on_timeout': False,
    },
    
    # ✅ WORKER: Process-level settings
    'worker': {
        'num_workers': 10,  # ✅ M4 Pro: 10 workers
        'max_memory_gb': 2.5,  # Per-worker limit
        'tick_batch_size': 50,
    },
    
    # ✅ CACHE: Per-worker LRU caches
    'cache': {
        'max_symbols': 75000,
        'ttl_seconds': 300,
    },
}

# ✅ Override Redis defaults if PROCESS_POOL_CONFIG is defined
# This ensures UnifiedRedisManager respects the config
import os
if 'PROCESS_POOL_CONFIG' in os.environ:
    # Use config from environment variable (JSON string)
    import json
    try:
        config = json.loads(os.environ['PROCESS_POOL_CONFIG'])
        if isinstance(config, dict):
            PROCESS_POOL_CONFIG.update(config)
    except Exception:
        pass
