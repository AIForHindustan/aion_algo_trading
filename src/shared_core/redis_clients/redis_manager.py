"""
Redis 8.2 Optimized Connection Manager
Process-specific connection pooling with Redis 8.2 enhancements
"""

import redis
import os
import time
import threading
from typing import Dict, Optional
from redis.connection import ConnectionPool

from shared_core.redis_clients.redis_client import get_redis_client
class RedisManager82:
    """
    ⚠️ DEPRECATED: Use UnifiedRedisManager instead.
    
    Redis 8.2 optimized connection manager with process-specific pools.
    
    Migration:
        OLD: RedisManager82.get_client(process_name="x", db=1)
        NEW: get_redis_client(process_name="x", db=1)
    
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
        # NOTE: PROCESS_POOL_CONFIG now returns {"string": N, "binary": M} per process
        if max_connections is None:
            try:
                from shared_core.redis_clients.redis_config import PROCESS_POOL_CONFIG
                pool_config = PROCESS_POOL_CONFIG.get(
                    process_name, 
                    PROCESS_POOL_CONFIG.get('default', {"string": 20, "binary": 10})
                )
                # Sum string + binary for total pool size (legacy compatibility)
                if isinstance(pool_config, dict):
                    max_connections = pool_config.get('string', 20) + pool_config.get('binary', 10)
                else:
                    max_connections = pool_config  # Legacy format
            except Exception:
                max_connections = 30  # Fallback default (string: 20 + binary: 10)
        else:
            # If max_connections provided, still check config for override
            try:
                from shared_core.redis_clients.redis_config import PROCESS_POOL_CONFIG
                pool_config = PROCESS_POOL_CONFIG.get(process_name)
                if pool_config is not None:
                    # Handle new dict format
                    if isinstance(pool_config, dict):
                        max_connections = pool_config.get('string', 20) + pool_config.get('binary', 10)
                    else:
                        max_connections = pool_config  # Legacy format
            except Exception:
                pass  # Use provided max_connections if config lookup fails
        
        # Thread-safe pool creation/caching
        with cls._lock:
            pool_key = f"{process_name}:{db}"
            
            if process_name not in cls._pools:
                cls._pools[process_name] = {}
            
            if db not in cls._pools[process_name]:
                # ✅ Import config values from single source of truth
                # NOTE: Now using REDIS_8_CONFIG_STRING for decode_responses=True clients
                try:
                    from shared_core.redis_clients.redis_config import REDIS_8_CONFIG_STRING
                    socket_timeout = REDIS_8_CONFIG_STRING.get('socket_timeout', 2)
                    socket_connect_timeout = REDIS_8_CONFIG_STRING.get('socket_connect_timeout', 5)
                    health_check_interval = REDIS_8_CONFIG_STRING.get('health_check_interval', 30)
                except Exception:
                    socket_timeout = 2
                    socket_connect_timeout = 5
                    health_check_interval = 30
                
                # Create process-specific pool for this db
                pool_config = {
                    'host': host,
                    'port': port,
                    'db': db,
                    'password': password,
                    'max_connections': max_connections,
                    # 'socket_keepalive': True,  # REMOVED - Deprecated in Redis 8.2
                    'retry_on_timeout': True,
                    'health_check_interval': health_check_interval,
                    'socket_timeout': socket_timeout,
                    'socket_connect_timeout': socket_connect_timeout,
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
    
    # get_crawler_client() removed - use get_client() directly with process_name="crawler_{crawler_name}"
    
    @classmethod
    def get_pool_stats(cls) -> Dict:
        """
        Get detailed statistics about connection pools.
        
        Returns:
            Dict with per-process stats including:
            - databases: list of DB numbers with pools
            - total_pools: number of pools for this process
            - max_connections: configured max connections per pool
            - in_use_connections: currently checked-out connections
            - available_connections: connections in pool available for use
            - created_connections: total connections created (may be less than max)
        """
        with cls._lock:
            stats = {}
            total_in_use = 0
            total_available = 0
            total_created = 0
            
            for process_name, db_pools in cls._pools.items():
                process_stats = {
                    'databases': list(db_pools.keys()),
                    'total_pools': len(db_pools),
                    'pools_detail': {}
                }
                
                for db, pool in db_pools.items():
                    # Get actual connection counts from pool
                    in_use = len(pool._in_use_connections) if hasattr(pool, '_in_use_connections') else 0
                    available = len(pool._available_connections) if hasattr(pool, '_available_connections') else 0
                    created = getattr(pool, '_created_connections', 0)
                    max_conn = pool.connection_kwargs.get('max_connections', 0)
                    
                    process_stats['pools_detail'][db] = {
                        'max_connections': max_conn,
                        'in_use': in_use,
                        'available': available,
                        'created': created,
                        'utilization': f"{(in_use / max(created, 1)) * 100:.1f}%" if created > 0 else "0%"
                    }
                    
                    total_in_use += in_use
                    total_available += available
                    total_created += created
                
                stats[process_name] = process_stats
            
            # Add summary
            stats['_summary'] = {
                'total_processes': len(cls._pools),
                'total_in_use': total_in_use,
                'total_available': total_available,
                'total_created': total_created,
                'overall_utilization': f"{(total_in_use / max(total_created, 1)) * 100:.1f}%" if total_created > 0 else "0%"
            }
            
            return stats


# ---------------------------------------------------------------------------
# UnifiedRedisManager - Re-exported from redis_client.py (canonical source)
# ---------------------------------------------------------------------------
# Import and re-export UnifiedRedisManager for backward compatibility
# Code importing from redis_manager.py will get the canonical manager

try:
    from shared_core.redis_clients.redis_client import UnifiedRedisManager
except ImportError:
    # Fallback: Use RedisManager82 as UnifiedRedisManager if import fails
    UnifiedRedisManager = RedisManager82


# Pre-configured convenience wrappers removed - use get_redis_client() directly
# Example: get_redis_client(process_name="intraday_scanner", db=1)
