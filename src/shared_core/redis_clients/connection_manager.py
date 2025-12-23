# shared_core/redis_clients/connection_manager.py
import logging
import threading
from typing import Dict, Optional

import redis
from redis.connection import ConnectionPool

try:
    import asyncio
    import aioredis
    AIOREDIS_AVAILABLE = True
except ImportError:  # pragma: no cover
    aioredis = None  # type: ignore
    import asyncio
    AIOREDIS_AVAILABLE = False

logger = logging.getLogger(__name__)


class RedisConnectionManager:
    """Global Redis connection pool manager"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._pools: Dict[str, ConnectionPool] = {}
                cls._instance._stats = {
                    'total_connections': 0,
                    'active_connections': 0,
                    'pool_hits': 0,
                    'pool_misses': 0
                }
            return cls._instance
    
    def get_pool(self, name: str, host: str = 'localhost', port: int = 6379, 
                 db: int = 1, password: Optional[str] = None,
                 max_connections: int = 50) -> ConnectionPool:
        """Get or create a connection pool"""
        pool_key = f"{host}:{port}:{db}"
        
        if pool_key not in self._instance._pools:
            self._instance._stats['pool_misses'] += 1
            # Use hiredis if available
            connection_kwargs = {
                'host': host,
                'port': port,
                'db': db,
                # 'socket_keepalive': True,  # REMOVED - Deprecated in Redis 8.2
                'socket_timeout': 10,
                'retry_on_timeout': True,
                'health_check_interval': 30,
            }
            
            if password:
                connection_kwargs['password'] = password
            
            # ✅ FIXED: redis-py 7.1+ with hiredis 3.x auto-detects and uses hiredis
            # No need to explicitly set parser_class - this caused socket_read_size errors
            # The library automatically uses hiredis when available
            try:
                import hiredis as hiredis_module
                logger.debug(
                    f"✅ hiredis {getattr(hiredis_module, '__version__', 'unknown')} available for pool {pool_key} (auto-detected)"
                )
            except ImportError:
                logger.debug("⚠️ hiredis not available, using default Redis parser")
            
            self._pools[pool_key] = ConnectionPool(
                max_connections=max_connections,
                **connection_kwargs
            )
        else:
            self._instance._stats['pool_hits'] += 1
        
        return self._instance._pools[pool_key]
    
    def get_stats(self) -> Dict:
        """Get connection pool statistics"""
        stats = self._instance._stats.copy()
        stats['pool_count'] = len(self._instance._pools)
        
        # Count active connections per pool
        active = 0
        total = 0
        for pool in self._pools.values():
            active += len(pool._in_use_connections) if hasattr(pool, '_in_use_connections') else 0
            total += pool._created_connections if hasattr(pool, '_created_connections') else 0
        
        stats['active_connections'] = active
        stats['total_connections'] = total
        stats['connection_utilization'] = active / max(total, 1)
        
        return stats
    
    def prune_idle_connections(self, max_idle_seconds: int = 300):
        """Prune idle connections"""
        import time
        current_time = time.time()
        
        for pool_key, pool in list(self._pools.items()):
            # Check if pool hasn't been used in a while
            if hasattr(pool, '_last_used'):
                if current_time - pool._last_used > max_idle_seconds:
                    pool.disconnect()
                    del self._pools[pool_key]


class AsyncRedisConnectionManager:
    """
    Lightweight async connection manager that hands out aioredis pools per worker.
    Keeps max connections capped per worker to avoid runaway growth when using asyncio tasks.
    """
    
    _pools: Dict[str, "aioredis.commands.Redis"] = {}
    _lock = asyncio.Lock()
    
    @classmethod
    async def get_redis_pool(
        cls,
        worker_id: str,
        url: str = "redis://localhost",
        *,
        minsize: int = 1,
        maxsize: int = 5,
        encoding: str = "utf-8",
    ):
        if not AIOREDIS_AVAILABLE:
            raise RuntimeError("aioredis is not available; install redis>=4.2[async]")
        
        if worker_id in cls._pools:
            return cls._pools[worker_id]
        
        async with cls._lock:
            if worker_id not in cls._pools:
                pool = await aioredis.create_redis_pool(
                    url,
                    minsize=minsize,
                    maxsize=maxsize,
                    encoding=encoding,
                )
                cls._pools[worker_id] = pool
                logger.info(
                    "🔗 Created async Redis pool for worker %s (max %s connections)",
                    worker_id,
                    maxsize,
                )
        return cls._pools[worker_id]
    
    @classmethod
    async def cleanup(cls):
        """Close all async pools."""
        for pool in list(cls._pools.values()):
            try:
                pool.close()
                await pool.wait_closed()
            except Exception:
                pass
        cls._pools.clear()
