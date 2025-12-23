# shared_core/redis_clients/async_connection_pool.py
"""
Async Redis Connection Pool
===========================

Provides async Redis connection pooling for high-performance async operations.
Uses redis-py 7.x native asyncio support.
"""

from __future__ import annotations

import os
import asyncio
import logging
from typing import Dict, Optional, Any

import redis.asyncio as aioredis
from shared_core.redis_clients.redis_config import REDIS_8_CONFIG_STRING, PROCESS_POOL_CONFIG

logger = logging.getLogger(__name__)

DEFAULT_ASYNC_CONFIG: Dict[str, Any] = REDIS_8_CONFIG_STRING.copy()


class AsyncRedisManager:
    """
    ✅ M4 ENFORCED: Async Redis Manager with process-wide pooling.
    Adapted for aioredis compatibility.
    """
    _async_pools: Dict[str, aioredis.ConnectionPool] = {}
    _async_pool_lock = asyncio.Lock()
    
    @classmethod
    async def get_client(cls, process_name: str = "default", **kwargs) -> aioredis.Redis:
        """
        Get async Redis client with pooled connections.
        """
        config = DEFAULT_ASYNC_CONFIG.copy()
        config.update(kwargs)
        
        # Enforce critical settings
        db = config.get('db', 1)
        config['decode_responses'] = True

        pool_limits = PROCESS_POOL_CONFIG.get(process_name, PROCESS_POOL_CONFIG.get('default'))
        if isinstance(pool_limits, dict):
            max_conns_default = pool_limits.get('string', 25)
        else:
            max_conns_default = pool_limits or 25
        max_conns = kwargs.get('max_connections', max_conns_default)
        
        # ✅ Build pool key based on connection parameters
        unix_socket_path = config.get('unix_socket_path')
        unix_socket_path_for_key = unix_socket_path if unix_socket_path else ''
        pool_key = f"async:{config.get('host', 'localhost')}:{config.get('port', 6379)}:{db}:{unix_socket_path_for_key}"
        
        async with cls._async_pool_lock:
            if pool_key not in cls._async_pools:
                pool_kwargs = {
                    'host': config.get('host', 'localhost'),
                    'port': config.get('port', 6379),
                    'db': db,
                    'max_connections': max_conns, 
                    'socket_connect_timeout': config.get('socket_connect_timeout', 0.3),
                    'socket_timeout': config.get('socket_timeout', 0.1),
                    'decode_responses': True,
                }
                
                # Handle Unix sockets
                if unix_socket_path and os.path.exists(unix_socket_path):
                    pool_kwargs['path'] = unix_socket_path
                    pool_kwargs.pop('host', None)
                    pool_kwargs.pop('port', None)
                    # For Unix sockets, connection_class is needed
                    pool_kwargs['connection_class'] = aioredis.UnixDomainSocketConnection
                else:
                    if unix_socket_path:
                        logger.warning(f"⚠️ Async Unix socket not found at {unix_socket_path}, falling back to TCP")
                
                # ✅ REMOVED: socket_keepalive deprecated in Redis 8.2
                # if tcp_keepalive_options:
                #     pool_kwargs['socket_keepalive'] = True
                #     pool_kwargs['socket_options'] = tcp_keepalive_options

                cls._async_pools[pool_key] = aioredis.ConnectionPool(**pool_kwargs)
                logger.info(f"✅ Created async Redis pool: {pool_key} (max={pool_kwargs['max_connections']})")
            
            pool = cls._async_pools[pool_key]
        
        return aioredis.Redis(connection_pool=pool)

    @classmethod
    async def cleanup(cls):
        """Cleanup all connection pools in this process."""
        async with cls._async_pool_lock:
            for pool in cls._async_pools.values():
                try:
                    await pool.disconnect()
                except Exception:
                    pass
            cls._async_pools.clear()


# Legacy support for existing code
class AsyncRedisPool(AsyncRedisManager):
    pass

async def get_realtime_client() -> aioredis.Redis:
    """Get client for realtime data (DB 1)."""
    return await AsyncRedisManager.get_client(db=1)

async def get_system_client() -> aioredis.Redis:
    """Get client for system/metadata (DB 0)."""
    return await AsyncRedisManager.get_client(db=0)

async def get_analytics_client() -> aioredis.Redis:
    """Get client for analytics/historical (DB 2)."""
    return await AsyncRedisManager.get_client(db=2)

async def get_validator_client() -> aioredis.Redis:
    """Get client for independent validator (DB 3)."""
    return await AsyncRedisManager.get_client(db=3)
