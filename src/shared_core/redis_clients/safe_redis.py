# shared_core/redis_clients/safe_redis.py
import redis
from functools import wraps
from shared_core.redis_clients.symbol_guard import SymbolGuard, get_symbol_guard

class SafeRedisClient(redis.Redis):
    """Redis client that prevents key corruption"""
    
    def __init__(self, *args, **kwargs):
        # Force decode_responses=True
        kwargs['decode_responses'] = True
        kwargs['encoding'] = 'utf-8'
        super().__init__(*args, **kwargs)
        self.symbol_guard = get_symbol_guard()  # Use global singleton
    
    def hset(self, name, key=None, value=None, mapping=None, **kwargs):
        """Override hset to clean keys"""
        # Clean the hash name (main key)
        clean_name = self.symbol_guard.guard_symbol(name, "redis_hset_name")
        
        # Clean mapping keys if provided
        if mapping:
            clean_mapping = {}
            for k, v in mapping.items():
                clean_k = self.symbol_guard.guard_symbol(k, "redis_hset_key")
                clean_mapping[clean_k] = v
            return super().hset(clean_name, mapping=clean_mapping, **kwargs)
        else:
            clean_key = self.symbol_guard.guard_symbol(key, "redis_hset_field")
            return super().hset(clean_name, clean_key, value, **kwargs)
    
    def get(self, name, *args, **kwargs):
        """Override get to clean key"""
        clean_name = self.symbol_guard.guard_symbol(name, "redis_get")
        return super().get(clean_name, *args, **kwargs)
    
    def set(self, name, *args, **kwargs):
        """Override set to clean key"""
        clean_name = self.symbol_guard.guard_symbol(name, "redis_set")
        return super().set(clean_name, *args, **kwargs)

# Replace ALL redis.Redis() with SafeRedisClient()