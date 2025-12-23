# StorageEngine - DEPRECATED

⚠️ **This module is DEPRECATED and will be removed in a future version.**

## Migration Guide

**Use `UnifiedDataStorage` instead:**

```python
# ❌ OLD (deprecated):
from shared_core.core.storage.engine import StorageEngine
storage = StorageEngine(redis_client=redis_client)
indicators = storage.retrieve_indicators(symbol, indicator_names)
storage.store_indicators(symbol, indicators)

# ✅ NEW (recommended):
from shared_core.redis_clients.unified_data_storage import UnifiedDataStorage
storage = UnifiedDataStorage(redis_client=redis_client)
indicators = storage.get_indicators(symbol, indicator_names)
storage.store_indicators(symbol, indicators)
```

## Why UnifiedDataStorage?

- ✅ Single source of truth for Redis operations
- ✅ Consistent key generation using `redis_key_standards`
- ✅ Automatic symbol normalization
- ✅ Better error handling and fallback logic
- ✅ Unified interface for indicators, Greeks, and custom metrics

## Backward Compatibility

`StorageEngine` is still available for backward compatibility but will show deprecation warnings.
All new code should use `UnifiedDataStorage`.




