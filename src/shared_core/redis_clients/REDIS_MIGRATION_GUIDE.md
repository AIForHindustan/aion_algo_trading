# Redis Client Migration Guide

## Overview
The codebase now uses a modern Redis client with RESP3, retry/backoff, and circuit breaker to fix constant connection issues.

## Quick Start

### 1. Set Environment Variables (Optional)
```bash
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_DB_DEFAULT=0
export REDIS_HEALTH_CHECK_INTERVAL=15
export REDIS_USE_MODERN=true  # Use modern RESP3 client (default: true)
```

### 2. Test Redis Health
```bash
python redis_files/redis_health.py
```

### 3. Migration Pattern

**BEFORE:**
```python
import redis
r = redis.Redis(host='localhost', port=6379, db=1)
```

**AFTER:**
```python
from redis_files.redis_client import get_redis_client

# For default DB (from config)
r = get_redis_client()

# For specific DB
r = get_redis_client().get_client(1)  # DB 1
```

## Benefits

1. **RESP3 Protocol**: Fixes Redis 8.x INFO/Streams parsing issues
2. **Retry/Backoff**: Built-in exponential backoff prevents tight reconnect loops
3. **Circuit Breaker**: Fast-fails during outages, reduces thundering herds
4. **Connection Pooling**: Shared singleton pool prevents connection storms
5. **Health Checks**: Automatic keepalive reduces FIN/ACK flapping

## Files Already Migrated

- ✅ `redis_files/redis_client.py` - Core client implementation
- ✅ `redis_files/redis_health.py` - Health check utilities
- ✅ `crawlers/base_crawler.py` - Uses `get_redis_client()` fallback

## Files Needing Migration (Lower Priority)

These files still use `redis.Redis()` directly but are less critical:
- `aion_alert_dashboard/alert_dashboard.py`
- `alerts/simple_instrument_manager.py`
- `scripts/redis_monitor.py`
- Various diagnostic/report scripts

## Running Scanner

The scanner automatically uses the modern client. No changes needed:

```bash
cd /Users/lokeshgupta/Projects/aion_algo_trading/intraday_trading
source .venv/bin/activate
python intraday_scanner/scanner_main.py
```

All components share the same tuned connection pool, preventing "Too many connections" errors.

