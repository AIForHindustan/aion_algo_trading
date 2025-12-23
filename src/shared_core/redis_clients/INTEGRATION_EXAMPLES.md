# Redis Canonical Key System - Integration Examples

This document provides integration examples for using the canonical key system across different services.

## Table of Contents

1. [Crawler Integration](#crawler-integration)
2. [Consumer Integration](#consumer-integration)
3. [Volume Baseline Service](#volume-baseline-service)
4. [Migration Utility](#migration-utility)
5. [Benefits](#benefits)

---

## Crawler Integration

### Example: Intraday Crawler

```python
# zerodha/crawlers/intraday_crawler.py

from shared_core.redis_clients import (
    canonical_key_builder,
    CanonicalRedisClient,
)
import json
from typing import Dict


class IntradayCrawler(BaseCrawler):
    def __init__(self, redis_client, ...):
        super().__init__(...)
        
        # Wrap Redis client with canonical key support
        self.redis_client = CanonicalRedisClient(redis_client)
    
    def _publish_to_redis(self, tick_data: Dict):
        """Publish tick data to Redis stream using canonical keys"""
        # Use canonical key builder
        stream_key = canonical_key_builder.live_processed_stream()
        
        # Client automatically uses canonical keys
        self.redis_client.xadd(
            stream_key,
            {"data": json.dumps(tick_data)},
            maxlen=5000,
            approximate=True
        )
    
    def write_tick_to_redis(self, symbol: str, tick_data: Dict):
        """Write tick to Redis hash using canonical keys"""
        # Get canonical key for tick hash
        tick_hash_key = canonical_key_builder.live_ticks_hash(symbol)
        
        # Convert tick_data to Redis-compatible format
        redis_data = {
            str(k): str(v) if v is not None else ""
            for k, v in tick_data.items()
        }
        
        # Store in hash (key automatically converted to canonical)
        self.redis_client.hset(tick_hash_key, mapping=redis_data)
        self.redis_client.expire(tick_hash_key, 300)  # 5min TTL
        
        # Also update latest price
        if 'last_price' in tick_data:
            price_key = canonical_key_builder.live_price_realtime(symbol)
            self.redis_client.set(price_key, tick_data['last_price'], ex=300)
```

---

## Consumer Integration

### Example: Pattern Detector

```python
# intraday_trading/patterns/pattern_detector.py

from shared_core.redis_clients import (
    canonical_key_builder,
    CanonicalRedisClient,
    map_tick_for_pattern,
)


class PatternDetector:
    def __init__(self, redis_client):
        # Wrap Redis client with canonical key support
        self.redis_client = CanonicalRedisClient(redis_client)
        
        # Get canonical stream name
        self.tick_stream = canonical_key_builder.live_processed_stream()
    
    def consume_ticks(self):
        """Consume ticks from Redis stream using canonical keys"""
        # Always uses canonical stream name, regardless of what crawler calls it
        messages = self.redis_client.xread(
            {self.tick_stream: "0-0"},
            count=100,
            block=1000  # Block for 1 second
        )
        
        for stream_name, entries in messages:
            for entry_id, fields in entries:
                # Parse tick data
                tick_data = json.loads(fields.get(b'data', b'{}'))
                
                # Map to pattern-expected field names
                pattern_data = map_tick_for_pattern(tick_data)
                
                # Process pattern detection
                self.detect_patterns(pattern_data)
    
    def get_tick_data(self, symbol: str) -> Dict:
        """Get latest tick data using canonical keys"""
        tick_hash_key = canonical_key_builder.live_ticks_hash(symbol)
        tick_data = self.redis_client.hgetall(tick_hash_key)
        
        # Normalize field names
        from shared_core.redis_clients import normalize_redis_tick_data
        return normalize_redis_tick_data(tick_data)
```

---

## Volume Baseline Service

### Example: Historical Volume Baseline

```python
# shared_core/volume_files/time_aware_volume_baseline.py

from shared_core.redis_clients import (
    canonical_key_builder,
    CanonicalRedisClient,
)


class HistoricalVolumeBaseline:
    def __init__(self, redis_client):
        # Wrap Redis client with canonical key support
        self.redis_client = CanonicalRedisClient(redis_client)
    
    def get_baseline_key(self, symbol: str) -> str:
        """Always returns canonical baseline key"""
        return canonical_key_builder.volume_baseline(symbol)
    
    def get_baseline(self, symbol: str) -> float:
        """Get volume baseline using canonical keys"""
        key = self.get_baseline_key(symbol)
        
        # redis_client.get(key) automatically resolves to canonical key
        baseline_data = self.redis_client.hgetall(key)
        
        if baseline_data:
            # Extract baseline value
            baseline_str = baseline_data.get('baseline_volume', '0')
            return float(baseline_str)
        
        return 0.0
    
    def set_baseline(self, symbol: str, baseline_value: float, avg_20d: float, avg_55d: float):
        """Set volume baseline using canonical keys"""
        key = self.get_baseline_key(symbol)
        
        baseline_data = {
            'baseline_volume': str(baseline_value),
            'avg_volume_20d': str(avg_20d),
            'avg_volume_55d': str(avg_55d),
        }
        
        # Store in hash (key automatically converted to canonical)
        self.redis_client.hset(key, mapping=baseline_data)
        self.redis_client.expire(key, 86400)  # 24 hour TTL
```

---

## Migration Utility

### Example: Migrating Old Keys to Canonical Keys

```python
# scripts/migrate_redis_keys.py

import redis
from shared_core.redis_clients import RedisKeyMigrator


def main():
    # Connect to Redis
    redis_client = redis.Redis(host='localhost', port=6379, db=1)
    
    # Create migrator
    migrator = RedisKeyMigrator(redis_client)
    
    # Step 1: Dry run to see what would be migrated
    print("=== DRY RUN: Checking what would be migrated ===")
    stats = migrator.migrate_keys(dry_run=True)
    print(f"Would migrate: {stats['migrated']} keys")
    print(f"Would skip: {stats['skipped']} keys")
    print(f"Would fail: {stats['failed']} keys")
    
    # Step 2: Find all alias keys
    print("\n=== Finding alias keys ===")
    found_aliases = migrator.find_alias_keys("*")
    for canonical, aliases in found_aliases.items():
        print(f"Canonical: {canonical}")
        print(f"  Aliases found: {aliases}")
    
    # Step 3: Perform actual migration
    if input("\nProceed with migration? (yes/no): ").lower() == 'yes':
        print("\n=== Performing migration ===")
        stats = migrator.migrate_keys(dry_run=False)
        print(f"Migrated: {stats['migrated']} keys")
        print(f"Skipped: {stats['skipped']} keys")
        print(f"Failed: {stats['failed']} keys")
        
        # Step 4: Optional cleanup of alias keys
        if input("\nClean up alias keys? (yes/no): ").lower() == 'yes':
            deleted = migrator.cleanup_aliases(dry_run=False, after_migration=True)
            print(f"Deleted {deleted} alias keys")


if __name__ == "__main__":
    main()
```

### Migration for Specific Categories

```python
# Migrate only tick storage keys
migrator = RedisKeyMigrator(redis_client)
stats = migrator.migrate_keys(
    dry_run=False,
    categories=['tick_storage']
)
```

---

## Benefits

### 1. Name Mismatch Resolution

All services automatically use canonical keys, eliminating naming inconsistencies:

```python
# All these resolve to the same canonical key
stream1 = canonical_key_builder.live_processed_stream()
stream2 = redis_key_mapper.get_canonical_key("ticks:intraday:processed")
stream3 = redis_key_mapper.resolve_key("tick_storage.processed_stream")

assert stream1 == stream2 == stream3  # All equal!
```

### 2. Backward Compatibility

Old keys still work during migration:

```python
# Old code using alias
client.get("ticks:live:processed")  # Works!

# New code using canonical
client.get("ticks:intraday:processed")  # Also works!

# Both resolve to the same canonical key
```

### 3. Centralized Management

One place to manage all Redis key patterns:

```yaml
# shared_core/redis_clients/redis_key_mapping.yaml
tick_storage:
  processed_stream:
    canonical: "ticks:intraday:processed"
    aliases:
      - "ticks:live:processed"
      - "ticks:processed"
```

### 4. Type Safety

Structured key building with validation:

```python
# Type-safe key building
key = canonical_key_builder.live_ticks_hash("RELIANCE")
# Returns: "ticks:RELIANCE" (canonical form)

# Automatic validation
key = canonical_key_builder.volume_baseline("NIFTY")
# Always returns canonical key, even if alias is used
```

### 5. Easy Refactoring

Change key patterns without breaking consumers:

```python
# Change in YAML config
# Old: "ticks:intraday:processed"
# New: "ticks:processed:v2"

# All consumers automatically use new key
# No code changes needed!
```

### 6. Consistent Monitoring

Standardized keys for monitoring and debugging:

```python
# All services use same key names
# Easy to monitor and debug in Redis
redis-cli> KEYS ticks:intraday:processed
redis-cli> HGETALL ticks:RELIANCE
redis-cli> GET vol:baseline:NIFTY
```

---

## Usage Patterns

### Pattern 1: Direct Key Building

```python
from shared_core.redis_clients import canonical_key_builder

# Build keys directly
stream_key = canonical_key_builder.live_processed_stream()
tick_key = canonical_key_builder.live_ticks_hash("RELIANCE")
baseline_key = canonical_key_builder.volume_baseline("NIFTY")
```

### Pattern 2: Using Canonical Client

```python
from shared_core.redis_clients import CanonicalRedisClient

client = CanonicalRedisClient(redis_client)

# Keys automatically converted
client.set("ticks:live:processed", "value")  # Uses canonical
client.get("ticks:intraday:processed")  # Also canonical
```

### Pattern 3: Using Key Mapper

```python
from shared_core.redis_clients import redis_key_mapper

# Convert any key to canonical
canonical = redis_key_mapper.get_canonical_key("ticks:live:processed")

# Resolve by category
key = redis_key_mapper.resolve_key("tick_storage.processed_stream")
```

---

## Best Practices

1. **Always use canonical_key_builder** for new code
2. **Wrap Redis clients** with CanonicalRedisClient for automatic conversion
3. **Use migration utility** to migrate existing keys
4. **Keep YAML config updated** with all key patterns
5. **Test with dry_run=True** before actual migration
6. **Monitor key usage** after migration to ensure consistency

---

## Troubleshooting

### Issue: Keys not converting

**Solution**: Ensure `redis_key_mapping.yaml` is loaded and contains the key mapping.

### Issue: Migration fails

**Solution**: Check Redis permissions and ensure keys exist before migration.

### Issue: Alias keys still in use

**Solution**: Run migration utility and optionally cleanup aliases after verification.

