# Redis Key Migration Strategy

## Overview

This document outlines the recommended strategy for migrating from legacy/alias Redis keys to canonical keys.

## Migration Order: Enforce Downstream First, Then Migrate Data

**Recommended Approach**: Update consumers first, then migrate data.

### Why This Order?

1. **Zero Downtime**: Consumers can read from both alias and canonical keys during transition
2. **Backward Compatible**: Old keys continue to work while new code uses canonical keys
3. **Safe Rollback**: Easy to revert if issues occur
4. **Gradual Migration**: Can migrate data incrementally without breaking services

---

## Migration Phases

### Phase 1: Enforce Downstream (Consumers) ✅ DO THIS FIRST

**Goal**: Update all consumers to use canonical keys for reading.

**Steps**:

1. **Update Pattern Detector**
   ```python
   # intraday_trading/patterns/pattern_detector.py
   from shared_core.redis_clients import (
       canonical_key_builder,
       CanonicalRedisClient,
   )
   
   class PatternDetector:
       def __init__(self, redis_client):
           # Wrap with canonical client
           self.redis_client = CanonicalRedisClient(redis_client)
           self.tick_stream = canonical_key_builder.live_processed_stream()
   ```

2. **Update Data Pipeline**
   ```python
   # intraday_trading/intraday_scanner/data_pipeline.py
   from shared_core.redis_clients import (
       canonical_key_builder,
       CanonicalRedisClient,
       normalize_redis_tick_data,
   )
   
   class DataPipeline:
       def __init__(self, redis_client):
           self.redis_client = CanonicalRedisClient(redis_client)
       
       def _fetch_all_tick_details_from_redis(self, symbol: str):
           tick_key = canonical_key_builder.live_ticks_hash(symbol)
           tick_data = self.redis_client.hgetall(tick_key)  # Auto-converts
           return normalize_redis_tick_data(tick_data)
   ```

3. **Update Volume Baseline Services**
   ```python
   # shared_core/volume_files/time_aware_volume_baseline.py
   from shared_core.redis_clients import canonical_key_builder, CanonicalRedisClient
   
   class HistoricalVolumeBaseline:
       def __init__(self, redis_client):
           self.redis_client = CanonicalRedisClient(redis_client)
       
       def get_baseline(self, symbol: str):
           key = canonical_key_builder.volume_baseline(symbol)
           return self.redis_client.hgetall(key)  # Auto-converts
   ```

**Benefits**:
- ✅ Consumers automatically convert aliases to canonical keys
- ✅ Can read from both old and new keys during transition
- ✅ No data migration needed yet
- ✅ Zero downtime

---

### Phase 2: Dual-Write Period (Optional but Recommended)

**Goal**: Have crawlers write to both alias and canonical keys temporarily.

**Steps**:

1. **Update Crawlers to Write to Canonical Keys**
   ```python
   # zerodha/crawlers/intraday_crawler.py
   from shared_core.redis_clients import (
       canonical_key_builder,
       CanonicalRedisClient,
   )
   
   class IntradayCrawler(BaseCrawler):
       def __init__(self, redis_client, ...):
           # Wrap with canonical client
           self.redis_client = CanonicalRedisClient(redis_client)
       
       def write_tick_to_redis(self, symbol: str, tick_data: dict):
           # Use canonical key builder
           tick_hash_key = canonical_key_builder.live_ticks_hash(symbol)
           stream_key = canonical_key_builder.live_processed_stream()
           
           # Write to canonical keys (auto-converts if alias used)
           self.redis_client.hset(tick_hash_key, mapping=tick_data)
           self.redis_client.xadd(stream_key, tick_data, maxlen=10000)
   ```

2. **Temporary Dual-Write (Optional)**
   ```python
   # During transition, write to both for safety
   def write_tick_to_redis(self, symbol: str, tick_data: dict):
       # Canonical key (new)
       canonical_key = canonical_key_builder.live_ticks_hash(symbol)
       self.redis_client.hset(canonical_key, mapping=tick_data)
       
       # Alias key (old - temporary, remove after migration)
       alias_key = f"ticks:latest:{symbol}"  # Old format
       if alias_key != canonical_key:  # Only if different
           self.redis_client.hset(alias_key, mapping=tick_data)
   ```

**Benefits**:
- ✅ New data goes to canonical keys
- ✅ Old consumers can still read from alias keys
- ✅ New consumers read from canonical keys
- ✅ Safe transition period

---

### Phase 3: Migrate Existing Data

**Goal**: Copy existing data from alias keys to canonical keys.

**Steps**:

1. **Dry Run First**
   ```python
   # scripts/migrate_redis_keys.py
   from shared_core.redis_clients import RedisKeyMigrator
   import redis
   
   redis_client = redis.Redis(host='localhost', port=6379, db=1)
   migrator = RedisKeyMigrator(redis_client)
   
   # Step 1: Check what would be migrated
   print("=== DRY RUN ===")
   stats = migrator.migrate_keys(dry_run=True)
   print(f"Would migrate: {stats['migrated']} keys")
   print(f"Would skip: {stats['skipped']} keys")
   ```

2. **Find Alias Keys**
   ```python
   # Step 2: Find all alias keys in Redis
   found_aliases = migrator.find_alias_keys("*")
   for canonical, aliases in found_aliases.items():
       print(f"Canonical: {canonical}")
       print(f"  Aliases: {aliases}")
   ```

3. **Perform Migration**
   ```python
   # Step 3: Migrate data
   print("\n=== MIGRATION ===")
   stats = migrator.migrate_keys(dry_run=False)
   print(f"Migrated: {stats['migrated']} keys")
   print(f"Failed: {stats['failed']} keys")
   ```

**Benefits**:
- ✅ Historical data available in canonical format
- ✅ All consumers can use canonical keys
- ✅ Consistent data access

---

### Phase 4: Verification

**Goal**: Verify all consumers are using canonical keys and data is accessible.

**Steps**:

1. **Check Consumer Usage**
   ```python
   # Verify consumers are using canonical keys
   from shared_core.redis_clients import redis_key_mapper
   
   # Check if key is managed
   assert redis_key_mapper.is_key_managed("ticks:intraday:processed")
   
   # Verify canonical conversion
   canonical = redis_key_mapper.get_canonical_key("ticks:live:processed")
   assert canonical == "ticks:intraday:processed"
   ```

2. **Monitor Redis Keys**
   ```bash
   # Check canonical keys exist
   redis-cli KEYS "ticks:intraday:processed"
   redis-cli KEYS "ticks:*"
   
   # Verify data in canonical keys
   redis-cli HGETALL "ticks:RELIANCE"
   ```

3. **Test Consumer Access**
   ```python
   # Test that consumers can read from canonical keys
   from shared_core.redis_clients import CanonicalRedisClient
   
   client = CanonicalRedisClient(redis_client)
   data = client.hgetall("ticks:RELIANCE")  # Should work
   ```

---

### Phase 5: Cleanup (Optional)

**Goal**: Remove alias keys after verification.

**Steps**:

1. **Cleanup Alias Keys**
   ```python
   # Only after verifying all consumers use canonical keys
   migrator = RedisKeyMigrator(redis_client)
   
   # Dry run cleanup
   deleted_count = migrator.cleanup_aliases(dry_run=True, after_migration=True)
   print(f"Would delete {deleted_count} alias keys")
   
   # Actual cleanup (use with caution!)
   if input("Proceed with cleanup? (yes/no): ").lower() == 'yes':
       deleted = migrator.cleanup_aliases(dry_run=False, after_migration=True)
       print(f"Deleted {deleted} alias keys")
   ```

**WARNING**: Only cleanup after:
- ✅ All consumers updated to use canonical keys
- ✅ All data migrated
- ✅ Verification complete
- ✅ Monitoring shows no issues

---

## Recommended Timeline

### Week 1: Enforce Downstream
- Update all consumers to use `CanonicalRedisClient`
- Update all consumers to use `canonical_key_builder`
- Test that consumers can read from both alias and canonical keys

### Week 2: Update Crawlers
- Update crawlers to write to canonical keys
- Optional: Enable dual-write for safety
- Monitor for issues

### Week 3: Migrate Data
- Run dry-run migration
- Review migration plan
- Execute migration
- Verify data integrity

### Week 4: Verification & Cleanup
- Verify all consumers working
- Monitor for issues
- Optional: Cleanup alias keys

---

## Quick Start: Enforce Downstream First

### Step 1: Update One Consumer (Test)

```python
# intraday_trading/patterns/pattern_detector.py
from shared_core.redis_clients import (
    canonical_key_builder,
    CanonicalRedisClient,
)

class PatternDetector:
    def __init__(self, redis_client):
        # ✅ CHANGE: Wrap with canonical client
        self.redis_client = CanonicalRedisClient(redis_client)
        
        # ✅ CHANGE: Use canonical key builder
        self.tick_stream = canonical_key_builder.live_processed_stream()
    
    def consume_ticks(self):
        # ✅ Now automatically uses canonical keys
        messages = self.redis_client.xread(
            {self.tick_stream: "0-0"},
            count=100
        )
```

### Step 2: Test

```python
# Verify it works
# Consumer can now read from both:
# - Old alias: "ticks:live:processed" → auto-converts to canonical
# - New canonical: "ticks:intraday:processed" → works directly
```

### Step 3: Roll Out to All Consumers

Update all consumers one by one:
1. Pattern detector ✅
2. Data pipeline ✅
3. Volume baseline services ✅
4. Other consumers ✅

### Step 4: Migrate Data (After All Consumers Updated)

```python
# Now safe to migrate data
migrator = RedisKeyMigrator(redis_client)
stats = migrator.migrate_keys(dry_run=False)
```

---

## Answer: Enforce Downstream First

**✅ RECOMMENDED**: Enforce downstream (consumers) first, then migrate data.

**Reasoning**:
1. **Zero Risk**: Consumers can read from both keys during transition
2. **No Downtime**: Services continue working with old keys
3. **Safe Migration**: Data migration can happen gradually
4. **Easy Rollback**: Can revert consumer changes if needed
5. **Proven Pattern**: Standard migration strategy for key changes

**Migration Order**:
1. ✅ **Phase 1**: Update consumers to use canonical keys (READ operations)
2. ✅ **Phase 2**: Update crawlers to write canonical keys (WRITE operations)
3. ✅ **Phase 3**: Migrate existing data from aliases to canonical
4. ✅ **Phase 4**: Verify everything works
5. ⚠️ **Phase 5**: Cleanup alias keys (optional, after verification)

---

## Checklist

### Before Migration
- [ ] All consumers updated to use `CanonicalRedisClient`
- [ ] All consumers updated to use `canonical_key_builder`
- [ ] Tested that consumers can read from both alias and canonical keys
- [ ] Monitoring in place

### During Migration
- [ ] Dry-run migration completed
- [ ] Migration plan reviewed
- [ ] Data migration executed
- [ ] Verification completed

### After Migration
- [ ] All consumers verified working
- [ ] Monitoring shows no issues
- [ ] Optional: Alias keys cleaned up

