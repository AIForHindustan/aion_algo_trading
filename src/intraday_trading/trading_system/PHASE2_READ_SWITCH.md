# Phase 2: Switch Reads to New Structure

## Overview

Phase 2 implements the "Read from New (1 week)" migration strategy:
- **Primary**: Read from NEW unified structure (DB 1)
- **Fallback**: Read from OLD structure (DB 0, 2, 5) if data not found
- **Monitoring**: Track read patterns and fallback rates

## Implementation

### 1. UnifiedRedisReader

New unified reader class that handles all read operations:

```python
from trading_system.storage.unified_reader import UnifiedRedisReader

reader = UnifiedRedisReader()

# Read tick data (tries new structure first)
tick_data = reader.read_tick('NIFTY')

# Read volume state
volume_state = reader.read_volume_state('NIFTY')

# Read indicator
rsi = reader.read_indicator('NIFTY', 'rsi')

# Read OHLC
ohlc = reader.read_ohlc('NIFTY')
```

### 2. TransitionalManager Updates

`TransitionalManager.read_tick()` now:
- ✅ Always tries NEW structure first
- ✅ Falls back to OLD structure if not found
- ✅ Tracks read statistics

### 3. Key Mappings

#### Volume Data
- **NEW**: `vol:state:{symbol}` (DB 1)
- **OLD**: `volume_state:{token}` (DB 2)

#### Indicators
- **NEW**: `ind:ta:{symbol}:rsi` (DB 1)
- **OLD**: `indicators:{symbol}:rsi` (DB 5)

#### OHLC Data
- **NEW**: `ohlc_latest:{symbol}` (DB 1)
- **OLD**: `ohlc_latest:{symbol}` (DB 2)

#### Tick Data
- **NEW**: `tick:latest:{symbol}` or `ticks:realtime:{symbol}` (DB 1)
- **OLD**: `tick:{symbol}` (DB 0)

## Migration Steps

### Step 1: Update Components to Use UnifiedRedisReader

Replace direct Redis reads with UnifiedRedisReader:

```python
# OLD
volume_data = redis_client.hgetall(f"volume_state:{token}")

# NEW
from trading_system.storage.unified_reader import UnifiedRedisReader
reader = UnifiedRedisReader()
volume_data = reader.read_volume_state(symbol, token=token)
```

### Step 2: Monitor Read Patterns

Check read statistics:

```python
stats = reader.get_read_stats()
print(f"New reads: {stats['new_reads']}")
print(f"Old fallbacks: {stats['old_fallbacks']}")
print(f"New read percentage: {stats['new_read_percentage']:.2f}%")
```

### Step 3: Verify Fallback Rate

Monitor fallback rate - should decrease over time as more data migrates:
- Week 1: ~20-30% fallbacks (expected)
- Week 2: ~10-15% fallbacks
- Week 3: <5% fallbacks (ready for Phase 3)

## Benefits

1. **Zero Downtime**: Old structure still accessible during transition
2. **Gradual Migration**: Components can migrate at their own pace
3. **Data Safety**: Fallback ensures no data loss
4. **Performance**: New structure optimized for reads
5. **Monitoring**: Track migration progress via read statistics

## Next Steps

After Phase 2 (1 week):
1. Monitor read statistics
2. Verify fallback rate < 5%
3. Proceed to Phase 3: Cutover (stop writing to old structure)

