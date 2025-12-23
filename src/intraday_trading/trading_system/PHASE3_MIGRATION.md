# Phase 3: Historical Data Migration

## Overview

Phase 3 migrates all historical data from the old Redis structure (multiple databases) to the new unified structure (DB 1).

## Usage

### Basic Usage

```python
from trading_system.storage.data_migrator import SafeRedisConsolidator

# Initialize migrator
migrator = SafeRedisConsolidator()

# Migrate all historical data
stats = migrator.migrate_historical_data()
```

### Dry Run (Test Mode)

```python
# Test migration without writing
stats = migrator.migrate_historical_data(dry_run=True, limit=100)
```

### Command Line Usage

```bash
# Run historical migration
python -m trading_system.storage.data_migrator --phase historical

# Dry run with limit
python -m trading_system.storage.data_migrator --phase historical --dry-run --limit 100

# Verify migration
python -m trading_system.storage.data_migrator --phase verify
```

## Migration Steps

The `migrate_historical_data()` method performs 6 steps:

1. **Volume Data** (DB 2 → DB 1)
   - `volume_state:*` → `vol:state:*`
   - `volume_averages:*` → `vol:baseline:*`

2. **Indicator Data** (DB 5 → DB 1)
   - `indicators:{symbol}:rsi` → `ind:ta:{symbol}:rsi`
   - `indicators:{symbol}:delta` → `ind:greeks:{symbol}:delta`
   - `indicators:{symbol}:volume_ratio` → `ind:volume:{symbol}:ratio`

3. **System Data** (DB 0 → DB 1)
   - `session:*` → `sys:session:*`
   - `system:*` → `sys:system:*`
   - `health:*` → `sys:health:*`

4. **Historical Tick Data**
   - Uses existing `volume_migration.py` script
   - Migrates from streams and hash keys

5. **OHLC Data**
   - `ohlc_latest:*`, `ohlc:*`, `ohlc_stats:*`
   - Preserves key structure

6. **Baseline Data**
   - `volume:baseline:*`
   - Preserves key structure

## Migration Statistics

The method returns a dictionary with:

```python
{
    'start_time': '2025-01-XX...',
    'end_time': '2025-01-XX...',
    'dry_run': False,
    'volume_migrated': 1234,
    'indicators_migrated': 567,
    'system_migrated': 89,
    'historical_ticks_migrated': 10000,
    'ohlc_migrated': 234,
    'baseline_migrated': 56,
    'errors': 0,
    'total_keys_migrated': 11180
}
```

## Safety Features

- ✅ **Idempotent**: Can be run multiple times safely
- ✅ **Dry Run Mode**: Test before actual migration
- ✅ **Limit Option**: Test with limited keys
- ✅ **Error Handling**: Continues on errors, reports at end
- ✅ **Migration Report**: Saves JSON report with timestamp
- ✅ **TTL Preservation**: Sets appropriate TTLs on migrated keys

## Key Mappings

### Volume Data
- `volume_state:{token}` → `vol:state:{token}`
- `volume_averages:{symbol}` → `vol:baseline:{symbol}`

### Indicator Data
- `indicators:{symbol}:rsi` → `ind:ta:{symbol}:rsi`
- `indicators:{symbol}:macd` → `ind:ta:{symbol}:macd`
- `indicators:{symbol}:delta` → `ind:greeks:{symbol}:delta`
- `indicators:{symbol}:gamma` → `ind:greeks:{symbol}:gamma`
- `indicators:{symbol}:volume_ratio` → `ind:volume:{symbol}:ratio`

### System Data
- `session:*` → `sys:session:*`
- `system:*` → `sys:system:*`
- `health:*` → `sys:health:*`

## Integration with Existing Scripts

The migration integrates with:
- `redis_files/volume_migration.py` - For historical tick migration
- Existing volume and indicator migration logic
- UnifiedRedisManager for optimized connections

## Next Steps

After Phase 3:
1. Verify migration with `--phase verify`
2. Monitor system performance
3. Proceed to Phase 4: Cutover (stop writing to old structure)

