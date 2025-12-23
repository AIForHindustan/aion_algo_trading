# ClickHouse Instrument Manager - Centralized Registry Integration

## Overview

The `ClickHouseInstrumentManager` has been updated to use the **centralized instrument registry** from `shared_core/instrument_token/instrument_registry.py`. This ensures a **single source of truth** for all instrument data across the entire system.

## Key Changes

### 1. Centralized Registry Integration
- ✅ Uses `get_unified_registry()` singleton from `shared_core/instrument_token/instrument_registry.py`
- ✅ Automatically loads instruments from:
  - **Zerodha**: `zerodha/core/data/token_lookup_enriched.json`
  - **Angel One**: `shared_core/instrument_token/instrument_csv_files/angel_one_instruments_master.json`

### 2. Updated Imports
```python
from shared_core.instrument_token import (
    ClickHouseInstrumentManager,
    get_unified_registry
)
```

### 3. Usage Pattern
```python
from clickhouse_driver import Client
from shared_core.instrument_token import ClickHouseInstrumentManager, get_unified_registry

# Initialize ClickHouse client
clickhouse_client = Client(
    host='localhost',
    port=9000,
    user='default',
    password='',
    database='trading'
)

# Get unified registry (singleton - single source of truth)
unified_registry = get_unified_registry()

# Initialize ClickHouse Instrument Manager
# It will automatically use the centralized registry if not provided
ch_manager = ClickHouseInstrumentManager(
    unified_registry=unified_registry,  # Optional - will use get_unified_registry() if None
    clickhouse_client=clickhouse_client
)

# Sync instruments to ClickHouse
ch_manager.sync_instruments_to_clickhouse()

# Run correlation analysis
correlation_data = ch_manager.get_correlation_data([
    'NSE:RELIANCE',
    'NSE:TCS',
    'NSE:INFY'
])
```

## Business Logic Preservation

### ✅ All Original Functionality Maintained
1. **`create_instrument_tables()`**: Creates `instrument_metadata` and `broker_instruments` tables
2. **`sync_instruments_to_clickhouse()`**: Syncs all instruments from registry to ClickHouse
3. **`get_correlation_data()`**: Retrieves instrument data for correlation analysis

### ✅ Enhanced Features
- Better error handling and logging
- Support for both `clickhouse-driver` and `clickhouse-connect` clients
- Automatic table creation before sync
- Proper type hints for better IDE support

## File Structure

```
shared_core/instrument_token/
├── __init__.py                          # Exports for easy imports
├── instrument_registry.py               # Centralized registry (single source of truth)
├── clickhouse_instrument_manager.py     # ClickHouse integration (updated)
├── example_usage.py                     # Usage examples
├── CLICKHOUSE_INTEGRATION.md            # This file
└── instrument_csv_files/                # Source CSV/JSON files
    ├── angel_one_instruments_master.json
    ├── angel_one_unmapped_instruments.json
    ├── zerodha_instruments_latest.csv
    └── README.md
```

## Single Source of Truth

All instrument data now flows from:
1. **Source Files** → `instrument_csv_files/` directory
2. **Centralized Registry** → `instrument_registry.py` (UnifiedInstrumentRegistry)
3. **ClickHouse Tables** → `instrument_metadata` and `broker_instruments`

## Benefits

1. **Consistency**: All scripts use the same instrument data
2. **Maintainability**: Update instruments in one place
3. **Traceability**: Source files are clearly identified
4. **Scalability**: Easy to add new brokers via adapters

## Migration Notes

- ✅ No breaking changes - existing code continues to work
- ✅ Backward compatible - can still pass `unified_registry` explicitly
- ✅ Automatic fallback to singleton if `unified_registry=None`

## Testing

```bash
# Test imports
python3 -c "from shared_core.instrument_token import ClickHouseInstrumentManager, get_unified_registry; print('✅ Imports work correctly')"
```

## Next Steps

1. Update any remaining scripts that use old import paths
2. Ensure all analytics scripts use the centralized registry
3. Monitor for any issues with instrument resolution


