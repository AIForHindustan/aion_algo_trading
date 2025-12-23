# Instrument Registry System - Overview

## Architecture

The instrument registry system provides a unified, broker-agnostic interface for instrument lookups with support for different registry types optimized for different use cases.

### Registry Types

1. **COMPREHENSIVE** (`RegistryType.COMPREHENSIVE`)
   - Contains all instruments from all brokers (~50,000+ instruments)
   - Use for: Order placement, manual trading, analysis
   - Memory: ~50-100 MB
   - Lookup time: ~2-5ms average

2. **INTRADAY_FOCUS** (`RegistryType.INTRADAY_FOCUS`)
   - Contains only NIFTY, BANKNIFTY, SENSEX futures and options (~430 instruments)
   - Use for: Real-time scanning, pattern detection, alert building
   - Memory: ~5-10 MB
   - Lookup time: ~0.5-1ms average

### Key Components

#### 1. Registry Factory (`registry_factory.py`)
- Creates and manages registry instances
- Singleton pattern per registry type
- Lazy initialization

#### 2. Feature Flags (`registry_config.py`)
- Controls which registry type each module uses
- Environment variable support
- Config file support

#### 3. Registry Helper (`registry_helper.py`)
- Unified interface for getting registries
- Automatic registry type selection based on module
- Performance monitoring wrapper

#### 4. Registry Synchronizer (`instrument_registry.py::RegistrySynchronizer`)
- Syncs new instruments from COMPREHENSIVE to INTRADAY_FOCUS
- Maintains focus set consistency

#### 5. Sync Monitor (`registry_sync_monitor.py`)
- Monitors synchronization status
- Tracks sync history and statistics
- Alerts on sync issues

## File Structure

### Active Files (DO NOT REMOVE)

#### Crawler Files
- `zerodha/crawlers/binary_crawler1/enriched_metadata_binary_crawler.json`
- `zerodha/crawlers/binary_crawler1/intraday_crawler_instruments.json`
- `zerodha/crawlers/binary_crawler1/hot_token_metadata.json`
- `zerodha/crawlers/intraday_zerodha_token.json`
- `zerodha/crawlers/master_token/nifty50_tokens.json`
- `zerodha/ubuntu_crawlers/crawlers/*/hot_token_metadata.json`
- `zerodha_websocket/crawlers/master_token/nifty50_tokens.json`
- `clickhouse_setup/zerodha_token_lookup_consolidated.json`

#### ML Backend Files
- `shared_core/instrument_token/token_lookup_enriched.json` ⚠️ **Used by ml_backend**

#### Registry System Files
- `shared_core/instrument_token/focus_instruments.json` - Focus symbol list
- `shared_core/instrument_token/canonical_instruments.yaml` - Canonical instrument definitions
- `shared_core/instrument_token/instrument_csv_files/*.csv` - Source CSV files
- `shared_core/instrument_token/instrument_csv_files/angel_one_instruments_master.json` - Angel One master

#### Enriched Metadata Files
- `shared_core/intraday/broker_instrument_enriched_tokens/angel_one_intraday_enriched_token.json`
- `shared_core/intraday/broker_instrument_enriched_tokens/binary_crawler1_enriched_metadata.json`

### Deprecated Files (Safe to Remove)

These files are no longer used by the registry system:
- `jsonl/zerodha_token_lookup_consolidated.json` (duplicate)
- `intraday_trading/comprehensive_session_lookup.json` (replaced by registry)
- `intraday_trading/indices_tokens_final.json` (replaced by focus_instruments.json)
- `intraday_trading/session_tokens_metadata.json` (replaced by registry)
- Temporary analysis/report files in `clickhouse_setup/` and `intraday_trading/config/`

## Usage

### Basic Usage

```python
from shared_core.instrument_token.registry_helper import (
    get_registry_for_module,
    resolve_symbol_with_monitoring,
)

# Get registry for a module (uses feature flags)
registry = get_registry_for_module("scanner")

# Resolve symbol with monitoring
result = resolve_symbol_with_monitoring(
    registry, "NIFTY25DEC26450CE", "zerodha", module_name="scanner"
)
```

### Convenience Functions

```python
from shared_core.instrument_token.registry_helper import (
    get_scanner_registry,
    get_pattern_detector_registry,
    get_order_manager_registry,
    get_alert_builder_registry,
)

# Module-specific registries
scanner_registry = get_scanner_registry()
pattern_registry = get_pattern_detector_registry()
order_registry = get_order_manager_registry()  # INTRADAY_FOCUS by default (set use_comprehensive_for_orders to override)
alert_registry = get_alert_builder_registry()
```

### Feature Flags

```bash
# Enable INTRADAY_FOCUS for scanner
export REGISTRY_USE_INTRADAY_FOCUS_SCANNER=true

# Enable INTRADAY_FOCUS for pattern detectors
export REGISTRY_USE_INTRADAY_FOCUS_PATTERNS=true

# Enable INTRADAY_FOCUS for alert builders
export REGISTRY_USE_INTRADAY_FOCUS_ALERTS=true
```

### Monitoring

```python
from shared_core.instrument_token.registry_helper import get_registry_stats
from shared_core.instrument_token.registry_sync_monitor import get_registry_sync_monitor

# Get performance metrics
stats = get_registry_stats()
print(f"Avg lookup time: {stats['performance_metrics']['avg_lookup_time_ms']:.2f}ms")

# Check sync status
monitor = get_registry_sync_monitor()
status = await monitor.check_sync_status()
print(f"Sync needed: {status['sync_needed']}")
```

## Migration Status

Check migration status:
```bash
python shared_core/instrument_token/migration_status.py
```

## Registry Sync

Monitor and sync registries:
```bash
# Check sync status
python -m shared_core.instrument_token.registry_sync_monitor --check

# Perform sync
python -m shared_core.instrument_token.registry_sync_monitor --sync

# Show statistics
python -m shared_core.instrument_token.registry_sync_monitor --stats

# Export report
python -m shared_core.instrument_token.registry_sync_monitor --export report.json
```

## Cleanup

Remove old/duplicate files:
```bash
# Dry run (safe)
python shared_core/instrument_token/cleanup_old_token_files.py --dry-run

# Actually remove files
python shared_core/instrument_token/cleanup_old_token_files.py --no-dry-run
```

## Best Practices

1. **Always use registry helper** - Don't access registries directly
2. **Use monitoring wrapper** - Wrap lookups with `resolve_symbol_with_monitoring()`
3. **Check feature flags** - Use `get_registry_feature_flags()` to see current settings
4. **Monitor performance** - Regularly check `get_registry_stats()`
5. **Sync regularly** - Run registry sync to keep INTRADAY_FOCUS up to date
6. **Keep crawler files** - Never remove files in `zerodha/crawlers/` or `ml_backend/`

## Troubleshooting

### Missing Instruments

If instruments are missing after enabling INTRADAY_FOCUS:
1. Check if symbol is in `focus_instruments.json`
2. Run registry sync: `python -m shared_core.instrument_token.registry_sync_monitor --sync`
3. Temporarily disable feature flag for that module

### Slow Lookups

1. Check performance metrics: `get_registry_stats()`
2. Verify registry caching is enabled
3. Consider using INTRADAY_FOCUS for high-frequency modules

### Sync Issues

1. Check sync status: `python -m shared_core.instrument_token.registry_sync_monitor --check`
2. Review sync history: `python -m shared_core.instrument_token.registry_sync_monitor --history 20`
3. Export report for analysis: `python -m shared_core.instrument_token.registry_sync_monitor --export report.json`
