# Registry Type Migration Guide

## Overview

This guide explains how to gradually migrate from `COMPREHENSIVE` to `INTRADAY_FOCUS` registry types using feature flags and performance monitoring.

## Architecture

### Registry Types

1. **COMPREHENSIVE**: All instruments from all brokers (~50,000+ instruments)
   - Use for: Order placement, manual trading, analysis
   - Pros: Complete coverage, no missing instruments
   - Cons: Higher memory usage, slower lookups

2. **INTRADAY_FOCUS**: Only NIFTY, BANKNIFTY, SENSEX futures and options (~430 instruments)
   - Use for: Real-time scanning, pattern detection, alert building
   - Pros: Fast lookups, low memory usage
   - Cons: Limited to focus instruments only

### Feature Flag System

Feature flags control which registry type each module uses:

```python
from shared_core.instrument_token.registry_config import get_registry_feature_flags

flags = get_registry_feature_flags()
# {
#     "use_intraday_focus_for_scanner": False,
#     "use_intraday_focus_for_patterns": False,
#     "use_intraday_focus_for_alerts": False,
#     "use_comprehensive_for_orders": True,  # Always True
#     "enable_performance_monitoring": True,
#     "enable_registry_caching": True,
# }
```

## Migration Steps

### Step 1: Enable Feature Flags Gradually

Start with one module at a time:

```bash
# Enable INTRADAY_FOCUS for scanner only
export REGISTRY_USE_INTRADAY_FOCUS_SCANNER=true

# Or set in code:
from shared_core.instrument_token.registry_config import REGISTRY_FEATURE_FLAGS
REGISTRY_FEATURE_FLAGS["use_intraday_focus_for_scanner"] = True
```

### Step 2: Monitor Performance

Check performance metrics:

```python
from shared_core.instrument_token.registry_helper import get_registry_stats

stats = get_registry_stats()
print(f"Average lookup time: {stats['performance_metrics']['avg_lookup_time_ms']:.2f}ms")
print(f"Cache hit rate: {stats['performance_metrics']['cache_hit_rate']:.2%}")
```

### Step 3: Update Modules

Use the registry helper instead of direct registry access:

```python
# OLD WAY (direct access)
from shared_core.instrument_token.instrument_registry import get_enhanced_instrument_service
registry = get_enhanced_instrument_service(RegistryType.COMPREHENSIVE)

# NEW WAY (with feature flags)
from shared_core.instrument_token.registry_helper import get_registry_for_module
registry = get_registry_for_module("scanner")  # Uses feature flags automatically
```

### Step 4: Use Monitoring Wrapper

Wrap registry lookups with monitoring:

```python
from shared_core.instrument_token.registry_helper import resolve_symbol_with_monitoring

# OLD WAY
result = registry.resolve_for_broker(symbol, broker)

# NEW WAY (with monitoring)
result = resolve_symbol_with_monitoring(registry, symbol, broker, module_name="scanner")
```

## Module-Specific Updates

### Scanner Module

```python
from shared_core.instrument_token.registry_helper import (
    get_scanner_registry,
    resolve_symbol_with_monitoring,
)

# In scanner initialization
self.registry = get_scanner_registry()

# In symbol resolution
result = resolve_symbol_with_monitoring(
    self.registry, symbol, broker, module_name="scanner"
)
```

### Pattern Detector

```python
from shared_core.instrument_token.registry_helper import (
    get_pattern_detector_registry,
    resolve_symbol_with_monitoring,
)

# In pattern detector initialization
self.registry = get_pattern_detector_registry()

# In symbol resolution
result = resolve_symbol_with_monitoring(
    self.registry, symbol, broker, module_name="pattern_detector"
)
```

### Order Manager

```python
from shared_core.instrument_token.registry_helper import (
    get_order_manager_registry,
    resolve_symbol_with_monitoring,
)

# Order manager defaults to INTRADAY_FOCUS (set use_comprehensive_for_orders to override)
self.registry = get_order_manager_registry()

# In symbol resolution
result = resolve_symbol_with_monitoring(
    self.registry, symbol, broker, module_name="order_manager"
)
```

## Performance Monitoring

### View Metrics

```python
from shared_core.instrument_token.registry_helper import get_registry_stats

stats = get_registry_stats()
print(stats)
# {
#     "cached_registries": ["COMPREHENSIVE", "INTRADAY_FOCUS"],
#     "cache_size": 2,
#     "performance_metrics": {
#         "lookup_count": 1000,
#         "avg_lookup_time_ms": 2.5,
#         "max_lookup_time_ms": 15.0,
#         "min_lookup_time_ms": 0.5,
#         "cache_hit_rate": 0.85,
#         ...
#     }
# }
```

### Reset Metrics

```python
from shared_core.instrument_token.registry_config import reset_performance_metrics

reset_performance_metrics()  # Useful for testing
```

## Environment Variables

Set feature flags via environment variables:

```bash
# Enable INTRADAY_FOCUS for scanner
export REGISTRY_USE_INTRADAY_FOCUS_SCANNER=true

# Enable INTRADAY_FOCUS for pattern detectors
export REGISTRY_USE_INTRADAY_FOCUS_PATTERNS=true

# Enable INTRADAY_FOCUS for alert builders
export REGISTRY_USE_INTRADAY_FOCUS_ALERTS=true

# Disable performance monitoring
export REGISTRY_ENABLE_MONITORING=false

# Disable registry caching
export REGISTRY_ENABLE_CACHING=false
```

## Configuration File

Create `shared_core/instrument_token/registry_config.json`:

```json
{
  "feature_flags": {
    "use_intraday_focus_for_scanner": true,
    "use_intraday_focus_for_patterns": false,
    "use_intraday_focus_for_alerts": false,
    "use_comprehensive_for_orders": true,
    "enable_performance_monitoring": true,
    "enable_registry_caching": true
  },
  "description": "Registry type feature flags for gradual migration"
}
```

## Troubleshooting

### Issue: Missing Instruments

If you see "instrument not found" errors after enabling INTRADAY_FOCUS:

1. Check if the symbol is in `focus_instruments.json`
2. Verify the symbol matches the focus set (NIFTY, BANKNIFTY, SENSEX)
3. Temporarily disable the feature flag for that module

### Issue: Slow Lookups

If lookups are slow:

1. Check performance metrics: `get_registry_stats()`
2. Verify registry caching is enabled
3. Consider using INTRADAY_FOCUS for high-frequency modules

### Issue: Feature Flags Not Working

1. Check environment variables are set correctly
2. Verify the config file exists and is valid JSON
3. Check logs for feature flag values

## Migration Checklist

- [ ] Create `registry_config.json` with initial flags (all False)
- [ ] Update `order_manager.py` to use `get_order_manager_registry()`
- [ ] Update scanner module to use `get_scanner_registry()`
- [ ] Update pattern detectors to use `get_pattern_detector_registry()`
- [ ] Enable feature flag for scanner (test)
- [ ] Monitor performance metrics
- [ ] Enable feature flag for pattern detectors (test)
- [ ] Monitor performance metrics
- [ ] Enable feature flag for alert builders (test)
- [ ] Monitor performance metrics
- [ ] Document any issues and fixes

## Best Practices

1. **Start Small**: Enable feature flags one module at a time
2. **Monitor Closely**: Watch performance metrics after each change
3. **Test Thoroughly**: Verify all instruments resolve correctly
4. **Keep COMPREHENSIVE for Orders**: Always use COMPREHENSIVE for order placement
5. **Use Monitoring**: Wrap all registry lookups with `resolve_symbol_with_monitoring()`
6. **Document Changes**: Update this guide with any issues or learnings
