# Redis Storage Schema Mapping

## Overview

This document explains how Redis storage schema matches `config/optimized_field_mapping.yaml` and how to use the key mapping utility for downstream consumers.

## Schema Matching

### Storage Behavior

**Redis Storage** (`shared_core/redis_clients/redis_storage.py`):
- **Stores fields with canonical names** from `optimized_field_mapping.yaml`
- Applies field mapping during storage using `resolve_session_field()` and `resolve_calculated_field()`
- Fields are normalized to canonical names before storing in Redis hashes/streams

**Field Mapping Flow**:
```
Raw Tick Data (from crawler)
    ↓
Field Mapping (resolve_session_field/resolve_calculated_field)
    ↓
Canonical Field Names (from YAML)
    ↓
Redis Storage (hset with canonical field names)
```

### YAML Config Structure

The `config/optimized_field_mapping.yaml` defines three main field categories:

1. **`websocket_core_fields`**: Core WebSocket fields from Zerodha API
2. **`redis_session_fields`**: Standardized Redis session field names
3. **`calculated_indicator_fields`**: Calculated indicator field names

### Example Field Mappings

From `redis_session_fields`:
- `zerodha_cumulative_volume` → `volume_traded_for_the_day` (canonical)
- `bucket_incremental_volume` → `bucket_incremental_volume` (canonical)
- `volume` → `volume` (maps to incremental volume)
- `last_price` → `last_price` (canonical)

From `calculated_indicator_fields`:
- `volume_ratio` → `volume_ratio` (canonical)
- `price_change` → `price_change` (canonical)
- `price_change_pct` → `price_change_pct` (canonical)

## Key Mapping Utility

### Purpose

The `RedisKeyMapping` utility (`shared_core/redis_clients/redis_key_mapping.py`) provides:
- Field name normalization (stored → canonical)
- Field name resolution (canonical → stored)
- Consumer-specific field mapping
- Field validation against YAML config

### Usage Examples

#### 1. Normalize Redis Tick Data

```python
from shared_core.redis_clients.redis_key_mapping import normalize_redis_tick_data

# Read tick data from Redis
tick_data = redis_client.hgetall("ticks:latest:RELIANCE")

# Normalize to canonical field names
normalized = normalize_redis_tick_data(tick_data)
# Returns: {'last_price': '2500.0', 'bucket_incremental_volume': '1000', ...}
```

#### 2. Map for Pattern Detector

```python
from shared_core.redis_clients.redis_key_mapping import map_tick_for_pattern

tick_data = redis_client.hgetall("ticks:latest:RELIANCE")
pattern_data = map_tick_for_pattern(tick_data)
# Returns: {
#   'last_price': '2500.0',
#   'price': '2500.0',  # Alias for patterns
#   'bucket_incremental_volume': '1000',
#   'volume': '1000',  # Alias for patterns
#   'volume_ratio': '1.5',
#   'price_change_pct': '2.5',
#   'price_change': '2.5',  # Alias for patterns
#   ...
# }
```

#### 3. Map for Scanner

```python
from shared_core.redis_clients.redis_key_mapping import map_tick_for_scanner

tick_data = redis_client.hgetall("ticks:latest:RELIANCE")
scanner_data = map_tick_for_scanner(tick_data)
# Returns: {
#   'last_price': '2500.0',
#   'bucket_incremental_volume': '1000',
#   'volume': '1000',  # Alias
#   'zerodha_cumulative_volume': '50000',
#   'cumulative_volume': '50000',  # Alias
#   ...
# }
```

#### 4. Map for Dashboard

```python
from shared_core.redis_clients.redis_key_mapping import map_tick_for_dashboard

tick_data = redis_client.hgetall("ticks:latest:RELIANCE")
dashboard_data = map_tick_for_dashboard(tick_data)
# Returns: {
#   'last_price': '2500.0',
#   'current_price': '2500.0',  # Display alias
#   'bucket_incremental_volume': '1000',
#   'volume': '1000',  # Display alias
#   ...
# }
```

#### 5. Advanced: Direct Mapping Class

```python
from shared_core.redis_clients.redis_key_mapping import get_redis_key_mapping

mapper = get_redis_key_mapping()

# Get bidirectional mapping for a field
mapping = mapper.get_field_mapping('volume')
# Returns: {
#   'stored': 'bucket_incremental_volume',
#   'canonical': 'volume',
#   'direction': 'stored_to_canonical'
# }

# Validate tick data
validation = mapper.validate_tick_data_fields(tick_data)
# Returns: {
#   'valid': True,
#   'normalized': {...},
#   'missing_fields': [],
#   'unknown_fields': []
# }
```

## Field Name Consistency

### Storage (Redis)

Fields are stored with **canonical names** from YAML:
- `last_price` (not `price`)
- `bucket_incremental_volume` (not `incremental_volume`)
- `volume_ratio` (not `vol_ratio`)
- `price_change_pct` (not `price_change`)

### Consumption (Patterns/Scanners)

Consumers can use either:
1. **Canonical names** (recommended): `last_price`, `bucket_incremental_volume`
2. **Consumer-specific aliases**: Use mapping utility to get aliases like `price`, `volume`

### Backward Compatibility

The mapping utility handles backward compatibility:
- Unknown fields are preserved as-is
- Legacy field names are mapped to canonical names
- Both stored and canonical names are supported

## Integration Points

### 1. Data Pipeline

```python
# In data_pipeline.py
from shared_core.redis_clients.redis_key_mapping import normalize_redis_tick_data

def _fetch_all_tick_details_from_redis(self, symbol: str):
    tick_data = redis_client.hgetall(f"ticks:latest:{symbol}")
    return normalize_redis_tick_data(tick_data)
```

### 2. Pattern Detector

```python
# In pattern_detector.py
from shared_core.redis_clients.redis_key_mapping import map_tick_for_pattern

def detect_patterns(self, tick_data: Dict):
    # Map to pattern-expected field names
    pattern_data = map_tick_for_pattern(tick_data)
    volume_ratio = pattern_data.get('volume_ratio')
    price_change = pattern_data.get('price_change')
    # ... use mapped fields
```

### 3. Scanner

```python
# In scanner_main.py
from shared_core.redis_clients.redis_key_mapping import map_tick_for_scanner

def process_tick(self, tick_data: Dict):
    scanner_data = map_tick_for_scanner(tick_data)
    volume = scanner_data.get('volume')
    cumulative_volume = scanner_data.get('cumulative_volume')
    # ... use mapped fields
```

## Verification

### Check Schema Match

```python
from shared_core.redis_clients.redis_key_mapping import get_redis_key_mapping
from shared_core.config_utils.yaml_field_loader import get_yaml_config

mapper = get_redis_key_mapping()
yaml_config = get_yaml_config()

# Verify session fields match
session_fields = yaml_config.get("redis_session_fields", {})
for stored, canonical in session_fields.items():
    resolved = mapper.resolve_canonical_field(canonical)
    assert resolved == stored or resolved == canonical, f"Mismatch: {stored} -> {canonical} -> {resolved}"
```

## Summary

✅ **Redis storage schema matches YAML config**: Fields are stored with canonical names from `optimized_field_mapping.yaml`

✅ **Key mapping utility available**: Use `RedisKeyMapping` or convenience functions for consumer-specific field mapping

✅ **Backward compatible**: Unknown fields preserved, legacy names supported

✅ **Consistent across consumers**: All consumers can use canonical names or get consumer-specific aliases via mapping utility

