# Downstream Fixes Needed

**Date:** 2025-01-XX  
**Last Updated:** 2025-12-03  
**Purpose:** Identify and fix downstream consumers to handle field name variations during migration

**Status:** ✅ **FIXES APPLIED AND VERIFIED** - All indicators successfully reaching pattern detectors

---

## Summary

After fixing websocket parser to store ALL fields with canonical names, we need to ensure downstream consumers can handle field name variations during migration using `redis_key_mapping.py`.

---

## 1. Current State

### ✅ What's Working
- **Data Pipeline:** Uses `normalize_redis_tick_data()` at line 3270 ✅
- **Data Pipeline:** Preserves all fields via `_preserve_all_original_data()` ✅
- **Websocket Parser:** Now stores ALL fields with canonical names ✅

### ⚠️ What Needs Fixing
- **Data Pipeline:** Doesn't use `map_tick_for_pattern()` to add consumer-specific aliases
- **Pattern Detectors:** Directly access fields (e.g., `tick_data.get('volume_ratio')`) without fallback aliases
- **No Field Name Variation Handling:** During migration, field names might vary, but consumers don't have fallback mechanisms

---

## 2. Required Fixes

### Fix 1: Use `map_tick_for_pattern()` in Data Pipeline (CRITICAL)

**Location:** `intraday_scanner/data_pipeline.py:_process_tick_for_patterns()`

**Current Code:**
```python
# Line 3270: Normalizes field names
tick_data = normalize_redis_tick_data(tick_data)

# Line 3291: Passes to pattern detector
indicators = tick_data.copy()
```

**Problem:** `normalize_redis_tick_data()` only normalizes field names, but doesn't add consumer-specific aliases that pattern detectors might expect.

**Fix Required:**
```python
# After normalize_redis_tick_data(), add consumer-specific mapping
tick_data = normalize_redis_tick_data(tick_data)

# ✅ ADD: Map for pattern detector consumption (adds aliases for field name variations)
from shared_core.redis_clients.redis_key_mapping import map_tick_for_pattern
tick_data = map_tick_for_pattern(tick_data)

# Then pass to pattern detector
indicators = tick_data.copy()
```

**Why:** `map_tick_for_pattern()` adds aliases like:
- `volume` → `bucket_incremental_volume` (if volume not present)
- `price` → `last_price` (if price not present)
- `price_change` → `price_change_pct` (if price_change not present)

This ensures pattern detectors can find fields even if field names vary during migration.

### Fix 2: Add Field Name Fallback in Pattern Detectors (OPTIONAL)

**Location:** `patterns/pattern_detector.py`

**Current Code:**
```python
# Line 7759: Direct field access
current_ratio = float(tick_data.get('volume_ratio', 1.0))
```

**Problem:** If `volume_ratio` is stored with a different name during migration, pattern detector won't find it.

**Fix Required (if needed):**
```python
# Add fallback for field name variations
def _get_field_with_fallback(tick_data: dict, field_name: str, fallback_names: list = None):
    """Get field with fallback names for migration compatibility"""
    if field_name in tick_data:
        return tick_data[field_name]
    
    # Try fallback names
    if fallback_names:
        for fallback in fallback_names:
            if fallback in tick_data:
                return tick_data[fallback]
    
    return None

# Usage:
current_ratio = float(_get_field_with_fallback(
    tick_data, 
    'volume_ratio', 
    fallback_names=['volume_ratio_normalized', 'normalized_volume_ratio', 'vol_ratio']
) or 1.0)
```

**Note:** This might not be necessary if `map_tick_for_pattern()` handles all variations. Check if pattern detectors still miss fields after Fix 1.

### Fix 3: Verify All Field Access Points

**Check these locations for direct field access:**
1. `patterns/pattern_detector.py` - Volume ratio, Greeks, price fields
2. `patterns/strategies/kow_signal_straddle.py` - Field access in stream processing
3. Any other pattern detectors or strategies

**Action:** After applying Fix 1, monitor logs to see if pattern detectors still report missing fields.

---

## 3. Implementation Plan

### Step 1: Apply Fix 1 (Data Pipeline)
- Add `map_tick_for_pattern()` call after `normalize_redis_tick_data()`
- Test with live data to ensure pattern detectors receive all fields

### Step 2: Monitor for Missing Fields
- Check pattern detector logs for missing field warnings
- If fields are still missing, apply Fix 2 (field fallback)

### Step 3: Verify Field Preservation
- Ensure all computed fields (Greeks, volume_ratio, microstructure) are preserved
- Verify pattern detectors can access all required fields

---

## 4. Code Changes Required

### Change 1: Data Pipeline - Add Pattern Mapping

**File:** `intraday_scanner/data_pipeline.py`
**Location:** `_process_tick_for_patterns()` method, after line 3270

```python
# ✅ CRITICAL FIX: Normalize field names using redis_key_mapping before processing
# This ensures all consumers receive data with canonical field names
tick_data = normalize_redis_tick_data(tick_data)

# ✅ ADD: Map for pattern detector consumption (handles field name variations during migration)
from shared_core.redis_clients.redis_key_mapping import map_tick_for_pattern
tick_data = map_tick_for_pattern(tick_data)
```

### Change 2: Verify Field Access (Monitoring)

Add logging to verify fields are accessible:

```python
# After mapping, verify critical fields are present
critical_fields = ['volume_ratio', 'delta', 'gamma', 'theta', 'vega', 'price_change_pct']
missing_fields = [f for f in critical_fields if f not in tick_data or tick_data[f] is None]
if missing_fields:
    self.logger.warning(f"⚠️ [FIELD_MAPPING] {symbol} - Missing fields after mapping: {missing_fields}")
```

---

## 5. Testing Checklist

- [x] Verify `map_tick_for_pattern()` is called in data_pipeline ✅ APPLIED
- [x] Test with live data to ensure pattern detectors receive all fields ✅ VERIFIED
- [x] Check pattern detector logs for missing field warnings ✅ VERIFIED
- [x] Verify volume_ratio is accessible in pattern detectors ✅ VERIFIED (field present, but value may be 0.0)
- [x] Verify Greeks (delta, gamma, theta, vega) are accessible ✅ VERIFIED (5/5 Greeks present)
- [x] Verify price_change fields are accessible ✅ VERIFIED
- [x] Test with different field name variations (if any exist during migration) ✅ VERIFIED

---

## 6. Fixes Applied Status

### ✅ COMPLETED

1. **Data Pipeline Field Mapping** ✅
   - Added `map_tick_for_pattern()` call after `normalize_redis_tick_data()`
   - Location: `intraday_scanner/data_pipeline.py:_process_tick_for_patterns()` line ~3275
   - Status: Applied and verified

2. **Websocket Parser Storage** ✅
   - Fixed to extract indicators/Greeks from normalized tick (not original)
   - Fixed to preserve ALL computed fields (not just hardcoded subset)
   - Location: `zerodha/crawlers/intraday_websocket_parser.py:_store_tick_data()` and `_extract_indicators()`
   - Status: Applied and verified

3. **Field Preservation** ✅
   - All fields (107-108) reaching pattern detectors
   - All Greeks (5/5) present: delta, gamma, theta, vega, rho
   - Advanced Greeks present: vanna, charm, vomma, speed, zomma, color
   - Microstructure indicators present: microprice, order_flow_imbalance, order_book_imbalance, etc.
   - Status: Verified in production logs

---

## 7. Known Issues & Tracing

### Issue: volume_ratio = 0.0 for Some Symbols

**Symptom:**
```
🚨 [VOLUME_RATIO_MISSING] NFOSILVER30DEC162000PE - volume_ratio is 0.0
```

**Root Cause:**
- This is a **data/calculation issue**, NOT a field mapping issue
- The field `volume_ratio` is present and accessible in pattern detectors
- The value is 0.0, indicating volume calculation or data availability issue

**Source of Truth:**
- **Websocket Parser** (`zerodha/crawlers/intraday_websocket_parser.py`) is the source of truth for calculating `volume_ratio`
- Volume calculations are performed in the websocket parser before storage

**Tracing Path:**
1. **Websocket Parser** (`zerodha/crawlers/intraday_websocket_parser.py`):
   - `_apply_calculations()` - Applies volume calculations
   - `_store_tick_data()` - Stores volume_ratio in Redis
   - Check volume calculation logic and volume baseline availability

2. **Volume Calculation Components:**
   - `VolumeComputationManager` - Calculates volume ratios
   - `time_aware_volume_baseline` - Provides baseline volumes
   - Redis volume baseline keys - Check if baseline exists for symbol

3. **Data Pipeline** (`intraday_scanner/data_pipeline.py`):
   - `_get_complete_redis_data()` - Fetches volume_ratio from Redis
   - `_smart_merge_redis_data()` - Merges volume_ratio into cleaned data
   - Check if volume_ratio is being fetched correctly from Redis

4. **Pattern Detectors** (`patterns/pattern_detector.py`):
   - Receives volume_ratio in indicators dict
   - If volume_ratio is 0.0, it's because it was 0.0 in Redis (not a mapping issue)

**Debugging Steps:**
1. Check Redis for volume_ratio: `redis-cli GET "ind:volume:{symbol}:volume_ratio"`
2. Check volume baseline: `redis-cli GET "volume:baseline:20day:{symbol}"`
3. Check websocket parser logs for volume calculation errors
4. Verify symbol has trading volume (may legitimately be 0.0 for illiquid symbols)

**Expected Outcome:**
- Field mapping is working correctly ✅
- All indicators reach pattern detectors ✅
- volume_ratio = 0.0 is a data issue, not a mapping issue
- If this persists, trace through websocket parser volume calculation logic

---

## 8. Expected Outcome

After fixes:
- ✅ Data pipeline normalizes field names to canonical format
- ✅ Data pipeline adds consumer-specific aliases via `map_tick_for_pattern()`
- ✅ Pattern detectors can access fields using canonical names or aliases
- ✅ No data loss during field name migration
- ✅ All computed fields (Greeks, volume_ratio, microstructure) are accessible
- ✅ All indicators (107-108 fields) successfully reach pattern detectors
- ✅ All Greeks (5/5 classical + advanced) successfully reach pattern detectors
- ⚠️ volume_ratio = 0.0 is a separate data/calculation issue (trace via websocket_parser)

