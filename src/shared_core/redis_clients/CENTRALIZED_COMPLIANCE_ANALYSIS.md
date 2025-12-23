# Centralized Redis Compliance Analysis

**Date:** 2025-01-XX  
**Purpose:** Identify which code follows centralized Redis standards and which violates them

---

## Executive Summary

**CRITICAL FINDING:** `redis_key_mapping.py` provides field normalization but is **NOT being enforced** at storage time, leading to potential data loss and field name inconsistencies.

---

## 1. What `redis_key_mapping.py` Provides

### Purpose
`redis_key_mapping.py` provides:
1. **Field Name Normalization:** Maps stored field names → canonical field names
2. **Field Preservation:** Preserves ALL fields including computed indicators
3. **Consumer Mapping:** Maps fields for different consumers (pattern, scanner, dashboard)
4. **Key Resolution:** Resolves Redis key aliases to canonical keys

### Key Function: `normalize_redis_tick_data()`
```python
def normalize_redis_tick_data(tick_data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize Redis tick data to canonical field names"""
    mapper = get_redis_key_mapping()
    return mapper.normalize_stored_fields(tick_data)
```

**What it does:**
- Maps stored field names (as they appear in Redis) to canonical names
- **Preserves ALL fields** including unknown/computed fields
- Ensures no data loss during normalization

---

## 2. Compliance Analysis

### ✅ FOLLOWING Centralized Way

#### 2.1 Websocket Parser - Storage (PARTIALLY COMPLIANT)
**File:** `zerodha/crawlers/intraday_websocket_parser.py`

**What it does correctly:**
- ✅ Uses `normalize_redis_tick_data()` before storing (line 1136)
- ✅ Uses key builder methods (`live_ticks_latest()`, `store_indicators()`) which normalize symbols internally
- ✅ Stores indicators via `store_indicators()` (centralized storage)

**What it does incorrectly:**
- ⚠️ **Normalizes field names but may not preserve ALL computed fields**
- ⚠️ Only stores subset of indicators (missing some microstructure fields)
- ⚠️ Only stores subset of Greeks (missing advanced Greeks + metadata)

**Code:**
```python
# Line 1136: Normalizes field names
normalized = normalize_redis_tick_data(tick)  # ✅ Uses centralized normalization

# Line 1139: Stores with normalized field names
key = self.key_builder.live_ticks_latest(symbol)  # ✅ Uses centralized key builder
self.redis_client.hset(key, mapping=normalized)  # ✅ Stores normalized data

# Line 1144-1146: Stores indicators (but incomplete list)
indicators = self._extract_indicators(tick)  # ⚠️ Only extracts subset
self.storage.store_indicators(symbol, indicators)  # ✅ Uses centralized storage
```

#### 2.2 Data Pipeline - Fetch (COMPLIANT)
**File:** `intraday_scanner/data_pipeline.py`

**What it does correctly:**
- ✅ Uses `normalize_redis_tick_data()` when fetching from Redis (line 1206)
- ✅ Uses key builder methods for fetching
- ✅ Uses UnifiedDataStorage for indicator fetching
- ✅ **FIXED:** Now fetches from `live_ticks_latest()` (matches storage key)

**Code:**
```python
# Line 1206: Normalizes fetched data
normalized_tick = normalize_redis_tick_data(tick_data)  # ✅ Uses centralized normalization

# Line 3270: Normalizes before processing
tick_data = normalize_redis_tick_data(tick_data)  # ✅ Uses centralized normalization
```

---

## 3. Violations Found

### ❌ VIOLATION 1: Incomplete Field Storage

**Location:** `zerodha/crawlers/intraday_websocket_parser.py:_extract_indicators()`

**Problem:** Only extracts subset of computed indicators, missing:
- Advanced microstructure fields (cumulative_volume_delta, best_bid_size, best_ask_size, total_bid_depth, total_ask_depth)
- Advanced Greeks (vanna, charm, vomma, speed, zomma, color)
- Option metadata (dte_years, trading_dte, option_price, expiry_series)
- Cross-asset metrics (usdinr_sensitivity, usdinr_correlation, etc.)

**Impact:** These computed fields are calculated but NOT stored, so they're lost

**Fix Required:**
```python
def _extract_indicators(self, tick: Dict[str, Any]) -> Dict[str, Any]:
    """Extract ALL indicator fields from tick - preserve everything"""
    indicators = {}
    
    # ✅ FIX: Preserve ALL fields from tick (not just subset)
    # Use normalize_redis_tick_data to ensure field names are canonical
    normalized_tick = normalize_redis_tick_data(tick)
    
    # Extract ALL computed fields (not just hardcoded list)
    computed_field_categories = [
        # Price/Volume
        'last_price', 'volume', 'volume_ratio', 'open_interest', 'normalized_volume',
        'volume_spike', 'volume_trend', 'bucket_incremental_volume',
        
        # Greeks (ALL - classical + advanced)
        'delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'implied_volatility',
        'vanna', 'charm', 'vomma', 'speed', 'zomma', 'color',
        'gamma_exposure', 'gamma_risk', 'delta_risk', 'theta_decay',
        
        # Option Metadata
        'dte_years', 'trading_dte', 'expiry_series', 'option_price',
        'underlying_price', 'spot_price', 'strike_price', 'moneyness',
        'intrinsic_value', 'time_value',
        
        # Microstructure (ALL fields)
        'microprice', 'order_flow_imbalance', 'order_book_imbalance',
        'cumulative_volume_delta', 'spread_absolute', 'spread_bps',
        'depth_imbalance', 'best_bid_size', 'best_ask_size',
        'total_bid_depth', 'total_ask_depth',
        
        # Cross-asset
        'usdinr_sensitivity', 'usdinr_correlation', 'usdinr_theoretical_price', 'usdinr_basis',
        
        # Market regime
        'buy_pressure', 'vix_level', 'volatility_regime',
        
        # OHLC
        'open', 'high', 'low', 'close',
    ]
    
    # Extract all computed fields
    for field in computed_field_categories:
        if field in normalized_tick:
            indicators[field] = normalized_tick[field]
    
    # ✅ CRITICAL: Also preserve ANY other fields that might be computed
    # This ensures no computed data is lost
    for key, value in normalized_tick.items():
        if key not in indicators and key not in ['symbol', 'tradingsymbol', 'instrument_token', 'timestamp']:
            # Preserve unknown computed fields
            indicators[key] = value
    
    return indicators
```

### ❌ VIOLATION 2: Incomplete Greeks Storage

**Location:** `zerodha/crawlers/intraday_websocket_parser.py:_store_tick_data()`

**Problem:** Only stores basic Greeks, missing:
- rho
- Advanced Greeks (vanna, charm, vomma, speed, zomma, color)
- Option metadata (dte_years, trading_dte, option_price, expiry_series)

**Current Code:**
```python
# Line 1149-1153: Only stores basic Greeks
greek_fields = ['delta', 'gamma', 'theta', 'vega', 'iv', 
               'implied_volatility', 'gamma_exposure']
```

**Fix Required:**
```python
# ✅ FIX: Store ALL Greeks and option metadata
greek_fields = [
    # Classical Greeks
    'delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'implied_volatility',
    # Advanced Greeks
    'vanna', 'charm', 'vomma', 'speed', 'zomma', 'color',
    # Gamma Exposure
    'gamma_exposure', 'gamma_risk', 'delta_risk', 'theta_decay',
    # Option Metadata
    'dte_years', 'trading_dte', 'option_price', 'expiry_series',
    'underlying_price', 'spot_price', 'strike_price', 'moneyness',
    'intrinsic_value', 'time_value'
]
```

### ❌ VIOLATION 3: Field Normalization Not Enforced at Storage

**Problem:** `normalize_redis_tick_data()` is used, but it only normalizes field names. It doesn't ensure ALL computed fields are stored.

**Current Flow:**
1. Websocket parser calculates fields → tick dict
2. `normalize_redis_tick_data(tick)` → normalizes field names
3. Stores normalized dict → Redis
4. **BUT:** Only stores subset via `_extract_indicators()` and `greek_fields` list

**Issue:** Computed fields not in the hardcoded lists are NOT stored, even though they're in the tick dict

**Fix Required:** Store ALL fields from normalized tick, not just hardcoded subset

---

## 4. Why `redis_key_mapping.py` Isn't Being Enforced (CLARIFIED)

### Understanding `redis_key_mapping.py` Purpose

**`redis_key_mapping.py` is designed for DOWNSTREAM CONSUMERS** (like data_pipeline) to handle field name variations during migration. It's NOT meant to be enforced at storage time - it's a fallback mechanism for consumers.

### Architecture

1. **Storage Side (Websocket Parser):**
   - Should store ALL fields with canonical names using `normalize_redis_tick_data()`
   - Ensures consistent field names in Redis

2. **Consumer Side (Data Pipeline):**
   - Uses `redis_key_mapping.py` to handle field name variations
   - Preserves ALL fields via `_preserve_all_original_data()`
   - Can handle multiple field name formats during migration

### The Problem (Before Fix)

```python
# Websocket parser flow (BEFORE FIX):
tick = calculate_all_fields()  # ✅ Calculates everything
normalized = normalize_redis_tick_data(tick)  # ✅ Normalizes field names
# BUT THEN:
indicators = self._extract_indicators(tick)  # ❌ Extracts from ORIGINAL tick (not normalized)
greeks = {k: tick[k] for k in greek_fields}  # ❌ Extracts from ORIGINAL tick (not normalized)
self.storage.store_indicators(symbol, indicators)  # ❌ Only stores hardcoded subset
```

**Result:** 
- Field names may not be canonical (extracted from original tick, not normalized)
- Many computed fields are never stored (only hardcoded subset)
- Data pipeline can't recover lost fields even with `redis_key_mapping.py`

---

## 5. Fixes Applied

### Fix 1: Extract from Normalized Tick (✅ APPLIED)
**File:** `zerodha/crawlers/intraday_websocket_parser.py:_store_tick_data()`

**Change:** Extract indicators and Greeks from **normalized** tick (not original)

```python
# BEFORE:
indicators = self._extract_indicators(tick)  # ❌ Original tick
greeks = {k: tick[k] for k in greek_fields}  # ❌ Original tick

# AFTER:
indicators = self._extract_indicators(normalized)  # ✅ Normalized tick (canonical field names)
greeks = {k: normalized[k] for k in greek_fields if k in normalized}  # ✅ Normalized tick
```

**Status:** ✅ Fixed - Now uses canonical field names

### Fix 2: Preserve ALL Computed Fields (✅ APPLIED)
**File:** `zerodha/crawlers/intraday_websocket_parser.py:_extract_indicators()`

**Change:** Extract ALL computed fields, not just hardcoded subset

```python
# BEFORE:
# Only extracted hardcoded list: last_price, volume, volume_ratio, open_interest, microprice, etc.

# AFTER:
# Extracts ALL fields from normalized tick:
# - All price/volume indicators
# - ALL microstructure fields (not just subset)
# - ALL cross-asset metrics
# - ALL market regime indicators
# - ANY other computed fields (preserves unknown fields)
```

**Status:** ✅ Fixed - Now preserves ALL computed fields

### Fix 3: Comprehensive Greeks Storage (✅ APPLIED)
**File:** `zerodha/crawlers/intraday_websocket_parser.py:_store_tick_data()`

**Change:** Store ALL Greeks (classical + advanced + option metadata)

```python
# BEFORE:
greek_fields = ['delta', 'gamma', 'theta', 'vega', 'iv', 'implied_volatility', 'gamma_exposure']

# AFTER:
greek_fields = [
    # Classical Greeks
    'delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'implied_volatility',
    # Advanced Greeks
    'vanna', 'charm', 'vomma', 'speed', 'zomma', 'color',
    # Gamma Exposure
    'gamma_exposure', 'gamma_risk', 'delta_risk', 'theta_decay',
    # Option Metadata
    'dte_years', 'trading_dte', 'option_price', 'expiry_series',
    'underlying_price', 'spot_price', 'strike_price', 'moneyness',
    'intrinsic_value', 'time_value'
]
```

**Status:** ✅ Fixed - Now stores ALL Greeks and option metadata

---

## 6. Recommended Additional Fixes (Future)
```python
def _store_tick_data(self, tick: Dict[str, Any]):
    """Store tick data in Redis using unified standards"""
    symbol = tick.get('symbol')
    if not symbol:
        return
    
    # ✅ FIX: Normalize field names FIRST
    normalized = normalize_redis_tick_data(tick)
    
    # ✅ FIX: Store ALL fields from normalized tick (not just subset)
    key = self.key_builder.live_ticks_latest(symbol)
    self.redis_client.hset(key, mapping=normalized)  # ✅ Stores ALL fields
    self.redis_client.expire(key, 300)
    
    # ✅ FIX: Extract ALL computed indicators (not just hardcoded list)
    # Use normalize_redis_tick_data to ensure we get canonical field names
    all_indicators = {}
    
    # Define categories of computed fields
    computed_categories = {
        'volume': ['volume_ratio', 'normalized_volume', 'volume_spike', 'volume_trend', 'bucket_incremental_volume'],
        'price': ['price_change', 'price_change_pct', 'price_move', 'price_movement'],
        'greeks': ['delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'implied_volatility',
                   'vanna', 'charm', 'vomma', 'speed', 'zomma', 'color',
                   'gamma_exposure', 'gamma_risk', 'delta_risk', 'theta_decay'],
        'option': ['dte_years', 'trading_dte', 'option_price', 'expiry_series',
                   'underlying_price', 'spot_price', 'strike_price', 'moneyness',
                   'intrinsic_value', 'time_value'],
        'microstructure': ['microprice', 'order_flow_imbalance', 'order_book_imbalance',
                          'cumulative_volume_delta', 'spread_absolute', 'spread_bps',
                          'depth_imbalance', 'best_bid_size', 'best_ask_size',
                          'total_bid_depth', 'total_ask_depth'],
        'cross_asset': ['usdinr_sensitivity', 'usdinr_correlation', 'usdinr_theoretical_price', 'usdinr_basis'],
        'regime': ['buy_pressure', 'vix_level', 'volatility_regime'],
        'ohlc': ['open', 'high', 'low', 'close'],
    }
    
    # Extract all computed fields
    for category, fields in computed_categories.items():
        for field in fields:
            if field in normalized:
                all_indicators[field] = normalized[field]
    
    # ✅ CRITICAL: Also preserve ANY other fields that might be computed
    # This ensures no computed data is lost
    for key, value in normalized.items():
        if key not in all_indicators and key not in ['symbol', 'tradingsymbol', 'instrument_token', 'timestamp', 'depth', 'ohlc']:
            # Preserve unknown computed fields
            all_indicators[key] = value
    
    # Store all indicators
    if all_indicators:
        self.storage.store_indicators(symbol, all_indicators)
    
    # Store Greeks separately (for backward compatibility)
    greek_fields = [f for f in computed_categories['greeks'] + computed_categories['option'] if f in normalized]
    if greek_fields:
        greeks = {k: normalized[k] for k in greek_fields}
        self.storage.store_indicators(symbol, greeks, category="greeks")
```

### Future Enhancement: Field Preservation Validation

Add validation to ensure ALL computed fields are stored:

```python
def _validate_field_preservation(self, original_tick: Dict, stored_data: Dict) -> bool:
    """Validate that all computed fields are preserved during storage"""
    computed_fields = [
        'volume_ratio', 'delta', 'gamma', 'theta', 'vega', 'microprice',
        # ... all computed fields
    ]
    
    missing_fields = []
    for field in computed_fields:
        if field in original_tick and field not in stored_data:
            missing_fields.append(field)
    
    if missing_fields:
        self.logger.warning(f"⚠️ [FIELD_PRESERVATION] Missing fields: {missing_fields}")
        return False
    
    return True
```

---

## 7. Summary

### ✅ Following Centralized Way
- **Key Builder:** Both websocket parser and data_pipeline use centralized key builder ✅
- **Symbol Normalization:** Both use `RedisKeyStandards.canonical_symbol()` ✅
- **Field Normalization:** Both use `normalize_redis_tick_data()` ✅
- **Storage Methods:** Both use `store_indicators()` from UnifiedDataStorage ✅

### ✅ Fixes Applied
- **Extract from Normalized Tick:** Indicators/Greeks now extracted from normalized tick (canonical field names) ✅
- **Preserve ALL Fields:** `_extract_indicators()` now preserves ALL computed fields, not just hardcoded subset ✅
- **Comprehensive Greeks:** Stores ALL Greeks (classical + advanced + option metadata) ✅

### Architecture Understanding
- **`redis_key_mapping.py` Purpose:** For DOWNSTREAM CONSUMERS to handle field name variations during migration
- **Storage Side:** Stores ALL fields with canonical names (via `normalize_redis_tick_data()`)
- **Consumer Side:** Uses `redis_key_mapping.py` to handle variations and preserve all fields (via `_preserve_all_original_data()`)

### Result
1. ✅ Storage stores ALL fields with canonical names
2. ✅ Consumers can handle field name variations via `redis_key_mapping.py`
3. ✅ No computed data is lost during storage or retrieval

