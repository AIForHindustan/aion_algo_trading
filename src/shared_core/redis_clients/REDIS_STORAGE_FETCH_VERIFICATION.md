# Redis Storage vs Fetch Verification

**Date:** 2025-01-XX  
**Purpose:** Cross-verify what data fields are stored in Redis vs what data_pipeline fetches

---

## Critical Finding: Key Mismatch

### ❌ MISMATCH: Storage Key vs Fetch Key

**Websocket Parser Stores In:**
```python
# zerodha/crawlers/intraday_websocket_parser.py:1139
key = self.key_builder.live_ticks_latest(symbol)  # Returns: ticks:latest:{canonical_symbol}
self.redis_client.hset(key, mapping=normalized)
```

**Data Pipeline Fetches From:**
```python
# intraday_scanner/data_pipeline.py:1199
tick_key = key_builder.live_ticks_hash(variant)  # Returns: ticks:{canonical_symbol}
tick_data = redis_client.hgetall(tick_key)
```

**Result:** Data pipeline is fetching from `ticks:{symbol}` but websocket parser stores in `ticks:latest:{symbol}` - **DIFFERENT KEYS!**

---

## 1. What Websocket Parser Stores

### Storage Location 1: `ticks:latest:{canonical_symbol}` (Hash)
**Key:** `live_ticks_latest(symbol)` → `ticks:latest:{canonical_symbol}`

**Fields Stored:**
- All fields from `normalize_redis_tick_data(tick)` - this normalizes field names but preserves ALL data
- Includes: last_price, volume, timestamps, OHLC, depth, and ALL computed fields

### Storage Location 2: Indicators (via `store_indicators()`)
**Key Pattern:** `ind:{category}:{canonical_symbol}:{indicator_name}`

**Fields Stored (from `_extract_indicators()`):**
```python
# Price-based indicators
- last_price
- volume
- volume_ratio
- open_interest

# Microstructure indicators
- microprice
- spread_absolute
- spread_bps
- order_book_imbalance
- depth_imbalance

# OHLC (extracted from tick['ohlc'])
- open
- high
- low
- close
```

### Storage Location 3: Greeks (via `store_indicators(..., category="greeks")`)
**Key Pattern:** `ind:greeks:{canonical_symbol}:{greek_name}`

**Fields Stored:**
```python
- delta
- gamma
- theta
- vega
- iv
- implied_volatility
- gamma_exposure
```

---

## 2. What Data Pipeline Fetches

### Fetch Method: `_get_complete_redis_data(symbol)`

#### Step 1: UnifiedDataStorage.get_indicators()
Fetches indicators using UnifiedDataStorage which tries all symbol variants.

**Categories Fetched:**
```python
data_categories = {
    'indicators': [
        'rsi', 'macd', 'sma_20', 'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200',
        'bollinger_bands', 'vwap', 'atr', 'price_change', 'price_change_pct',
        'iv_baseline', 'price_move', 'price_movement', 'z_score'
    ],
    'volume': ['volume_ratio', 'normalized_volume', 'volume_spike', 'volume_trend'],
    'greeks': [
        # Classical Greeks
        'delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'implied_volatility',
        'dte_years', 'trading_dte', 'option_price', 'expiry_series',
        # Advanced Greeks
        'vanna', 'charm', 'vomma', 'speed', 'zomma', 'color',
        # Gamma Exposure
        'gamma_exposure'
    ],
    'price': ['last_price', 'open', 'high', 'low', 'close'],
    'ohlc': ['ohlc_open', 'ohlc_high', 'ohlc_low', 'ohlc_close'],
    'custom': [
        # Microstructure
        'microprice', 'order_flow_imbalance', 'order_book_imbalance',
        'cumulative_volume_delta', 'spread_absolute', 'spread_bps',
        'depth_imbalance', 'best_bid_size', 'best_ask_size',
        'total_bid_depth', 'total_ask_depth',
        # Cross-asset
        'usdinr_sensitivity', 'usdinr_correlation', 'usdinr_theoretical_price', 'usdinr_basis',
        # Market regime
        'buy_pressure', 'vix_level'
    ]
}
```

#### Step 2: Fallback Manual Fetch
If UnifiedDataStorage doesn't find everything, tries individual keys:
- `key_builder.live_greeks(variant, field)` for Greeks
- `key_builder.live_indicator(variant, field, category)` for indicators

#### Step 3: Tick Hash Fetch (⚠️ MISMATCH)
```python
# Line 1199: Fetches from WRONG key!
tick_key = key_builder.live_ticks_hash(variant)  # ticks:{symbol}
tick_data = redis_client.hgetall(tick_key)
```

**Problem:** Websocket parser stores in `ticks:latest:{symbol}` but data_pipeline fetches from `ticks:{symbol}`

---

## 3. Field Comparison

### Fields Stored by Websocket Parser

| Storage Location | Fields |
|------------------|--------|
| `ticks:latest:{symbol}` (Hash) | ALL fields from normalize_redis_tick_data(tick) |
| `ind:{category}:{symbol}:{indicator}` | last_price, volume, volume_ratio, open_interest, microprice, spread_absolute, spread_bps, order_book_imbalance, depth_imbalance, open, high, low, close |
| `ind:greeks:{symbol}:{greek}` | delta, gamma, theta, vega, iv, implied_volatility, gamma_exposure |

### Fields Fetched by Data Pipeline

| Fetch Method | Fields |
|--------------|--------|
| UnifiedDataStorage.get_indicators() | All indicators, volume, greeks, price, ohlc, custom (comprehensive list) |
| Manual key fetch | Individual indicator keys (fallback) |
| `live_ticks_hash()` (⚠️ WRONG KEY) | Tick hash data (but websocket stores in `live_ticks_latest()`) |

---

## 4. Missing Fields Analysis

### Fields Stored But Not Fetched

1. **Tick Hash Data:** Websocket parser stores ALL tick data in `ticks:latest:{symbol}`, but data_pipeline fetches from `ticks:{symbol}` (different key)
   - **Impact:** All fields stored in `ticks:latest:{symbol}` are NOT being fetched
   - **Fields Affected:** All fields from normalize_redis_tick_data(tick) that aren't also stored as indicators

2. **Missing Indicator Fields:**
   - Websocket parser stores: `last_price`, `volume`, `open_interest` as indicators
   - Data pipeline fetches these, but might miss if stored only in tick hash

### Fields Fetched But Not Stored

1. **Technical Indicators:** Data pipeline tries to fetch RSI, MACD, EMAs, etc., but websocket parser doesn't store these
   - **Status:** ✅ OK - These are calculated elsewhere (calculations.py, scanner_main.py)

2. **Advanced Greeks:** Data pipeline fetches vanna, charm, vomma, speed, zomma, color, but websocket parser only stores basic Greeks
   - **Status:** ⚠️ May be missing if calculated but not stored

---

## 5. Critical Issues Found

### Issue 1: Key Mismatch (CRITICAL)
**Problem:** Websocket parser stores in `ticks:latest:{symbol}` but data_pipeline fetches from `ticks:{symbol}`

**Impact:** All tick data stored in `ticks:latest:{symbol}` is NOT being fetched by data_pipeline

**Fix Required:**
```python
# intraday_scanner/data_pipeline.py:1199
# CHANGE FROM:
tick_key = key_builder.live_ticks_hash(variant)  # ticks:{symbol}

# CHANGE TO:
tick_key = key_builder.live_ticks_latest(variant)  # ticks:latest:{symbol}
```

### Issue 2: Incomplete Indicator Storage
**Problem:** Websocket parser only stores a subset of indicators:
- Stores: last_price, volume, volume_ratio, open_interest, microprice, spread_absolute, spread_bps, order_book_imbalance, depth_imbalance, OHLC
- Missing: Many microstructure fields that data_pipeline expects (cumulative_volume_delta, best_bid_size, best_ask_size, total_bid_depth, total_ask_depth)

**Impact:** Some computed fields may not be stored and therefore not available to data_pipeline

### Issue 3: Incomplete Greeks Storage
**Problem:** Websocket parser only stores basic Greeks:
- Stores: delta, gamma, theta, vega, iv, implied_volatility, gamma_exposure
- Missing: rho, dte_years, trading_dte, option_price, expiry_series, advanced Greeks (vanna, charm, vomma, speed, zomma, color)

**Impact:** Advanced Greeks calculated elsewhere may not be stored

---

## 6. Recommended Fixes

### Fix 1: Correct Tick Hash Key (CRITICAL)
```python
# intraday_scanner/data_pipeline.py:1199
# Change from live_ticks_hash to live_ticks_latest
tick_key = key_builder.live_ticks_latest(variant)  # ✅ Matches storage key
tick_data = redis_client.hgetall(tick_key)
```

### Fix 2: Enhance Websocket Parser Indicator Storage
```python
# zerodha/crawlers/intraday_websocket_parser.py:_extract_indicators()
# Add missing microstructure fields:
micro_fields = [
    'microprice', 'spread_absolute', 'spread_bps', 
    'order_book_imbalance', 'depth_imbalance',
    # ✅ ADD MISSING:
    'cumulative_volume_delta', 'best_bid_size', 'best_ask_size',
    'total_bid_depth', 'total_ask_depth'
]
```

### Fix 3: Enhance Greeks Storage
```python
# zerodha/crawlers/intraday_websocket_parser.py:_store_tick_data()
# Add missing Greeks:
greek_fields = [
    'delta', 'gamma', 'theta', 'vega', 'iv', 
    'implied_volatility', 'gamma_exposure',
    # ✅ ADD MISSING:
    'rho', 'dte_years', 'trading_dte', 'option_price', 'expiry_series',
    'vanna', 'charm', 'vomma', 'speed', 'zomma', 'color'
]
```

### Fix 4: Verify All Computed Fields Are Stored
Ensure websocket parser stores ALL computed fields from calculations pipeline:
- All Greeks (classical + advanced)
- All microstructure metrics
- All volume calculations
- All option metadata

---

## 7. Verification Checklist

After fixes:
- [ ] Verify websocket parser stores in `ticks:latest:{symbol}`
- [ ] Verify data_pipeline fetches from `ticks:latest:{symbol}` (not `ticks:{symbol}`)
- [ ] Verify all computed indicators are stored
- [ ] Verify all Greeks (classical + advanced) are stored
- [ ] Verify all microstructure metrics are stored
- [ ] Verify data_pipeline fetches all stored fields
- [ ] Test with live data to confirm no data loss

---

## 7. Fixes Applied

### Fix 1: Corrected Tick Hash Key (✅ APPLIED)
**File:** `intraday_scanner/data_pipeline.py:1201`
**Change:** Changed from `live_ticks_hash()` to `live_ticks_latest()` to match websocket parser storage

```python
# BEFORE:
tick_key = key_builder.live_ticks_hash(variant)  # ticks:{symbol}

# AFTER:
tick_key = key_builder.live_ticks_latest(variant)  # ticks:latest:{symbol} ✅
```

**Status:** ✅ Fixed - Now fetches from correct key

### Fix 2: Other Usage (Underlying Price Lookup)
**File:** `intraday_scanner/data_pipeline.py:5791`
**Context:** Used for underlying price lookup (GOLD/SILVER futures)
**Status:** ⚠️ May need verification - This is for futures, not options, so may be correct

---

## 8. Summary

### Current State (After Fix 1)
- ✅ **Key Match:** Storage and fetch now use same key (`ticks:latest:{symbol}`)
- ⚠️ **Incomplete Storage:** Some computed fields may not be stored by websocket parser
- ✅ **Complete Fetch:** Data pipeline now fetches from correct key and tries all symbol variants

### Remaining Issues
- ⚠️ **Incomplete Indicator Storage:** Websocket parser may not store all computed indicators
- ⚠️ **Incomplete Greeks Storage:** Websocket parser may not store all Greeks (advanced + metadata)

### Next Steps
1. Verify websocket parser stores ALL computed fields from calculations pipeline
2. Enhance websocket parser indicator extraction to include all microstructure fields
3. Enhance websocket parser Greeks storage to include all Greeks and option metadata

