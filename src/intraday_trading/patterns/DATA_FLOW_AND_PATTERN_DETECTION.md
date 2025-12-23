# Data Flow and Pattern Detection Architecture

## Overview

This document explains how tick data flows from ingestion through pattern detection to alert generation, with **UnifiedDataStorage** initialized once and reused across all pattern checks.

---

## Complete Data Flow

```
Zerodha WebSocket → Redis Stream (ticks:intraday:processed)
    ↓
data_pipeline.py::_process_market_tick()
    ↓
data_pipeline.py::_clean_tick_data()
    ├─ Clean and validate tick data
    ├─ Extract OHLC, Greeks, volume, microstructure
    └─ Store computed indicators via UnifiedDataStorage
    ↓
data_pipeline.py::_process_tick_for_patterns_fast()
    ↓
pattern_detector.py::detect_patterns(indicators)
    ├─ Fetch missing indicators from UnifiedDataStorage (reused instance)
    ├─ Check all 8 core patterns:
    │   ├─ Advanced Patterns (5): order_flow_breakout, gamma_exposure_reversal,
    │   │   microstructure_divergence, cross_asset_arbitrage, vix_momentum
    │   ├─ Strategies (2): kow_signal_straddle, ict_iron_condor
    │   └─ Game Theory (1): game_theory_signal
    └─ Return detected patterns
    ↓
alert_manager.py::process_patterns()
    └─ Generate alerts for matched patterns
```

---

## UnifiedDataStorage Initialization (Single Instance Pattern)

### 1. data_pipeline.py (Line 1548-1556)

**Initialization**: Once in `DataPipeline.__init__()`

```python
# ✅ UNIFIED STORAGE: Initialize UnifiedDataStorage once and reuse
try:
    from shared_core.redis_clients.unified_data_storage import UnifiedDataStorage
    self._unified_storage = UnifiedDataStorage(redis_client=self.realtime_client)
    self.logger.info("✅ UnifiedDataStorage initialized for data_pipeline")
except Exception as e:
    self.logger.warning(f"⚠️ Failed to initialize UnifiedDataStorage: {e}")
    self._unified_storage = None
```

**Usage**: After `_clean_tick_data()`, stores computed indicators (Line 5975)

```python
# Store computed indicators if any were computed
if computed_indicators:
    self._unified_storage.store_indicators(symbol, computed_indicators)
```

### 2. pattern_detector.py (Line 894-904)

**Initialization**: Once in `PatternDetector.__init__()`

```python
# ✅ UNIFIED STORAGE: Initialize UnifiedDataStorage once and reuse for all pattern checks
# Prevents connection pool exhaustion and ensures consistent data access
self._unified_storage = None
if redis_client:
    try:
        from shared_core.redis_clients.unified_data_storage import UnifiedDataStorage
        self._unified_storage = UnifiedDataStorage(redis_client=redis_client)
        self.logger.info("✅ UnifiedDataStorage initialized for pattern_detector (single instance, reused for all patterns)")
    except Exception as e:
        self.logger.warning(f"⚠️ Failed to initialize UnifiedDataStorage: {e}")
        self._unified_storage = None
```

**Usage**: Reused throughout pattern detection to fetch missing indicators

```python
# ✅ REUSE: Use pre-initialized UnifiedDataStorage instance from __init__
if self._unified_storage:
    fetched_indicators = self._unified_storage.get_indicators(symbol, missing_indicators)
```

---

## Tick Data Processing Steps

### Step 1: Clean Tick Data (`data_pipeline.py::_clean_tick_data()`)

**Location**: `data_pipeline.py:5512-5980`

**What it does**:
- Validates and normalizes all tick fields
- Extracts OHLC from Zerodha tick
- Preserves Greeks (delta, gamma, theta, vega, rho, advanced Greeks)
- Preserves microstructure (microprice, order_flow_imbalance, cumulative_volume_delta)
- Preserves volume metrics (volume_ratio, normalized_volume)
- Preserves technical indicators (RSI, MACD, EMA, VWAP, ATR, Bollinger Bands)

**Output**: Cleaned tick dictionary with all preserved fields

### Step 2: Store Indicators (`data_pipeline.py::_clean_tick_data_core_enhanced()`)

**Location**: `data_pipeline.py:5970-5978`

**What it does**:
- Stores computed indicators via `UnifiedDataStorage.store_indicators()`
- Categorizes indicators automatically:
  - **Greeks**: `ind:greeks:{symbol}:{greek_name}` (delta, gamma, theta, vega, rho, vanna, charm, etc.)
  - **Technical Analysis**: `ind:ta:{symbol}:{indicator_name}` (rsi, macd, ema_*, vwap, atr)
  - **Microstructure**: `ind:custom:{symbol}:{indicator_name}` (microprice, order_flow_imbalance)
  - **Volume**: `ind:volume:{symbol}:{indicator_name}` (volume_ratio, normalized_volume)
  - **Cross-asset**: `ind:custom:{symbol}:{indicator_name}` (usdinr_sensitivity, usdinr_correlation)

**Storage**: Redis DB 1, TTL 86400 seconds (24 hours)

### Step 3: Pattern Detection (`pattern_detector.py::detect_patterns()`)

**Location**: `pattern_detector.py:4504-4700`

**What it does**:
1. **Receives cleaned tick data** from `data_pipeline._process_tick_for_patterns_fast()`
2. **Fetches missing indicators** from Redis using `UnifiedDataStorage` (reused instance)
3. **Enriches indicators** with VIX regime and thresholds (cached for 3 minutes)
4. **Checks all 8 core patterns**:
   - Advanced patterns (5): Via `_detect_advanced_patterns_with_data()`
   - Straddle strategies (2): Via `_detect_straddle_patterns()`
   - Game theory (1): Via `_detect_game_theory_patterns()`
5. **Filters patterns** by:
   - Enabled patterns (instrument config)
   - Volume requirements (for volume-dependent patterns)
   - Killzone suitability
   - Commodity correlation adjustments

**Returns**: List of detected patterns with confidence scores

---

## Pattern Detection Flow

### For Each Tick:

```python
# 1. data_pipeline receives tick
cleaned = self._clean_tick_data(tick_data)

# 2. Store computed indicators
self._unified_storage.store_indicators(symbol, computed_indicators)

# 3. Pass to pattern detector
patterns = self.pattern_detector.detect_patterns(cleaned)

# 4. Pattern detector fetches missing data (if needed)
if self._unified_storage:
    fetched = self._unified_storage.get_indicators(symbol, missing_indicators)

# 5. Check all 8 patterns
for pattern_handler in self.option_pattern_handlers.values():
    pattern = pattern_handler(symbol, enriched_indicators)
    if pattern:
        patterns.append(pattern)

# 6. Generate alerts
if patterns:
    alert_manager.process_patterns(patterns)
```

---

## UnifiedDataStorage Benefits

### 1. **Single Instance Pattern**
- Initialized once in `__init__()` for both `DataPipeline` and `PatternDetector`
- Reused throughout the lifecycle
- **Prevents connection pool exhaustion**

### 2. **Consistent Data Access**
- All indicators stored with standardized key format
- Automatic symbol normalization (canonical_symbol)
- Handles symbol variant lookup internally

### 3. **Categorized Storage**
- Greeks: `ind:greeks:{symbol}:{greek_name}`
- Technical Analysis: `ind:ta:{symbol}:{indicator_name}`
- Microstructure: `ind:custom:{symbol}:{indicator_name}`
- Volume: `ind:volume:{symbol}:{indicator_name}`

### 4. **Automatic TTL Management**
- Default TTL: 86400 seconds (24 hours)
- All indicators in DB 1 (live data)

---

## Pattern Condition Checking

### How Patterns Access Tick Data:

1. **Direct from cleaned tick**: Most indicators are already in the tick data passed to `detect_patterns()`
2. **Fetch from Redis**: Missing indicators are fetched using `self._unified_storage.get_indicators()`
3. **Cached context**: VIX regime and thresholds are cached for 3 minutes via `MarketContext`

### Example: Order Flow Breakout Pattern

```python
# In advanced/__init__.py::OrderFlowBreakoutDetector.detect()
def detect(self, symbol: str, indicators: Dict[str, Any]):
    # Indicators already contain:
    # - order_flow_imbalance (from cleaned tick)
    # - microprice (from cleaned tick)
    # - volume_ratio (from cleaned tick or fetched)
    # - vix_regime (from MarketContext cache)
    
    ofi = indicators.get('order_flow_imbalance', 0.0)
    volume_ratio = indicators.get('volume_ratio', 1.0)
    
    # Check pattern conditions
    if ofi > threshold and volume_ratio > 2.0:
        return create_pattern(...)
```

---

## Alert Generation

After pattern detection, alerts are generated via:

```python
# In scanner_main.py or alert_manager.py
if patterns:
    for pattern in patterns:
        alert = alert_manager.create_alert(pattern)
        alert_manager.send_alert(alert)
```

**Alert contains**:
- Pattern type
- Symbol
- Direction (BUY/SELL)
- Entry price
- Stop loss
- Target price
- Confidence score
- Timestamp

---

## Key Takeaways

1. **UnifiedDataStorage is initialized ONCE** in both `DataPipeline` and `PatternDetector.__init__()`
2. **All tick data** is cleaned and stored via `UnifiedDataStorage.store_indicators()`
3. **Patterns fetch missing data** using the reused `self._unified_storage` instance
4. **All 8 patterns** check conditions against the same enriched indicators dictionary
5. **Alerts are generated** for patterns that meet their conditions

---

## Verification

To verify the flow is working:

1. **Check initialization logs**:
   ```
   ✅ UnifiedDataStorage initialized for data_pipeline
   ✅ UnifiedDataStorage initialized for pattern_detector
   ```

2. **Check data storage logs**:
   ```
   ✅ [DATA_PIPELINE_STORED] {symbol}: Stored {count} computed indicators
   ```

3. **Check pattern detection logs**:
   ```
   🔍 [PATTERN_DETECTOR] detect_patterns() called for {symbol}
   ✅ [ADVANCED_INDICATORS] {symbol} - Fetched {count} indicators via UnifiedDataStorage
   ```

4. **Check pattern matches**:
   ```
   ✅ [ADVANCED_PATTERNS] {symbol} - order_flow_breakout DETECTED (confidence: {score})
   ```

---

## Summary

- **Single instance**: `UnifiedDataStorage` initialized once, reused everywhere
- **Complete data**: All tick data preserved through `_clean_tick_data()`
- **Consistent storage**: All indicators stored via `UnifiedDataStorage.store_indicators()`
- **Efficient fetching**: Missing indicators fetched using reused instance
- **Pattern checking**: All 8 patterns check conditions against enriched indicators
- **Alert generation**: Matched patterns trigger alerts automatically







