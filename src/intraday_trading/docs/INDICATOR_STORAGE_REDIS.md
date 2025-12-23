# Indicator Calculation and Storage in Redis

**Last Updated:** 2025-11-14  
**Status:** Production Ready

---

## Overview

Indicators are calculated in real-time using `HybridCalculations` and stored atomically in **Redis DB 1** using a unified key structure. The system follows a priority chain: **Redis Cache → TA-Lib → Polars → pandas_ta → Simple Fallback**.

---

## Architecture Flow

### 1. Calculation Phase

```
Tick Data → DataPipeline.process_tick()
    ↓
HybridCalculations.batch_calculate_indicators(tick_data, symbol)
    ↓
Priority Chain:
  1. Redis Cache (if available)
  2. TA-Lib (C library - fastest)
  3. Polars (vectorized Python)
  4. pandas_ta (fallback)
  5. Simple calculation (last resort)
    ↓
Returns: Dict of indicators {rsi, atr, ema_20, macd, ...}
```

### 2. Storage Phase (Atomic)

```
Calculated Indicators → HybridCalculations._store_indicators_atomic()
    ↓
For each indicator:
  - Determine category (ta, greeks, volume, custom)
  - Build Redis key using DatabaseAwareKeyBuilder
  - Store in Redis DB 1 with TTL
    ↓
Redis DB 1: ind:{category}:{symbol}:{indicator_name}
```

---

## Redis Storage Details

### Database Assignment

**All indicators stored in Redis DB 1 (Live Data)**

- **Database:** DB 1 (realtime/live data)
- **Key Format:** `ind:{category}:{canonical_symbol}:{indicator_name}`
- **TTL:** 
  - Technical indicators: 300 seconds (5 minutes)
  - Greeks: 3600 seconds (1 hour)

### Key Structure

#### Technical Indicators (TA)
```
ind:ta:{symbol}:rsi
ind:ta:{symbol}:atr
ind:ta:{symbol}:ema_20
ind:ta:{symbol}:ema_50
ind:ta:{symbol}:vwap
ind:ta:{symbol}:macd
ind:ta:{symbol}:bollinger_bands
```

#### Greeks (Options)
```
ind:greeks:{symbol}:delta
ind:greeks:{symbol}:gamma
ind:greeks:{symbol}:theta
ind:greeks:{symbol}:vega
ind:greeks:{symbol}:rho
```

#### Volume Indicators
```
ind:volume:{symbol}:volume_ratio
ind:volume:{symbol}:volume_profile
```

#### Custom Indicators
```
ind:custom:{symbol}:{indicator_name}
```

### Key Building

**Location:** `redis_files/redis_key_standards.py`

```python
from redis_files.redis_key_standards import get_key_builder, RedisKeyStandards

key_builder = get_key_builder()
canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)

# For technical indicators
category = RedisKeyStandards._categorize_indicator(indicator_name)
key = key_builder.live_indicator(canonical_symbol, indicator_name, category)
# Result: ind:ta:NIFTY25NOV26000CE:rsi

# For Greeks
key = key_builder.live_greeks(canonical_symbol, greek_name)
# Result: ind:greeks:NIFTY25NOV26000CE:delta
```

### Storage Implementation

**Location:** `intraday_scanner/calculations.py` → `HybridCalculations._store_indicators_atomic()`

```python
def _store_indicators_atomic(self, symbol: str, indicators: dict):
    """
    Atomic storage: Store immediately after calculation
    """
    key_builder = get_key_builder()
    canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
    db1_client = self.redis_client.get_client(1)  # DB 1
    
    for indicator_name, value in indicators.items():
        # Determine category and build key
        if indicator_name in ['delta', 'gamma', 'theta', 'vega', 'rho']:
            key = key_builder.live_greeks(canonical_symbol, indicator_name)
            ttl = 3600  # 1 hour
        else:
            category = RedisKeyStandards._categorize_indicator(indicator_name)
            key = key_builder.live_indicator(canonical_symbol, indicator_name, category)
            ttl = 300   # 5 minutes
        
        # Store value
        if isinstance(value, dict):
            # Complex indicators (MACD, Bollinger Bands)
            payload = json.dumps({
                'value': value,
                'timestamp': int(time.time() * 1000),
                'symbol': canonical_symbol,
                'indicator_type': indicator_name
            })
        else:
            # Simple numeric/string values (RSI, EMA, ATR)
            payload = str(value)
        
        db1_client.setex(key, ttl, payload)
```

---

## Indicator Retrieval

### Priority Chain (When Reading)

```
1. Redis DB 1 (ind:{category}:{symbol}:{indicator})
   ↓ (if not found)
2. TA-Lib calculation
   ↓ (if fails)
3. Polars calculation
   ↓ (if fails)
4. pandas_ta calculation
   ↓ (if fails)
5. Simple fallback calculation
```

### Retrieval Methods

#### 1. Via IndicatorLifecycleManager

**Location:** `core/indicators/lifecycle.py`

```python
lifecycle = IndicatorLifecycleManager(storage_engine, redis_client=redis_client)
indicators = await lifecycle.get_indicators(
    symbol="NIFTY25NOV26000CE",
    required_indicators=["rsi", "atr", "ema_20", "delta"],
    context_data={"ticks": tick_data, "last_tick": last_tick}
)
```

**Flow:**
1. Check `StorageEngine.retrieve_indicators()` (reads from Redis DB 1)
2. Calculate missing indicators via `HybridCalculations`
3. Store calculated indicators back to Redis
4. Return complete indicator set

#### 2. Via DataPipeline (Fallback)

**Location:** `intraday_scanner/data_pipeline.py` → `_fetch_indicators_from_redis()`

```python
def _fetch_indicators_from_redis(self, symbols: List[str]) -> Dict[str, Dict]:
    """
    Fetch indicators from Redis DB 1 (fallback mechanism)
    """
    # Try new unified structure: ind:{category}:{symbol}:{indicator}
    # Fallback to old structure: indicators:{symbol}:{indicator}
```

**Key Lookup:**
1. Try `ind:{category}:{symbol}:{indicator}` (new structure)
2. Fallback to `indicators:{symbol}:{indicator}` (legacy, for migration)

#### 3. Direct Redis Access

```python
from redis_files.redis_client import redis_manager
from redis_files.redis_key_standards import get_key_builder, RedisKeyStandards

db1_client = redis_manager.get_client(1)  # DB 1
key_builder = get_key_builder()
canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)

# Build key
category = RedisKeyStandards._categorize_indicator("rsi")
key = key_builder.live_indicator(canonical_symbol, "rsi", category)
# Result: ind:ta:NIFTY25NOV26000CE:rsi

# Retrieve
value = db1_client.get(key)
if value:
    # Parse JSON for complex indicators, or convert to float for simple
    rsi_value = float(value) if isinstance(value, (str, bytes)) else json.loads(value)
```

---

## Supported Indicators

### Technical Indicators (TA Category)

| Indicator | Key Format | Storage Type | TTL |
|-----------|------------|--------------|-----|
| RSI | `ind:ta:{symbol}:rsi` | String (float) | 300s |
| ATR | `ind:ta:{symbol}:atr` | String (float) | 300s |
| EMA (5, 10, 20, 50, 100, 200) | `ind:ta:{symbol}:ema_{period}` | String (float) | 300s |
| VWAP | `ind:ta:{symbol}:vwap` | String (float) | 300s |
| MACD | `ind:ta:{symbol}:macd` | JSON (dict) | 300s |
| Bollinger Bands | `ind:ta:{symbol}:bollinger_bands` | JSON (dict) | 300s |

### Greeks (Options Only)

| Greek | Key Format | Storage Type | TTL |
|-------|------------|--------------|-----|
| Delta | `ind:greeks:{symbol}:delta` | String (float) | 3600s |
| Gamma | `ind:greeks:{symbol}:gamma` | String (float) | 3600s |
| Theta | `ind:greeks:{symbol}:theta` | String (float) | 3600s |
| Vega | `ind:greeks:{symbol}:vega` | String (float) | 3600s |
| Rho | `ind:greeks:{symbol}:rho` | String (float) | 3600s |

### Volume Indicators

| Indicator | Key Format | Storage Type | TTL |
|-----------|------------|--------------|-----|
| Volume Ratio | `ind:volume:{symbol}:volume_ratio` | String (float) | 300s |
| Volume Profile | `ind:volume:{symbol}:volume_profile` | JSON (dict) | 300s |

### Option Metrics

| Metric | Key Format | Storage Type | TTL |
|--------|------------|--------------|-----|
| DTE Years | `ind:custom:{symbol}:dte_years` | String (float) | 300s |
| Trading DTE | `ind:custom:{symbol}:trading_dte` | String (int) | 300s |
| Expiry Series | `ind:custom:{symbol}:expiry_series` | String | 300s |

---

## Symbol Normalization

### Canonical Symbol Format

**Location:** `redis_files/redis_key_standards.py` → `RedisKeyStandards.canonical_symbol()`

```python
from redis_files.redis_key_standards import RedisKeyStandards

# Handles various formats
symbol = "NFO:NIFTY25NOV26000CE"
canonical = RedisKeyStandards.canonical_symbol(symbol)
# Result: "NIFTY25NOV26000CE" (removes exchange prefix)

symbol = "NIFTY25NOV26000CE"
canonical = RedisKeyStandards.canonical_symbol(symbol)
# Result: "NIFTY25NOV26000CE" (unchanged)
```

### Symbol Variants for Lookup

**Location:** `redis_files/redis_key_standards.py` → `RedisKeyStandards.get_indicator_symbol_variants()`

When retrieving indicators, the system tries multiple symbol variants:

```python
variants = RedisKeyStandards.get_indicator_symbol_variants("NFO:NIFTY25NOV26000CE")
# Returns: ["NIFTY25NOV26000CE", "NFO:NIFTY25NOV26000CE", ...]
```

This ensures indicators are found regardless of exchange prefix format.

---

## Data Flow Examples

### Example 1: Real-Time Tick Processing

```
1. Tick arrives → DataPipeline.process_tick()
2. HybridCalculations.batch_calculate_indicators([tick], "NIFTY25NOV26000CE")
   - Calculates: rsi=65.5, atr=12.3, ema_20=25800.0
3. HybridCalculations._store_indicators_atomic()
   - Stores to Redis DB 1:
     * ind:ta:NIFTY25NOV26000CE:rsi = "65.5" (TTL: 300s)
     * ind:ta:NIFTY25NOV26000CE:atr = "12.3" (TTL: 300s)
     * ind:ta:NIFTY25NOV26000CE:ema_20 = "25800.0" (TTL: 300s)
4. Pattern detector requests indicators
   - IndicatorLifecycleManager.get_indicators()
   - Retrieves from Redis DB 1 (cache hit)
   - Returns: {rsi: 65.5, atr: 12.3, ema_20: 25800.0}
```

### Example 2: Option Greeks Calculation

```
1. Option tick arrives → DataPipeline.process_tick()
2. HybridCalculations.batch_calculate_indicators([tick], "NIFTY25NOV26000CE")
   - Calculates Greeks: delta=0.65, gamma=0.02, theta=-0.15
3. HybridCalculations._store_indicators_atomic()
   - Stores to Redis DB 1:
     * ind:greeks:NIFTY25NOV26000CE:delta = "0.65" (TTL: 3600s)
     * ind:greeks:NIFTY25NOV26000CE:gamma = "0.02" (TTL: 3600s)
     * ind:greeks:NIFTY25NOV26000CE:theta = "-0.15" (TTL: 3600s)
4. Pattern detector requests Greeks
   - IndicatorLifecycleManager.get_indicators(..., required_indicators=["delta", "gamma"])
   - Retrieves from Redis DB 1 (cache hit)
   - Returns: {delta: 0.65, gamma: 0.02}
```

### Example 3: Complex Indicator (MACD)

```
1. Tick arrives → HybridCalculations calculates MACD
2. MACD result: {macd: 12.5, signal: 10.2, histogram: 2.3}
3. Storage:
   - Key: ind:ta:NIFTY25NOV26000CE:macd
   - Value: JSON string:
     {
       "value": {"macd": 12.5, "signal": 10.2, "histogram": 2.3},
       "timestamp": 1734172800000,
       "symbol": "NIFTY25NOV26000CE",
       "indicator_type": "macd"
     }
   - TTL: 300s
4. Retrieval:
   - db1_client.get("ind:ta:NIFTY25NOV26000CE:macd")
   - Parse JSON → Extract "value" → Return dict
```

---

## Integration Points

### 1. Pattern Detector

**Location:** `patterns/pattern_detector.py`

```python
# Pattern detector requests indicators
indicators = await lifecycle_manager.get_indicators(
    symbol=symbol,
    required_indicators=["rsi", "atr", "ema_20", "delta", "gamma"],
    context_data={"ticks": ticks, "last_tick": last_tick}
)

# Indicators are retrieved from Redis DB 1 (if cached) or calculated fresh
# Pattern detection uses these indicators
```

### 2. Alert Manager

**Location:** `alerts/alert_manager.py`

```python
# Alert manager uses indicators from pattern detector
# Indicators are already calculated and stored by this point
```

### 3. Scanner Main

**Location:** `intraday_scanner/scanner_main.py`

```python
# Scanner uses IndicatorLifecycleManager to get indicators
indicators = await lifecycle_manager.get_indicators(
    symbol=symbol,
    required_indicators=required_indicators,
    context_data={"ticks": ticks, "last_tick": last_tick}
)
```

---

## Key Standards Reference

### DatabaseAwareKeyBuilder Methods

**Location:** `redis_files/redis_key_standards.py`

```python
key_builder = get_key_builder()

# Technical indicators
key = key_builder.live_indicator(symbol, "rsi", "ta")
# Result: ind:ta:{symbol}:rsi

# Greeks
key = key_builder.live_greeks(symbol, "delta")
# Result: ind:greeks:{symbol}:delta

# Volume indicators
key = key_builder.live_indicator(symbol, "volume_ratio", "volume")
# Result: ind:volume:{symbol}:volume_ratio
```

### Indicator Categorization

**Location:** `redis_files/redis_key_standards.py` → `RedisKeyStandards._categorize_indicator()`

```python
category = RedisKeyStandards._categorize_indicator("rsi")
# Returns: "ta"

category = RedisKeyStandards._categorize_indicator("delta")
# Returns: "greeks"

category = RedisKeyStandards._categorize_indicator("volume_ratio")
# Returns: "volume"
```

---

## Troubleshooting

### Issue: Indicators Not Found in Redis

**Check:**
1. Verify Redis DB 1 connection: `redis_manager.get_client(1)`
2. Check key format: Should be `ind:{category}:{symbol}:{indicator}`
3. Verify symbol normalization: Use `RedisKeyStandards.canonical_symbol()`
4. Check TTL: Indicators expire after 300s (TA) or 3600s (Greeks)

**Debug:**
```python
from redis_files.redis_client import redis_manager
from redis_files.redis_key_standards import get_key_builder, RedisKeyStandards

db1 = redis_manager.get_client(1)
key_builder = get_key_builder()
symbol = RedisKeyStandards.canonical_symbol("NIFTY25NOV26000CE")
key = key_builder.live_indicator(symbol, "rsi", "ta")
value = db1.get(key)
print(f"Key: {key}, Value: {value}")
```

### Issue: Indicators Not Being Stored

**Check:**
1. Verify `HybridCalculations._store_indicators_atomic()` is called
2. Check Redis client is DB 1: `redis_client.get_client(1)`
3. Verify key builder is used: `get_key_builder().live_indicator(...)`
4. Check for exceptions in logs: `logger.error` in `_store_indicators_atomic()`

### Issue: Wrong Database

**Check:**
1. All indicators must be in **DB 1** (not DB 2, DB 5, etc.)
2. Use `redis_client.get_client(1)` explicitly
3. Verify key prefix: Should start with `ind:` (not `indicators:`)

---

## Migration Notes

### Legacy Key Format (Deprecated)

**Old format:** `indicators:{symbol}:{indicator_name}`  
**New format:** `ind:{category}:{symbol}:{indicator_name}`

**Migration:**
- New code uses unified format (`ind:{category}:...`)
- Old format still supported for backward compatibility
- Retrieval tries new format first, falls back to old format

---

## Performance Considerations

### TTL Strategy

- **Technical Indicators (300s):** Short TTL because they change frequently
- **Greeks (3600s):** Longer TTL because they change less frequently
- **Volume Indicators (300s):** Short TTL for real-time accuracy

### Caching Benefits

- **Redis Cache Hit:** O(1) retrieval, no calculation needed
- **Reduces CPU:** Avoids redundant TA-Lib/Polars calculations
- **Faster Pattern Detection:** Indicators available immediately

### Storage Optimization

- **Atomic Storage:** Indicators stored immediately after calculation
- **Batch Operations:** Multiple indicators stored in single operation
- **Symbol Variants:** Multiple symbol formats stored for fast lookup

---

## Related Documentation

- **Indicator Calculations:** `docs/INDICATORS_GREEKS.md`
- **Data Pipeline:** `intraday_scanner/DATA_PIPELINE_TECHNICAL.md`
- **Redis Key Standards:** `redis_files/redis_key_standards.py`
- **Storage Engine:** `core/storage/engine.py`

---

**End of Documentation**

