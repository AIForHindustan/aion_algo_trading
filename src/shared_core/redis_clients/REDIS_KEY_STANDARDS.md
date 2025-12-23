# Redis Key Lookup Standards - ENFORCED RULE

## CRITICAL RULE: Direct Key Lookups Only

**ALL Redis operations MUST use direct key lookups. Pattern matching (KEYS, SCAN) is FORBIDDEN.**

### Why This Rule Exists

1. **Performance**: O(1) direct lookup vs O(N) blocking scan
2. **Non-blocking**: Pattern matching blocks Redis during scans
3. **Predictable**: Direct lookups have consistent performance
4. **Connection Safety**: Prevents connection pool exhaustion from slow operations

### ✅ CORRECT: Direct Key Lookups

```python
# Direct key lookup - symbol/ID is known
alert_id = "alert_12345"
data = redis_client.get(f"alert:{alert_id}")

# Direct key lookup - indicator for known symbol
indicators = redis_client.get(f"indicators:{symbol}:RSI")

# Direct hash lookup
session_data = redis_client.hgetall(f"session:{symbol}:{date}")

# Stream iteration (for time-series data)
messages = redis_client.xrevrange("alerts:stream", count=100)

# Sorted set range (known key)
ohlc_data = redis_client.zrange(f"ohlc_daily:{symbol}", -100, -1)
```

### ❌ FORBIDDEN: Pattern Matching

```python
# ❌ FORBIDDEN - Pattern matching
keys = redis_client.keys("alert:*")
keys = redis_client.keys("news:symbol:*")
keys = redis_client.scan(match="pattern:*")

# ❌ FORBIDDEN - Even with SCAN (still O(N))
for key in redis_client.scan_iter(match="pattern:*"):
    ...
```

### How to Replace Pattern Matching

#### Scenario 1: Iterating Over Alerts

**❌ BAD:**
```python
alert_keys = redis_client.keys("alert:*")
for key in alert_keys:
    data = redis_client.get(key)
```

**✅ GOOD - Use Streams:**
```python
# Alerts are published to streams - read from stream
messages = redis_client.xrevrange("alerts:stream", count=1000)
for msg_id, msg_data in messages:
    alert_data = json.loads(msg_data['data'])
```

**✅ GOOD - Known Alert IDs:**
```python
# If you have alert IDs (from another source), use direct lookup
alert_ids = ["alert_123", "alert_456", "alert_789"]  # From stream/config
for alert_id in alert_ids:
    data = redis_client.get(f"alert:{alert_id}")
```

#### Scenario 2: Getting Indicators for Multiple Symbols

**❌ BAD:**
```python
indicator_keys = redis_client.keys("indicators:*:RSI")
for key in indicator_keys:
    ...
```

**✅ GOOD - Known Symbol List:**
```python
# Symbols come from config/active universe, not Redis scan
symbols = ["NIFTY", "BANKNIFTY", "RELIANCE"]  # From crawler config
for symbol in symbols:
    rsi = redis_client.get(f"indicators:{symbol}:RSI")
```

#### Scenario 3: News Data

**❌ BAD:**
```python
news_keys = redis_client.keys("news:symbol:*")
```

**✅ GOOD - Direct Lookup Per Symbol:**
```python
# Get symbols from tick data or active universe
for symbol in active_symbols:
    news_key = f"news:symbol:{symbol}"
    news_data = redis_client.zrange(news_key, -1, -1)
```

#### Scenario 4: Time-Series Data

**❌ BAD:**
```python
bucket_keys = redis_client.keys("bucket_incremental_volume:*:5min:*")
```

**✅ GOOD - Direct Key Construction:**
```python
# Build key from known parameters
symbol = "NIFTY"
resolution = "5min"
bucket_key = f"bucket_incremental_volume:history:{resolution}:{resolution}:{symbol}"
buckets = redis_client.lrange(bucket_key, -500, -1)
```

## Indicator Batch Calculation

Indicators are calculated in **batches**, not per-tick:

1. **Batch Processing**: `HybridCalculations.batch_process_symbols()` processes multiple ticks per symbol
2. **Polars DataFrame**: Uses Polars for fast vectorized operations
3. **Batch Size**: Processes up to 50 ticks per symbol at once
4. **Pipeline Integration**: Data pipeline accumulates ticks in `batch_buffer`, then flushes for batch calculation

**Flow:**
```
Tick → batch_buffer → _flush_batch() → batch_process_symbols() → Store indicators
```

**Key Methods:**
- `batch_calculate_indicators()`: Batch calculation for single symbol
- `batch_process_symbols()`: Process multiple symbols with multiple ticks each
- Uses Polars DataFrame for O(N) vectorized operations instead of O(N²) loops

## Enforcement

- **Code Review**: Reject PRs with pattern matching
- **Static Analysis**: Use tools to detect `.keys()` and `.scan()` calls
- **Runtime Checks**: (Optional) Add runtime validation in `RedisKeyStandards.validate_no_pattern_matching()`

## Migration Checklist

- [x] Dashboard: Removed `.keys("alert:*")` → Use streams
- [x] Dashboard: Removed `.keys("forward_validation:alert:*")` → Use validation stream
- [x] Data Pipeline: Removed `.keys("news:symbol:*")` → Direct lookup per known symbol
- [ ] Dashboard: Fix remaining news pattern matching (news:MARKET_NEWS:*, news:item:*, news:latest:*)
- [ ] Dashboard: Fix news:*:* pattern matching
- [ ] Review all files in grep results for remaining violations

