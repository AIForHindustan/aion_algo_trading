# Cumulative Volume Data Flow Analysis

## Summary
- **Total Occurrences**: `cumulative_volume` appears **54 times** in `redis_client.py`
- **Field Variations**: 3 canonical fields + legacy aliases
- **Storage Locations**: 5 Redis key patterns + in-memory sessions

---

## Field Name Variations

### Canonical Fields
1. **`zerodha_cumulative_volume`** (Primary - from Zerodha WebSocket)
   - Source: Zerodha WebSocket API
   - Storage: Redis hash buckets, daily keys, history lists
   - Mapped via: `SESSION_FIELD_ZERODHA_CUM`

2. **`bucket_cumulative_volume`** (Secondary - calculated bucket cumulative)
   - Storage: Session data, bucket metadata
   - Mapped via: `SESSION_FIELD_BUCKET_CUM`

3. **`cumulative_volume`** (Legacy alias)
   - Backward compatibility only
   - Mapped to `zerodha_cumulative_volume`

### Legacy Aliases (for backward compatibility)
- `volume_traded_for_the_day`
- `cumulative`
- `volume_traded`

---

## Data Flow Pipeline

### 1. **ENTRY POINT: WebSocket Parser**
**File**: `crawlers/websocket_message_parser.py`

```python
# Line 361-363: WebSocket parser receives cumulative from Zerodha
tick_data["zerodha_cumulative_volume"] = cumulative_int  # Canonical
tick_data["volume_traded_for_the_day"] = cumulative_int  # Legacy alias
tick_data["cumulative_volume"] = cumulative_int          # Legacy alias
```

**Flow**:
- Zerodha WebSocket → Raw tick data → Parser normalizes
- **Calculation Point**: Incremental volume calculated here (NOT cumulative)
- Cumulative is **passed through** as-is from Zerodha

---

### 2. **ENTRY POINT: store_ticks_optimized()**
**File**: `redis_files/redis_client.py` (Line 729)

```python
# Lines 776-786: Extract cumulative from tick with fallbacks
bucket_cumulative_volume = tick.get("bucket_cumulative_volume")
if bucket_cumulative_volume is None:
    bucket_cumulative_volume = tick.get("zerodha_cumulative_volume")
if bucket_cumulative_volume is None:
    bucket_cumulative_volume = tick.get("bucket_incremental_volume")  # Fallback

# Pass to update_volume_buckets (Line 816-822)
self.update_volume_buckets(
    symbol=symbol,
    bucket_incremental_volume=bucket_incremental_volume,
    bucket_cumulative_volume=bucket_cumulative_volume,  # ← Passed here
    timestamp=bucket_timestamp_candidate or time.time(),
    last_price=last_price,
)
```

**Flow**:
- Incoming ticks → Normalize fields → Extract cumulative → Pass to bucket updater

---

### 3. **PROCESSING: update_volume_buckets()**
**File**: `redis_files/redis_client.py` (Line 2702)

```python
# Line 2751: Convert to integer
cumulative_int = _to_int(bucket_cumulative_volume)

# Lines 2830-2831: Store in history entry
if cumulative_int is not None:
    history_entry[SESSION_FIELD_ZERODHA_CUM] = cumulative_int

# Lines 2839-2844: Store in daily key
if cumulative_int is not None:
    date_str = ts.strftime("%Y-%m-%d")
    daily_key = f"bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:daily:{date_str}"
    pipe.hset(daily_key, SESSION_FIELD_ZERODHA_CUM, cumulative_int)
    pipe.hset(daily_key, "bucket_cumulative_volume", cumulative_int)  # ← Dual storage
```

**Storage Actions**:
1. **History Lists**: `bucket_incremental_volume:history:{resolution}:{symbol}`
   - Stores cumulative in JSON entries pushed to list
   - Field: `SESSION_FIELD_ZERODHA_CUM`

2. **Daily Hash Keys**: `bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:daily:{date}`
   - Stores cumulative as hash field
   - Fields: `SESSION_FIELD_ZERODHA_CUM` + `bucket_cumulative_volume` (dual storage)

**Note**: Cumulative is **NOT stored** in intraday buckets (only incremental is)

---

### 4. **PROCESSING: CumulativeDataTracker**
**File**: `redis_files/redis_client.py` (Line 2951)

#### 4a. **update_symbol_data()** - Line 3297
```python
# Lines 3320-3323: Store cumulative in session data
if bucket_incremental_volume > 0:
    session["bucket_cumulative_volume"] = bucket_incremental_volume  # ⚠️ BUG: Uses incremental!
    session["zerodha_cumulative_volume"] = bucket_incremental_volume
```

**⚠️ ISSUE**: This method uses `bucket_incremental_volume` for cumulative fields (incorrect!)

#### 4b. **update_symbol_data_direct()** - Line 3616
```python
# Lines 3639-3640: Store raw cumulative as-is
session.update({
    'bucket_cumulative_volume': bucket_cumulative_volume,  # ✅ Correct
    'zerodha_cumulative_volume': bucket_cumulative_volume,  # ✅ Correct
    # ...
})

# Line 3654: Store in history
self.volume_history[symbol].append((timestamp, bucket_cumulative_volume))
```

**Storage Actions**:
1. **In-Memory Session**: `session_data[symbol]` dict
   - Fields: `bucket_cumulative_volume`, `zerodha_cumulative_volume`

2. **Redis Session Key**: `session:{symbol}:{session_date}`
   - JSON-encoded session data
   - TTL: 86400 seconds

3. **Time Buckets**: Bucket metadata includes `cumulative_open` and `cumulative_close`
   - Lines 3389-3390: Initialize from session
   - Line 3401: Update `cumulative_close` from session

---

### 5. **RETRIEVAL POINTS**

#### 5a. **get_cumulative_data()** - Line 3445
```python
# Get from in-memory session or Redis
session_key = f"session:{symbol}:{session_date}"
data = self.redis.get(session_key)
```

#### 5b. **get_time_buckets()** - Line 2995, 3468, 3677
```python
# Lines 3044, 3111: Extract from bucket hashes
"zerodha_cumulative_volume": int(hash_data.get(b"zerodha_cumulative_volume", 0))
```

#### 5c. **validate_volume_consistency()** - Line 2498
```python
# Line 2504-2505: Check last_cumulative key
last_cumulative_key = f"last_cumulative:{symbol}"
last_cumulative = self.redis_client.get(last_cumulative_key)

# Lines 2545-2552: Check daily cumulative
daily_key = f"bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:daily:{date_str}"
dd = self.redis_client.hgetall(daily_key)
if "zerodha_cumulative_volume" in dd:
    daily_cum = int(dd.get("zerodha_cumulative_volume", 0))
elif "bucket_cumulative_volume" in dd:
    daily_cum = int(dd.get("bucket_cumulative_volume", 0))
```

#### 5d. **get_volume_from_bucket()** - Line 2899
```python
# Lines 2915-2916: Fallback to cumulative if incremental not found
elif 'zerodha_cumulative_volume' in bucket_data:
    bucket_incremental_volume = float(bucket_data['zerodha_cumulative_volume'] or 0)
```

**⚠️ ISSUE**: This incorrectly uses cumulative volume as incremental!

---

## Redis Storage Locations

### 1. **Intraday Bucket Hashes** (NOT stored here)
**Key Pattern**: `bucket_incremental_volume:bucket_incremental_volume:bucket{1|2}:{symbol}:buckets:{hour}:{index}`
- **Cumulative**: ❌ NOT stored
- **Only Incremental**: ✅ Stored in `bucket_incremental_volume` field

### 2. **Daily Cumulative Hash**
**Key Pattern**: `bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:daily:{date}`
**Fields**:
- `SESSION_FIELD_ZERODHA_CUM` (canonical field name)
- `bucket_cumulative_volume` (dual storage for compatibility)

### 3. **History Lists**
**Key Pattern**: `bucket_incremental_volume:history:{resolution}:{symbol}`
**Data**: JSON entries containing:
- `timestamp`
- `SESSION_FIELD_BUCKET_INC` (incremental)
- `SESSION_FIELD_ZERODHA_CUM` (cumulative) ← if provided
- `hour`, `bucket_index`

### 4. **Session Data**
**Key Pattern**: `session:{symbol}:{session_date}`
**Data**: JSON-encoded session dict containing:
- `bucket_cumulative_volume`
- `zerodha_cumulative_volume`
- `time_buckets` (nested dict)

### 5. **Last Cumulative Tracking**
**Key Pattern**: `last_cumulative:{symbol}`
**Data**: Single integer value
**Purpose**: Track last cumulative for incremental calculation

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Zerodha WebSocket                                        │
│    └─> Raw tick with cumulative volume                       │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. WebSocket Parser                                         │
│    └─> Normalize: zerodha_cumulative_volume                  │
│    └─> Calculate: bucket_incremental_volume                 │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Data Pipeline / store_ticks_optimized()                 │
│    └─> Extract cumulative from tick                         │
│    └─> Pass to update_volume_buckets()                      │
└───────────────────────┬─────────────────────────────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────────┐          ┌───────────────────┐
│ 4a. update_volume │          │ 4b. CumulativeData │
│     _buckets()     │          │     Tracker        │
│                   │          │                    │
│ ┌───────────────┐ │          │ ┌────────────────┐ │
│ │ History Lists │ │          │ │ Session Data    │ │
│ │ (JSON)        │ │          │ │ (In-Memory)     │ │
│ └───────────────┘ │          │ └────────────────┘ │
│                   │          │ ┌────────────────┐ │
│ ┌───────────────┐ │          │ │ Redis Session  │ │
│ │ Daily Hash    │ │          │ │ (JSON)         │ │
│ │ (HASH)        │ │          │ └────────────────┘ │
│ └───────────────┘ │          │                    │
└───────────────────┘          └───────────────────┘
        │                               │
        └───────────────┬───────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Redis Storage                                            │
│    ├─> bucket_incremental_volume:history:* (Lists)         │
│    ├─> bucket_incremental_volume:*:daily:* (Hashes)        │
│    ├─> session:*:* (Strings/JSON)                          │
│    └─> last_cumulative:* (Strings)                          │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. Retrieval Methods                                        │
│    ├─> get_cumulative_data()                                │
│    ├─> get_time_buckets()                                  │
│    ├─> validate_volume_consistency()                        │
│    └─> get_volume_from_bucket()                            │
└─────────────────────────────────────────────────────────────┘
```

---

## Key Issues Identified

### ❌ Issue 1: `update_symbol_data()` Uses Incremental for Cumulative
**Location**: Line 3322-3323
```python
session["bucket_cumulative_volume"] = bucket_incremental_volume  # BUG!
session["zerodha_cumulative_volume"] = bucket_incremental_volume  # BUG!
```
**Fix**: Should receive `bucket_cumulative_volume` parameter or extract from tick

### ❌ Issue 2: `get_volume_from_bucket()` Falls Back to Cumulative
**Location**: Line 2915-2916
```python
elif 'zerodha_cumulative_volume' in bucket_data:
    bucket_incremental_volume = float(bucket_data['zerodha_cumulative_volume'] or 0)
```
**Fix**: Should NOT use cumulative as incremental fallback

### ⚠️ Issue 3: Dual Storage in Daily Keys
**Location**: Lines 2842-2843
```python
pipe.hset(daily_key, SESSION_FIELD_ZERODHA_CUM, cumulative_int)
pipe.hset(daily_key, "bucket_cumulative_volume", cumulative_int)  # Redundant?
```
**Note**: This is intentional for backward compatibility but adds storage overhead

---

## Usage Statistics

| Method/Field | Occurrences | Purpose |
|--------------|-------------|---------|
| `zerodha_cumulative_volume` | 28 | Primary cumulative field |
| `bucket_cumulative_volume` | 18 | Secondary/calculated cumulative |
| `cumulative_volume` | 8 | Legacy alias |
| `update_volume_buckets()` | 1 | Main processing method |
| `CumulativeDataTracker` | 1 | Session-based tracker class |
| `get_cumulative_data()` | 1 | Retrieval method |
| `validate_volume_consistency()` | 1 | Debug/validation method |

---

## Recommendations

1. **Fix `update_symbol_data()`**: Accept cumulative parameter or extract from tick
2. **Fix `get_volume_from_bucket()`**: Remove cumulative fallback
3. **Consolidate Field Names**: Migrate fully to canonical names (`zerodha_cumulative_volume`)
4. **Document Storage Schema**: Clarify which fields are stored where
5. **Add Validation**: Ensure cumulative is never used as incremental

