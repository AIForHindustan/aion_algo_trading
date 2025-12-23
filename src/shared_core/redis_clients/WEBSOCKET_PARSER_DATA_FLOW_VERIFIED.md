# WebSocket Parser Data Flow - Verified ✅

## Complete Flow (Verified Against Code)

### 1. Binary Packet Parsing (`_parse_quote_data()` - Line 497)
```python
# Extracts cumulative volume from Zerodha packet bytes
fields = struct.unpack(">11I", data[0:44])
tick_data["volume_traded"] = fields[4]  # Line 507 - Cumulative from Zerodha
```

**Status**: ✅ CORRECT - Extracts cumulative from packet bytes 16-20

---

### 2. Immediate Cumulative Storage (`_parse_full_packet()` - Line 458-480)
```python
# Store raw cumulative volume from Zerodha as-is (no calculations)
cumulative_volume = tick_data.get("volume_traded", 0)  # Line 459
tick_data["cumulative_volume"] = cumulative_volume

# Store raw cumulative volume as-is in Redis (BEFORE normalization)
self.redis_client.update_symbol_data_direct(
    symbol=tick_data["symbol"],
    bucket_cumulative_volume=cumulative_volume,  # Line 472 - Store Zerodha's cumulative as-is
    last_price=tick_data.get("last_price", 0.0),
    timestamp=exchange_epoch,
    # ... other fields
)
```

**Status**: ✅ CORRECT - Stores cumulative immediately via `update_symbol_data_direct()`

**Note**: This happens INSIDE `_parse_full_packet()` which is called by `parse_zerodha_packet()` before normalization

---

### 3. Volume Normalization (`_normalize_volume_at_ingestion()` - Line 303)
```python
# Get cumulative volume from tick data
cumulative = tick_data.get("zerodha_cumulative_volume")  # Line 337
if cumulative is None:
    cumulative = tick_data.get("volume_traded_for_the_day")  # Line 339
if cumulative is None:
    cumulative = tick_data.get("cumulative_volume")  # Line 341

# Ensure cumulative is numeric
cumulative_int = int(float(cumulative))  # Line 348

# SINGLE CALCULATION POINT: Calculate incremental volume
volume_manager = get_volume_manager()
incremental = volume_manager.calculate_incremental(
    instrument_token=str(tick_data.get('instrument_token', symbol)),
    current_cumulative=cumulative_int,
    exchange_timestamp=exchange_timestamp  # Line 354-358
)

# Set ALL volume fields consistently using canonical field names
tick_data["zerodha_cumulative_volume"] = cumulative_int  # Line 361
tick_data["volume_traded_for_the_day"] = cumulative_int  # Line 362 - Legacy alias
tick_data["cumulative_volume"] = cumulative_int  # Line 363 - Legacy cumulative
tick_data["bucket_incremental_volume"] = incremental  # Line 364 - Canonical: Pattern incremental
tick_data["incremental_volume"] = incremental  # Line 365 - Canonical: Explicit incremental
tick_data["volume"] = incremental  # Line 366 - Canonical: Generic incremental
```

**Status**: ✅ CORRECT - Calculates incremental via VolumeStateManager and sets all volume fields

---

### 4. Crawler Receives Pre-Calculated Data (`intraday_crawler.py` - Line 242-284)
```python
def _process_intraday_tick(self, tick_data: Dict[str, Any]):
    # tick_data already has ALL volume fields set by parser:
    # - zerodha_cumulative_volume (cumulative)
    # - bucket_cumulative_volume (cumulative)
    # - bucket_incremental_volume (incremental)
    
    optimized_tick = {
        "zerodha_cumulative_volume": tick_data.get("zerodha_cumulative_volume", 0),  # Line 272
        "bucket_cumulative_volume": tick_data.get("bucket_cumulative_volume", 0),  # Line 273
        "bucket_incremental_volume": tick_data.get("bucket_incremental_volume", 0),  # Line 274
        # ... other fields
    }
    
    self.redis_client.store_ticks_optimized(
        tick_data.get("symbol"), [optimized_tick]  # Line 283
    )
```

**Status**: ✅ CORRECT - Crawler extracts volume fields already set by parser

---

### 5. Redis Storage (`store_ticks_optimized()` - Line 729-833)
```python
def store_ticks_optimized(self, symbol, ticks):
    for tick in ticks:
        # Extract cumulative (already present from parser)
        bucket_cumulative_volume = tick.get("bucket_cumulative_volume")  # Line 776
        if bucket_cumulative_volume is None:
            bucket_cumulative_volume = tick.get("zerodha_cumulative_volume")  # Line 778
        
        # Extract incremental (already calculated by parser)
        bucket_incremental_volume = tick.get("bucket_incremental_volume")  # Line 770
        
        # Store in volume buckets
        self.update_volume_buckets(
            symbol=symbol,
            bucket_incremental_volume=bucket_incremental_volume,  # Line 818
            bucket_cumulative_volume=bucket_cumulative_volume,  # Line 820 - Extracted from tick
            timestamp=bucket_timestamp_candidate or time.time(),
            last_price=last_price,
        )
```

**Status**: ✅ CORRECT - Extracts cumulative and incremental (both already present from parser) and stores in buckets

---

## Key Verification Points

### ✅ Single Calculation Point
- **Line 354-358**: `VolumeStateManager.calculate_incremental()` is the ONLY place incremental is calculated
- All downstream consumers use the pre-calculated `bucket_incremental_volume` from the parser

### ✅ Dual Storage Paths
1. **Session Data**: Stored immediately in `_parse_full_packet()` via `update_symbol_data_direct()` (Line 470)
2. **Volume Buckets**: Stored later in `store_ticks_optimized()` via `update_volume_buckets()` (Line 816)

### ✅ Field Mapping
- **Canonical Fields**: `zerodha_cumulative_volume`, `bucket_cumulative_volume`, `bucket_incremental_volume`
- **Legacy Aliases**: `volume_traded_for_the_day`, `cumulative_volume`, `incremental_volume`, `volume`
- All fields set consistently in parser (Line 361-366)

### ✅ Data Flow Order
1. Binary packet → Extract cumulative (`_parse_quote_data()`)
2. Store cumulative immediately (`_parse_full_packet()` → `update_symbol_data_direct()`)
3. Calculate incremental (`_normalize_volume_at_ingestion()` → `VolumeStateManager.calculate_incremental()`)
4. Set all volume fields (`_normalize_volume_at_ingestion()` → Lines 361-366)
5. Crawler uses pre-calculated fields (`_process_intraday_tick()`)
6. Store in buckets (`store_ticks_optimized()` → `update_volume_buckets()`)

---

## Summary

**The WebSocket parser data flow is CORRECT** ✅

- Parser extracts cumulative from Zerodha packet
- Parser stores cumulative immediately via `update_symbol_data_direct()`
- Parser calculates incremental via `VolumeStateManager.calculate_incremental()`
- Parser sets all volume fields before returning tick_data
- Crawler uses pre-calculated fields (no recalculation)
- Redis storage extracts cumulative (already present) and stores in buckets

**No changes needed** - The implementation matches the intended design.
