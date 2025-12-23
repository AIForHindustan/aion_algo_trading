# Data Pipeline Technical Documentation

**⚠️ IMPORTANT: Always read docstrings in source code before making assumptions. This document provides high-level architecture and troubleshooting guidance, but method signatures, parameters, and implementation details are documented in the actual code.**

**Last Updated**: November 2025  
**File**: `intraday_scanner/data_pipeline.py`  
**Status**: ✅ Redis 8.2 Optimized

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Core Components](#core-components)
3. [Data Flow](#data-flow)
4. [Key Functions & Methods](#key-functions--methods)
5. [News Processing](#news-processing)
6. [OHLC Data Handling](#ohlc-data-handling)
7. [Indicator Calculations](#indicator-calculations)
8. [Storage & Redis Operations](#storage--redis-operations)
9. [Code Quality & Maintenance](#code-quality--maintenance)
10. [Troubleshooting Guide](#troubleshooting-guide)

---

## Architecture Overview

### Purpose

The `DataPipeline` class is the main data ingestion pipeline that:
- Subscribes to Redis channels (ticks, news, indices, premarket orders)
- Parses and cleans incoming tick data from Zerodha WebSocket
- Calculates derived fields and indicators
- Stores data in Redis (multi-database architecture)
- Forwards news and alerts to `alert_manager`
- Provides tick data to pattern detection engine

### Redis 8.2 Integration

- **Database Segmentation**: Uses `store_by_data_type()` for automatic DB routing
- **Connection Management**: Uses `RedisManager82.get_client()` for process-specific pools
- **Storage Layer**: `RedisStorage.queue_tick_for_storage()` for batched tick storage
- **Hot Token Mapper**: Uses `hot_token_mapper.py` (no Redis dependency for token resolution)

### Key Design Principles

1. **Separation of Concerns**: Storage only, no calculations in Redis layer
2. **Event-Driven**: Channel-based processing (no polling)
3. **Batched Operations**: Redis 8.2 optimizations (hset mapping, stream maxlen)
4. **Fallback Chains**: Cache → Redis → Fresh calculation

---

## Core Components

### Initialization

```python
DataPipeline(
    redis_client=None,
    config=None,
    pattern_detector=None,
    alert_manager=None,
    tick_processor=None
)
```

**Key Initializations**:
- `RedisStorage`: Batched tick storage (Redis 8.2 optimized)
- `HybridCalculations`: Indicator calculations (Polars-based, with cache)
- `HistoricalArchive`: Tier 2 archival (Parquet files, optional PostgreSQL)
- `HotTokenMapper`: Token-to-symbol resolution (no Redis dependency)

---

## Data Flow

### Tick Processing Flow

```
Zerodha WebSocket → Redis Channel (market_data.ticks)
    ↓
_process_market_tick() → Parse message
    ↓
_clean_tick_data() → Comprehensive cleaning
    ↓
process_tick() → Storage + calculations
    ↓
redis_storage.queue_tick_for_storage() → Batched storage (Redis 8.2)
    ↓
hybrid_calculations.batch_calculate_indicators() → Indicator calculation
    ↓
_store_calculated_indicators() → Store in Redis DB 1 (via UnifiedDataStorage)
    ↓
unified_storage.store_indicators() → Technical indicators (ind:ta:*)
unified_storage.store_greeks() → Greeks (ind:greeks:*)
```

### News Processing Flow

```
gift_nifty_gap.py → Stores news in news:symbol:{symbol} keys (every 30 seconds)
    ↓
Publishes to market_data.news channel
    ↓
_process_news_from_channel() → Processes channel message
    ↓
alert_manager.send_news_alert() → Forwards to alert manager
```

**Note**: News is stored in Redis every 30 seconds (not streamed). Channel-based approach processes news when published.

### OHLC Data Flow

```
Zerodha Tick → Contains OHLC in every tick
    ↓
_clean_ohlc_data() → Extracts OHLC from tick
    ↓
tick_data['ohlc'] → Available in cleaned tick
    ↓
scanner_main._get_current_ohlc() → Uses OHLC from tick_data (optimized)
    ↓
Fallback to Redis only if OHLC missing in tick
```

**Optimization**: OHLC is extracted from Zerodha ticks, not fetched from Redis. Redis is only used as fallback.

---

## Key Functions & Methods

### Tick Processing

#### `_process_market_tick(data)` (Line 1706)
- **Purpose**: Parse Redis stream message into tick data
- **Input**: Raw message from `market_data.ticks` channel
- **Output**: Cleaned tick data dictionary
- **Key Steps**:
  1. Extract symbol using `_extract_symbol()`
  2. Set symbol in data dictionary
  3. Call `_clean_tick_data()` for comprehensive cleaning
  4. Call `process_tick()` for storage and calculations

#### `_clean_tick_data(tick_data)` (Line 3804)
- **Purpose**: Comprehensive cleaning and validation
- **Trusts**: Symbol from upstream (`_process_market_tick` sets it)
- **Cleans**:
  - Numeric fields (last_price, volume, timestamps)
  - OHLC data (extracted from Zerodha tick)
  - Market depth (bid/ask levels)
  - Field normalization and type conversion
- **Returns**: Cleaned tick data dictionary

#### `process_tick(tick_data)` (Line 1588)
- **Purpose**: Storage and indicator calculations
- **Trusts**: `tick_data` is already cleaned by `_clean_tick_data()`
- **Steps**:
  1. Token mapping (via `hot_token_mapper`)
  2. Queue for storage (via `redis_storage.queue_tick_for_storage()`)
  3. Calculate indicators (via `hybrid_calculations.batch_calculate_indicators()`)
  4. Store indicators (via `_store_calculated_indicators()`)

### Derived Fields

#### `_calculate_derived_fields(cleaned)` (Line 3483)
- **Purpose**: Calculate derived/computed fields before storage
- **Why Needed**: Redis stores data, doesn't calculate. Derived fields must be computed before storage.
- **Calculates**:
  - `price_change_pct`: `(last_price - open_price) / open_price * 100`
  - Unified schema derived fields (via `calculate_derived_fields()`)
  - Field mappings (`weighted_bid` → `weighted_bid_price`)
  - Depth-based calculations (`bid_ask_spread`, `mid_price`)
- **Status**: ✅ **NECESSARY** - `price_change_pct` is actively used downstream

### Symbol Extraction

#### `_extract_symbol(tick_data)` (Line 3108)
- **Purpose**: Extract trading symbol from tick data
- **Methods**:
  1. Token resolution via `hot_token_mapper`
  2. Symbol extraction from field names
  3. Fallback to default if not found
- **Note**: Only called as defensive fallback. Upstream functions set symbol explicitly.

#### `_extract_underlying_from_symbol(symbol)` (Line 2668)
- **Purpose**: Extract underlying equity from F&O instruments
- **Handles**: Options (CE/PE), Futures (FUT), Index names
- **Used For**: News lookup (F&O instruments map to underlying equity)

### Sector Mapping

#### `_get_sector_from_symbol_name(symbol)` (Line 2755)
- **Purpose**: Map symbol to sector using keyword pattern matching
- **Source**: Hardcoded keyword matching (does not use sector_volatility.json)
- **Note**: `sector_volatility.json` is now in `shared_core/config/` and used by MM exploitation strategies
- **Priority**: Sector-specific indices (NIFTY_BANK, NIFTY_IT) over NIFTY_50
- **Fallback**: Pattern matching for symbols not in JSON

---

## News Processing

### Architecture

**Storage**: `gift_nifty_gap.py` stores news in symbol-specific keys (`news:symbol:{symbol}`) every 30 seconds.

**Processing**: `_process_news_from_channel()` processes news from `market_data.news` channel (event-driven, no polling).

### Key Functions

#### `_process_news_from_channel(data)` (Line 4046)
- **Status**: ✅ **ACTIVELY USED**
- **Purpose**: Process news from Redis channel and forward to alert_manager
- **Key Points**:
  - No duplicate storage (gift_nifty_gap.py already stores news)
  - Invalidates cache for symbols to fetch fresh news
  - Forwards to `alert_manager.send_news_alert()`
- **Removed**: Duplicate storage (`news:latest:{timestamp}`, `news:MARKET_NEWS:{timestamp}`)

#### `_get_news_for_symbol(symbol)` (Line 2472)
- **Purpose**: Get recent news for symbol from Redis
- **Source**: Reads from `news:symbol:{symbol}` sorted sets
- **Cache**: 30-minute cache to reduce Redis calls
- **Uses**: `_get_news_keys_for_symbol()` for F&O → underlying equity mapping

#### `_get_news_keys_for_symbol(symbol)` (Line 2561)
- **Purpose**: Map symbol to Redis keys for news lookup
- **Architectural Fix**: F&O instruments map to underlying equity (news doesn't exist for F&O)
- **Returns**: List of keys to try (underlying equity + sector indices)

#### `_calculate_news_relevance(symbol, news_data)` (Line 2807)
- **Purpose**: Calculate relevance score (0.0-1.0) for news to symbol
- **Uses**: Sector matching, keyword matching, company-specific terms
- **For F&O**: Uses underlying equity symbol for relevance calculation

### Removed Functions

- `_process_news_from_symbol_keys()` (Line 4113) - ❌ **REMOVED** (orphaned, redundant)
  - Was polling-based (every 30 seconds)
  - Redundant with channel-based approach
  - News already stored by gift_nifty_gap.py

---

## OHLC Data Handling

### Zerodha Provides OHLC in Every Tick

**Key Point**: Zerodha WebSocket provides OHLC (Open, High, Low, Close) in every tick. No need to fetch from Redis.

### Data Flow

```
Zerodha Tick → Contains OHLC
    ↓
_clean_ohlc_data() → Extracts OHLC
    ↓
tick_data['ohlc'] → Available in cleaned tick
    ↓
scanner_main._get_current_ohlc(tick_data) → Uses OHLC from tick_data
    ↓
Fallback to Redis only if OHLC missing
```

### Optimization Applied

**Before**: `scanner_main._get_current_ohlc()` fetched OHLC from Redis for every tick.

**After**: Uses OHLC from `tick_data` first, falls back to Redis only if missing.

**Impact**: Reduces Redis calls from every tick to only when OHLC is missing (rare edge case).

### Functions That Legitimately Fetch Historical OHLC

These functions fetch historical OHLC (not current tick), which is correct:

1. `alert_manager._get_ohlc_data_from_redis()` - Last 20 days for ATR calculation
2. `pattern_detector._get_timeframe_data()` - Multi-timeframe analysis (1h, 4h, 1d)
3. `redis_client._get_ohlc_buckets()` - Historical OHLC buckets for time-series analysis
4. `dragonfly/batch_calculate_indicators_55d.get_55d_ohlc_from_redis()` - 55-day historical data

---

## Indicator Calculations

### HybridCalculations Integration

**Primary Calculator**: `HybridCalculations` (from `calculations.py`)
- **Method**: `batch_calculate_indicators([tick_data], symbol)`
- **Features**: Polars-based, in-memory cache, Redis fallback
- **Indicators**: 22+ indicators (technical, Greeks, option metrics)

### Storage

#### `_store_calculated_indicators(indicators_dict)` (Line 3783)
- **Purpose**: Store calculated indicators in Redis DB 1
- **Method**: Uses `UnifiedDataStorage` for standardized storage
- **Database**: **DB 1** (realtime) via `unified_storage.store_indicators()` and `unified_storage.store_greeks()`
- **Key Patterns**: 
  - Technical indicators: `ind:ta:{symbol}:{indicator_name}` (e.g., `ind:ta:NIFTY:rsi`)
  - Greeks: `ind:greeks:{symbol}:{greek_name}` (e.g., `ind:greeks:NIFTY25NOV26000CE:delta`)
  - Volume indicators: `ind:volume:{symbol}:{indicator_name}`
- **Indicators Stored**: All 22+ calculated indicators (technical, Greeks, option metrics, volume profiles)
- **Greeks Storage**: Extracted from top-level `indicators` dict and stored separately via `store_greeks()`

#### `_fetch_indicators_from_redis(symbol)` (Line 2345)
- **Purpose**: Fetch indicators from Redis (fallback mechanism)
- **Priority**: DB 1 (realtime indicators) → Cache → Fresh calculation
- **Used By**: `process_batch_indicators()` as fallback if calculations fail

### Batch Processing

#### `process_batch_indicators(symbols)` (Line 2085)
- **Purpose**: Batch calculate indicators for multiple symbols
- **Method**:
  1. Calculate via `HybridCalculations.batch_calculate_indicators()`
  2. Fallback to Redis if calculations fail or return empty
  3. Store results in **Redis DB 1** via `UnifiedDataStorage`

---

## Storage & Redis Operations

### Redis Database Segmentation

| Database | Purpose | Data Type | Key Pattern |
|----------|---------|-----------|-------------|
| **DB 0** | Default | General | Various |
| **DB 1** | Realtime | Streams, news, alerts, **indicators, Greeks** | `ticks:intraday:processed`, `news:*`, `alerts:*`, `ind:ta:*`, `ind:greeks:*`, `ind:volume:*` |
| **DB 2** | Volume State | Volume profiles | `volume:*`, `volume_profile:*` |
| **DB 3** | Premarket | Premarket data | `premarket:*` |

**Note**: All indicators (technical, Greeks, volume) are stored in **DB 1** via `UnifiedDataStorage`, not DB 5. The key patterns use the `ind:` prefix with category suffixes (`ta:`, `greeks:`, `volume:`).

### Storage Methods

#### `redis_storage.queue_tick_for_storage(tick_data)`
- **Purpose**: Queue tick for batched Redis storage
- **Method**: Batched storage (Redis 8.2 optimized with hset mapping)
- **Database**: DB 1 (realtime) via `store_by_data_type("stream_data", ...)`
- **Batching**: Flushes when batch size (50) or time threshold (100ms) reached

#### `store_by_data_type(data_type, key, value, ttl=None)`
- **Purpose**: Route data to correct database based on data type
- **Routing**: Automatic DB selection based on `data_type` parameter
- **Used For**: All Redis storage operations (news, indicators, alerts, etc.)

### Historical Archive

#### `HistoricalArchive` (Tier 2 Archival)
- **Purpose**: Long-term storage of all tick data
- **Default**: Parquet files (`historical_archive/parquet/`)
- **Optional**: PostgreSQL/TimescaleDB (if `enable_postgres_archive=True` and `psycopg2` installed)
- **Status**: Currently using Parquet (PostgreSQL not configured)

---

## Code Quality & Maintenance

### Orphaned Code Removed

**Total Removed**: 11 orphaned functions, 3 unused imports, 1 entire class

**Key Removals**:
- `_process_news_from_symbol_keys()` - Orphaned, redundant with channel-based approach
- `NewsIntegratedPatternDetector` - Entire class removed
- Various helper functions that were never called

**Verification**: Use `rg` to search for function usage before removing.

### Redundancy Eliminated

1. **Symbol Extraction**: Symbol extracted once upstream, trusted downstream
2. **OHLC Data**: Uses OHLC from tick_data, not Redis (unless missing)
3. **News Storage**: No duplicate storage (gift_nifty_gap.py handles storage)
4. **Cleaning Logic**: Comprehensive cleaning in `_clean_tick_data()`, not duplicated in `process_tick()`

### Precomputed Indicators

**Legacy Method Removed**: `_precompute_all_indicators()` (only calculated 4 indicators)

**Replaced With**: `HybridCalculations` cache/Redis fallback chain:
1. HybridCalculations in-memory cache
2. Redis DB 1 (realtime indicators via UnifiedDataStorage)
3. Fresh calculation via `HybridCalculations.batch_calculate_indicators()`

### Greeks Storage & Debugging

**Storage Method**: Greeks are extracted from top-level `indicators` dict (not nested `'greeks'` key) and stored via `unified_storage.store_greeks()` in Redis DB 1.

**Key Pattern**: `ind:greeks:{canonical_symbol}:{greek_name}` (e.g., `ind:greeks:NIFTY25NOV26000CE:delta`)

**Debugging**: Comprehensive debugging added via `_calculate_greeks_with_comprehensive_debug()`:
- `[GREEKS_DEEP_DEBUG]` - Detailed calculation steps
- `[GREEKS_STORAGE]` - Storage operations
- `[GREEKS_FALLBACK]` - Fallback calculations

**Important**: Greeks are added directly to `indicators` dict in `calculations.py` via `indicators.update(greeks)`, so extraction looks at top-level keys, not a nested structure.

### Volume Ratio Preservation

**Issue Fixed**: `calculate_derived_fields()` in `config/schemas.py` was recalculating `volume_ratio` as `volume / normalized_volume`, overwriting the correct value from websocket parser.

**Fix**: Only calculate `volume_ratio` if it doesn't already exist in `tick_data`. This preserves the correct value from VolumeManager/websocket parser.

**Flow**: `volume_ratio` is calculated by websocket parser → preserved through `_clean_tick_data()` → preserved in `calculations.py` → stored in Redis.

---

## Troubleshooting Guide

### Common Issues

#### 1. "Redis not available" errors
- **Check**: Redis connection via `redis_files/redis_health.py`
- **Verify**: `RedisManager82.get_client()` is working
- **Solution**: Ensure Redis is running and connection config is correct

#### 2. "Symbol not found" errors
- **Check**: `hot_token_mapper.py` is initialized
- **Verify**: Token lookup file exists: `core/data/token_lookup_enriched.json`
- **Solution**: Ensure token mapper is loaded before processing ticks

#### 3. "OHLC missing" warnings
- **Check**: Zerodha tick data structure
- **Verify**: `_clean_ohlc_data()` is extracting OHLC correctly
- **Solution**: Fallback to Redis should handle edge cases

#### 4. "News not processed" issues
- **Check**: `gift_nifty_gap.py` is publishing to `market_data.news` channel
- **Verify**: `_process_news_from_channel()` is subscribed to channel
- **Solution**: Ensure news is being published and channel subscription is active

#### 5. "Indicators not calculated" errors
- **Check**: `HybridCalculations` is initialized
- **Verify**: `process_batch_indicators()` is being called
- **Solution**: Check Redis fallback is working if calculations fail

### Debugging Commands

```bash
# Check if function is called
rg -n "\bfunction_name\b" intraday_scanner/data_pipeline.py

# Check if function is used externally
rg -n "\bfunction_name\b" --type py | grep -v "data_pipeline.py"

# Check Redis operations
rg -n "store_by_data_type|retrieve_by_data_type" intraday_scanner/data_pipeline.py

# Check news processing
rg -n "_process_news|_get_news" intraday_scanner/data_pipeline.py

# Check OHLC handling
rg -n "ohlc|OHLC" intraday_scanner/data_pipeline.py
```

### Performance Monitoring

- **Tick Processing Rate**: Check `stats["ticks_processed"]`
- **Error Rate**: Check `stats["errors"]`
- **Cache Hit Rate**: Monitor `news_cache` and `HybridCalculations._cache`
- **Redis Load**: Monitor Redis connection pool usage

---

## Key Standards & Best Practices

### 1. Always Read Docstrings
- Method signatures, parameters, return values are documented in code
- Don't assume - check the actual implementation

### 2. Use Hot Token Mapper
- No Redis dependency for token resolution
- O(1) lookup after initial load

### 3. Trust Upstream Data
- If symbol is extracted upstream, trust it downstream
- Only perform defensive checks if truly missing

### 4. Separation of Concerns
- Storage layer: Only stores/relays data
- Calculations: Handled by `HybridCalculations`
- Alert creation: Handled by `alert_manager`

### 5. Event-Driven Processing
- Use channel subscriptions (push), not polling (pull)
- Process data when it arrives, not on schedule

---

## Related Documentation

- **Data Feeding**: `crawlers/data_feeding.md` - WebSocket, binary parsing, Redis publishing
- **Redis Operations**: `redis_files/redis_document.md` - Complete Redis client documentation
- **Pattern Detection**: `patterns/` - Pattern detection system
- **Alert Management**: `alerts/alert_manager.py` - Alert processing pipeline

---

**Remember**: This document provides high-level guidance. Always read the actual code and docstrings for implementation details, method signatures, and exact behavior.

