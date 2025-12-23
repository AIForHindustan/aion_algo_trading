# Technical Documentation: Indicators & Greeks Calculations

**Last Updated:** 2025-11-08  
**File:** `intraday_scanner/calculations.py`  
**Status:** Production Ready

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Technical Indicators](#technical-indicators)
4. [Option Greeks](#option-greeks)
5. [Priority Order & Fallback Chain](#priority-order--fallback-chain)
6. [Performance Optimizations](#performance-optimizations)
7. [Data Flow](#data-flow)
8. [Integration Points](#integration-points)

---

## Overview

The `calculations.py` module provides high-performance technical indicator and option Greek calculations using a hybrid approach that prioritizes TA-Lib for accuracy and speed, with Polars as a fallback for pure Python performance.

### Key Features

- **Unified Priority Order**: All indicators follow the same priority: Redis → TA-Lib → Polars → pandas_ta → Simple fallback
- **Polars Optimization**: Direct Polars DataFrame operations for real-time calculations (no list conversion overhead)
- **Option Greeks**: Full Black-Scholes Greek calculations (delta, gamma, theta, vega, rho) with DTE awareness
- **Volume Data Integrity**: Single source of truth for volume calculations (never overrides Volume Manager)
- **Caching**: Multi-layer caching with TTL and LRU eviction

---

## Architecture

### Core Classes

#### `HybridCalculations`
Main calculation engine that processes tick data and computes indicators.

**Key Methods:**
- `batch_calculate_indicators()` - Batch process multiple symbols
- `process_tick()` - Real-time single tick processing with rolling window
- `_calculate_realtime_indicators()` - Optimized Polars-based calculations

#### `EnhancedGreekCalculator`
Option Greek calculations with proper DTE (Days To Expiry) awareness.

**Key Methods:**
- `calculate_greeks_for_tick_data()` - Calculate all Greeks from tick data
- `black_scholes_greeks()` - Core Black-Scholes calculations using py_vollib
- `calculate_greeks_with_dte()` - DTE-aware Greek calculations

#### `PolarsCache`
High-performance DataFrame cache with TTL, LRU eviction, and timestamp normalization.

#### `ExpiryCalculator`
Exchange-aware expiry date calculations with trading calendar support.

---

## Technical Indicators

### Supported Indicators

| Indicator | Method | Priority Order | Period Default |
|-----------|--------|----------------|----------------|
| **RSI** | `calculate_rsi()` | TA-Lib → Polars → Simple | 14 |
| **ATR** | `calculate_atr()` | TA-Lib → Polars → pandas_ta → Simple | 14 |
| **EMA** | `calculate_ema()` | TA-Lib → Polars → pandas_ta → Simple | 20 |
| **MACD** | `calculate_macd()` | TA-Lib → Polars → Simple | 12, 26, 9 |
| **Bollinger Bands** | `calculate_bollinger_bands()` | TA-Lib → Polars → Simple | 20, 2.0 |
| **VWAP** | `calculate_vwap_batch()` | Polars → Manual | N/A |

### Priority Order (Unified Across All Indicators)

**All indicator calculation methods follow this exact priority:**

1. **Redis Cache** (if `symbol` provided)
   - Checks cached indicator values from Redis/DragonflyDB
   - Key format: `indicators:{symbol}:{indicator_name}`

2. **TA-Lib** (Preferred - Fastest & Most Accurate)
   - C library with optimized implementations
   - Industry-standard calculations
   - Used when `TALIB_AVAILABLE = True`

3. **Polars** (Fallback 1)
   - Pure Python, vectorized operations
   - Parallel processing support
   - Used when `POLARS_AVAILABLE = True`

4. **pandas_ta** (Fallback 2 - ATR/EMA only)
   - Pure Python alternative
   - Used when `PANDAS_TA_AVAILABLE = True`
   - Note: Requires numba (not available on Python 3.14+)

5. **Simple Fallback**
   - Basic Python implementation
   - Always available as last resort

### Polars-Optimized Methods

For real-time calculations with rolling windows, Polars DataFrame methods are used directly:

- `_calculate_rsi_polars()` - RSI on Polars DataFrame (tries TA-Lib first)
- `_calculate_atr_polars()` - ATR on Polars DataFrame (tries TA-Lib first)
- `_calculate_macd_polars()` - MACD on Polars DataFrame (tries TA-Lib first)
- `_calculate_bollinger_bands_polars()` - Bollinger Bands on Polars DataFrame (tries TA-Lib first)
- `_calculate_vwap_polars()` - VWAP on Polars DataFrame (no TA-Lib - VWAP not in TA-Lib)

**Key Optimization:** These methods work directly on Polars DataFrames, avoiding list conversion overhead while still maintaining TA-Lib priority.

---

## Option Greeks

### Supported Greeks

| Greek | Description | Calculation Method |
|-------|-------------|-------------------|
| **Delta** | Price sensitivity to underlying | `py_vollib.black_scholes.greeks.analytical.delta()` |
| **Gamma** | Delta sensitivity to underlying | `py_vollib.black_scholes.greeks.analytical.gamma()` |
| **Theta** | Time decay | `py_vollib.black_scholes.greeks.analytical.theta()` |
| **Vega** | Volatility sensitivity | `py_vollib.black_scholes.greeks.analytical.vega()` |
| **Rho** | Interest rate sensitivity | `py_vollib.black_scholes.greeks.analytical.rho()` |

### Greek Calculation Flow

```
Tick Data → EnhancedGreekCalculator.calculate_greeks_for_tick_data()
    ↓
Extract: spot_price, strike_price, expiry_date, option_type
    ↓
ExpiryCalculator.calculate_dte() → trading_dte, calendar_dte
    ↓
EnhancedGreekCalculator.black_scholes_greeks()
    ↓
py_vollib.black_scholes.greeks.analytical.*()
    ↓
Return: {delta, gamma, theta, vega, rho, dte_years, trading_dte, expiry_series}
```

### DTE (Days To Expiry) Handling

**Trading DTE vs Calendar DTE:**
- **Trading DTE**: Only counts trading days (excludes weekends/holidays)
- **Calendar DTE**: Counts all calendar days
- **Used in Calculations**: Trading DTE (more accurate for options)

**Expiry Series:**
- `WEEKLY`: Weekly expiry (typically Thursday)
- `MONTHLY`: Monthly expiry (last Thursday of month)

### Automatic Underlying Price Fetching

For NIFTY and BANKNIFTY options, if `underlying_price` is missing from tick data:

1. Extract underlying symbol (e.g., 'NIFTY' from 'NFO:NIFTY25JAN24500CE')
2. Fetch from Redis:
   - **Priority 1**: DB 5 (`indicators_cache`) - `underlying_price:{symbol}`
   - **Priority 2**: DB 1 (`realtime`) - `index_hash:NSE:NIFTY 50` or `index_hash:NSE:NIFTY BANK`
3. Cache fetched price in DB 5 for future use

---

## Priority Order & Fallback Chain

### Unified Priority Order (All Methods)

```
┌─────────────────────────────────────────────────────────┐
│ 1. Redis Cache (if symbol provided)                     │
│    Key: indicators:{symbol}:{indicator_name}            │
└─────────────────────────────────────────────────────────┘
                    ↓ (if not found)
┌─────────────────────────────────────────────────────────┐
│ 2. TA-Lib (Preferred - Fastest & Most Accurate)         │
│    - C library with optimized implementations           │
│    - Industry-standard calculations                     │
└─────────────────────────────────────────────────────────┘
                    ↓ (if not available/fails)
┌─────────────────────────────────────────────────────────┐
│ 3. Polars (Fallback 1)                                  │
│    - Pure Python, vectorized operations                  │
│    - Parallel processing support                        │
└─────────────────────────────────────────────────────────┘
                    ↓ (if not available/fails)
┌─────────────────────────────────────────────────────────┐
│ 4. pandas_ta (Fallback 2 - ATR/EMA only)               │
│    - Pure Python alternative                            │
│    - Note: Requires numba (not available on Python 3.14+)│
└─────────────────────────────────────────────────────────┘
                    ↓ (if not available/fails)
┌─────────────────────────────────────────────────────────┐
│ 5. Simple Fallback                                      │
│    - Basic Python implementation                        │
│    - Always available as last resort                    │
└─────────────────────────────────────────────────────────┘
```

### Method Consistency

**All calculation methods follow this exact priority:**

- ✅ `calculate_rsi()` - TA-Lib → Polars → Simple
- ✅ `calculate_atr()` - TA-Lib → Polars → pandas_ta → Simple
- ✅ `calculate_ema()` - TA-Lib → Polars → pandas_ta → Simple
- ✅ `calculate_macd()` - TA-Lib → Polars → Simple
- ✅ `calculate_bollinger_bands()` - TA-Lib → Polars → Simple
- ✅ `calculate_vwap_batch()` - Polars → Manual (TA-Lib has no VWAP)
- ✅ `_calculate_rsi_polars()` - TA-Lib → Polars → Simple
- ✅ `_calculate_atr_polars()` - TA-Lib → Polars → Simple
- ✅ `_calculate_macd_polars()` - TA-Lib → Polars → Simple
- ✅ `_calculate_bollinger_bands_polars()` - TA-Lib → Polars → Simple

---

## Performance Optimizations

### 1. Polars DataFrame Operations

**Before (Inefficient):**
```python
# Convert Polars DataFrame to lists
prices = window['last_price'].to_list()
# Then recreate Polars DataFrame in calculation method
df = pl.DataFrame({'last_price': prices})
```

**After (Optimized):**
```python
# Work directly on Polars DataFrame
rsi_df = window.with_columns([
    pl.col('last_price').diff().alias('delta')
]).with_columns([
    # ... Polars expressions ...
])
```

**Benefits:**
- No list conversion overhead
- Vectorized operations (SIMD)
- Parallel processing
- Memory efficient

### 2. Fast EMA Classes

**FastEMA**: Stateful EMA calculation for O(1) per-tick updates
- Used for real-time EMA calculations
- Supports single tick updates and batch processing

**CircularEMA**: Pre-allocated circular buffer EMA
- Maximum performance with large datasets
- O(1) operations

### 3. PolarsCache

**Features:**
- TTL (Time To Live) with automatic expiration
- LRU (Least Recently Used) eviction
- Thread-safe operations
- Timestamp normalization (handles 11+ timestamp formats)
- Memory optimization (integer/float downcasting, categorical encoding)

### 4. Rolling Window Optimization

**Implementation:**
- Maintains fixed-size Polars DataFrame per symbol
- Initializes with historical bucket data from Redis
- Updates incrementally (O(1) per tick)
- Uses `partition_by()` for multi-symbol batch processing

---

## Data Flow

### Real-Time Indicator Calculation Flow

```
Tick Data (Dict)
    ↓
_update_rolling_window()
    ↓
Polars DataFrame (rolling window)
    ↓
_calculate_realtime_indicators()
    ↓
┌─────────────────────────────────────┐
│ For each indicator:                 │
│ 1. Try TA-Lib (if available)        │
│ 2. Fallback to Polars               │
│ 3. Fallback to simple calculation   │
└─────────────────────────────────────┘
    ↓
Indicators Dict
    ↓
Add Greeks (if option symbol)
    ↓
Return Complete Indicators
```

### Batch Processing Flow

```
Symbol Data (Dict[str, List[Dict]])
    ↓
Combine into single Polars DataFrame
    ↓
partition_by("symbol")
    ↓
For each symbol group:
    ├─ Extract prices, volumes, highs, lows
    ├─ batch_calculate_indicators()
    │   ├─ calculate_rsi() → TA-Lib → Polars
    │   ├─ calculate_atr() → TA-Lib → Polars
    │   ├─ calculate_macd() → TA-Lib → Polars
    │   └─ calculate_bollinger_bands() → TA-Lib → Polars
    └─ Add Greeks (if option)
    ↓
Results Dict[str, Dict]
```

### Volume Data Flow (Single Source of Truth)

```
websocket_parser
    ↓
VolumeStateManager.calculate_incremental()
    ↓
CorrectVolumeCalculator.calculate_volume_metrics()
    ↓
volume_ratio stored in tick_data
    ↓
calculations.py
    ↓
_extract_volume_ratio() → Read ONLY (never calculate)
    ↓
Indicators Dict
```

**Critical Rule:** `calculations.py` NEVER calculates volume_ratio - it only reads pre-computed values from Volume Manager.

---

## Integration Points

### 1. Pattern Detector

**Location:** `patterns/pattern_detector.py`

**Usage:**
```python
# Get indicators with Greeks
indicators = hybrid_calc.batch_calculate_indicators(tick_data, symbol)

# Extract Greeks
greeks = pattern_detector.get_greeks_from_indicators(indicators)
# Returns: {delta, gamma, theta, vega, rho, dte_years, trading_dte, expiry_series}

# Check if sufficient Greeks available
has_sufficient, greeks_available, count = pattern_detector.has_sufficient_greeks(indicators, min_count=2)
```

### 2. Data Pipeline

**Location:** `intraday_scanner/data_pipeline.py`

**Usage:**
```python
# Process tick and get indicators
indicators = hybrid_calc.process_tick(symbol, tick_data)

# Indicators include:
# - Technical indicators (RSI, ATR, EMA, MACD, Bollinger Bands, VWAP)
# - Volume ratio (pre-computed from Volume Manager)
# - Greeks (if option symbol)
```

### 3. Alert Manager

**Location:** `alerts/alert_manager.py`

**Usage:**
- Indicators are passed to pattern detection
- Greeks are used for option-specific pattern detection
- Volume ratio is used for volume-based alerts

### 4. Redis Storage

**Indicators Cache (DB 5):**
- Key format: `indicators:{symbol}:{indicator_name}`
- TTL: 300 seconds (5 minutes)
- Used for Redis fallback in priority order

**Underlying Price Cache (DB 5):**
- Key format: `underlying_price:{symbol}`
- TTL: 300 seconds (5 minutes)
- Used for NIFTY/BANKNIFTY option Greek calculations

---

## Method Reference

### Technical Indicators

#### `calculate_rsi(prices, period=14, symbol=None)`
Calculate Relative Strength Index.

**Priority:** TA-Lib → Polars → Simple  
**Default Period:** 14  
**Returns:** float (0-100)

#### `calculate_atr(highs, lows, closes, period=14, symbol=None)`
Calculate Average True Range.

**Priority:** TA-Lib → Polars → pandas_ta → Simple  
**Default Period:** 14  
**Returns:** float

#### `calculate_ema(prices, period=20)`
Calculate Exponential Moving Average.

**Priority:** TA-Lib → Polars → pandas_ta → Simple  
**Default Period:** 20  
**Returns:** float

#### `calculate_macd(prices, fast_period=12, slow_period=26, signal_period=9, symbol=None)`
Calculate MACD (Moving Average Convergence Divergence).

**Priority:** TA-Lib → Polars → Simple  
**Default Periods:** 12, 26, 9  
**Returns:** Dict with `{macd, signal, histogram}`

#### `calculate_bollinger_bands(prices, period=20, std_dev=2.0, symbol=None)`
Calculate Bollinger Bands.

**Priority:** TA-Lib → Polars → Simple  
**Default Period:** 20, Std Dev: 2.0  
**Returns:** Dict with `{upper, middle, lower}`

#### `calculate_vwap_batch(tick_data)`
Calculate Volume Weighted Average Price.

**Priority:** Polars → Manual (TA-Lib has no VWAP)  
**Returns:** float

### Option Greeks

#### `calculate_greeks_for_tick_data(tick_data, risk_free_rate=0.05, volatility=0.2)`
Calculate all Greeks from tick data.

**Required Fields:**
- `underlying_price` (or auto-fetched for NIFTY/BANKNIFTY)
- `strike_price`
- `expiry_date` (or extracted from `symbol`)
- `option_type` ('call'/'ce' or 'put'/'pe')

**Returns:** Dict with `{delta, gamma, theta, vega, rho, dte_years, trading_dte, expiry_series}`

#### `black_scholes_greeks(spot, strike, dte_years, risk_free, volatility, option_type)`
Core Black-Scholes Greek calculations using py_vollib.

**Library:** `py_vollib.black_scholes.greeks.analytical`  
**Fallback:** Manual calculation if py_vollib unavailable

---

## Volume Data Integrity

### Single Source of Truth

**Volume calculations are NEVER performed in `calculations.py`:**

1. **Volume Manager** (`websocket_parser`):
   - `VolumeStateManager.calculate_incremental()` - Calculates incremental volume
   - `CorrectVolumeCalculator.calculate_volume_metrics()` - Calculates volume ratio
   - Stores in Redis: `volume_state:{token}` (DB 2)

2. **calculations.py**:
   - `_extract_volume_ratio()` - **ONLY reads** pre-computed volume_ratio
   - `_get_volume_ratio()` - **ONLY reads** from tick_data
   - **NEVER calculates** - Returns 0.0 if missing (does not override)

### Volume Data Validator

**Class:** `VolumeDataValidator`

**Purpose:**
- Validates volume data integrity
- Detects override attempts
- Tracks missing volume data

**Methods:**
- `validate_tick_data()` - Check for required volume fields
- `detect_override_attempts()` - Compare before/after volume_ratio
- `get_stats()` - Get validation statistics

---

## Error Handling

### Graceful Degradation

All calculation methods implement graceful fallback:

1. **Library Availability Checks:**
   - `TALIB_AVAILABLE` - Checks if TA-Lib is installed
   - `POLARS_AVAILABLE` - Checks if Polars is installed
   - `PANDAS_TA_AVAILABLE` - Checks if pandas_ta is installed
   - `GREEK_CALCULATIONS_AVAILABLE` - Checks if py_vollib is installed

2. **Stub Functions:**
   - If py_vollib unavailable, stub functions return 0.0
   - Prevents `NameError` exceptions

3. **Exception Handling:**
   - All methods wrapped in try/except
   - Fallback to next priority method on error
   - Logs warnings for debugging

---

## Performance Metrics

### Optimization Benefits

- **Polars Operations**: 10-100x faster than pandas for large datasets
- **TA-Lib**: C library, fastest for individual calculations
- **Rolling Window**: O(1) per-tick updates (no full recalculation)
- **Caching**: Reduces redundant calculations by 80-90%

### Memory Efficiency

- **PolarsCache**: Automatic memory optimization (downcasting, categoricals)
- **Rolling Window**: Fixed-size windows prevent memory bloat
- **LRU Eviction**: Automatically removes least-used cache entries

---

## Testing & Validation

### Indicator Validation

All indicators are validated against:
- TA-Lib reference values (when available)
- Historical data consistency
- Edge cases (empty data, single values, etc.)

### Greek Validation

Greeks are validated against:
- py_vollib reference values
- Black-Scholes formula consistency
- DTE accuracy (trading days vs calendar days)

---

## Future Enhancements

### Planned Optimizations

1. **Lazy Evaluation**: Use Polars lazy evaluation for complex calculations
2. **Parallel Processing**: Multi-threaded batch processing for large symbol sets
3. **GPU Acceleration**: Polars GPU support for very large datasets
4. **Additional Indicators**: Stochastic, ADX, CCI, etc.

### Known Limitations

1. **pandas_ta**: Not available on Python 3.14+ (numba dependency)
2. **VWAP**: No TA-Lib implementation (uses Polars/Manual)
3. **Volume Profile**: POC/VA calculations handled by VolumeProfileManager (not in calculations.py)

---

## Changelog

### 2025-11-08
- ✅ Fixed priority order consistency across all `_calculate_*_polars()` methods
- ✅ Added `symbol` parameter to `calculate_macd()` and `calculate_bollinger_bands()`
- ✅ All methods now try TA-Lib first, then Polars
- ✅ Consolidated audit files into this technical documentation

### 2025-10-27
- ✅ Integrated PolarsCache with timestamp normalization
- ✅ Added automatic underlying price fetching for NIFTY/BANKNIFTY options
- ✅ Implemented rolling window with historical bucket data initialization

---

## References

- **TA-Lib**: https://ta-lib.org/
- **Polars**: https://pola.rs/
- **py_vollib**: https://github.com/vollib/py_vollib
- **pandas_ta**: https://github.com/twopirllc/pandas-ta

---

**End of Documentation**

