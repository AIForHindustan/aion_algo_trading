# Intraday Scanner

**Main scanner orchestrator for pattern detection and alert generation.**

---

## 📋 Overview

The `intraday_scanner/` directory contains the core scanner that:

1. **Consumes tick data** from Redis streams
2. **Calculates indicators** (RSI, MACD, Bollinger Bands, etc.)
3. **Detects patterns** using pattern detectors
4. **Generates alerts** via alert manager
5. **Orchestrates** the entire trading pipeline

---

## 🏗️ Architecture

```
Redis Stream (ticks:intraday:processed)
    ↓
data_pipeline.py
    ├─ Stream consumption
    ├─ Tick cleaning
    ├─ Indicator calculation
    └─ Volume ratio calculation
    ↓
scanner_main.py
    ├─ Pattern detection
    ├─ Alert generation
    └─ System orchestration
    ↓
alerts:stream (Redis DB1)
```

---

## 📁 Key Files

### `scanner_main.py`
**Purpose**: Main scanner orchestrator

**Key Responsibilities**:
- Initializes all components (DataPipeline, PatternDetector, AlertManager)
- Manages background threads (health monitor, stream processor)
- Routes pattern signals to alert manager
- Handles system lifecycle (start/stop)

**Key Classes**:
- `IntradayScanner` - Main scanner class

**Key Methods**:
- `start()` - Start scanner
- `stop()` - Stop scanner gracefully
- `_process_tick()` - Process individual tick
- `_handle_pattern_signal()` - Route pattern to alert manager

**Database**: Redis DB1 (reads ticks, writes alerts)

---

### `data_pipeline.py`
**Purpose**: Data processing pipeline

**Key Responsibilities**:
- Consumes `ticks:intraday:processed` stream
- Cleans and normalizes tick data
- Calculates indicators using `HybridCalculations`
- Calculates volume ratios using `AssetTypeAwareBaseline`
- Stores indicators in Redis DB1

**Key Classes**:
- `DataPipeline` - Main pipeline class

**Key Methods**:
- `process_tick()` - Process single tick
- `_clean_tick_data()` - Comprehensive tick cleaning
- `_calculate_derived_fields()` - Calculate price_change, etc.
- `_store_calculated_indicators()` - Store in Redis

**Storage**:
- Indicators: `ind:ta:{symbol}:{indicator}` (DB1)
- Greeks: `ind:greeks:{symbol}:{greek}` (DB1)
- Session: `session:{symbol}:{date}` (DB1)

---

### `calculations.py`
**Purpose**: Unified calculation engine

**Key Responsibilities**:
- Single source of truth for all calculations
- Technical indicators (RSI, MACD, Bollinger Bands)
- Greeks (delta, gamma, theta, vega, rho)
- Volume metrics

**Key Classes**:
- `HybridCalculations` - Unified calculation class

**Key Methods**:
- `calculate_rsi()` - RSI calculation
- `calculate_macd()` - MACD calculation
- `calculate_bollinger_bands()` - Bollinger Bands
- `calculate_greeks()` - Options Greeks

**Storage**: Delegates to `RedisStorageManager.store_indicators_atomic()`

---

### `calculations/redis_storage.py`
**Purpose**: Atomic indicator storage

**Key Responsibilities**:
- Atomic batch storage of indicators
- Canonical key generation
- TTL management

**Key Classes**:
- `RedisStorageManager` - Storage manager

**Key Methods**:
- `store_indicators_atomic()` - Batch store indicators
- `store_greeks_directly()` - Store Greeks

**Database**: Redis DB1

---

## 🔄 Data Flow

### Tick Processing Flow

```
1. Consume tick from stream
   └─ ticks:intraday:processed (DB1)
   ↓
2. Clean tick data
   ├─ Normalize fields
   ├─ Calculate derived fields (price_change, etc.)
   └─ Preserve net_change/change from Zerodha
   ↓
3. Calculate indicators
   ├─ Technical indicators (RSI, MACD, etc.)
   ├─ Greeks (if options)
   └─ Volume ratios (AssetTypeAwareBaseline)
   ↓
4. Store in Redis
   ├─ ind:ta:{symbol}:{indicator}
   ├─ ind:greeks:{symbol}:{greek}
   └─ session:{symbol}:{date}
   ↓
5. Pattern detection
   └─ pattern_detector.py
   ↓
6. Alert generation
   └─ alert_manager.send_alert()
```

---

## 🔌 Integration Points

### Input
- **Redis Stream**: `ticks:intraday:processed` (DB1)
- **Source**: `zerodha/crawlers/intraday_crawler.py`

### Output
- **Pattern Signals**: To `alerts/alert_manager.py`
- **Redis Stream**: `alerts:stream` (DB1)

### Dependencies
- `patterns/pattern_detector.py` - Pattern detection
- `alerts/alert_manager.py` - Alert generation
- `dynamic_thresholds/time_aware_volume_baseline.py` - Volume baselines
- `redis_files/redis_client.py` - Redis clients

---

## 🚀 Usage

### Start Scanner

```bash
# With config file
python intraday_trading/intraday_scanner/scanner_main.py --config configs/scanner.json

# With continuous mode (tailing ticks)
python intraday_trading/intraday_scanner/scanner_main.py --config configs/scanner.json --continuous
```

### Configuration

**File**: `configs/scanner.json`

```json
{
    "redis": {
        "host": "localhost",
        "port": 6379,
        "db": 1
    },
    "patterns": {
        "enabled": ["volume_spike", "breakout", "ict_killzone"]
    },
    "alerts": {
        "min_confidence": 0.70
    }
}
```

---

## 📊 Key Metrics

### Processing Performance
- **Tick Processing**: Real-time (per tick)
- **Indicator Calculation**: Batched (10 ticks)
- **Pattern Detection**: Real-time (per tick)
- **Alert Generation**: Real-time (per pattern signal)

### Storage
- **Indicators**: Canonical keys only (`ind:ta:{symbol}:{indicator}`)
- **TTL**: Variable (1h-7d based on indicator type)
- **Database**: Redis DB1 (trading data)

---

## ✅ Best Practices

1. **Use canonical keys only** (no symbol variants)
2. **Preserve `net_change`** from Zerodha
3. **Calculate volume ratios** using `AssetTypeAwareBaseline`
4. **Store indicators atomically** via `RedisStorageManager`
5. **Route patterns** to alert manager (no formatting in scanner)

---

## 🔍 Troubleshooting

### No Ticks Processing
- Check Redis stream: `ticks:intraday:processed`
- Verify crawler is publishing ticks
- Check Redis connection

### Missing Indicators
- Verify `HybridCalculations` is initialized
- Check indicator calculation errors in logs
- Verify Redis storage keys

### Alerts Not Generating
- Check pattern detection is enabled
- Verify confidence thresholds
- Check alert manager initialization

---

**Last Updated**: November 2025

