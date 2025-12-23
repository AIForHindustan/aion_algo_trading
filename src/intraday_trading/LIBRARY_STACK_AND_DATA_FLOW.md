# Current Library Stack and Data Flow Documentation

**Generated:** November 7, 2025  
**System:** Intraday Trading Scanner & Alert System

---

## 1. 📚 Current Library Inventory

### **Data Processing Libraries**

| Library | Version | Purpose | Status |
|---------|---------|---------|--------|
| **pandas** | Latest | Data manipulation and analysis | ✅ Primary |
| **numpy** | 1.24.3 | Numerical computing, mathematical operations | ✅ Core |
| **polars** | 0.19.0 | High-performance DataFrame operations (faster than pandas) | ✅ Preferred for batch |
| **duckdb** | Latest | In-memory analytical database | ✅ Used |
| **pyarrow** | 12.0.1 | Apache Arrow for efficient data interchange | ✅ Active |
| **orjson** | 3.9.0 | Fast JSON parsing (faster than standard json) | ✅ Active |

### **Technical Indicators Libraries**

| Library | Version | Purpose | Status |
|---------|---------|---------|--------|
| **ta-lib** | Latest | Primary technical analysis (150+ indicators) | ✅ Primary (fallback) |
| **pandas_ta** | Latest | Pure Python alternative to TA-Lib | ✅ Preferred |
| **numba** | 0.58.0 | JIT compilation for ultra-fast calculations | ✅ Active (EMA acceleration) |
| **llvmlite** | 0.41.0 | LLVM backend for Numba | ✅ Active |

**Indicators Supported:**
- **Trend:** EMA, SMA, MACD, ADX, Aroon, Parabolic SAR
- **Momentum:** RSI, Stochastic, Williams %R, CCI, ROC
- **Volatility:** Bollinger Bands, ATR, Keltner Channels, Donchian Channels
- **Volume:** OBV, AD Line, Chaikin Money Flow, Money Flow Index
- **Pattern Recognition:** Candlestick patterns, Doji, Hammer, Engulfing patterns
- **Custom:** VWAP, Volume Profile, Support/Resistance levels

### **Options Pricing & Greeks**

| Library | Version | Purpose | Status |
|---------|---------|---------|--------|
| **py_vollib** | 1.0.1+ | Options pricing and Greeks (Black-Scholes, Binomial) | ✅ Primary |
| **py_lets_be_rational** | Latest | Advanced options pricing (Implied Volatility) | ✅ Available |
| **scipy.stats** | 1.10.0+ | Statistical distributions (norm for Black-Scholes) | ✅ Active |
| **pandas_market_calendars** | 4.3.3+ | Trading calendar for DTE calculations | ✅ Active |

**Greeks Calculated:**
- Delta, Gamma, Theta, Vega, Rho
- Black-Scholes analytical Greeks
- Implied Volatility calculations

### **Stream Processing**

| Library | Version | Purpose | Status |
|---------|---------|---------|--------|
| **redis** | 5.0.0 | Redis client for data storage and streaming | ✅ Primary |
| **websocket-client** | 1.6.0 | WebSocket client for real-time data | ✅ Active |
| **asyncio** | Built-in | Asynchronous I/O for concurrent processing | ✅ Active |

**Stream Infrastructure:**
- **Redis Streams:** Primary streaming mechanism
- **Consumer Groups:** For parallel processing
- **DragonflyDB:** High-performance alternative (migration in progress)

### **Alert/Notification System**

| Library | Version | Purpose | Status |
|---------|---------|---------|--------|
| **requests** | Latest | HTTP requests for Telegram API | ✅ Active |
| **redis** | 5.0.0 | Redis pub/sub for alert distribution | ✅ Active |
| **threading** | Built-in | Multi-threaded notification delivery | ✅ Active |

**Notification Channels:**
- **Telegram:** Primary alert delivery (multiple bots/channels)
- **Redis Pub/Sub:** Internal alert distribution
- **macOS Notifications:** Local system notifications

### **Data Storage/Retrieval**

| Library | Version | Purpose | Status |
|---------|---------|---------|--------|
| **redis** | 5.0.0 | Primary data store (multi-database architecture) | ✅ Primary |
| **DragonflyDB** | Latest | High-performance alternative (migration in progress) | ✅ Testing |
| **pyarrow** | 12.0.1 | Parquet file format support | ✅ Active |
| **duckdb** | Latest | SQL queries on parquet files | ✅ Active |

**Storage Architecture:**
- **Redis DB 0:** System/metadata
- **Redis DB 1:** Realtime streams (ticks, alerts, patterns)
- **Redis DB 2:** Analytics (volume profiles, OHLC, metrics)
- **Redis DB 3:** Independent Validator (signal quality)
- **Redis DB 5:** Indicators Cache (technical indicators, Greeks)

---

## 2. 🔄 Current Data Flow

### **Complete Pipeline Structure**

```
WebSocket (Zerodha) 
    ↓
Crawlers (intraday_crawler.py)
    ↓
Binary Parser (ProductionZerodhaBinaryConverter)
    ↓
Redis Streams (ticks:raw:binary, ticks:intraday:processed)
    ↓
Data Pipeline (data_pipeline.py)
    ├─ Deduplication (DedupeManager)
    ├─ Batch Processing (batch_size: 10 ticks)
    └─ Indicator Calculation (HybridCalculations)
    ↓
Redis Storage (DB 5: Indicators Cache)
    ↓
Pattern Detection (pattern_detector.py)
    ├─ Redis Pipelining (batch reads)
    ├─ Volume Profile Integration
    └─ Pattern Matching (20+ patterns)
    ↓
Alert Manager (alert_manager.py)
    ├─ Enhanced Validation (6-path qualification)
    ├─ Conflict Resolution
    └─ Pre-validation (multi-layer)
    ↓
Retail Alert Filter (filters.py)
    ├─ Profitability Checks
    ├─ Cooldown Management
    └─ VIX Regime Filtering
    ↓
Notifiers (notifiers.py)
    ├─ TelegramNotifier
    ├─ RedisNotifier
    └─ MacOSNotifier
    ↓
Alert Validator (alert_validator.py)
    ├─ Real-time Validation
    ├─ Performance Tracking
    └─ Forward Validation
```

### **Processing Details**

#### **Tick Processing Mode**
- **Primary:** Individual tick processing (real-time)
- **Secondary:** Batch processing for indicators (batch_size: 10 ticks)
- **Archive:** Batch processing for historical data (batch_size: 1000 ticks)

#### **Batch Sizes & Time Windows**

| Component | Batch Size | Time Window | Processing Mode |
|-----------|------------|-------------|-----------------|
| **Data Pipeline** | 10 ticks | Real-time | Continuous |
| **Indicator Calculation** | 174 symbols max | Per tick | Real-time |
| **Pattern Detection** | Pipeline reads | Per tick | Real-time |
| **Historical Archive** | 1000 ticks | 5-10 min | Scheduled |
| **DragonflyDB Streaming** | 1000 ticks | Max speed | Batch |

#### **Calculation Timing**

| Component | Timing | Location |
|-----------|--------|----------|
| **Indicator Calculation** | Real-time (per tick) | `HybridCalculations` |
| **Pattern Detection** | Real-time (per tick) | `PatternDetector` |
| **Volume Profile** | Every 100 ticks or 1 min | `VolumeProfileManager` |
| **Historical Analysis** | Scheduled (5-10 min) | Background threads |
| **Alert Validation** | Real-time (per alert) | `AlertValidator` |

---

## 3. 📊 Current Performance Metrics

### **Tick Throughput**

| Metric | Value | Notes |
|--------|-------|-------|
| **Current Throughput** | ~50-100 ticks/second | Varies by market activity |
| **Peak Throughput** | ~500-1000 ticks/second | During high-volume periods |
| **DragonflyDB Target** | 836 TPS (TA-Lib workflow) | 1.7x improvement over Redis |
| **Stream Processing** | 1000 ticks/batch | Max speed mode |

### **Latency Metrics**

| Operation | Current Latency | Target (DragonflyDB) | Improvement |
|-----------|----------------|---------------------|-------------|
| **Tick Receipt → Indicator** | ~10-50ms | ~5-25ms | 2x faster |
| **Indicator → Pattern Detection** | ~5-20ms | ~3-12ms | 1.7x faster |
| **Pattern → Alert Generation** | ~2-10ms | ~1-5ms | 2x faster |
| **Alert → Notification** | ~50-200ms | ~25-100ms | 2x faster |
| **End-to-End (Tick → Alert)** | ~67-280ms | ~34-142ms | 2x faster |

**Note:** Latency varies based on:
- Market activity (more symbols = higher latency)
- Redis connection pool availability
- Pattern detection complexity
- Alert validation depth

### **Resource-Intensive Components**

| Component | Resource Usage | Bottleneck |
|-----------|----------------|------------|
| **Indicator Calculation** | High CPU | TA-Lib calculations, Numba JIT |
| **Pattern Detection** | Medium CPU | Redis pipelining, pattern matching |
| **Volume Profile** | Medium Memory | Price-volume distribution storage |
| **Alert Validation** | Low CPU | Forward validation queries |
| **Redis I/O** | High I/O | Stream reads/writes, hash operations |

### **Current Infrastructure**

| Component | Details |
|-----------|---------|
| **Primary Machine** | macOS (Apple Silicon M4) |
| **Location** | `/Users/lokeshgupta/Projects/aion_algo_trading/intraday_trading` |
| **Python Version** | Python 3.13 |
| **Virtual Environment** | `.venv` (activated) |
| **Redis** | Local (127.0.0.1:6379) - Legacy |
| **DragonflyDB** | Ubuntu VM (192.168.64.2:6379) - Testing |
| **Network** | UTM network (192.168.64.1/2) |
| **SSH Tunnels** | Mac→VM (port 6380), VM→Mac (port 6381) |

**Infrastructure Notes:**
- **Local Development:** macOS with local Redis
- **Testing:** Ubuntu VM with DragonflyDB (migration in progress)
- **Network:** UTM virtual network for VM communication
- **Deployment Options:** Railway.app, Render.com, Fly.io, or Cloudflare Tunnel

---

## 4. 🔍 Performance Bottlenecks & Optimization Opportunities

### **Current Bottlenecks**

1. **Redis I/O Operations**
   - **Issue:** High latency on hash operations (HGETALL, HSET)
   - **Impact:** Pattern detection waits for Redis responses
   - **Solution:** DragonflyDB migration (2-8x faster)

2. **Indicator Calculation**
   - **Issue:** TA-Lib calculations on every tick
   - **Impact:** CPU-intensive, especially for many symbols
   - **Solution:** Numba JIT acceleration (already implemented)

3. **Pattern Detection Pipeline**
   - **Issue:** Multiple Redis round-trips per pattern check
   - **Impact:** Latency accumulation
   - **Solution:** Redis pipelining (already implemented)

4. **Volume Profile Updates**
   - **Issue:** Memory growth with price-volume distribution
   - **Impact:** Memory usage increases over time
   - **Solution:** Periodic cleanup (already implemented)

### **Optimization Status**

| Optimization | Status | Impact |
|--------------|--------|--------|
| **Numba JIT (EMA)** | ✅ Implemented | O(1) single tick updates |
| **Redis Pipelining** | ✅ Implemented | Reduced round-trips |
| **Batch Processing** | ✅ Implemented | 10 ticks/batch |
| **DragonflyDB Migration** | 🔄 In Progress | 2-8x performance gain |
| **Connection Pooling** | ✅ Implemented | RedisManager82 |
| **Deduplication** | ✅ Implemented | Prevents duplicate processing |

---

## 5. 📈 Performance Targets & Goals

### **Throughput Goals**

- **Current:** 50-100 ticks/second
- **Target:** 200-500 ticks/second (with DragonflyDB)
- **Peak Target:** 1000+ ticks/second (optimized)

### **Latency Goals**

- **Current End-to-End:** 67-280ms
- **Target End-to-End:** <100ms (with DragonflyDB)
- **Target Tick → Alert:** <50ms (optimized)

### **Resource Goals**

- **CPU Usage:** <70% average (currently ~67% during peak)
- **Memory Usage:** <2GB (currently ~1.3GB)
- **Redis Connections:** <100 (currently ~52)

---

## 6. 🔧 Configuration Files

### **Key Configuration Files**

- `requirements_apple_silicon.txt` - Python dependencies
- `redis_files/redis_config.py` - Redis database configuration
- `intraday_scanner/data_pipeline.py` - Batch size configuration
- `patterns/pattern_detector.py` - Pattern detection settings
- `alerts/config/telegram_config.json` - Telegram notification config

### **Environment Variables**

```bash
# Redis Configuration
REDIS_HOST=127.0.0.1
REDIS_PORT=6379

# DragonflyDB (via tunnel)
DRAGONFLY_HOST=127.0.0.1
DRAGONFLY_PORT=6380

# Processing Configuration
BATCH_SIZE=10
MAX_BATCH_SIZE=174
ARCHIVE_BATCH_SIZE=1000
```


#
**Document Version:** 1.0  
**Last Updated:** November 7, 2025

