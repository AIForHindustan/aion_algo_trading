# Intraday Trading System

**Core trading system: pattern detection, data processing, and forward-testing validation.**

---

## 📋 Overview

The `intraday_trading/` directory contains the core trading system that:

1. **Processes tick data** from Redis streams (via Optimized Stream Processor - 5 workers)
2. **Calculates indicators** (RSI, MACD, Bollinger Bands, Greeks)
3. **Detects trading patterns** (8 core patterns)
4. **Validates alerts** through forward-testing
5. **Tracks performance** across multiple time windows

---

## 🏗️ System Architecture

```
Redis Stream (ticks:intraday:processed)
    ↓
intraday_scanner/optimized_stream_processor.py (Optional)
    ├─ 5 workers, batch processing (50 ticks/batch)
    └─ High-performance async processing
    ↓
intraday_scanner/data_pipeline.py
    ├─ Stream consumption
    ├─ Tick cleaning
    ├─ Indicator calculation
    └─ Volume ratio calculation
    ↓
intraday_scanner/scanner_main.py
    ├─ Pattern detection orchestration
    └─ Alert routing
    ↓
patterns/pattern_detector.py
    ├─ 8 Core Pattern Detection
    │   ├─ Advanced Patterns (5)
    │   ├─ Strategies (2)
    │   └─ Game Theory (1)
    ↓
alerts/alert_manager.py
    └─ Alert generation
    ↓
alert_validation/alert_validator.py
    ├─ Forward-testing validation
    └─ Performance tracking
```

---

## 📁 Directory Structure

### `intraday_scanner/`
**Purpose**: Main scanner orchestrator

**Key Files**:
- `scanner_main.py` - Main scanner orchestrator
- `data_pipeline.py` - Data processing pipeline
- `calculations.py` - Unified calculation engine
- `calculations/redis_storage.py` - Atomic indicator storage

**See**: [intraday_scanner/README.md](intraday_scanner/README.md)

---

### `patterns/`
**Purpose**: Pattern detection logic

**Key Files**:
- `pattern_detector.py` - Main pattern detection orchestrator
- `base_detector.py` - Base detector class
- `volume_thresholds.py` - Volume-based thresholds
- `delta_analyzer.py` - Options delta analysis
- `kow_signal_straddle.py` - Kow straddle strategy
- `ict/` - ICT patterns (killzone, FVG, iron condor)

**See**: [patterns/README.md](patterns/README.md)

---

### `alert_validation/`
**Purpose**: Forward-testing validation system

**Key Files**:
- `alert_validator.py` - Main validator (independent auditor)
- `performance_tracker.py` - Performance tracking
- `alert_storage_db2.py` - Alert storage in DB2
- Reporting scripts (fetch, publish, send reports)

**See**: [alert_validation/README.md](alert_validation/README.md)

---

### `config/`
**Purpose**: System configuration

**Key Files**:
- `thresholds.py` - Pattern thresholds and confidence settings
- `schemas.py` - Data normalization and schemas

---

### `utils/`
**Purpose**: Utility functions

**Key Files**:
- `vix_utils.py` - VIX regime classification (single source of truth)
- `update_all_20day_averages.py` - Baseline data seeding

---

## 🔄 Data Flow

### Complete Pipeline

```
1. Data Ingestion (zerodha/crawlers/)
   └─ WebSocket → Redis stream
   ↓
2. Data Processing (intraday_scanner/data_pipeline.py)
   ├─ Stream consumption
   ├─ Indicator calculation
   └─ Volume ratio calculation
   ↓
3. Pattern Detection (patterns/pattern_detector.py)
   ├─ Volume patterns
   ├─ Breakout/reversal patterns
   ├─ ICT patterns
   └─ Options patterns
   ↓
4. Alert Generation (alerts/alert_manager.py)
   ├─ Validation
   ├─ Filtering
   └─ Notification
   ↓
5. Forward-Testing (alert_validation/alert_validator.py)
   ├─ Time-window validation
   ├─ MFE/MAE calculation
   └─ Performance tracking
```

---

## 🚀 Quick Start

### Start Scanner

```bash
python intraday_trading/intraday_scanner/scanner_main.py --config configs/scanner.json
```

### Start Validator

```bash
python -m intraday_trading.alert_validation.alert_validator
```

### Update Baselines

```bash
python intraday_trading/utils/update_all_20day_averages.py
```

---

## 📊 Key Components

### Data Pipeline
- **File**: `intraday_scanner/data_pipeline.py`
- **Purpose**: Processes ticks, calculates indicators
- **Storage**: Redis DB1 (indicators, Greeks, session data)

### Pattern Detector
- **File**: `patterns/pattern_detector.py`
- **Purpose**: Detects 20+ trading patterns
- **Output**: Pattern signals with confidence scores

### Alert Validator
- **File**: `alert_validation/alert_validator.py`
- **Purpose**: Forward-testing validation
- **Storage**: Redis DB2 (analytics, performance metrics)

---

## 🔌 Integration Points

### Input
- **Redis Stream**: `ticks:intraday:processed` (DB1)
- **Source**: `zerodha/crawlers/intraday_crawler.py`

### Output
- **Pattern Signals**: To `alerts/alert_manager.py`
- **Analytics**: Redis DB2

### Dependencies
- `alerts/` - Alert generation
- `zerodha/` - Data ingestion
- `redis_files/` - Redis infrastructure
- `dynamic_thresholds/` - Adaptive thresholds

---

## ✅ Best Practices

1. **Use canonical keys only** (no symbol variants)
2. **Calculate volume ratios** using `AssetTypeAwareBaseline`
3. **Apply VIX regime adjustments** to confidence
4. **Generate `alert_id`** for all alerts
5. **Store analytics in DB2** (not DB1)

---

**Last Updated**: November 2025

## 🏛️ **SINGLE SOURCES OF TRUTH**

**⚠️ CRITICAL: All code MUST use these centralized implementations. Never create duplicate implementations.**

### **🔌 Redis Client Management**

**Source of Truth**: `redis_files/redis_client.py` - `RedisClientFactory` class

**Method Signatures**:
```python
class RedisClientFactory:
    @staticmethod
    def get_trading_client(process_name: str = "trading") -> redis.Redis:
        """Returns DB1 client for trading operations (live data)."""
    
    @staticmethod
    def get_analytics_client(process_name: str = "analytics") -> redis.Redis:
        """Returns DB2 client for analytics operations."""
    
    @staticmethod
    def get_client(db: int = 1, process_name: str = "default", **kwargs) -> redis.Redis:
        """Get Redis client for specified database (DB1=live, DB2=analytics)."""
```

**Usage**:
```python
from redis.clients.factory import RedisClientFactory

# Trading operations (DB1)
trading_client = RedisClientFactory.get_trading_client()

# Analytics operations (DB2)
analytics_client = RedisClientFactory.get_analytics_client()
```

**Verification**: `rg 'RedisClientFactory|get_trading_client|get_analytics_client'`

---

### **🔤 Symbol Parsing & Normalization**

**Source of Truth**: `redis_files/redis_key_standards.py` - `UniversalSymbolParser` class

**Method Signatures**:
```python
def get_symbol_parser() -> UniversalSymbolParser:
    """Returns singleton UniversalSymbolParser instance."""

class UniversalSymbolParser:
    def parse_symbol(self, symbol: str) -> ParsedSymbol:
        """Parse symbol into components (base, exchange, expiry, strike, option_type)."""
    
    def normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol to canonical format."""
```

**Canonical Symbol**:
```python
class RedisKeyStandards:
    @staticmethod
    def canonical_symbol(symbol: str) -> str:
        """Returns canonical symbol format (uses UniversalSymbolParser internally)."""
```

**Usage**:
```python
from redis_files.redis_key_standards import get_symbol_parser, RedisKeyStandards

# Parse symbol
parser = get_symbol_parser()
parsed = parser.parse_symbol("BANKNIFTY25NOV57900CE")

# Get canonical symbol
canonical = RedisKeyStandards.canonical_symbol("BANKNIFTY25NOV57900CE")
# Returns: "NFOBANKNIFTY25NOV57900CE"
```

**Verification**: `rg 'UniversalSymbolParser|get_symbol_parser|canonical_symbol'`

---

### **🔑 Redis Key Generation**

**Source of Truth**: `redis_files/redis_key_standards.py` - `DatabaseAwareKeyBuilder` class

**Method Signatures**:
```python
def get_key_builder() -> DatabaseAwareKeyBuilder:
    """Returns singleton DatabaseAwareKeyBuilder instance."""

class DatabaseAwareKeyBuilder:
    def live_indicator(self, symbol: str, indicator_name: str, category: str = "ta") -> str:
        """Generate indicator key: ind:{category}:{symbol}:{indicator_name} (DB1)."""
    
    def live_greeks(self, symbol: str, greek_name: str) -> str:
        """Generate Greeks key: ind:greeks:{symbol}:{greek_name} (DB1)."""
    
    def live_ticks_stream(self, symbol: str) -> str:
        """Generate tick stream key: ticks:stream:{symbol} (DB1)."""
    
    def live_ticks_hash(self, symbol: str) -> str:
        """Generate tick hash key: ticks:{symbol} (DB1)."""
    
    def analytics_pattern_performance(self, symbol: str, pattern_type: str) -> str:
        """Generate pattern performance key: pattern_performance:{symbol}:{pattern_type} (DB2)."""
    
    @staticmethod
    def get_database(key: str) -> RedisDatabase:
        """Determine which database a key belongs to based on prefix."""
```

**Usage**:
```python
from redis_files.redis_key_standards import get_key_builder, RedisKeyStandards

builder = get_key_builder()
canonical = RedisKeyStandards.canonical_symbol("BANKNIFTY25NOV57900CE")

# Generate keys
rsi_key = builder.live_indicator(canonical, "rsi", "ta")
# Returns: "ind:ta:NFOBANKNIFTY25NOV57900CE:rsi"

delta_key = builder.live_greeks(canonical, "delta")
# Returns: "ind:greeks:NFOBANKNIFTY25NOV57900CE:delta"
```

**Verification**: `rg 'DatabaseAwareKeyBuilder|get_key_builder|live_indicator|live_greeks'`

---

### **💾 Indicator Storage**

**Source of Truth**: `intraday_scanner/calculations/redis_storage.py` - `RedisStorageManager` class

**Method Signatures**:
```python
class RedisStorageManager:
    def __init__(self, redis_client=None):
        """Initialize with Redis client (uses RedisClientFactory if None)."""
    
    def store_indicators_atomic(self, symbol: str, indicators: dict) -> None:
        """
        Store indicators atomically to Redis DB1.
        
        Args:
            symbol: Trading symbol (will be canonicalized)
            indicators: Dict of indicator_name -> value
        
        Features:
            - Canonical-only storage (single key per indicator)
            - Automatic TTL (300s for TA, 3600s for Greeks)
            - Category-based key generation
        """
    
    def store_greeks_directly(self, symbol: str, greeks: dict, ttl: int = 3600) -> None:
        """Store Greeks directly to Redis DB1."""
```

**Usage**:
```python
from intraday_scanner.calculations.redis_storage import RedisStorageManager
from redis.clients.factory import RedisClientFactory

# Get client
client = RedisClientFactory.get_trading_client()

# Store indicators
storage = RedisStorageManager(redis_client=client)
indicators = {"rsi": 65.5, "macd": 12.3}
storage.store_indicators_atomic("BANKNIFTY25NOV57900CE", indicators)
```

**Verification**: `rg 'RedisStorageManager|store_indicators_atomic'`

---

### **📊 Technical Indicator Calculations**

**Source of Truth**: `intraday_scanner/calculations.py` - `HybridCalculations` class

**Method Signatures**:
```python
class HybridCalculations:
    def __init__(self, redis_client=None):
        """Initialize with Redis client for caching."""
    
    def batch_calculate_indicators(self, tick_data_list: List[Dict], symbol: str = None) -> Dict[str, Any]:
        """Calculate all indicators for tick data (RSI, MACD, ATR, EMA, Bollinger, VWAP, Greeks)."""
    
    def calculate_rsi(self, prices: List[float], period: int = 14, symbol: str = None) -> float:
        """Calculate RSI (Priority: Redis → TA-Lib → Polars → pandas_ta → Simple)."""
    
    def calculate_macd(self, prices: List[float], fast_period: int = 12, 
                      slow_period: int = 26, signal_period: int = 9, symbol: str = None) -> Dict:
        """Calculate MACD (Priority: Redis → TA-Lib → Polars → pandas_ta → Simple)."""
    
    def calculate_atr(self, high: List[float], low: List[float], close: List[float], 
                     period: int = 14, symbol: str = None) -> float:
        """Calculate ATR (Priority: Redis → TA-Lib → Polars → pandas_ta → Simple)."""
    
    def calculate_greeks(self, symbol: str, underlying_price: float, strike: float, 
                        expiry_date: datetime, option_type: str, iv: float = None) -> Dict:
        """Calculate option Greeks (delta, gamma, theta, vega, rho, IV)."""
```

**Usage**:
```python
from intraday_scanner.calculations import HybridCalculations
from redis.clients.factory import RedisClientFactory

# Initialize
client = RedisClientFactory.get_trading_client()
calc = HybridCalculations(redis_client=client)

# Calculate indicators
indicators = calc.batch_calculate_indicators([tick_data], symbol="BANKNIFTY25NOV57900CE")
```

**Verification**: `rg 'HybridCalculations|batch_calculate_indicators|calculate_rsi|calculate_macd'`

---

### **📅 Market Calendar & Expiry**

**Source of Truth**: `shared_core/market_calendar.py` - `MarketCalendar` class

**Method Signatures**:
```python
def get_cached_calendar(exchange: str = "NSE") -> MarketCalendar:
    """Returns cached MarketCalendar instance for exchange."""

class MarketCalendar:
    def is_trading_day(self, date: datetime) -> bool:
        """Check if date is a trading day (excludes holidays)."""
    
    def get_next_weekly_expiry(self, date: datetime = None) -> datetime:
        """Get next weekly expiry date for NIFTY/BANKNIFTY."""
    
    def get_next_monthly_expiry(self, date: datetime = None) -> datetime:
        """Get next monthly expiry date."""
    
    def calculate_trading_dte(self, expiry_date: datetime, current_date: datetime = None) -> int:
        """Calculate trading days to expiry (excludes holidays)."""
```

**Usage**:
```python
from shared_core.market_calendar import get_cached_calendar

calendar = get_cached_calendar("NSE")
is_trading = calendar.is_trading_day(datetime.now())
dte = calendar.calculate_trading_dte(expiry_date)
```

**Verification**: `rg 'MarketCalendar|get_cached_calendar|is_trading_day|calculate_trading_dte'`

---

### **🔢 Volume Management**

**Source of Truth**: `shared_core/volume_files/volume_computation_manager.py` - `VolumeComputationManager` class

**Method Signatures**:
```python
class VolumeManager:
    def __init__(self, redis_client: redis.Redis = None, token_resolver=None):
        """Initialize with Redis client (uses RedisClientFactory if None)."""
    
    def calculate_incremental(self, instrument_token: str, current_cumulative: int, 
                             exchange_timestamp: datetime, symbol: Optional[str] = None) -> int:
        """Calculate incremental volume from cumulative volume."""
    
    def get_volume_ratio(self, symbol: str, current_volume: int) -> float:
        """Calculate volume ratio against baseline."""
```

**Usage**:
```python
from shared_core.volume_files.volume_computation_manager import VolumeManager
from redis.clients.factory import RedisClientFactory

client = RedisClientFactory.get_trading_client()
volume_mgr = VolumeManager(redis_client=client)

incremental = volume_mgr.calculate_incremental(token, cumulative, timestamp, symbol)
ratio = volume_mgr.get_volume_ratio(symbol, current_volume)
```

**Verification**: `rg 'VolumeManager|calculate_incremental|get_volume_ratio'`

---

### **📦 Tick Storage**

**Source of Truth**: `redis_files/unified_tick_storage.py` - `UnifiedTickStorage` class

**Method Signatures**:
```python
def get_unified_tick_storage(redis_client=None) -> UnifiedTickStorage:
    """Returns singleton UnifiedTickStorage instance."""

class UnifiedTickStorage:
    def __init__(self, redis_client=None):
        """Initialize with Redis client (uses RedisClientFactory if None)."""
    
    def store_tick(self, symbol: str, tick_data: Dict[str, Any], 
                   ttl: Optional[int] = 3600, publish_to_stream: bool = True) -> bool:
        """Store tick data to Redis DB1 (uses canonical symbol)."""
    
    def get_tick(self, symbol: str) -> Dict[str, Any]:
        """Get latest tick data from Redis DB1 (uses canonical symbol)."""
```

**Usage**:
```python
from redis_files.unified_tick_storage import get_unified_tick_storage

storage = get_unified_tick_storage()
storage.store_tick("BANKNIFTY25NOV57900CE", tick_data)
tick = storage.get_tick("BANKNIFTY25NOV57900CE")
```

**Verification**: `rg 'UnifiedTickStorage|get_unified_tick_storage|store_tick|get_tick'`

---

## 🗄️ **REDIS DATABASE ARCHITECTURE**

### **Database Structure**

| Database | Purpose | Client Method |
|----------|---------|---------------|
| **DB 1** | Live trading data (ticks, OHLC, indicators, Greeks, volume) | `RedisClientFactory.get_trading_client()` |
| **DB 2** | Analytics (pattern history, performance metrics, validations) | `RedisClientFactory.get_analytics_client()` |

### **Key Format Standards**

**Indicators (DB1)**:
- Format: `ind:{category}:{symbol}:{indicator_name}`
- Categories: `ta` (technical), `greeks` (option Greeks), `volume` (volume indicators)
- Example: `ind:ta:NFOBANKNIFTY25NOV57900CE:rsi`

**Greeks (DB1)**:
- Format: `ind:greeks:{symbol}:{greek_name}`
- Example: `ind:greeks:NFOBANKNIFTY25NOV57900CE:delta`

**Ticks (DB1)**:
- Stream: `ticks:stream:{symbol}`
- Hash: `ticks:{symbol}`

**Analytics (DB2)**:
- Pattern Performance: `pattern_performance:{symbol}:{pattern_type}`
- Pattern Metrics: `pattern_metrics:{symbol}:{pattern_type}`

**⚠️ CRITICAL**: Always use `DatabaseAwareKeyBuilder` methods - never hardcode keys.

---

## 🚀 **QUICK START**

### **1. Environment Setup**
```bash
source .venv/bin/activate
pip install -r requirements_apple_silicon.txt
```

### **2. Redis Connection**
```python
from redis.clients.factory import RedisClientFactory

# Get trading client (DB1)
client = RedisClientFactory.get_trading_client()
print(client.ping())  # Should return True
```

### **3. Store Indicators**
```python
from intraday_scanner.calculations.redis_storage import RedisStorageManager
from redis.clients.factory import RedisClientFactory

client = RedisClientFactory.get_trading_client()
storage = RedisStorageManager(redis_client=client)

indicators = {"rsi": 65.5, "macd": 12.3}
storage.store_indicators_atomic("BANKNIFTY25NOV57900CE", indicators)
```

### **4. Calculate Indicators**
```python
from intraday_scanner.calculations import HybridCalculations
from redis.clients.factory import RedisClientFactory

client = RedisClientFactory.get_trading_client()
calc = HybridCalculations(redis_client=client)

indicators = calc.batch_calculate_indicators([tick_data], symbol="BANKNIFTY25NOV57900CE")
```

---

## 📚 **DOCUMENTATION**

- **Indicator Storage**: `intraday_scanner/calculations/LEGACY_VARIANT_DEPRECATION.md`
- **Data Pipeline**: `intraday_scanner/DATA_PIPELINE_TECHNICAL.md`
- **Indicators & Greeks**: `docs/INDICATORS_GREEKS.md`
- **Alert Validation**: `alert_validation/README.md`

---

## ✅ **VERIFICATION COMMANDS**

```bash
# Check Redis client usage
rg 'RedisClientFactory|get_trading_client|get_analytics_client'

# Check symbol parsing
rg 'UniversalSymbolParser|get_symbol_parser|canonical_symbol'

# Check key generation
rg 'DatabaseAwareKeyBuilder|get_key_builder|live_indicator|live_greeks'

# Check indicator storage
rg 'RedisStorageManager|store_indicators_atomic'

# Check calculations
rg 'HybridCalculations|batch_calculate_indicators'

# Check market calendar
rg 'MarketCalendar|get_cached_calendar|is_trading_day'

# Check volume management
rg 'VolumeManager|calculate_incremental|get_volume_ratio'

# Check tick storage
rg 'UnifiedTickStorage|get_unified_tick_storage|store_tick|get_tick'
```

---

**Remember**: Always use the single sources of truth listed above. Never create duplicate implementations.
