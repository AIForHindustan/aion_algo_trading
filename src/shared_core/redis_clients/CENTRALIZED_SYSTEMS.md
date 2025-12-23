# Centralized Redis Systems - Active Architecture

This document describes the **active centralized systems** in `shared_core/redis_clients/` that serve as the single source of truth for Redis operations.

---

## 1. UnifiedRedisManager - Connection Management

**Location:** `redis_client.py`  
**Status:** ✅ Active - Preferred interface  
**Purpose:** Centralized Redis connection management with process-specific connection pools

### Access
```python
from shared_core.redis_clients import UnifiedRedisManager
```

### Function Signature
```python
UnifiedRedisManager.get_client(
    process_name: str,
    db: int = 0,
    max_connections: int = None,
    host: str = 'localhost',
    port: int = 6379,
    password: Optional[str] = None,
    decode_responses: bool = True
) -> redis.Redis
```

### Usage
```python
# Get client for DB 1 (live data)
client = UnifiedRedisManager.get_client(process_name="pattern_detector", db=1)

# Get client for DB 2 (analytics)
analytics_client = UnifiedRedisManager.get_client(process_name="pattern_detector", db=2)
```

### Key Features
- Process-specific connection pools (prevents connection exhaustion)
- Automatic pool size from `PROCESS_POOL_CONFIG`
- Thread-safe pool management
- Redis 8.2 health check intervals

---

## 2. DatabaseAwareKeyBuilder - Key Building

**Location:** `redis_key_standards.py`  
**Status:** ✅ Active - Source of truth for key generation  
**Purpose:** Builds Redis keys with explicit database assignment and validation

### Access
```python
from shared_core.redis_clients import get_key_builder

builder = get_key_builder()  # Returns DatabaseAwareKeyBuilder instance
```

### Key Methods

#### Database Assignment
```python
@staticmethod
def get_database(key: str) -> RedisDatabase
```
Determines which database a key belongs to based on its prefix.

#### DB1: Live Data Keys
```python
@staticmethod
def live_ohlc_latest(symbol: str) -> str
def live_ohlc_timeseries(symbol: str, interval: str = "1d") -> str
def live_ohlc_daily(symbol: str) -> str
def live_session(symbol: str, date: str) -> str
def live_underlying_price(symbol: str) -> str
def live_price_realtime(symbol: str) -> str
def live_ticks_hash(symbol: str) -> str
def live_ticks_latest(symbol: str) -> str
def live_ticks_stream(symbol: str) -> str
def live_ticks_raw_stream(symbol: str) -> str
def live_processed_stream() -> str
def live_unified_raw_stream() -> str
def live_raw_binary_stream() -> str
def live_historical_ticks_raw(symbol: str) -> str
def live_volume_state(instrument_token: str) -> str
def live_volume_baseline(symbol: str) -> str
def live_volume_profile(symbol: str, profile_type: str, date: Optional[str] = None) -> str
def live_volume_profile_realtime(symbol: str) -> str
def live_volume_profile_poc(symbol: str) -> str
def live_indicator(symbol: str, indicator_name: str, category: Optional[str] = None) -> str
def live_greeks(symbol: str, greek_name: Optional[str] = None) -> str
def live_bucket_history(symbol: str, resolution: str = "5min") -> str
def live_option_chain(underlying: str, strike: int, option_type: str, field: str) -> str
def get_option_chain_key(underlying: str) -> str
```

**Note on Bucket Storage:**
- Buckets are stored in **DB1** as session-based history lists
- Key pattern: `bucket_incremental_volume:history:{resolution}:{symbol}`
- Use `CumulativeDataTracker.get_time_buckets()` for proper session-based retrieval
- Legacy `get_rolling_window_buckets()` has been replaced with session-based approach

#### DB2: Analytics Keys
```python
@staticmethod
def analytics_pattern_history(pattern_type: str, symbol: str, field: str) -> str
def analytics_scanner_performance(metric: str, timeframe: str) -> str
def analytics_alert_performance_stats() -> str
def analytics_alert_performance_pattern(pattern: str) -> str
def analytics_signal_quality(symbol: str, pattern_type: str) -> str
def analytics_pattern_performance(symbol: str, pattern_type: str) -> str
def analytics_pattern_metrics(symbol: str, pattern_type: str) -> str
def analytics_validation(alert_id: str) -> str
def analytics_legacy_cache(symbol: str, indicator_name: str) -> str
def analytics_final_validation(pattern_type: str, symbol: str) -> str
def analytics_pattern_window_aggregate(pattern_type: str, window_key: str) -> str
def analytics_time_window_performance(pattern_type: str, symbol: str, window_key: str) -> str
def analytics_alert_timeline(alert_id: str) -> str
def analytics_sharpe_inputs(pattern_type: str, window_key: str) -> str
```

#### Alert Streams (DB1)
```python
@staticmethod
def live_alerts_stream() -> str
def live_alerts_telegram() -> str
def live_pattern_stream(pattern_type: str, symbol: str) -> str
```

#### Alert Validation Storage (DB2)
```python
@staticmethod
def analytics_alert_storage(alert_id: str) -> str
def analytics_validation_storage(alert_id: str) -> str
def analytics_validation_stream() -> str
def analytics_validation_recent() -> str
```

### Usage
```python
from shared_core.redis_clients import get_key_builder

builder = get_key_builder()

# Build keys for live data (DB1)
ohlc_key = builder.live_ohlc_latest("NIFTY")
tick_key = builder.live_ticks_hash("RELIANCE")
indicator_key = builder.live_indicator("NIFTY", "rsi", "ta")
greeks_key = builder.live_greeks("NIFTY25NOV26400CE", "delta")

# Build keys for analytics (DB2)
pattern_key = builder.analytics_pattern_history("high", "BANKNIFTY", "values")
performance_key = builder.analytics_pattern_performance("NIFTY", "breakout")
```

---

## 3. RedisKeyStandards - Symbol Normalization

**Location:** `redis_key_standards.py`  
**Status:** ✅ Active - Source of truth for symbol normalization  
**Purpose:** Standardized Redis key lookup utilities with database awareness

### Access
```python
from shared_core.redis_clients import RedisKeyStandards
```

### Key Methods

#### Symbol Normalization
```python
@staticmethod
def canonical_symbol(symbol: str) -> str
```
Canonical Redis symbol format (matches OHLC/indicator storage). Transforms Zerodha symbol naming to actual expiry dates.

#### Symbol Variants
```python
@staticmethod
def get_indicator_symbol_variants(symbol: str) -> List[str]
```
Returns prioritized symbol variants for indicator storage/retrieval.

#### Indicator Categorization
```python
@staticmethod
def _categorize_indicator(indicator: str) -> str
```
Categorizes indicator for unified key structure. Returns: `'ta'`, `'greeks'`, `'volume'`, or `'custom'`.

#### Database Client Selection
```python
@staticmethod
def get_redis_client(key: str, redis_clients: Dict[RedisDatabase, Any]) -> Any
```
Get the correct Redis client based on key prefix.

### Usage
```python
from shared_core.redis_clients import RedisKeyStandards

# Normalize symbol
canonical = RedisKeyStandards.canonical_symbol("NIFTY25NOV26400CE")
# Returns: "NFONIFTY25NOV26400CE" (with exchange prefix)

# Get symbol variants for indicator lookup
variants = RedisKeyStandards.get_indicator_symbol_variants("NIFTY")
# Returns: ["NFONIFTY", "NIFTY", "NSE:NIFTY", ...]
```

---

## 4. RedisKeyMapping - Field Name Mapping

**Location:** `redis_key_mapping.py`  
**Status:** ✅ Active - Handles field name mapping  
**Purpose:** Maps between stored Redis field names and canonical field names

### Access
```python
from shared_core.redis_clients import get_redis_key_mapping, redis_key_mapper

mapper = get_redis_key_mapping()  # Or use singleton: redis_key_mapper
```

### Key Methods

#### Field Normalization
```python
def normalize_stored_fields(self, tick_data: Dict[str, Any]) -> Dict[str, Any]
```
Normalize stored Redis fields to canonical names. Preserves ALL fields including computed indicators.

#### Field Resolution
```python
def resolve_canonical_field(self, canonical_field: str) -> str
```
Resolve canonical field name to stored field name (as it appears in Redis).

#### Key Resolution
```python
def get_canonical_key(self, key: str) -> str
```
Convert any key/alias to its canonical form.

```python
def resolve_key(self, category: str, **kwargs) -> str
```
Resolve a canonical key by category and parameters.

#### Consumer Mapping
```python
def map_tick_data_for_consumer(
    self, 
    tick_data: Dict[str, Any], 
    consumer_type: str = "pattern"
) -> Dict[str, Any]
```
Map tick data to consumer-expected field names. Consumer types: `"pattern"`, `"scanner"`, `"dashboard"`.

### Convenience Functions
```python
from shared_core.redis_clients import (
    normalize_redis_tick_data,
    map_tick_for_pattern,
    map_tick_for_scanner,
    map_tick_for_dashboard,
    resolve_redis_key,
    get_redis_key_fields
)

# Normalize tick data
normalized = normalize_redis_tick_data(tick_data)

# Map for specific consumer
pattern_data = map_tick_for_pattern(tick_data)
scanner_data = map_tick_for_scanner(tick_data)
dashboard_data = map_tick_for_dashboard(tick_data)

# Resolve Redis key
key = resolve_redis_key("ticks:latest:{symbol}", symbol="NIFTY")
```

### Usage
```python
from shared_core.redis_clients import redis_key_mapper

# Normalize fields from Redis
tick_data = redis_client.hgetall("ticks:NIFTY")
normalized = redis_key_mapper.normalize_stored_fields(tick_data)

# Resolve canonical key
canonical_key = redis_key_mapper.resolve_key("tick_storage.hash", symbol="NIFTY")
```

---

## 5. UnifiedDataStorage - Data Storage

**Location:** `unified_data_storage.py`  
**Status:** ✅ Active - Source of truth for storage operations  
**Purpose:** Unified interface for storing and retrieving all trading data in Redis

### Access
```python
from shared_core.redis_clients import get_unified_storage

storage = get_unified_storage(redis_client=None)
```

### Function Signatures

#### Initialization
```python
def __init__(
    self, 
    redis_url='redis://localhost:6379/1',  # DEPRECATED, use redis_client
    redis_client=None, 
    db: int = 1
)
```

#### Storage Methods
```python
def store_tick_data(self, symbol: str, data: Dict[str, Any], ttl: int = 3600)
def store_indicators(self, symbol: str, indicators: Dict[str, float], ttl: int = 86400)
def store_greeks(self, symbol: str, greeks: Dict[str, float], ttl: int = 86400)
```

#### Retrieval Methods
```python
def get_all_data(self, symbol: str) -> Dict[str, Any]
def get_tick_data(self, symbol: str) -> Dict[str, Any]
def get_indicators(
    self, 
    symbol: str, 
    indicator_names: Optional[List[str]] = None
) -> Dict[str, Any]
def get_all_indicators(self, symbol: str) -> Dict[str, Any]
def get_greeks(self, symbol: str) -> Dict[str, float]
```

### Usage
```python
from shared_core.redis_clients import get_unified_storage

storage = get_unified_storage()

# Store data
storage.store_indicators("NIFTY", {"rsi": 65.5, "macd": 12.3})
storage.store_greeks("NIFTY25NOV26400CE", {"delta": 0.5, "gamma": 0.02})

# Retrieve data
all_data = storage.get_all_data("NIFTY")
indicators = storage.get_indicators("NIFTY", ["rsi", "macd", "atr"])
greeks = storage.get_greeks("NIFTY25NOV26400CE")
```

---

## 6. UnifiedTickStorage - Tick Storage

**Location:** `unified_tick_storage.py`  
**Status:** ✅ Active - Enforces standards-compliant keys  
**Purpose:** Single source of truth for tick storage operations (uses ONLY `ticks:{symbol}`, NOT `tick:{symbol}`)

### Access
```python
from shared_core.redis_clients import get_unified_tick_storage

tick_storage = get_unified_tick_storage(redis_client=None, db=1)
```

### Function Signatures

#### Initialization
```python
def __init__(self, redis_client=None, db: int = 1)
```

#### Storage
```python
def store_tick(
    self, 
    symbol: str, 
    tick_data: Dict[str, Any], 
    ttl: Optional[int] = 3600, 
    publish_to_stream: bool = True
) -> bool
```

#### Retrieval
```python
def get_tick(self, symbol: str) -> Dict[str, Any]
def get_tick_from_stream(self, symbol: str, count: int = 1) -> List[Dict[str, Any]]
def exists(self, symbol: str) -> bool
```

### Usage
```python
from shared_core.redis_clients import get_unified_tick_storage

tick_storage = get_unified_tick_storage()

# Store tick (uses ticks:{symbol} hash)
tick_storage.store_tick("NIFTY", {
    "last_price": 19500.0,
    "volume": 1000000,
    "timestamp_ms": 1234567890
})

# Retrieve tick
tick_data = tick_storage.get_tick("NIFTY")

# Check existence
if tick_storage.exists("NIFTY"):
    print("Tick data available")
```

---

## 7. UniversalSymbolParser - Symbol Parsing

**Location:** `redis_key_standards.py`  
**Status:** ✅ Active - Source of truth for symbol parsing  
**Purpose:** Universal symbol parser for all asset classes (equity, futures, options, currency, commodity)

### Access
```python
from shared_core.redis_clients import get_symbol_parser

parser = get_symbol_parser()  # Returns UniversalSymbolParser instance
```

### Function Signature
```python
def parse_symbol(self, symbol: str, segment: str = "") -> ParsedSymbol
```

### ParsedSymbol Dataclass
```python
@dataclass
class ParsedSymbol:
    raw_symbol: str
    normalized: str
    asset_class: str  # equity, futures, options, currency, commodity
    instrument_type: str  # EQ, FUT, OPT, CUR, COM
    base_symbol: str  # Underlying symbol
    expiry: Optional[datetime] = None
    strike: Optional[float] = None
    option_type: Optional[str] = None  # CE, PE
    lot_size: int = 1
```

### Usage
```python
from shared_core.redis_clients import get_symbol_parser

parser = get_symbol_parser()

# Parse symbol
parsed = parser.parse_symbol("NIFTY25NOV26400CE")
print(parsed.asset_class)      # "options"
print(parsed.instrument_type)  # "OPT"
print(parsed.base_symbol)      # "NIFTY"
print(parsed.strike)           # 26400.0
print(parsed.option_type)      # "CE"
print(parsed.expiry)           # datetime object
```

---

## 8. Redis8Config - Configuration

**Location:** `redis_config.py`  
**Status:** ✅ Active - Single source of truth for Redis config  
**Purpose:** Unified Redis configuration with feature awareness

### Access
```python
from shared_core.redis_clients.redis_config import (
    Redis8Config,
    get_redis_config,
    get_database_for_data_type,
    get_ttl_for_data_type
)
```

### Function Signatures

#### Configuration Class
```python
class Redis8Config:
    def __init__(self, environment: Optional[str] = None)
    def get_db_config(self, db_name: str) -> Dict[str, Any]
    def get_db_features(self, db_name: str) -> Dict[str, bool]
    def apply_low_latency_optimizations(self, redis_client) -> bool
```

#### Helper Functions
```python
def get_redis_config(environment: Optional[str] = None) -> Dict[str, Any]
def get_database_for_data_type(data_type: str) -> int
def get_ttl_for_data_type(data_type: str) -> int
```

### Database Assignment
- **DB 0:** System / metadata
- **DB 1:** Real-time data (ticks, OHLC, volume, indicators, alerts)
- **DB 2:** Analytics (pattern history, performance metrics, validations)
- **DB 3:** Independent validator system

### Usage
```python
from shared_core.redis_clients.redis_config import (
    get_database_for_data_type,
    get_ttl_for_data_type
)

# Get database for data type
db = get_database_for_data_type("equity_prices")  # Returns: 1

# Get TTL for data type
ttl = get_ttl_for_data_type("analytics_data")  # Returns: 57600 (16 hours)
```

---

## 9. VolumeStateManager - Volume State

**Location:** `volume_state_manager.py`  
**Status:** ✅ Active - Single source of truth for volume state  
**Purpose:** Volume state management with Redis persistence

### Access
```python
from shared_core.redis_clients import get_volume_computation_manager

volume_manager = get_volume_computation_manager(redis_client=None, token_resolver=None)
```

### Function Signatures

#### Initialization
```python
def __init__(self, redis_client: redis.Redis, token_resolver=None)
```

#### Core Methods
```python
def calculate_incremental(
    self, 
    instrument_token: str, 
    current_cumulative: int, 
    exchange_timestamp: datetime
) -> int
```
Calculate incremental volume with proper session reset handling. Returns incremental volume (never uses cumulative as incremental).

```python
def get_volume_profile_data(self, symbol: str) -> Dict
```
Get volume profile data for pattern detection.

```python
def track_straddle_volume(
    self, 
    underlying_symbol: str, 
    ce_symbol: str, 
    pe_symbol: str, 
    ce_volume: int, 
    pe_volume: int, 
    exchange_timestamp: datetime
) -> Dict[str, int]
```
Track volume for straddle strategy (CE + PE combined).

### Usage
```python
from shared_core.redis_clients import get_volume_computation_manager
from datetime import datetime

volume_manager = get_volume_computation_manager()

# Calculate incremental volume
incremental = volume_manager.calculate_incremental(
    instrument_token="256265",
    current_cumulative=1000000,
    exchange_timestamp=datetime.now()
)

# Get volume profile
profile = volume_manager.get_volume_profile_data("NIFTY")

# Track straddle volume
straddle_metrics = volume_manager.track_straddle_volume(
    underlying_symbol="NIFTY",
    ce_symbol="NIFTY25NOV26400CE",
    pe_symbol="NIFTY25NOV26400PE",
    ce_volume=500000,
    pe_volume=600000,
    exchange_timestamp=datetime.now()
)
```

---

## 10. CanonicalKeyBuilder - YAML-Based Key Building

**Location:** `canonical_key_builder.py`  
**Status:** ✅ Active - Uses canonical mapping from YAML  
**Purpose:** Enhanced key builder that uses canonical mapping from `redis_key_mapping.yaml`

### Access
```python
from shared_core.redis_clients import canonical_key_builder

# canonical_key_builder is a singleton CanonicalKeyBuilder instance
```

### Key Methods
```python
def live_processed_stream(self) -> str
def live_raw_stream(self) -> str
def live_raw_stream_symbol(self, symbol: str) -> str
def symbol_data(self, symbol: str, data_type: str = "latest") -> str
def live_greeks(self, symbol: str, greek_name: str) -> str
def volume_baseline(self, symbol: str) -> str
def order_book_imbalance(self, symbol: str, depth: int = 5) -> str
def time_bucket(self, symbol: str, timeframe: str = "1m") -> str
def pattern_detected(self, symbol: str) -> str
def live_ticks_hash(self, symbol: str) -> str
def live_ticks_raw_stream(self, symbol: str) -> str
def live_ohlc_latest(self, symbol: str) -> str
def live_volume_profile_poc(self, symbol: str) -> str
def live_volume_profile(self, symbol: str, profile_type: str = "session") -> str
def live_indicator(self, symbol: str, indicator_name: str, category: str = "ta") -> str
```

### Usage
```python
from shared_core.redis_clients import canonical_key_builder

# Get canonical keys
stream_key = canonical_key_builder.live_processed_stream()
tick_key = canonical_key_builder.live_ticks_hash("NIFTY")
indicator_key = canonical_key_builder.live_indicator("NIFTY", "rsi", "ta")
```

---

## 11. CanonicalRedisClient - Automatic Key Conversion

**Location:** `canonical_redis_client.py`  
**Status:** ✅ Active - Auto-converts keys via decorator  
**Purpose:** Redis client wrapper that automatically uses canonical keys

### Access
```python
from shared_core.redis_clients import CanonicalRedisClient
import redis

raw_client = redis.Redis(host='localhost', port=6379, db=1)
client = CanonicalRedisClient(raw_client)
```

### Wrapped Methods
All Redis operations automatically convert keys to canonical form:
- `get(key)`, `set(key, value)`
- `hget(name, key)`, `hset(name, mapping)`, `hgetall(name)`
- `xadd(stream, fields)`, `xread(streams, count)`
- `publish(channel, message)`
- `keys(pattern)`, `scan(cursor, match)`
- `delete(*keys)`, `exists(*keys)`
- `expire(key, time)`, `ttl(key)`

### Usage
```python
from shared_core.redis_clients import CanonicalRedisClient
import redis

raw_client = redis.Redis(host='localhost', port=6379, db=1)
client = CanonicalRedisClient(raw_client)

# Keys are automatically converted to canonical form
client.set("ticks:intraday:processed", "value")  # Uses canonical key
value = client.get("ticks:live:processed")  # Alias converted to canonical
```

---

## 12. @canonical_keys Decorator

**Location:** `decorators.py`  
**Status:** ✅ Active - Automatic key conversion  
**Purpose:** Decorator that automatically converts Redis keys to canonical form

### Usage
```python
from shared_core.redis_clients import canonical_keys

@canonical_keys
def get(self, key: str):
    return self._client.get(key)
```

---

## Summary: How to Use

### 1. Get Redis Client
```python
from shared_core.redis_clients import UnifiedRedisManager

client = UnifiedRedisManager.get_client(process_name="your_process", db=1)
```

### 2. Build Keys
```python
from shared_core.redis_clients import get_key_builder

builder = get_key_builder()
key = builder.live_ohlc_latest("NIFTY")
```

### 3. Normalize Symbols
```python
from shared_core.redis_clients import RedisKeyStandards

canonical = RedisKeyStandards.canonical_symbol("NIFTY25NOV26400CE")
```

**Important Notes:**
- **Always use `RedisKeyStandards.canonical_symbol()`** for symbol normalization (section 3 standard)
- **For volume baseline keys:** Use `canonical_symbol()` before passing to `builder.live_volume_baseline()` (the key builder doesn't normalize internally)
- **For OHLC keys:** Key builder methods (`live_ohlc_latest()`, etc.) normalize internally using `normalize_symbol()`, so just pass the raw symbol
- **For ticks/indicators/greeks keys:** Key builder methods normalize internally using `canonical_symbol()`, so just pass the raw symbol
- **For watchlists/comparisons:** Use `canonical_symbol()` for consistency

### 4. Store Data
```python
from shared_core.redis_clients import get_unified_storage

storage = get_unified_storage()
storage.store_indicators("NIFTY", {"rsi": 65.5})
```

### 5. Retrieve Data
```python
indicators = storage.get_indicators("NIFTY", ["rsi", "macd"])
```

### 6. Retrieve Bucket Data (Session-Based)
```python
from shared_core.redis_clients.redis_client import CumulativeDataTracker

# Initialize tracker with DB1 client
client = UnifiedRedisManager.get_client(process_name="your_process", db=1)
tracker = CumulativeDataTracker(client)

# Get buckets using session-based retrieval
buckets = tracker.get_time_buckets(
    symbol="NIFTY",
    session_date=None,  # Uses current session
    lookback_minutes=60,
    use_history_lists=True  # Use efficient history lists
)

# Or use wrapper method (if available)
if hasattr(redis_client, 'get_rolling_window_buckets'):
    buckets = redis_client.get_rolling_window_buckets(
        symbol="NIFTY",
        lookback_minutes=60,
        session_date=None
    )
```

**Bucket Storage Details:**
- **Storage:** Buckets are stored in **DB1** as session-based history lists
- **Key Pattern:** `bucket_incremental_volume:history:{resolution}:{symbol}`
- **Resolutions:** `1min`, `2min`, `5min`, `10min`
- **Retrieval:** Use `CumulativeDataTracker.get_time_buckets()` for proper session-based access
- **Legacy:** `get_rolling_window_buckets()` has been replaced with session-based approach using `CumulativeDataTracker`

### 6. Normalize Fields
```python
from shared_core.redis_clients import normalize_redis_tick_data

normalized = normalize_redis_tick_data(tick_data)
```

---

## Database Assignment

- **DB 1 (realtime):** Live ticks, OHLC, volume, indicators, session data, alerts
- **DB 2 (analytics):** Pattern history, performance metrics, validations, volume profiles
- **DB 3 (independent_validator_db):** Signal quality, pattern performance, validation metrics

---

## Key Standards

- ✅ Use `ticks:{symbol}` (plural) - NOT `tick:{symbol}` (singular)
- ✅ Use `DatabaseAwareKeyBuilder` for all key generation
- ✅ Use `RedisKeyStandards.canonical_symbol()` for symbol normalization
- ✅ Use `UnifiedRedisManager.get_client()` for connections
- ✅ Use `UnifiedDataStorage` for storage operations
- ❌ FORBIDDEN: Pattern matching (KEYS, SCAN) except in admin/debug scripts
- ❌ FORBIDDEN: Direct key construction without key builder

---

## Known Anomalies in intraday_trading

The following anomalies have been identified and documented. These are legacy patterns that should be migrated to use the centralized key builder when possible.

### Legacy Key Patterns

1. **`ticks:realtime:{symbol}`** (transitional_manager.py, data_migrator.py)
   - **Status:** Legacy key pattern used during migration
   - **Current Usage:** Transitional manager for zero-downtime migration
   - **Key Builder Alternative:** `builder.live_ticks_stream(symbol)` returns `ticks:stream:{symbol}`
   - **Note:** This pattern is intentionally used for backward compatibility during migration. Consider migrating to `ticks:stream:{symbol}` after migration is complete.

2. **`tick:recent:{symbol}`** (trace_live_data_flow.py)
   - **Status:** Legacy key pattern
   - **Current Usage:** Debug/trace scripts
   - **Key Builder Alternative:** Use `builder.live_ticks_latest(symbol)` for latest tick data
   - **Note:** No direct method exists for "recent" ticks. Use `live_ticks_latest()` or `live_ticks_hash()` instead.

3. **`volume:current:{symbol}`** (trace_live_data_flow.py)
   - **Status:** Legacy key pattern
   - **Current Usage:** Debug/trace scripts
   - **Key Builder Alternative:** Use `builder.live_volume_baseline(symbol)` for volume baseline
   - **Note:** No direct method exists for "current" volume. Use volume baseline or volume profile methods.

4. **`greeks:{symbol}`** (trace_live_data_flow.py)
   - **Status:** Legacy key pattern
   - **Current Usage:** Debug/trace scripts
   - **Key Builder Alternative:** Use `builder.live_greeks(symbol, greek_name)` for specific Greeks
   - **Note:** Greeks are stored per-greek, not as a single hash. Use `live_greeks(symbol, "delta")` etc.

### Migration Scripts

The following files intentionally use pattern matching and hardcoded keys for migration/debug purposes:
- `data_migrator.py` - Migration script (uses patterns for bulk operations)
- `debug_redis_data.py` - Debug script (uses patterns for diagnostics)
- `audit_redis_migration.py` - Audit script (uses patterns for validation)
- `token_debugger.py` - Debug script (uses patterns for key discovery)

These scripts are exempt from the "no pattern matching" rule as they are admin/debug tools.

---

## Known Anomalies in aion_trading_dashboard

The following anomalies have been identified in the dashboard. These are either system-level keys or keys from external systems that are not part of the centralized key builder.

### System-Level Keys (DB 0)

These keys are dashboard-specific system keys stored in DB 0 (system database) and are not part of the trading data key builder:

1. **`user:{account_number}`** (optimized_main.py, user_preferences.py, order_api.py)
   - **Status:** System key for user data storage
   - **Current Usage:** User preferences, account data, trading settings
   - **Database:** DB 0 (system)
   - **Note:** These are dashboard-specific system keys, not trading data keys. No key builder method exists as they are outside the trading data scope.

2. **`account_key:{account_number}`** (optimized_main.py)
   - **Status:** System key for account key mappings
   - **Current Usage:** Maps account numbers to broker account keys
   - **Database:** DB 0 (system)
   - **Note:** Dashboard-specific system key for account management.

3. **`positions:{account_number}:{strategy_tag}`** (optimized_main.py, order_api.py, robot_order_executor.py)
   - **Status:** System key for position tracking
   - **Current Usage:** Tracks user positions per strategy
   - **Database:** DB 0 (system)
   - **Note:** Dashboard-specific system key for position management.

4. **`pending_orders_capital`** (capital_manager.py)
   - **Status:** System key for capital reservation
   - **Current Usage:** Hash storing pending order capital reservations
   - **Database:** DB 0 (system)
   - **Note:** Dashboard-specific system key for capital management.

### External System Keys

1. **`index:NSE:{name}`** (optimized_main.py, websocket_router.py)
   - **Status:** External system key from gift_nifty_gap.py
   - **Current Usage:** Index data published by gift_nifty_gap.py crawler
   - **Database:** DB 1 (realtime)
   - **Key Builder Alternative:** No direct method exists. These keys are published by an external crawler (gift_nifty_gap.py) with format `index:NSE:{name}` where name can be "NIFTY 50", "NIFTY BANK", "INDIA VIX".
   - **Note:** These keys are intentionally hardcoded to match the exact format published by gift_nifty_gap.py. The centralized key builder focuses on trading data keys, not external crawler-specific keys.

### Summary

The dashboard correctly uses the centralized key builder for all trading data keys (alerts, OHLC, indicators, etc.). The hardcoded keys found are:
- **System keys (DB 0):** Dashboard-specific user/account/position management (intentionally outside key builder scope)
- **External keys:** Keys from external crawlers like gift_nifty_gap.py (intentionally hardcoded to match external format)

No action required for these keys as they are outside the scope of the centralized trading data key builder.

---

## Known Anomalies in zerodha

The following anomalies have been identified in the zerodha crawlers. These are either pattern matching for scanning (acceptable for debug) or keys for external systems.

### Pattern Matching (Debug/Admin)

1. **`"ticks:latest:*"`** (websocket_message_parser.py)
   - **Status:** Pattern match for scanning Redis keys
   - **Current Usage:** Debug/admin script for finding underlying prices
   - **Note:** Pattern matching is acceptable for debug/admin scripts. This is used for scanning Redis to find commodity futures prices.

2. **`"index:*"`** (websocket_message_parser.py)
   - **Status:** Pattern match for scanning Redis keys
   - **Current Usage:** Debug/admin script for finding index prices
   - **Note:** Pattern matching is acceptable for debug/admin scripts. This is used for scanning Redis to find index prices.

### External System Streams

1. **`ticks:stream`** (data_mining_crawler.py, ubuntu crawlers)
   - **Status:** ClickHouse pipeline stream
   - **Current Usage:** Stream for ClickHouse data ingestion (port 6380)
   - **Database:** DB 1 (port 6380 - ClickHouse dedicated port)
   - **Key Builder Alternative:** No direct method exists. This is a dedicated stream for ClickHouse pipeline, separate from the trading data streams.
   - **Note:** Ubuntu crawlers (data_mining_crawler, research_crawler, commodity_crawler) publish to `ticks:stream` on port 6380 for ClickHouse ingestion only. This is intentionally separate from the intraday trading streams on port 6379.

### Port Configuration

**✅ VERIFIED:**
- **Intraday crawlers:** Use port 6379 (trading/realtime data) - ✅ Correct
- **Ubuntu crawlers:** Use port 6380 (ClickHouse pipeline) - ✅ Correct

All crawlers now explicitly specify their ports:
- Intraday crawlers: `port=6379` in `UnifiedRedisManager.get_client()` calls
- Ubuntu crawlers: `port=6380` in `UnifiedRedisManager.get_client()` calls

### Summary

The zerodha crawlers correctly use the centralized key builder for all trading data keys. The hardcoded patterns found are:
- **Pattern matching:** Used in debug/admin scripts for scanning (acceptable)
- **External streams:** `ticks:stream` for ClickHouse pipeline (intentionally separate from trading streams)

All port configurations are now explicit and correct.

---

## Known Anomalies in Rest of Codebase

The following anomalies have been identified in utility scripts, diagnostic tools, and backfill/migration scripts.

### Utility Scripts (utils/)

1. **`utils/historical_archive.py`** - Stream configuration dictionary
   - **Status:** Acceptable - Configuration dictionary for stream management
   - **Current Usage:** Defines maxlen for streams in a configuration dict
   - **Note:** These are configuration values, not direct key usage. The script uses them for stream management operations.

2. **`utils/performance_monitor.py`** - Stream name strings
   - **Status:** Acceptable - Monitoring script that checks stream health
   - **Current Usage:** Stream monitoring and health checks
   - **Note:** Monitoring scripts may use hardcoded stream names for diagnostic purposes.

### Diagnostic Scripts (scripts/)

1. **`scripts/validate_downstream_consumers.py`** - Pattern strings for scanning
   - **Status:** Acceptable - Diagnostic script with pattern matching
   - **Current Usage:** Validates downstream consumer access patterns
   - **Note:** Uses pattern strings like `"ticks:raw:{symbol}"` and `"ticks:latest:{symbol}"` for scanning. These are acceptable for diagnostic scripts.

2. **`scripts/audit_pattern_detection_inputs.py`** - Example key patterns in documentation
   - **Status:** Acceptable - Documentation/example strings
   - **Current Usage:** Shows example key patterns in print statements
   - **Note:** These are documentation strings showing example key formats, not actual key usage.

3. **`scripts/emergency_diagnostic.py`** - Pattern matching for scanning
   - **Status:** Acceptable - Emergency diagnostic script
   - **Current Usage:** Scans Redis for indicator and Greeks keys using patterns
   - **Note:** Pattern matching is acceptable for diagnostic/emergency scripts.

### Backfill/Migration Scripts (clickhouse_setup/)

1. **`clickhouse_setup/backfill_redis_to_clickhouse.py`** - Stream name tuples
   - **Status:** Acceptable - Backfill script configuration
   - **Current Usage:** Defines streams to process in a configuration list
   - **Note:** Backfill scripts intentionally use hardcoded stream names to process historical data from specific streams. These are configuration values, not direct key construction.

2. **`clickhouse_setup/backfill_ticks_to_clickhouse.py`** - Stream configuration dictionary
   - **Status:** Acceptable - Backfill script configuration
   - **Current Usage:** Defines stream configurations for backfill operations
   - **Note:** Configuration dictionaries for backfill operations are acceptable.

3. **`clickhouse_setup/scripts/redis_clickhouse_bridge.py`** - Default stream constant
   - **Status:** Acceptable - Bridge configuration
   - **Current Usage:** Default stream name for bridge operations
   - **Note:** Configuration constants are acceptable.

### Summary

The rest of the codebase correctly uses the centralized key builder for all production trading data keys. The hardcoded keys found are:
- **Configuration dictionaries:** Stream management and backfill configurations (acceptable)
- **Diagnostic scripts:** Pattern matching for scanning and validation (acceptable)
- **Documentation strings:** Example key patterns in print statements (acceptable)

All production code that builds Redis keys for trading data now uses the centralized key builder.

---

## Symbol Normalization Migration Status (2025-01-XX)

### ✅ Completed Migration

All legacy `normalize_symbol()` imports and usages have been replaced with `RedisKeyStandards.canonical_symbol()` across the codebase:

1. **patterns/pattern_detector.py** - ✅ All `normalize_symbol()` calls replaced with `RedisKeyStandards.canonical_symbol()`
2. **intraday_scanner/calculations.py** - ✅ All `normalize_symbol()` and `normalize_ohlc_symbol()` calls replaced
3. **patterns/mm_exploitation_strategies.py** - ✅ All `KEY_BUILDER.normalize_symbol()` calls replaced
4. **audit_redis_migration.py** - ✅ Legacy import removed
5. **migrate_redis_data.py** - ✅ Legacy import removed

### ⚠️ Backward Compatibility Wrappers (Acceptable)

The following wrapper functions are kept for backward compatibility but internally use `RedisKeyStandards.canonical_symbol()`:

1. **intraday_scanner/data_pipeline.py** - `_normalize_symbol()` method
   - Status: ✅ Acceptable - Wrapper that uses `RedisKeyStandards.canonical_symbol()` internally
   - Note: Marked as DEPRECATED, delegates to `_get_unified_symbol()` or uses `RedisKeyStandards.canonical_symbol()` directly

2. **patterns/game_theory_engine.py** - `_normalize_symbol()` function
   - Status: ✅ Acceptable - Wrapper that uses `RedisKeyStandards.canonical_symbol()` internally
   - Note: Used internally within the module, correctly delegates to `RedisKeyStandards.canonical_symbol()`

### 📝 Documentation Files (No Changes Needed)

The following files contain references to `normalize_symbol()` but are documentation/examples only:
- `README.md` - Documentation examples
- `PATTERN_ISSUES_RESOLUTION_ANALYSIS.md` - Historical analysis document

### Summary

✅ **All production code now uses `RedisKeyStandards.canonical_symbol()` for symbol normalization**
- Legacy `normalize_symbol()` imports removed
- Legacy `normalize_ohlc_symbol()` alias removed
- All direct calls replaced with `RedisKeyStandards.canonical_symbol()`
- Backward compatibility wrappers correctly delegate to centralized method

**Standard Usage:**
```python
from shared_core.redis_clients import RedisKeyStandards

canonical = RedisKeyStandards.canonical_symbol("NIFTY25NOV26400CE")
```

