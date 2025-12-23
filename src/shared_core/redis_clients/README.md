# Redis Infrastructure

**Redis client management, key standards, and storage utilities.**

---

## 📋 Overview

The `redis_files/` directory provides:

1. **Redis Client Factory** - Centralized Redis connection management
2. **Key Standards** - Canonical key generation and symbol parsing
3. **Storage Utilities** - Unified data storage interfaces

---

## 🏗️ Architecture

```
RedisClientFactory
    ├─ get_trading_client() → DB1 (live data)
    ├─ get_analytics_client() → DB2 (analytics)
    └─ Connection pooling
    ↓
RedisKeyStandards
    ├─ Canonical key generation
    ├─ Symbol parsing/normalization
    └─ Database-aware key builders
    ↓
Unified Storage
    ├─ Indicator storage
    ├─ Greeks storage
    └─ Session data storage
```

---

## 📁 Key Files

### `redis_client.py`
**Purpose**: Redis client factory and connection management

**Key Classes**:
- `RedisClientFactory` - Centralized client factory

**Key Methods**:
```python
@staticmethod
def get_trading_client(process_name: str = "trading") -> redis.Redis:
    """Returns DB1 client for trading operations (live data)."""
    # Always returns DB1

@staticmethod
def get_analytics_client(process_name: str = "analytics") -> redis.Redis:
    """Returns DB2 client for analytics operations."""
    # Always returns DB2

@classmethod
def get_client(cls, db: int = 1, process_name: str = "default", **kwargs) -> redis.Redis:
    """Get Redis client for specified database."""
    # DB1 = trading, DB2 = analytics
```

**Database Routing**:
- **DB1 (Trading)**: Live data, streams, indicators, session data
- **DB2 (Analytics)**: Validation results, performance metrics, pattern history

**Connection Pooling**:
- Process-specific connection pools
- Automatic reconnection
- Thread-safe

---

### `redis_key_standards.py`
**Purpose**: Canonical key generation and symbol parsing

**Key Classes**:
- `RedisKeyStandards` - Static key builders
- `DatabaseAwareKeyBuilder` - Database-aware key builders
- `UniversalSymbolParser` - Symbol parsing and normalization

**Key Methods**:

#### Symbol Parsing
```python
def get_symbol_parser() -> UniversalSymbolParser:
    """Returns singleton UniversalSymbolParser instance."""

class UniversalSymbolParser:
    def parse_symbol(self, symbol: str) -> ParsedSymbol:
        """Parse symbol into components."""
    
    def normalize_symbol(self, symbol: str) -> str:
        """Normalize to canonical format."""
```

#### Key Generation
```python
def get_key_builder() -> DatabaseAwareKeyBuilder:
    """Returns singleton DatabaseAwareKeyBuilder instance."""

class DatabaseAwareKeyBuilder:
    def live_indicator(self, symbol: str, indicator: str) -> str:
        """ind:ta:{symbol}:{indicator} (DB1)"""
    
    def live_greeks(self, symbol: str, greek: str) -> str:
        """ind:greeks:{symbol}:{greek} (DB1)"""
    
    def live_session(self, symbol: str, date: str) -> str:
        """session:{symbol}:{date} (DB1)"""
    
    def analytics_time_window_performance(self, pattern: str, symbol: str, window: str) -> str:
        """analytics:time_window:performance:{pattern}:{symbol}:{window} (DB2)"""
```

**Canonical Symbol Format**:
- Options: `NFOBANKNIFTY25NOV57900CE`
- Futures: `NFOBANKNIFTY25NOVFUT`
- Equities: `NSE:RELIANCE`

---

### `unified_data_storage.py`
**Purpose**: Unified storage interface

**Key Classes**:
- `UnifiedDataStorage` - Unified storage manager

**Key Methods**:
```python
def store_indicators(self, indicators: Dict[str, Dict[str, float]]) -> bool:
    """Store indicators atomically."""
    # Uses RedisStorageManager.store_indicators_atomic()

def store_greeks(self, greeks: Dict[str, Dict[str, float]]) -> bool:
    """Store Greeks atomically."""
    # Uses RedisStorageManager.store_greeks_directly()
```

---

## 🔑 Key Standards

### Indicator Keys (DB1)
```
ind:ta:{symbol}:rsi
ind:ta:{symbol}:macd
ind:ta:{symbol}:bb_upper
ind:ta:{symbol}:bb_lower
```

### Greeks Keys (DB1)
```
ind:greeks:{symbol}:delta
ind:greeks:{symbol}:gamma
ind:greeks:{symbol}:theta
ind:greeks:{symbol}:vega
ind:greeks:{symbol}:rho
```

### Session Keys (DB1)
```
session:{symbol}:{date}
```

### Analytics Keys (DB2)
```
analytics:time_window:performance:{pattern}:{symbol}:{window}
analytics:alert:timeline:{alert_id}
analytics:pattern:history:{pattern}:{symbol}:timeline
```

---

## 🔌 Integration Points

### Usage in Code

```python
from redis_files.redis_client import RedisClientFactory
from redis_files.redis_key_standards import get_key_builder, RedisKeyStandards

# Get Redis clients
trading_client = RedisClientFactory.get_trading_client(process_name="scanner")
analytics_client = RedisClientFactory.get_analytics_client(process_name="validator")

# Get key builder
builder = get_key_builder()

# Generate keys
rsi_key = builder.live_indicator("BANKNIFTY25NOV57900CE", "rsi")
analytics_key = builder.analytics_time_window_performance("volume_spike", "BANKNIFTY", "5min")

# Parse symbols
canonical = RedisKeyStandards.canonical_symbol("BANKNIFTY25NOV57900CE")
```

---

## ✅ Best Practices

1. **Always use `RedisClientFactory`** for Redis connections
2. **Use canonical keys only** (no symbol variants)
3. **Use `get_key_builder()`** for key generation
4. **Route to correct DB** (DB1 for trading, DB2 for analytics)
5. **Include process_name** for connection pooling

---

## 🔍 Troubleshooting

### Connection Issues
- Check Redis is running
- Verify host/port configuration
- Check process_name for connection pooling

### Key Not Found
- Verify canonical symbol format
- Check database (DB1 vs DB2)
- Verify key builder usage

---

**Last Updated**: November 2025

