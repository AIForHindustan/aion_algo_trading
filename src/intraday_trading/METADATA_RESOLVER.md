# Metadata Resolver - Complete Documentation

**File:** `crawlers/metadata_resolver.py`  
**Class:** `MetadataResolver`

This document provides comprehensive documentation for the Metadata Resolver, which provides centralized instrument metadata resolution for all crawlers.

---

## Table of Contents

1. [Overview](#overview)
2. [Class Structure](#class-structure)
3. [Initialization](#initialization)
4. [Core Methods](#core-methods)
5. [Usage Examples](#usage-examples)

---

## Overview

The Metadata Resolver provides:

- **Centralized Metadata**: Single source of truth for instrument metadata
- **Token Mapping**: Token -> symbol/metadata resolution
- **Thread-Safe Singleton**: Thread-safe singleton pattern for performance
- **In-Memory Cache**: Caches metadata in memory for fast lookups
- **Fallback Handling**: Provides fallback for unknown tokens

**Key Features:**
- Uses `core/data/token_lookup_enriched.json` (250K+ instruments)
- Thread-safe singleton pattern
- In-memory caching for performance
- Fallback for unknown tokens

---

## Class Structure

### `MetadataResolver`

**Signature:**
```python
class MetadataResolver:
    """
    Centralized metadata resolver for all crawlers.
    
    Provides instrument token to symbol/metadata mapping using the master
    token_lookup.json file from core/data directory.
    """
```

**Singleton Pattern:**
- Thread-safe singleton implementation
- Single instance shared across all crawlers
- Lazy initialization on first access

---

## Initialization

### `__new__(cls)`

**Signature:**
```python
def __new__(cls) -> 'MetadataResolver'
```

**Purpose:** Singleton pattern for thread-safe access

**Code Snippet:**
```python
def __new__(cls):
    """Singleton pattern for thread-safe access"""
    if cls._instance is None:
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
    return cls._instance
```

**Thread Safety:**
- Uses `_lock` for thread-safe initialization
- Double-checked locking pattern
- Returns existing instance if already created

**Returns:** `MetadataResolver` instance (singleton)

---

### `__init__()`

**Signature:**
```python
def __init__(self) -> None
```

**Purpose:** Initialize metadata resolver and load token mapping

**Code Snippet:**
```python
def __init__(self):
    if not hasattr(self, 'token_cache'):
        self.token_cache = {}
        self._load_token_mapping()
```

**Initialization Steps:**
1. Check if already initialized (singleton check)
2. Initialize token cache dictionary
3. Load token mapping from file

**Returns:** `None`

---

### `_load_token_mapping()`

**Signature:**
```python
def _load_token_mapping(self) -> None
```

**Purpose:** Load token mapping from `core/data/token_lookup_enriched.json`

**Code Snippet:**
```python
def _load_token_mapping(self):
    """Load token mapping from core/data/token_lookup_enriched.json"""
    try:
        # Point to the master token_lookup_enriched.json in core/data
        project_root = Path(__file__).parent.parent
        lookup_file = project_root / 'core' / 'data' / 'token_lookup_enriched.json'
        
        if not lookup_file.exists():
            logger.error(f"❌ Token lookup file not found: {lookup_file}")
            return
        
        logger.info(f"📋 Loading token metadata from: {lookup_file}")
        
        with open(lookup_file, 'r') as f:
            token_data = json.load(f)
        
        for token_str, info in token_data.items():
            try:
                token = int(token_str)
                # Handle token_lookup_enriched.json structure
                key_field = info.get("key", "")
                if ":" in key_field:
                    tradingsymbol = key_field.split(":", 1)[1]
                else:
                    tradingsymbol = info.get("name", "")
                
                self.token_cache[token] = {
                    'symbol': tradingsymbol or info.get('name', ''),
                    'exchange': info.get('exchange', 'NSE'),
                    'instrument_type': info.get('instrument_type', 'EQ'),
                    'segment': info.get('segment') or info.get('source', ''),
                    'tradingsymbol': tradingsymbol or info.get('name', ''),
                    'key': key_field,
                    'source': info.get('source', 'Zerodha_API'),
                    'name': info.get('name', '')
                }
            except (ValueError, TypeError):
                # Skip non-integer keys
                continue
        
        logger.info(f"✅ Loaded metadata for {len(self.token_cache)} instruments")
        
    except Exception as e:
        logger.error(f"❌ Error loading token mapping: {e}")
        raise
```

**Data Source:**
- File: `core/data/token_lookup_enriched.json`
- Expected: 250K+ instruments
- Format: `{token: {metadata}}`

**Metadata Structure:**
```python
{
    'symbol': str,              # Trading symbol
    'exchange': str,            # Exchange (NSE, BSE, etc.)
    'instrument_type': str,     # Instrument type (EQ, CE, PE, FUT, etc.)
    'segment': str,             # Segment
    'tradingsymbol': str,       # Trading symbol
    'key': str,                 # Key field (e.g., "NSE:RELIANCE")
    'source': str,              # Source (Zerodha_API)
    'name': str                 # Instrument name
}
```

**Returns:** `None`

---

## Core Methods

### `get_metadata(instrument_token)`

**Signature:**
```python
def get_metadata(self, instrument_token: int) -> Dict[str, Any]
```

**Purpose:** Get metadata for instrument token

**Code Snippet:**
```python
def get_metadata(self, instrument_token: int) -> Dict[str, Any]:
    """
    Get metadata for instrument token
    
    Args:
        instrument_token: Zerodha instrument token
        
    Returns:
        Dict containing symbol, exchange, instrument_type, segment, tradingsymbol
    """
    metadata = self.token_cache.get(instrument_token, {
        'symbol': f'UNKNOWN_{instrument_token}',
        'exchange': 'UNKNOWN',
        'instrument_type': 'UNKNOWN',
        'segment': 'UNKNOWN',
        'tradingsymbol': f'UNKNOWN_{instrument_token}',
        'key': f'UNKNOWN_{instrument_token}',
        'source': 'UNKNOWN'
    })
    
    return metadata
```

**Parameters:**
- `instrument_token`: Zerodha instrument token (int)

**Returns:** Dictionary with metadata (fallback for unknown tokens)

**Example:**
```python
from crawlers.metadata_resolver import metadata_resolver

metadata = metadata_resolver.get_metadata(256265)
# Returns: {
#     'symbol': 'NIFTY25NOV25700CE',
#     'exchange': 'NFO',
#     'instrument_type': 'CE',
#     'segment': 'F&O',
#     'tradingsymbol': 'NIFTY25NOV25700CE',
#     'key': 'NFO:NIFTY25NOV25700CE',
#     'source': 'Zerodha_API',
#     'name': 'NIFTY'
# }
```

---

### `token_to_symbol(instrument_token)`

**Signature:**
```python
def token_to_symbol(self, instrument_token: int) -> str
```

**Purpose:** Convert instrument token to symbol

**Code Snippet:**
```python
def token_to_symbol(self, instrument_token: int) -> str:
    """
    Convert instrument token to symbol
    
    Args:
        instrument_token: Zerodha instrument token
        
    Returns:
        Symbol string
    """
    metadata = self.get_metadata(instrument_token)
    return metadata.get('symbol', f'UNKNOWN_{instrument_token}')
```

**Parameters:**
- `instrument_token`: Zerodha instrument token (int)

**Returns:** Symbol string (e.g., `"NIFTY25NOV25700CE"`)

**Example:**
```python
symbol = metadata_resolver.token_to_symbol(256265)
# Returns: "NIFTY25NOV25700CE"
```

---

### `get_token_metadata(instrument_token)`

**Signature:**
```python
def get_token_metadata(self, instrument_token: int) -> Dict[str, Any]
```

**Purpose:** Get complete token metadata (alias for `get_metadata`)

**Code:**
```python
def get_token_metadata(self, instrument_token: int) -> Dict[str, Any]:
    """Get complete token metadata (alias for get_metadata)"""
    return self.get_metadata(instrument_token)
```

**Returns:** Dictionary with metadata

---

### `get_instrument_info(instrument_token)`

**Signature:**
```python
def get_instrument_info(self, instrument_token: int) -> Dict[str, Any]
```

**Purpose:** Get instrument info (compatible with legacy InstrumentMapper API)

**Code:**
```python
def get_instrument_info(self, instrument_token: int) -> Dict[str, Any]:
    """
    Get instrument info (compatible with legacy InstrumentMapper API)
    
    Returns same structure as InstrumentMapper.get_instrument_info()
    """
    metadata = self.get_metadata(instrument_token)
    return {
        'symbol': metadata.get('symbol', ''),
        'exchange': metadata.get('exchange', ''),
        'instrument_type': metadata.get('instrument_type', ''),
        'segment': metadata.get('segment', ''),
        'tradingsymbol': metadata.get('tradingsymbol', ''),
    }
```

**Returns:** Dictionary with instrument info (compatible format)

---

### `get_stats()`

**Signature:**
```python
def get_stats(self) -> Dict[str, Any]
```

**Purpose:** Get resolver statistics

**Code:**
```python
def get_stats(self) -> Dict[str, Any]:
    """Get resolver statistics"""
    return {
        'total_tokens': len(self.token_cache),
        'cache_size': len(self.token_cache),
    }
```

**Returns:** Dictionary with statistics

---

## Usage Examples

### Basic Usage

```python
from crawlers.metadata_resolver import metadata_resolver

# Get metadata for token
metadata = metadata_resolver.get_metadata(256265)
print(metadata['symbol'])  # "NIFTY25NOV25700CE"

# Convert token to symbol
symbol = metadata_resolver.token_to_symbol(256265)
print(symbol)  # "NIFTY25NOV25700CE"

# Get instrument info
info = metadata_resolver.get_instrument_info(256265)
print(info)  # {'symbol': '...', 'exchange': '...', ...}
```

---

### In Crawler Context

```python
from crawlers.metadata_resolver import metadata_resolver

# In intraday crawler
instrument_info = {}
for token in instruments:
    instrument_info[token] = metadata_resolver.get_instrument_info(token)

# Use in parser
symbol = metadata_resolver.token_to_symbol(instrument_token)
```

---

### Global Instance

**Access Pattern:**
```python
from crawlers.metadata_resolver import metadata_resolver

# metadata_resolver is a global singleton instance
# All crawlers share the same instance
```

**Thread Safety:**
- Singleton pattern ensures thread-safe access
- All crawlers use the same instance
- In-memory cache is shared across threads

---

## Summary

The Metadata Resolver provides:

1. **Centralized Metadata**: Single source of truth for all crawlers
2. **Fast Lookups**: In-memory caching for performance
3. **Thread Safety**: Singleton pattern with thread-safe initialization
4. **Fallback Handling**: Graceful handling of unknown tokens
5. **Compatibility**: Compatible with legacy InstrumentMapper API

**Key Points:**
- Uses `token_lookup_enriched.json` (250K+ instruments)
- Thread-safe singleton pattern
- In-memory cache for fast lookups
- Fallback for unknown tokens (`UNKNOWN_{token}`)

---

**Next:** Read [BULLETPROOF_PARSER.md](./BULLETPROOF_PARSER.md) for parser implementation details.

