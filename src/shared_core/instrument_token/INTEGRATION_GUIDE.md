# Instrument Registry Integration Guide

## Overview

The centralized instrument registry provides a unified interface for accessing instrument data across multiple brokers (Zerodha, Angel One, etc.) with conflict resolution, metadata enrichment, and source tracking.

## Architecture

```
Source Files (CSV/JSON)
    ↓
Broker Adapters (ZerodhaAdapter, AngelOneAdapter)
    ↓
UnifiedInstrumentRegistry (conflict resolution, enrichment)
    ↓
EnhancedInstrumentService (broker-specific resolution)
    ↓
Backward-Compatible InstrumentRegistry (for existing code)
```

## Usage Examples

### 1. For Scanner/WebSocket (Zerodha-focused)

```python
from shared_core.instrument_token import get_enhanced_instrument_service

service = get_enhanced_instrument_service()

# Get Zerodha token for NIFTY futures
nifty_fut = service.resolve_for_broker("NFO:NIFTY", "zerodha", "FUT")
print(f"Zerodha token: {nifty_fut['symboltoken']}")
print(f"Unified ID: {nifty_fut['unified_id']}")
print(f"Source file: {nifty_fut['source_file']}")
```

### 2. For Order Execution (Multi-broker)

```python
from shared_core.instrument_token import get_enhanced_instrument_service

def place_order(unified_id: str, broker: str, quantity: int):
    service = get_enhanced_instrument_service()
    unified_inst = service.registry.get_by_unified_id(unified_id)
    
    if not unified_inst:
        raise ValueError(f"Unknown instrument: {unified_id}")
    
    broker_inst = unified_inst.get_broker_instrument(broker)
    if not broker_inst:
        available = list(unified_inst.broker_instruments.keys())
        raise ValueError(f"Instrument not available on {broker}. Available on: {available}")
    
    # Use broker_inst.broker_token for order placement
    if broker == "zerodha":
        zerodha_api.place_order(broker_inst.broker_token, quantity)
    elif broker == "angel_one":
        angel_one_api.place_order(broker_inst.broker_token, quantity)
```

### 3. For Analytics (Cross-broker correlation)

```python
from shared_core.instrument_token import get_enhanced_instrument_service

def get_correlation_instruments(symbols: List[str]) -> List[Dict]:
    service = get_enhanced_instrument_service()
    instruments = []
    
    for symbol in symbols:
        unified_insts = service.registry.resolve_symbol(symbol)
        for unified_inst in unified_insts:
            # Use Zerodha instrument for market data analysis
            zerodha_inst = unified_inst.get_broker_instrument("zerodha")
            if zerodha_inst:
                instruments.append({
                    'unified_id': unified_inst.unified_id,
                    'symbol': unified_inst.canonical_symbol,
                    'zerodha_token': zerodha_inst.broker_token,
                    'sector': unified_inst.sector,
                    'instrument_type': zerodha_inst.instrument_type
                })
    
    return instruments
```

### 4. Backward-Compatible Usage (Existing Code)

```python
from shared_core.instrument_token.instrument_registry import get_enhanced_instrument_service

registry = get_enhanced_instrument_service()
result = registry.resolve_for_broker("RELIANCE", "zerodha")
print(f"Token: {result['symboltoken']}")
```

### 5. Usage within OrderManager (pattern → broker payload)

```python
from shared_core.instrument_token import get_enhanced_instrument_service

registry = get_enhanced_instrument_service()
instrument_details = registry.resolve_for_broker(
    "NIFTY25DEC26200CE",
    "ANGEL_ONE"
)
# Returns:
# {
#   "symboltoken": "54321",
#   "tradingsymbol": "NIFTY25DEC26200CE",
#   "exchange": "NFO",
#   "lot_size": 75,
#   ...
# }
```

## Key Features

### 1. Multi-Broker Support
- Handles different field names and formats across brokers
- Normalizes exchange codes and instrument types
- Maps broker-specific tokens to unified IDs

### 2. Conflict Resolution
- Automatically resolves when one broker token maps to multiple unified IDs
- Prefers unified IDs with more broker support
- Logs warnings for manual review

### 3. Source Tracking
- Tracks which CSV/JSON file each instrument came from
- Enables traceability back to original broker dumps
- Source files stored in `shared_core/instrument_token/instrument_csv_files/`

### 4. Metadata Enrichment
- Canonical names and symbols (most common across brokers)
- Sector and industry data (when available)
- Clean symbol names (removed exchange-specific suffixes)

### 5. Analytics Ready
- Clean, canonical data for correlation analysis
- Cross-broker instrument matching
- Unified IDs for consistent referencing

## File Structure

```
shared_core/instrument_token/
├── instrument_registry.py          # Core registry and service
├── instrument_csv_files/           # Source files (traceable)
│   ├── zerodha_instruments_latest.csv
│   ├── angel_one_instruments_master.json
│   └── angel_one_unmapped_instruments.json
└── __init__.py                      # Exports
```

## Statistics

The registry automatically tracks:
- Total unified instruments
- Broker-specific instrument counts
- Mapping coverage ratio
- Conflict warnings

Access via: `registry.stats`

## Migration Notes

### Existing Code
- Legacy modules should import `get_enhanced_instrument_service()` directly from `shared_core.instrument_token.instrument_registry`.
- The service remains backward-compatible with existing resolver calls.

### New Code
- Use `get_enhanced_instrument_service()` for all new implementations.
- Provides richer metadata and better conflict resolution.
- Supports instrument type filtering out of the box.

## Adding New Brokers

1. Create a new `BrokerAdapter` subclass
2. Implement `load_instruments()`, `normalize_exchange()`, `normalize_instrument_type()`
3. Register in `get_unified_registry()`:
   ```python
   new_broker_adapter = NewBrokerAdapter(path_to_source_file)
   registry.register_broker_adapter("new_broker", new_broker_adapter)
   ```

## Troubleshooting

### Token Not Found
- Check if symbol exists in source CSV/JSON files
- Verify exchange code is correct
- Check instrument type filter (if used)

### Multiple Unified IDs
- Registry automatically resolves conflicts
- Warnings logged for manual review
- Prefers IDs with more broker support

### Missing Metadata
- Sector/industry data requires enrichment step
- Use `registry.enrich_with_sector_data()` if needed
- Canonical names auto-generated from broker data
