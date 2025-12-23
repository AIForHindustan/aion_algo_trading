# Token Lookup Enriched JSON - Single Source of Truth

## 📍 Centralized Location

**File**: `shared_core/instrument_token/token_lookup_enriched.json`

This is the **SINGLE SOURCE OF TRUTH** for all instrument token lookup data.

## 📊 Source Data

**CSV Source**: `shared_core/instrument_token/instrument_csv_files/zerodha_instruments_latest.csv`

- **Instruments**: 117,471
- **Size**: ~45 MB
- **Last Updated**: Generated from latest CSV

## 🔄 Generation

**Script**: `shared_core/instrument_token/generate_token_lookup_from_csv.py`

```bash
# Generate/update the centralized JSON file
python shared_core/instrument_token/generate_token_lookup_from_csv.py
```

## 📋 JSON Structure

```json
{
  "223969541": {
    "instrument_token": 223969541,
    "exchange_token": "874881",
    "tradingsymbol": "BANKEX25NOVFUT",
    "name": "BANKEX",
    "last_price": 0.0,
    "expiry": "2025-11-27",
    "strike": 0.0,
    "tick_size": 0.05,
    "lot_size": 30,
    "instrument_type": "FUT",
    "segment": "BFO-FUT",
    "exchange": "BFO",
    "key": "BFO:BANKEX25NOVFUT",
    "source": "zerodha_csv"
  }
}
```

### Fields Included

✅ **All CSV fields EXCEPT `created_at`**:
- `instrument_token`
- `exchange_token`
- `tradingsymbol`
- `name`
- `last_price`
- `expiry`
- `strike`
- `tick_size`
- `lot_size`
- `instrument_type`
- `segment`
- `exchange`
- `key` (generated: `exchange:tradingsymbol`)
- `source` (always `"zerodha_csv"`)

❌ **Excluded**: `created_at` (CSV file timestamp, not instrument metadata)

## 🔌 Usage in Code

### Recommended: Use Helper Function

```python
from shared_core.instrument_token import load_token_lookup, get_token_lookup_path

# Load the centralized token lookup
token_lookup = load_token_lookup()

# Get path to file
lookup_path = get_token_lookup_path()
```

### Direct Access

```python
import json
from pathlib import Path

lookup_file = Path(__file__).resolve().parents[2] / "shared_core" / "instrument_token" / "token_lookup_enriched.json"
with open(lookup_file, 'r') as f:
    token_lookup = json.load(f)
```

## 📝 Migration from Old Locations

### Old Locations (Deprecated)
- ❌ `zerodha/core/data/token_lookup_enriched.json`
- ❌ `intraday_trading/core/data/token_lookup_enriched.json`
- ❌ `zerodha/ubuntu_crawlers/core/data/token_lookup_enriched.json` (moved from clickhouse_setup)
- ❌ `crawlers/intraday/token_lookup_enriched.json`

### New Location (Single Source of Truth)
- ✅ `shared_core/instrument_token/token_lookup_enriched.json`

## 🔄 Update Process

When `zerodha_instruments_latest.csv` is updated:

1. **Update CSV**: Copy new CSV to `shared_core/instrument_token/instrument_csv_files/zerodha_instruments_latest.csv`
2. **Regenerate JSON**: Run `python shared_core/instrument_token/generate_token_lookup_from_csv.py`
3. **All scripts automatically use the new data** (no code changes needed)

## ✅ Benefits

1. **Single Source of Truth**: One file, one location
2. **Full Metadata**: All CSV fields included (except `created_at`)
3. **Traceable**: Know exactly which CSV file was used
4. **Centralized**: Easy to find and update
5. **Backward Compatible**: Includes `key` field for existing code

## 📚 Related Files

- **CSV Source**: `shared_core/instrument_token/instrument_csv_files/zerodha_instruments_latest.csv`
- **Generator Script**: `shared_core/instrument_token/generate_token_lookup_from_csv.py`
- **Registry**: `shared_core/instrument_token/instrument_registry.py`
- **Helper Module**: `shared_core/instrument_token/__init__.py`

---

**Last Updated**: November 29, 2025

