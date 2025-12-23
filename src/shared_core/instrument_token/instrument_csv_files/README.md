# Instrument Source Files

This directory contains the **source CSV/JSON files** for instrument data from different brokers.

## Files

### Zerodha
- **`zerodha_instruments_latest.csv`** (12MB, Nov 26, 2025)
  - Source: Consolidated from `zerodha_token_list/zerodha_instruments_20251126_080928.csv`
  - Contains: All Zerodha instruments with tokens, symbols, exchange, expiry, strike, etc.
  - Columns: `instrument_token`, `exchange_token`, `tradingsymbol`, `name`, `last_price`, `expiry`, `strike`, `tick_size`, `lot_size`, `instrument_type`, `segment`, `exchange`, `created_at`
  - Used by: `ZerodhaAdapter` in `shared_core/instrument_token/instrument_registry.py`
  - **Note**: This is the source file that gets processed into `zerodha/core/data/token_lookup_enriched.json`
  - **All Zerodha CSV files consolidated in**: `zerodha_token_list/` (root level + archive/)

### Angel One
- **`angel_one_instruments_master.json`** (28MB, Nov 23, 2025)
  - Source: Root-level `angel_one_instruments_master.json`
  - Contains: All Angel One instruments with tokens and metadata
  - Used by: `AngelOneAdapter` in `shared_core/instrument_token/instrument_registry.py`

- **`angel_one_unmapped_instruments.json`** (42KB, Nov 23, 2025)
  - Source: Root-level `angel_one_unmapped_instruments.json`
  - Contains: Instruments that couldn't be mapped/validated
  - Used for: Debugging and validation

## Purpose

These files serve as the **traceable source** for instrument data:
1. **Source files** (this directory) → Raw broker data
2. **Processed files** (`zerodha/core/data/token_lookup_enriched.json`) → Enriched/processed data
3. **Registry** (`shared_core/instrument_token/instrument_registry.py`) → Unified access layer

## Updating Files

When new instrument dumps are available:
1. Copy the latest CSV/JSON to this directory
2. Update the filename to include date: `zerodha_instruments_YYYYMMDD_HHMMSS.csv`
3. Update `zerodha_instruments_latest.csv` to point to the newest file
4. Regenerate `token_lookup_enriched.json` using the update scripts

## Related Files

- `shared_core/instrument_token/instrument_registry.py` - Unified registry that loads from these sources
- `shared_core/instrument_token/token_lookup_enriched.json` - **SINGLE SOURCE OF TRUTH** - Processed/enriched JSON (generated from CSV)
- `shared_core/instrument_token/generate_token_lookup_from_csv.py` - Script that generates enriched JSON from CSV
- `shared_core/instrument_token/TOKEN_LOOKUP_CENTRALIZED.md` - Documentation for centralized token lookup

## Generating token_lookup_enriched.json

To generate the centralized JSON file from CSV:

```bash
python shared_core/instrument_token/generate_token_lookup_from_csv.py
```

This creates: `shared_core/instrument_token/token_lookup_enriched.json`

**All scripts should use this centralized location.**

