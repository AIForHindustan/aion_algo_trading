# Gamma Exposure Analysis for NSENIFTY25D1625700CE

## Issue
- Symbol: `NSENIFTY25D1625700CE`
- Gamma Exposure: Missing (value: 0.0)
- Basic Greeks: 3/5 (missing `gamma` and `vega`)
- Advanced Greeks: 6/6 (all present)

## Root Cause

`gamma_exposure` is **NOT provided by Zerodha websocket data**. It needs to be **calculated** from:
- `gamma` (Greek)
- `underlying_price` (spot price)
- `open_interest` (OI data)
- `contract_multiplier` (lot size × tick value)

## Current Flow

1. **WebSocket Parser** (`zerodha/crawlers/intraday_websocket_parser.py`):
   - `_store_greeks()` extracts Greeks from `tick_data`
   - Only stores what's **already in `tick_data`**
   - If `gamma_exposure` is not in `tick_data`, it won't be stored

2. **Pattern Detector** (`patterns/pattern_detector.py`):
   - `_enrich_indicators_from_redis()` calls `storage.get_greeks(symbol)`
   - `get_greeks()` reads from Redis key: `ind:greeks:{symbol}:gamma_exposure`
   - If key doesn't exist, returns empty dict → `gamma_exposure` is missing

3. **Calculations** (`intraday_scanner/calculations.py`):
   - `_calculate_gamma_exposure()` exists but calculates **underlying-level GEX** (total GEX for all strikes)
   - Does NOT calculate per-option `gamma_exposure`

## Solution

**Option 1: Calculate in WebSocket Parser** (Recommended)
- Calculate `gamma_exposure` when storing Greeks if `gamma` is available
- Formula: `gamma_exposure = gamma * underlying_price^2 * open_interest * contract_multiplier / 100`
- Store it as a Greek so it's available for pattern detection

**Option 2: Calculate in Pattern Detector**
- Calculate `gamma_exposure` in `_enrich_indicators_from_redis()` if missing
- Requires `gamma`, `underlying_price`, and `open_interest` to be available

**Option 3: Calculate in HybridCalculations**
- Add `gamma_exposure` calculation to `batch_calculate_indicators()`
- Ensures it's available for all symbols that need it

## Redis Key Pattern

- **Storage**: `ind:greeks:{canonical_symbol}:gamma_exposure` (DB 1)
- **Retrieval**: `DatabaseAwareKeyBuilder.live_greeks(symbol, 'gamma_exposure')`
- **Current Status**: Key likely doesn't exist for `NSENIFTY25D1625700CE`

## Next Steps

1. Check if `gamma` is available for this symbol (it's missing: 3/5 basic Greeks)
2. If `gamma` is missing, `gamma_exposure` cannot be calculated
3. If `gamma` is available but `gamma_exposure` is missing, add calculation logic
4. Verify `open_interest` is available in tick data or Redis

