# Instrument Config End-to-End Integration

**Date**: 2025-11-30  
**Status**: ✅ **COMPLETE** - Instrument config wired end-to-end

---

## Summary

Successfully wired `config/instrument_config.py` end-to-end across:
1. Pattern detection system (lot sizes, pattern filtering, position sizing, volume baselines)
2. Dashboard backend API (expose configs)
3. Symbol-to-config resolution (weekly vs monthly options)

**All changes maintain backward compatibility** - existing logic continues to work with graceful fallbacks.

---

## Files Created/Modified

### New Files
- `config/instrument_config_manager.py` - Centralized manager for instrument config resolution

### Modified Files
- `patterns/kow_signal_straddle.py` - Lot size retrieval with instrument config support
- `patterns/pattern_detector.py` - Pattern filtering, position sizing, volume baseline support
- `patterns/volume_thresholds.py` - Volume multiplier support
- `aion_trading_dashboard/backend/optimized_main.py` - API endpoint for instrument configs

---

## Integration Points

### 1. Lot Size Retrieval

**Location**: `patterns/kow_signal_straddle.py:676-699`

**Changes**:
- `_get_lot_size()` now accepts optional `indicators` parameter
- **Priority 1**: Instrument config (`instrument_config.py`)
- **Priority 2**: Shared core Greek thresholds (existing)
- **Priority 3**: Config file (existing)
- **Priority 4**: Hardcoded fallback (existing)

**Backward Compatibility**: ✅ All existing call sites continue to work (indicators parameter is optional)

**Example**:
```python
lot_size = self._get_lot_size(underlying, indicators=enhanced_indicators)
```

---

### 2. Pattern Filtering

**Location**: `patterns/pattern_detector.py:4834-4840`

**Changes**:
- `_should_allow_pattern_type()` now checks `enabled_patterns` from instrument config
- Applied to ALL pattern categories:
  - Momentum patterns
  - Breakout/Reversal patterns
  - Volume patterns
  - Microstructure patterns
  - ICT patterns
  - Advanced patterns
  - Straddle patterns
  - Volume profile patterns
  - Option patterns

**Backward Compatibility**: ✅ Defaults to `True` if config not found (patterns enabled by default)

**Example**:
```python
if not self._should_allow_pattern_type(pattern_name, symbol=symbol, indicators=enhanced_indicators):
    continue  # Skip disabled pattern
```

---

### 3. Position Sizing

**Location**: `patterns/pattern_detector.py:1220-1258`

**Changes**:
- `calculate_position_size()` now accepts optional `symbol` and `indicators` parameters
- Gets `risk_per_trade` from instrument config if available
- Falls back to default `0.01` (1%) if config not found

**Backward Compatibility**: ✅ All existing call sites continue to work (new parameters are optional)

**Example**:
```python
position_size = self.calculate_position_size(
    entry_price, stop_loss,
    risk_per_trade=0.01,  # Default, overridden by instrument config if available
    symbol=symbol,
    indicators=indicators
)
```

---

### 4. Volume Baseline Calculation

**Location**: `patterns/pattern_detector.py:4411-4464`

**Changes**:
- `_get_fresh_volume_baseline()` now accepts optional `indicators` parameter
- Supports `baseline_days` from instrument config:
  - Weekly options: 7 days
  - Monthly options: 20 days
  - Commodities: 15 days
- Maps `baseline_days` to appropriate Redis field (`avg_volume_20d`, `avg_volume_55d`)

**Backward Compatibility**: ✅ Defaults to 20d baseline if config not found

**Example**:
```python
baseline_data = self._get_fresh_volume_baseline(symbol, indicators=enhanced_indicators)
avg_volume_baseline = baseline_data.get('avg_volume_baseline') or baseline_data.get('avg_volume_20d', 0)
```

---

### 5. Volume Multiplier

**Location**: `patterns/volume_thresholds.py:55-160`

**Changes**:
- `calculate_volume_spike_threshold()` now accepts optional `indicators` parameter
- Applies `volume_multiplier` from instrument config to threshold calculation
- Multipliers:
  - NIFTY Weekly: 1.0x
  - NIFTY Monthly: 1.2x
  - BANKNIFTY Monthly: 1.5x
  - Commodities: 1.0x

**Backward Compatibility**: ✅ Defaults to 1.0x if config not found

**Example**:
```python
threshold = self.calculate_volume_spike_threshold(
    symbol, bucket_resolution, current_time,
    vix_regime=vix_regime,
    indicators=indicators  # For volume_multiplier lookup
)
```

---

### 6. Dashboard Backend API

**Location**: `aion_trading_dashboard/backend/optimized_main.py:6039-6075`

**New Endpoint**: `GET /api/instruments/config`

**Features**:
- Returns all instrument configs
- Optional `symbol` query parameter for filtering
- Requires authentication (via `get_current_user`)
- Rate limited: 30 requests/minute

**Response Format**:
```json
{
  "configs": [
    {
      "config_key": "NIFTY_WEEKLY",
      "symbol": "NIFTY",
      "instrument_type": "WEEKLY_OPTION",
      "current_lot_size": 75,
      "future_lot_size": 65,
      "baseline_days": 7,
      "volume_multiplier": 1.0,
      "atr_period": 14,
      "enabled_patterns": ["order_flow_breakout", "gamma_exposure_reversal", "kow_straddle"],
      "risk_per_trade": 0.01
    },
    ...
  ],
  "total": 5
}
```

---

## Symbol-to-Config Resolution

### Weekly vs Monthly Options

**Location**: `config/instrument_config_manager.py:47-95`

**Logic**:
1. Extract underlying from symbol (e.g., `NIFTY` from `NIFTY25NOV26000CE`)
2. Check DTE (days to expiry) from indicators:
   - `DTE < 7`: Weekly option → `{UNDERLYING}_WEEKLY`
   - `DTE >= 7`: Monthly option → `{UNDERLYING}_MONTHLY`
3. Fallback to monthly if weekly not found

**Example**:
```python
# Symbol: NIFTY25NOV26000CE
# DTE: 3 days → Weekly option
# Config: NIFTY_WEEKLY (baseline_days=7, lot_size=75)

# Symbol: NIFTY25DEC26000CE
# DTE: 25 days → Monthly option
# Config: NIFTY_MONTHLY (baseline_days=20, lot_size=75)
```

---

## Configuration Structure

### InstrumentConfig Dataclass

```python
@dataclass
class InstrumentConfig:
    symbol: str                    # Base symbol (e.g., "NIFTY")
    instrument_type: str          # WEEKLY_OPTION, MONTHLY_OPTION, COMMODITY
    current_lot_size: int         # Current lot size (2025)
    future_lot_size: int          # Future lot size (2026+)
    baseline_days: int            # Volume baseline days (7, 15, 20)
    volume_multiplier: float      # Volume threshold multiplier
    atr_period: int               # ATR calculation period
    enabled_patterns: List[str]   # List of enabled pattern types
    risk_per_trade: float         # Risk per trade (0.01 = 1%)
```

### Current Configs

| Config Key | Symbol | Type | Lot Size | Baseline Days | Volume Mult | Risk % |
|------------|--------|------|----------|---------------|-------------|--------|
| `NIFTY_WEEKLY` | NIFTY | WEEKLY_OPTION | 75/65 | 7 | 1.0x | 1.0% |
| `NIFTY_MONTHLY` | NIFTY | MONTHLY_OPTION | 75/65 | 20 | 1.2x | 1.5% |
| `BANKNIFTY_MONTHLY` | BANKNIFTY | MONTHLY_OPTION | 35/30 | 20 | 1.5x | 1.2% |
| `GOLD` | GOLD | COMMODITY | 1/1 | 15 | 1.0x | 0.8% |
| `SILVER` | SILVER | COMMODITY | 1/1 | 15 | 1.0x | 0.8% |

---

## Backward Compatibility

### All Changes Are Backward Compatible

1. **Optional Parameters**: All new parameters are optional with sensible defaults
2. **Graceful Fallbacks**: If instrument config not found, system uses existing logic
3. **No Breaking Changes**: Existing call sites continue to work without modification
4. **Default Behavior**: Patterns enabled by default, lot sizes from existing sources

---

## Testing Checklist

- [x] InstrumentConfigManager compiles without errors
- [x] Lot size retrieval works with and without instrument config
- [x] Pattern filtering respects enabled_patterns
- [x] Position sizing uses risk_per_trade from config
- [x] Volume baseline supports baseline_days
- [x] Volume multiplier applied to thresholds
- [x] Dashboard API endpoint added
- [ ] End-to-end testing with live data
- [ ] Frontend component for config editing (TODO)
- [ ] Integration testing with dashboard

---

## Next Steps

### 1. Frontend Component (TODO)

**Location**: `aion_trading_dashboard/frontend/`

**Required**:
- Component to display instrument configs
- Form to edit config values
- Save/update functionality
- Real-time validation

**API Endpoints Needed**:
- `PUT /api/instruments/config/{config_key}` - Update config
- `POST /api/instruments/config` - Create new config

### 2. Config Persistence

**Current**: Configs are hardcoded in `config/instrument_config.py`

**Future**: Store configs in Redis or database for runtime updates

### 3. 2026 Lot Size Migration

**Current**: `future_lot_size` field exists but not used

**Future**: Add date-based logic to switch from `current_lot_size` to `future_lot_size` on 2026-01-01

---

## Key Takeaways

1. ✅ **All integration points wired** - Lot sizes, pattern filtering, position sizing, volume baselines
2. ✅ **Backward compatible** - Existing logic continues to work
3. ✅ **Graceful fallbacks** - System works even if config not found
4. ✅ **Dashboard API added** - Configs exposed via REST endpoint
5. ✅ **Symbol resolution** - Weekly vs monthly options detected automatically
6. ⏳ **Frontend pending** - UI component needed for config editing

---

**Document Version**: 1.0  
**Last Updated**: 2025-11-30  
**Author**: Instrument Config Integration

