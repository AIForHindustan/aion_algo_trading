# Pattern Detection System

**Trading pattern detection using technical indicators, volume analysis, and ICT concepts.**

---

## 📋 Overview

The `patterns/` directory contains all pattern detection logic:

1. **Base Detector** - Common pattern detection infrastructure
2. **Pattern Detector** - Main pattern detection orchestrator
3. **Volume Patterns** - Volume spike and breakout detection
4. **ICT Patterns** - ICT killzone, FVG, iron condor
5. **Options Patterns** - Delta analyzer, theta decay
6. **Strategy Patterns** - Kow signal straddle

---

## 🏗️ Architecture

```
Indicator Data (Redis DB1)
    ↓
pattern_detector.py
    ├─ Volume pattern detection
    ├─ Breakout/reversal detection
    ├─ ICT pattern detection
    ├─ Options pattern detection
    └─ Strategy pattern detection
    ↓
Pattern Signals
    ├─ Pattern type
    ├─ Confidence score
    ├─ Direction (BUY/SELL)
    └─ Entry price
    ↓
Alert Manager
```

---

## 📁 Key Files

### `pattern_detector.py`
**Purpose**: Main pattern detection orchestrator

**Key Responsibilities**:
- Reads indicators from Redis (pipelined for performance)
- Detects multiple pattern types:
  - Volume patterns (spike, breakout)
  - Breakout/reversal patterns
  - ICT patterns (killzone, FVG, iron condor)
  - Options patterns (delta reversal)
  - Strategy patterns (Kow straddle)
- Calculates confidence scores (pattern-specific)
- Applies VIX regime adjustments
- Generates pattern signals

**Key Classes**:
- `PatternDetector` - Main detector class

**Key Methods**:
- `detect_patterns()` - Main detection entry point
- `_detect_volume_patterns()` - Volume pattern detection
- `_detect_ict_patterns()` - ICT pattern detection
- `_detect_options_patterns()` - Options pattern detection

**Integration**:
- Uses `EnhancedICTKillzoneDetector` for killzone filtering
- Uses `AssetTypeAwareBaseline` for volume thresholds
- Uses `config/thresholds.py` for confidence thresholds

---

### `base_detector.py`
**Purpose**: Base class for pattern detectors

**Key Responsibilities**:
- Common infrastructure for all detectors
- Redis client management
- Indicator fetching
- Confidence calculation helpers

**Key Classes**:
- `BasePatternDetector` - Base class

---

### `volume_thresholds.py`
**Purpose**: Volume-based threshold calculations

**Key Responsibilities**:
- Calculates volume spike thresholds
- Uses `AssetTypeAwareBaseline` for time-aware baselines
- Asset-type aware adjustments (Equity, Futures, Options)

**Key Functions**:
- `calculate_volume_spike_threshold()` - Spike threshold
- `get_asset_aware_threshold()` - Asset-type aware threshold

---

### `ict/` Subdirectory

#### `killzone.py`
**Purpose**: ICT Killzone detection with volume confirmation

**Key Classes**:
- `EnhancedICTKillzoneDetector` - Enhanced killzone detector

**Features**:
- DST handling for London/New York timezones
- Volume confirmation for institutional activity
- VIX regime awareness
- Cross-timezone verification

#### `ict_iron_condor.py`
**Purpose**: ICT Iron Condor strategy

**Key Classes**:
- `ICTIronCondor` - Iron condor detector

**Requirements**:
- Current Nifty/Bank Nifty price
- Option chain data with premiums
- Recent OHLC data (5-10 days)
- India VIX data
- FVG detection
- ICT Kill Zone timing

#### `fvg.py`
**Purpose**: Fair Value Gap detection

**Key Classes**:
- `EnhancedICTFVGDetector` - FVG detector

---

### `delta_analyzer.py`
**Purpose**: Options delta-based reversal detection

**Key Features**:
- DTE-aware delta adjustments
- PCR (Put-Call Ratio) analysis
- Delta-price divergence detection
- Near-expiry special handling

**Key Classes**:
- `DeltaAnalyzer` - Delta analyzer

---

### `kow_signal_straddle.py`
**Purpose**: Kow Signal Straddle strategy

**Key Features**:
- Entry signal generation
- Exit/reentry signal tracking
- `alert_id` and `strategy_id` generation
- `parent_alert_id` linking for follow-up alerts

**Key Classes**:
- `KowSignalStraddle` - Straddle detector

---

## 🔄 Data Flow

### Pattern Detection Flow

```
1. Read indicators from Redis
   ├─ ind:ta:{symbol}:rsi
   ├─ ind:ta:{symbol}:macd
   ├─ ind:ta:{symbol}:bb_upper/bb_lower
   └─ Volume ratios (from session data)
   ↓
2. Detect patterns
   ├─ Volume patterns (volume_thresholds.py)
   ├─ Breakout/reversal (pattern_mathematics.py)
   ├─ ICT patterns (ict/ subdirectory)
   ├─ Options patterns (delta_analyzer.py)
   └─ Strategy patterns (kow_signal_straddle.py)
   ↓
3. Calculate confidence
   ├─ Pattern-specific confidence
   ├─ VIX regime adjustment
   └─ Volume confirmation (for volume patterns)
   ↓
4. Generate pattern signal
   ├─ Pattern type
   ├─ Confidence score
   ├─ Direction (BUY/SELL)
   ├─ Entry price
   └─ Pattern-specific fields
   ↓
5. Send to alert manager
   └─ alert_manager.send_alert()
```

---

## 📊 Pattern Registry

**File**: `patterns/data/pattern_registry_config.json`

**Structure**:
```json
{
    "enabled_patterns": {
        "volume_spike": {
            "enabled": true,
            "tier": "TIER_1",
            "min_confidence": 0.70
        },
        "ict_iron_condor": {
            "enabled": true,
            "tier": "TIER_1",
            "category": "STRADDLE_STRATEGIES",
            "parent_child_mapping": {
                "entry": ["exit", "reentry"]
            }
        }
    }
}
```

---

## 🔌 Integration Points

### Input
- **Indicators**: Redis DB1 (`ind:ta:{symbol}:{indicator}`)
- **Volume Data**: Redis DB1 (`session:{symbol}:{date}`)
- **VIX Data**: Redis DB1 (via `utils/vix_utils.py`)

### Output
- **Pattern Signals**: To `alerts/alert_manager.py`

### Dependencies
- `config/thresholds.py` - Confidence thresholds
- `dynamic_thresholds/time_aware_volume_baseline.py` - Volume baselines
- `utils/vix_utils.py` - VIX regime classification
- `patterns/utils/pattern_schema.py` - Pattern validation

---

## 🚀 Usage

### Detect Patterns

```python
from intraday_trading.patterns.pattern_detector import PatternDetector

detector = PatternDetector(redis_client=redis_client)

# Detect patterns for a symbol
signals = detector.detect_patterns(symbol="BANKNIFTY25NOV57900CE", tick_data={...})

# Send signals to alert manager
for signal in signals:
    alert_manager.send_alert(signal)
```

### Add New Pattern

1. Create pattern detector class (inherit from `BasePatternDetector`)
2. Add detection logic in `pattern_detector.py`
3. Register in `pattern_registry_config.json`
4. Add template in `alerts/notifiers.py` (`PATTERN_DESCRIPTIONS`)

---

## 📝 Pattern Types

### Volume Patterns
- `volume_spike` - Sudden volume increase
- `volume_breakout` - Volume-confirmed breakout

### Breakout/Reversal Patterns
- `breakout_pattern` - Price breakout
- `reversal_pattern` - Price reversal

### ICT Patterns
- `ict_killzone` - ICT killzone timing
- `ict_iron_condor` - Iron condor strategy
- `ict_fvg` - Fair value gap

### Options Patterns
- `delta_reversal` - Delta-based reversal

### Strategy Patterns
- `kow_signal_straddle` - Kow straddle strategy

---

## ✅ Best Practices

1. **Use asset-aware thresholds** for volume patterns
2. **Apply VIX regime adjustments** to confidence
3. **Include killzone filtering** for ICT patterns
4. **Generate `alert_id`** for all pattern signals
5. **Link follow-up alerts** with `parent_alert_id`

---

## 🔍 Troubleshooting

### Patterns Not Detecting
- Check indicators are stored in Redis
- Verify pattern is enabled in registry
- Check confidence thresholds

### Low Confidence Scores
- Verify VIX regime classification
- Check volume ratios meet thresholds
- Verify pattern-specific requirements

---

**Last Updated**: November 2025

