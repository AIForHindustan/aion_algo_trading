# Pattern Detection Analysis - Why Other Patterns Are Not Triggering

**Date**: 2025-12-01  
**Status**: Analysis Complete

---

## Executive Summary

From scanner logs analysis, **only Kow Signal Straddle is triggering**. All other patterns are either:
1. **Failing condition checks** (option patterns)
2. **Not being called** (advanced patterns for equity/futures)
3. **Returning None silently** (no error logs)

---

## 1. Option Patterns Status

### Patterns Being Checked:
1. ✅ **`kow_signal_straddle`** - **TRIGGERING** (confidence: 0.95)
2. ❌ **`delta_hedge_opportunity`** - **FAILING**
   - **Reason**: Gamma too low (0.0004 < 0.1 threshold)
   - **Reason**: Delta not in range (0.2658 not in 0.4-0.6 range)
   - **Log**: `🔍 [DELTA_HEDGE] NFOBANKNIFTY30DEC59700PE - Values: delta=-0.2658 (abs=0.2658), gamma=0.0004 (abs=0.0004) | Gamma check: 0.0004 > 0.1 = False | Delta check: 0.4 <= 0.2658 <= 0.6 = False`

3. ❌ **`gamma_squeeze`** - **FAILING**
   - **Reason**: Gamma too low (0.0004 < 0.15 threshold)
   - **Reason**: Vega too high (47.26 > 0.1, should be < 0.1)
   - **Log**: `🔍 [GAMMA_SQUEEZE] NFOBANKNIFTY30DEC59700PE - Values: gamma=0.0004 (abs=0.0004), vega=47.2642 (abs=47.2642) | Gamma check: 0.0004 > 0.15 = False | Vega check: 47.2642 < 0.1 = False`

4. ❌ **`theta_decay_alert`** - **FAILING**
   - **Reason**: IV too low (0.0609 < 0.1500 threshold)
   - **Log**: `🔍 [THETA_DECAY] NFOBANKNIFTY30DEC59700PE - Rejected: IV condition failed | IV=0.0609 (threshold: >0.1500)`

### Root Cause for Option Patterns:
- **Greeks values are too low** for current market conditions
- **IV is too low** (0.06 vs 0.15 threshold) - market is in low volatility regime
- **Thresholds may be too strict** for current market conditions

---

## 2. Advanced Patterns Status (Equity/Futures)

### Patterns That Should Be Running:
1. **`order_flow_breakout`** - No logs found
2. **`gamma_exposure_reversal`** - No logs found
3. **`microstructure_divergence`** - No logs found
4. **`cross_asset_arbitrage`** - No logs found
5. **`vix_momentum`** - No logs found
6. **`ict_iron_condor`** - No logs found
7. **`game_theory_signal`** - No logs found

### Root Cause Analysis:

#### Issue 1: Only Options Are Being Streamed
- **Evidence**: All symbols in logs are options (NFOBANKNIFTY30DEC59700PE, NFONIFTY30DEC26600CE, etc.)
- **No equity/futures symbols** in logs
- **Conclusion**: Advanced patterns may not be running because **only options are being streamed by the crawler**

#### Issue 2: Advanced Patterns May Be Returning None Silently
- **Code Location**: `patterns/pattern_detector.py:6306` - `_detect_advanced_patterns_with_data()`
- **Called at**: Line 5875 in `_detect_patterns_with_pipelining()`
- **No error logs** for advanced patterns
- **No debug logs** showing they're being called
- **Conclusion**: Patterns may be returning `None` or empty list without logging

#### Issue 3: Volume Requirements May Be Filtering Patterns
- **Code Location**: `patterns/pattern_detector.py:5884-5892`
- **Logic**: Volume-dependent patterns require `volume_ratio >= required_volume`
- **Current volume_ratio**: Very low (0.17x, 0.12x, 0.06x for options)
- **Conclusion**: If advanced patterns are volume-dependent, they're being filtered out

---

## 3. Data Available in Indicators

From logs, indicators include:
- ✅ **Greeks**: delta, gamma, theta, vega, rho, dte_years, trading_dte
- ✅ **TA Indicators**: rsi, atr, vwap, ema_5, ema_10, ema_20, ema_50, ema_100, ema_200, macd, signal, histogram
- ✅ **Microstructure**: microprice, order_flow_imbalance, order_book_imbalance, spread_absolute, spread_bps, depth_imbalance, cumulative_volume_delta
- ✅ **Volume**: volume_ratio (but very low: 0.17x, 0.12x, 0.06x)
- ✅ **VIX**: vix_value (11.58), vix_regime (LOW)
- ✅ **Price Data**: last_price, price_change, upper, middle, lower (Bollinger Bands)

**All required data appears to be available** - the issue is likely:
1. **Condition thresholds too strict**
2. **Volume requirements not met**
3. **Patterns returning None silently**

---

## 4. Recommendations

### Immediate Actions:

1. **Add Debug Logging for Advanced Patterns**
   - Add logging in `_detect_advanced_patterns_with_data()` to show when patterns are called
   - Log when patterns return None and why
   - Log volume ratio checks

2. **Check Instrument Config**
   - Verify `enabled_patterns` in instrument config
   - Check if patterns are disabled for options vs equity/futures

3. **Review Threshold Values**
   - **Option patterns**: Gamma thresholds (0.1, 0.15) may be too high for current market
   - **IV threshold** (0.15) may be too high when VIX is 11.58
   - **Vega threshold** (< 0.1) seems incorrect - vega of 47 is normal for options

4. **Check Volume Requirements**
   - Current volume_ratio is very low (0.17x, 0.12x, 0.06x)
   - If advanced patterns require volume_ratio >= 1.0x, they won't trigger
   - Review `get_pattern_volume_requirement()` for each pattern

5. **Verify Symbol Universe**
   - Check if crawler is streaming equity/futures symbols
   - If only options are streamed, advanced patterns won't run (they're designed for equity/futures)

### Code Changes Needed:

1. **Add logging in `_detect_advanced_patterns_with_data()`**:
```python
def _detect_advanced_patterns_with_data(self, enhanced_indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
    patterns = []
    symbol = enhanced_indicators.get("symbol", "UNKNOWN")
    
    # ✅ ADD: Log when method is called
    self.logger.info(f"🔍 [ADVANCED_PATTERNS] {symbol} - Starting advanced pattern detection")
    
    # ... existing code ...
    
    # ✅ ADD: Log when patterns return None
    if self.order_flow_detector:
        try:
            pattern = self.order_flow_detector.detect(symbol, enhanced_indicators)
            if pattern:
                patterns.append(pattern)
                self.logger.info(f"✅ [ADVANCED_PATTERNS] {symbol} - order_flow_breakout detected")
            else:
                self.logger.debug(f"❌ [ADVANCED_PATTERNS] {symbol} - order_flow_breakout returned None")
        except Exception as e:
            self.logger.warning(f"⚠️ [ADVANCED_PATTERNS] {symbol} - order_flow_breakout error: {e}")
```

2. **Add logging for volume filtering**:
```python
# In _detect_patterns_with_pipelining(), around line 5884
if pattern_name and is_volume_dependent_pattern(pattern_name):
    required_volume = get_pattern_volume_requirement(pattern_name, redis_client=self.redis_client)
    self.logger.debug(f"🔍 [VOLUME_FILTER] {symbol} - {pattern_name} requires volume_ratio >= {required_volume}, current={volume_ratio:.2f}")
    if volume_ratio >= required_volume:
        # ... existing code ...
    else:
        self.logger.debug(f"❌ [VOLUME_FILTER] {symbol} - {pattern_name} filtered out (volume_ratio {volume_ratio:.2f} < {required_volume})")
```

3. **Review threshold values** in `shared_core/config_utils/thresholds.py`:
   - `delta_hedge_opportunity`: gamma threshold (0.1), delta range (0.4-0.6)
   - `gamma_squeeze`: gamma threshold (0.15), vega threshold (< 0.1) - **This seems wrong**
   - `theta_decay_alert`: IV threshold (0.15)

---

## 5. Next Steps

1. **Add debug logging** to see why advanced patterns aren't triggering
2. **Check crawler configuration** to see if equity/futures are being streamed
3. **Review threshold values** - may need to adjust for current market conditions
4. **Check volume requirements** - may be too strict for current market
5. **Verify instrument config** - ensure patterns are enabled for the symbols being streamed

---

## 6. Summary

**Current Status**:
- ✅ Kow Signal Straddle: **Working** (triggering correctly)
- ❌ Other Option Patterns: **Failing** (thresholds too strict)
- ❌ Advanced Patterns: **Not triggering** (likely volume filtering or not being called)

**Primary Issues**:
1. **Only options are being streamed** (no equity/futures)
2. **Volume ratios are very low** (0.17x, 0.12x, 0.06x)
3. **Thresholds may be too strict** for current market conditions
4. **No debug logging** for advanced patterns (can't see why they're not triggering)

**Immediate Fix**: Add comprehensive debug logging to understand why patterns aren't triggering.









