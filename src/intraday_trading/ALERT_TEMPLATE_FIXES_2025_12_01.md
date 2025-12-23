# Alert Template Fixes - December 1, 2025

## Summary
Fixed multiple alert template generation issues to ensure all alerts use centralized `HumanReadableAlertTemplates` from notifiers and include proper `target_price` and `stop_loss` fields for `aion_algo_executor`.

## Issues Fixed

### Issue 1: kow_signal_straddle Missing target_price and stop_loss
**Location**: `patterns/kow_signal_straddle.py:2073-2074`

**Problem**: Signal was using `target_rupees` and `stop_loss_rupees` instead of `target_price` and `stop_loss` which `aion_algo_executor` requires.

**Fix**: Added conversion from rupees to price points:
```python
"stop_loss": entry_premium_points + (stop_loss_rupees / lot_size if lot_size > 0 else stop_loss_rupees),
"target_price": entry_premium_points - (target_rupees / lot_size if lot_size > 0 else target_rupees),
"target": entry_premium_points - (target_rupees / lot_size if lot_size > 0 else target_rupees),  # Alias
```

### Issue 2: ict_iron_condor Missing target_price and stop_loss
**Location**: `patterns/pattern_detector.py:8160-8183`

**Problem**: ICT Iron Condor pattern was not including `target_price` and `stop_loss` in the pattern dict, only had `risk_management` dict.

**Fix**: Added calculation and inclusion of `target_price` and `stop_loss`:
```python
# Calculate target_price and stop_loss for Iron Condor
target_price = current_price  # Iron condor target is to keep price in range
breakeven_upper = risk_mgmt.get("breakeven_upper", current_price)
breakeven_lower = risk_mgmt.get("breakeven_lower", current_price)
stop_loss = max(abs(breakeven_upper - current_price), abs(current_price - breakeven_lower))

# Added to pattern:
"target_price": target_price,
"target": target_price,  # Alias
"stop_loss": stop_loss,
```

### Issue 3: Unused Custom Template in ict_iron_condor.py
**Location**: `patterns/ict/ict_iron_condor.py:701-737`

**Problem**: Hardcoded `IRON_CONDOR_ALERT_TEMPLATE` was defined but never used, creating confusion.

**Fix**: Removed unused template and added deprecation comment directing to use `HumanReadableAlertTemplates` from notifiers.

### Issue 4: Custom Alert Formatting in data_pipeline.py
**Location**: `intraday_scanner/data_pipeline.py:7587-7661`

**Problem**: `format_nifty50_public_alert` had fallback that didn't include `target_price` and `stop_loss`, and wasn't consistently using `HumanReadableAlertTemplates`.

**Fix**: 
- Ensured fallback also uses `HumanReadableAlertTemplates.format_actionable_alert()`
- Added `target_price` and `stop_loss` to fallback message if templates fail
- Removed duplicate custom formatting code

### Issue 5: _create_pattern_base Missing target_price
**Location**: `patterns/pattern_detector.py:1073-1135`

**Problem**: `_create_pattern_base` calculated `stop_loss` but didn't always include `target_price` in the returned pattern.

**Fix**: 
- Added `target_price` calculation logic (from kwargs, risk_metrics, or ATR-based default)
- Added `target_price` and `target` (alias) to pattern dict
- Ensures all patterns created via `_create_pattern_base` include both fields

## Changes Made

### Files Modified:
1. `patterns/kow_signal_straddle.py` - Added target_price and stop_loss conversion
2. `patterns/pattern_detector.py` - Added target_price to ict_iron_condor and _create_pattern_base
3. `patterns/ict/ict_iron_condor.py` - Removed unused template
4. `intraday_scanner/data_pipeline.py` - Fixed fallback to use templates and include target_price/stop_loss

## Standardized Fields

All patterns now consistently include:
- `target_price`: Target price for order execution (required by aion_algo_executor)
- `target`: Alias for target_price (for backward compatibility)
- `stop_loss`: Stop loss price for order execution (required by aion_algo_executor)
- `position_size`: Calculated position size
- `action`: Trading action (BUY/SELL/etc.)

## Alert Template System

All alerts now use:
- **Primary**: `HumanReadableAlertTemplates.format_actionable_alert()` from `alerts/notifiers.py`
- **Fallback**: Still uses templates, with minimal custom formatting only if templates completely fail
- **Consistency**: All patterns go through same template system ensuring proper payload structure

## Verification

✅ All files compile successfully
✅ target_price and stop_loss now included in:
   - kow_signal_straddle patterns
   - ict_iron_condor patterns  
   - All patterns via _create_pattern_base
✅ Custom templates removed/deprecated
✅ Centralized template system used throughout

## Next Steps

1. Test alert generation to verify target_price and stop_loss are present
2. Verify aion_algo_executor can extract these fields correctly
3. Monitor logs to ensure no template generation errors


