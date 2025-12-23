# Kow Signal Straddle Alert Bypass Issue

## Problem Identified

The Kow Signal Straddle alert is **bypassing UnifiedAlertBuilder** and the detailed entry/exit logic because of a **double alert path**:

### Current Flow (BROKEN):

1. **Strategy sends alert** (`kow_signal_straddle.py:1606-1608`):
   - `_send_alert(signal)` → Formats and sends via TelegramNotifier ✅
   - `_store_signal_in_redis(signal)` → Creates algo payload via UnifiedAlertBuilder ✅
   - Returns signal to pattern_detector

2. **Pattern Detector re-processes** (`pattern_detector.py:7246-7265`):
   - Takes the signal from strategy
   - Creates a **NEW simplified pattern dict** (strips most fields)
   - Sends this simplified pattern through pattern_detector's alert pipeline
   - This **bypasses UnifiedAlertBuilder** and doesn't have entry/exit details ❌

### The Issue:

```python
# pattern_detector.py:7246-7265
if signal and isinstance(signal, dict):
    # Convert straddle signal to standard pattern format
    pattern = {
        "symbol": symbol,
        "pattern": "kow_signal_straddle",
        "confidence": signal.get("confidence", 0.85),
        "signal": signal.get("action", "NEUTRAL"),
        # ... ONLY BASIC FIELDS - missing:
        # - stop_loss, target_price (for algo executor)
        # - ce_stop_loss_points, pe_stop_loss_points
        # - ce_target_points, pe_target_points
        # - entry_premium_rupees, stop_loss_rupees, target_rupees
        # - All the detailed entry/exit logic
    }
    patterns.append(pattern)  # This goes through pattern_detector's alert pipeline
```

**Result**: The alert that reaches the user is the simplified one from pattern_detector, **NOT** the detailed one from the strategy with UnifiedAlertBuilder payload.

## Root Cause

1. **Architectural Violation**: Strategy was sending alerts when called from pattern_detector
2. **Pattern Detector Role**: Should be an orchestrator that ONLY validates conditions, not sends alerts
3. **Double Alert Path**: Strategy sent alerts, then pattern_detector created simplified pattern and sent again

## Architectural Fix Applied

### Correct Architecture:
1. **Pattern Detector** = Orchestrator
   - Validates if tick meets pattern conditions
   - Returns patterns (does NOT send alerts)
   
2. **Strategy** = Condition Validator
   - Checks entry/exit conditions
   - Returns signals (does NOT send alerts when called from pattern_detector)
   - Only sends alerts in standalone mode (direct tick listener)

3. **Alert System** = Alert Sender
   - Receives patterns from pattern_detector
   - Uses UnifiedAlertBuilder to create payloads
   - Sends alerts via notifiers

## Fix Applied

### Fix 1: Strategy Only Validates Conditions (Not Send Alerts)
Modified `kow_signal_straddle.py:on_tick()` to accept `send_alert=False` parameter:

```python
async def on_tick(self, tick_data: Dict, send_alert: bool = True) -> Optional[Dict]:
    """
    ✅ ORCHESTRATOR PATTERN: When called from pattern_detector, this method should ONLY
    validate conditions and return signals. The alert system will handle sending alerts.
    """
    # ... validate conditions ...
    signal = await self._generate_straddle_signals(tick_data)
    
    if signal:
        # ✅ ORCHESTRATOR: Only send alerts if explicitly requested (standalone mode)
        if send_alert:
            await self._send_alert(signal)
            await self._store_signal_in_redis(signal)
        else:
            # Return signal to pattern_detector - alert system will handle sending
            pass
    
    return signal
```

### Fix 2: Pattern Detector Calls Strategy with send_alert=False
Modified `pattern_detector.py:_detect_straddle_patterns()` to pass `send_alert=False`:

```python
# ✅ ORCHESTRATOR: Call strategy to VALIDATE CONDITIONS ONLY (not send alerts)
signal = loop.run_until_complete(
    self.straddle_strategy.on_tick(enriched_indicators, send_alert=False)
)
```

### Fix 3: Preserve All Fields from Strategy Signal
Modified `pattern_detector.py` to preserve ALL fields from strategy signal:

```python
if signal and isinstance(signal, dict):
    # ✅ PRESERVE ALL FIELDS from strategy signal (don't strip UnifiedAlertBuilder fields)
    pattern = signal.copy()  # Start with full signal
    pattern.update({
        "symbol": symbol,
        "pattern": pattern.get("pattern", "kow_signal_straddle"),
        "pattern_type": pattern.get("pattern_type", "kow_signal_straddle"),
    })
    patterns.append(pattern)
```

## Verification

After fix, verify:
1. ✅ Alert includes `stop_loss` and `target_price` fields
2. ✅ Alert includes leg-specific stops/targets (`ce_stop_loss_points`, `pe_stop_loss_points`, etc.)
3. ✅ `alert_payload:{symbol}:{pattern}:{timestamp_ms}` key exists in Redis
4. ✅ Payload includes all UnifiedAlertBuilder fields (stop_loss config, target config, position management, etc.)
5. ✅ No duplicate alerts (strategy sends once, pattern_detector doesn't re-send)

## Files to Modify

1. `patterns/pattern_detector.py` - Line 7246-7265 (`_detect_straddle_patterns` method)
2. Test: Run scanner and verify alert payload in Redis

