# Alert Flow Trace - After Validation

## Flow After Pattern Validation

### 1. Pattern Detected
- Pattern detected by `PatternDetector.detect_patterns()`
- Pattern published to `patterns:global` stream
- Pattern passed to `ProductionAlertManager.send_alert()`

### 2. ProductionAlertManager.send_alert() (line 2482)
```
send_alert(pattern)
  ↓
_prepare_alert(pattern)  # Enriches with risk_metrics
  ↓
Filter check: self.filter.should_send_alert(pattern)
  ↓ (if True)
Grouping: self.enhanced_validator.add_alert_to_group(pattern)
  ↓
_send_notifications(pattern)  # Line 2521, 2529, 2536, etc.
```

### 3. _send_notifications() (line 2680)
```
_send_notifications(pattern)
  ↓
send_trading_alert(pattern)  # Line 2695
```

### 4. send_trading_alert() (line 2618)
**CRITICAL CHECK**: Validates if signal is actionable
```python
_is_actionable_signal(signal)  # Line 2643
  - Checks for: ['action', 'entry_price', 'stop_loss', 'target', 'quantity']
  - If missing → Logs "NON-ACTIONABLE" and RETURNS (doesn't send!)
```

**If actionable**:
- Sends to TelegramNotifier
- Sends to RedisNotifier  
- Sends to MacOSNotifier
- Publishes to validator

## Issue: Patterns Not Getting risk_metrics

Patterns need `risk_metrics` to be actionable. The risk_metrics should come from:
1. `UnifiedAlertBuilder.build_algo_alert()` - should be called when pattern is created
2. Or `_prepare_alert()` in alert_manager should enrich it

**Check**: Are patterns being enriched with risk_metrics before reaching `send_trading_alert()`?

## Current Status

From logs:
- ✅ Pattern detected: `order_flow_breakout` for `NSENIFTY25D1626250CE`
- ✅ Filter passed: `Filter returned True`
- ✅ Grouping applied: `action=SEND_IMMEDIATE`
- ✅ `_send_notifications` called
- ❓ `send_trading_alert` - Need to check if it has risk_metrics

## Next Steps

1. Add logging in `send_trading_alert` to show if risk_metrics exist
2. Check if `_prepare_alert` is enriching patterns with risk_metrics
3. Verify patterns are getting `action`, `entry_price`, `stop_loss`, `target`, `quantity`

