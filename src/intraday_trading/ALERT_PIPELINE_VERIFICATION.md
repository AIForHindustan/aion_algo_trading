# Alert Pipeline Verification Report

## 1. ✅ Patterns Published to Redis Streams

### Location: `patterns/pattern_detector.py:5958-5998`
- **Stream**: `patterns:global` (DB 1)
- **Method**: `detect_patterns()` publishes after pattern detection
- **Format**: JSON in `data` field
- **Status**: ✅ **WORKING** - Code exists and logs success

```python
# Publish to patterns:global stream
db1_client.xadd(
    'patterns:global',
    {'data': json.dumps(pattern_data)},
    maxlen=1000  # Keep last 1000 patterns
)
```

**Verification**: 
- ✅ Code publishes to `patterns:global` stream
- ✅ Logs: `✅ Published {len(patterns)} patterns to patterns:global stream for {symbol}`
- ⚠️ **ISSUE**: No consumer found for `patterns:global` - this is for analytics only

---

## 2. ⚠️ Alert Consumers - NOT FOUND

### Search Results:
- **`alerts:stream`**: Used by `alert_validator.py`, `publish_top5_morning_alerts.py`, dashboard
- **Consumer Groups**: Found `alert_news_hook.py` and `alert_trade_executor_service.py` but **NO active consumer for `alerts:stream`**

### Issue:
- Patterns are sent to `alert_manager.send_alert()` directly (not via stream)
- `alerts:stream` is read by validators/dashboards but **no active consumer group found**
- **Patterns bypass stream** - they go directly to `ProductionAlertManager.send_alert()`

**Flow**:
```
PatternDetector.detect_patterns() 
  → patterns:global (analytics only)
  → alert_manager.send_alert() (direct call, NOT via stream)
```

---

## 3. ⚠️ Filtering After Pattern Detection

### Location: `alerts/filters.py:713-790` and `alerts/alert_manager.py:2504-2511`

**Filter Chain**:
1. `ProductionAlertManager.send_alert()` (line 2482)
2. `_prepare_alert()` - enriches with risk_metrics
3. **`self.filter.should_send_alert(pattern)`** ← **FILTERING HAPPENS HERE**
4. If passed → grouping → notifications

### Confidence Threshold Check: `alerts/filters.py:1177-1185`

```python
# Get pattern-specific confidence threshold
min_confidence = get_confidence_threshold(pattern, vix_regime, self.redis_client)
# Fallback: 0.70
if confidence < min_confidence:
    return False  # ❌ REJECTED
```

**Config Values**:
- `scanner.json`: `min_confidence: 0.6` (for pattern detection)
- `scanner.json`: `alerts.min_confidence: 0.70` (for alert filtering)
- `filters.py`: Hardcoded fallback `0.70` if dynamic threshold unavailable

**Issue**: 
- ✅ Filtering **IS happening** after pattern detection
- ⚠️ **Confidence threshold mismatch**: 
  - Patterns detected with `confidence >= 0.6` (scanner.json)
  - But filtered with `confidence >= 0.70` (alerts.min_confidence)
  - **This causes patterns with 0.6-0.69 confidence to be rejected**

---

## 4. ⚠️ Confidence Threshold Issues

### Multiple Threshold Sources:

1. **`configs/scanner.json`**:
   - `min_confidence: 0.6` (pattern detection)
   - `alerts.min_confidence: 0.70` (alert filtering)

2. **`alerts/filters.py:1180`**:
   - Fallback: `0.70` if dynamic threshold unavailable

3. **`alerts/filters.py:1177`**:
   - Dynamic: `get_confidence_threshold(pattern, vix_regime, redis_client)`
   - May return different values based on pattern type and VIX regime

### Problem:
- **Patterns with confidence 0.6-0.69 are detected but filtered out**
- This creates a "detection but no alert" scenario

---

## 5. ✅ Risk Metrics Enrichment

### Location: `patterns/pattern_detector.py:_run_detection_pipeline()`

**Status**: ✅ **WORKING**
- `UnifiedAlertBuilder.build_algo_alert()` is called for each pattern
- Populates `risk_metrics`, `action`, `entry_price`, `stop_loss`, `target`, `quantity`
- Logs: `✅ [RISK_METRICS] {symbol} {pattern_type}: Enriched with risk_metrics`

---

## Summary of Issues

### ✅ Working:
1. Patterns published to `patterns:global` stream (analytics)
2. Risk metrics enrichment in `_run_detection_pipeline`
3. Filter chain is properly wired

### ⚠️ Issues Found:

1. **Confidence Threshold Mismatch**:
   - Patterns detected with `confidence >= 0.6`
   - But filtered with `confidence >= 0.70`
   - **Fix**: Align thresholds or lower filter threshold to 0.6

2. **No Active Consumer for `alerts:stream`**:
   - Patterns go directly to `alert_manager.send_alert()` (not via stream)
   - `alerts:stream` is only read by validators/dashboards
   - **This is actually OK** - direct call is faster than stream

3. **Multi-Layer Pre-Validation**:
   - `filters.py:755` calls `_perform_multi_layer_pre_validation()`
   - This may reject patterns even if confidence is high
   - **Check logs** for "Failed multi-layer pre-validation" messages

---

## Recommendations

1. **Fix Confidence Threshold**:
   ```python
   # In alerts/filters.py:1180, change fallback to match scanner.json
   min_confidence = 0.60  # Match scanner.json min_confidence
   ```

2. **Add Diagnostic Logging**:
   - Log when patterns are rejected due to confidence threshold
   - Log when patterns are rejected due to multi-layer pre-validation

3. **Verify Multi-Layer Pre-Validation**:
   - Check logs for `❌ [FILTER_DEBUG] {symbol} {pattern}: Failed multi-layer pre-validation`
   - This may be the main reason patterns are not getting through

