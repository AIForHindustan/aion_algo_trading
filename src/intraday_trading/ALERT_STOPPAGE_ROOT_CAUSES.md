# Alert Stoppage Root Causes - Investigation Report

## Executive Summary

**Issue**: Alerts stop after the first run

**Root Causes Identified**:
1. ❌ **CRITICAL BUG**: `_mark_cooldown()` method is **MISSING** - cooldown tracker never updated
2. ⚠️ **Alert Grouping**: Alerts can be grouped and delayed up to 5 minutes
3. ⚠️ **Group Flush Requirements**: Groups need 2+ alerts AND 60 seconds OR 5 minutes elapsed
4. ⚠️ **In-Memory State**: Groups and cooldowns lost on scanner restart

---

## 1. ❌ CRITICAL: Missing `_mark_cooldown()` Method

### Location
- **Called from**: `alerts/alert_manager.py:2828` → `self.filter._mark_cooldown(pattern)`
- **Expected in**: `alerts/filters.py`
- **Status**: **METHOD DOES NOT EXIST**

### Impact
- Cooldown tracker (`self.cooldown_tracker`) is **NEVER UPDATED**
- This means:
  - First alert: No entry in tracker → Cooldown check passes → Alert sent
  - Second alert (within 8s): Still no entry (because `_mark_cooldown()` doesn't exist) → Cooldown check passes → Alert sent
  - **BUT**: If there's any other blocking mechanism, alerts would stop

### Expected Implementation
```python
def _mark_cooldown(self, alert_data: Dict[str, Any]) -> None:
    """Update cooldown tracker after alert is sent."""
    key_symbol = alert_data.get('symbol', '')
    key_pattern = alert_data.get('pattern', alert_data.get('pattern_type', ''))
    cooldown_key = f"{key_symbol}:{key_pattern}".strip(':')
    current_time = time.time()
    
    with self.cooldown_lock:
        self.cooldown_tracker[cooldown_key] = current_time
        logger.debug(f"✅ [COOLDOWN] Updated cooldown for {cooldown_key} at {current_time}")
```

---

## 2. ⚠️ Alert Grouping Delays

### Location
- **File**: `alerts/alert_manager.py`
- **Method**: `add_alert_to_group()`
- **Config**: `time_window_seconds: 300` (5 minutes), `min_group_size: 2`

### How It Works
1. Alert arrives → `add_alert_to_group()` called
2. If `should_group_alert()` returns `True`:
   - Alert added to group
   - If group should flush → Returns `SEND_GROUPED` → Alert sent ✅
   - If group NOT ready → Returns `GROUPED` → Alert **NOT SENT** ⚠️

### Group Flush Conditions
Group is flushed (and alerts sent) when:
- Group size >= `max_group_size` (50 alerts) **OR**
- Time elapsed >= `time_window_seconds` (300 seconds = 5 minutes) **OR**
- Group size >= `min_group_size` (2) **AND** time >= 60 seconds

### Problem Scenario
1. First alert arrives → Grouped → `action='GROUPED'` → **NOT SENT**
2. Second alert arrives (within 60 seconds):
   - If same group → Group size = 2, but time < 60s → Still `GROUPED` → **NOT SENT**
   - If different group → New group created → **NOT SENT**
3. Alerts wait for:
   - 60 seconds to pass (if group has 2+ alerts)
   - OR 5 minutes to pass (time window)
   - OR 50 alerts in same group

### Impact
- **Single alerts**: Wait 5 minutes before flush (if no second alert arrives)
- **Multiple alerts**: Wait 60 seconds minimum (if 2+ alerts in group)
- **Appears as "alerts stopped"** because they're grouped but not sent immediately

---

## 3. ⚠️ Cooldown Check Flow

### Current Flow
```
send_alert(pattern)
  ↓
Filter: should_send_alert(pattern)
  ├─ _check_cooldowns() → Checks tracker (always empty because _mark_cooldown() missing)
  ├─ Multi-layer pre-validation
  └─ 6-path qualification
  ↓ (if True)
Grouping: add_alert_to_group(pattern)
  ├─ If should_group_alert() → Returns GROUPED → NOT SENT
  └─ If NOT should_group_alert() → Returns SEND_IMMEDIATE → SENT
  ↓
_mark_alert_sent(pattern)
  └─ Calls filter._mark_cooldown(pattern) → **DOES NOTHING** (method missing)
```

### Issue
- Cooldown check happens **BEFORE** grouping
- If cooldown passes, alert goes to grouping
- If grouping returns `GROUPED`, alert is not sent
- Cooldown is never updated (because `_mark_cooldown()` missing)
- Next alert: Cooldown check passes again, but grouping may still return `GROUPED`

---

## 4. ⚠️ In-Memory State Loss

### State Storage
- **Cooldown Tracker**: `self.cooldown_tracker = {}` (in-memory dict)
- **Active Groups**: `self.active_groups = {}` (in-memory dict)

### Impact on Restart
- If scanner restarts:
  - All cooldown state is **LOST**
  - All active groups are **LOST**
  - Alerts waiting in groups are **LOST**
  - Cooldown tracker is **EMPTY** (allows all alerts through)

---

## 5. ⚠️ Redis Deduplication

### Location
- **File**: `alerts/alert_manager.py:748`
- **Method**: `_check_redis_deduplication()`
- **Called in**: `validate_enhanced_alert()`

### Impact
- If Redis deduplication finds duplicate → Alert **REJECTED**
- This could block alerts if deduplication logic is too aggressive

---

## 6. ⚠️ Group Cleanup

### Location
- **File**: `alerts/alert_manager.py:703-721`
- **Method**: `_cleanup_stale_groups()`
- **Interval**: Every 10 minutes (`_cleanup_interval = 600`)

### Logic
- Groups older than `time_window_seconds * 2` (10 minutes) are flushed
- If group is below `min_group_size`, individual alerts are sent
- **BUT**: If cleanup runs and group is lost, alerts in that group are lost

---

## Root Cause Analysis

### Most Likely Cause: Alert Grouping

**Scenario**:
1. First alert arrives → Pattern detected → Filter passes → **Grouped** (`action='GROUPED'`)
2. Alert is **NOT SENT** (waiting for flush)
3. Second alert arrives (same symbol/pattern):
   - If within 60 seconds → Still grouped → **NOT SENT**
   - If different symbol/pattern → New group → **NOT SENT**
4. Alerts wait for:
   - 60 seconds (if 2+ alerts in group)
   - OR 5 minutes (time window)
5. **Appears as "alerts stopped"** because they're grouped but not visible

### Secondary Cause: Missing `_mark_cooldown()`

- Cooldown tracker never updated
- This means cooldown check always passes (no entries)
- **BUT**: This should allow alerts through, not block them
- However, if there's any other blocking mechanism, alerts would stop

---

## Evidence to Check

### 1. Check Logs for Grouping Actions
Look for these log messages:
- `📦 Alert GROUPED (not sent yet, waiting for flush)`
- `🔍 DEBUG: Grouping result for {symbol}: action=GROUPED`
- `📊 Grouped alert sent` (when group is flushed)

### 2. Check for Cooldown Messages
Look for:
- `Cooldown active for {key}: {remaining}s remaining` (should NOT appear if `_mark_cooldown()` missing)

### 3. Check Group Flush Messages
Look for:
- `Group flush: time window elapsed`
- `Group flush: tier-adjusted min size + time reached`

### 4. Check if Scanner Restarted
- If scanner restarted, all groups would be lost
- Check logs for scanner initialization messages

---

## Recommended Fixes (After Investigation)

1. **Add `_mark_cooldown()` method** to `filters.py`
2. **Reduce `time_window_seconds`** from 300 to 60 seconds
3. **Reduce `min_group_size`** from 2 to 1 (or make it pattern-specific)
4. **Add logging** to show when alerts are grouped vs sent immediately
5. **Consider persisting groups to Redis** to survive restarts
6. **Add group flush on scanner shutdown** to send pending alerts

---

## Next Steps

1. **Check logs** for grouping actions (`GROUPED` vs `SEND_IMMEDIATE`)
2. **Check if groups are being flushed** (look for `SEND_GROUPED` actions)
3. **Verify `_mark_cooldown()` exists** - If not, this is a critical bug
4. **Check if scanner is restarting** - Groups would be lost on restart
5. **Monitor group sizes** - Are alerts accumulating in groups?

