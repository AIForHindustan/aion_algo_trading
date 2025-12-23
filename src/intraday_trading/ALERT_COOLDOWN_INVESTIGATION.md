# Alert Cooldown Investigation Report

## Issue
Alerts stop after the first run - suspected cooldown period blocking subsequent alerts.

## Investigation Findings

### 1. ✅ Cooldown System Exists

**Location**: `alerts/filters.py`

**Cooldown Periods**:
- **Derivatives** (FUT/CE/PE): **8 seconds** (`cooldown_periods.get('derivative', 8)`)
- **Equity**: **4 seconds** (`cooldown_periods.get('equity', 4)`)
- **High Volatility** (PANIC regime): **12 seconds**

**Cooldown Key Format**: `{symbol}:{pattern}` (e.g., `NSENIFTY25D1625700CE:vix_momentum`)

**Flow**:
1. `_check_cooldowns()` checks if enough time has passed since last alert
2. If cooldown is active → Returns `False` → Alert is **REJECTED**
3. Cooldown is **NOT updated** in `_check_cooldowns()` - it's only checked
4. Cooldown is updated **AFTER alert is sent** via `_mark_alert_sent()` → `_mark_cooldown()`

### 2. ⚠️ CRITICAL ISSUE: `_mark_cooldown()` Method Missing

**Problem**: 
- `alert_manager.py:2828` calls `self.filter._mark_cooldown(pattern)`
- But `_mark_cooldown()` method is **NOT FOUND** in `filters.py`
- This means cooldown tracker is **NEVER UPDATED** after alerts are sent

**Impact**:
- First alert: Cooldown check passes (no entry in tracker) → Alert sent
- Second alert (within 8 seconds): Cooldown check passes (still no entry because `_mark_cooldown()` doesn't exist) → Alert sent
- **BUT**: If there's any other mechanism blocking, alerts would stop

### 3. ⚠️ Alert Grouping May Delay Alerts

**Location**: `alerts/alert_manager.py:422-458`

**Grouping Logic**:
- `add_alert_to_group()` can return:
  - `SEND_IMMEDIATE` - Alert sent immediately ✅
  - `SEND_GROUPED` - Group flushed, summary sent ✅
  - `GROUPED` - Alert grouped but **NOT SENT YET** ⚠️

**Group Flush Conditions** (`should_flush_group()`):
- Group size >= `max_group_size` (50 alerts)
- Time elapsed >= `time_window_seconds` (300 seconds = 5 minutes)
- Group size >= `min_group_size` (2) AND time >= 60 seconds

**Problem**:
- If alert is grouped (`action='GROUPED'`), it's **NOT sent immediately**
- It waits for:
  1. Group to reach `min_group_size` (2 alerts) AND 60 seconds elapsed
  2. OR 5 minutes to pass
  3. OR group to reach 50 alerts

**This could cause alerts to appear "stopped" if**:
- Only 1 alert arrives → Grouped but not flushed (needs 2+ alerts)
- Group never reaches `min_group_size` → Waits 5 minutes before flush
- If scanner restarts → Groups are lost (in-memory only)

### 4. ⚠️ Cooldown Check Happens BEFORE Grouping

**Flow in `send_alert()`**:
```
1. Filter check: self.filter.should_send_alert(pattern)
   ↓ (includes cooldown check)
2. Grouping: self.enhanced_validator.add_alert_to_group(pattern)
   ↓
3. If GROUPED → Alert NOT sent (waits for flush)
```

**Issue**:
- Cooldown is checked in step 1
- If cooldown passes, alert goes to grouping
- If grouping returns `GROUPED`, alert is not sent
- **Cooldown is NOT updated** (because `_mark_cooldown()` doesn't exist)
- Next alert within 8 seconds: Cooldown check passes again (no entry in tracker)
- But if grouping keeps returning `GROUPED`, alerts never get sent

### 5. ⚠️ Missing `_mark_cooldown()` Implementation

**Expected Behavior**:
```python
def _mark_cooldown(self, alert_data: Dict[str, Any]) -> None:
    """Update cooldown tracker after alert is sent."""
    key_symbol = alert_data.get('symbol', '')
    key_pattern = alert_data.get('pattern', alert_data.get('pattern_type', ''))
    cooldown_key = f"{key_symbol}:{key_pattern}".strip(':')
    current_time = time.time()
    
    with self.cooldown_lock:
        self.cooldown_tracker[cooldown_key] = current_time
```

**Current Status**: This method **DOES NOT EXIST** in `filters.py`

### 6. ⚠️ Group Cleanup May Lose Alerts

**Location**: `alerts/alert_manager.py:434-436`

**Cleanup Logic**:
- `_cleanup_stale_groups()` runs every 10 minutes (`_cleanup_interval = 600`)
- If groups are cleaned up, alerts in those groups may be lost

### 7. ⚠️ In-Memory State (Lost on Restart)

**Cooldown Tracker**: `self.cooldown_tracker = {}` (in-memory dict)
**Active Groups**: `self.active_groups = {}` (in-memory dict)

**Impact**:
- If scanner restarts, all cooldown state is lost
- All active groups are lost
- Alerts waiting in groups are lost

## Root Causes Identified

### Primary Issue: Missing `_mark_cooldown()` Method
- Cooldown tracker is never updated
- This means cooldown check always passes (no entries in tracker)
- **BUT**: This should allow alerts to go through, not block them

### Secondary Issue: Alert Grouping Delays
- Alerts can be grouped and not sent immediately
- If only 1 alert arrives, it waits for a second alert or 5 minutes
- This could make it appear that "alerts stopped"

### Tertiary Issue: Group Flush Logic
- Groups need `min_group_size=2` alerts AND 60 seconds elapsed
- OR 5 minutes elapsed
- If scanner restarts, groups are lost

## Recommended Investigation Steps

1. **Check if `_mark_cooldown()` exists** - If not, this is a bug
2. **Check logs for grouping actions** - Look for `GROUPED` vs `SEND_IMMEDIATE`
3. **Check if groups are being flushed** - Look for `SEND_GROUPED` actions
4. **Check if scanner is restarting** - Groups would be lost on restart
5. **Check cooldown debug logs** - Look for "Cooldown active" messages

## Next Steps

1. Add `_mark_cooldown()` method to `filters.py` if missing
2. Add logging to show when alerts are grouped vs sent immediately
3. Consider reducing `time_window_seconds` from 300 to 60 seconds
4. Consider reducing `min_group_size` from 2 to 1 for faster flush
5. Consider persisting groups to Redis to survive restarts

