# Scanner Worker Architecture & The Pending Message Problem

## Question: "We have mp why aren't workers properly assigned their tasks?"

The scanner uses **ThreadPoolExecutor** (not multiprocessing), but the real issue is **pending messages blocking the Redis consumer**, not worker assignment.

---

## Architecture Overview

### Data Flow:

```
Redis Stream (ticks:intraday:processed)
    ↓
RobustStreamConsumer (consumer thread)
    ↓ puts ticks into
Queue (tick_queue)
    ↓ main loop reads from
MarketScanner.get_next_tick()
    ↓ submits to
ThreadPoolExecutor (10 workers)
    ↓ workers process
Pattern Detection + Alert Generation
```

### Components:

1. **Consumer Thread** (`RobustStreamConsumer`)
   - Runs in daemon thread
   - Calls `XREADGROUP` to read from Redis stream
   - **BLOCKS** waiting for messages (2000ms timeout)
   - Puts received ticks into `tick_queue`

2. **Main Loop** (`scanner_main.py`)
   - Calls `data_pipeline.get_next_tick()` with 1s timeout
   - Gets tick from `tick_queue`
   - Submits to ThreadPoolExecutor

3. **Worker Pool** (`ThreadPoolExecutor`)
   - 10 worker threads (configurable)
   - Process ticks in parallel
   - Semaphore-based backpressure (200 max queued)

---

## The REAL Problem: Pending Messages

### What Are Pending Messages?

When a consumer reads a message with `XREADGROUP`, Redis marks it as "pending" for that consumer. The message stays pending until:
1. Consumer acknowledges it with `XACK`, OR
2. Consumer is deleted

### Why This Blocks Everything:

The `RobustStreamConsumer` reads messages in this order:

```python
# Step 1: Try to read NEW messages
new_messages = xreadgroup(streams={stream_key: '>'})  # '>' = new only

# Step 2: Only if NO new messages, try PENDING messages
if not new_messages:
    pending_messages = xreadgroup(streams={stream_key: '0'})  # '0' = pending
```

**The Problem:**
- Scanner crashes/restarts → leaves 50 pending messages
- On restart, consumer tries to read NEW messages
- But `XREADGROUP` with `>` **BLOCKS** when there are pending messages for this consumer
- Consumer gets stuck waiting, no data flows
- Main loop sees no ticks → "No data for 50 cycles"

### Why This Happens So Often:

Every scanner restart:
1. Creates new consumer: `scanner_{PID}`
2. Reads 50 messages → marked pending
3. If crash before processing → 50 pending forever
4. Multiple restarts → multiple consumers → hundreds of pending

**From your logs:**
```
scanner_group: 100 pending, 3 consumers
  → scanner_48992: 0 pending, idle 1365s
  → scanner_50219: 50 pending, idle 986s
  → scanner_50766: 50 pending, idle 71s  ← Current one, STUCK!
```

---

## Why Workers Aren't Getting Tasks

It's **NOT** a worker assignment problem. The ThreadPoolExecutor is fine. The issue is:

```
Consumer Thread (BLOCKED waiting for Redis)
    ↓ NO DATA FLOWING
Queue (tick_queue) ← EMPTY
    ↓ NO DATA TO READ
Main Loop → get_next_tick() returns None
    ↓ NO TASKS TO SUBMIT
ThreadPoolExecutor Workers ← IDLE (no work to do)
```

**The workers are idle because the consumer is blocked!**

---

## The Solution

### 1. Enhanced Startup Script

**`start_scanner_clean.sh`** now does:

```bash
# BEFORE starting scanner:
1. Stop old scanner
2. Clean stale consumers (>5 min idle)
3. Acknowledge ALL pending messages
4. Clear Python cache

# AFTER scanner starts (background mode):
5. Wait 30 seconds for initialization
6. Clear any pending messages claimed during startup
7. Verify consumer is healthy (idle < 30s)
```

### 2. Two Startup Modes

```bash
# Foreground (default) - runs in terminal, Ctrl+C to stop
./start_scanner_clean.sh

# Background - runs in background, auto-verifies, exits
./start_scanner_clean.sh --background

# Debug + Background
./start_scanner_clean.sh --debug --background
```

### 3. Background Mode Benefits

When using `--background`:
1. Scanner starts in background (nohup)
2. Script waits 30s for full startup
3. Clears any pending messages from startup
4. Verifies consumer health
5. Exits, leaving scanner running

This ensures **clean startup every time**.

---

## How to Verify Workers Are Working

### 1. Check Consumer State
```bash
python check_redis_stream.py
```

**Good:**
```
scanner_group: 0 pending, 1 consumers
  → scanner_50766: 0 pending, idle 5.2s  ✅ Active!
```

**Bad:**
```
scanner_group: 50 pending, 1 consumers
  → scanner_50766: 50 pending, idle 75s  ❌ Stuck!
```

### 2. Check Worker Activity
```bash
# Watch for pattern detections (workers active)
tail -f logs/scanner_main.log | grep "DETECTED"

# Should see:
✅ [BREAKOUT] NIFTY25NOV25750PE - DETECTED: 0.95 confidence
✅ [MOMENTUM] ICICIBANK25NOVFUT - DETECTED: 0.92 confidence
```

### 3. Check Thread Pool Stats
```bash
# Look for worker metrics in logs
grep "submitted\|completed" logs/scanner_main.log | tail -20

# Should see increasing counts:
submitted: 1250, completed: 1247, backlog: 3
```

---

## Performance Tuning

### If Workers Are Overwhelmed:

**Increase worker count** in `scanner_main.py`:

```python
# Current:
worker_count = min(max_workers, 10)  # Max 10 workers

# Increase to:
worker_count = min(max_workers, 20)  # Max 20 workers
```

### If Queue Is Full:

**Increase backlog limit**:

```python
# Current:
backlog_limit = min(worker_count * 20, 200)  # Max 200 queued

# Increase to:
backlog_limit = min(worker_count * 30, 500)  # Max 500 queued
```

### If Consumer Is Slow:

**Increase batch size**:

```python
# In RobustStreamConsumer (data_pipeline.py line 121):
count=50,  # Current: 50 messages per batch

# Increase to:
count=100,  # 100 messages per batch
```

---

## Monitoring Script

Create `monitor_workers.sh`:

```bash
#!/bin/bash
while true; do
    clear
    echo "=== Scanner Worker Health ==="
    date
    echo ""
    
    # Consumer state
    echo "📊 Consumer State:"
    python check_redis_stream.py | grep -A 5 "scanner_group"
    
    # Recent patterns (worker output)
    echo ""
    echo "🔍 Recent Patterns (last 10):"
    tail -100 logs/scanner_main.log | grep "DETECTED" | tail -10
    
    # Worker metrics
    echo ""
    echo "📈 Worker Metrics:"
    tail -50 logs/scanner_main.log | grep -E "(submitted|completed|backlog)" | tail -5
    
    sleep 10
done
```

Usage:
```bash
chmod +x monitor_workers.sh
./monitor_workers.sh
```

---

## Summary

### The Issue Was NOT:
- ❌ Workers not being assigned tasks
- ❌ ThreadPoolExecutor problem
- ❌ Multiprocessing configuration

### The Issue WAS:
- ✅ **Pending messages blocking Redis consumer**
- ✅ Consumer thread stuck in XREADGROUP
- ✅ No data flowing to main loop
- ✅ Workers idle because no tasks to process

### The Solution:
- ✅ Enhanced startup script with pending cleanup
- ✅ Background mode with post-startup verification
- ✅ Automatic pending message clearing
- ✅ Consumer health monitoring

---

**Use the enhanced startup script and this problem won't occur again!**

```bash
./start_scanner_clean.sh --background
```

Then monitor:
```bash
python check_redis_stream.py  # Every few minutes
tail -f logs/scanner_main.log | grep "DETECTED"  # Continuous
```





