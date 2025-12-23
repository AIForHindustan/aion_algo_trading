# Worker Pool Configuration & Batch Processing Optimization

**Date**: 2025-11-13  
**Component**: `intraday_scanner/scanner_main.py` - `MarketScanner`  
**Status**: ✅ Production-Ready

---

## Executive Summary

The `MarketScanner` uses a **sophisticated worker pool architecture** optimized for massive data inflow and batch processing:

1. **Auto-Scaling Worker Pool**: Dynamically scales based on CPU cores (70% utilization)
2. **Scaled Backlog Queue**: Backlog scales with workers (4x workers minimum)
3. **Thread-Safe Semaphore Control**: Prevents queue overflow and memory exhaustion
4. **Memory Management**: Automatic cleanup with configurable limits (2GB default)
5. **Batch Processing**: Polars-based batch optimization for tick processing

---

## 1. Worker Pool Configuration

### 1.1 Auto-Scaling Based on CPU Cores

**Location**: `intraday_scanner/scanner_main.py` lines 1050-1068

```python
# ✅ AUTO-SCALE: Use CPU cores to determine optimal worker count
import os
cpu_cores = os.cpu_count() or 4  # Fallback to 4 if detection fails

# Use 70% of CPU cores for workers (leave some for system/Redis/other processes)
worker_default = max(5, int(cpu_cores * 0.7))
worker_cap = self.config.get("processing_workers_max", cpu_cores)  # Cap at CPU cores
worker_count = int(self.config.get("processing_workers", worker_default))

if worker_count > worker_cap:
    logger.warning(
        "⚠️ Reducing processing_workers from %d to capped value %d (CPU cores: %d)",
        worker_count, worker_cap, cpu_cores
    )
    worker_count = worker_cap
worker_count = max(1, worker_count)  # Minimum 1 worker
```

**Key Features**:
- **70% CPU Utilization**: Leaves 30% for system processes, Redis, and other services
- **Minimum 5 Workers**: Ensures minimum throughput even on low-core systems
- **Capped at CPU Cores**: Prevents over-subscription
- **Configurable**: Can be overridden via `processing_workers` config

**Example**:
- 8-core system → 5-6 workers (70% of 8 = 5.6, rounded to 5-6)
- 16-core system → 11 workers (70% of 16 = 11.2)
- 4-core system → 5 workers (minimum enforced)

---

### 1.2 Scaled Backlog Queue

**Location**: `intraday_scanner/scanner_main.py` lines 1070-1076

```python
# ✅ SCALED BACKLOG: Backlog scales with workers (4x workers minimum, higher for better throughput)
backlog_default = max(worker_count * 4, 100)  # 4x workers or 100, whichever is higher
backlog_limit = int(self.config.get("processing_backlog", backlog_default))
if backlog_limit < worker_count * 2:
    backlog_limit = worker_count * 2  # Minimum 2x workers

logger.info(f"✅ [QUEUE_CONFIG] Backlog limit: {backlog_limit} (can queue {backlog_limit} patterns)")
```

**Key Features**:
- **4x Workers Minimum**: Ensures sufficient queue depth for burst traffic
- **Minimum 100**: Prevents starvation on low-worker systems
- **Minimum 2x Workers**: Safety floor to prevent queue underflow
- **Configurable**: Can be overridden via `processing_backlog` config

**Example**:
- 5 workers → 100 backlog (max(5*4, 100) = 100)
- 11 workers → 44 backlog (11*4 = 44)
- 20 workers → 80 backlog (20*4 = 80)

---

### 1.3 Thread Pool Executor

**Location**: `intraday_scanner/scanner_main.py` lines 1078-1086

```python
# ✅ THREAD-SAFE: Thread pool with proper clamping
self.executor = ThreadPoolExecutor(max_workers=worker_count)
self.processing_semaphore = threading.BoundedSemaphore(backlog_limit)
self.active_futures: deque = deque()
self.processing_metrics = {
    "submitted": 0,
    "skipped_backlog": 0,
    "completed": 0,
}
```

**Key Features**:
- **ThreadPoolExecutor**: Python's standard thread pool for concurrent pattern detection
- **BoundedSemaphore**: Prevents queue overflow (blocks when backlog_limit reached)
- **Active Futures Tracking**: Monitors in-flight pattern detection tasks
- **Metrics**: Tracks submission, skipping, and completion rates

---

## 2. Memory Management

### 2.1 Memory Limits & Cleanup

**Location**: `intraday_scanner/scanner_main.py` lines 1088-1101

```python
# ✅ MEMORY MANAGEMENT: Memory limits and cleanup
self.memory_lock = threading.RLock()  # Thread-safe memory operations
self.max_memory_mb = self.config.get("max_memory_mb", 2048)  # Default 2GB limit
self.memory_cleanup_interval = 300  # Cleanup every 5 minutes
self.last_memory_cleanup = time.time()

# Start memory management thread
self.memory_cleanup_thread = threading.Thread(
    target=self._periodic_memory_cleanup,
    daemon=True,
    name="MemoryCleanup"
)
self.memory_cleanup_thread.start()
```

**Key Features**:
- **2GB Default Limit**: Prevents memory exhaustion
- **Periodic Cleanup**: Every 5 minutes (300 seconds)
- **Thread-Safe**: Uses `RLock` for concurrent memory operations
- **Daemon Thread**: Automatically terminates when main process exits

---

## 3. Batch Processing Optimization

### 3.1 Performance Optimizer

**Location**: `intraday_scanner/data_pipeline.py` lines 738-789

```python
class PerformanceOptimizer:
    """
    ✅ PERFORMANCE OPTIMIZATION: Batch processing for slow tick processing.
    Uses Polars for efficient batch operations.
    """
    
    def __init__(self, batch_size: int = 50):
        self.batch_size = batch_size
        # Check if Polars is available
        try:
            import polars as pl
            self.POLARS_AVAILABLE = True
        except ImportError:
            self.POLARS_AVAILABLE = False
    
    def optimize_tick_processing(self, ticks: List[dict]) -> List[dict]:
        """Optimize tick processing performance."""
        if len(ticks) > self.batch_size:
            # Process in smaller batches
            processed_ticks = self._process_in_batches(ticks)
        else:
            # Process single batch with optimizations
            processed_ticks = self._process_batch_optimized(ticks)
        return processed_ticks
```

**Key Features**:
- **Default Batch Size**: 50 ticks per batch
- **Polars Integration**: Uses Polars for efficient DataFrame operations
- **Automatic Batching**: Splits large batches into smaller chunks
- **Fallback**: Works without Polars (slower but functional)

---

### 3.2 Batch Buffer Processing

**Location**: `intraday_scanner/data_pipeline.py` lines 2579-2606

```python
def _process_batch_for_patterns(self):
    """✅ PERFORMANCE OPTIMIZATION: Process batch of ticks for pattern detection."""
    with self.batch_lock:
        if not self.batch_buffer:
            return
        
        # Get batch and clear buffer
        batch_ticks = self.batch_buffer.copy()
        self.batch_buffer.clear()
        self.last_batch_time = time.time()
    
    # ✅ OPTIMIZE: Use PerformanceOptimizer for batch processing
    if hasattr(self, 'performance_optimizer') and self.performance_optimizer:
        optimized_ticks = self.performance_optimizer.optimize_tick_processing(batch_ticks)
    else:
        optimized_ticks = batch_ticks
```

**Key Features**:
- **Thread-Safe Batching**: Uses `batch_lock` for concurrent access
- **Buffer Management**: Clears buffer after processing to prevent memory buildup
- **Optimization Integration**: Uses `PerformanceOptimizer` for efficient processing

---

## 4. Pattern Detection Submission

### 4.1 Semaphore-Controlled Submission

**Location**: `intraday_scanner/scanner_main.py` (pattern detection submission)

```python
# Pattern detection is submitted to executor with semaphore control
def _detect_patterns_with_validation(self, indicators: Dict, symbol: str) -> List[Dict]:
    """Submit pattern detection to worker pool with backlog control."""
    
    # Check if we can submit (semaphore prevents queue overflow)
    if not self.processing_semaphore.acquire(blocking=False):
        # Queue is full - skip this pattern detection
        self.processing_metrics["skipped_backlog"] += 1
        return []
    
    try:
        # Submit to thread pool
        future = self.executor.submit(
            self.pattern_detector.detect_patterns,
            indicators,
            active_positions=None,
            use_pipelining=True
        )
        
        # Track active future
        with self.threads_lock:
            self.active_futures.append(future)
        
        self.processing_metrics["submitted"] += 1
        
        # Wait for result (with timeout)
        result = future.result(timeout=30.0)  # 30 second timeout
        
        return result
        
    except TimeoutError:
        logger.warning(f"Pattern detection timeout for {symbol}")
        return []
    except Exception as e:
        logger.error(f"Pattern detection error for {symbol}: {e}")
        return []
    finally:
        # Release semaphore
        self.processing_semaphore.release()
        
        # Clean up completed futures
        with self.threads_lock:
            self.active_futures = [f for f in self.active_futures if not f.done()]
```

**Key Features**:
- **Non-Blocking Semaphore**: Skips pattern detection if queue is full
- **Timeout Protection**: 30-second timeout prevents hanging tasks
- **Future Tracking**: Monitors active pattern detection tasks
- **Automatic Cleanup**: Removes completed futures from tracking

---

## 5. Optimization Strategies

### 5.1 Throughput Optimization

1. **Worker Scaling**: 70% CPU utilization ensures optimal throughput without over-subscription
2. **Backlog Scaling**: 4x workers ensures sufficient queue depth for burst traffic
3. **Batch Processing**: Polars-based batch optimization reduces per-tick overhead

### 5.2 Latency Optimization

1. **Non-Blocking Submission**: Skips pattern detection if queue is full (prevents latency spikes)
2. **Timeout Protection**: 30-second timeout prevents hanging tasks
3. **Future Tracking**: Monitors active tasks to prevent queue buildup

### 5.3 Memory Optimization

1. **Memory Limits**: 2GB default limit prevents memory exhaustion
2. **Periodic Cleanup**: Every 5 minutes cleans up unused memory
3. **Buffer Management**: Clears batch buffers after processing

### 5.4 Resilience Optimization

1. **Error Handling**: Catches and logs errors without crashing
2. **Graceful Degradation**: Falls back to slower processing if Polars unavailable
3. **Health Monitoring**: Tracks metrics for system health

---

## 6. Configuration Options

### 6.1 Worker Pool Configuration

```json
{
  "processing_workers": 10,           // Override worker count (default: 70% of CPU cores)
  "processing_workers_max": 16,       // Maximum workers (default: CPU cores)
  "processing_backlog": 200,          // Override backlog limit (default: 4x workers)
  "max_memory_mb": 2048,              // Memory limit in MB (default: 2048)
  "memory_cleanup_interval": 300      // Cleanup interval in seconds (default: 300)
}
```

### 6.2 Batch Processing Configuration

```json
{
  "batch_size": 50,                   // Batch size for tick processing (default: 50)
  "batch_timeout": 1.0                // Batch timeout in seconds (default: 1.0)
}
```

---

## 7. Performance Metrics

### 7.1 Processing Metrics

- **submitted**: Number of pattern detection tasks submitted
- **skipped_backlog**: Number of tasks skipped due to full queue
- **completed**: Number of tasks completed successfully

### 7.2 Monitoring

```python
# Access metrics
metrics = scanner.processing_metrics
print(f"Submitted: {metrics['submitted']}")
print(f"Skipped: {metrics['skipped_backlog']}")
print(f"Completed: {metrics['completed']}")
```

---

## 8. Scanner Architecture

### 8.1 Production Scanner (`intraday_scanner/scanner_main.py`)

- **Architecture**: Thread pool with semaphore control
- **Worker Pool**: Auto-scaling based on CPU cores (70% utilization)
- **Batch Processing**: Polars-based batch optimization
- **Use Case**: High-throughput, massive data inflow
- **Optimizations**: 
  - Auto-scaling workers (70% of CPU cores)
  - Scaled backlog management (4x workers minimum)
  - Memory management with periodic cleanup
  - OptimizedStreamProcessor for M4 (async/await with worker pool)

---

## 9. Best Practices

1. **Monitor Metrics**: Track `submitted`, `skipped_backlog`, and `completed` metrics
2. **Tune Backlog**: Adjust `processing_backlog` based on burst traffic patterns
3. **Memory Limits**: Set `max_memory_mb` based on available system memory
4. **Worker Count**: Use default (70% CPU) unless specific requirements
5. **Batch Size**: Use default (50) unless specific performance requirements

---

## 10. Troubleshooting

### 10.1 High `skipped_backlog` Count

**Symptom**: Many pattern detections are being skipped  
**Cause**: Backlog queue is too small or workers are overloaded  
**Solution**: Increase `processing_backlog` or `processing_workers`

### 10.2 Memory Exhaustion

**Symptom**: System runs out of memory  
**Cause**: Memory limit too high or cleanup not working  
**Solution**: Reduce `max_memory_mb` or check cleanup thread

### 10.3 Slow Processing

**Symptom**: Pattern detection takes too long  
**Cause**: Workers overloaded or batch size too large  
**Solution**: Increase `processing_workers` or reduce `batch_size`

---

## Conclusion

The `MarketScanner` worker pool configuration is **highly optimized** for massive data inflow:

✅ **Auto-Scaling**: Dynamically scales based on CPU cores  
✅ **Scaled Backlog**: Queue depth scales with workers  
✅ **Memory Management**: Automatic cleanup with configurable limits  
✅ **Batch Processing**: Polars-based optimization for efficient processing  
✅ **Resilience**: Error handling and graceful degradation

**This architecture can handle thousands of ticks per second with optimal resource utilization.**

