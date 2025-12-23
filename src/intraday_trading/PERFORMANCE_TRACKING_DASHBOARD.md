# Performance Tracking & Dashboard Infrastructure

**Last Updated**: November 2025  
**Status**: ✅ Active Infrastructure with Dashboard Integration

---

## Table of Contents

1. [Current Infrastructure Overview](#current-infrastructure-overview)
2. [Performance Metrics Collection](#performance-metrics-collection)
3. [Redis Storage Schema](#redis-storage-schema)
4. [Dashboard Integration](#dashboard-integration)
5. [How to Populate Dashboard](#how-to-populate-dashboard)
6. [Enhancement Opportunities](#enhancement-opportunities)
7. [Quick Start Guide](#quick-start-guide)

---

## Current Infrastructure Overview

### Performance Tracking Components

#### 1. **PerformanceMonitor** (`utils/performance_monitor.py`)
**Purpose**: Infrastructure health monitoring (Redis streams, memory, archive)

**Monitors:**
- Redis stream lengths (`ticks:intraday:processed`, `ticks:raw:binary`, `alerts:stream`)
- Consumer group pending messages
- Redis memory usage (used_memory, peak_memory, connected_clients)
- Historical archive buffer size
- Archive statistics (ticks_archived, batches_written, errors)

**Status**: ✅ Active (wired in `scanner_main.py` lines 682-697)

**Output**: Logs to console every 60 seconds (configurable)

#### 2. **PerformanceTracker** (`alert_validation/performance_tracker.py`)
**Purpose**: Alert processing latency tracking (Prometheus metrics)

**Metrics:**
- `alert_latency_seconds` (Histogram) - Alert processing latency
- `alerts_processed_total` (Counter) - Total alerts processed

**Status**: ✅ Available (Prometheus integration ready)

**Output**: Prometheus metrics endpoint (if HTTP server started)

#### 3. **LiveSharpeDashboard** (`intraday_scanner/scanner_main.py`)
**Purpose**: Trading performance monitoring (Sharpe ratios, pattern rankings)

**Monitors:**
- Pattern Sharpe ratios (all 22 patterns)
- Total returns per pattern
- Trade counts per pattern
- Performance rankings (Sharpe rank, return rank, overall rank)

**Status**: ✅ Active (wired in `scanner_main.py` lines 699-715)

**Storage**: Redis DB 3 (`pattern_metrics:{symbol}:{pattern_type}`)

#### 4. **Scanner Performance Stats** (`scanner_main.py` - `_log_performance_stats()`)
**Purpose**: Tick processing performance metrics

**Metrics:**
- Total ticks processed
- Processing times (avg, min, max)
- Throughput (ticks/second)
- Slow ticks count (>1s)
- Error count
- Uptime

**Status**: ✅ Active (logged every 1000 ticks and on shutdown)

**Output**: Logs to console

#### 5. **Alert Validation Dashboard** (`aion_alert_dashboard/alert_dashboard.py`)
**Purpose**: Real-time alert visualization and validation metrics

**Features:**
- Sent alerts visualization
- News enrichment tracking
- Technical indicators display
- Options Greeks display
- Pattern analysis
- Summary statistics

**Status**: ✅ Active (Dash-based web dashboard)

**Access**: http://localhost:53056

---

## Performance Metrics Collection

### Current Metrics Being Collected

#### Infrastructure Metrics (PerformanceMonitor)

**Redis Streams:**
- Stream length (messages in queue)
- Consumer group pending messages
- Consumer count per group

**Redis Memory:**
- Used memory (human-readable)
- Peak memory (human-readable)
- Connected clients count

**Historical Archive:**
- Buffer size (ticks)
- Ticks archived (total)
- Batches written (total)
- Errors count

**Collection Method**: Logged to console every 60 seconds

#### Trading Performance Metrics (LiveSharpeDashboard)

**Pattern Performance:**
- Sharpe ratio per pattern
- Total return per pattern
- Trade count per pattern
- Last trade timestamp

**Storage**: Redis DB 3 (`pattern_metrics:{symbol}:{pattern_type}`)

**Collection Method**: Real-time from Redis DB 3

#### Processing Performance Metrics (Scanner)

**Tick Processing:**
- Total ticks processed
- Average processing time
- Min/Max processing time
- Throughput (ticks/second)
- Slow ticks count (>1s)
- Error count
- Uptime

**Collection Method**: Logged every 1000 ticks and on shutdown

#### Alert Processing Metrics (PerformanceTracker)

**Alert Latency:**
- Processing latency (histogram)
- Total alerts processed (counter)

**Collection Method**: Prometheus metrics (if HTTP server started)

---

## Redis Storage Schema

### Performance Metrics Storage

#### DB 3: Pattern Metrics (Independent Validator DB)

**Key Pattern**: `pattern_metrics:{symbol}:{pattern_type}`

**Storage Format**: Hash

**Fields:**
```json
{
  "sharpe_ratio": "1.25",
  "total_return": "0.15",
  "total_trades": "42",
  "cumulative_return": "0.15",
  "trade_count": "42",
  "last_updated": "2025-11-01T14:30:00",
  "last_trade_time": "2025-11-01T14:25:00"
}
```

**TTL**: 7 days (604,800 seconds)

**Access**: Via `LiveSharpeDashboard.get_pattern_performance_summary()`

**Key Standard**: `RedisKeyStandards.get_pattern_metrics_key(symbol, pattern_type)`

#### DB 1: Validation Results

**Key Pattern**: `validation_results:recent`

**Storage Format**: List (FIFO, max 100 entries)

**Structure**:
```json
{
  "alert_id": "SYMBOL_pattern_timestamp",
  "symbol": "NIFTY25NOV26000CE",
  "alert_type": "ict_killzone",
  "validation_result": {
    "is_valid": true,
    "confidence_score": 0.85,
    "volume_validation": {...},
    "expected_move_validation": {...}
  },
  "processed_at": "2025-11-01T14:30:00"
}
```

**Access**: Via `AlertValidationDashboard.load_validation_results()`

#### DB 0: Forward Validation Results

**Key Pattern**: `forward_validation:results`

**Storage Format**: Hash

**Key Format**: `{alert_id}`

**Value Format**: JSON string
```json
{
  "status": "SUCCESS",
  "success_ratio": 0.75,
  "max_directional_move_pct": 1.5,
  "processed_at": "2025-11-01T14:35:00"
}
```

**Access**: Via `AlertValidationDashboard.get_statistical_model_data()`

#### DB 0: Alert Performance Stats

**Key Pattern**: `alert_performance:stats`

**Storage Format**: Hash/String (JSON)

**Fields** (if implemented):
- Total alerts sent
- Success rate
- Average confidence
- Pattern breakdown

**Status**: ⚠️ Key defined in RedisKeyStandards but may not be actively populated

---

## Dashboard Integration

### Current Dashboards

#### 1. Alert Validation Dashboard (`aion_alert_dashboard/alert_dashboard.py`)

**Technology**: Dash (Plotly)

**Features:**
- Sent alerts visualization
- News enrichment
- Technical indicators
- Options Greeks
- Pattern analysis
- Summary statistics
- **Statistical Model Section** (validation performance)

**Data Sources:**
- Redis DB 0: Alert data (`alert:*`, `alerts:stream`)
- Redis DB 1: OHLC, indicators, Greeks (`ohlc_latest:*`, `indicators:*`, `greeks:*`)
- Redis DB 1: Validation results (`validation_results:recent`)
- Redis DB 0: Forward validation (`forward_validation:results`)

**Access**: http://localhost:53056

**Status**: ✅ Active

#### 2. Performance Monitor (Console Logs)

**Technology**: Logging

**Output**: Console logs every 60 seconds

**Features:**
- Redis stream health
- Memory usage
- Archive buffer status
- Warning detection

**Status**: ✅ Active

---

## How to Populate Dashboard

### Option 1: Enhance Existing Alert Dashboard

#### Add Performance Metrics Section

**Step 1**: Add performance metrics loading method

```python
# In alert_dashboard.py
def load_performance_metrics(self):
    """Load performance metrics from Redis"""
    metrics = {}
    
    # Get scanner performance stats (if stored in Redis)
    # Key: scanner:performance:stats
    stats_key = "scanner:performance:stats"
    stats_data = self.redis_clients[0].get(stats_key)
    if stats_data:
        metrics['scanner'] = json.loads(stats_data)
    
    # Get pattern performance from LiveSharpeDashboard
    # Access via scanner_main.py LiveSharpeDashboard
    # Or directly from Redis DB 3
    pattern_metrics = {}
    pattern_keys = self.redis_clients[3].keys("pattern_metrics:*")
    for key in pattern_keys:
        pattern_data = self.redis_clients[3].hgetall(key)
        if pattern_data:
            # Parse key: pattern_metrics:{symbol}:{pattern_type}
            key_parts = key.split(":")
            if len(key_parts) >= 3:
                symbol = key_parts[-2]
                pattern_type = key_parts[-1]
                pattern_metrics[f"{symbol}:{pattern_type}"] = {
                    'sharpe_ratio': float(pattern_data.get('sharpe_ratio', 0)),
                    'total_return': float(pattern_data.get('total_return', 0)),
                    'total_trades': int(pattern_data.get('total_trades', 0))
                }
    
    metrics['patterns'] = pattern_metrics
    return metrics
```

**Step 2**: Add performance dashboard layout

```python
# Add to dashboard layout
dbc.Row([
    dbc.Col([
        html.H3("Performance Metrics"),
        dcc.Graph(id='performance-metrics-chart'),
        dcc.Interval(
            id='performance-interval',
            interval=60*1000,  # Update every 60 seconds
            n_intervals=0
        )
    ])
])
```

**Step 3**: Add callback for performance updates

```python
@app.callback(
    Output('performance-metrics-chart', 'figure'),
    Input('performance-interval', 'n_intervals')
)
def update_performance_metrics(n):
    metrics = dashboard.load_performance_metrics()
    
    # Create chart with pattern Sharpe ratios
    fig = go.Figure()
    
    # Add Sharpe ratio bars
    patterns = list(metrics['patterns'].keys())
    sharpe_ratios = [metrics['patterns'][p]['sharpe_ratio'] for p in patterns]
    
    fig.add_trace(go.Bar(
        x=patterns,
        y=sharpe_ratios,
        name='Sharpe Ratio'
    ))
    
    fig.update_layout(
        title='Pattern Performance (Sharpe Ratios)',
        xaxis_title='Pattern',
        yaxis_title='Sharpe Ratio'
    )
    
    return fig
```

### Option 2: Store Scanner Performance Stats in Redis

**Current State**: Scanner performance stats are only logged, not stored in Redis

**Enhancement**: Store stats in Redis for dashboard access

```python
# In scanner_main.py _log_performance_stats()
def _log_performance_stats(self, stats, processed_count, final=False):
    """Log performance statistics and store in Redis"""
    # ... existing logging code ...
    
    # ✅ NEW: Store in Redis for dashboard access
    try:
        performance_data = {
            'total_ticks': stats['total_ticks'],
            'avg_processing_time': avg_time,
            'min_processing_time': min_time,
            'max_processing_time': max_time,
            'throughput_ticks_per_second': ticks_per_second,
            'slow_ticks': stats['slow_ticks'],
            'errors': stats['errors'],
            'uptime_seconds': total_time,
            'last_updated': datetime.now().isoformat()
        }
        
        # Store in Redis DB 0
        stats_key = "scanner:performance:stats"
        self.redis_client.set(
            stats_key,
            json.dumps(performance_data),
            ex=3600  # 1 hour TTL
        )
    except Exception as e:
        logger.warning(f"Failed to store performance stats in Redis: {e}")
```

### Option 3: Create Dedicated Performance Dashboard

**New File**: `utils/performance_dashboard.py`

```python
#!/usr/bin/env python3
"""
Performance Metrics Dashboard
Real-time visualization of system performance metrics
"""

import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
from redis_files.redis_manager import RedisManager82
import json
from datetime import datetime

app = dash.Dash(__name__)

class PerformanceDashboard:
    def __init__(self):
        self.redis_clients = {}
        for db in [0, 1, 3]:
            self.redis_clients[db] = RedisManager82.get_client(
                process_name="performance_dashboard",
                db=db,
                max_connections=None
            )
    
    def get_scanner_stats(self):
        """Get scanner performance stats from Redis"""
        stats_key = "scanner:performance:stats"
        stats_data = self.redis_clients[0].get(stats_key)
        if stats_data:
            return json.loads(stats_data)
        return None
    
    def get_pattern_metrics(self):
        """Get pattern performance metrics from Redis DB 3"""
        pattern_keys = self.redis_clients[3].keys("pattern_metrics:*")
        metrics = []
        
        for key in pattern_keys:
            pattern_data = self.redis_clients[3].hgetall(key)
            if pattern_data:
                key_parts = key.split(":")
                if len(key_parts) >= 3:
                    metrics.append({
                        'symbol': key_parts[-2],
                        'pattern_type': key_parts[-1],
                        'sharpe_ratio': float(pattern_data.get('sharpe_ratio', 0)),
                        'total_return': float(pattern_data.get('total_return', 0)),
                        'total_trades': int(pattern_data.get('total_trades', 0))
                    })
        
        return metrics

dashboard = PerformanceDashboard()

app.layout = html.Div([
    html.H1("Performance Metrics Dashboard"),
    
    # Scanner Stats
    html.Div(id='scanner-stats'),
    
    # Pattern Performance
    dcc.Graph(id='pattern-performance-chart'),
    
    # Auto-refresh
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # Update every 60 seconds
        n_intervals=0
    )
])

@app.callback(
    [Output('scanner-stats', 'children'),
     Output('pattern-performance-chart', 'figure')],
    Input('interval-component', 'n_intervals')
)
def update_dashboard(n):
    # Get scanner stats
    scanner_stats = dashboard.get_scanner_stats()
    stats_html = html.Div([
        html.H3("Scanner Performance"),
        html.P(f"Total Ticks: {scanner_stats.get('total_ticks', 0) if scanner_stats else 'N/A'}"),
        html.P(f"Throughput: {scanner_stats.get('throughput_ticks_per_second', 0):.1f} ticks/sec" if scanner_stats else 'N/A'),
        html.P(f"Avg Processing Time: {scanner_stats.get('avg_processing_time', 0):.3f}s" if scanner_stats else 'N/A'),
    ]) if scanner_stats else html.Div("No scanner stats available")
    
    # Get pattern metrics
    pattern_metrics = dashboard.get_pattern_metrics()
    
    # Create chart
    if pattern_metrics:
        patterns = [f"{m['symbol']}:{m['pattern_type']}" for m in pattern_metrics]
        sharpe_ratios = [m['sharpe_ratio'] for m in pattern_metrics]
        
        fig = go.Figure(data=[
            go.Bar(x=patterns, y=sharpe_ratios, name='Sharpe Ratio')
        ])
        fig.update_layout(
            title='Pattern Performance (Sharpe Ratios)',
            xaxis_title='Pattern',
            yaxis_title='Sharpe Ratio'
        )
    else:
        fig = go.Figure()
        fig.add_annotation(
            text="No pattern metrics available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    return stats_html, fig

if __name__ == '__main__':
    app.run_server(debug=True, port=53057)
```

---

## Enhancement Opportunities

### Missing Metrics That Should Be Tracked

#### 1. **Real-Time Processing Metrics**
- Current tick processing rate (per second)
- Queue depth (pending ticks)
- Backlog size
- Worker thread utilization

**Storage**: Redis DB 0 (`scanner:performance:realtime`)

**Update Frequency**: Every 10 seconds

#### 2. **Pattern Detection Metrics**
- Patterns detected per minute
- Pattern confidence distribution
- Pattern rejection reasons
- VIX regime impact on patterns

**Storage**: Redis DB 3 (`pattern_detection:stats`)

**Update Frequency**: Every 60 seconds

#### 3. **Alert Processing Metrics**
- Alerts sent per hour
- Alert success rate (validation)
- Alert latency
- Alert deduplication rate

**Storage**: Redis DB 0 (`alert:performance:stats`)

**Update Frequency**: Every 60 seconds

#### 4. **System Health Metrics**
- Redis connection pool utilization
- Memory usage per component
- CPU usage
- Network I/O

**Storage**: Redis DB 0 (`system:health:metrics`)

**Update Frequency**: Every 30 seconds

### Recommended Enhancements

#### 1. **Store Scanner Stats in Redis**

**File**: `intraday_scanner/scanner_main.py`

**Method**: `_log_performance_stats()`

**Enhancement**: Store stats in Redis for dashboard access

```python
# Add to _log_performance_stats()
stats_key = "scanner:performance:stats"
self.redis_client.set(
    stats_key,
    json.dumps(performance_data),
    ex=3600  # 1 hour TTL
)
```

#### 2. **Add Real-Time Metrics Collection**

**New Method**: `_collect_realtime_metrics()`

**Frequency**: Every 10 seconds

**Metrics**:
- Current processing rate
- Queue depth
- Backlog size
- Active workers

#### 3. **Add Pattern Detection Metrics**

**New Method**: `_collect_pattern_metrics()`

**Frequency**: Every 60 seconds

**Metrics**:
- Patterns detected count
- Average confidence
- Rejection reasons breakdown
- VIX regime distribution

#### 4. **Integrate Prometheus Metrics**

**File**: `alert_validation/performance_tracker.py`

**Enhancement**: Start HTTP server for Prometheus scraping

```python
# Start Prometheus HTTP server
start_http_server(8000)  # Metrics available at http://localhost:8000/metrics
```

#### 5. **Create Unified Performance Dashboard**

**New File**: `utils/performance_dashboard.py`

**Features**:
- Real-time scanner stats
- Pattern performance charts
- Alert processing metrics
- System health indicators
- Historical trends

---

## Quick Start Guide

### View Current Performance Metrics

#### 1. **Console Logs (PerformanceMonitor)**
```bash
# PerformanceMonitor logs every 60 seconds
# Check scanner logs for performance output
tail -f logs/scanner_detailed.log | grep "Performance Monitor"
```

#### 2. **Pattern Performance (LiveSharpeDashboard)**
```python
# In Python
from intraday_scanner.scanner_main import MarketScanner

scanner = MarketScanner(config)
scanner.start()

# Get pattern performance summary
summary = scanner.get_sharpe_performance_summary()
print(summary)
```

#### 3. **Alert Dashboard**
```bash
# Start alert dashboard
python aion_alert_dashboard/alert_dashboard.py

# Access at http://localhost:53056
```

#### 4. **Redis Direct Access**
```bash
# Check pattern metrics
redis-cli -n 3 KEYS "pattern_metrics:*"
redis-cli -n 3 HGETALL "pattern_metrics:NIFTY:volume_breakout"

# Check validation results
redis-cli -n 1 LLEN "validation_results:recent"
redis-cli -n 1 LRANGE "validation_results:recent" 0 -1
```

### Populate Dashboard with Current Data

#### Step 1: Ensure Metrics Are Being Collected

**Check PerformanceMonitor**:
```python
# In scanner_main.py, verify PerformanceMonitor is started
# Lines 682-697 should show: "✓ Performance Monitor started"
```

**Check LiveSharpeDashboard**:
```python
# In scanner_main.py, verify LiveSharpeDashboard is initialized
# Lines 699-715 should show: "✓ LiveSharpeDashboard initialized"
```

#### Step 2: Access Pattern Metrics

**Via Python**:
```python
from redis_files.redis_manager import RedisManager82
import json

redis_db3 = RedisManager82.get_client(process_name="metrics_check", db=3)

# Get all pattern metrics
pattern_keys = redis_db3.keys("pattern_metrics:*")
for key in pattern_keys:
    metrics = redis_db3.hgetall(key)
    print(f"{key}: {metrics}")
```

**Via Redis CLI**:
```bash
redis-cli -n 3 KEYS "pattern_metrics:*"
redis-cli -n 3 HGETALL "pattern_metrics:NIFTY:volume_breakout"
```

#### Step 3: Enhance Dashboard (Optional)

**Add Performance Section to Alert Dashboard**:
1. Add `load_performance_metrics()` method (see Option 1 above)
2. Add performance layout section
3. Add callback for auto-refresh

**Or Create Dedicated Dashboard**:
1. Create `utils/performance_dashboard.py` (see Option 3 above)
2. Run: `python utils/performance_dashboard.py`
3. Access at http://localhost:53057

---

## Summary

### Current Infrastructure ✅

- **PerformanceMonitor**: Infrastructure health (Redis, streams, archive)
- **LiveSharpeDashboard**: Trading performance (Sharpe ratios, pattern rankings)
- **PerformanceTracker**: Alert latency (Prometheus metrics)
- **Scanner Stats**: Tick processing metrics (logged)
- **Alert Dashboard**: Alert visualization with validation metrics

### Storage Locations

- **DB 3**: Pattern metrics (`pattern_metrics:{symbol}:{pattern_type}`)
- **DB 1**: Validation results (`validation_results:recent`)
- **DB 0**: Forward validation (`forward_validation:results`)
- **Console Logs**: PerformanceMonitor output, Scanner stats

### Dashboard Access

- **Alert Dashboard**: http://localhost:53056 (active)
- **Performance Dashboard**: Not yet created (see Option 3 above)

### Next Steps

1. **Store Scanner Stats in Redis** (see Enhancement #1)
2. **Add Performance Section to Alert Dashboard** (see Option 1)
3. **Create Dedicated Performance Dashboard** (see Option 3)
4. **Add Real-Time Metrics Collection** (see Enhancement #2)
5. **Integrate Prometheus** (see Enhancement #4)

---

**Remember**: Current metrics are being collected but not all are stored in Redis for dashboard access. Follow the enhancement steps above to populate a dashboard with real-time performance data.

