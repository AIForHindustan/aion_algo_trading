# 🏗️ COMPLETE CLICKHOUSE ARCHITECTURE

## System Overview

```
[Zerodha WS] → [Redis Streams] → [ClickHouse] → [Analysis Tools]

     ↓              ↓               ↓              ↓

  Binary        Buffer &        Analytics      Grafana,
  Data          Enrichment      Database       Python
```

## Architecture Flow

### 1. **Zerodha WebSocket (WS)**
   - Source of real-time market data
   - Receives binary tick data from Zerodha Kite Connect
   - Streams data to Redis for buffering

### 2. **Redis Streams**
   - Acts as a high-performance buffer
   - Handles data enrichment and transformation
   - Provides backpressure handling during high-volume periods
   - Enables reliable data delivery to ClickHouse

### 3. **ClickHouse**
   - Columnar analytics database
   - Stores time-series tick data efficiently
   - Enables fast analytical queries on large datasets
   - Supports real-time aggregations and window functions

### 4. **Analysis Tools**
   - **Grafana**: Real-time dashboards and visualizations
   - **Python**: Custom analysis scripts, backtesting, and ML models
   - **Other Tools**: Custom trading algorithms and pattern recognition

---

## ClickHouse Installation

### macOS Installation (via Homebrew)

```bash
brew install clickhouse
```

### Verify Installation

```bash
clickhouse --version
```

### Start ClickHouse Server

```bash
# Start ClickHouse server (default port: 8123 for HTTP, 9000 for native)
clickhouse server

# Or run in background
clickhouse server --daemonize
```

### Start ClickHouse Client

```bash
clickhouse client
```

---

## Configuration

### Default Configuration Location

- **macOS (Homebrew)**: `/opt/homebrew/etc/clickhouse-server/`
- **Config file**: `config.xml`
- **Users file**: `users.xml`

### Key Configuration Settings

#### Server Configuration (`config.xml`)

```xml
<!-- Listen on all interfaces -->
<listen_host>0.0.0.0</listen_host>

<!-- HTTP port -->
<http_port>8123</http_port>

<!-- Native protocol port -->
<listen_port>9000</listen_port>

<!-- Data directory -->
<path>/opt/homebrew/var/lib/clickhouse/</path>

<!-- Temporary files -->
<tmp_path>/opt/homebrew/var/lib/clickhouse/tmp/</tmp_path>
```

#### User Configuration (`users.xml`)

```xml
<users>
    <default>
        <password></password>
        <networks>
            <ip>::/0</ip>
        </networks>
        <profile>default</profile>
        <quota>default</quota>
    </default>
</users>
```

---

## Data Schema Design

### 1. ClickHouse Table Structure

#### Main Tick Data Table (Optimized for Real-time)

```sql
-- Main tick data table (optimized for real-time)
CREATE TABLE tick_data (
    instrument_token UInt32,
    symbol String,
    exchange String,
    timestamp DateTime64(3, 'Asia/Kolkata'),
    exchange_timestamp DateTime64(3, 'Asia/Kolkata'),
    
    -- Price data
    last_price Float64,
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    average_traded_price Float64,
    
    -- Volume and OI
    volume UInt64,
    last_traded_quantity UInt32,
    total_buy_quantity UInt64,
    total_sell_quantity UInt64,
    open_interest UInt64,
    oi_day_high UInt64,
    oi_day_low UInt64,
    
    -- Market depth (L2/L3 data)
    bid_1_price Float64, bid_1_quantity UInt32, bid_1_orders UInt16,
    bid_2_price Float64, bid_2_quantity UInt32, bid_2_orders UInt16,
    bid_3_price Float64, bid_3_quantity UInt32, bid_3_orders UInt16,
    bid_4_price Float64, bid_4_quantity UInt32, bid_4_orders UInt16,
    bid_5_price Float64, bid_5_quantity UInt32, bid_5_orders UInt16,
    
    ask_1_price Float64, ask_1_quantity UInt32, ask_1_orders UInt16,
    ask_2_price Float64, ask_2_quantity UInt32, ask_2_orders UInt16,
    ask_3_price Float64, ask_3_quantity UInt32, ask_3_orders UInt16,
    ask_4_price Float64, ask_4_quantity UInt32, ask_4_orders UInt16,
    ask_5_price Float64, ask_5_quantity UInt32, ask_5_orders UInt16,
    
    -- Metadata
    packet_type String,
    session_type Enum8('pre_market' = 1, 'regular' = 2, 'post_market' = 3),
    data_quality Enum8('raw' = 1, 'enriched' = 2, 'corrected' = 3),
    
    -- Processing info
    processed_at DateTime64(3, 'UTC') DEFAULT now(),
    batch_id UUID,
    
    -- Derived fields for partitioning
    date Date DEFAULT toDate(timestamp),
    hour UInt8 DEFAULT toHour(timestamp)
) 
ENGINE = MergeTree()
PARTITION BY (date, hour)
ORDER BY (instrument_token, timestamp, exchange_timestamp)
SETTINGS index_granularity = 8192;
```

#### News Data Table

```sql
-- News data table
CREATE TABLE news_data (
    id String,
    source String,
    title String,
    content String,
    link String,
    published_at DateTime64(3, 'Asia/Kolkata'),
    symbols Array(String), -- Affected symbols
    sentiment Float32, -- -1 to +1
    sentiment_confidence Float32,
    categories Array(String),
    
    date Date DEFAULT toDate(published_at),
    processed_at DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY date
ORDER BY (published_at, source, id);
```

#### Indices Data Table

```sql
-- Indices data table
CREATE TABLE indices_data (
    instrument_token UInt32,
    symbol String,
    timestamp DateTime64(3, 'Asia/Kolkata'),
    last_price Float64,
    net_change Float64,
    percent_change Float64,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume UInt64,
    
    date Date DEFAULT toDate(timestamp),
    hour UInt8 DEFAULT toHour(timestamp)
)
ENGINE = MergeTree()
PARTITION BY (date, hour)
ORDER BY (symbol, timestamp);
```

#### Real-time Materialized Views for Common Queries

```sql
-- Real-time materialized views for common queries
CREATE MATERIALIZED VIEW tick_data_1min
ENGINE = AggregatingMergeTree()
PARTITION BY date
ORDER BY (instrument_token, timestamp)
AS SELECT
    instrument_token,
    toStartOfMinute(timestamp) as timestamp,
    date,
    argMax(last_price, exchange_timestamp) as last_price,
    min(last_price) as low,
    max(last_price) as high,
    sum(volume) as volume,
    argMax(open_interest, exchange_timestamp) as open_interest
FROM tick_data
GROUP BY instrument_token, timestamp, date;
```

### Schema Features

- **Timezone Support**: All timestamps use `'Asia/Kolkata'` timezone for Indian market data
- **Market Depth**: Full L2/L3 order book data (5 levels of bid/ask)
- **Session Tracking**: Enum types for pre-market, regular, and post-market sessions
- **Data Quality**: Tracks data enrichment and correction status
- **Hourly Partitioning**: Efficient partitioning by date and hour for high-frequency data
- **Batch Processing**: UUID batch_id for tracking data ingestion batches
- **Automatic Date/Hour**: Derived fields for efficient querying and partitioning

---

## Data Ingestion from Redis Streams

### Python Integration Example

```python
import redis
import clickhouse_connect
from datetime import datetime

# Redis connection
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# ClickHouse connection
ch_client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password=''
)

def ingest_from_redis_stream():
    """Read from Redis Streams and write to ClickHouse"""
    import uuid
    from datetime import datetime
    
    stream_name = 'zerodha_ticks'
    batch_id = uuid.uuid4()
    
    while True:
        # Read from Redis Stream
        messages = redis_client.xread({stream_name: '$'}, count=100, block=1000)
        
        if messages:
            rows = []
            for stream, msgs in messages:
                for msg_id, data in msgs:
                    # Parse and transform data
                    ts = datetime.fromisoformat(data['timestamp'])
                    
                    row = {
                        'instrument_token': int(data['instrument_token']),
                        'symbol': data['symbol'],
                        'exchange': data['exchange'],
                        'timestamp': ts,
                        'exchange_timestamp': datetime.fromisoformat(data.get('exchange_timestamp', data['timestamp'])),
                        
                        # Price data
                        'last_price': float(data.get('last_price', 0)),
                        'open_price': float(data.get('open_price', 0)),
                        'high_price': float(data.get('high_price', 0)),
                        'low_price': float(data.get('low_price', 0)),
                        'close_price': float(data.get('close_price', 0)),
                        'average_traded_price': float(data.get('average_traded_price', 0)),
                        
                        # Volume and OI
                        'volume': int(data.get('volume', 0)),
                        'last_traded_quantity': int(data.get('last_traded_quantity', 0)),
                        'total_buy_quantity': int(data.get('total_buy_quantity', 0)),
                        'total_sell_quantity': int(data.get('total_sell_quantity', 0)),
                        'open_interest': int(data.get('open_interest', 0)),
                        'oi_day_high': int(data.get('oi_day_high', 0)),
                        'oi_day_low': int(data.get('oi_day_low', 0)),
                        
                        # Market depth - Bid levels
                        'bid_1_price': float(data.get('bid_1_price', 0)),
                        'bid_1_quantity': int(data.get('bid_1_quantity', 0)),
                        'bid_1_orders': int(data.get('bid_1_orders', 0)),
                        'bid_2_price': float(data.get('bid_2_price', 0)),
                        'bid_2_quantity': int(data.get('bid_2_quantity', 0)),
                        'bid_2_orders': int(data.get('bid_2_orders', 0)),
                        'bid_3_price': float(data.get('bid_3_price', 0)),
                        'bid_3_quantity': int(data.get('bid_3_quantity', 0)),
                        'bid_3_orders': int(data.get('bid_3_orders', 0)),
                        'bid_4_price': float(data.get('bid_4_price', 0)),
                        'bid_4_quantity': int(data.get('bid_4_quantity', 0)),
                        'bid_4_orders': int(data.get('bid_4_orders', 0)),
                        'bid_5_price': float(data.get('bid_5_price', 0)),
                        'bid_5_quantity': int(data.get('bid_5_quantity', 0)),
                        'bid_5_orders': int(data.get('bid_5_orders', 0)),
                        
                        # Market depth - Ask levels
                        'ask_1_price': float(data.get('ask_1_price', 0)),
                        'ask_1_quantity': int(data.get('ask_1_quantity', 0)),
                        'ask_1_orders': int(data.get('ask_1_orders', 0)),
                        'ask_2_price': float(data.get('ask_2_price', 0)),
                        'ask_2_quantity': int(data.get('ask_2_quantity', 0)),
                        'ask_2_orders': int(data.get('ask_2_orders', 0)),
                        'ask_3_price': float(data.get('ask_3_price', 0)),
                        'ask_3_quantity': int(data.get('ask_3_quantity', 0)),
                        'ask_3_orders': int(data.get('ask_3_orders', 0)),
                        'ask_4_price': float(data.get('ask_4_price', 0)),
                        'ask_4_quantity': int(data.get('ask_4_quantity', 0)),
                        'ask_4_orders': int(data.get('ask_4_orders', 0)),
                        'ask_5_price': float(data.get('ask_5_price', 0)),
                        'ask_5_quantity': int(data.get('ask_5_quantity', 0)),
                        'ask_5_orders': int(data.get('ask_5_orders', 0)),
                        
                        # Metadata
                        'packet_type': data.get('packet_type', ''),
                        'session_type': data.get('session_type', 'regular'),
                        'data_quality': data.get('data_quality', 'raw'),
                        'batch_id': str(batch_id),
                        
                        # Derived fields (auto-calculated by ClickHouse)
                        'date': ts.date(),
                        'hour': ts.hour
                    }
                    rows.append(row)
            
            # Batch insert to ClickHouse
            if rows:
                ch_client.insert('tick_data', rows)
                print(f"Inserted {len(rows)} rows with batch_id: {batch_id}")

if __name__ == '__main__':
    ingest_from_redis_stream()
```

---

## Common Queries

### Real-time Price Queries

```sql
-- Latest price for all instruments
SELECT 
    symbol,
    exchange,
    argMax(last_price, exchange_timestamp) as latest_price,
    argMax(volume, exchange_timestamp) as latest_volume,
    max(exchange_timestamp) as last_update
FROM tick_data
WHERE date = today()
  AND session_type = 'regular'
GROUP BY symbol, exchange
ORDER BY last_update DESC;
```

### Volume Analysis

```sql
-- Total volume by instrument today
SELECT 
    symbol,
    exchange,
    sum(volume) as total_volume,
    sum(total_buy_quantity) as total_buy,
    sum(total_sell_quantity) as total_sell,
    count() as tick_count
FROM tick_data
WHERE date = today()
  AND session_type = 'regular'
GROUP BY symbol, exchange
ORDER BY total_volume DESC
LIMIT 20;
```

### OHLC Aggregation

```sql
-- 1-minute OHLC bars (using materialized view)
SELECT 
    instrument_token,
    timestamp,
    last_price as close,
    low,
    high,
    volume,
    open_interest
FROM tick_data_1min
WHERE date = today()
ORDER BY timestamp DESC
LIMIT 100;

-- Or calculate from raw data
SELECT 
    symbol,
    toStartOfMinute(timestamp) as minute,
    min(low_price) as low,
    max(high_price) as high,
    argMin(open_price, timestamp) as open,
    argMax(close_price, timestamp) as close,
    sum(volume) as volume
FROM tick_data
WHERE date = today()
  AND symbol = 'NIFTY'
  AND session_type = 'regular'
GROUP BY symbol, minute
ORDER BY minute;
```

### Market Depth Analysis

```sql
-- Current order book depth
SELECT 
    symbol,
    exchange,
    timestamp,
    bid_1_price, bid_1_quantity, bid_1_orders,
    ask_1_price, ask_1_quantity, ask_1_orders,
    bid_1_price - ask_1_price as spread
FROM tick_data
WHERE date = today()
  AND instrument_token = 256265
ORDER BY timestamp DESC
LIMIT 1;
```

### Open Interest Analysis

```sql
-- OI changes over time
SELECT 
    symbol,
    toStartOfMinute(timestamp) as minute,
    argMax(open_interest, exchange_timestamp) as current_oi,
    argMax(oi_day_high, exchange_timestamp) as day_high_oi,
    argMax(oi_day_low, exchange_timestamp) as day_low_oi
FROM tick_data
WHERE date = today()
  AND open_interest > 0
GROUP BY symbol, minute
ORDER BY minute DESC;
```

### Time Range Queries

```sql
-- Price movement over time range
SELECT 
    timestamp,
    exchange_timestamp,
    last_price,
    volume,
    total_buy_quantity,
    total_sell_quantity,
    session_type
FROM tick_data
WHERE date = today()
  AND instrument_token = 256265
  AND timestamp >= now() - INTERVAL 1 HOUR
ORDER BY timestamp;
```

### Data Quality Queries

```sql
-- Check data quality distribution
SELECT 
    data_quality,
    session_type,
    count() as count,
    min(timestamp) as first_tick,
    max(timestamp) as last_tick
FROM tick_data
WHERE date = today()
GROUP BY data_quality, session_type
ORDER BY count DESC;
```

### Simple Analysis Queries

#### Real-time Price Movements

```sql
-- Top gainers for the day
SELECT 
    symbol,
    argMax(last_price, timestamp) as current_price,
    min(last_price) as day_low,
    max(last_price) as day_high,
    (current_price - min(last_price)) / min(last_price) * 100 as gain_pct
FROM tick_data 
WHERE date = today() 
GROUP BY symbol
ORDER BY gain_pct DESC
LIMIT 10;
```

#### Volume Analysis

```sql
-- Volume analysis by minute for specific symbols
SELECT
    toStartOfMinute(timestamp) as minute,
    symbol,
    sum(volume) as total_volume
FROM tick_data
WHERE date = today() AND symbol IN ('RELIANCE', 'INFY', 'HDFC')
GROUP BY minute, symbol
ORDER BY minute DESC, total_volume DESC;
```

#### Bid-Ask Spread Analysis

```sql
-- Bid-Ask spread analysis for the last hour
SELECT
    symbol,
    avg(ask_1_price - bid_1_price) as avg_spread,
    avg((ask_1_price - bid_1_price) / bid_1_price * 100) as spread_pct
FROM tick_data
WHERE date = today() AND timestamp > now() - INTERVAL 1 HOUR
GROUP BY symbol
ORDER BY avg_spread DESC;
```

---

## Performance Optimization

### Table Engine Selection

- **MergeTree**: Best for time-series data with partitioning
- **ReplacingMergeTree**: For deduplication
- **SummingMergeTree**: For pre-aggregated metrics

### Partitioning Strategy

```sql
-- Hourly partitioning for high-frequency tick data (current implementation)
PARTITION BY (date, hour)

-- Benefits:
-- - Efficient query pruning for time-range queries
-- - Faster partition management and deletion
-- - Better parallel processing

-- Alternative partitioning strategies:
-- Daily partitioning (for lower frequency data)
PARTITION BY date

-- Monthly partitioning (for historical data)
PARTITION BY toYYYYMM(date)
```

### Indexing

```sql
-- Primary key for fast lookups (current implementation)
ORDER BY (instrument_token, timestamp, exchange_timestamp)

-- Benefits:
-- - Fast lookups by instrument_token
-- - Efficient time-range queries
-- - Supports exchange_timestamp ordering for accurate sequencing

-- Secondary indexes for symbol lookups
ALTER TABLE tick_data ADD INDEX symbol_idx symbol TYPE bloom_filter GRANULARITY 1;

-- Index for exchange lookups
ALTER TABLE tick_data ADD INDEX exchange_idx exchange TYPE bloom_filter GRANULARITY 1;
```

### Compression

```sql
-- Enable compression for storage efficiency
SETTINGS compression = 'lz4'
```

---

## Monitoring & Maintenance

### System Tables

```sql
-- Check table sizes
SELECT 
    database,
    table,
    formatReadableSize(sum(bytes)) as size,
    sum(rows) as rows
FROM system.parts
WHERE active
GROUP BY database, table
ORDER BY sum(bytes) DESC;

-- Check query performance
SELECT 
    query,
    query_duration_ms,
    read_rows,
    formatReadableSize(read_bytes) as read_size
FROM system.query_log
WHERE type = 'QueryFinish'
ORDER BY query_duration_ms DESC
LIMIT 10;
```

### Backup & Restore

```bash
# Backup table
clickhouse-client --query "SELECT * FROM tick_data FORMAT Native" > backup.native

# Restore table
clickhouse-client --query "INSERT INTO tick_data FORMAT Native" < backup.native
```

---

## Integration with Grafana

### ClickHouse Data Source Configuration

1. Install ClickHouse plugin in Grafana
2. Add data source:
   - **Type**: ClickHouse
   - **URL**: `http://localhost:8123`
   - **Database**: `default`
   - **User**: `default`

### Example Grafana Queries

```sql
-- Time series for price chart
SELECT 
    $__time(timestamp),
    last_price as value
FROM tick_data
WHERE $__timeFilter(timestamp)
  AND symbol = '$symbol'
ORDER BY timestamp
```

---

## Data Publishing Guidelines

### Where to Publish What Data

**IMPORTANT: All data must be enriched with metadata before publishing to ClickHouse.**

**Current Status**: ✅ **Data is published WITH metadata to Redis streams**

All components (commodity_crawler, data_migrator, OHLC ingestion) enrich data BEFORE publishing/inserting. See `clickhouse_setup/REDIS_PUBLISHING_GUIDE.md` for details.

#### 1. Real-time Tick Data (WebSocket)

**Source**: Zerodha WebSocket (commodity_crawler, intraday_crawler, etc.)

**Publish To**:
- **Redis Stream**: `ticks:stream` on **port 6380** (ClickHouse dedicated Redis)
- **Format**: Enriched tick data with full metadata
- **Enrichment**: Required - use `token_lookup_enriched.json` to enrich before publishing

**Example**:
```python
# In your crawler (e.g., commodity_crawler.py)
from trading_system.storage.data_migrator import ClickHouseDataImporter

# Initialize importer (loads metadata automatically)
importer = ClickHouseDataImporter()

# Enrich and publish tick data
enriched_data = importer._enrich_tick_data(tick_data)
redis_client = redis.Redis(host='localhost', port=6380, decode_responses=True)
redis_client.xadd('ticks:stream', enriched_data, maxlen=100000, approximate=True)
```

**Key Points**:
- ✅ **Port 6380**: ClickHouse dedicated Redis (separate from main system Redis on 6379)
- ✅ **Enrichment Required**: All data must have metadata from `token_lookup_enriched.json`
- ✅ **Stream Name**: `ticks:stream` (standardized for ClickHouse pipeline)
- ✅ **Data Quality**: Mark as `'enriched'` after metadata enrichment

#### 2. Historical OHLC Data (90 Days)

**Source**: JSON files (e.g., `commodity_historical_data_consolidated.json`)

**Publish To**:
- **Direct to ClickHouse**: Use `ingest_ohlc_historical.py` script
- **Enrichment**: Automatic - uses ClickHouse `token_metadata` table for fast lookups

**Setup (One-time)**:
```bash
# First, load 400k+ token metadata into ClickHouse (one-time, ~2-3 minutes)
python clickhouse_setup/load_token_metadata_to_clickhouse.py

# This creates:
# - token_metadata table (all 400k+ tokens)
# - hot_token_metadata materialized view (top 10k frequently accessed tokens)
```

**Usage**:
```bash
# Ingest 90 days of OHLC data (uses ClickHouse for fast metadata lookups)
python clickhouse_setup/ingest_ohlc_historical.py

# Or specify custom file
python clickhouse_setup/ingest_ohlc_historical.py --file /path/to/ohlc_data.json

# Dry run to validate
python clickhouse_setup/ingest_ohlc_historical.py --dry-run
```

**Key Points**:
- ✅ **Fast ClickHouse Lookups**: Uses `token_metadata` table instead of loading 136MB JSON file
- ✅ **Hot Token Optimization**: Queries `hot_token_metadata` first (faster for frequent tokens)
- ✅ **Automatic Fallback**: Falls back to JSON if ClickHouse table not found
- ✅ **Only Enriched Data**: Records without metadata are skipped
- ✅ **Batch Processing**: Processes in batches of 1000 records (configurable)

#### 3. Historical Data Migration (Redis → ClickHouse)

**Source**: Existing Redis databases (DB 0, DB 1, DB 2)

**Publish To**:
- **Redis Stream**: `ticks:stream` on **port 6380**
- **Method**: Use `data_migrator.py` in `clickhouse` mode

**Usage**:
```bash
# Import historical data from Redis
python trading_system/storage/data_migrator.py --mode clickhouse --historical

# Continuous import (real-time + historical)
python trading_system/storage/data_migrator.py --mode clickhouse --continuous
```

**Key Points**:
- ✅ **Automatic Enrichment**: Importer enriches all data before publishing
- ✅ **Port 6380**: Publishes to ClickHouse Redis stream
- ✅ **Multiple Sources**: Scans DB 0, DB 1, DB 2 for historical data

### Data Enrichment Requirements

**All data published to ClickHouse must be enriched with metadata:**

1. **Metadata Source**: 
   - **Primary**: ClickHouse `token_metadata` table (fast, recommended)
   - **Fallback**: `core/data/token_lookup_enriched.json` (136MB JSON file, slower)
   
2. **Setup Token Metadata in ClickHouse** (One-time):
   ```bash
   # Load 400k+ tokens into ClickHouse for fast lookups
   python clickhouse_setup/load_token_metadata_to_clickhouse.py
   ```
   This creates:
   - `token_metadata` table: All 400k+ tokens
   - `hot_token_metadata` view: Top 10k frequently accessed tokens (auto-updated)

3. **Required Fields After Enrichment**:
   - `symbol`: Trading symbol
   - `exchange`: Exchange (NSE, BSE, NFO, MCX, etc.)
   - `sector`: Sector classification
   - `asset_class`: Asset class (EQ, CE, PE, FUT, etc.)
   - `instrument_type`: Instrument type
   - `data_quality`: Must be `'enriched'` (not `'raw'`)

3. **Enrichment Process** (Recommended - ClickHouse):
   ```python
   from clickhouse_driver import Client
   
   # Connect to ClickHouse
   client = Client(host='localhost', database='trading')
   
   # Fast lookup from hot_token_metadata (or token_metadata)
   instrument_token = tick_data.get('instrument_token')
   result = client.execute(
       'SELECT symbol, tradingsymbol, exchange, sector, asset_class, instrument_type '
       'FROM hot_token_metadata WHERE instrument_token = %s',
       [instrument_token]
   )
   
   if not result:
       # Fallback to full token_metadata table
       result = client.execute(
           'SELECT symbol, tradingsymbol, exchange, sector, asset_class, instrument_type '
           'FROM token_metadata WHERE instrument_token = %s',
           [instrument_token]
       )
   
   if result:
       row = result[0]
       tick_data['symbol'] = row[0] or row[1]
       tick_data['exchange'] = row[2]
       tick_data['sector'] = row[3]
       tick_data['asset_class'] = row[4]
       tick_data['instrument_type'] = row[5]
       tick_data['data_quality'] = 'enriched'
   ```

4. **Enrichment Process** (Fallback - JSON):
   ```python
   # Load metadata from JSON (slower, use ClickHouse instead)
   with open('core/data/token_lookup_enriched.json', 'r') as f:
       token_metadata = json.load(f)
   
   # Enrich tick data
   instrument_token = tick_data.get('instrument_token')
   if instrument_token in token_metadata:
       metadata = token_metadata[instrument_token]
       tick_data['symbol'] = metadata.get('symbol')
       tick_data['exchange'] = metadata.get('exchange')
       tick_data['sector'] = metadata.get('sector')
       tick_data['asset_class'] = metadata.get('asset_class')
       tick_data['data_quality'] = 'enriched'
   ```

### Data Flow Summary

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                              │
├─────────────────────────────────────────────────────────────┤
│ 1. Zerodha WebSocket (commodity_crawler, etc.)              │
│ 2. Historical OHLC JSON files                               │
│ 3. Existing Redis databases (DB 0, 1, 2)                   │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│              ENRICHMENT (Required)                          │
├─────────────────────────────────────────────────────────────┤
│ Option A: ClickHouse (Fast, Recommended)                    │
│   • Query hot_token_metadata view (top 10k tokens)          │
│   • Fallback to token_metadata table (all 400k+ tokens)    │
│   • Fast indexed lookups by instrument_token                │
│                                                              │
│ Option B: JSON File (Fallback, Slower)                      │
│   • Load token_lookup_enriched.json (136MB)                 │
│   • In-memory dictionary lookup                             │
│                                                              │
│ • Enrich each record with: symbol, exchange, sector, etc.  │
│ • Mark data_quality as 'enriched'                           │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│              PUBLISH TO CLICKHOUSE                          │
├─────────────────────────────────────────────────────────────┤
│ Option A: Redis Stream (port 6380)                          │
│   → ticks:stream → ClickHouse Pipeline → ClickHouse          │
│                                                              │
│ Option B: Direct Insert                                     │
│   → ingest_ohlc_historical.py → ClickHouse                   │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│              CLICKHOUSE DATABASE                            │
├─────────────────────────────────────────────────────────────┤
│ Database: trading                                           │
│ Table: tick_data (enriched with full metadata)              │
└─────────────────────────────────────────────────────────────┘
```

### Quick Reference

| Data Type | Publish To | Port | Stream/Table | Enrichment Method |
|-----------|-----------|------|---------------|-------------------|
| Real-time ticks | Redis Stream | 6380 | `ticks:stream` | ClickHouse `token_metadata` (recommended) |
| Historical OHLC | Direct/Stream | 6380 | `tick_data` | ClickHouse `token_metadata` (fast lookups) |
| Historical Redis | Redis Stream | 6380 | `ticks:stream` | ClickHouse `token_metadata` or JSON fallback |
| News data | Redis Stream | 6380 | `news:stream` | Optional |
| Indices data | Redis Stream | 6380 | `indices:stream` | Optional |

**Remember**: 
- ✅ **Port 6380** = ClickHouse Redis (separate from main system)
- ✅ **Enrichment Required** = All data must have metadata before ingestion
- ✅ **Stream Name** = `ticks:stream` for tick data
- ✅ **Use ClickHouse** = Load `token_metadata` table once, use for fast lookups (not JSON file)
- ✅ **Hot Tokens** = `hot_token_metadata` view for top 10k frequently accessed tokens

---

## Troubleshooting

### Common Issues

1. **Port Already in Use**
   ```bash
   # Check if ClickHouse is running
   lsof -i :8123
   lsof -i :9000
   ```

2. **Permission Issues**
   ```bash
   # Fix data directory permissions
   sudo chown -R $(whoami) /opt/homebrew/var/lib/clickhouse/
   ```

3. **Memory Issues**
   - Adjust `max_memory_usage` in `users.xml`
   - Use `SET max_memory_usage = 10000000000;` in queries

### Logs

```bash
# Server logs
tail -f /opt/homebrew/var/log/clickhouse-server/clickhouse-server.log

# Error logs
tail -f /opt/homebrew/var/log/clickhouse-server/clickhouse-server.err.log
```

---

## Best Practices

1. **Batch Inserts**: Always use batch inserts (1000+ rows) for better performance
2. **Partitioning**: Partition by date/month for efficient querying
3. **TTL**: Set TTL to automatically remove old data
4. **Materialized Views**: Use materialized views for common aggregations
5. **Compression**: Enable compression to save storage space
6. **Monitoring**: Regularly check system tables for performance metrics

---

## Resources

- [ClickHouse Documentation](https://clickhouse.com/docs)
- [ClickHouse GitHub](https://github.com/ClickHouse/ClickHouse)
- [ClickHouse Playground](https://play.clickhouse.com/)

---

## Running the System

### 1. Run Setup

```bash
# Make setup script executable
chmod +x setup_mac_m4.sh

# Run the setup script
./setup_mac_m4.sh
```

### 2. Start the Pipeline

```bash
# Start the ClickHouse ingestion pipeline (in terminal 1)
cd clickhouse_setup
python clickhouse_pipeline.py
```

The pipeline will:
- Connect to Redis Streams on the configured port
- Read tick data from Redis
- Batch insert data into ClickHouse
- Handle errors and retries automatically

### 3. Integrate with Zerodha WebSocket

Modify your existing Zerodha WebSocket code to push data to the pipeline:

```python
from clickhouse_setup.clickhouse_pipeline import ClickHousePipeline

# Initialize pipeline
pipeline = ClickHousePipeline()

# In your WebSocket tick handler
def on_ticks(ws, ticks):
    for tick in ticks:
        # Instead of writing to files, push to pipeline
        pipeline.push_tick_data(tick)
```

The pipeline will handle:
- Data transformation and enrichment
- Batching for optimal performance
- Writing to ClickHouse
- Error handling and retries

### 4. Query Data

```bash
# Connect to ClickHouse client
clickhouse-client --database=trading

# Or run queries directly
clickhouse-client --database=trading --query="SELECT count(*) FROM tick_data"

# Check today's data
clickhouse-client --database=trading --query="SELECT count(*) FROM tick_data WHERE date = today()"

# View recent ticks
clickhouse-client --database=trading --query="SELECT symbol, last_price, volume FROM tick_data WHERE date = today() ORDER BY timestamp DESC LIMIT 10"
```

### 5. Monitor the System

```bash
# Check pipeline status (if logging enabled)
tail -f clickhouse_setup/pipeline.log

# Monitor Redis Streams
redis-cli -p 6380 XINFO STREAM zerodha_ticks

# Check ClickHouse table sizes
clickhouse-client --query="SELECT formatReadableSize(sum(bytes)) as size FROM system.parts WHERE table = 'tick_data'"
```

### Complete Workflow

1. **Start Redis** (if not already running):
   ```bash
   cd clickhouse_setup
   ./start_redis_6380_daemon.sh
   ```

2. **Start ClickHouse** (if not already running):
   ```bash
   clickhouse server
   ```

3. **Start the Pipeline**:
   ```bash
   python clickhouse_pipeline.py
   ```

4. **Start Your WebSocket Client**:
   - Your existing Zerodha WebSocket code
   - Modified to call `pipeline.push_tick_data(tick)`

5. **Query and Analyze**:
   - Use ClickHouse client or Python scripts
   - Set up Grafana dashboards
   - Run analysis queries

---

## Next Steps

1. ✅ ClickHouse installed
2. ✅ Redis configuration created
3. ⬜ Configure ClickHouse server
4. ⬜ Create database and tables
5. ⬜ Set up Redis Streams → ClickHouse ingestion pipeline
6. ⬜ Integrate with Zerodha WebSocket
7. ⬜ Configure Grafana dashboards
8. ⬜ Implement monitoring and alerting

