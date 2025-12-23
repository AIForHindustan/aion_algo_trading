# Database Architecture & Correlation Analysis Setup

**Last Updated**: November 1, 2025

## Database Files Overview

### Primary Production Database

#### `tick_data_production.db` (30.20 GB) ⭐ **LATEST & PRIMARY**
- **Modified**: October 29, 2025 20:31
- **Status**: ✅ Active Production Database
- **Purpose**: Primary tick data storage for correlation analysis

**Tables**:
1. **`tick_data_corrected`** (26.7M rows) - Main tick data table
   - **Columns**: 67 columns including full market depth (bid/ask 1-5), OHLC, volume, OI
   - **Date Range**: 1970-01-01 to 2025-10-29 20:12:01
   - **Unique Instruments**: 79,660
   - **Unique Symbols**: 15,271
   - **Schema**: Production schema with PRIMARY KEY (instrument_token, exchange_timestamp_ns, source_file)
   - **Key Features**:
     - Complete market depth data (5 levels bid/ask)
     - Full OHLC data
     - Volume and OI tracking
     - Packet type classification
     - Data quality markers
     - Session tracking

2. **`token_mappings`** (243K rows)
   - Instrument metadata and mappings
   - Unique Instruments: 243,568

3. **`daily_indices`** (42K rows)
   - Daily index data with news sentiment
   - Date Range: 2025-10-06 to 2025-10-29

4. **`news_data`** (229K rows)
   - News articles with sentiment analysis

5. **`crypto_klines`** (54K rows)
   - Crypto market data

6. **`crypto_trades`** (3.7M rows)
   - Crypto trade data

7. **`processed_files_log`** (0 rows)
   - Tracks processed binary files

8. **`tick_data_corrected_backup_20251023_2039`** (8.8M rows)
   - Backup from October 23, 2025
   - Date Range: 2025-10-21 to 2025-10-23

9. **`test_jsonl_ingestion`** (100 rows)
   - Test data

### Secondary Databases

#### `nse_tick_data.duckdb` (4.25 GB)
- **Modified**: October 21, 2025 21:35
- **Status**: ⚠️ Older data snapshot
- **Purpose**: Historical snapshot database

**Tables**:
1. **`tick_data_corrected`** (184K rows)
   - Date Range: 2025-10-21 15:39 to 16:05 (single session)
   - Unique Instruments: 47,069
   - **Note**: This appears to be a single day's data

2. **`token_mappings`** (760K rows)
   - Larger token mapping than production DB

3. **`tick_data_full`** (0 rows)
   - Empty table

4. **`processed_files_log`** (1 row)

#### Legacy Databases (Empty/Minimal)

- **`intraday_trading.db`** (12KB) - Empty
- **`new_tick_data.db`** (268KB) - Empty `tick_data_full` table
- **`old_tick_data.db`** (12KB) - Empty

## Database Architecture

### Schema: `tick_data_corrected`

**Primary Key**: `(instrument_token, exchange_timestamp_ns, source_file)`

**Core Fields**:
```sql
-- Instrument Identifiers
instrument_token BIGINT NOT NULL
symbol VARCHAR
exchange VARCHAR
segment VARCHAR
instrument_type VARCHAR
expiry DATE
strike_price DOUBLE
option_type VARCHAR
lot_size INTEGER
tick_size DOUBLE
is_expired BOOLEAN

-- Timestamps
timestamp TIMESTAMP
exchange_timestamp TIMESTAMP
exchange_timestamp_ns BIGINT
last_traded_timestamp TIMESTAMP
last_traded_timestamp_ns BIGINT

-- Price Data
last_price DOUBLE
open_price DOUBLE
high_price DOUBLE
low_price DOUBLE
close_price DOUBLE
average_traded_price DOUBLE

-- Volume & OI
volume BIGINT
last_traded_quantity BIGINT
total_buy_quantity BIGINT
total_sell_quantity BIGINT
open_interest BIGINT
oi_day_high BIGINT
oi_day_low BIGINT

-- Market Depth (5 levels)
bid_1_price ... bid_5_price DOUBLE
bid_1_quantity ... bid_5_quantity BIGINT
bid_1_orders ... bid_5_orders INTEGER
ask_1_price ... ask_5_price DOUBLE
ask_1_quantity ... ask_5_quantity BIGINT
ask_1_orders ... ask_5_orders INTEGER

-- Metadata
packet_type VARCHAR
data_quality VARCHAR
session_type VARCHAR
source_file VARCHAR
processing_batch VARCHAR
session_id VARCHAR
processed_at TIMESTAMP
parser_version VARCHAR
```

## Correlation Analysis Requirements

### Multi-Instrument Correlation Database

For correlation analysis, we need:

1. **Time-aligned data** across multiple instruments
2. **Efficient querying** for cross-instrument correlations
3. **Parquet format** for optimal storage and query performance
4. **Partitioning** by date/instrument for fast access

### Recommended Architecture

```
correlation_analysis_db/
├── tick_data_parquet/          # Partitioned parquet files
│   ├── date=2025-10-29/
│   │   ├── instrument_token=408065/
│   │   ├── instrument_token=260105/
│   │   └── ...
│   └── date=2025-10-28/
│       └── ...
├── daily_correlations/          # Pre-computed correlations
│   ├── date=2025-10-29/
│   └── ...
└── metadata/
    ├── instrument_list.parquet
    └── symbol_mappings.parquet
```

## Data Flow: Binary → Parquet → Database

### Current Flow
```
Binary Files → production_binary_converter.py → DuckDB (tick_data_production.db)
```

### Recommended Flow for Correlation Analysis
```
Binary Files → Parquet Converter → Parquet Files (partitioned) → DuckDB (for querying)
                                      ↓
                              Direct Parquet Queries (faster)
```

## Issues & Solutions

### Issue 1: Date Range Anomaly
- **Problem**: `tick_data_production.db` shows date range starting from 1970-01-01
- **Likely Cause**: Invalid timestamp conversion (epoch time issue)
- **Impact**: Affects time-based queries and correlations

### Issue 2: Multiple Empty/Minimal Databases
- **Problem**: `intraday_trading.db`, `new_tick_data.db`, `old_tick_data.db` are empty
- **Impact**: Confusion about which database to use
- **Solution**: Archive or remove unused databases

### Issue 3: Schema Consistency
- **Problem**: Both databases use same schema, but data ranges differ
- **Solution**: Merge `nse_tick_data.duckdb` into `tick_data_production.db` if needed

### Issue 4: Parquet Conversion Before Ingestion
- **Problem**: Currently converting binary → DuckDB directly
- **Solution**: Add parquet intermediate step for better performance

## Recommendations

1. **Use `tick_data_production.db` as primary** for correlation analysis
2. **Fix timestamp issues** in existing data (filter out 1970 dates)
3. **Create parquet export pipeline** from existing DuckDB
4. **Set up partitioned parquet storage** for correlation queries
5. **Archive or remove** unused database files

## Solution: DuckDB → Parquet → Correlation Database

### Step 1: Export DuckDB to Parquet

**Script**: `export_duckdb_to_parquet.py`

```bash
# Export with date partitioning (recommended for correlation)
python export_duckdb_to_parquet.py \
  --db tick_data_production.db \
  --output correlation_analysis_db/tick_data_parquet \
  --partition date \
  --batch-size 1000000

# Export with instrument partitioning (for multi-instrument queries)
python export_duckdb_to_parquet.py \
  --db tick_data_production.db \
  --output correlation_analysis_db/tick_data_parquet \
  --partition instrument \
  --batch-size 1000000
```

**Features**:
- Automatic date filtering (removes 1970 invalid dates)
- Partitioned by date or instrument for efficient querying
- Batch processing for large datasets
- Zstd compression for optimal storage
- Dictionary encoding for string columns
- Statistics metadata for query optimization

### Step 2: Create Correlation Analysis Database

**Recommended Structure**:
```
correlation_analysis_db/
├── tick_data_parquet/          # Partitioned parquet files
│   ├── date=2025-10-29/
│   │   ├── batch_0000.parquet
│   │   ├── batch_0001.parquet
│   │   └── ...
│   └── date=2025-10-28/
│       └── ...
├── daily_correlations/          # Pre-computed correlations
│   ├── date=2025-10-29/
│   │   ├── nifty_banknifty_correlation.parquet
│   │   └── ...
│   └── ...
└── metadata/
    ├── instrument_list.parquet
    └── symbol_mappings.parquet
```

### Step 3: Query Parquet Files Directly

DuckDB can query parquet files directly without loading into database:

```sql
-- Query parquet files directly
SELECT 
    instrument_token,
    symbol,
    timestamp,
    last_price,
    volume
FROM 'correlation_analysis_db/tick_data_parquet/date=2025-10-29/*.parquet'
WHERE symbol IN ('NIFTY', 'BANKNIFTY')
ORDER BY timestamp;

-- Multi-instrument correlation query
WITH nifty_data AS (
    SELECT timestamp, last_price as nifty_price
    FROM 'correlation_analysis_db/tick_data_parquet/date=2025-10-29/*.parquet'
    WHERE symbol = 'NIFTY'
),
banknifty_data AS (
    SELECT timestamp, last_price as banknifty_price
    FROM 'correlation_analysis_db/tick_data_parquet/date=2025-10-29/*.parquet'
    WHERE symbol = 'BANKNIFTY'
)
SELECT 
    n.timestamp,
    n.nifty_price,
    b.banknifty_price,
    CORR(n.nifty_price, b.banknifty_price) OVER (
        PARTITION BY DATE(n.timestamp)
        ORDER BY n.timestamp
        ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
    ) as rolling_correlation
FROM nifty_data n
JOIN banknifty_data b ON n.timestamp = b.timestamp
ORDER BY n.timestamp;
```

### Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Binary Files (Zerodha WebSocket)                         │
│    - Raw binary tick data (184-byte packets)                │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Binary → Parquet Converter                               │
│    - production_binary_converter.py                         │
│    - Converts binary to Arrow batches                       │
│    - Writes partitioned parquet files                       │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Parquet Files (Partitioned)                              │
│    - date=YYYY-MM-DD/                                       │
│    - instrument_token=XXXXX/                                │
│    - Compressed with Zstd                                   │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. DuckDB Ingestion (Optional)                               │
│    - Can query parquet directly (no ingestion needed)      │
│    - Or load into correlation_analysis_db                    │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Correlation Analysis                                     │
│    - Multi-instrument queries                                │
│    - Rolling correlations                                    │
│    - Time-aligned price movements                            │
└─────────────────────────────────────────────────────────────┘
```

### Key Benefits

1. **Performance**: Parquet columnar format enables fast queries
2. **Storage**: Zstd compression reduces size by ~70-80%
3. **Partitioning**: Date/instrument partitioning speeds up queries
4. **No Duplication**: Query parquet directly without DB ingestion
5. **Scalability**: Can handle 100M+ rows efficiently

### Next Steps

1. ✅ Export `tick_data_production.db` to parquet (use script above)
2. ✅ Create correlation analysis queries using parquet
3. ⏳ Set up automated parquet export from new binary data
4. ⏳ Build correlation dashboard using parquet queries
5. ⏳ Archive old DuckDB files after parquet export verified

