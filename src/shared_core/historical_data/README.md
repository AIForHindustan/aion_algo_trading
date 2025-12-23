# Historical Data Directory - Overview

## 📁 Directory Contents

The `shared_core/historical_data/` directory contains **static historical data files** that serve as reference data and seed data for analysis. This directory currently contains:

### 1. **VIX Historical Data** (1 file)
- `hist_india_vix_-29-05-2025-to-29-11-2025.csv`
  - **Format**: CSV with OHLC data
  - **Columns**: Date, Open, High, Low, Close, Prev. Close, Change, % Change
  - **Time Range**: May 29, 2025 to November 29, 2025 (6 months)
  - **Purpose**: Historical VIX data for backtesting, volatility analysis, and regime classification

### 2. **Option Chain Snapshots** (8 files)
- `option-chain-ED-NIFTY-02-Dec-2025.csv`
- `option-chain-ED-NIFTY-09-Dec-2025.csv`
- `option-chain-ED-NIFTY-16-Dec-2025.csv`
- `option-chain-ED-NIFTY-23-Dec-2025.csv`
- `option-chain-ED-BANKNIFTY-30-Dec-2025.csv`
- `option-chain-ED-MIDCPNIFTY-30-Dec-2025.csv`
- `option-chain-COM-SILVER-02-Dec-2025.csv`
- `option-chain-COM-SILVER-02-Mar-2026.csv`

**Format**: CSV with option chain data
- **Structure**: Calls and Puts side-by-side with strike prices
- **Columns**: OI, CHNG IN OI, VOLUME, IV, LTP, CHNG, BID QTY, BID, ASK, ASK QTY, STRIKE (repeated for both sides)
- **Purpose**: Snapshot of option chain at specific expiry dates for:
  - Max pain analysis
  - Put-call ratio calculations
  - Open interest distribution
  - IV skew analysis
  - Historical option pricing reference

**Total Files**: 9 CSV files (~1,381 total lines)

---

## 🎯 Purpose & Benefits

### 1. **Reference Data for Analysis**
- **Backtesting**: Historical VIX data enables backtesting of volatility-based strategies
- **Option Chain Analysis**: Snapshot data for understanding option market structure at specific points in time
- **Regime Classification**: VIX historical data helps classify market regimes (LOW, NORMAL, HIGH, PANIC)

### 2. **Seed Data for ClickHouse**
- These files can be ingested into ClickHouse for:
  - Historical VIX data → `indices_data` table (with `symbol = 'NSE:INDIA VIX'`)
  - Option chain data → Can be parsed and stored in `tick_data` table (if needed for historical analysis)

### 3. **Research & Development**
- **Pattern Recognition**: Historical data helps identify patterns in volatility and option behavior
- **Strategy Development**: Test strategies against historical VIX regimes
- **Market Microstructure**: Analyze option chain structure changes over time

### 4. **Data Validation**
- Compare real-time data against historical snapshots
- Validate data quality and consistency
- Debug issues with current data capture

---

## 💾 How This Data is Stored in ClickHouse

### Current Storage Architecture

#### 1. **VIX Data → `indices_data` Table**

**Real-time VIX Data** (Currently Active):
- **Source**: `crawlers/gift_nifty_gap.py` (updates every 30 seconds)
- **Stream**: `indices:stream` (Redis port 6379, DB 1)
- **Table**: `indices_data`
- **Schema**:
```sql
CREATE TABLE indices_data (
    instrument_token UInt32,
    symbol String,                  -- 'NSE:INDIA VIX'
    tradingsymbol String,
    exchange String DEFAULT 'NSE',
    segment String DEFAULT 'INDEX',
    timestamp DateTime64(3, 'Asia/Kolkata'),
    last_price Float64,            -- VIX value
    net_change Float64,
    percent_change Float64,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume UInt64,
    date Date DEFAULT toDate(timestamp),
    hour UInt8 DEFAULT toHour(timestamp)
) ENGINE = MergeTree()
PARTITION BY (date, hour)
ORDER BY (symbol, timestamp, instrument_token);
```

**Historical VIX CSV** (`hist_india_vix_*.csv`):
- **Status**: ⚠️ **NOT YET INGESTED** into ClickHouse
- **Potential Use**: Can be ingested to backfill historical VIX data
- **Ingestion Method**: Would need a script to:
  1. Parse CSV dates (format: `29-MAY-2025`)
  2. Convert to ClickHouse `DateTime64` format
  3. Insert into `indices_data` with `symbol = 'NSE:INDIA VIX'` and `data_quality = 'historical'`

#### 2. **Option Chain Data → `tick_data` Table (Potential)**

**Current Real-time Option Data**:
- **Source**: Real-time tick data from crawlers
- **Table**: `tick_data`
- **Schema**: See `clickhouse_setup/create_tables.sql`
- **Fields Relevant for Options**:
  - `symbol` (e.g., `NIFTY25JAN24000CE`)
  - `instrument_type` (`CE`, `PE`, `FUT`)
  - `open_interest` (OI)
  - `volume`
  - `last_price` (LTP)
  - `bid_1_price`, `ask_1_price` (bid/ask)
  - `strike`, `expiry` (from metadata)

**Historical Option Chain CSV** (`option-chain-*.csv`):
- **Status**: ⚠️ **NOT YET INGESTED** into ClickHouse
- **Structure**: These are **snapshot files** (not time-series)
- **Potential Use**: 
  - Parse option chain snapshots
  - Extract individual option contracts
  - Ingest as historical records into `tick_data` with `data_quality = 'historical'`
  - **Note**: These are single-day snapshots, not continuous time-series

#### 3. **Historical OHLC Data → `tick_data` Table**

**Commodity Historical Data** (Different Location):
- **Location**: `clickhouse_setup/historical_data/commodity/`
- **Format**: JSON files (consolidated)
- **Ingestion**: `clickhouse_setup/ingest_ohlc_historical.py`
- **Table**: `tick_data` (with `data_quality = 'historical'`)
- **Purpose**: 90 days of historical OHLC data for commodities

---

## 🔄 Data Flow Comparison

### Real-time Data Flow (Currently Active)
```
Crawlers (Zerodha API)
    ↓
Redis Streams (indices:stream, ticks:stream)
    ↓
ClickHouse Bridge (redis_clickhouse_bridge.py)
    ↓
ClickHouse Tables (indices_data, tick_data)
    ↓
Analytics & Queries
```

### Historical Data Flow (Potential)
```
CSV/JSON Files (shared_core/historical_data/)
    ↓
Ingestion Scripts (to be created)
    ↓
ClickHouse Tables (indices_data, tick_data)
    ↓
Analytics & Queries
```

---

## 📊 Benefits of Having This Data

### 1. **Backtesting & Strategy Development**
- Test volatility-based strategies against historical VIX regimes
- Analyze option chain behavior during different market conditions
- Validate pattern detection algorithms

### 2. **Data Completeness**
- Fill gaps in real-time data capture
- Provide historical context for current market conditions
- Enable long-term trend analysis

### 3. **Research & Analysis**
- **VIX Analysis**: Study volatility patterns, spikes, and mean reversion
- **Option Chain Analysis**: Understand max pain, put-call ratios, OI distribution
- **Market Regime Classification**: Use historical VIX to train/validate regime classifiers

### 4. **Data Quality Validation**
- Compare real-time data against historical snapshots
- Detect anomalies in current data capture
- Validate data consistency over time

---

## 🚀 Recommended Next Steps

### 1. **Ingest VIX Historical Data**
Create a script to ingest `hist_india_vix_*.csv` into `indices_data`:
```python
# shared_core/utils/ingest_vix_historical.py
# Parse CSV → Convert dates → Insert into indices_data
# Set symbol = 'NSE:INDIA VIX', data_quality = 'historical'
```

### 2. **Parse Option Chain Snapshots** (Optional)
If needed for historical analysis:
```python
# shared_core/utils/parse_option_chain_snapshots.py
# Parse CSV → Extract individual options → Insert into tick_data
# Set data_quality = 'historical', timestamp = snapshot date
```

### 3. **Automate Historical Data Updates**
- Set up periodic downloads of VIX historical data
- Archive option chain snapshots for future analysis
- Maintain a historical data catalog

### 4. **Create Historical Data Views**
In ClickHouse, create materialized views for:
- Historical VIX averages (20d, 55d)
- Historical option chain statistics
- Volatility regime transitions

---

## 📝 File Naming Convention

- **VIX Files**: `hist_india_vix_<start_date>-to-<end_date>.csv`
- **Option Chain Files**: `option-chain-<type>-<underlying>-<expiry_date>.csv`
  - `ED` = Equity Derivatives (NIFTY, BANKNIFTY, etc.)
  - `COM` = Commodities (SILVER, GOLD, etc.)

---

## 🔍 Related Files & Scripts

- **Real-time VIX Capture**: `crawlers/gift_nifty_gap.py`
- **Historical OHLC Ingestion**: `clickhouse_setup/ingest_ohlc_historical.py`
- **ClickHouse Schema**: `clickhouse_setup/create_tables.sql`
- **VIX Utilities**: `shared_core/utils/vix_utils.py`
- **VIX Regime Manager**: `shared_core/utils/vix_regimes.py`

---

## ⚠️ Important Notes

1. **Static vs. Dynamic**: These are **static snapshot files**, not continuously updated
2. **Not in ClickHouse Yet**: These files are **not currently ingested** into ClickHouse
3. **Reference Data**: Primarily used as reference/seed data for analysis
4. **Real-time Data**: Real-time VIX and option data is captured separately via crawlers → ClickHouse
5. **Data Quality**: Historical CSV files may need validation/cleaning before ingestion

---

## 📈 Usage Examples

### Query Historical VIX from ClickHouse (After Ingestion)
```sql
-- Get VIX data for a date range
SELECT 
    date,
    last_price as vix_value,
    close as vix_close
FROM indices_data
WHERE symbol = 'NSE:INDIA VIX'
  AND date >= '2025-05-29'
  AND date <= '2025-11-29'
ORDER BY date;
```

### Analyze Option Chain from CSV (Current)
```python
import pandas as pd

# Load option chain snapshot
df = pd.read_csv('shared_core/historical_data/option-chain-ED-NIFTY-02-Dec-2025.csv')

# Parse calls and puts
# Calculate max pain, put-call ratio, etc.
```

---

## 🎯 Summary

The `shared_core/historical_data/` directory contains **static historical reference data**:
- ✅ **VIX Historical Data**: 6 months of daily VIX OHLC data
- ✅ **Option Chain Snapshots**: 8 snapshots of option chains at specific expiry dates
- ⚠️ **Not Yet in ClickHouse**: These files are reference data, not yet ingested
- 🔄 **Real-time Data**: VIX and options are captured in real-time via crawlers → ClickHouse
- 📊 **Benefits**: Backtesting, research, data validation, historical analysis

**Key Insight**: This directory serves as a **reference/seed data repository**, while real-time data flows through the crawler → Redis → ClickHouse pipeline.

