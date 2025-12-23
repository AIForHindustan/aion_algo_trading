# NSE FII/DII Reports and Index-Level Institutional Flows

## 📍 NSE Website Links

### FII/DII Trading Activity Reports

1. **Current Reports** (Daily)
   - URL: https://www.nseindia.com/reports/fii-dii
   - Description: Daily FII/FPI and DII trading activity across NSE, BSE, and MSEI
   - Data: Overall market institutional flows (not index-specific)

2. **Historical Reports**
   - URL: https://www.nseindia.com/all-reports/historical-equities-fii-fpi-dii-trading-activity
   - Description: Historical FII/FPI and DII trading activity data
   - Use: Backtesting, trend analysis, historical flow patterns

3. **Bulk and Block Deals**
   - URL: https://www.nseindia.com/report-detail/display-bulk-and-block-deals
   - Description: Large institutional transactions (block deals and bulk deals)
   - Use: Identify specific institutional activity, derive index-level flows

---

## 🔍 What Data is Available?

### ✅ Direct from NSE API

1. **Overall Market FII/DII Flows**
   - **Endpoint**: `/api/fiidiiTradeReact`
   - **Data**: Daily buy/sell values for DII and FII (overall market)
   - **Fields**: `category`, `buy_value`, `sell_value`, `net_value`
   - **Table**: `nse_dii_fii_trading`
   - **Limitation**: Not index-specific (e.g., NIFTY, BANKNIFTY)

2. **Block Deals**
   - **Endpoint**: `/api/block-deals`
   - **Data**: Individual large transactions with symbol, client, deal type, value
   - **Fields**: `symbol`, `deal_type`, `value`, `client_name`
   - **Table**: `nse_block_deals`
   - **Use**: Can derive index-level flows from this

3. **Bulk Deals**
   - **Endpoint**: `/api/bulk-deals`
   - **Data**: Similar to block deals, different threshold
   - **Table**: `nse_block_deals` (same table)

### ⚠️ What NSE Does NOT Provide

- **Direct Index-Level FII/DII Flows**: NSE does not provide FII/DII flows specifically for indices like NIFTY 50, NIFTY BANK, etc.
- **Index Constituent Flows**: No direct API for institutional flows by index

---

## 💡 Solution: Derived Index-Level Flows

Since NSE doesn't provide direct index-level FII/DII flows, we **derive** them from block deals:

### How It Works

1. **Fetch Block Deals**: Get all block deals for a date
2. **Identify Index Constituents**: Map symbols to indices (NIFTY_50, NIFTY_BANK, NIFTY_IT)
3. **Aggregate by Index**: Sum buy/sell values for stocks in each index
4. **Calculate Net Flow**: `net_value = buy_value - sell_value`

### Implementation

```python
# Function: calculate_index_level_flows()
# Location: shared_core/utils/nse_reports_ingestion.py

# Automatically calculated when block deals are processed
python -m shared_core.utils.nse_reports_ingestion --report-type block_deals --auto-date
```

### ClickHouse Table

```sql
CREATE TABLE nse_index_institutional_flows (
    index_name String,        -- 'NIFTY_50', 'NIFTY_BANK', 'NIFTY_IT'
    date Date,
    buy_value Float64,        -- Total buy value (crores)
    sell_value Float64,       -- Total sell value (crores)
    net_value Float64,        -- buy_value - sell_value
    deal_count UInt32,        -- Number of block deals
    fetched_at DateTime64(3, 'UTC') DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, index_name);
```

### Query Example

```python
# Get index-level flows for NIFTY 50
query = """
SELECT date, buy_value, sell_value, net_value, deal_count
FROM nse_index_institutional_flows
WHERE index_name = 'NIFTY_50'
  AND date >= today() - 30
ORDER BY date DESC
"""
```

---

## 📊 Supported Indices

Currently supported indices (constituents can be expanded):

1. **NIFTY_50**: Top 50 stocks by market cap
2. **NIFTY_BANK**: Banking sector stocks
3. **NIFTY_IT**: IT sector stocks

**Note**: Index constituents are hardcoded in `calculate_index_level_flows()`. For production, should load from:
- `shared_core/instrument_token/instrument_registry.py`
- NSE index constituents API (if available)
- Static CSV/JSON files

---

## 🎯 Usage

### 1. Fetch Overall Market FII/DII Flows

```bash
# Fetch overall market flows
python -m shared_core.utils.nse_reports_ingestion --report-type dii_fii --auto-date
```

**Result**: Stored in `nse_dii_fii_trading` table

### 2. Fetch Block Deals (Auto-calculates Index Flows)

```bash
# Fetch block deals (automatically calculates index-level flows)
python -m shared_core.utils.nse_reports_ingestion --report-type block_deals --auto-date
```

**Result**: 
- Block deals stored in `nse_block_deals` table
- Index-level flows automatically calculated and stored in `nse_index_institutional_flows` table

### 3. Query Index-Level Flows

```python
from clickhouse_driver import Client

clickhouse = Client(host='localhost', database='trading')

# Get NIFTY 50 flows for last 30 days
query = """
SELECT date, buy_value, sell_value, net_value
FROM nse_index_institutional_flows
WHERE index_name = 'NIFTY_50'
  AND date >= today() - 30
ORDER BY date DESC
"""

results = clickhouse.execute(query)
for date, buy, sell, net in results:
    print(f"{date}: Net Flow = {net:.2f} crores")
```

---

## ⚠️ Limitations

### 1. **Derived Metric, Not Direct**
- Index-level flows are **derived** from block deals, not direct FII/DII data
- Block deals represent only **large transactions** (not all institutional activity)
- May not capture all institutional flows (smaller transactions excluded)

### 2. **Index Constituents**
- Currently uses hardcoded index constituents
- Should be updated periodically (indices rebalance quarterly)
- Missing constituents will not be included in calculations

### 3. **Symbol Matching**
- Relies on exact symbol matching
- May miss variations (e.g., "RELIANCE" vs "RELIANCE.NS")
- Should use instrument registry for robust matching

### 4. **Time Lag**
- Block deals are reported after market close
- Index flows calculated when block deals are processed
- Not real-time (daily batch processing)

---

## 🔄 Future Improvements

### 1. **Load Index Constituents Dynamically**
```python
from shared_core.instrument_token.instrument_registry import get_enhanced_instrument_service

# Get index constituents from instrument registry
service = get_enhanced_instrument_service()
nifty_50_constituents = service.get_index_constituents('NIFTY_50')
```

### 2. **Expand Index Coverage**
- Add more indices (NIFTY_MIDCAP, NIFTY_SMALLCAP, etc.)
- Support sectoral indices
- Support custom index definitions

### 3. **Improve Symbol Matching**
- Use instrument registry for robust symbol matching
- Handle symbol variations and exchanges
- Support multiple symbol formats

### 4. **Add Confidence Metrics**
- Track how much of index flow is captured (deal_count / total_constituents)
- Flag when coverage is low
- Provide confidence scores

### 5. **Real-Time Updates**
- Stream block deals as they occur
- Update index flows incrementally
- Provide real-time index flow dashboard

---

## 📝 Summary

### What We Have

✅ **Overall Market FII/DII Flows**: Direct from NSE API (`/api/fiidiiTradeReact`)
✅ **Block Deals**: Direct from NSE API (`/api/block-deals`)
✅ **Index-Level Flows**: Derived from block deals (approximation)

### What NSE Provides

✅ Overall market FII/DII flows
✅ Block deals with symbol-level detail
❌ Direct index-level FII/DII flows (not available)

### Our Solution

✅ Derive index-level flows from block deals
✅ Aggregate by index constituents
✅ Store in `nse_index_institutional_flows` table
✅ Automatic calculation when block deals are processed

### Key Insight

**NSE does not provide direct index-level FII/DII flows**. We derive them by:
1. Fetching block deals (large institutional transactions)
2. Mapping symbols to index constituents
3. Aggregating buy/sell values by index

This is a **reasonable approximation** but not a perfect substitute for direct index-level flows (which don't exist).

---

## 🔗 Related Files

- **Ingestion Script**: `shared_core/utils/nse_reports_ingestion.py`
- **ClickHouse Schema**: `clickhouse_setup/create_nse_reports_tables.sql`
- **Documentation**: `shared_core/utils/NSE_REPORTS_GUIDE.md`
- **Market Intelligence**: `shared_core/market_intelligence/unified_signal_generator.py`

---

## 📞 NSE Support

If you need more granular data:
- Contact NSE Data Services: https://www.nseindia.com/data-services
- Check NSE Data Portal: https://www.nseindia.com/market-data
- Explore NSE APIs: https://www.nseindia.com/api

---

**Last Updated**: November 2025

