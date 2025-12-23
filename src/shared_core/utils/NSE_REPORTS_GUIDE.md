# NSE Reports Ingestion Guide

## Overview

This system downloads and ingests NSE reports directly into ClickHouse for analysis. The reports provide institutional flow data, derivatives activity, and market summaries that are critical for trading system analysis.

## Useful Reports for Trading System

### HIGH PRIORITY (Daily Ingestion Recommended)

1. **DII/FII Trading Activity**
   - **Use Case**: Institutional flow analysis, sentiment indicators
   - **Key Metrics**: Daily buy/sell values, net flow
   - **Trading Signals**: 
     - Strong DII buying = bullish signal
     - FII selling = bearish signal
     - Divergence between DII/FII = contrarian opportunity

2. **Block Deals**
   - **Use Case**: Large institutional transactions, insider activity
   - **Key Metrics**: Deal size, price, client names
   - **Trading Signals**:
     - Large block deals = institutional interest
     - Price premium/discount = sentiment indicator

3. **Bulk Deals**
   - **Use Case**: Significant transactions, promoter activity
   - **Key Metrics**: Similar to block deals but different threshold
   - **Trading Signals**: Same as block deals

4. **Derivatives Summary**
   - **Use Case**: F&O market analysis, options flow
   - **Key Metrics**: Turnover, open interest
   - **Trading Signals**:
     - High OI = strong conviction
     - OI changes = flow direction

5. **Open Interest Data**
   - **Use Case**: Options flow analysis, max pain calculation
   - **Key Metrics**: Strike-wise OI, call/put ratios
   - **Trading Signals**:
     - PCR (Put-Call Ratio) = sentiment
     - OI concentration = support/resistance levels

### MEDIUM PRIORITY (Weekly/Monthly Ingestion)

6. **Market Summary**
   - **Use Case**: Market regime analysis, sector rotation
   - **Key Metrics**: Index levels, advances/declines
   - **Trading Signals**: Market breadth indicators

7. **Delivery Data**
   - **Use Case**: Quality of move analysis
   - **Key Metrics**: Delivery percentage, delivery volume
   - **Trading Signals**:
     - High delivery = strong conviction
     - Low delivery = weak move (may reverse)

8. **Corporate Actions**
   - **Use Case**: Event-driven trading, ex-date adjustments
   - **Key Metrics**: Dividends, bonus, splits
   - **Trading Signals**: Ex-date effects, dividend capture

## Installation

```bash
# Install dependencies
pip install requests clickhouse-driver

# Or use clickhouse-connect
pip install requests clickhouse-connect
```

## Usage

### Single Report

```bash
# Download DII/FII data for today
python -m shared_core.utils.nse_reports_ingestion --report-type dii_fii --auto-date

# Download block deals for specific date
python -m shared_core.utils.nse_reports_ingestion --report-type block_deals --date 2025-01-15

# Download all HIGH priority reports
python -m shared_core.utils.nse_reports_ingestion --report-type all --auto-date
```

### Cron Job Setup

Create cron jobs for automated daily ingestion:

```bash
# Edit crontab
crontab -e

# Add these entries (adjust paths as needed):
# Daily DII/FII at 6:30 PM IST (after market close)
30 18 * * 1-5 cd /path/to/aion_algo_trading && /usr/bin/python3 -m shared_core.utils.nse_reports_ingestion --report-type dii_fii --auto-date --cron-mode >> logs/nse_reports.log 2>&1

# Block deals at 7:00 PM IST
0 19 * * 1-5 cd /path/to/aion_algo_trading && /usr/bin/python3 -m shared_core.utils.nse_reports_ingestion --report-type block_deals --auto-date --cron-mode >> logs/nse_reports.log 2>&1

# Derivatives summary at 6:45 PM IST
45 18 * * 1-5 cd /path/to/aion_algo_trading && /usr/bin/python3 -m shared_core.utils.nse_reports_ingestion --report-type derivatives_summary --auto-date --cron-mode >> logs/nse_reports.log 2>&1

# All HIGH priority reports at 7:30 PM IST
30 19 * * 1-5 cd /path/to/aion_algo_trading && /usr/bin/python3 -m shared_core.utils.nse_reports_ingestion --report-type all --auto-date --cron-mode >> logs/nse_reports.log 2>&1
```

## ClickHouse Tables

Tables are automatically created on first run. To create manually:

```bash
clickhouse-client < clickhouse_setup/create_nse_reports_tables.sql
```

### Table Schemas

- `nse_dii_fii_trading`: DII/FII daily trading activity
- `nse_block_deals`: Block deals transactions
- `nse_bulk_deals`: Bulk deals transactions
- `nse_derivatives_summary`: Daily derivatives summary
- `nse_market_summary`: Daily market summary
- `nse_delivery_data`: Stock delivery data
- `nse_corporate_actions`: Corporate actions (dividends, bonus, splits)

## Query Examples

### Get DII/FII Net Flow for Last 30 Days

```sql
SELECT 
    date,
    category,
    net_value
FROM nse_dii_fii_trading
WHERE date >= today() - 30
ORDER BY date DESC, category;
```

### Top Block Deals by Value

```sql
SELECT 
    date,
    symbol,
    client_name,
    deal_type,
    value
FROM nse_block_deals
WHERE date >= today() - 7
ORDER BY value DESC
LIMIT 20;
```

### Derivatives Turnover Trend

```sql
SELECT 
    date,
    total_turnover,
    futures_turnover,
    options_turnover
FROM nse_derivatives_summary
WHERE date >= today() - 30
ORDER BY date DESC;
```

## Integration with Trading System

### Use in Pattern Detection

```python
from shared_core.utils.nse_reports_ingestion import get_dii_fii_flow
from clickhouse_driver import Client

# Get recent DII/FII flow
ch_client = Client(host='localhost', port=9000, database='trading')
dii_flow = ch_client.execute(
    "SELECT net_value FROM nse_dii_fii_trading WHERE category='DII' AND date >= today() - 5 ORDER BY date DESC"
)

# Use in pattern confidence calculation
if dii_flow and dii_flow[0][0] > 1000:  # Strong DII buying
    confidence_boost = 0.05  # Increase confidence
```

### Use in Alert Filtering

```python
# Filter alerts based on institutional flow
def should_alert_with_institutional_flow(symbol, alert_data):
    # Check recent block deals
    block_deals = ch_client.execute(
        "SELECT value FROM nse_block_deals WHERE symbol=? AND date >= today() - 3",
        [symbol]
    )
    
    if block_deals and block_deals[0][0] > 100:  # Large block deal
        return True  # High institutional interest
    
    return False
```

## Troubleshooting

### NSE API Rate Limiting

If you encounter rate limiting:
- Increase delays between requests (currently 2 seconds)
- Use different IP addresses for different reports
- Schedule reports at different times

### Missing Data

If reports are missing:
- Check if market was open on that date
- Verify NSE website structure hasn't changed
- Check logs for API errors

### ClickHouse Connection Issues

If ClickHouse ingestion fails:
- Verify ClickHouse is running: `clickhouse-client --query "SELECT 1"`
- Check credentials in config
- Verify table schemas match

## Future Enhancements

1. **Real-time Updates**: WebSocket integration for live updates
2. **Historical Backfill**: Download historical reports
3. **Advanced Analytics**: Pre-computed indicators (DII/FII divergence, etc.)
4. **Alert Integration**: Automatic alerts on significant institutional activity
5. **Sector-wise Analysis**: Aggregate by sector for sector rotation signals

