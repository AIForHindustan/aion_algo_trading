#!/opt/homebrew/bin/python3.13
"""
NSE Reports Downloader and ClickHouse Ingestion System

Downloads and ingests NSE reports directly into ClickHouse for analysis.

USEFUL REPORTS FOR TRADING SYSTEM:
==================================

1. **DII/FII Trading Activity** (HIGH PRIORITY)
   - Daily DII/FII buy/sell data
   - Block deals data
   - Bulk deals data
   - Use: Institutional flow analysis, sentiment indicators
   
   **NSE Website Links:**
   - Current Reports: https://www.nseindia.com/reports/fii-dii
   - Historical Reports: https://www.nseindia.com/all-reports/historical-equities-fii-fpi-dii-trading-activity
   - Bulk/Block Deals: https://www.nseindia.com/report-detail/display-bulk-and-block-deals
   
   **Note on Index-Level Flows:**
   - NSE provides overall market DII/FII flows (not index-specific)
   - Index-level flows can be derived by analyzing block deals for index constituents
   - See `_calculate_index_level_flows()` function for derived index flows

2. **Derivatives Reports** (HIGH PRIORITY)
   - Daily derivatives summary
   - F&O turnover
   - Open interest data
   - Use: Options flow analysis, OI-based signals

3. **Market Data Reports** (MEDIUM PRIORITY)
   - Daily market summary
   - Sector-wise performance
   - Top gainers/losers
   - Use: Market regime analysis, sector rotation

4. **Corporate Actions** (MEDIUM PRIORITY)
   - Dividend announcements
   - Bonus issues
   - Stock splits
   - Use: Event-driven trading, ex-date adjustments

5. **Delivery Reports** (MEDIUM PRIORITY)
   - Delivery percentage
   - Delivery volume
   - Use: Quality of move analysis

USAGE:
    python -m shared_core.utils.nse_reports_ingestion --report-type dii_fii --date 2025-01-15
    python -m shared_core.utils.nse_reports_ingestion --report-type block_deals --auto-date
    python -m shared_core.utils.nse_reports_ingestion --report-type all --cron-mode

CRON SETUP:
    # Daily at 6:30 PM IST (after market close)
    30 18 * * 1-5 cd /path/to/aion_algo_trading && python -m shared_core.utils.nse_reports_ingestion --report-type dii_fii --auto-date >> logs/nse_reports.log 2>&1
    
    # Block deals at 7:00 PM IST
    0 19 * * 1-5 cd /path/to/aion_algo_trading && python -m shared_core.utils.nse_reports_ingestion --report-type block_deals --auto-date >> logs/nse_reports.log 2>&1

CREATED: January 2025
"""

import sys
from pathlib import Path
import json
import csv
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
import time
import argparse

# Add project root to path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# ClickHouse imports
try:
    from clickhouse_driver import Client as ClickHouseDriverClient
    CLICKHOUSE_DRIVER_AVAILABLE = True
except ImportError:
    CLICKHOUSE_DRIVER_AVAILABLE = False
    try:
        import clickhouse_connect
        CLICKHOUSE_AVAILABLE = True
    except ImportError:
        CLICKHOUSE_AVAILABLE = False

# NSE Report Types and URLs
NSE_REPORTS = {
    'dii_fii': {
        'name': 'DII/FII Trading Activity',
        'url_template': 'https://www.nseindia.com/api/fiidiiTradeReact',
        'params': {'from': '{date}', 'to': '{date}'},
        'priority': 'HIGH',
        'schedule': 'daily_after_close',
        'description': 'Daily DII/FII buy/sell data for institutional flow analysis'
    },
    'block_deals': {
        'name': 'Block Deals',
        'url_template': 'https://www.nseindia.com/api/block-deals',
        'params': {'from': '{date}', 'to': '{date}'},
        'priority': 'HIGH',
        'schedule': 'daily_after_close',
        'description': 'Block deals data for large institutional transactions'
    },
    'bulk_deals': {
        'name': 'Bulk Deals',
        'url_template': 'https://www.nseindia.com/api/bulk-deals',
        'params': {'from': '{date}', 'to': '{date}'},
        'priority': 'HIGH',
        'schedule': 'daily_after_close',
        'description': 'Bulk deals data for significant transactions'
    },
    'derivatives_summary': {
        'name': 'Derivatives Summary',
        'url_template': 'https://www.nseindia.com/api/derivatives-summary',
        'params': {'date': '{date}'},
        'priority': 'HIGH',
        'schedule': 'daily_after_close',
        'description': 'Daily derivatives summary for F&O analysis'
    },
    'oi_data': {
        'name': 'Open Interest Data',
        'url_template': 'https://www.nseindia.com/api/oi-data',
        'params': {'date': '{date}'},
        'priority': 'HIGH',
        'schedule': 'daily_after_close',
        'description': 'Open interest data for options flow analysis'
    },
    'market_summary': {
        'name': 'Market Summary',
        'url_template': 'https://www.nseindia.com/api/market-summary',
        'params': {'date': '{date}'},
        'priority': 'MEDIUM',
        'schedule': 'daily_after_close',
        'description': 'Daily market summary for regime analysis'
    },
    'delivery_data': {
        'name': 'Delivery Data',
        'url_template': 'https://www.nseindia.com/api/delivery-data',
        'params': {'date': '{date}'},
        'priority': 'MEDIUM',
        'schedule': 'daily_after_close',
        'description': 'Delivery percentage and volume for quality analysis'
    },
    'corporate_actions': {
        'name': 'Corporate Actions',
        'url_template': 'https://www.nseindia.com/api/corporate-actions',
        'params': {'from': '{date}', 'to': '{date}'},
        'priority': 'MEDIUM',
        'schedule': 'daily_after_close',
        'description': 'Corporate actions (dividends, bonus, splits) for event trading'
    }
}

def get_nse_session() -> requests.Session:
    """Create a requests session with proper headers for NSE website"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.nseindia.com/',
        'Origin': 'https://www.nseindia.com',
    })
    return session

def get_trading_date(date_str: Optional[str] = None) -> str:
    """
    Get trading date (skip weekends/holidays)
    
    ARGS:
        date_str: Date string in YYYY-MM-DD format (default: today)
        
    RETURNS:
        str: Trading date in DD-MM-YYYY format (NSE format)
    """
    if date_str:
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            target_date = datetime.now()
    else:
        target_date = datetime.now()
    
    # Skip weekends (go back to last Friday if weekend)
    while target_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
        target_date -= timedelta(days=1)
    
    # Format for NSE (DD-MM-YYYY)
    return target_date.strftime('%d-%m-%Y')

def fetch_nse_report(session: requests.Session, report_type: str, date_str: str) -> Optional[Dict[str, Any]]:
    """
    Fetch a specific NSE report
    
    ARGS:
        session: requests.Session with NSE headers
        report_type: Type of report (key from NSE_REPORTS)
        date_str: Date in DD-MM-YYYY format
        
    RETURNS:
        dict: Report data or None if failed
    """
    if report_type not in NSE_REPORTS:
        logger.error(f"Unknown report type: {report_type}")
        return None
    
    report_config = NSE_REPORTS[report_type]
    url = report_config['url_template']
    
    # Prepare parameters
    params = {}
    for key, value_template in report_config['params'].items():
        params[key] = value_template.format(date=date_str)
    
    try:
        # First, visit main page to get cookies
        session.get('https://www.nseindia.com/', timeout=10)
        time.sleep(1)
        
        # Fetch report
        logger.info(f"📥 Fetching {report_config['name']} for {date_str}...")
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"✅ Successfully fetched {report_config['name']}")
        return {
            'report_type': report_type,
            'report_name': report_config['name'],
            'date': date_str,
            'data': data,
            'fetched_at': datetime.now().isoformat()
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error fetching {report_config['name']}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"   Response status: {e.response.status_code}")
            logger.error(f"   Response text: {e.response.text[:200]}")
        return None
    except Exception as e:
        logger.error(f"❌ Error parsing {report_config['name']}: {e}")
        return None

def parse_dii_fii_data(report_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parse DII/FII trading data"""
    parsed_records = []
    
    try:
        # NSE API returns a list directly, not a dict with 'data' key
        data = report_data.get('data', [])
        
        # Handle both list and dict responses
        if isinstance(data, list):
            # Direct list response (current NSE API format)
            for record in data:
                category_raw = record.get('category', '')
                # Extract category from string like "DII **" or "FII/FPI *"
                if 'DII' in category_raw:
                    category = 'DII'
                elif 'FII' in category_raw or 'FPI' in category_raw:
                    category = 'FII'
                else:
                    category = category_raw.split()[0] if category_raw else 'UNKNOWN'
                
                # Convert date from "28-Nov-2025" to "28-11-2025" format
                date_str = record.get('date', report_data.get('date', ''))
                # Parse and reformat date
                try:
                    if date_str:
                        date_obj = datetime.strptime(date_str, '%d-%b-%Y')
                        formatted_date = date_obj.strftime('%d-%m-%Y')
                    else:
                        formatted_date = report_data.get('date', '')
                except:
                    formatted_date = report_data.get('date', '')
                
                parsed_records.append({
                    'category': category,
                    'date': formatted_date,
                    'buy_value': float(record.get('buyValue', 0) or 0),
                    'sell_value': float(record.get('sellValue', 0) or 0),
                    'net_value': float(record.get('netValue', 0) or 0),
                    'category_name': category_raw.strip(' *'),
                    'fetched_at': report_data['fetched_at']
                })
        elif isinstance(data, dict):
            # Legacy format (dict with nested data)
            dii_data = data.get('data', [])
            if isinstance(dii_data, list):
                for record in dii_data:
                    parsed_records.append({
                        'category': 'DII',
                        'date': report_data['date'],
                        'buy_value': float(record.get('buyValue', 0) or 0),
                        'sell_value': float(record.get('sellValue', 0) or 0),
                        'net_value': float(record.get('netValue', 0) or 0),
                        'category_name': record.get('category', 'DII'),
                        'fetched_at': report_data['fetched_at']
                    })
            
            fii_data = data.get('fiiData', [])
            if isinstance(fii_data, list):
                for record in fii_data:
                    parsed_records.append({
                        'category': 'FII',
                        'date': report_data['date'],
                        'buy_value': float(record.get('buyValue', 0) or 0),
                        'sell_value': float(record.get('sellValue', 0) or 0),
                        'net_value': float(record.get('netValue', 0) or 0),
                        'category_name': record.get('category', 'FII'),
                        'fetched_at': report_data['fetched_at']
                    })
        
    except Exception as e:
        logger.error(f"Error parsing DII/FII data: {e}", exc_info=True)
    
    return parsed_records

def parse_block_deals_data(report_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parse block deals data"""
    parsed_records = []
    
    try:
        data = report_data.get('data', {})
        deals = data.get('data', [])
        
        if isinstance(deals, list):
            for deal in deals:
                parsed_records.append({
                    'symbol': deal.get('SYMBOL', ''),
                    'security_name': deal.get('SECURITY_NAME', ''),
                    'client_name': deal.get('CLIENT_NAME', ''),
                    'deal_type': deal.get('DEAL_TYPE', ''),
                    'quantity': int(deal.get('QTY', 0) or 0),
                    'price': float(deal.get('PRICE', 0) or 0),
                    'value': float(deal.get('VALUE', 0) or 0),
                    'date': report_data['date'],
                    'fetched_at': report_data['fetched_at']
                })
        
    except Exception as e:
        logger.error(f"Error parsing block deals data: {e}")
    
    return parsed_records

def parse_derivatives_summary(report_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parse derivatives summary data"""
    parsed_records = []
    
    try:
        data = report_data.get('data', {})
        
        # Extract key metrics
        parsed_records.append({
            'date': report_data['date'],
            'futures_turnover': float(data.get('futuresTurnover', 0) or 0),
            'options_turnover': float(data.get('optionsTurnover', 0) or 0),
            'total_turnover': float(data.get('totalTurnover', 0) or 0),
            'futures_oi': float(data.get('futuresOI', 0) or 0),
            'options_oi': float(data.get('optionsOI', 0) or 0),
            'total_oi': float(data.get('totalOI', 0) or 0),
            'fetched_at': report_data['fetched_at']
        })
        
    except Exception as e:
        logger.error(f"Error parsing derivatives summary: {e}")
    
    return parsed_records

def calculate_index_level_flows(clickhouse_client, date_str: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Calculate index-level institutional flows from block deals
    
    NOTE: NSE does not provide direct index-level FII/DII flows.
    This function derives index-level flows by:
    1. Getting block deals for the date
    2. Identifying which stocks belong to which indices
    3. Aggregating buy/sell values by index
    
    ARGS:
        clickhouse_client: ClickHouse client instance
        date_str: Date in YYYY-MM-DD format (default: today)
        
    RETURNS:
        List of index-level flow records
    """
    if not clickhouse_client:
        logger.warning("ClickHouse client not available for index-level flow calculation")
        return []
    
    try:
        # Get trading date
        nse_date = get_trading_date(date_str)
        # Convert to YYYY-MM-DD for ClickHouse
        if date_str:
            try:
                target_date = datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                target_date = datetime.now()
        else:
            target_date = datetime.now()
        clickhouse_date = target_date.strftime('%Y-%m-%d')
        
        # Index constituents mapping (simplified - should be loaded from instrument registry)
        # For now, using common indices
        index_constituents = {
            'NIFTY_50': ['RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'HINDUNILVR', 'ICICIBANK', 
                        'KOTAKBANK', 'BHARTIARTL', 'LT', 'SBIN', 'AXISBANK', 'ASIANPAINT',
                        'MARUTI', 'TITAN', 'ULTRACEMCO', 'NESTLEIND', 'BAJFINANCE', 'WIPRO',
                        'ONGC', 'TATAMOTORS', 'JSWSTEEL', 'POWERGRID', 'M&M', 'ADANIENT',
                        'TATASTEEL', 'HCLTECH', 'ITC', 'NTPC', 'INDUSINDBK', 'SUNPHARMA',
                        'COALINDIA', 'TECHM', 'GRASIM', 'DIVISLAB', 'BAJAJFINSV', 'CIPLA',
                        'HDFCLIFE', 'EICHERMOT', 'DRREDDY', 'BPCL', 'HEROMOTOCO', 'APOLLOHOSP',
                        'SBILIFE', 'BRITANNIA', 'HDFC', 'ADANIPORTS', 'TATACONSUM', 'VEDL',
                        'PIDILITIND', 'HINDALCO', 'MARICO'],
            'NIFTY_BANK': ['HDFCBANK', 'ICICIBANK', 'KOTAKBANK', 'AXISBANK', 'SBIN', 
                          'INDUSINDBK', 'PNB', 'BANKBARODA', 'FEDERALBNK', 'IDFCFIRSTB',
                          'AUBANK', 'BANDHANBNK', 'RBLBANK', 'YESBANK'],
            'NIFTY_IT': ['TCS', 'INFY', 'HCLTECH', 'WIPRO', 'TECHM', 'LTIM', 'MPHASIS',
                        'PERSISTENT', 'COFORGE', 'MINDTREE']
        }
        
        # Query block deals for the date
        query = """
        SELECT symbol, deal_type, value
        FROM nse_block_deals
        WHERE date = %(date)s
        """
        
        try:
            if CLICKHOUSE_DRIVER_AVAILABLE and isinstance(clickhouse_client, ClickHouseDriverClient):
                results = clickhouse_client.execute(query, {'date': clickhouse_date})
            elif CLICKHOUSE_AVAILABLE:
                results = clickhouse_client.query(query, parameters={'date': clickhouse_date}).result_rows
            else:
                # Fallback: try execute method
                results = clickhouse_client.execute(query, [clickhouse_date])
        except Exception as e:
            logger.error(f"Error querying block deals: {e}")
            return []
        
        if not results:
            logger.info(f"No block deals found for {clickhouse_date}")
            return []
        
        # Aggregate flows by index
        index_flows = {}
        for symbol, deal_type, value in results:
            symbol_upper = symbol.upper()
            
            # Find which indices this stock belongs to
            for index_name, constituents in index_constituents.items():
                if symbol_upper in constituents or any(symbol_upper.startswith(c) for c in constituents):
                    if index_name not in index_flows:
                        index_flows[index_name] = {
                            'index_name': index_name,
                            'date': clickhouse_date,
                            'buy_value': 0.0,
                            'sell_value': 0.0,
                            'net_value': 0.0,
                            'deal_count': 0,
                            'fetched_at': datetime.now().isoformat()
                        }
                    
                    if deal_type.upper() == 'BUY':
                        index_flows[index_name]['buy_value'] += float(value or 0)
                    elif deal_type.upper() == 'SELL':
                        index_flows[index_name]['sell_value'] += float(value or 0)
                    
                    index_flows[index_name]['deal_count'] += 1
        
        # Calculate net values
        for index_name, flow_data in index_flows.items():
            flow_data['net_value'] = flow_data['buy_value'] - flow_data['sell_value']
        
        logger.info(f"Calculated index-level flows for {len(index_flows)} indices")
        return list(index_flows.values())
        
    except Exception as e:
        logger.error(f"Error calculating index-level flows: {e}")
        return []

def ensure_clickhouse_tables(clickhouse_client):
    """Ensure ClickHouse tables exist for NSE reports"""
    
    # DII/FII Trading Activity Table
    dii_fii_table_sql = """
    CREATE TABLE IF NOT EXISTS nse_dii_fii_trading (
        category String,
        date Date,
        buy_value Float64,
        sell_value Float64,
        net_value Float64,
        category_name String,
        fetched_at DateTime64(3, 'UTC') DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(date)
    ORDER BY (date, category)
    SETTINGS index_granularity = 8192
    """
    
    # Index-Level Institutional Flows Table (derived from block deals)
    index_flows_table_sql = """
    CREATE TABLE IF NOT EXISTS nse_index_institutional_flows (
        index_name String,
        date Date,
        buy_value Float64,
        sell_value Float64,
        net_value Float64,
        deal_count UInt32,
        fetched_at DateTime64(3, 'UTC') DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(date)
    ORDER BY (date, index_name)
    SETTINGS index_granularity = 8192
    """
    
    # Block Deals Table
    block_deals_table_sql = """
    CREATE TABLE IF NOT EXISTS nse_block_deals (
        symbol String,
        security_name String,
        client_name String,
        deal_type String,
        quantity UInt64,
        price Float64,
        value Float64,
        date Date,
        fetched_at DateTime64(3, 'UTC') DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(date)
    ORDER BY (date, symbol, client_name)
    SETTINGS index_granularity = 8192
    """
    
    # Derivatives Summary Table
    derivatives_summary_table_sql = """
    CREATE TABLE IF NOT EXISTS nse_derivatives_summary (
        date Date,
        futures_turnover Float64,
        options_turnover Float64,
        total_turnover Float64,
        futures_oi Float64,
        options_oi Float64,
        total_oi Float64,
        fetched_at DateTime64(3, 'UTC') DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(date)
    ORDER BY date
    SETTINGS index_granularity = 8192
    """
    
    try:
        if isinstance(clickhouse_client, ClickHouseDriverClient):
            clickhouse_client.execute(dii_fii_table_sql)
            clickhouse_client.execute(block_deals_table_sql)
            clickhouse_client.execute(derivatives_summary_table_sql)
            clickhouse_client.execute(index_flows_table_sql)
        else:
            clickhouse_client.command(dii_fii_table_sql)
            clickhouse_client.command(block_deals_table_sql)
            clickhouse_client.command(derivatives_summary_table_sql)
            clickhouse_client.command(index_flows_table_sql)
        
        logger.info("✅ ClickHouse tables ensured")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to create ClickHouse tables: {e}")
        return False

def ingest_to_clickhouse(clickhouse_client, table_name: str, records: List[Dict[str, Any]]) -> int:
    """
    Ingest records into ClickHouse
    
    ARGS:
        clickhouse_client: ClickHouse client instance
        table_name: Target table name
        records: List of records to insert
        
    RETURNS:
        int: Number of records inserted
    """
    if not records:
        return 0
    
    try:
        if isinstance(clickhouse_client, ClickHouseDriverClient):
            # clickhouse_driver: Convert to list of tuples
            columns = list(records[0].keys())
            data = [tuple(record.get(col) for col in columns) for record in records]
            clickhouse_client.execute(
                f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES",
                data
            )
        else:
            # clickhouse-connect: Use insert method
            import pandas as pd
            df = pd.DataFrame(records)
            clickhouse_client.insert_df(table_name, df)
        
        logger.info(f"✅ Ingested {len(records)} records into {table_name}")
        return len(records)
        
    except Exception as e:
        logger.error(f"❌ Failed to ingest into {table_name}: {e}")
        return 0

def process_report(report_type: str, date_str: Optional[str] = None, 
                  clickhouse_config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Process a single NSE report: fetch, parse, and ingest
    
    ARGS:
        report_type: Type of report to process
        date_str: Date in YYYY-MM-DD format (None = today)
        clickhouse_config: ClickHouse connection config
        
    RETURNS:
        dict: Processing statistics
    """
    stats = {
        'report_type': report_type,
        'date': date_str or get_trading_date(),
        'fetched': False,
        'parsed': False,
        'ingested': False,
        'records_count': 0
    }
    
    # Get trading date in NSE format
    nse_date = get_trading_date(date_str)
    stats['nse_date'] = nse_date
    
    # Initialize ClickHouse client
    clickhouse_client = None
    if clickhouse_config:
        try:
            if CLICKHOUSE_DRIVER_AVAILABLE:
                clickhouse_client = ClickHouseDriverClient(
                    host=clickhouse_config.get('host', 'localhost'),
                    port=clickhouse_config.get('port', 9000),
                    user=clickhouse_config.get('username', 'default'),
                    password=clickhouse_config.get('password', ''),
                    database=clickhouse_config.get('database', 'trading')
                )
            elif CLICKHOUSE_AVAILABLE:
                clickhouse_client = clickhouse_connect.get_client(
                    host=clickhouse_config.get('host', 'localhost'),
                    port=clickhouse_config.get('port', 8123),
                    username=clickhouse_config.get('username', 'default'),
                    password=clickhouse_config.get('password', ''),
                    database=clickhouse_config.get('database', 'trading')
                )
        except Exception as e:
            logger.error(f"❌ Failed to connect to ClickHouse: {e}")
            clickhouse_client = None
    
    # Fetch report
    session = get_nse_session()
    report_data = fetch_nse_report(session, report_type, nse_date)
    
    if not report_data:
        return stats
    
    stats['fetched'] = True
    
    # Parse report data
    parsed_records = []
    if report_type == 'dii_fii':
        parsed_records = parse_dii_fii_data(report_data)
    elif report_type == 'block_deals':
        parsed_records = parse_block_deals_data(report_data)
    elif report_type == 'bulk_deals':
        parsed_records = parse_block_deals_data(report_data)  # Similar structure
    elif report_type == 'derivatives_summary':
        parsed_records = parse_derivatives_summary(report_data)
    # Add more parsers as needed
    
    if parsed_records:
        stats['parsed'] = True
        stats['records_count'] = len(parsed_records)
    
    # Ingest to ClickHouse
    if clickhouse_client and parsed_records:
        # Ensure tables exist
        ensure_clickhouse_tables(clickhouse_client)
        
        # Map report types to table names
        table_mapping = {
            'dii_fii': 'nse_dii_fii_trading',
            'block_deals': 'nse_block_deals',
            'bulk_deals': 'nse_block_deals',  # Use same table
            'derivatives_summary': 'nse_derivatives_summary'
        }
        
        table_name = table_mapping.get(report_type)
        if table_name:
            rows_inserted = ingest_to_clickhouse(clickhouse_client, table_name, parsed_records)
            if rows_inserted > 0:
                stats['ingested'] = True
        
        # Calculate and ingest index-level flows if block deals were processed
        if report_type in ['block_deals', 'bulk_deals'] and rows_inserted > 0:
            logger.info("📊 Calculating index-level institutional flows from block deals...")
            index_flows = calculate_index_level_flows(clickhouse_client, date_str)
            if index_flows:
                index_rows = ingest_to_clickhouse(clickhouse_client, 'nse_index_institutional_flows', index_flows)
                if index_rows > 0:
                    logger.info(f"✅ Ingested {index_rows} index-level flow records")
                    stats['index_flows_count'] = index_rows
    
    return stats

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="NSE Reports Downloader and ClickHouse Ingestion")
    parser.add_argument('--report-type', type=str, required=True,
                       choices=list(NSE_REPORTS.keys()) + ['all'],
                       help='Type of report to download')
    parser.add_argument('--date', type=str, default=None,
                       help='Date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--auto-date', action='store_true',
                       help='Automatically use last trading day')
    parser.add_argument('--cron-mode', action='store_true',
                       help='Cron mode: quiet output, exit codes')
    parser.add_argument('--clickhouse-host', default='localhost',
                       help='ClickHouse host')
    parser.add_argument('--clickhouse-port', type=int, default=9000,
                       help='ClickHouse port')
    parser.add_argument('--clickhouse-database', default='trading',
                       help='ClickHouse database')
    parser.add_argument('--clickhouse-username', default='default',
                       help='ClickHouse username')
    parser.add_argument('--clickhouse-password', default='',
                       help='ClickHouse password')
    parser.add_argument('--calculate-index-flows', action='store_true',
                       help='Calculate index-level flows from block deals (requires block_deals to be fetched)')
    
    args = parser.parse_args()
    
    # ClickHouse config
    clickhouse_config = {
        'host': args.clickhouse_host,
        'port': args.clickhouse_port,
        'database': args.clickhouse_database,
        'username': args.clickhouse_username,
        'password': args.clickhouse_password
    }
    
    # Determine date
    date_str = None
    if args.auto_date:
        date_str = None  # Will use today
    elif args.date:
        date_str = args.date
    
    # Process reports
    if args.report_type == 'all':
        # Process all HIGH priority reports
        high_priority_reports = [k for k, v in NSE_REPORTS.items() if v['priority'] == 'HIGH']
        
        all_stats = []
        for report_type in high_priority_reports:
            stats = process_report(report_type, date_str, clickhouse_config)
            all_stats.append(stats)
            time.sleep(2)  # Rate limiting
        
        # Summary
        if not args.cron_mode:
            print("\n" + "=" * 70)
            print("NSE REPORTS INGESTION SUMMARY")
            print("=" * 70)
            for stats in all_stats:
                status = "✅" if stats['ingested'] else "❌"
                print(f"{status} {stats['report_type']}: {stats['records_count']} records")
            print("=" * 70)
        
        # Exit code for cron
        if args.cron_mode:
            all_success = all(s['ingested'] for s in all_stats)
            sys.exit(0 if all_success else 1)
    else:
        # Process single report
        stats = process_report(args.report_type, date_str, clickhouse_config)
        
        if not args.cron_mode:
            print("\n" + "=" * 70)
            print(f"NSE REPORT: {stats['report_type']}")
            print("=" * 70)
            print(f"Date: {stats['nse_date']}")
            print(f"Fetched: {'✅' if stats['fetched'] else '❌'}")
            print(f"Parsed: {'✅' if stats['parsed'] else '❌'}")
            print(f"Ingested: {'✅' if stats['ingested'] else '❌'}")
            print(f"Records: {stats['records_count']}")
            print("=" * 70)
        
        # Exit code for cron
        if args.cron_mode:
            sys.exit(0 if stats['ingested'] else 1)

if __name__ == "__main__":
    main()

