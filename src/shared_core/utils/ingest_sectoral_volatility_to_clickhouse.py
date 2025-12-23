#!/opt/homebrew/bin/python3.13
"""
Ingest Sectoral Volatility Data into ClickHouse

Reads from shared_core/config/sector_volatility.json and ingests into ClickHouse
for use by MarketRegimeEngine.

USAGE:
    python -m shared_core.utils.ingest_sectoral_volatility_to_clickhouse
    python -m shared_core.utils.ingest_sectoral_volatility_to_clickhouse --date 2025-01-15

CRON SETUP:
    # Run daily after update_sectoral_volatility.py
    0 19 * * 1-5 cd /path/to/aion_algo_trading && python -m shared_core.utils.ingest_sectoral_volatility_to_clickhouse --auto-date >> logs/sectoral_volatility.log 2>&1
"""

import sys
from pathlib import Path
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
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
        logger.error("No ClickHouse client available. Install clickhouse-driver or clickhouse-connect")

def ensure_sectoral_volatility_table(clickhouse_client):
    """Ensure sectoral_volatility table exists in ClickHouse"""
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sectoral_volatility (
        index_name String,              -- e.g., 'NIFTY_50', 'NIFTY_BANK'
        symbol String,                  -- Trading symbol
        instrument_token UInt32,        -- Instrument token
        current_price Float64,          -- Current index price
        volatility_20d_pct Float64,     -- 20-day volatility percentage
        volatility_55d_pct Float64,     -- 55-day volatility percentage
        data_points UInt32,             -- Number of data points used
        date Date,                          -- Trading date
        last_updated DateTime64(3, 'UTC') DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(date)
    ORDER BY (date, index_name)
    SETTINGS index_granularity = 8192
    """
    
    try:
        if isinstance(clickhouse_client, ClickHouseDriverClient):
            clickhouse_client.execute(create_table_sql)
        else:
            clickhouse_client.command(create_table_sql)
        logger.info("✅ Sectoral volatility table ensured")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to create sectoral_volatility table: {e}")
        return False

def load_sectoral_volatility_json(json_path: Path) -> Dict[str, Any]:
    """Load sectoral volatility data from JSON file"""
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        logger.info(f"✅ Loaded sectoral volatility data from {json_path}")
        return data
    except Exception as e:
        logger.error(f"❌ Failed to load {json_path}: {e}")
        return {}

def parse_sectoral_volatility_data(json_data: Dict[str, Any], date_str: str) -> List[Dict[str, Any]]:
    """Parse sectoral volatility JSON into ClickHouse records"""
    records = []
    
    try:
        index_volatility = json_data.get('index_volatility', {})
        
        for index_name, index_data in index_volatility.items():
            if not isinstance(index_data, dict):
                continue
            
            # Parse date
            try:
                if date_str:
                    trading_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                else:
                    trading_date = datetime.now().date()
            except ValueError:
                trading_date = datetime.now().date()
            
            record = {
                'index_name': index_name,
                'symbol': index_data.get('symbol', index_name),
                'instrument_token': int(index_data.get('instrument_token', 0) or 0),
                'current_price': float(index_data.get('current_price', 0) or 0),
                'volatility_20d_pct': float(index_data.get('volatility_20d_pct', 0) or 0),
                'volatility_55d_pct': float(index_data.get('volatility_55d_pct', 0) or 0),
                'data_points': int(index_data.get('data_points', 0) or 0),
                'date': trading_date,
                'last_updated': datetime.now()
            }
            
            records.append(record)
        
        logger.info(f"✅ Parsed {len(records)} sectoral volatility records")
        return records
        
    except Exception as e:
        logger.error(f"❌ Error parsing sectoral volatility data: {e}")
        return []

def ingest_to_clickhouse(clickhouse_client, records: List[Dict[str, Any]]) -> int:
    """Ingest records into ClickHouse sectoral_volatility table"""
    if not records:
        return 0
    
    table_name = 'sectoral_volatility'
    
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

def get_trading_date(date_str: Optional[str] = None) -> str:
    """Get trading date (skip weekends)"""
    if date_str:
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            target_date = datetime.now()
    else:
        target_date = datetime.now()
    
    # Skip weekends
    while target_date.weekday() >= 5:
        target_date -= timedelta(days=1)
    
    return target_date.strftime('%Y-%m-%d')

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Ingest Sectoral Volatility into ClickHouse")
    parser.add_argument('--date', type=str, default=None,
                       help='Date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--auto-date', action='store_true',
                       help='Automatically use last trading day')
    parser.add_argument('--json-path', type=str, default=None,
                       help='Path to sector_volatility.json (default: shared_core/config/sector_volatility.json)')
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
    parser.add_argument('--cron-mode', action='store_true',
                       help='Cron mode: quiet output, exit codes')
    
    args = parser.parse_args()
    
    # Determine date
    date_str = None
    if args.auto_date:
        date_str = None  # Will use today
    elif args.date:
        date_str = args.date
    
    trading_date = get_trading_date(date_str)
    
    # JSON file path
    if args.json_path:
        json_path = Path(args.json_path)
    else:
        json_path = project_root / 'shared_core' / 'config' / 'sector_volatility.json'
    
    if not json_path.exists():
        logger.error(f"❌ Sectoral volatility JSON not found: {json_path}")
        sys.exit(1)
    
    # Initialize ClickHouse client
    clickhouse_client = None
    try:
        if CLICKHOUSE_DRIVER_AVAILABLE:
            clickhouse_client = ClickHouseDriverClient(
                host=args.clickhouse_host,
                port=args.clickhouse_port,
                user=args.clickhouse_username,
                password=args.clickhouse_password,
                database=args.clickhouse_database
            )
        elif CLICKHOUSE_AVAILABLE:
            clickhouse_client = clickhouse_connect.get_client(
                host=args.clickhouse_host,
                port=args.clickhouse_port,
                username=args.clickhouse_username,
                password=args.clickhouse_password,
                database=args.clickhouse_database
            )
        else:
            logger.error("❌ No ClickHouse client available")
            sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Failed to connect to ClickHouse: {e}")
        sys.exit(1)
    
    # Ensure table exists
    if not ensure_sectoral_volatility_table(clickhouse_client):
        sys.exit(1)
    
    # Load JSON data
    json_data = load_sectoral_volatility_json(json_path)
    if not json_data:
        logger.error("❌ Failed to load sectoral volatility data")
        sys.exit(1)
    
    # Parse data
    records = parse_sectoral_volatility_data(json_data, trading_date)
    if not records:
        logger.warning("⚠️ No records to ingest")
        sys.exit(0)
    
    # Ingest to ClickHouse
    rows_inserted = ingest_to_clickhouse(clickhouse_client, records)
    
    if not args.cron_mode:
        print("\n" + "=" * 70)
        print("SECTORAL VOLATILITY INGESTION SUMMARY")
        print("=" * 70)
        print(f"Date: {trading_date}")
        print(f"Records ingested: {rows_inserted}")
        print(f"Source: {json_path}")
        print("=" * 70)
    
    # Exit code for cron
    if args.cron_mode:
        sys.exit(0 if rows_inserted > 0 else 1)

if __name__ == "__main__":
    main()

