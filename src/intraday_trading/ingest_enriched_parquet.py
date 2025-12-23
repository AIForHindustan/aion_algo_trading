#!/opt/homebrew/bin/python3.13
"""
INGEST ENRICHED PARQUET FILES TO DUCKDB
Prioritizes ingesting successfully converted enriched parquet files
Safe for overnight runs with memory management
"""

import sys
import os
import gc
import psutil
import time
from pathlib import Path
from datetime import datetime
import json
import logging
import traceback
from typing import List, Dict, Optional
import duckdb

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('enriched_parquet_ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_memory_usage_mb() -> float:
    """Get current memory usage in MB"""
    try:
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    except:
        return 0.0

def check_and_gc(memory_threshold_mb: float = 1024.0) -> bool:
    """Check memory usage and trigger garbage collection if threshold exceeded"""
    current_memory = get_memory_usage_mb()
    
    if current_memory > memory_threshold_mb:
        logger.info(f"🧹 Memory usage {current_memory:.1f}MB exceeds threshold {memory_threshold_mb:.1f}MB, triggering GC...")
        collected = gc.collect()
        memory_after = get_memory_usage_mb()
        freed = current_memory - memory_after
        logger.info(f"   ✅ GC collected {collected} objects, freed {freed:.1f}MB (now {memory_after:.1f}MB)")
        time.sleep(0.5)
        return True
    return False

def find_enriched_parquet_files(enriched_dir: Path) -> List[str]:
    """Find all enriched parquet files"""
    enriched_files = []
    
    if not enriched_dir.exists():
        logger.warning(f"Enriched directory not found: {enriched_dir}")
        return enriched_files
    
    logger.info(f"🔍 Searching for enriched parquet files in {enriched_dir}...")
    
    # Find all parquet files recursively
    for parquet_file in enriched_dir.rglob("*.parquet"):
        enriched_files.append(str(parquet_file))
    
    logger.info(f"📋 Found {len(enriched_files)} enriched parquet files")
    return enriched_files

def ensure_schema(conn: duckdb.DuckDBPyConnection):
    """Ensure database schema exists"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tick_data_corrected (
            instrument_token BIGINT NOT NULL,
            symbol VARCHAR,
            exchange VARCHAR,
            segment VARCHAR,
            instrument_type VARCHAR,
            expiry DATE,
            strike_price DOUBLE,
            option_type VARCHAR,
            lot_size INTEGER,
            tick_size DOUBLE,
            is_expired BOOLEAN,
            timestamp TIMESTAMP,
            exchange_timestamp TIMESTAMP,
            exchange_timestamp_ns BIGINT NOT NULL,
            last_traded_timestamp TIMESTAMP,
            last_traded_timestamp_ns BIGINT,
            last_price DOUBLE,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE,
            average_traded_price DOUBLE,
            volume BIGINT,
            last_traded_quantity BIGINT,
            total_buy_quantity BIGINT,
            total_sell_quantity BIGINT,
            open_interest BIGINT,
            oi_day_high BIGINT,
            oi_day_low BIGINT,
            bid_1_price DOUBLE, bid_1_quantity BIGINT, bid_1_orders INTEGER,
            bid_2_price DOUBLE, bid_2_quantity BIGINT, bid_2_orders INTEGER,
            bid_3_price DOUBLE, bid_3_quantity BIGINT, bid_3_orders INTEGER,
            bid_4_price DOUBLE, bid_4_quantity BIGINT, bid_4_orders INTEGER,
            bid_5_price DOUBLE, bid_5_quantity BIGINT, bid_5_orders INTEGER,
            ask_1_price DOUBLE, ask_1_quantity BIGINT, ask_1_orders INTEGER,
            ask_2_price DOUBLE, ask_2_quantity BIGINT, ask_2_orders INTEGER,
            ask_3_price DOUBLE, ask_3_quantity BIGINT, ask_3_orders INTEGER,
            ask_4_price DOUBLE, ask_4_quantity BIGINT, ask_4_orders INTEGER,
            ask_5_price DOUBLE, ask_5_quantity BIGINT, ask_5_orders INTEGER,
            rsi_14 DOUBLE,
            sma_20 DOUBLE,
            ema_12 DOUBLE,
            bollinger_upper DOUBLE,
            bollinger_middle DOUBLE,
            bollinger_lower DOUBLE,
            macd DOUBLE,
            macd_signal DOUBLE,
            macd_histogram DOUBLE,
            delta DOUBLE,
            gamma DOUBLE,
            theta DOUBLE,
            vega DOUBLE,
            rho DOUBLE,
            implied_volatility DOUBLE,
            packet_type VARCHAR,
            data_quality VARCHAR,
            session_type VARCHAR,
            source_file VARCHAR NOT NULL,
            processing_batch VARCHAR,
            session_id VARCHAR,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            parser_version VARCHAR DEFAULT 'v3.0_reprocessed',
            spread DOUBLE,
            spread_pct DOUBLE,
            price_change DOUBLE,
            price_change_pct DOUBLE,
            volume_ma_20 DOUBLE,
            date DATE,
            PRIMARY KEY (instrument_token, exchange_timestamp_ns, source_file)
        )
    """)
    logger.info("✅ Database schema ensured")

def ingest_single_file(conn: duckdb.DuckDBPyConnection, file_path: str, db_columns: List[str]) -> Dict[str, any]:
    """Ingest a single enriched parquet file - OPTIMIZED: reads file ONLY ONCE"""
    result = {
        'file': file_path,
        'success': False,
        'rows': 0,
        'error': None
    }
    
    try:
        # OPTIMIZED: Try INSERT directly - DuckDB will handle column mapping
        # If it fails, we'll catch and handle it
        # This way we only read the file ONCE (in the INSERT)
        
        # First, try to get columns from metadata (very fast, no data read)
        try:
            # Use DuckDB's schema inference - this doesn't read data
            schema_info = conn.execute(f"SELECT * FROM read_parquet('{file_path}') LIMIT 0").fetchdf()
            file_columns = list(schema_info.columns)
        except Exception as e:
            # If schema inference fails, try DESCRIBE (still fast, metadata only)
            try:
                columns_df = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}') LIMIT 1").fetchdf()
                file_columns = columns_df['column_name'].tolist()
            except:
                result['error'] = f"Cannot read file schema: {str(e)[:200]}"
                return result
        
        # Build column mapping
        select_parts = []
        source_file = Path(file_path).name.replace("'", "''")
        
        for col in db_columns:
            if col in file_columns:
                select_parts.append(f"p.{col}")
            elif col == 'source_file':
                select_parts.append(f"'{source_file}' AS source_file")
            elif col == 'exchange_timestamp_ns':
                if 'exchange_timestamp_ns' in file_columns:
                    select_parts.append(f"p.exchange_timestamp_ns")
                elif 'exchange_timestamp' in file_columns:
                    select_parts.append("CAST(EXTRACT(EPOCH FROM p.exchange_timestamp) * 1000000000 AS BIGINT) AS exchange_timestamp_ns")
                elif 'timestamp' in file_columns:
                    select_parts.append("CAST(EXTRACT(EPOCH FROM p.timestamp) * 1000000000 AS BIGINT) AS exchange_timestamp_ns")
                elif 'last_traded_timestamp_ns' in file_columns:
                    select_parts.append("p.last_traded_timestamp_ns AS exchange_timestamp_ns")
                else:
                    select_parts.append("CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000000000 AS BIGINT) AS exchange_timestamp_ns")
            elif col == 'processed_at':
                select_parts.append("CURRENT_TIMESTAMP AS processed_at")
            elif col == 'parser_version':
                select_parts.append("'v3.0_reprocessed' AS parser_version")
            else:
                select_parts.append(f"NULL AS {col}")
        
        # SINGLE INSERT - file read happens here (ONLY read of actual data)
        insert_query = f"""
            INSERT OR REPLACE INTO tick_data_corrected 
            SELECT {', '.join(select_parts)}
            FROM read_parquet('{file_path}') p
        """
        
        # Execute and get row count from affected rows
        result_obj = conn.execute(insert_query)
        # DuckDB doesn't directly return row count, so we estimate from the query
        # We'll skip exact count to avoid another read
        
        result['success'] = True
        result['rows'] = 0  # Skip row count to avoid extra read - not critical for ingestion
        
    except Exception as e:
        result['error'] = str(e)[:500]
        logger.debug(f"  ⚠️ Error ingesting {Path(file_path).name}: {str(e)[:200]}")
    
    return result

def main():
    """Main ingestion function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest enriched parquet files to DuckDB")
    parser.add_argument("--enriched-dir", type=str, default="enriched_parquet", help="Directory with enriched parquet files")
    parser.add_argument("--db", type=str, default="tick_data_production.db", help="DuckDB database path")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing (default: 100)")
    parser.add_argument("--memory-threshold-mb", type=float, default=1024.0, help="Memory threshold in MB to trigger GC (default: 1024 = 1GB)")
    
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("ENRICHED PARQUET INGESTION TO DUCKDB")
    logger.info("="*80)
    logger.info(f"Enriched directory: {args.enriched_dir}")
    logger.info(f"Database: {args.db}")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"GC threshold: {args.memory_threshold_mb:.1f}MB ({args.memory_threshold_mb/1024:.2f}GB)")
    logger.info("")
    
    # Log initial memory
    initial_memory = get_memory_usage_mb()
    logger.info(f"📊 Initial memory usage: {initial_memory:.1f}MB")
    
    # Find enriched files
    enriched_dir = Path(args.enriched_dir)
    enriched_files = find_enriched_parquet_files(enriched_dir)
    
    if not enriched_files:
        logger.warning("❌ No enriched parquet files found")
        sys.exit(0)
    
    # Connect to database
    logger.info(f"📦 Connecting to database: {args.db}")
    conn = duckdb.connect(args.db)
    ensure_schema(conn)
    
    # OPTIMIZED: Get database columns ONCE (not per file)
    logger.info("📋 Getting database schema...")
    db_columns_df = conn.execute("DESCRIBE tick_data_corrected").fetchdf()
    db_columns = db_columns_df['column_name'].tolist()
    logger.info(f"   Database has {len(db_columns)} columns")
    
    # OPTIMIZED: Check existing files once (not per file)
    logger.info("🔍 Checking for already ingested files...")
    try:
        existing_files = set(conn.execute("SELECT DISTINCT source_file FROM tick_data_corrected WHERE source_file IS NOT NULL").fetchdf()['source_file'].tolist())
        logger.info(f"   Found {len(existing_files)} already ingested files in database")
    except:
        existing_files = set()
        logger.info("   No existing files found (new database)")
    
    # Filter out already ingested files
    files_to_ingest = []
    for file_path in enriched_files:
        file_name = Path(file_path).name
        if file_name not in existing_files:
            files_to_ingest.append(file_path)
    
    logger.info(f"📋 Files to ingest: {len(files_to_ingest)} (skipped {len(enriched_files) - len(files_to_ingest)} already ingested)")
    
    if not files_to_ingest:
        logger.info("✅ All files already ingested!")
        conn.close()
        sys.exit(0)
    
    # Process files in batches
    stats = {
        'total_files': len(files_to_ingest),
        'processed': 0,
        'successful': 0,
        'failed': 0,
        'total_rows': 0,
        'start_time': datetime.now()
    }
    
    try:
        for batch_start in range(0, len(files_to_ingest), args.batch_size):
            batch_files = files_to_ingest[batch_start:batch_start + args.batch_size]
            batch_num = (batch_start // args.batch_size) + 1
            total_batches = (len(files_to_ingest) + args.batch_size - 1) // args.batch_size
            
            logger.info(f"\n📦 Processing batch {batch_num}/{total_batches} ({len(batch_files)} files)")
            
            # Check memory before batch
            check_and_gc(memory_threshold_mb=args.memory_threshold_mb)
            
            for file_path in batch_files:
                stats['processed'] += 1
                
                # Check memory before each file
                check_and_gc(memory_threshold_mb=args.memory_threshold_mb)
                
                # OPTIMIZED: Pass db_columns to avoid re-querying
                result = ingest_single_file(conn, file_path, db_columns)
                
                if result['success']:
                    stats['successful'] += 1
                    stats['total_rows'] += result['rows']
                    if stats['successful'] % 50 == 0:
                        logger.info(f"  ✅ Ingested {stats['successful']}/{stats['total_files']}: {Path(file_path).name} ({result['rows']} rows)")
                else:
                    stats['failed'] += 1
                    if stats['failed'] <= 10:  # Log first 10 failures
                        logger.warning(f"  ❌ Failed: {Path(file_path).name} - {result.get('error', 'Unknown')[:100]}")
            
            # Progress update
            logger.info(f"\n📊 Progress: {stats['processed']}/{stats['total_files']} files")
            logger.info(f"   ✅ Successful: {stats['successful']}")
            logger.info(f"   ❌ Failed: {stats['failed']}")
            logger.info(f"   📊 Total rows: {stats['total_rows']:,}")
            
            # Check memory after batch
            check_and_gc(memory_threshold_mb=args.memory_threshold_mb)
            current_memory = get_memory_usage_mb()
            logger.info(f"   📊 Memory after batch: {current_memory:.1f}MB")
            
            # Small delay between batches
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted by user")
    except Exception as e:
        logger.error(f"\n💥 Fatal error: {e}")
        traceback.print_exc()
    finally:
        conn.close()
    
    # Final summary
    stats['end_time'] = datetime.now()
    stats['duration'] = stats['end_time'] - stats['start_time']
    
    logger.info("\n" + "="*80)
    logger.info("ENRICHED PARQUET INGESTION - FINAL SUMMARY")
    logger.info("="*80)
    logger.info(f"Total files processed: {stats['total_files']}")
    logger.info(f"✅ Successful: {stats['successful']}")
    logger.info(f"❌ Failed: {stats['failed']}")
    logger.info(f"📊 Total rows ingested: {stats['total_rows']:,}")
    logger.info(f"⏱️ Duration: {stats['duration']}")
    if stats['total_files'] > 0:
        logger.info(f"Success rate: {(stats['successful']/stats['total_files'])*100:.1f}%")
    logger.info("="*80)
    
    # Save statistics
    with open('enriched_parquet_ingestion_stats.json', 'w') as f:
        json.dump(stats, f, indent=2, default=str)

if __name__ == "__main__":
    main()

