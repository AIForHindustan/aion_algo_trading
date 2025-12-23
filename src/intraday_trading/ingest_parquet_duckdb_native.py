#!/opt/homebrew/bin/python3.13
"""
Ingest Parquet Files Using DuckDB Native SQL Only
- Uses DuckDB's read_parquet() function
- Joins with token metadata for enrichment
- Single connection, sequential processing (no connection conflicts)
"""

import sys
from pathlib import Path
import json
import duckdb
from typing import List, Optional
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from token_cache import TokenCacheManager
from binary_to_parquet.production_binary_converter import ensure_production_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_token_metadata_into_duckdb(conn: duckdb.DuckDBPyConnection, token_cache: TokenCacheManager) -> str:
    """Load token metadata into DuckDB as a temporary table and return table name"""
    # Build metadata as a list of tuples for DuckDB insertion
    metadata_rows = []
    for token_str, info in token_cache.token_map.items():
        try:
            token = int(token_str)
            
            # Extract symbol from 'key' field or 'name'
            key_field = info.get("key", "")
            if ":" in key_field:
                symbol = key_field.split(":", 1)[1]
                exchange = key_field.split(":", 1)[0]
            else:
                symbol = info.get("name", "")
                exchange = info.get("exchange", "")
            
            metadata_rows.append((
                token,
                symbol or info.get("name", ""),
                exchange or info.get("exchange", ""),
                info.get("instrument_type", ""),
                info.get("segment") or info.get("source", ""),
            ))
        except (ValueError, TypeError):
            continue
    
    if not metadata_rows:
        logger.warning("No token metadata to load")
        return None
    
    # Create temporary table
    view_name = "token_metadata_lookup"
    
    try:
        conn.execute(f"DROP TABLE IF EXISTS {view_name}")
    except:
        pass
    
    # Create and populate table
    conn.execute(f"""
        CREATE TEMP TABLE {view_name} (
            token BIGINT,
            symbol VARCHAR,
            exchange VARCHAR,
            instrument_type VARCHAR,
            segment VARCHAR
        )
    """)
    
    conn.executemany(
        f"INSERT INTO {view_name} VALUES (?, ?, ?, ?, ?)",
        metadata_rows
    )
    
    logger.info(f"Loaded {len(metadata_rows):,} token metadata entries into {view_name}")
    return view_name


def ingest_parquet_file_duckdb_native(
    conn: duckdb.DuckDBPyConnection,
    file_path: Path,
    token_metadata_view: Optional[str] = None
) -> dict:
    """Ingest parquet file using DuckDB native read_parquet() with SQL joins"""
    try:
        # First check if file can be read and what schema it has
        # Try DuckDB native read first, fallback to pandas/pyarrow if it fails
        columns = None
        sample = None
        use_pandas_fallback = False
        
        # Try DuckDB read first
        try:
            # Try to read first row to check schema with DuckDB
            sample = conn.execute(f"SELECT * FROM read_parquet('{file_path}') LIMIT 1").fetchone()
            if not sample:
                return {"success": False, "error": "Empty parquet file", "row_count": 0}
            
            # Get column names
            columns = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}') LIMIT 1").fetchdf()['column_name'].tolist()
        except Exception as duckdb_error:
            # DuckDB read failed - try pandas/pyarrow as fallback
            error_str = str(duckdb_error)
            # Catch all parquet read errors that DuckDB can't handle
            if any(err in error_str for err in [
                'TProtocolException', 'don\'t know what type', 'Invalid data', 'Out of buffer', 
                'Snappy', 'corrupted', 'dictionary offset', 'Parquet file is likely corrupted',
                'Invalid Input', 'decompression failure', 'Binder Error', 'Referenced column'
            ]):
                logger.debug(f"DuckDB read failed for {file_path.name}, trying pandas fallback: {error_str[:100]}")
                try:
                    import pandas as pd
                    import pyarrow.parquet as pq
                    
                    # Try pandas read first (read full file, then take sample)
                    try:
                        df_full = pd.read_parquet(file_path)
                        df_sample = df_full.head(1) if len(df_full) > 0 else df_full
                    except Exception as pandas_err1:
                        # Try pyarrow directly with row group recovery
                        try:
                            parquet_file = pq.ParquetFile(file_path)
                            # Try reading first row group
                            if parquet_file.num_row_groups > 0:
                                table = parquet_file.read_row_groups([0])
                                df_sample = table.to_pandas()
                            else:
                                raise Exception("No row groups")
                        except Exception as pyarrow_err:
                            # Try reading with essential columns only
                            try:
                                parquet_file = pq.ParquetFile(file_path)
                                essential_cols = ['instrument_token', 'token', 'exchange_timestamp', 'last_price', 'volume']
                                available_cols = [col for col in parquet_file.schema.names if col in essential_cols]
                                if available_cols:
                                    table = parquet_file.read_row_groups([0], columns=available_cols)
                                    df_sample = table.to_pandas()
                                else:
                                    raise Exception("No essential columns found")
                            except:
                                raise pandas_err1
                    
                    if len(df_sample) == 0:
                        return {"success": False, "error": "Empty parquet file", "row_count": 0}
                    
                    columns = df_sample.columns.tolist()
                    sample = df_sample.iloc[0].to_dict() if len(df_sample) > 0 else {}
                    use_pandas_fallback = True
                    logger.debug(f"Successfully read schema via pandas fallback: {len(columns)} columns")
                except Exception as pandas_error:
                    # Both DuckDB and pandas failed - return error
                    return {"success": False, "error": f"DuckDB: {error_str[:100]}, Pandas: {str(pandas_error)[:100]}", "row_count": 0}
            else:
                # Unknown error, re-raise
                raise
        
        # At this point, we should have columns set (either from DuckDB or pandas fallback)
        if columns is None:
            return {"success": False, "error": "Failed to read file schema", "row_count": 0}
        
        # Check if this is already in the correct schema (has instrument_token)
        if 'instrument_token' in columns:
                # Check if we need to enrich with token metadata (if symbol is missing/UNKNOWN)
                needs_enrichment = False
                if token_metadata_view and not use_pandas_fallback:
                    # Check if there are any rows with missing/UNKNOWN symbols (only if using DuckDB directly)
                    try:
                        unknown_check = conn.execute(f"""
                            SELECT COUNT(*) 
                            FROM read_parquet('{file_path}') 
                            WHERE instrument_token > 0 
                            AND (symbol IS NULL OR symbol = '' OR symbol LIKE 'UNKNOWN%')
                        """).fetchone()[0]
                        if unknown_check > 0:
                            needs_enrichment = True
                            logger.info(f"Found {unknown_check} rows with missing/UNKNOWN symbols, enriching with token metadata")
                    except:
                        # If this query fails, skip enrichment check
                        pass
                
                # Get row count and filter UNKNOWN symbols
                if use_pandas_fallback:
                    # Count will be done later when reading with pandas - skip count check for now
                    count = 1  # Placeholder, will be set when reading full file
                else:
                    count_query = f"""
                        SELECT COUNT(*) 
                        FROM read_parquet('{file_path}') 
                        WHERE instrument_token > 0
                        AND (symbol IS NULL OR symbol = '' OR symbol NOT LIKE 'UNKNOWN%')
                    """
                    count = conn.execute(count_query).fetchone()[0]
                    if count == 0:
                        return {"success": False, "error": "No valid rows (all filtered out)", "row_count": 0}
                
                # Build column mapping - handle nested structures and map to DB schema
                # Handle different parquet schemas (with ohlc struct, depth struct, etc.)
                column_mapping = {}
                
                # Map standard columns - only use metadata join if enrichment is needed
                if 'symbol' in columns:
                    if needs_enrichment and token_metadata_view:
                        column_mapping['symbol'] = "COALESCE(p.symbol, m.symbol, '')"
                    else:
                        column_mapping['symbol'] = "COALESCE(p.symbol, '')"
                elif needs_enrichment and token_metadata_view:
                    column_mapping['symbol'] = f"COALESCE(m.symbol, '')"
                else:
                    column_mapping['symbol'] = "''"
                
                if 'exchange' in columns:
                    if needs_enrichment and token_metadata_view:
                        column_mapping['exchange'] = "COALESCE(p.exchange, m.exchange, '')"
                    else:
                        column_mapping['exchange'] = "COALESCE(p.exchange, '')"
                elif needs_enrichment and token_metadata_view:
                    column_mapping['exchange'] = f"COALESCE(m.exchange, '')"
                else:
                    column_mapping['exchange'] = "''"
                
                if 'segment' in columns:
                    if needs_enrichment and token_metadata_view:
                        column_mapping['segment'] = "COALESCE(p.segment, m.segment, '')"
                    else:
                        column_mapping['segment'] = "COALESCE(p.segment, '')"
                elif needs_enrichment and token_metadata_view:
                    column_mapping['segment'] = f"COALESCE(m.segment, '')"
                else:
                    column_mapping['segment'] = "''"
                
                if 'instrument_type' in columns:
                    if needs_enrichment and token_metadata_view:
                        column_mapping['instrument_type'] = "COALESCE(p.instrument_type, m.instrument_type, '')"
                    else:
                        column_mapping['instrument_type'] = "COALESCE(p.instrument_type, '')"
                elif needs_enrichment and token_metadata_view:
                    column_mapping['instrument_type'] = f"COALESCE(m.instrument_type, '')"
                else:
                    column_mapping['instrument_type'] = "''"
                
                # Price columns
                column_mapping['last_price'] = "COALESCE(p.last_price, 0.0)"
                
                # OHLC handling - check if ohlc struct exists
                if 'ohlc' in columns:
                    column_mapping['open_price'] = "COALESCE(p.ohlc.open, p.last_price, 0.0)"
                    column_mapping['high_price'] = "COALESCE(p.ohlc.high, p.last_price, 0.0)"
                    column_mapping['low_price'] = "COALESCE(p.ohlc.low, p.last_price, 0.0)"
                    column_mapping['close_price'] = "COALESCE(p.ohlc.close, p.last_price, 0.0)"
                elif 'open_price' in columns:
                    column_mapping['open_price'] = "COALESCE(p.open_price, p.last_price, 0.0)"
                    column_mapping['high_price'] = "COALESCE(p.high_price, p.last_price, 0.0)"
                    column_mapping['low_price'] = "COALESCE(p.low_price, p.last_price, 0.0)"
                    column_mapping['close_price'] = "COALESCE(p.close_price, p.last_price, 0.0)"
                else:
                    column_mapping['open_price'] = "COALESCE(p.last_price, 0.0)"
                    column_mapping['high_price'] = "COALESCE(p.last_price, 0.0)"
                    column_mapping['low_price'] = "COALESCE(p.last_price, 0.0)"
                    column_mapping['close_price'] = "COALESCE(p.last_price, 0.0)"
                
                # Volume
                if 'volume' in columns:
                    column_mapping['volume'] = "COALESCE(p.volume, 0)"
                elif 'volume_traded' in columns:
                    column_mapping['volume'] = "COALESCE(p.volume_traded, 0)"
                else:
                    column_mapping['volume'] = "0"
                
                # Timestamps - handle TIMESTAMP_NS type
                # Priority: exchange_timestamp_ns > timestamp_ms > exchange_timestamp_epoch > exchange_timestamp > timestamp
                if 'exchange_timestamp_ns' in columns:
                    # Prefer exchange_timestamp_ns if available (BIGINT nanoseconds)
                    column_mapping['exchange_timestamp_ns'] = "COALESCE(CAST(p.exchange_timestamp_ns AS BIGINT), 0)"
                    # Convert nanoseconds to timestamp
                    column_mapping['exchange_timestamp'] = "TO_TIMESTAMP(CAST(p.exchange_timestamp_ns AS BIGINT) / 1000000000.0)"
                elif 'timestamp_ms' in columns:
                    # Use timestamp_ms (milliseconds) as anchor - convert to timestamp and nanoseconds
                    column_mapping['exchange_timestamp_ns'] = "CAST(COALESCE(CAST(p.timestamp_ms AS BIGINT), 0) * 1000000 AS BIGINT)"
                    column_mapping['exchange_timestamp'] = "TO_TIMESTAMP(CAST(p.timestamp_ms AS BIGINT) / 1000.0)"
                elif 'exchange_timestamp_epoch' in columns:
                    # Use epoch (seconds) - convert to timestamp and nanoseconds
                    column_mapping['exchange_timestamp'] = "TO_TIMESTAMP(p.exchange_timestamp_epoch)"
                    column_mapping['exchange_timestamp_ns'] = "CAST(p.exchange_timestamp_epoch * 1000000000 AS BIGINT)"
                elif 'exchange_timestamp' in columns:
                    # Check if exchange_timestamp is numeric (DOUBLE/epoch) or TIMESTAMP or string
                    # If it's a string or problematic, fallback to timestamp_ms if available
                    if 'timestamp_ms' in columns:
                        # exchange_timestamp is problematic (string/NaN), use timestamp_ms as anchor
                        column_mapping['exchange_timestamp_ns'] = "CAST(COALESCE(CAST(p.timestamp_ms AS BIGINT), 0) * 1000000 AS BIGINT)"
                        column_mapping['exchange_timestamp'] = "TO_TIMESTAMP(CAST(p.timestamp_ms AS BIGINT) / 1000.0)"
                    elif 'exchange_timestamp_ns' not in columns:
                        # Try to handle as epoch (numeric) first, fallback to TIMESTAMP
                        column_mapping['exchange_timestamp'] = "CASE WHEN p.exchange_timestamp IS NULL OR CAST(p.exchange_timestamp AS VARCHAR) = '' THEN CURRENT_TIMESTAMP ELSE TRY_CAST(p.exchange_timestamp AS TIMESTAMP) END"
                        column_mapping['exchange_timestamp_ns'] = "CAST(COALESCE(TRY_CAST(p.exchange_timestamp AS DOUBLE) * 1000000000, EXTRACT(EPOCH FROM COALESCE(TRY_CAST(p.exchange_timestamp AS TIMESTAMP), CURRENT_TIMESTAMP)) * 1000000000) AS BIGINT)"
                    else:
                        # exchange_timestamp is TIMESTAMP_NS - extract properly
                        column_mapping['exchange_timestamp'] = "CAST(p.exchange_timestamp AS TIMESTAMP)"
                        column_mapping['exchange_timestamp_ns'] = "CAST(EXTRACT(EPOCH FROM p.exchange_timestamp) * 1000000000 AS BIGINT)"
                elif 'timestamp' in columns:
                    # Try to parse timestamp string or use epoch
                    # If timestamp_ms is available, prefer it
                    if 'timestamp_ms' in columns:
                        column_mapping['exchange_timestamp_ns'] = "CAST(COALESCE(CAST(p.timestamp_ms AS BIGINT), 0) * 1000000 AS BIGINT)"
                        column_mapping['exchange_timestamp'] = "TO_TIMESTAMP(CAST(p.timestamp_ms AS BIGINT) / 1000.0)"
                    else:
                        column_mapping['exchange_timestamp'] = "CAST(p.timestamp AS TIMESTAMP)"
                        # If timestamp is already numeric (epoch), use it directly
                        column_mapping['exchange_timestamp_ns'] = "CAST(p.timestamp AS BIGINT)"
                else:
                    column_mapping['exchange_timestamp'] = "CURRENT_TIMESTAMP"
                    column_mapping['exchange_timestamp_ns'] = "CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000000000 AS BIGINT)"
                
                # Build complete column list for DB schema (82 columns total)
                # IMPORTANT: Order must match the actual database schema exactly!
                db_columns = [
                    'instrument_token', 'symbol', 'exchange', 'segment', 'instrument_type',
                    'expiry', 'strike_price', 'option_type', 'lot_size', 'tick_size', 'is_expired',
                    'timestamp', 'exchange_timestamp', 'exchange_timestamp_ns',
                    'last_traded_timestamp', 'last_traded_timestamp_ns', 'last_price',
                    'open_price', 'high_price', 'low_price', 'close_price', 'average_traded_price',
                    'volume', 'last_traded_quantity', 'total_buy_quantity', 'total_sell_quantity',
                    'open_interest', 'oi_day_high', 'oi_day_low',
                    'bid_1_price', 'bid_1_quantity', 'bid_1_orders',
                    'bid_2_price', 'bid_2_quantity', 'bid_2_orders',
                    'bid_3_price', 'bid_3_quantity', 'bid_3_orders',
                    'bid_4_price', 'bid_4_quantity', 'bid_4_orders',
                    'bid_5_price', 'bid_5_quantity', 'bid_5_orders',
                    'ask_1_price', 'ask_1_quantity', 'ask_1_orders',
                    'ask_2_price', 'ask_2_quantity', 'ask_2_orders',
                    'ask_3_price', 'ask_3_quantity', 'ask_3_orders',
                    'ask_4_price', 'ask_4_quantity', 'ask_4_orders',
                    'ask_5_price', 'ask_5_quantity', 'ask_5_orders',
                    # Technical indicators (BEFORE packet_type in database schema!)
                    'rsi_14', 'sma_20', 'ema_12', 'bollinger_upper', 'bollinger_middle', 'bollinger_lower',
                    'macd', 'macd_signal', 'macd_histogram',
                    # Option Greeks
                    'delta', 'gamma', 'theta', 'vega', 'rho', 'implied_volatility',
                    'packet_type', 'data_quality', 'session_type', 
                    'source_file', 'processing_batch', 'session_id',
                    'processed_at', 'parser_version'
                ]
                
                select_cols = []
                for col in db_columns:
                    if col in column_mapping:
                        select_cols.append(column_mapping[col] + f" AS {col}")
                    elif col == 'instrument_token':
                        select_cols.append("p.instrument_token")
                    elif col in ['rsi_14', 'sma_20', 'ema_12', 'bollinger_upper', 'bollinger_middle', 'bollinger_lower',
                                  'macd', 'macd_signal', 'macd_histogram', 'delta', 'gamma', 'theta', 'vega', 'rho', 'implied_volatility']:
                        # Technical indicators and Greeks - NULL by default (check BEFORE col in columns)
                        select_cols.append(f"CAST(NULL AS DOUBLE) AS {col}")
                    elif col == 'source_file':
                        select_cols.append(f"'{file_path}' AS source_file")
                    elif col == 'processing_batch':
                        select_cols.append("'parquet_ingestion_duckdb_native' AS processing_batch")
                    elif col == 'timestamp':
                        select_cols.append("CURRENT_TIMESTAMP AS timestamp")
                    elif col == 'processed_at':
                        select_cols.append("CURRENT_TIMESTAMP AS processed_at")
                    elif col == 'parser_version':
                        select_cols.append("'v2.0' AS parser_version")
                    elif col in ['last_traded_timestamp', 'last_traded_timestamp_ns']:
                        # Use last_trade_time if available (TIMESTAMP_NS), otherwise use exchange_timestamp
                        if 'last_trade_time' in columns:
                            if col == 'last_traded_timestamp':
                                select_cols.append("CAST(p.last_trade_time AS TIMESTAMP) AS last_traded_timestamp")
                            else:
                                # Extract epoch from TIMESTAMP_NS and convert to nanoseconds
                                select_cols.append("CAST(EXTRACT(EPOCH FROM p.last_trade_time) * 1000000000 AS BIGINT) AS last_traded_timestamp_ns")
                        elif 'exchange_timestamp' in column_mapping:
                            if col == 'last_traded_timestamp':
                                select_cols.append(column_mapping['exchange_timestamp'].replace(' AS exchange_timestamp', '') + " AS last_traded_timestamp")
                            else:
                                # Use exchange_timestamp_ns (already converted to nanoseconds)
                                select_cols.append(column_mapping['exchange_timestamp_ns'].replace(' AS exchange_timestamp_ns', '') + " AS last_traded_timestamp_ns")
                        else:
                            select_cols.append("NULL AS " + col)
                    elif col == 'average_traded_price':
                        if 'average_traded_price' in columns:
                            select_cols.append("COALESCE(p.average_traded_price, p.last_price, 0.0) AS average_traded_price")
                        else:
                            select_cols.append("COALESCE(p.last_price, 0.0) AS average_traded_price")
                    elif col in ['total_buy_quantity', 'total_sell_quantity']:
                        if col in columns:
                            select_cols.append(f"COALESCE(p.{col}, 0) AS {col}")
                        else:
                            select_cols.append(f"0 AS {col}")
                    elif col == 'open_interest':
                        if 'oi' in columns:
                            select_cols.append("COALESCE(p.oi, 0) AS open_interest")
                        elif 'open_interest' in columns:
                            select_cols.append("COALESCE(p.open_interest, 0) AS open_interest")
                        else:
                            select_cols.append("0 AS open_interest")
                    elif col in ['oi_day_high', 'oi_day_low']:
                        if col in columns:
                            select_cols.append(f"COALESCE(p.{col}, 0) AS {col}")
                        else:
                            select_cols.append("0 AS " + col)
                    elif 'bid_' in col or 'ask_' in col:
                        # Handle depth structure or direct columns
                        if 'depth' in columns:
                            # Extract from depth struct
                            if 'bid_1' in col:
                                if 'price' in col:
                                    select_cols.append("COALESCE(p.depth.buy[1].price, p.best_bid_price, 0.0) AS " + col)
                                elif 'quantity' in col:
                                    select_cols.append("COALESCE(p.depth.buy[1].quantity, p.best_bid_quantity, 0) AS " + col)
                                elif 'orders' in col:
                                    select_cols.append("COALESCE(p.depth.buy[1].orders, p.best_bid_orders, 0) AS " + col)
                                else:
                                    select_cols.append("0 AS " + col)
                            elif 'ask_1' in col:
                                if 'price' in col:
                                    select_cols.append("COALESCE(p.depth.sell[1].price, p.best_ask_price, 0.0) AS " + col)
                                elif 'quantity' in col:
                                    select_cols.append("COALESCE(p.depth.sell[1].quantity, p.best_ask_quantity, 0) AS " + col)
                                elif 'orders' in col:
                                    select_cols.append("COALESCE(p.depth.sell[1].orders, p.best_ask_orders, 0) AS " + col)
                                else:
                                    select_cols.append("0 AS " + col)
                            else:
                                select_cols.append("0 AS " + col)
                        elif col in columns:
                            select_cols.append(f"COALESCE(p.{col}, 0) AS {col}")
                        else:
                            select_cols.append("0 AS " + col)
                    else:
                        # Default values
                        if 'timestamp' in col or col == 'expiry':
                            select_cols.append(f"NULL AS {col}")
                        elif col in ['option_type', 'packet_type', 'data_quality', 'session_type', 'session_id']:
                            select_cols.append(f"'' AS {col}")
                        elif col in ['strike_price', 'lot_size', 'tick_size']:
                            select_cols.append(f"NULL AS {col}")
                        elif col == 'is_expired':
                            select_cols.append("FALSE AS is_expired")
                        elif col in columns:
                            # Column exists in parquet - use it directly
                            select_cols.append(f"p.{col}")
                        else:
                            select_cols.append(f"NULL AS {col}")
                
                # Build join clause if enrichment needed
                join_clause = ""
                if needs_enrichment and token_metadata_view:
                    join_clause = f"LEFT JOIN {token_metadata_view} m ON p.instrument_token = m.token"
                
                # Build WHERE clause - filter UNKNOWN symbols
                # If using pandas fallback, data is already filtered, so just check instrument_token
                if use_pandas_fallback:
                    where_clause = "WHERE p.instrument_token > 0"
                else:
                    where_clause = """
                        WHERE p.instrument_token > 0
                        AND (p.symbol IS NULL OR p.symbol = '' OR p.symbol NOT LIKE 'UNKNOWN%')
                    """
                
                # Build insert query with aggregation for duplicates
                # Group by primary key (instrument_token + exchange_timestamp_ns) and aggregate values
                # This ensures we don't lose data when multiple rows have same PK but different values
                # Use MAX for volumes/quantities/prices to capture the maximum values
                
                # Build aggregated SELECT with GROUP BY on primary key
                agg_select_cols = []
                for col in db_columns:
                    if col == 'instrument_token':
                        agg_select_cols.append("p.instrument_token")
                    elif col == 'exchange_timestamp_ns':
                        # Group by this - use the calculated value
                        # Priority: exchange_timestamp_ns > timestamp_ms > exchange_timestamp_epoch > exchange_timestamp
                        if 'exchange_timestamp_ns' in columns:
                            agg_select_cols.append("CAST(p.exchange_timestamp_ns AS BIGINT) AS exchange_timestamp_ns")
                        elif 'timestamp_ms' in columns:
                            # Use timestamp_ms (milliseconds) as anchor - convert to nanoseconds
                            agg_select_cols.append("CAST(COALESCE(CAST(p.timestamp_ms AS BIGINT), 0) * 1000000 AS BIGINT) AS exchange_timestamp_ns")
                        elif 'exchange_timestamp_epoch' in columns:
                            agg_select_cols.append("CAST(p.exchange_timestamp_epoch * 1000000000 AS BIGINT) AS exchange_timestamp_ns")
                        elif 'exchange_timestamp' in columns:
                            # Handle DOUBLE exchange_timestamp or string - prefer timestamp_ms if available
                            if 'timestamp_ms' in columns:
                                agg_select_cols.append("CAST(COALESCE(CAST(p.timestamp_ms AS BIGINT), 0) * 1000000 AS BIGINT) AS exchange_timestamp_ns")
                            else:
                                agg_select_cols.append("CAST(COALESCE(TRY_CAST(p.exchange_timestamp AS DOUBLE) * 1000000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000000000) AS BIGINT) AS exchange_timestamp_ns")
                        else:
                            agg_select_cols.append("CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000000000 AS BIGINT) AS exchange_timestamp_ns")
                    elif col == 'exchange_timestamp':
                        # Use the mapping if available, otherwise derive from exchange_timestamp_ns or timestamp_ms
                        if col in column_mapping:
                            expr = column_mapping[col]
                            agg_select_cols.append(f"MAX({expr}) AS {col}")
                        elif 'exchange_timestamp_ns' in columns:
                            agg_select_cols.append("TO_TIMESTAMP(CAST(p.exchange_timestamp_ns AS BIGINT) / 1000000000.0) AS exchange_timestamp")
                        elif 'timestamp_ms' in columns:
                            # Use timestamp_ms (milliseconds) as anchor
                            agg_select_cols.append("TO_TIMESTAMP(CAST(p.timestamp_ms AS BIGINT) / 1000.0) AS exchange_timestamp")
                        elif 'exchange_timestamp_epoch' in columns:
                            agg_select_cols.append("TO_TIMESTAMP(p.exchange_timestamp_epoch) AS exchange_timestamp")
                        elif 'exchange_timestamp' in columns:
                            # Handle DOUBLE exchange_timestamp or string - prefer timestamp_ms if available
                            if 'timestamp_ms' in columns:
                                agg_select_cols.append("TO_TIMESTAMP(CAST(p.timestamp_ms AS BIGINT) / 1000.0) AS exchange_timestamp")
                            else:
                                # Try TIMESTAMP first, then try DOUBLE as seconds, milliseconds, microseconds, or nanoseconds
                                # TO_TIMESTAMP expects seconds (epoch), so we need to divide by appropriate factor
                                # Use COALESCE to try different conversions in order
                                agg_select_cols.append("""
                                    CASE 
                                        WHEN p.exchange_timestamp IS NULL OR CAST(p.exchange_timestamp AS VARCHAR) = '' 
                                            THEN CURRENT_TIMESTAMP
                                        ELSE COALESCE(
                                            TRY_CAST(p.exchange_timestamp AS TIMESTAMP),
                                            -- Try as milliseconds (most common for these files - values like 1759135104587)
                                            CASE WHEN TRY_CAST(p.exchange_timestamp AS DOUBLE) > 1e9 
                                                THEN TO_TIMESTAMP(TRY_CAST(p.exchange_timestamp AS DOUBLE) / 1000.0)
                                                ELSE NULL
                                            END,
                                            -- Try as seconds (if < 1e9)
                                            CASE WHEN TRY_CAST(p.exchange_timestamp AS DOUBLE) < 1e9 
                                                THEN TO_TIMESTAMP(TRY_CAST(p.exchange_timestamp AS DOUBLE))
                                                ELSE NULL
                                            END,
                                            -- Try as microseconds (if 1e12 <= x < 1e15)
                                            CASE WHEN TRY_CAST(p.exchange_timestamp AS DOUBLE) >= 1e12 AND TRY_CAST(p.exchange_timestamp AS DOUBLE) < 1e15
                                                THEN TO_TIMESTAMP(TRY_CAST(p.exchange_timestamp AS DOUBLE) / 1000000.0)
                                                ELSE NULL
                                            END,
                                            -- Try as nanoseconds (if >= 1e15)
                                            CASE WHEN TRY_CAST(p.exchange_timestamp AS DOUBLE) >= 1e15
                                                THEN TO_TIMESTAMP(TRY_CAST(p.exchange_timestamp AS DOUBLE) / 1000000000.0)
                                                ELSE NULL
                                            END,
                                            CURRENT_TIMESTAMP
                                        )
                                    END AS exchange_timestamp
                                """.strip().replace('\n', ' '))
                        else:
                            agg_select_cols.append("CURRENT_TIMESTAMP AS exchange_timestamp")
                    elif col in ['total_buy_quantity', 'total_sell_quantity']:
                        # Handle these explicitly - they exist in parquet
                        if col in columns:
                            agg_select_cols.append(f"MAX(COALESCE(p.{col}, 0)) AS {col}")
                        else:
                            agg_select_cols.append(f"0 AS {col}")
                    elif col in ['rsi_14', 'sma_20', 'ema_12', 'bollinger_upper', 'bollinger_middle', 'bollinger_lower',
                                  'macd', 'macd_signal', 'macd_histogram']:
                        # Technical indicators - NULL as DOUBLE (check BEFORE column_mapping)
                        agg_select_cols.append(f"CAST(NULL AS DOUBLE) AS {col}")
                    elif col in ['delta', 'gamma', 'theta', 'vega', 'rho', 'implied_volatility']:
                        # Option Greeks - NULL as DOUBLE (check BEFORE column_mapping)
                        agg_select_cols.append(f"CAST(NULL AS DOUBLE) AS {col}")
                    elif col in ['source_file', 'processing_batch', 'timestamp', 'processed_at', 'parser_version']:
                        if col == 'source_file':
                            agg_select_cols.append(f"'{file_path}' AS {col}")
                        elif col == 'processing_batch':
                            agg_select_cols.append(f"'parquet_ingestion_duckdb_native' AS {col}")
                        elif col == 'timestamp':
                            agg_select_cols.append(f"CURRENT_TIMESTAMP AS {col}")
                        elif col == 'processed_at':
                            agg_select_cols.append(f"CURRENT_TIMESTAMP AS {col}")
                        elif col == 'parser_version':
                            agg_select_cols.append(f"'v2.0' AS {col}")
                    elif col in column_mapping:
                        # For numeric columns (prices, volumes, quantities), use MAX to aggregate
                        expr = column_mapping[col]
                        if 'price' in col.lower() or 'volume' in col.lower() or 'quantity' in col.lower() or 'interest' in col.lower():
                            # Wrap in MAX() for aggregation
                            if 'COALESCE' in expr:
                                # Extract the inner expression
                                inner = expr.split('(')[1].rsplit(')')[0] if ')' in expr else expr
                                agg_select_cols.append(f"MAX({expr}) AS {col}")
                            else:
                                agg_select_cols.append(f"MAX({expr}) AS {col}")
                        else:
                            # For other columns, use MAX or FIRST
                            agg_select_cols.append(f"MAX({expr}) AS {col}")
                    elif col in columns:
                        # Column exists in parquet - use it with aggregation
                        if 'price' in col.lower() or 'volume' in col.lower() or 'quantity' in col.lower() or 'interest' in col.lower():
                            agg_select_cols.append(f"MAX(COALESCE(p.{col}, 0)) AS {col}")
                        else:
                            agg_select_cols.append(f"MAX(p.{col}) AS {col}")
                    else:
                        # Default NULL
                        agg_select_cols.append(f"NULL AS {col}")
                
                # Build GROUP BY expression for exchange_timestamp_ns
                # Priority: exchange_timestamp_ns > timestamp_ms > exchange_timestamp_epoch > exchange_timestamp
                if 'exchange_timestamp_ns' in columns:
                    group_by_expr = "CAST(p.exchange_timestamp_ns AS BIGINT)"
                elif 'timestamp_ms' in columns:
                    # Use timestamp_ms (milliseconds) as anchor - convert to nanoseconds
                    group_by_expr = "CAST(COALESCE(CAST(p.timestamp_ms AS BIGINT), 0) * 1000000 AS BIGINT)"
                elif 'exchange_timestamp_epoch' in columns:
                    group_by_expr = "CAST(p.exchange_timestamp_epoch * 1000000000 AS BIGINT)"
                elif 'exchange_timestamp' in columns:
                    # Handle DOUBLE exchange_timestamp or string - prefer timestamp_ms if available
                    if 'timestamp_ms' in columns:
                        group_by_expr = "CAST(COALESCE(CAST(p.timestamp_ms AS BIGINT), 0) * 1000000 AS BIGINT)"
                    else:
                        group_by_expr = "CAST(COALESCE(TRY_CAST(p.exchange_timestamp AS DOUBLE) * 1000000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000000000) AS BIGINT)"
                else:
                    group_by_expr = "CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000000000 AS BIGINT)"
                
                # If we used pandas fallback, we need to write to temp parquet and read that
                parquet_source = str(file_path)
                temp_file = None
                
                if use_pandas_fallback:
                    # Read full file with pandas and write to temp location
                    import pandas as pd
                    import tempfile
                    import os
                    
                    try:
                        # Read with pandas
                        df = pd.read_parquet(file_path)
                        if len(df) == 0:
                            return {"success": False, "error": "Empty parquet file", "row_count": 0}
                        
                        # Filter rows
                        df = df[
                            (df['instrument_token'] > 0) &
                            (df['symbol'].isna() | (df['symbol'] == '') | (~df['symbol'].astype(str).str.startswith('UNKNOWN', na=False)))
                        ].copy()
                        
                        if len(df) == 0:
                            return {"success": False, "error": "No valid rows (all filtered out)", "row_count": 0}
                        
                        # Write to temp parquet
                        temp_dir = Path(file_path).parent / ".temp_parquet"
                        temp_dir.mkdir(exist_ok=True)
                        temp_file = temp_dir / f"temp_{Path(file_path).name}"
                        df.to_parquet(temp_file, index=False, engine='pyarrow')
                        
                        # Use temp file for ingestion
                        parquet_source = str(temp_file)
                        count = len(df)  # Set count from pandas read
                    except Exception as pandas_error:
                        return {"success": False, "error": f"Pandas fallback failed: {str(pandas_error)[:100]}", "row_count": 0}
                
                insert_query = f"""
                    INSERT OR REPLACE INTO tick_data_corrected
                    SELECT {', '.join(agg_select_cols)}
                    FROM read_parquet('{parquet_source}') p
                    {join_clause}
                    {where_clause}
                    GROUP BY 
                        p.instrument_token,
                        {group_by_expr}
                """
                
                conn.execute(insert_query)
                
                # Clean up temp file if used
                if temp_file and temp_file.exists():
                    try:
                        temp_file.unlink()
                    except:
                        pass
                
                return {
                    "success": True,
                    "row_count": count or 0,
                    "original_rows": count or 0
                }
        
        # If we get here, need to transform (has 'token' instead of 'instrument_token')
        # But first check if 'token' actually exists in the file
        if 'token' not in columns:
            # File has neither 'instrument_token' nor 'token' - can't process
            return {"success": False, "error": f"File has neither 'instrument_token' nor 'token' column. Available columns: {', '.join(columns[:10])}", "row_count": 0}
        
        # Build INSERT query with metadata enrichment using SQL
        if token_metadata_view:
            insert_query = f"""
                INSERT OR REPLACE INTO tick_data_corrected
                SELECT
                    CAST(p.token AS BIGINT) as instrument_token,
                    COALESCE(m.symbol, '') as symbol,
                    COALESCE(m.exchange, '') as exchange,
                    COALESCE(m.segment, '') as segment,
                    COALESCE(m.instrument_type, '') as instrument_type,
                    NULL as expiry,
                    NULL as strike_price,
                    NULL as option_type,
                    NULL as lot_size,
                    NULL as tick_size,
                    CAST(p.timestamp AS TIMESTAMP) as exchange_timestamp,
                    CAST(p.timestamp AS BIGINT) as exchange_timestamp_ns,
                    CAST(p.timestamp AS TIMESTAMP) as last_traded_timestamp,
                    CAST(p.timestamp AS BIGINT) as last_traded_timestamp_ns,
                    CURRENT_TIMESTAMP as timestamp,
                    CAST(p.last_price AS DOUBLE) as last_price,
                    CAST(COALESCE(p.open, p.last_price) AS DOUBLE) as open_price,
                    CAST(COALESCE(p.high, p.last_price) AS DOUBLE) as high_price,
                    CAST(COALESCE(p.low, p.last_price) AS DOUBLE) as low_price,
                    CAST(COALESCE(p.close, p.last_price) AS DOUBLE) as close_price,
                    CAST(p.last_price AS DOUBLE) as average_traded_price,
                    CAST(COALESCE(p.volume, 0) AS BIGINT) as volume,
                    CAST(COALESCE(p.total_buy_quantity, 0) AS BIGINT) as total_buy_quantity,
                    CAST(COALESCE(p.total_sell_quantity, 0) AS BIGINT) as total_sell_quantity,
                    CAST(COALESCE(p.oi, 0) AS BIGINT) as open_interest,
                    NULL as oi_day_high,
                    NULL as oi_day_low,
                    CAST(COALESCE(p.bid_price_1, 0) AS DOUBLE) as bid_1_price,
                    CAST(COALESCE(p.bid_quantity_1, 0) AS BIGINT) as bid_1_quantity,
                    0 as bid_1_orders,
                    NULL as bid_2_price,
                    0 as bid_2_quantity,
                    0 as bid_2_orders,
                    NULL as bid_3_price,
                    0 as bid_3_quantity,
                    0 as bid_3_orders,
                    NULL as bid_4_price,
                    0 as bid_4_quantity,
                    0 as bid_4_orders,
                    NULL as bid_5_price,
                    0 as bid_5_quantity,
                    0 as bid_5_orders,
                    CAST(COALESCE(p.ask_price_1, 0) AS DOUBLE) as ask_1_price,
                    CAST(COALESCE(p.ask_quantity_1, 0) AS BIGINT) as ask_1_quantity,
                    0 as ask_1_orders,
                    NULL as ask_2_price,
                    0 as ask_2_quantity,
                    0 as ask_2_orders,
                    NULL as ask_3_price,
                    0 as ask_3_quantity,
                    0 as ask_3_orders,
                    NULL as ask_4_price,
                    0 as ask_4_quantity,
                    0 as ask_4_orders,
                    NULL as ask_5_price,
                    0 as ask_5_quantity,
                    0 as ask_5_orders,
                    'full' as packet_type,
                    'complete' as data_quality,
                    'regular' as session_type,
                    '{file_path}' as source_file,
                    'parquet_ingestion_duckdb_native' as processing_batch
                FROM read_parquet('{file_path}') p
                LEFT JOIN {token_metadata_view} m ON CAST(p.token AS BIGINT) = m.token
                WHERE CAST(p.token AS BIGINT) > 0
            """
        else:
            # No metadata enrichment
            insert_query = f"""
                INSERT OR REPLACE INTO tick_data_corrected
                SELECT
                    CAST(token AS BIGINT) as instrument_token,
                    '' as symbol,
                    '' as exchange,
                    '' as segment,
                    '' as instrument_type,
                    NULL as expiry,
                    NULL as strike_price,
                    NULL as option_type,
                    NULL as lot_size,
                    NULL as tick_size,
                    CAST(timestamp AS TIMESTAMP) as exchange_timestamp,
                    CAST(timestamp AS BIGINT) as exchange_timestamp_ns,
                    CAST(timestamp AS TIMESTAMP) as last_traded_timestamp,
                    CAST(timestamp AS BIGINT) as last_traded_timestamp_ns,
                    CURRENT_TIMESTAMP as timestamp,
                    CAST(last_price AS DOUBLE) as last_price,
                    CAST(COALESCE(open, last_price) AS DOUBLE) as open_price,
                    CAST(COALESCE(high, last_price) AS DOUBLE) as high_price,
                    CAST(COALESCE(low, last_price) AS DOUBLE) as low_price,
                    CAST(COALESCE(close, last_price) AS DOUBLE) as close_price,
                    CAST(last_price AS DOUBLE) as average_traded_price,
                    CAST(COALESCE(volume, 0) AS BIGINT) as volume,
                    CAST(COALESCE(total_buy_quantity, 0) AS BIGINT) as total_buy_quantity,
                    CAST(COALESCE(total_sell_quantity, 0) AS BIGINT) as total_sell_quantity,
                    CAST(COALESCE(oi, 0) AS BIGINT) as open_interest,
                    NULL as oi_day_high,
                    NULL as oi_day_low,
                    CAST(COALESCE(bid_price_1, 0) AS DOUBLE) as bid_1_price,
                    CAST(COALESCE(bid_quantity_1, 0) AS BIGINT) as bid_1_quantity,
                    0 as bid_1_orders,
                    NULL as bid_2_price,
                    0 as bid_2_quantity,
                    0 as bid_2_orders,
                    NULL as bid_3_price,
                    0 as bid_3_quantity,
                    0 as bid_3_orders,
                    NULL as bid_4_price,
                    0 as bid_4_quantity,
                    0 as bid_4_orders,
                    NULL as bid_5_price,
                    0 as bid_5_quantity,
                    0 as bid_5_orders,
                    CAST(COALESCE(ask_price_1, 0) AS DOUBLE) as ask_1_price,
                    CAST(COALESCE(ask_quantity_1, 0) AS BIGINT) as ask_1_quantity,
                    0 as ask_1_orders,
                    NULL as ask_2_price,
                    0 as ask_2_quantity,
                    0 as ask_2_orders,
                    NULL as ask_3_price,
                    0 as ask_3_quantity,
                    0 as ask_3_orders,
                    NULL as ask_4_price,
                    0 as ask_4_quantity,
                    0 as ask_4_orders,
                    NULL as ask_5_price,
                    0 as ask_5_quantity,
                    0 as ask_5_orders,
                    'full' as packet_type,
                    'complete' as data_quality,
                    'regular' as session_type,
                    '{file_path}' as source_file,
                    'parquet_ingestion_duckdb_native' as processing_batch
                FROM read_parquet('{file_path}')
                WHERE CAST(token AS BIGINT) > 0
            """
        
        # Get row count before insert - check if we can read with DuckDB or need pandas fallback
        try:
            count_query = f"SELECT COUNT(*) FROM read_parquet('{file_path}') WHERE CAST(token AS BIGINT) > 0"
            row_count = conn.execute(count_query).fetchone()[0]
        except Exception as count_error:
            # If DuckDB can't read, try pandas fallback for count too
            if any(err in str(count_error) for err in ['TProtocolException', 'don\'t know what type', 'Invalid data', 'Out of buffer', 'Snappy']):
                try:
                    import pandas as pd
                    df = pd.read_parquet(file_path)
                    row_count = len(df[df['token'] > 0]) if 'token' in df.columns else len(df)
                except:
                    row_count = 0
            else:
                raise
        
        # Execute insert - handle errors if file schema doesn't match
        try:
            conn.execute(insert_query)
        except Exception as insert_error:
            error_str = str(insert_error)
            # If INSERT fails due to missing columns, try pandas fallback
            if any(err in error_str for err in ['not found in FROM clause', 'Referenced column', 'TProtocolException', 'don\'t know what type', 'Invalid data']):
                try:
                    import pandas as pd
                    # Read with pandas and transform
                    df = pd.read_parquet(file_path)
                    if 'token' not in df.columns:
                        return {"success": False, "error": f"File missing 'token' column. Available: {', '.join(df.columns[:10])}", "row_count": 0}
                    
                    # Filter and transform
                    df = df[df['token'] > 0].copy()
                    if len(df) == 0:
                        return {"success": False, "error": "No valid rows after filtering", "row_count": 0}
                    
                    # Write to temp and ingest
                    temp_dir = Path(file_path).parent / ".temp_parquet"
                    temp_dir.mkdir(exist_ok=True)
                    temp_file = temp_dir / f"temp_{Path(file_path).name}"
                    df.to_parquet(temp_file, index=False, engine='pyarrow')
                    
                    # Retry insert with temp file
                    insert_query_temp = insert_query.replace(f"read_parquet('{file_path}')", f"read_parquet('{temp_file}')")
                    conn.execute(insert_query_temp)
                    
                    # Clean up
                    try:
                        temp_file.unlink()
                    except:
                        pass
                    
                    row_count = len(df)
                except Exception as pandas_error:
                    return {"success": False, "error": f"INSERT failed: {error_str[:100]}, Pandas fallback failed: {str(pandas_error)[:100]}", "row_count": 0}
            else:
                raise
        
        return {
            "success": True,
            "row_count": row_count,
            "original_rows": row_count
        }
        
    except Exception as e:
        logger.error(f"Error ingesting {file_path}: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "row_count": 0
        }


def get_pending_parquet_files(db_path: str, search_dirs: List[str] = None, date_filter: str = None) -> List[Path]:
    """Get list of parquet files that haven't been ingested
    
    Args:
        db_path: Path to DuckDB database
        search_dirs: Directories to search for parquet files
        date_filter: Optional date filter (e.g., '20251103' for Nov 3, 2025)
    """
    if search_dirs is None:
        search_dirs = [
            "real_parquet_data",
            "parquet_output",
            "binary_to_parquet",
            "tick_data_storage",
            "crawlers",
            "research",
            "research_data",
        ]
    
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        # Get ingested parquet files
        if date_filter:
            ingested_parquet = conn.execute("""
                SELECT DISTINCT source_file
                FROM tick_data_corrected
                WHERE source_file LIKE '%.parquet'
                AND source_file LIKE ?
            """, [f"%{date_filter}%"]).fetchall()
        else:
            ingested_parquet = conn.execute("""
                SELECT DISTINCT source_file
                FROM tick_data_corrected
                WHERE source_file LIKE '%.parquet'
            """).fetchall()
        
        ingested_paths = {Path(row[0]) for row in ingested_parquet if row[0]}
        conn.close()
    except Exception as e:
        logger.warning(f"Error reading database (assuming empty): {e}")
        ingested_paths = set()
    
    # Find parquet files on disk
    excluded_patterns = ['.venv', '__pycache__', 'site-packages', 'tests', 'test_', 'backtesting']
    
    all_parquet = set()
    for directory in search_dirs:
        dir_path = Path(directory)
        if dir_path.exists():
            for f in dir_path.rglob('*.parquet'):
                if any(excluded in str(f) for excluded in excluded_patterns):
                    continue
                if f.exists():
                    # Apply date filter if specified
                    if date_filter and date_filter not in str(f):
                        continue
                    all_parquet.add(f)
    
    # Find missing ones
    missing_parquet = all_parquet - ingested_paths
    return sorted(list(missing_parquet))


def enrich_parquet_file_worker(file_path_str: str, token_cache_path: str) -> dict:
    """Worker function to enrich a single parquet file with token metadata"""
    import sys
    from pathlib import Path
    import pandas as pd
    import pyarrow.parquet as pq
    import logging
    
    # Add project root to path for imports in worker
    project_root = Path(__file__).parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    from token_cache import TokenCacheManager
    
    # Setup logger for worker
    worker_logger = logging.getLogger(__name__)
    
    file_path = Path(file_path_str)
    try:
        # Load token cache in worker
        token_cache = TokenCacheManager(cache_path=token_cache_path, verbose=False)
        
        # These files contain binary tick data - parse them as binary, not parquet
        df = None
        
        # Method 1: Try reading as parquet first (in case some are actually parquet)
        try:
            df = pd.read_parquet(file_path)
            worker_logger.debug(f"  Read as parquet: {len(df)} rows")
        except Exception:
            # Method 2: Parse as binary tick data
            try:
                from binary_to_parquet.production_binary_converter import ProductionZerodhaBinaryConverter
                
                worker_logger.info(f"  Parsing {file_path.name} as binary tick data...")
                
                # Read raw bytes
                raw_data = Path(file_path).read_bytes()
                
                # Initialize converter
                converter = ProductionZerodhaBinaryConverter(
                    db_path=None,
                    token_cache=token_cache,
                    drop_unknown_tokens=False,
                    ensure_schema=False,
                )
                
                # Parse as binary
                packets = converter._detect_packet_format(raw_data)
                
                if not packets:
                    raise Exception("No packets detected in binary data")
                
                worker_logger.info(f"  Parsed {len(packets):,} packets from binary data")
                
                # Enrich packets
                enriched_records, _, _ = converter._enrich_packets(
                    packets, file_path, None
                )
                
                if not enriched_records:
                    raise Exception("No records after enrichment")
                
                # Convert to DataFrame
                df = pd.DataFrame(enriched_records)
                worker_logger.info(f"  Parsed {len(df)} rows from binary data")
                
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Failed to parse binary data: {str(e)[:200]}",
                    "file": str(file_path)
                }
        
        if df is None:
            # File couldn't be read at all - return the actual read error
            return {
                "success": False,
                "error": f"Cannot read file (corrupted): {read_error[:200] if read_error else 'All read methods failed'}",
                "file": str(file_path),
                "recovery_attempted": True
            }
        
        if len(df) == 0:
            return {"success": False, "error": "Empty file after recovery", "file": str(file_path)}
        
        # Enrich with token metadata
        if 'instrument_token' in df.columns:
            # Use vectorized operations for better performance
            def enrich_token(token_val):
                if pd.isna(token_val) or token_val is None:
                    return None
                try:
                    token = int(token_val)
                    if token in token_cache.token_map:
                        return token_cache.token_map[token]
                except (ValueError, TypeError):
                    pass
                return None
            
            # Create metadata columns
            metadata_list = df['instrument_token'].apply(enrich_token)
            
            # Extract symbol, exchange, segment, instrument_type from metadata
            # TokenCacheManager returns normalized metadata with 'symbol', 'exchange', 'segment', 'instrument_type'
            def extract_symbol(metadata):
                if not metadata:
                    return None
                return metadata.get("symbol", "")
            
            def extract_exchange(metadata):
                if not metadata:
                    return None
                return metadata.get("exchange", "")
            
            # Only update if missing or UNKNOWN - handle missing columns gracefully
            if 'symbol' in df.columns:
                mask_symbol = (df['symbol'].isna()) | (df['symbol'] == '') | (df['symbol'].astype(str).str.startswith('UNKNOWN', na=False))
                df.loc[mask_symbol, 'symbol'] = metadata_list.apply(extract_symbol)
            else:
                df['symbol'] = metadata_list.apply(extract_symbol)
            
            if 'exchange' in df.columns:
                mask_exchange = (df['exchange'].isna()) | (df['exchange'] == '')
                df.loc[mask_exchange, 'exchange'] = metadata_list.apply(extract_exchange)
            else:
                df['exchange'] = metadata_list.apply(extract_exchange)
            
            if 'segment' in df.columns:
                mask_segment = (df['segment'].isna()) | (df['segment'] == '')
                df.loc[mask_segment, 'segment'] = metadata_list.apply(lambda m: m.get("segment", "") if m else "")
            else:
                df['segment'] = metadata_list.apply(lambda m: m.get("segment", "") if m else "")
            
            if 'instrument_type' in df.columns:
                mask_instrument_type = (df['instrument_type'].isna()) | (df['instrument_type'] == '')
                df.loc[mask_instrument_type, 'instrument_type'] = metadata_list.apply(lambda m: m.get("instrument_type", "") if m else "")
            else:
                df['instrument_type'] = metadata_list.apply(lambda m: m.get("instrument_type", "") if m else "")
            
            # Add exchange_timestamp_ns if missing (try ALL fallbacks in order of preference)
            if 'exchange_timestamp_ns' not in df.columns or df['exchange_timestamp_ns'].isna().all() or (df['exchange_timestamp_ns'] == 0).all():
                timestamp_set = False
                
                # Fallback 1: Try exchange_timestamp_epoch (most accurate)
                if 'exchange_timestamp_epoch' in df.columns and df['exchange_timestamp_epoch'].notna().any():
                    try:
                        df['exchange_timestamp_ns'] = (df['exchange_timestamp_epoch'] * 1000000000).astype('int64')
                        if (df['exchange_timestamp_ns'] > 0).any():
                            timestamp_set = True
                    except:
                        pass
                
                # Fallback 2: Try exchange_timestamp (datetime or numeric)
                if not timestamp_set and 'exchange_timestamp' in df.columns and df['exchange_timestamp'].notna().any():
                    try:
                        # Try converting datetime to nanoseconds
                        if pd.api.types.is_datetime64_any_dtype(df['exchange_timestamp']):
                            df['exchange_timestamp_ns'] = (df['exchange_timestamp'].astype('int64') // 1).astype('int64')
                        else:
                            # Try as numeric (already in nanoseconds or seconds)
                            numeric_ts = pd.to_numeric(df['exchange_timestamp'], errors='coerce')
                            # If values are small (< 1e12), assume seconds, else assume nanoseconds
                            if numeric_ts.max() < 1e12:
                                df['exchange_timestamp_ns'] = (numeric_ts * 1000000000).astype('int64')
                            else:
                                df['exchange_timestamp_ns'] = numeric_ts.astype('int64')
                        
                        if (df['exchange_timestamp_ns'] > 0).any():
                            timestamp_set = True
                    except:
                        pass
                
                # Fallback 3: Try timestamp column (datetime or numeric)
                if not timestamp_set and 'timestamp' in df.columns and df['timestamp'].notna().any():
                    try:
                        # Try converting datetime to nanoseconds
                        if pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                            df['exchange_timestamp_ns'] = (df['timestamp'].astype('int64') // 1).astype('int64')
                        else:
                            # Try as numeric
                            numeric_ts = pd.to_numeric(df['timestamp'], errors='coerce')
                            if numeric_ts.max() < 1e12:
                                df['exchange_timestamp_ns'] = (numeric_ts * 1000000000).astype('int64')
                            else:
                                df['exchange_timestamp_ns'] = numeric_ts.astype('int64')
                        
                        if (df['exchange_timestamp_ns'] > 0).any():
                            timestamp_set = True
                    except:
                        pass
                
                # Fallback 4: Try last_traded_timestamp
                if not timestamp_set and 'last_traded_timestamp' in df.columns and df['last_traded_timestamp'].notna().any():
                    try:
                        if pd.api.types.is_datetime64_any_dtype(df['last_traded_timestamp']):
                            df['exchange_timestamp_ns'] = (df['last_traded_timestamp'].astype('int64') // 1).astype('int64')
                        else:
                            numeric_ts = pd.to_numeric(df['last_traded_timestamp'], errors='coerce')
                            if numeric_ts.max() < 1e12:
                                df['exchange_timestamp_ns'] = (numeric_ts * 1000000000).astype('int64')
                            else:
                                df['exchange_timestamp_ns'] = numeric_ts.astype('int64')
                        
                        if (df['exchange_timestamp_ns'] > 0).any():
                            timestamp_set = True
                    except:
                        pass
                
                # Fallback 5: Try last_traded_timestamp_ns (already in nanoseconds)
                if not timestamp_set and 'last_traded_timestamp_ns' in df.columns and df['last_traded_timestamp_ns'].notna().any():
                    try:
                        df['exchange_timestamp_ns'] = pd.to_numeric(df['last_traded_timestamp_ns'], errors='coerce').astype('int64')
                        if (df['exchange_timestamp_ns'] > 0).any():
                            timestamp_set = True
                    except:
                        pass
                
                # Fallback 6: Extract from filename pattern YYYYMMDD_HHMMSS
                if not timestamp_set:
                    import re
                    from pathlib import Path
                    from datetime import datetime
                    file_path_obj = Path(file_path)
                    match = re.search(r'(\d{8})_(\d{6})', file_path_obj.stem)
                    if match:
                        date_str = match.group(1)
                        time_str = match.group(2)
                        try:
                            dt = datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
                            base_ns = int(dt.timestamp() * 1e9)
                            # Create incremental timestamps for each row (1 second intervals)
                            df['exchange_timestamp_ns'] = (base_ns + pd.Series(range(len(df))) * 1_000_000_000).astype('int64')
                            timestamp_set = True
                        except:
                            pass
                
                # Fallback 7: Use current time as last resort
                if not timestamp_set:
                    import time
                    base_ns = int(time.time() * 1e9)
                    df['exchange_timestamp_ns'] = (base_ns + pd.Series(range(len(df))) * 1_000_000_000).astype('int64')
            
            # Ensure exchange_timestamp_ns is not null and is int64
            if 'exchange_timestamp_ns' not in df.columns:
                import time
                base_ns = int(time.time() * 1e9)
                df['exchange_timestamp_ns'] = (base_ns + pd.Series(range(len(df))) * 1_000_000_000).astype('int64')
            
            # Fill any remaining nulls with 0, then convert to int64
            df['exchange_timestamp_ns'] = df['exchange_timestamp_ns'].fillna(0).astype('int64')
            
            # Ensure no zeros remain (shouldn't happen with fallbacks, but safety check)
            if (df['exchange_timestamp_ns'] == 0).any():
                # For rows with zero, use filename fallback
                import re
                from pathlib import Path
                from datetime import datetime
                file_path_obj = Path(file_path)
                match = re.search(r'(\d{8})_(\d{6})', file_path_obj.stem)
                if match:
                    date_str = match.group(1)
                    time_str = match.group(2)
                    try:
                        dt = datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
                        base_ns = int(dt.timestamp() * 1e9)
                        zero_mask = df['exchange_timestamp_ns'] == 0
                        df.loc[zero_mask, 'exchange_timestamp_ns'] = (base_ns + pd.Series(range(zero_mask.sum())) * 1_000_000_000).astype('int64').values
                    except:
                        pass
            
            # Filter out UNKNOWN symbols
            before = len(df)
            df = df[
                (df['instrument_token'] > 0) &
                (df['symbol'].notna()) &
                (df['symbol'] != '') &
                (~df['symbol'].astype(str).str.startswith('UNKNOWN', na=False))
            ].copy()
            filtered = before - len(df)
            
            if len(df) == 0:
                return {"success": False, "error": "All rows filtered", "file": str(file_path)}
            
            # Write enriched parquet back (temporary location)
            enriched_path = file_path.parent / f".enriched_{file_path.name}"
            df.to_parquet(enriched_path, index=False, engine='pyarrow')
            
            return {
                "success": True,
                "file": str(file_path),
                "enriched_file": str(enriched_path),
                "rows": len(df),
                "filtered": filtered
            }
        else:
            return {"success": False, "error": "No instrument_token column", "file": str(file_path)}
            
    except Exception as e:
        return {"success": False, "error": str(e)[:200], "file": str(file_path)}


def bulk_ingest_enriched_parquets(conn: duckdb.DuckDBPyConnection, enriched_files: List[str], db_columns: List[str]) -> dict:
    """Bulk ingest multiple enriched parquet files using DuckDB native read_parquet()"""
    try:
        # Process files individually to get correct source_file for each
        total_rows = 0
        files_ingested = 0
        errors = 0
        for enriched_file in enriched_files:
            try:
                # Extract original file path from enriched file name
                enriched_path = Path(enriched_file)
                original_file = str(enriched_path.parent / enriched_path.name.replace('.enriched_', ''))
                
                # Get columns from this file
                sample_cols = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{enriched_file}') LIMIT 1").fetchdf()['column_name'].tolist()
                
                # Build SELECT query mapping columns
                select_parts = []
                for col in db_columns:
                    if col in sample_cols:
                        if col == 'source_file':
                            select_parts.append(f"'{original_file}' AS source_file")
                        else:
                            select_parts.append(f"p.{col}")
                    elif col == 'source_file':
                        select_parts.append(f"'{original_file}' AS source_file")
                    elif col == 'processing_batch':
                        select_parts.append("'parquet_ingestion_bulk' AS processing_batch")
                    elif col == 'timestamp' or col == 'processed_at':
                        select_parts.append(f"CURRENT_TIMESTAMP AS {col}")
                    elif col == 'parser_version':
                        select_parts.append("'v2.0' AS parser_version")
                    else:
                        select_parts.append(f"NULL AS {col}")
                
                # Insert with aggregation for duplicates
                # For columns that should be aggregated (volumes, quantities, prices), use MAX
                # For other columns, use ANY_VALUE or MAX
                agg_select_parts = []
                group_by_cols = ['instrument_token', 'exchange_timestamp_ns']
                
                for col in db_columns:
                    if col == 'source_file':
                        # Always override source_file with the original file path
                        agg_select_parts.append(f"'{original_file}' AS source_file")
                    elif col == 'processing_batch':
                        agg_select_parts.append("'parquet_ingestion_bulk' AS processing_batch")
                    elif col == 'timestamp' or col == 'processed_at':
                        agg_select_parts.append(f"CURRENT_TIMESTAMP AS {col}")
                    elif col == 'parser_version':
                        agg_select_parts.append("'v2.0' AS parser_version")
                    elif col in ['rsi_14', 'sma_20', 'ema_12', 'bollinger_upper', 'bollinger_middle', 'bollinger_lower',
                                  'macd', 'macd_signal', 'macd_histogram', 'delta', 'gamma', 'theta', 'vega', 'rho', 'implied_volatility']:
                        # Technical indicators and Greeks - NULL as DOUBLE (check BEFORE col in sample_cols)
                        # NEVER read these from parquet file, always use NULL
                        agg_select_parts.append(f"CAST(NULL AS DOUBLE) AS {col}")
                    elif col in sample_cols and col not in ['rsi_14', 'sma_20', 'ema_12', 'bollinger_upper', 'bollinger_middle', 'bollinger_lower',
                                                              'macd', 'macd_signal', 'macd_histogram', 'delta', 'gamma', 'theta', 'vega', 'rho', 'implied_volatility']:
                        # Column exists in parquet file - use it with proper casting
                        if col in group_by_cols:
                            if col == 'exchange_timestamp_ns':
                                # Ensure it's BIGINT
                                agg_select_parts.append(f"CAST(p.{col} AS BIGINT) AS {col}")
                            else:
                                agg_select_parts.append(f"p.{col}")
                        elif col in ['volume', 'total_buy_quantity', 'total_sell_quantity', 'open_interest', 
                                     'last_price', 'open_price', 'high_price', 'low_price', 'close_price',
                                     'average_traded_price', 'oi_day_high', 'oi_day_low']:
                            # Aggregate numeric columns with MAX and explicit casting
                            agg_select_parts.append(f"MAX(CAST(p.{col} AS DOUBLE)) AS {col}")
                        elif col == 'exchange_timestamp':
                            # Handle TIMESTAMP_NS or TIMESTAMP types
                            # Try to cast to TIMESTAMP, but handle TIMESTAMP_NS properly
                            agg_select_parts.append(f"ANY_VALUE(CAST(p.{col} AS TIMESTAMP)) AS {col}")
                        else:
                            # Use ANY_VALUE for other columns (they should be same for same PK)
                            agg_select_parts.append(f"ANY_VALUE(p.{col}) AS {col}")
                    else:
                        # Column doesn't exist in parquet - use NULL with proper type
                        if col in ['last_price', 'open_price', 'high_price', 'low_price', 'close_price', 'average_traded_price',
                                   'volume', 'total_buy_quantity', 'total_sell_quantity', 'open_interest', 'oi_day_high', 'oi_day_low']:
                            agg_select_parts.append(f"CAST(NULL AS DOUBLE) AS {col}")
                        elif col in ['instrument_token', 'exchange_timestamp_ns', 'last_traded_timestamp_ns']:
                            agg_select_parts.append(f"CAST(NULL AS BIGINT) AS {col}")
                        elif col == 'exchange_timestamp' or col == 'last_traded_timestamp':
                            agg_select_parts.append(f"CAST(NULL AS TIMESTAMP) AS {col}")
                        else:
                            agg_select_parts.append(f"NULL AS {col}")
                
                # Build explicit column list for parquet read to avoid schema inference issues
                # Exclude technical indicators that don't exist in the file
                tech_indicators = ['rsi_14', 'sma_20', 'ema_12', 'bollinger_upper', 'bollinger_middle', 'bollinger_lower',
                                  'macd', 'macd_signal', 'macd_histogram', 'delta', 'gamma', 'theta', 'vega', 'rho', 'implied_volatility']
                parquet_cols = [col for col in sample_cols if col not in tech_indicators]
                parquet_cols_str = ', '.join([f"p.{col}" for col in parquet_cols])
                
                # Build GROUP BY expression for exchange_timestamp_ns
                # Priority: exchange_timestamp_ns > timestamp_ms > exchange_timestamp_epoch > exchange_timestamp
                if 'exchange_timestamp_ns' in sample_cols:
                    group_by_expr = "CAST(p.exchange_timestamp_ns AS BIGINT)"
                elif 'timestamp_ms' in sample_cols:
                    # Use timestamp_ms (milliseconds) as anchor - convert to nanoseconds
                    group_by_expr = "CAST(COALESCE(CAST(p.timestamp_ms AS BIGINT), 0) * 1000000 AS BIGINT)"
                elif 'exchange_timestamp_epoch' in sample_cols:
                    group_by_expr = "CAST(p.exchange_timestamp_epoch * 1000000000 AS BIGINT)"
                elif 'exchange_timestamp' in sample_cols:
                    # Handle DOUBLE exchange_timestamp or string - prefer timestamp_ms if available
                    if 'timestamp_ms' in sample_cols:
                        group_by_expr = "CAST(COALESCE(CAST(p.timestamp_ms AS BIGINT), 0) * 1000000 AS BIGINT)"
                    else:
                        group_by_expr = "CAST(COALESCE(TRY_CAST(p.exchange_timestamp AS DOUBLE) * 1000000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000000000) AS BIGINT)"
                else:
                    group_by_expr = "CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000000000 AS BIGINT)"
                
                insert_query = f"""
                    INSERT OR REPLACE INTO tick_data_corrected
                    SELECT {', '.join(agg_select_parts)}
                    FROM (
                        SELECT {parquet_cols_str}
                        FROM read_parquet('{enriched_file}') p
                    ) p
                    WHERE p.instrument_token > 0
                    GROUP BY p.instrument_token, {group_by_expr}
                """
                
                # Debug: Log the SQL query if it contains bollinger_upper
                if 'bollinger_upper' in insert_query:
                    logger.warning(f"WARNING: bollinger_upper found in SQL query for {enriched_file}")
                    # Find the bollinger_upper line
                    for part in agg_select_parts:
                        if 'bollinger_upper' in part.lower():
                            logger.warning(f"  bollinger_upper part: {part}")
                
                try:
                    conn.execute(insert_query)
                except Exception as e:
                    # Log the SQL query on error for debugging
                    if 'bollinger_upper' in str(e).lower():
                        logger.error(f"Error with bollinger_upper for {enriched_file}")
                        logger.error(f"SQL query (first 1000 chars): {insert_query[:1000]}")
                        # Check what columns DuckDB sees
                        try:
                            sample_cols_check = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{enriched_file}') LIMIT 1").fetchdf()
                            logger.error(f"Columns DuckDB sees: {sample_cols_check['column_name'].tolist()}")
                        except:
                            pass
                    raise
                
                # Get row count for this file
                count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{enriched_file}') WHERE instrument_token > 0").fetchone()[0]
                total_rows += count
                files_ingested += 1
                
            except Exception as e:
                logger.error(f"Failed to ingest {enriched_file}: {type(e).__name__}: {e}")
                import traceback
                errors += 1
                continue
        
        return {"success": True, "files_ingested": files_ingested, "rows_ingested": total_rows, "errors": errors}
    except Exception as e:
        logger.error(f"Bulk ingest error: {type(e).__name__}: {e}")
        import traceback
        return {"success": False, "error": str(e)}


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Enrich and bulk ingest parquet files using DuckDB native SQL")
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--token-cache", default="core/data/token_lookup_enriched.json", 
                       help="Token lookup path")
    parser.add_argument("--search-dirs", nargs="+", help="Directories to search")
    parser.add_argument("--auto", action="store_true", help="Auto-find and ingest pending files")
    parser.add_argument("--date-filter", type=str, help="Filter files by date (e.g., '20251103')")
    parser.add_argument("--limit", type=int, help="Limit number of files")
    parser.add_argument("--workers", type=int, default=3, help="Number of parallel workers for enrichment")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for bulk ingestion")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    
    args = parser.parse_args()
    
    if not args.auto:
        parser.print_help()
        return
    
    # Get pending files
    search_dirs = args.search_dirs or [
        "real_parquet_data",
        "parquet_output",
        "binary_to_parquet",
        "tick_data_storage",
        "crawlers",
        "research",
        "research_data",
    ]
    file_paths = get_pending_parquet_files(args.db, search_dirs, date_filter=args.date_filter)
    
    if args.limit:
        file_paths = file_paths[:args.limit]
    
    logger.info(f"Found {len(file_paths):,} pending parquet files")
    
    if not file_paths:
        logger.info("No pending files")
        return
    
    if args.dry_run:
        logger.info("DRY RUN - Would process these files")
        for f in file_paths[:10]:
            logger.info(f"  {f}")
        return
    
    # STEP 1: Enrich parquet files in parallel using multiprocessing
    logger.info(f"📦 Step 1: Enriching {len(file_paths):,} parquet files with token metadata (workers={args.workers})...")
    
    enriched_files = []
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(enrich_parquet_file_worker, str(f), args.token_cache): f 
            for f in file_paths
        }
        
        for future in as_completed(futures):
            result = future.result()
            if result.get("success"):
                enriched_files.append(result["enriched_file"])
                if len(enriched_files) % 100 == 0:
                    logger.info(f"  Enriched: {len(enriched_files):,}/{len(file_paths):,} files")
            else:
                # Log first 10 errors
                if len([f for f in enriched_files if not f]) < 10:
                    logger.warning(f"  Failed to enrich {result.get('file', 'unknown')}: {result.get('error', 'unknown error')}")
    
    logger.info(f"✅ Enriched {len(enriched_files):,} files")
    
    # STEP 2: Bulk ingest enriched parquet files using DuckDB native read_parquet()
    logger.info(f"📥 Step 2: Bulk ingesting {len(enriched_files):,} enriched parquet files...")
    
    conn = duckdb.connect(args.db)
    ensure_production_schema(conn)
    
    # IMPORTANT: Order must match the actual database schema exactly!
    db_columns = [
        'instrument_token', 'symbol', 'exchange', 'segment', 'instrument_type',
        'expiry', 'strike_price', 'option_type', 'lot_size', 'tick_size', 'is_expired',
        'timestamp', 'exchange_timestamp', 'exchange_timestamp_ns',
        'last_traded_timestamp', 'last_traded_timestamp_ns', 'last_price',
        'open_price', 'high_price', 'low_price', 'close_price', 'average_traded_price',
        'volume', 'last_traded_quantity', 'total_buy_quantity', 'total_sell_quantity',
        'open_interest', 'oi_day_high', 'oi_day_low',
        'bid_1_price', 'bid_1_quantity', 'bid_1_orders',
        'bid_2_price', 'bid_2_quantity', 'bid_2_orders',
        'bid_3_price', 'bid_3_quantity', 'bid_3_orders',
        'bid_4_price', 'bid_4_quantity', 'bid_4_orders',
        'bid_5_price', 'bid_5_quantity', 'bid_5_orders',
        'ask_1_price', 'ask_1_quantity', 'ask_1_orders',
        'ask_2_price', 'ask_2_quantity', 'ask_2_orders',
        'ask_3_price', 'ask_3_quantity', 'ask_3_orders',
        'ask_4_price', 'ask_4_quantity', 'ask_4_orders',
        'ask_5_price', 'ask_5_quantity', 'ask_5_orders',
        # Technical indicators (BEFORE packet_type in database schema!)
        'rsi_14', 'sma_20', 'ema_12', 'bollinger_upper', 'bollinger_middle', 'bollinger_lower',
        'macd', 'macd_signal', 'macd_histogram',
        # Option Greeks
        'delta', 'gamma', 'theta', 'vega', 'rho', 'implied_volatility',
        'packet_type', 'data_quality', 'session_type', 
        'source_file', 'processing_batch', 'session_id',
        'processed_at', 'parser_version'
    ]
    
    total_rows = 0
    files_ingested = 0
    errors = 0
    
    # Process in batches
    for i in range(0, len(enriched_files), args.batch_size):
        batch = enriched_files[i:i + args.batch_size]
        try:
            result = bulk_ingest_enriched_parquets(conn, batch, db_columns)
            if result.get("success"):
                total_rows += result.get("row_count", 0)
                files_ingested += len(batch)
                logger.info(f"Progress: {files_ingested}/{len(enriched_files)} files, {total_rows:,} rows")
            else:
                errors += len(batch)
                logger.error(f"Batch failed: {result.get('error')}")
        except Exception as e:
            errors += len(batch)
            logger.error(f"Exception in batch: {e}")
    
    conn.close()
    
    # Clean up enriched temp files
    for enriched_file in enriched_files:
        try:
            Path(enriched_file).unlink()
        except:
            pass
    
    logger.info(f"✅ Complete: {files_ingested} files, {total_rows:,} rows, {errors} errors")


if __name__ == "__main__":
    main()

