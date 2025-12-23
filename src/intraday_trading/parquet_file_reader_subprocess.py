#!/opt/homebrew/bin/python3.13
"""
ISOLATED PARQUET FILE READER - Subprocess Worker
Reads parquet files in complete isolation to contain Rust panics
This script is called as a subprocess to read individual files safely
"""

import sys
import json
import traceback
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# CRITICAL: Set environment variables BEFORE importing PyArrow
# This must happen before any PyArrow imports to prevent thread pool initialization
import os
os.environ['ARROW_CPU_COUNT'] = '1'
os.environ['ARROW_IO_THREADS'] = '1'
os.environ['OMP_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'
os.environ['NUMEXPR_NUM_THREADS'] = '1'

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None

try:
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
    # Disable thread pool
    if hasattr(pq, 'set_use_threads'):
        pq.set_use_threads(False)
except ImportError:
    PYARROW_AVAILABLE = False
    pq = None

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    duckdb = None

def _normalize_datetime_columns(df):
    """Normalize datetime columns to datetime[ms, UTC]"""
    if not POLARS_AVAILABLE or df is None:
        return df
    
    try:
        # Get all datetime columns
        datetime_cols = [col for col in df.columns if df[col].dtype in [pl.Datetime, pl.Datetime('ms'), pl.Datetime('us'), pl.Datetime('ns')]]
        
        if not datetime_cols:
            return df
        
        # Convert each datetime column to datetime[ms, UTC]
        for col in datetime_cols:
            try:
                df = df.with_columns([
                    pl.col(col).cast(pl.Datetime('ms', 'UTC')).alias(col)
                ])
            except:
                pass
        
        return df
    except:
        return df

def read_parquet_safe(file_path: str) -> dict:
    """
    Read parquet file safely with multiple fallback methods.
    Returns dict with 'success', 'data' (as JSON-serializable), 'error', 'method'
    """
    result = {
        'success': False,
        'data': None,
        'error': None,
        'method': None,
        'rows': 0
    }
    
    # Method 1: Try DuckDB (handles schema inconsistencies best)
    if DUCKDB_AVAILABLE:
        try:
            conn = duckdb.connect()
            conn.execute("SET memory_limit='1GB'")
            conn.execute("PRAGMA disable_optimizer")
            conn.execute("SET threads=1")
            
            arrow_table = conn.execute(f"SELECT * FROM read_parquet('{file_path}')").arrow()
            df = pl.from_arrow(arrow_table)
            df = _normalize_datetime_columns(df)
            conn.close()
            
            # Convert to dict for JSON serialization
            data = df.to_dicts()
            result['success'] = True
            result['data'] = data
            result['method'] = 'duckdb'
            result['rows'] = len(df)
            return result
        except Exception as e:
            result['error'] = str(e)[:200]
    
    # Method 2: Try Polars direct
    if POLARS_AVAILABLE:
        try:
            df = pl.read_parquet(file_path)
            df = _normalize_datetime_columns(df)
            data = df.to_dicts()
            result['success'] = True
            result['data'] = data
            result['method'] = 'polars'
            result['rows'] = len(df)
            return result
        except Exception as e:
            result['error'] = str(e)[:200]
    
    # Method 3: Try PyArrow then convert to Polars
    if PYARROW_AVAILABLE and POLARS_AVAILABLE:
        try:
            table = pq.read_table(file_path, use_threads=False)
            df = pl.from_arrow(table)
            df = _normalize_datetime_columns(df)
            data = df.to_dicts()
            result['success'] = True
            result['data'] = data
            result['method'] = 'pyarrow'
            result['rows'] = len(df)
            return result
        except Exception as e:
            result['error'] = str(e)[:200]
    
    # Method 4: Try reading row groups individually
    if PYARROW_AVAILABLE and POLARS_AVAILABLE:
        try:
            parquet_file = pq.ParquetFile(file_path)
            all_rows = []
            
            for i in range(parquet_file.num_row_groups):
                try:
                    row_group = parquet_file.read_row_group(i)
                    df_group = pl.from_arrow(row_group)
                    df_group = _normalize_datetime_columns(df_group)
                    all_rows.extend(df_group.to_dicts())
                except:
                    continue
            
            if all_rows:
                result['success'] = True
                result['data'] = all_rows
                result['method'] = 'rowgroup'
                result['rows'] = len(all_rows)
                return result
        except Exception as e:
            result['error'] = str(e)[:200]
    
    return result

def main():
    """Main entry point - called as subprocess"""
    if len(sys.argv) < 2:
        print(json.dumps({
            'success': False,
            'error': 'No file path provided',
            'method': None,
            'rows': 0
        }))
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    try:
        result = read_parquet_safe(file_path)
        print(json.dumps(result, default=str))
        
        if result['success']:
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        error_result = {
            'success': False,
            'error': str(e)[:500],
            'method': None,
            'rows': 0
        }
        print(json.dumps(error_result))
        sys.exit(1)

if __name__ == "__main__":
    main()

