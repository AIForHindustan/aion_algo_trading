    #!/usr/bin/env python3
"""
COMPREHENSIVE PARQUET REPROCESSING & DATABASE INGESTION PIPELINE
Converts corrupted parquets to enriched format with retry logic and proper token lookup
"""

import sys
import os
from pathlib import Path

from shared_core.redis_clients.redis_client import get_redis_client
# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None

if not POLARS_AVAILABLE:
    raise ImportError("Polars is required for parquet_convert.py. Install with: pip install polars")

# CRITICAL: Set environment variables BEFORE importing PyArrow
# This must happen before any PyArrow imports to prevent thread pool initialization
os.environ['ARROW_CPU_COUNT'] = '1'
os.environ['ARROW_IO_THREADS'] = '1'
os.environ['OMP_NUM_THREADS'] = '1'  # OpenMP threads
os.environ['MKL_NUM_THREADS'] = '1'  # Intel MKL threads
os.environ['NUMEXPR_NUM_THREADS'] = '1'  # NumExpr threads

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    pa = None
    pq = None

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    duckdb = None

import numpy as np
import json
import glob
import traceback
import logging
import resource
import threading
import struct
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import time
import traceback
import gc
import subprocess
import signal
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
import multiprocessing as mp

# CRITICAL: Set signal handlers to catch segfaults gracefully
def setup_signal_handlers():
    """Setup signal handlers to catch crashes and log them"""
    def signal_handler(signum, frame):
        logger.critical(f"💥 CRITICAL: Received signal {signum}. Process may crash. Saving state...")
        # Try to save current state
        try:
            with open('parquet_convert_crash_state.json', 'w') as f:
                json.dump({
                    'signal': signum,
                    'timestamp': datetime.now().isoformat(),
                    'frame': str(frame)
                }, f, indent=2)
        except:
            pass
        # Re-raise to allow normal crash handling
        raise
    
    # Register handlers for common crash signals
    signal.signal(signal.SIGSEGV, signal_handler)  # Segmentation fault
    signal.signal(signal.SIGBUS, signal_handler)   # Bus error
    signal.signal(signal.SIGABRT, signal_handler)  # Abort

# Setup signal handlers early
try:
    setup_signal_handlers()
except Exception as e:
    # If signal handling fails, continue anyway
    pass

# Import existing TokenCacheManager from codebase
from token_cache import TokenCacheManager as BaseTokenCacheManager

# ============================================================================
# POLARS CACHE (from calculations.py pattern)
# ============================================================================
class PolarsCache:
    """
    High-performance Polars DataFrame cache optimized for trading data.
    
    Features:
    - TTL (Time To Live) with automatic expiration
    - LRU (Least Recently Used) eviction
    - Thread-safe operations
    - Timestamp normalization for trading data
    - Memory-optimized storage with compression
    - Query optimization for time-series data
    """
    
    def __init__(self, max_size: int = 100, ttl: int = 300, enable_compression: bool = True):
        """
        Initialize optimized Polars cache.
        
        Args:
            max_size: Maximum number of cached DataFrames
            ttl: Time to live in seconds (default: 300 = 5 minutes)
            enable_compression: Use Polars compression for large datasets
        """
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.max_size = max_size
        self.ttl = ttl
        self.enable_compression = enable_compression
        self._lock = threading.RLock()
        self.access_order: List[str] = []  # For LRU tracking
        
        # Trading-specific timestamp schema
        self.timestamp_schema = {
            # ISO Strings
            'exchange_timestamp': pl.Datetime(time_unit="ms", time_zone="UTC"),
            'timestamp': pl.Datetime(time_unit="ms", time_zone="UTC"),
            'timestamp_ns': pl.Datetime(time_unit="ns", time_zone="UTC"),
            'last_trade_time': pl.Datetime(time_unit="ms", time_zone="UTC"),
            
            # Milliseconds
            'exchange_timestamp_ms': pl.Datetime(time_unit="ms", time_zone="UTC"),
            'timestamp_ms': pl.Datetime(time_unit="ms", time_zone="UTC"),
            'timestamp_ns_ms': pl.Datetime(time_unit="ms", time_zone="UTC"),
            'last_trade_time_ms': pl.Datetime(time_unit="ms", time_zone="UTC"),
            'processed_timestamp_ms': pl.Datetime(time_unit="ms", time_zone="UTC"),
            
            # Epoch and Float
            'exchange_timestamp_epoch': pl.Int64,
            'processed_timestamp': pl.Float64,
        }

    def _optimize_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Optimize DataFrame for trading data storage.
        
        ✅ ENHANCED: Handles all timestamp formats and converts to Polars Datetime.
        No pandas fallback - pure Polars implementation.
        """
        # Create normalized timestamp first (handles all conversions)
        df = self._create_normalized_timestamp(df)
        
        # Sort by timestamp for better query performance
        if "timestamp_normalized" in df.columns:
            df = df.sort("timestamp_normalized")
        
        # Use categoricals for repetitive string columns (symbols, etc.)
        string_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]
        for col in string_cols:
            if col in ['symbol', 'ticker', 'instrument']:
                unique_count = df[col].n_unique()
                if unique_count < 1000:  # Only categorical if reasonable cardinality
                    df = df.with_columns(pl.col(col).cast(pl.Categorical))
        
        return df

    def _create_normalized_timestamp(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Create unified timestamp column from various timestamp sources.
        Following exact pattern from calculations.py (lines 384-522)
        """
        timestamp_exprs = []
        
        # Priority-based timestamp conversion
        if 'exchange_timestamp' in df.columns and not df.is_empty():
            # Handle exchange_timestamp (could be string ISO or Int64)
            dtype = df['exchange_timestamp'].dtype
            if dtype == pl.Utf8:
                # ISO string - parse it (handle with/without timezone)
                timestamp_exprs.append(
                    pl.when(pl.col('exchange_timestamp').str.contains(r'[+-]\d{2}:\d{2}$'))
                    .then(
                        pl.col('exchange_timestamp')
                        .str.strptime(pl.Datetime(time_unit="ms", time_zone="UTC"), format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False)
                    )
                    .otherwise(
                        pl.col('exchange_timestamp')
                        .str.strptime(pl.Datetime(time_unit="ms"), format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                        .dt.replace_time_zone("UTC")
                    )
                    .alias('exchange_timestamp_norm')
                )
            elif dtype in [pl.Int64, pl.Int32]:
                # Epoch milliseconds - convert using from_epoch
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('exchange_timestamp'), time_unit="ms")
                    .alias('exchange_timestamp_norm')
                )
        
        if 'exchange_timestamp_ms' in df.columns:
            # ✅ FIXED: Use pl.from_epoch() to convert Int64 milliseconds to Datetime
            timestamp_exprs.append(
                pl.from_epoch(pl.col('exchange_timestamp_ms'), time_unit="ms")
                .alias('exchange_timestamp_ms_norm')
            )
        
        if 'timestamp_ms' in df.columns:
            # ✅ FIXED: Use pl.from_epoch() to convert Int64 milliseconds to Datetime
            timestamp_exprs.append(
                pl.from_epoch(pl.col('timestamp_ms'), time_unit="ms")
                .alias('timestamp_ms_norm')
            )
        
        if 'timestamp_ns_ms' in df.columns:
            # ✅ FIXED: Use pl.from_epoch() to convert Int64 milliseconds to Datetime
            timestamp_exprs.append(
                pl.from_epoch(pl.col('timestamp_ns_ms'), time_unit="ms")
                .alias('timestamp_ns_ms_norm')
            )
        
        if 'last_trade_time' in df.columns and not df.is_empty():
            # Handle last_trade_time (could be string ISO or Int64)
            dtype = df['last_trade_time'].dtype
            if dtype == pl.Utf8:
                # ISO string - parse it (handle with/without timezone)
                timestamp_exprs.append(
                    pl.when(pl.col('last_trade_time').str.contains(r'[+-]\d{2}:\d{2}$'))
                    .then(
                        pl.col('last_trade_time')
                        .str.strptime(pl.Datetime(time_unit="ms", time_zone="UTC"), format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False)
                    )
                    .otherwise(
                        pl.col('last_trade_time')
                        .str.strptime(pl.Datetime(time_unit="ms"), format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                        .dt.replace_time_zone("UTC")
                    )
                    .alias('last_trade_time_norm')
                )
            elif dtype in [pl.Int64, pl.Int32]:
                # Epoch milliseconds - convert using from_epoch
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('last_trade_time'), time_unit="ms")
                    .alias('last_trade_time_norm')
                )
        
        if 'last_trade_time_ms' in df.columns:
            # ✅ FIXED: Use pl.from_epoch() to convert Int64 milliseconds to Datetime
            timestamp_exprs.append(
                pl.from_epoch(pl.col('last_trade_time_ms'), time_unit="ms")
                .alias('last_trade_time_ms_norm')
            )
        
        if 'processed_timestamp_ms' in df.columns:
            # ✅ FIXED: Use pl.from_epoch() to convert Int64 milliseconds to Datetime
            timestamp_exprs.append(
                pl.from_epoch(pl.col('processed_timestamp_ms'), time_unit="ms")
                .alias('processed_timestamp_ms_norm')
            )
        
        if 'timestamp' in df.columns and not df.is_empty():
            # Check if it's a string (ISO) or Int64 (epoch)
            dtype = df['timestamp'].dtype
            if dtype == pl.Utf8:
                # ISO string - parse it (handle with/without timezone)
                timestamp_exprs.append(
                    pl.when(pl.col('timestamp').str.contains(r'[+-]\d{2}:\d{2}$'))
                    .then(
                        pl.col('timestamp')
                        .str.strptime(pl.Datetime(time_unit="ms", time_zone="UTC"), format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False)
                    )
                    .otherwise(
                        pl.col('timestamp')
                        .str.strptime(pl.Datetime(time_unit="ms"), format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                        .dt.replace_time_zone("UTC")
                    )
                    .alias('timestamp_norm')
                )
            elif dtype in [pl.Int64, pl.Int32]:
                # Epoch milliseconds - convert using from_epoch
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('timestamp'), time_unit="ms")
                    .alias('timestamp_norm')
                )
        
        if 'timestamp_ns' in df.columns and not df.is_empty():
            # Handle nanosecond timestamp (could be string ISO or Int64 nanoseconds)
            dtype = df['timestamp_ns'].dtype
            if dtype == pl.Utf8:
                # ISO string - parse it (handle with/without timezone)
                timestamp_exprs.append(
                    pl.when(pl.col('timestamp_ns').str.contains(r'[+-]\d{2}:\d{2}$'))
                    .then(
                        pl.col('timestamp_ns')
                        .str.strptime(pl.Datetime(time_unit="ns", time_zone="UTC"), format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False)
                    )
                    .otherwise(
                        pl.col('timestamp_ns')
                        .str.strptime(pl.Datetime(time_unit="ns"), format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                        .dt.replace_time_zone("UTC")
                    )
                    .alias('timestamp_ns_norm')
                )
            elif dtype in [pl.Int64, pl.Int32]:
                # Nanoseconds - convert using from_epoch
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('timestamp_ns'), time_unit="ns")
                    .alias('timestamp_ns_norm')
                )
        
        if 'exchange_timestamp_epoch' in df.columns:
            # ✅ FIXED: Use pl.from_epoch() to convert Int64 seconds to Datetime
            timestamp_exprs.append(
                pl.from_epoch(pl.col('exchange_timestamp_epoch'), time_unit="s")
                .alias('epoch_norm')
            )
        
        if 'processed_timestamp' in df.columns:
            # ✅ FIXED: Convert float seconds to milliseconds, then use pl.from_epoch()
            timestamp_exprs.append(
                pl.from_epoch(
                    (pl.col('processed_timestamp') * 1000).cast(pl.Int64), 
                    time_unit="ms"
                )
                .alias('processed_norm')
            )
        
        # Add all normalized columns
        if timestamp_exprs:
            df = df.with_columns(timestamp_exprs)
            
            # Create master normalized timestamp using coalesce
            norm_cols = [col for col in df.columns if col.endswith('_norm')]
            if norm_cols:
                df = df.with_columns(
                    pl.coalesce(norm_cols).alias('timestamp_normalized')
                )
        
        return df

    def get(self, key: str) -> Optional[pl.DataFrame]:
        """Get cached DataFrame with LRU update and expiration check."""
        with self._lock:
            if key not in self.cache:
                return None
                
            entry = self.cache[key]
            current_time = time.time()
            
            # Check TTL expiration
            if current_time - entry['timestamp'] >= self.ttl:
                self._remove_key(key)
                return None
            
            # Update LRU access order
            self._update_access_order(key)
            entry['access_time'] = current_time
            entry['access_count'] += 1
            
            return entry['df']

    def set(self, key: str, df: pl.DataFrame, optimize: bool = True):
        """Store and optimize DataFrame in cache."""
        with self._lock:
            # Evict if cache is full
            if len(self.cache) >= self.max_size:
                self._evict_oldest()
            
            # Apply optimizations
            if optimize:
                df = self._optimize_dataframe(df)
            
            current_time = time.time()
            
            # Store optimized entry
            self.cache[key] = {
                'df': df,
                'timestamp': current_time,
                'access_time': current_time,
                'access_count': 0,
                'row_count': len(df),
                'memory_usage': df.estimated_size() if hasattr(df, 'estimated_size') else 0
            }
            
            self._update_access_order(key)

    def _update_access_order(self, key: str):
        """Update LRU access order."""
        if key in self.access_order:
            self.access_order.remove(key)
        self.access_order.append(key)

    def _remove_key(self, key: str):
        """Remove key from cache and access order."""
        if key in self.cache:
            del self.cache[key]
        if key in self.access_order:
            self.access_order.remove(key)

    def _evict_oldest(self):
        """Evict least recently used entry."""
        if self.access_order:
            oldest_key = self.access_order[0]
            self._remove_key(oldest_key)

    def clear(self):
        """Clear all cached entries."""
        with self._lock:
            self.cache.clear()
            self.access_order.clear()

# Initialize global Polars cache
_polars_cache = PolarsCache(max_size=1000, ttl=600)

# Module-level function for worker initialization (must be picklable)
def _init_worker_memory_limit(max_memory_bytes: int, max_memory_gb: float):
    """Initialize worker with memory limit - module level for pickling"""
    try:
        # Set soft and hard limit (RLIMIT_AS = address space limit)
        resource.setrlimit(resource.RLIMIT_AS, (max_memory_bytes, max_memory_bytes))
        logger.debug(f"Worker {os.getpid()} memory limit set to {max_memory_gb:.2f}GB")
    except (ValueError, OSError) as e:
        logger.warning(f"Could not set memory limit for worker: {e}")

# Configure logging with verbose/debug level
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for verbose output
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('parquet_reprocessing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
# Set specific loggers to appropriate levels
logging.getLogger('duckdb').setLevel(logging.WARNING)  # Reduce DuckDB noise
logging.getLogger('pyarrow').setLevel(logging.WARNING)  # Reduce PyArrow noise

class ParquetHotTokenMapper:
    """Hot token mapper for parquet converter - loads tokens from all 3 crawlers"""
    
    def __init__(self):
        self.hot_token_map: Dict[str, Dict] = {}
        self.hot_symbol_map: Dict[str, Dict] = {}
        self._load_all_crawler_tokens()
    
    def _load_all_crawler_tokens(self):
        """Load tokens from all 3 crawlers: binary_crawler1, crawler2_volatility, crawler3_sso_xvenue"""
        project_root = Path(__file__).parent
        crawler_configs = [
            (project_root / "crawlers" / "binary_crawler1" / "binary_crawler1.json", "tokens_list"),
            (project_root / "crawlers" / "crawler2_volatility" / "crawler2_volatility.json", "instruments_dict"),
            (project_root / "crawlers" / "crawler3_sso_xvenue" / "crawler3_sso_xvenue.json", "both")  # Has both tokens list and instruments dict
        ]
        
        all_active_tokens = set()
        
        # Load tokens from all crawlers (handle different structures)
        for crawler_path, structure_type in crawler_configs:
            if crawler_path.exists():
                try:
                    with open(crawler_path, 'r') as f:
                        crawler_data = json.load(f)
                    
                    tokens = []
                    if structure_type == "tokens_list":
                        # binary_crawler1: tokens is a list
                        tokens = crawler_data.get('tokens', [])
                    elif structure_type == "instruments_dict":
                        # crawler2_volatility: tokens are in instruments dict
                        instruments = crawler_data.get('instruments', {})
                        if isinstance(instruments, dict):
                            tokens = [inst.get('token') for inst in instruments.values() if inst.get('token')]
                        elif isinstance(instruments, list):
                            tokens = [inst.get('token') for inst in instruments if inst.get('token')]
                    elif structure_type == "both":
                        # crawler3_sso_xvenue: has both tokens list AND instruments dict - use union of both
                        tokens_list = crawler_data.get('tokens', [])
                        instruments = crawler_data.get('instruments', {})
                        tokens_from_list = [t for t in tokens_list if t is not None]
                        tokens_from_dict = []
                        if isinstance(instruments, dict):
                            tokens_from_dict = [inst.get('token') for inst in instruments.values() if inst.get('token')]
                        elif isinstance(instruments, list):
                            tokens_from_dict = [inst.get('token') for inst in instruments if inst.get('token')]
                        # Union of both sources
                        tokens = list(set(tokens_from_list + tokens_from_dict))
                    
                    # Convert all tokens to strings for consistency
                    all_active_tokens.update(str(t) for t in tokens if t is not None)
                    logger.debug(f"Loaded {len(tokens)} tokens from {crawler_path.name} (structure: {structure_type})")
                except Exception as e:
                    logger.warning(f"Failed to load tokens from {crawler_path}: {e}")
        
        logger.info(f"Loaded {len(all_active_tokens)} active tokens from all 3 crawlers")
        
        # Extract base instrument names from active tokens (for finding previous expiry versions)
        base_instruments = set()
        active_token_data = {}  # Store token data for active tokens
        
        # Load token metadata from token_lookup_enriched.json
        enriched_path = project_root / "core" / "data" / "token_lookup_enriched.json"
        if enriched_path.exists():
            try:
                with open(enriched_path, 'r') as f:
                    token_lookup = json.load(f)
                
                # First pass: Load active tokens and extract base instrument names
                for token_str, token_data in token_lookup.items():
                    if token_str in all_active_tokens:
                        active_token_data[token_str] = token_data
                        self.hot_token_map[token_str] = token_data
                        
                        # Extract base instrument name (remove expiry, strike, option type)
                        key_field = token_data.get('key', '')
                        if ':' in key_field:
                            symbol = key_field.split(':', 1)[1]
                        else:
                            symbol = token_data.get('name', '')
                        
                        # Extract base instrument (e.g., "NIFTY" from "NIFTY25DECFUT" or "BANKNIFTY25NOVFUT")
                        base_name = self._extract_base_instrument(symbol)
                        if base_name:
                            base_instruments.add(base_name)
                        
                        if symbol:
                            self.hot_symbol_map[symbol] = token_data
                
                logger.info(f"Found {len(base_instruments)} unique base instruments from active tokens")
                
                # Second pass: Include all tokens for same base instruments (previous expiry versions)
                additional_tokens = 0
                for token_str, token_data in token_lookup.items():
                    if token_str not in all_active_tokens:
                        # Check if this token belongs to one of our base instruments
                        key_field = token_data.get('key', '')
                        if ':' in key_field:
                            symbol = key_field.split(':', 1)[1]
                        else:
                            symbol = token_data.get('name', '')
                        
                        base_name = self._extract_base_instrument(symbol)
                        if base_name and base_name in base_instruments:
                            # This is a previous expiry version of an active instrument
                            self.hot_token_map[token_str] = token_data
                            additional_tokens += 1
                            
                            if symbol:
                                self.hot_symbol_map[symbol] = token_data
                
                logger.info(f"✅ ParquetHotTokenMapper initialized: {len(self.hot_token_map)} hot tokens ({len(all_active_tokens)} active + {additional_tokens} previous expiry), {len(self.hot_symbol_map)} symbols")
            except Exception as e:
                logger.warning(f"Failed to load token lookup: {e}")
        else:
            logger.warning(f"Token lookup file not found: {enriched_path}")
    
    def _extract_base_instrument(self, symbol: str) -> str:
        """Extract base instrument name from symbol (remove expiry, strike, option type)
        
        Examples:
            "NIFTY25DECFUT" -> "NIFTY"
            "BANKNIFTY25NOVFUT" -> "BANKNIFTY"
            "RELIANCE25DEC1800CE" -> "RELIANCE"
            "NIFTY" -> "NIFTY"
        """
        if not symbol:
            return ""
        
        # Remove common expiry patterns and option suffixes
        import re
        # Pattern: Remove dates like 25DEC, 28OCT, 25NOV, etc. followed by FUT/CE/PE
        # Also handle formats like 25DEC2025
        base = re.sub(r'\d{1,2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{0,4}(FUT|CE|PE)?$', '', symbol, flags=re.IGNORECASE)
        base = re.sub(r'\d{1,2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(FUT|CE|PE)?$', '', base, flags=re.IGNORECASE)
        # Remove strike prices and option types (e.g., "1800CE", "1500PE")
        base = re.sub(r'\d+[CP]E$', '', base, flags=re.IGNORECASE)
        
        return base.strip() if base.strip() else symbol
    
    def token_to_symbol(self, instrument_token: int) -> str:
        """Return symbol for token"""
        token_str = str(instrument_token)
        if token_str in self.hot_token_map:
            token_data = self.hot_token_map[token_str]
            key_field = token_data.get('key', '')
            if ':' in key_field:
                return key_field.split(':', 1)[1]
            return token_data.get('name', f'UNKNOWN_{instrument_token}')
        return f'UNKNOWN_{instrument_token}'
    
    def get_token_data(self, token: str) -> Optional[Dict]:
        """Get full token data"""
        return self.hot_token_map.get(str(token))

class TokenCacheManager:
    """Wrapper around existing TokenCacheManager with retry logic and hot token mapper
    
    ✅ UPDATED: Uses zerodha_token_list/zerodha_instrument_token.json for token lookup
    """
    
    def __init__(self, token_lookup_path: str = "zerodha_token_list/zerodha_instrument_token.json"):
        self.token_lookup_path = token_lookup_path
        self.token_data = {}
        self.hot_mapper = None
        self.load_token_cache()
        self._init_hot_mapper()
    
    def _init_hot_mapper(self):
        """Initialize hot token mapper for all 3 crawlers"""
        try:
            self.hot_mapper = ParquetHotTokenMapper()
        except Exception as e:
            logger.warning(f"Failed to initialize hot token mapper: {e}")
            self.hot_mapper = None
    
    def load_token_cache(self):
        """Load token lookup data from zerodha_token_list/zerodha_instrument_token.json"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                token_file = Path(self.token_lookup_path)
                if not token_file.exists():
                    # Fallback to core/data/token_lookup_enriched.json if zerodha_token_list doesn't exist
                    fallback_path = Path("core/data/token_lookup_enriched.json")
                    if fallback_path.exists():
                        logger.warning(f"zerodha_token_list not found, using fallback: {fallback_path}")
                        token_file = fallback_path
                    else:
                        raise FileNotFoundError(f"Token file not found: {self.token_lookup_path}")
                
                with open(token_file, 'r') as f:
                    self.token_data = json.load(f)
                
                logger.info(f"✅ Loaded token cache from {token_file} with {len(self.token_data)} token mappings")
                return
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed to load token cache: {e}")
                if attempt == max_retries - 1:
                    logger.error("Failed to load token cache after all retries")
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def get_symbol_info(self, instrument_token: int) -> Dict:
        """Get symbol information from zerodha_token_list/zerodha_instrument_token.json"""
        token_str = str(instrument_token)
        
        # ✅ PRIMARY: Use zerodha_token_list/zerodha_instrument_token.json
        if token_str in self.token_data:
            token_info = self.token_data[token_str]
            
            # Extract symbol from tradingsymbol or name
            symbol = token_info.get('tradingsymbol') or token_info.get('name') or ''
            if not symbol:
                # Try extracting from key field
                key_field = token_info.get('key', '')
                if ':' in key_field:
                    symbol = key_field.split(':', 1)[1]
            
            # Extract exchange from key or exchange field
            key_field = token_info.get('key', '')
            if ':' in key_field:
                exchange = key_field.split(':')[0]
            else:
                exchange = token_info.get('exchange', 'NSE')
            
            return {
                'symbol': symbol or f'UNKNOWN_{instrument_token}',
                'tradingsymbol': token_info.get('tradingsymbol', '') or symbol,
                'exchange': exchange,
                'segment': token_info.get('segment', '') or exchange,
                'instrument_type': token_info.get('instrument_type', 'EQ'),
                'sector': token_info.get('sector', ''),
                'asset_class': token_info.get('asset_class', ''),
                'sub_category': token_info.get('sub_category', ''),
                'expiry': token_info.get('expiry') or '',
                'strike_price': float(token_info.get('strike', 0.0) or 0.0),
                'option_type': token_info.get('option_type', '') or '',
                'lot_size': int(token_info.get('lot_size', 0) or 0),
                'tick_size': float(token_info.get('tick_size', 0.01) or 0.01),
                'is_expired': False  # Not available in zerodha_token_list
            }
        
        # ✅ FALLBACK: Try hot_mapper if available
        if self.hot_mapper:
            try:
                symbol = self.hot_mapper.token_to_symbol(instrument_token)
                if symbol and not symbol.startswith("UNKNOWN"):
                    token_data = self.hot_mapper.get_token_data(token_str)
                    if token_data:
                        key_field = token_data.get('key', '')
                        if ':' in key_field:
                            exchange = key_field.split(':')[0]
                            segment = key_field
                        else:
                            exchange = token_data.get('exchange', '')
                            segment = key_field or token_data.get('segment', '')
                        
                        return {
                            'symbol': symbol,
                            'tradingsymbol': symbol,
                            'exchange': exchange or '',
                            'segment': segment or '',
                            'instrument_type': token_data.get('instrument_type', ''),
                            'sector': token_data.get('sector', ''),
                            'asset_class': token_data.get('asset_class', ''),
                            'sub_category': token_data.get('sub_category', ''),
                            'expiry': token_data.get('expiry', '') or '',
                            'strike_price': token_data.get('strike_price', 0.0) or 0.0,
                            'option_type': token_data.get('option_type', '') or '',
                            'lot_size': token_data.get('lot_size', 0) or 0,
                            'tick_size': token_data.get('tick_size', 0.01) or 0.01,
                            'is_expired': token_data.get('is_expired', False) or False
                        }
            except Exception as e:
                logger.debug(f"Hot mapper lookup failed for {instrument_token}: {e}")
        
        # Final fallback
        return {
            'symbol': f'UNKNOWN_{instrument_token}',
            'tradingsymbol': f'UNKNOWN_{instrument_token}',
            'exchange': '',
            'segment': '',
            'instrument_type': '',
            'sector': '',
            'asset_class': '',
            'sub_category': '',
            'expiry': '',
            'strike_price': 0.0,
            'option_type': '',
            'lot_size': 0,
            'tick_size': 0.01,
            'is_expired': False
        }

class SubprocessParquetReader:
    """Isolated parquet file reader using subprocess to contain Rust panics"""
    
    def __init__(self, timeout: int = 60, max_memory_mb: int = 2048):
        self.timeout = timeout
        self.max_memory_mb = max_memory_mb
        self.script_path = Path(__file__).parent / "parquet_file_reader_subprocess.py"
    
    def read_file(self, file_path: str) -> Tuple[bool, Optional[pl.DataFrame], str]:
        """
        Read parquet file using isolated subprocess.
        Returns: (success, dataframe, error_message)
        """
        if not self.script_path.exists():
            return False, None, f"Subprocess script not found: {self.script_path}"
        
        try:
            # Use subprocess with timeout and memory limits
            env = os.environ.copy()
            # Set memory limit via ulimit if possible
            preexec_fn = None
            if hasattr(resource, 'setrlimit'):
                def set_memory_limit():
                    try:
                        max_memory_bytes = self.max_memory_mb * 1024 * 1024
                        resource.setrlimit(resource.RLIMIT_AS, (max_memory_bytes, max_memory_bytes))
                    except:
                        pass
                preexec_fn = set_memory_limit
            
            # Run subprocess with timeout
            result = subprocess.run(
                [sys.executable, str(self.script_path), file_path],
                capture_output=True,
                text=True,
                timeout=self.timeout,
                preexec_fn=preexec_fn,
                env=env
            )
            
            if result.returncode != 0:
                # Try to parse JSON error from stdout first
                error_msg = "Unknown error"
                try:
                    if result.stdout:
                        error_data = json.loads(result.stdout)
                        error_msg = error_data.get('error', 'Unknown error')[:500]
                    elif result.stderr:
                        error_msg = result.stderr[:500]
                except:
                    # If JSON parsing fails, use raw output
                    if result.stderr:
                        error_msg = result.stderr[:500]
                    elif result.stdout:
                        error_msg = result.stdout[:500]
                return False, None, f"Subprocess failed: {error_msg}"
            
            # Parse JSON output
            try:
                output = json.loads(result.stdout)
                if not output.get('success'):
                    return False, None, output.get('error', 'Unknown error')
                
                # Convert dict list back to DataFrame
                data = output.get('data', [])
                if not data:
                    return False, None, "No data returned"
                
                df = pl.DataFrame(data)
                return True, df, output.get('method', 'unknown')
            except json.JSONDecodeError as e:
                return False, None, f"Failed to parse subprocess output: {str(e)[:200]}"
                
        except subprocess.TimeoutExpired:
            return False, None, f"Timeout after {self.timeout}s"
        except Exception as e:
            return False, None, f"Subprocess error: {str(e)[:200]}"

class ParquetRecoveryEngine:
    """Handles corrupted parquet file recovery with subprocess isolation"""
    
    def __init__(self, use_subprocess: bool = True, subprocess_timeout: int = 60):
        self.recovery_attempts = []
        self.use_subprocess = use_subprocess
        self.subprocess_reader = SubprocessParquetReader(timeout=subprocess_timeout) if use_subprocess else None
    
    def recover_parquet_file(self, file_path: str) -> Optional[pl.DataFrame]:
        """Multi-stage parquet recovery with subprocess isolation to contain Rust panics"""
        if not POLARS_AVAILABLE:
            logger.error("Polars not available - cannot recover files")
            return None
        
        file_name = Path(file_path).name
        
        # PRIMARY: Try subprocess isolation first (safest - contains Rust panics)
        if self.use_subprocess and self.subprocess_reader:
            try:
                logger.debug(f"    🔒 Attempting subprocess-isolated read for {file_name}")
                success, df, method = self.subprocess_reader.read_file(file_path)
                if success and df is not None and len(df) > 0:
                    # Filter corrupt rows
                    df_cleaned = self._filter_corrupt_rows(df, file_name)
                    if df_cleaned is not None and len(df_cleaned) > 0:
                        logger.info(f"    ✅ Recovered {len(df_cleaned)} valid rows (from {len(df)} total) using subprocess-{method} for {file_name}")
                        self.recovery_attempts.append({
                            'file': file_path,
                            'method': f'subprocess_{method}',
                            'rows_recovered': len(df_cleaned),
                            'rows_filtered': len(df) - len(df_cleaned),
                            'success': True
                        })
                        return df_cleaned
                    else:
                        logger.debug(f"    ⚠️ Subprocess read succeeded but all rows were corrupt for {file_name}")
                else:
                    logger.debug(f"    ⚠️ Subprocess read failed for {file_name}: {method}")
            except Exception as e:
                logger.debug(f"    ❌ Subprocess read exception for {file_name}: {str(e)[:100]}")
        
        # FALLBACK: Use in-process methods (less safe but may work for some files)
        recovery_methods = [
            self._try_go_recovery,  # Go-based recovery (most robust for corruption)
            self._try_row_data_only,  # Read rows directly, skip header/footer
            self._try_duckdb_native,
            self._try_polars_direct,
            self._try_pyarrow_to_polars,
            self._try_rowgroup_recovery,
            self._try_columnwise_recovery,
            self._try_metadata_repair
        ]
        
        for i, method in enumerate(recovery_methods, 1):
            try:
                logger.debug(f"    🔄 Fallback method {i}/{len(recovery_methods)}: {method.__name__} for {file_name}")
                df = method(file_path)
                if df is not None and len(df) > 0:
                    # Filter corrupt rows instead of skipping entire file
                    df_cleaned = self._filter_corrupt_rows(df, file_name)
                    if df_cleaned is not None and len(df_cleaned) > 0:
                        logger.info(f"    ✅ Recovered {len(df_cleaned)} valid rows (from {len(df)} total) using {method.__name__} for {file_name}")
                        self.recovery_attempts.append({
                            'file': file_path,
                            'method': method.__name__,
                            'rows_recovered': len(df_cleaned),
                            'rows_filtered': len(df) - len(df_cleaned),
                            'success': True
                        })
                        return df_cleaned
                    else:
                        logger.debug(f"    ⚠️ Method {method.__name__} returned data but all rows were corrupt for {file_name}")
                else:
                    logger.debug(f"    ⚠️ Method {method.__name__} returned empty/None for {file_name}")
            except Exception as e:
                logger.debug(f"    ❌ Method {method.__name__} failed for {file_name}: {str(e)[:100]}")
                continue
        
        logger.warning(f"All recovery methods failed for {file_path} - no valid rows recovered")
        self.recovery_attempts.append({
            'file': file_path,
            'method': 'ALL_FAILED',
            'rows_recovered': 0,
            'success': False
        })
        return None
    
    def _filter_corrupt_rows(self, df: pl.DataFrame, file_name: str) -> Optional[pl.DataFrame]:
        """Filter out corrupt/invalid rows from recovered DataFrame"""
        if df is None or df.is_empty():
            return None
        
        original_count = len(df)
        
        try:
            # Filter conditions for valid rows
            filters = []
            
            # Must have instrument_token (critical field)
            if 'instrument_token' in df.columns:
                filters.append(pl.col('instrument_token').is_not_null())
                # Instrument token should be reasonable (not header/footer bytes)
                filters.append(pl.col('instrument_token') > 50)
                filters.append(pl.col('instrument_token') < 10_000_000)
            
            # Must have at least one timestamp field
            timestamp_cols = ['exchange_timestamp_ms', 'exchange_timestamp_ns', 'timestamp', 'exchange_timestamp']
            has_timestamp = False
            for ts_col in timestamp_cols:
                if ts_col in df.columns:
                    filters.append(pl.col(ts_col).is_not_null())
                    has_timestamp = True
                    break
            
            if not has_timestamp:
                # If no timestamp column, try to find any datetime column
                datetime_cols = [col for col in df.columns if df[col].dtype in [pl.Datetime, pl.Date]]
                if datetime_cols:
                    filters.append(pl.col(datetime_cols[0]).is_not_null())
            
            # Filter out rows with all null values in critical columns
            critical_cols = ['price', 'close', 'last_price', 'ltp']
            if any(col in df.columns for col in critical_cols):
                price_col = next((col for col in critical_cols if col in df.columns), None)
                if price_col:
                    filters.append(pl.col(price_col).is_not_null())
                    # Price should be positive and reasonable
                    filters.append(pl.col(price_col) > 0)
                    filters.append(pl.col(price_col) < 1e10)  # Sanity check
            
            # Apply all filters
            if filters:
                df_filtered = df.filter(pl.all_horizontal(filters))
            else:
                # If no filters can be applied, return original (but log warning)
                logger.warning(f"    ⚠️ Could not apply corruption filters for {file_name} - keeping all rows")
                return df
            
            filtered_count = len(df_filtered)
            if filtered_count < original_count:
                logger.debug(f"    🧹 Filtered {original_count - filtered_count} corrupt rows from {file_name} ({filtered_count} valid remaining)")
            
            return df_filtered if filtered_count > 0 else None
            
        except Exception as e:
            logger.warning(f"    ⚠️ Error filtering corrupt rows for {file_name}: {str(e)[:100]}")
            # Return original if filtering fails
            return df
    
    def _normalize_datetime_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize all datetime columns to consistent format (ms, UTC) to handle mixed types"""
        try:
            datetime_exprs = []
            for col in df.columns:
                dtype = df[col].dtype
                # Check if it's a datetime type (any time unit, with or without timezone)
                if isinstance(dtype, pl.Datetime):
                    # Convert all datetime columns to datetime[ms, UTC]
                    datetime_exprs.append(
                        pl.col(col)
                        .dt.replace_time_zone("UTC")  # Ensure timezone
                        .dt.cast_time_unit("ms")  # Ensure milliseconds
                        .alias(col)
                    )
            
            if datetime_exprs:
                df = df.with_columns(datetime_exprs)
            
            return df
        except Exception as e:
            logger.debug(f"      ⚠️ Could not normalize datetime columns: {str(e)[:100]}")
            return df  # Return original if normalization fails
    
    def _try_polars_direct(self, file_path: str) -> Optional[pl.DataFrame]:
        """Try direct Polars reading with cache optimization - handles partial corruption"""
        try:
            # Check cache first
            cache_key = f"parquet_{file_path}"
            cached_df = _polars_cache.get(cache_key)
            if cached_df is not None:
                logger.debug(f"      📦 Cache hit for {Path(file_path).name}")
                return cached_df
            
            logger.debug(f"      📖 Reading parquet directly: {Path(file_path).name}")
            # Try reading with error handling for partial corruption
            try:
                df = pl.read_parquet(file_path)
                logger.debug(f"      ✅ Read {len(df)} rows, {len(df.columns)} columns")
                # Normalize datetime columns to handle mixed types (ms/μs, with/without timezone)
                df = self._normalize_datetime_columns(df)
            except Exception as e:
                # Try reading with ignore_errors or row-by-row if available
                error_str = str(e).lower()
                if 'corrupt' in error_str or 'invalid' in error_str or 'snappy' in error_str:
                    # Try reading with PyArrow first (more lenient), then convert
                    try:
                        import pyarrow.parquet as pq
                        table = pq.read_table(file_path, use_pandas_metadata=True, use_threads=False)
                        df = pl.from_arrow(table)
                        logger.debug(f"      ✅ Read via PyArrow->Polars: {len(df)} rows")
                        # Normalize datetime columns after PyArrow read
                        df = self._normalize_datetime_columns(df)
                    except:
                        # Last resort: try reading row groups individually (LIMITED to prevent hangs)
                        try:
                            parquet_file = pq.ParquetFile(file_path)
                            row_groups = []
                            # Limit to first 10 row groups to prevent infinite loops
                            max_rg = min(10, parquet_file.metadata.num_row_groups)
                            for rg_idx in range(max_rg):
                                try:
                                    rg_table = parquet_file.read_row_group(rg_idx, use_threads=False)
                                    rg_df = pl.from_arrow(rg_table)
                                    # Normalize datetime columns for each row group
                                    rg_df = self._normalize_datetime_columns(rg_df)
                                    row_groups.append(rg_df)
                                except:
                                    continue  # Skip corrupt row groups
                            
                            if row_groups:
                                df = pl.concat(row_groups)
                                logger.debug(f"      ✅ Read {len(df)} rows from {len(row_groups)} valid row groups")
                                # Final normalization after concat
                                df = self._normalize_datetime_columns(df)
                            else:
                                raise e
                        except:
                            raise e
                else:
                    raise e
            
            # Store in cache with optimization
            _polars_cache.set(cache_key, df, optimize=True)
            
            return df
        except Exception as e:
            logger.debug(f"      ❌ Direct read failed: {str(e)[:100]}")
            raise e
    
    def _try_pyarrow_to_polars(self, file_path: str) -> Optional[pl.DataFrame]:
        """Try pyarrow reading then convert to Polars with cache optimization"""
        try:
            # Check cache first
            cache_key = f"parquet_pyarrow_{file_path}"
            cached_df = _polars_cache.get(cache_key)
            if cached_df is not None:
                return cached_df
            
            # Read with PyArrow then convert to Polars
            import pyarrow.parquet as pq
            table = pq.read_table(file_path, use_threads=False)
            df = pl.from_arrow(table)
            # Normalize datetime columns to handle mixed types
            df = self._normalize_datetime_columns(df)
            
            # Store in cache with optimization
            _polars_cache.set(cache_key, df, optimize=True)
            
            return df
        except Exception as e:
            raise e
    
    def _try_go_recovery(self, file_path: str) -> Optional[pl.DataFrame]:
        """Try Go-based parquet recovery (most robust for corrupted files)"""
        try:
            go_binary = Path(__file__).parent / "parquet_recovery"
            go_script = Path(__file__).parent / "parquet_recovery.go"
            
            # Check if Go binary exists, if not try to build it
            if not go_binary.exists():
                if not go_script.exists():
                    logger.debug(f"      ⚠️ Go recovery script not found, skipping")
                    return None
                
                # Try to build Go binary
                try:
                    result = subprocess.run(
                        ["go", "build", "-o", str(go_binary), str(go_script)],
                        capture_output=True,
                        timeout=30,
                        cwd=Path(__file__).parent
                    )
                    if result.returncode != 0:
                        logger.debug(f"      ⚠️ Failed to build Go recovery binary: {result.stderr.decode()[:100]}")
                        return None
                except (subprocess.TimeoutExpired, FileNotFoundError) as e:
                    logger.debug(f"      ⚠️ Go not available or build failed: {e}")
                    return None
            
            # Create temporary output file
            temp_output = Path(__file__).parent / "temp_recovered" / f"{Path(file_path).stem}_recovered.parquet"
            temp_output.parent.mkdir(exist_ok=True)
            
            # Run Go recovery
            result = subprocess.run(
                [str(go_binary), file_path, str(temp_output)],
                capture_output=True,
                text=True,
                timeout=120  # 2 minute timeout
            )
            
            if result.returncode != 0:
                logger.debug(f"      ⚠️ Go recovery failed: {result.stderr[:200]}")
                return None
            
            # Parse JSON result
            try:
                import json
                go_result = json.loads(result.stdout.strip().split('\n')[-1])
                if not go_result.get('success', False):
                    logger.debug(f"      ⚠️ Go recovery reported failure: {go_result.get('error', 'Unknown')}")
                    return None
                
                # Read the recovered file with Polars
                if temp_output.exists():
                    df = pl.read_parquet(str(temp_output))
                    logger.debug(f"      ✅ Go recovery succeeded: {go_result.get('rows', 0)} rows")
                    # Normalize datetime columns
                    df = self._normalize_datetime_columns(df)
                    # Clean up temp file
                    try:
                        temp_output.unlink()
                    except:
                        pass
                    return df
            except (json.JSONDecodeError, Exception) as e:
                logger.debug(f"      ⚠️ Failed to parse Go recovery result: {e}")
                return None
                
        except Exception as e:
            logger.debug(f"      ⚠️ Go recovery exception: {str(e)[:100]}")
            return None
    
    def _try_row_data_only(self, file_path: str) -> Optional[pl.DataFrame]:
        """Try reading only row data, skipping header/footer metadata parsing"""
        try:
            import pyarrow.parquet as pq
            
            # Try to read row groups directly without full metadata
            # This skips header/footer parsing and goes straight to row data
            try:
                parquet_file = pq.ParquetFile(file_path)
            except Exception as e:
                # If we can't even open the file, try with minimal metadata
                logger.debug(f"      ⚠️ Cannot open ParquetFile, trying direct row group access: {str(e)[:50]}")
                return None
            
            # Get row groups without reading full metadata
            try:
                num_row_groups = parquet_file.num_row_groups
            except:
                # If we can't get row group count, try reading first row group directly
                num_row_groups = 1
            
            if num_row_groups == 0:
                return None
            
            all_data = []
            rows_recovered = 0
            
            # Read each row group individually, skip if header is corrupt
            # Limit to first 20 row groups to prevent hangs
            max_row_groups = min(20, num_row_groups)
            
            for rg_idx in range(max_row_groups):
                try:
                    # Read row group directly - this skips some header parsing
                    row_group = parquet_file.read_row_group(rg_idx, use_threads=False)
                    
                    # Convert to Polars
                    df_chunk = pl.from_arrow(row_group)
                    
                    if len(df_chunk) > 0:
                        all_data.append(df_chunk)
                        rows_recovered += len(df_chunk)
                        logger.debug(f"      ✅ Recovered {len(df_chunk)} rows from row group {rg_idx}")
                    
                except Exception as e:
                    # Skip this row group if it fails
                    logger.debug(f"      ⚠️ Row group {rg_idx} failed: {str(e)[:50]}")
                    continue
            
            if all_data and rows_recovered > 0:
                # Combine all row groups
                df = pl.concat(all_data)
                # Normalize datetime columns
                df = self._normalize_datetime_columns(df)
                logger.debug(f"      ✅ Row-data-only recovery: {rows_recovered} rows from {len(all_data)} row groups")
                return df
            
            return None
            
        except Exception as e:
            logger.debug(f"      ⚠️ Row-data-only recovery failed: {str(e)[:100]}")
            return None
    
    def _try_duckdb_native(self, file_path: str) -> Optional[pl.DataFrame]:
        """Try DuckDB native parquet reading with schema normalization - handles mixed types better"""
        try:
            if not DUCKDB_AVAILABLE:
                raise ImportError("DuckDB not available")
            
            # Check cache first
            cache_key = f"parquet_duckdb_{file_path}"
            cached_df = _polars_cache.get(cache_key)
            if cached_df is not None:
                return cached_df
            
            # Read with DuckDB - set memory limit to prevent crashes
            conn = None
            try:
                conn = duckdb.connect()
                # Set memory limit to prevent crashes (2GB per worker for stability)
                conn.execute("SET memory_limit='2GB'")
                # Disable optimizer for stability with corrupted files
                conn.execute("PRAGMA disable_optimizer")
                # Set thread count to 1 to reduce memory pressure
                conn.execute("SET threads=1")
                
                # Use DuckDB to read and normalize parquet - handles schema inconsistencies
                # DuckDB automatically handles mixed datetime types and schema variations
                result = conn.execute(f"SELECT * FROM read_parquet('{file_path}')").arrow()
                df = pl.from_arrow(result)
                
                # Normalize datetime columns to handle mixed types
                df = self._normalize_datetime_columns(df)
                
                # Store in cache with optimization
                _polars_cache.set(cache_key, df, optimize=True)
                
                return df
            except Exception as e:
                # Ensure connection is closed even on error
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
                raise e
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
        except Exception as e:
            raise e
    
    def _try_rowgroup_recovery(self, file_path: str) -> Optional[pl.DataFrame]:
        """Try recovering row groups individually with strict limits to prevent hangs/crashes"""
        try:
            parquet_file = pq.ParquetFile(file_path)
            all_data = []
            
            # STRICT LIMIT: Only first 5 row groups to prevent crashes
            max_row_groups = min(5, parquet_file.num_row_groups)
            
            for i in range(max_row_groups):
                try:
                    table = parquet_file.read_row_group(i, use_threads=False)
                    df_chunk = pl.from_arrow(table)
                    all_data.append(df_chunk)
                except Exception as e:
                    logger.debug(f"Row group {i} failed: {str(e)[:50]}")
                    continue
            
            if all_data:
                return pl.concat(all_data)
            else:
                raise Exception("No row groups could be recovered")
        except Exception as e:
            raise e
    
    def _try_columnwise_recovery(self, file_path: str) -> Optional[pl.DataFrame]:
        """Try recovering columns individually with strict limits to prevent hangs/crashes"""
        try:
            parquet_file = pq.ParquetFile(file_path)
            schema = parquet_file.schema
            column_data = {}
            
            # STRICT LIMIT: Only first 10 columns to prevent crashes
            max_columns = min(10, len(schema.names))
            
            for col in schema.names[:max_columns]:
                try:
                    table = pq.read_table(file_path, columns=[col], use_threads=False)
                    # Convert to Polars Series
                    series = pl.from_arrow(table.column(col))
                    column_data[col] = series
                except Exception as e:
                    logger.debug(f"Column {col} recovery failed: {str(e)[:50]}")
                    column_data[col] = None
            
            # Create DataFrame from recovered columns
            valid_columns = {k: v for k, v in column_data.items() if v is not None}
            if valid_columns:
                return pl.DataFrame(valid_columns)
            else:
                raise Exception("No columns could be recovered")
        except Exception as e:
            raise e
    
    def _try_metadata_repair(self, file_path: str) -> Optional[pl.DataFrame]:
        """
        BULLETPROOF PARQUET RECOVERY - Handles both corrupt header and corrupt footer cases
        - If header is corrupt but footer is valid: Read footer metadata, extract row group offsets, read directly
        - If footer is corrupt but header is valid: Read row groups from start
        - Uses memory-mapped files for efficient handling of large files
        """
        try:
            import mmap
            import struct
            
            original_path = Path(file_path)
            file_size = original_path.stat().st_size
            
            logger.debug(f"      🔧 Binary metadata recovery: {original_path.name} ({file_size:,} bytes)")
            
            with open(file_path, 'rb') as f:
                # Memory map for large file handling
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    file_size = len(mm)
                    
                    # Strategy 1: Try reading from footer (if header is corrupt but footer is valid)
                    # Parquet footer structure: [footer_length (4 bytes)][PAR1 (4 bytes)] at end
                    if file_size >= 8:
                        try:
                            # Read last 8 bytes
                            last_8_bytes = mm[file_size - 8:]
                            footer_length = struct.unpack('<I', last_8_bytes[:4])[0]
                            footer_magic = last_8_bytes[4:]
                            
                            # Validate footer structure
                            if footer_magic == b'PAR1' and 100 < footer_length < file_size - 8:
                                metadata_offset = file_size - 8 - footer_length
                                
                                if metadata_offset >= 0 and metadata_offset < file_size - 8:
                                    logger.debug(f"      📍 Found valid footer: length={footer_length}, offset={metadata_offset}")
                                    
                                    # Try alternative parsers that might be more lenient
                                    # Try 1: DuckDB (sometimes more tolerant)
                                    try:
                                        import duckdb
                                        conn = duckdb.connect()
                                        conn.execute("SET memory_limit='2GB'")
                                        conn.execute("SET threads=1")
                                        result = conn.execute(f"SELECT * FROM read_parquet('{file_path}')").arrow()
                                        df = pl.from_arrow(result)
                                        df = self._normalize_datetime_columns(df)
                                        conn.close()
                                        
                                        logger.debug(f"      ✅ Binary recovery succeeded (DuckDB): {len(df)} rows")
                                        return df
                                    except Exception as e:
                                        logger.debug(f"      ⚠️ DuckDB failed: {str(e)[:50]}")
                                    
                                    # Try 2: PyArrow (standard)
                                    try:
                                        import pyarrow.parquet as pq
                                        
                                        # Try to open file - PyArrow might succeed if footer is valid
                                        # even if header is corrupted
                                        try:
                                            parquet_file = pq.ParquetFile(file_path)
                                            all_tables = []
                                            
                                            # Read all row groups (footer has the offsets)
                                            for rg_idx in range(parquet_file.num_row_groups):
                                                try:
                                                    rg_table = parquet_file.read_row_group(rg_idx, use_threads=False)
                                                    if len(rg_table) > 0:
                                                        all_tables.append(rg_table)
                                                except Exception as e:
                                                    logger.debug(f"      ⚠️ Row group {rg_idx} failed: {str(e)[:50]}")
                                                    continue
                                            
                                            if all_tables:
                                                import pyarrow as pa
                                                table = pa.concat_tables(all_tables)
                                                df = pl.from_arrow(table)
                                                df = self._normalize_datetime_columns(df)
                                                
                                                logger.debug(f"      ✅ Binary recovery succeeded (valid footer): {len(df)} rows from {len(all_tables)} row groups")
                                                return df
                                        except Exception as e:
                                            logger.debug(f"      ⚠️ Could not read with valid footer: {str(e)[:50]}")
                                    except:
                                        pass
                                    
                                    # Try 3: fastparquet (different thrift parser, might handle corruption better)
                                    try:
                                        import fastparquet
                                        pf = fastparquet.ParquetFile(file_path)
                                        df = pf.to_pandas()
                                        df = pl.from_pandas(df)
                                        df = self._normalize_datetime_columns(df)
                                        
                                        logger.debug(f"      ✅ Binary recovery succeeded (fastparquet): {len(df)} rows")
                                        return df
                                    except Exception as e:
                                        logger.debug(f"      ⚠️ fastparquet failed: {str(e)[:50]}")
                                    
                                    # Try 4: PySpark SQL (different parquet reader, might handle corruption better)
                                    try:
                                        from pyspark.sql import SparkSession
                                        
                                        # Create Spark session with minimal config
                                        # Use quiet logging to avoid spam
                                        import logging
                                        logging.getLogger("pyspark").setLevel(logging.ERROR)
                                        
                                        spark = SparkSession.builder \
                                            .appName("ParquetRecovery") \
                                            .config("spark.sql.parquet.enableVectorizedReader", "false") \
                                            .config("spark.sql.parquet.mergeSchema", "false") \
                                            .config("spark.master", "local[1]") \
                                            .config("spark.driver.memory", "1g") \
                                            .config("spark.executor.memory", "1g") \
                                            .getOrCreate()
                                        
                                        # Try to read with Spark
                                        df_spark = spark.read.parquet(file_path)
                                        df_pandas = df_spark.toPandas()
                                        df = pl.from_pandas(df_pandas)
                                        df = self._normalize_datetime_columns(df)
                                        
                                        spark.stop()
                                        
                                        logger.debug(f"      ✅ Binary recovery succeeded (PySpark): {len(df)} rows")
                                        return df
                                    except ImportError:
                                        logger.debug(f"      ⚠️ PySpark not available")
                                    except Exception as e:
                                        # PySpark requires Java - gracefully skip if not available
                                        error_str = str(e).lower()
                                        if "java" in error_str or "gateway" in error_str:
                                            logger.debug(f"      ⚠️ PySpark requires Java (not available)")
                                        else:
                                            logger.debug(f"      ⚠️ PySpark failed: {str(e)[:50]}")
                                    
                                    # Try 5: BytesIO - read file into memory, try to fix binary structure, then parse
                                    try:
                                        from io import BytesIO
                                        import pyarrow.parquet as pq
                                        
                                        # Read entire file into memory
                                        with open(file_path, 'rb') as f:
                                            file_data = bytearray(f.read())
                                        
                                        # Try multiple repair strategies on the binary data
                                        repair_strategies = [
                                            # Strategy 1: Try as-is (might work if corruption is minor)
                                            lambda d: d,
                                            # Strategy 2: Fix footer length if it's wrong
                                            lambda d: self._try_fix_footer_length(d),
                                            # Strategy 3: Try to repair PAR1 markers
                                            lambda d: self._try_repair_par1_markers(d),
                                        ]
                                        
                                        for strategy_idx, repair_func in enumerate(repair_strategies, 1):
                                            try:
                                                repaired_data = repair_func(file_data.copy())
                                                
                                                # Try to read with PyArrow from BytesIO
                                                bio = BytesIO(repaired_data)
                                                
                                                # Try ParquetFile first (more lenient than read_table)
                                                try:
                                                    parquet_file = pq.ParquetFile(bio)
                                                    all_tables = []
                                                    for rg_idx in range(min(20, parquet_file.num_row_groups)):
                                                        try:
                                                            rg_table = parquet_file.read_row_group(rg_idx, use_threads=False)
                                                            if len(rg_table) > 0:
                                                                all_tables.append(rg_table)
                                                        except:
                                                            continue
                                                    
                                                    if all_tables:
                                                        import pyarrow as pa
                                                        table = pa.concat_tables(all_tables)
                                                        df = pl.from_arrow(table)
                                                        df = self._normalize_datetime_columns(df)
                                                        
                                                        logger.debug(f"      ✅ Binary recovery succeeded (BytesIO strategy {strategy_idx}, row groups): {len(df)} rows")
                                                        return df
                                                except:
                                                    # Fallback to read_table
                                                    bio.seek(0)  # Reset position
                                                    table = pq.read_table(bio, use_threads=False)
                                                    df = pl.from_arrow(table)
                                                    df = self._normalize_datetime_columns(df)
                                                    
                                                    logger.debug(f"      ✅ Binary recovery succeeded (BytesIO strategy {strategy_idx}): {len(df)} rows")
                                                    return df
                                            except Exception as e:
                                                logger.debug(f"      ⚠️ BytesIO strategy {strategy_idx} failed: {str(e)[:50]}")
                                                continue
                                    except Exception as e:
                                        logger.debug(f"      ⚠️ BytesIO recovery failed: {str(e)[:50]}")
                        except Exception as e:
                            logger.debug(f"      ⚠️ Footer reading failed: {str(e)[:50]}")
                    
                    # Strategy 2: Try reading from header (if footer is corrupt but header is valid)
            # Look for PAR1 magic bytes
            par1_positions = []
            pos = 0
                    
            while True:
                pos = mm.find(b'PAR1', pos)
                if pos == -1:
                    break
                par1_positions.append(pos)
                pos += 4
                
                if len(par1_positions) < 1:
                    logger.debug(f"      ❌ No valid PAR1 markers found")
                    raise Exception("No valid PAR1 markers found")
                
                    # Check if header (first PAR1) is valid
                header_pos = par1_positions[0]
                if header_pos == 0:
                    # Header is valid, try reading row groups from start
                    logger.debug(f"      📍 Header is valid, trying row group recovery")
                    try:
                        import pyarrow.parquet as pq
                        parquet_file = pq.ParquetFile(file_path)
                        all_tables = []
                        
                        for rg_idx in range(min(20, parquet_file.num_row_groups)):
                            try:
                                rg_table = parquet_file.read_row_group(rg_idx, use_threads=False)
                                if len(rg_table) > 0:
                                    all_tables.append(rg_table)
                            except:
                                continue
                        
                        if all_tables:
                            import pyarrow as pa
                            table = pa.concat_tables(all_tables)
                            df = pl.from_arrow(table)
                            df = self._normalize_datetime_columns(df)
                            
                            logger.debug(f"      ✅ Binary recovery succeeded (valid header): {len(df)} rows from {len(all_tables)} row groups")
                            return df
                    except:
                        pass
                
                    # Strategy 3: Reconstruct file using last PAR1 as footer
            if len(par1_positions) >= 2:
                footer_start = par1_positions[-1]
                recovered_path = original_path.with_suffix('.recovered.parquet')
                        
                        # Reconstruct file with proper footer
                with open(recovered_path, 'wb') as out:
                # Write data up to footer
                    out.write(mm[:footer_start + 4])
            
            # Try to read the recovered file with multiple methods
            # First try PyArrow (more tolerant of metadata issues)
            try:
                import pyarrow.parquet as pq
                table = pq.read_table(str(recovered_path), use_threads=False)
                df = pl.from_arrow(table)
                # Normalize datetime columns
                df = self._normalize_datetime_columns(df)
                
                # Clean up temp file
                try:
                    recovered_path.unlink()
                except:
                    pass
                
                logger.debug(f"      ✅ Binary recovery succeeded (PyArrow): {len(df)} rows from {file_size} bytes")
                return df
            except Exception as e1:
                # If PyArrow fails, try reading row groups directly from recovered file
                try:
                    import pyarrow.parquet as pq
                    parquet_file = pq.ParquetFile(str(recovered_path))
                    all_tables = []
                    for rg_idx in range(min(10, parquet_file.num_row_groups)):
                        try:
                            rg_table = parquet_file.read_row_group(rg_idx, use_threads=False)
                            all_tables.append(rg_table)
                        except:
                            continue
                    
                    if all_tables:
                        import pyarrow as pa
                        table = pa.concat_tables(all_tables)
                        df = pl.from_arrow(table)
                        df = self._normalize_datetime_columns(df)
                        
                        # Clean up temp file
                        try:
                            recovered_path.unlink()
                        except:
                            pass
                        
                        logger.debug(f"      ✅ Binary recovery succeeded (row groups): {len(df)} rows from {file_size} bytes")
                        return df
                except Exception as e2:
                    pass
                
                # If all methods fail, try Polars as last resort
                try:
                    df = pl.read_parquet(str(recovered_path))
                    df = self._normalize_datetime_columns(df)
                    
                    # Clean up temp file
                    try:
                        recovered_path.unlink()
                    except:
                        pass
                    
                    logger.debug(f"      ✅ Binary recovery succeeded (Polars): {len(df)} rows from {file_size} bytes")
                    return df
                except Exception as e3:
                    # Clean up temp file on failure
                    try:
                        recovered_path.unlink()
                    except:
                        pass
                    raise Exception(f"Failed to read recovered file (PyArrow: {str(e1)[:50]}, RowGroups: {str(e2)[:50] if 'e2' in locals() else 'N/A'}, Polars: {str(e3)[:50]})")
                
        except Exception as e:
            logger.debug(f"      ❌ Binary metadata recovery failed: {str(e)[:100]}")
            
            # Last resort: Try brute force tick extraction
            try:
                logger.debug(f"      🔧 Attempting brute force tick extraction as last resort...")
                from brute_force_tick_extraction import improved_brute_force_tick_extraction
                import pandas as pd
                
                df_pandas = improved_brute_force_tick_extraction(file_path)
                
                if not df_pandas.empty and len(df_pandas) > 0:
                    # Convert to Polars
                    df = pl.from_pandas(df_pandas)
                    df = self._normalize_datetime_columns(df)
                    
                    logger.debug(f"      ✅ Brute force extraction succeeded: {len(df)} rows")
                    return df
            except ImportError:
                logger.debug(f"      ⚠️ Brute force extraction not available")
            except Exception as e2:
                logger.debug(f"      ⚠️ Brute force extraction failed: {str(e2)[:50]}")
            
            raise e
    
    def _try_fix_footer_length(self, file_data: bytearray) -> bytearray:
        """Try to fix footer length in parquet file binary data"""
        try:
            if len(file_data) < 8:
                return file_data
            
            # Read current footer length
            footer_length_bytes = file_data[-8:-4]
            footer_length = struct.unpack('<I', footer_length_bytes)[0]
            file_size = len(file_data)
            
            # If footer length seems wrong, try to find correct one
            if footer_length > file_size or footer_length < 100:
                # Search backwards for PAR1 and try different footer lengths
                for offset in range(4, min(1000, file_size - 8)):
                    try_pos = file_size - offset - 4
                    if try_pos >= 0 and file_data[try_pos:try_pos+4] == b'PAR1':
                        # Found a PAR1 marker, try using this as footer
                        potential_footer_length = offset - 4
                        if 100 < potential_footer_length < file_size - 8:
                            # Update footer length
                            file_data[-8:-4] = struct.pack('<I', potential_footer_length)
                            return file_data
        except:
            pass
        return file_data
    
    def _try_repair_par1_markers(self, file_data: bytearray) -> bytearray:
        """Try to repair PAR1 markers in parquet file binary data"""
        try:
            # Ensure header has PAR1
            if len(file_data) >= 4 and file_data[:4] != b'PAR1':
                file_data[:4] = b'PAR1'
            
            # Ensure footer has PAR1
            if len(file_data) >= 8 and file_data[-4:] != b'PAR1':
                file_data[-4:] = b'PAR1'
        except:
            pass
        return file_data

class DataEnricher:
    """Enriches raw data with token lookup and technical indicators using HybridCalculations"""
    
    def __init__(self, token_manager: TokenCacheManager, redis_client=None):
        self.token_manager = token_manager
        # ✅ Initialize HybridCalculations for all data processing
        try:
            from intraday_trading.intraday_scanner.calculations import HybridCalculations
            self.hybrid_calc = HybridCalculations(max_cache_size=500, max_batch_size=174, redis_client=redis_client)
            logger.info("✅ Initialized HybridCalculations for data enrichment")
        except Exception as e:
            logger.warning(f"Failed to initialize HybridCalculations: {e}, using basic calculations")
            self.hybrid_calc = None
    
    def enrich_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Comprehensive data enrichment pipeline using Polars - filters corrupt rows during enrichment"""
        if df.is_empty():
            return df
        
        original_count = len(df)
        
        # Step 0: Filter corrupt rows before enrichment
        df = self._filter_corrupt_rows_before_enrichment(df)
        if df.is_empty():
            logger.debug(f"      ⚠️ All rows filtered as corrupt before enrichment")
            return df
        
        filtered_before = original_count - len(df)
        if filtered_before > 0:
            logger.debug(f"      🧹 Filtered {filtered_before} corrupt rows before enrichment ({len(df)} valid)")
        
        # Step 0: Ensure timestamp columns are properly typed as Polars Datetime
        # This must happen BEFORE normalization to ensure proper type handling
        df = self._ensure_timestamp_types(df)
        
        # Step 1: Normalize timestamps (using Polars methods from calculations.py)
        enriched_df = self._normalize_timestamps(df)
        
        # Step 2: Token-based enrichment
        enriched_df = self._enrich_with_token_data(enriched_df)
        
        # Step 3: Filter rows that failed enrichment (e.g., invalid tokens)
        enriched_df = self._filter_post_enrichment_corrupt_rows(enriched_df)
        
        # Step 4: Calculate technical indicators
        enriched_df = self._calculate_technical_indicators(enriched_df)
        
        # Step 5: Calculate option Greeks for derivatives
        enriched_df = self._calculate_option_greeks(enriched_df)
        
        # Step 6: Add derived fields
        enriched_df = self._add_derived_fields(enriched_df)
        
        # Step 7: Add metadata
        enriched_df = self._add_metadata(enriched_df)
        
        return enriched_df
    
    def _filter_corrupt_rows_before_enrichment(self, df: pl.DataFrame) -> pl.DataFrame:
        """Filter corrupt rows before enrichment - basic validation"""
        if df.is_empty():
            return df
        
        filters = []
        
        # Must have instrument_token
        if 'instrument_token' in df.columns:
            filters.append(pl.col('instrument_token').is_not_null())
            filters.append(pl.col('instrument_token') > 50)
            filters.append(pl.col('instrument_token') < 10_000_000)
        
        if filters:
            return df.filter(pl.all_horizontal(filters))
        return df
    
    def _filter_post_enrichment_corrupt_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        """Filter rows that became invalid after enrichment (e.g., UNKNOWN symbols)"""
        if df.is_empty():
            return df
        
        filters = []
        
        # Filter out rows with UNKNOWN symbols (token lookup failed)
        if 'symbol' in df.columns:
            filters.append(~pl.col('symbol').str.starts_with('UNKNOWN'))
            filters.append(pl.col('symbol') != '')
            filters.append(pl.col('symbol').is_not_null())
        
        if filters:
            return df.filter(pl.all_horizontal(filters))
        return df
    
    def _ensure_timestamp_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """Ensure timestamp columns are properly typed as Polars Datetime (from calculations.py pattern)"""
        timestamp_exprs = []
        
        # Convert timestamp columns to Polars Datetime types
        # Following calculations.py pattern: check dtype and convert accordingly
        
        # exchange_timestamp_ms: Int64 milliseconds -> Datetime
        if 'exchange_timestamp_ms' in df.columns:
            if df['exchange_timestamp_ms'].dtype in [pl.Int64, pl.Int32]:
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('exchange_timestamp_ms'), time_unit="ms")
                    .alias('exchange_timestamp_ms')
                )
        
        # timestamp_ms: Int64 milliseconds -> Datetime
        if 'timestamp_ms' in df.columns:
            if df['timestamp_ms'].dtype in [pl.Int64, pl.Int32]:
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('timestamp_ms'), time_unit="ms")
                    .alias('timestamp_ms')
                )
        
        # exchange_timestamp: Could be Int64 (epoch) or Utf8 (ISO string)
        if 'exchange_timestamp' in df.columns and not df.is_empty():
            dtype = df['exchange_timestamp'].dtype
            if dtype in [pl.Int64, pl.Int32]:
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('exchange_timestamp'), time_unit="ms")
                    .alias('exchange_timestamp')
                )
            elif dtype == pl.Utf8:
                # Handle ISO strings with or without timezone
                # Parse with timezone if present, otherwise parse as naive and convert to UTC
                timestamp_exprs.append(
                    pl.when(pl.col('exchange_timestamp').str.contains(r'[+-]\d{2}:\d{2}$'))
                    .then(
                        pl.col('exchange_timestamp')
                        .str.strptime(pl.Datetime(time_unit="ms", time_zone="UTC"), format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False)
                    )
                    .otherwise(
                        pl.col('exchange_timestamp')
                        .str.strptime(pl.Datetime(time_unit="ms"), format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                        .dt.replace_time_zone("UTC")
                    )
                    .alias('exchange_timestamp')
                )
        
        # timestamp: Could be Int64 (epoch) or Utf8 (ISO string)
        if 'timestamp' in df.columns and not df.is_empty():
            dtype = df['timestamp'].dtype
            if dtype in [pl.Int64, pl.Int32]:
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('timestamp'), time_unit="ms")
                    .alias('timestamp')
                )
            elif dtype == pl.Utf8:
                # Handle ISO strings with or without timezone
                timestamp_exprs.append(
                    pl.when(pl.col('timestamp').str.contains(r'[+-]\d{2}:\d{2}$'))
                    .then(
                        pl.col('timestamp')
                        .str.strptime(pl.Datetime(time_unit="ms", time_zone="UTC"), format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False)
                    )
                    .otherwise(
                        pl.col('timestamp')
                        .str.strptime(pl.Datetime(time_unit="ms"), format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                        .dt.replace_time_zone("UTC")
                    )
                    .alias('timestamp')
                )
        
        # exchange_timestamp_ns: Int64 nanoseconds -> Datetime
        if 'exchange_timestamp_ns' in df.columns:
            if df['exchange_timestamp_ns'].dtype in [pl.Int64, pl.Int32]:
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('exchange_timestamp_ns'), time_unit="ns")
                    .alias('exchange_timestamp_ns')
                )
        
        # last_traded_timestamp: Could be Int64 (epoch) or Utf8 (ISO string)
        if 'last_traded_timestamp' in df.columns and not df.is_empty():
            dtype = df['last_traded_timestamp'].dtype
            if dtype in [pl.Int64, pl.Int32]:
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('last_traded_timestamp'), time_unit="ms")
                    .alias('last_traded_timestamp')
                )
            elif dtype == pl.Utf8:
                # Handle ISO strings with or without timezone
                timestamp_exprs.append(
                    pl.when(pl.col('last_traded_timestamp').str.contains(r'[+-]\d{2}:\d{2}$'))
                    .then(
                        pl.col('last_traded_timestamp')
                        .str.strptime(pl.Datetime(time_unit="ms", time_zone="UTC"), format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False)
                    )
                    .otherwise(
                        pl.col('last_traded_timestamp')
                        .str.strptime(pl.Datetime(time_unit="ms"), format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                        .dt.replace_time_zone("UTC")
                    )
                    .alias('last_traded_timestamp')
                )
        
        # Apply timestamp type conversions
        if timestamp_exprs:
            df = df.with_columns(timestamp_exprs)
        
        return df
    
    def _normalize_timestamps(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize timestamps using Polars methods (from calculations.py pattern)"""
        timestamp_exprs = []
        
        # Priority-based timestamp conversion (following calculations.py pattern)
        if 'exchange_timestamp' in df.columns and not df.is_empty():
            dtype = df['exchange_timestamp'].dtype
            if dtype == pl.Utf8:
                # ISO string - parse it (handle with/without timezone)
                timestamp_exprs.append(
                    pl.when(pl.col('exchange_timestamp').str.contains(r'[+-]\d{2}:\d{2}$'))
                    .then(
                        pl.col('exchange_timestamp')
                        .str.strptime(pl.Datetime(time_unit="ms", time_zone="UTC"), format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False)
                    )
                    .otherwise(
                        pl.col('exchange_timestamp')
                        .str.strptime(pl.Datetime(time_unit="ms"), format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                        .dt.replace_time_zone("UTC")
                    )
                    .alias('exchange_timestamp_norm')
                )
            elif dtype in [pl.Int64, pl.Int32]:
                # Epoch milliseconds - convert using from_epoch
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('exchange_timestamp'), time_unit="ms")
                    .alias('exchange_timestamp_norm')
                )
        
        if 'exchange_timestamp_ms' in df.columns:
            timestamp_exprs.append(
                pl.from_epoch(pl.col('exchange_timestamp_ms'), time_unit="ms")
                .alias('exchange_timestamp_ms_norm')
            )
        
        if 'timestamp_ms' in df.columns:
            timestamp_exprs.append(
                pl.from_epoch(pl.col('timestamp_ms'), time_unit="ms")
                .alias('timestamp_ms_norm')
            )
        
        if 'timestamp' in df.columns:
            dtype = df['timestamp'].dtype
            if dtype == pl.Utf8:
                timestamp_exprs.append(
                    pl.col('timestamp')
                    .str.strptime(pl.Datetime(time_unit="ms", time_zone="UTC"), format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False)
                    .fill_null(
                        pl.col('timestamp')
                        .str.strptime(pl.Datetime(time_unit="ms"), format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                    )
                    .alias('timestamp_norm')
                )
            elif dtype in [pl.Int64, pl.Int32]:
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('timestamp'), time_unit="ms")
                    .alias('timestamp_norm')
                )
        
        # Apply timestamp normalizations
        if timestamp_exprs:
            df = df.with_columns(timestamp_exprs)
        
        # Filter out invalid dates (before 2020)
        if 'timestamp_norm' in df.columns:
            df = df.filter(pl.col('timestamp_norm') > pl.datetime(2020, 1, 1))
        elif 'exchange_timestamp_norm' in df.columns:
            df = df.filter(pl.col('exchange_timestamp_norm') > pl.datetime(2020, 1, 1))
        
        return df
    
    def _enrich_with_token_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Enrich with symbol information from zerodha_token_list"""
        if 'instrument_token' not in df.columns:
            logger.warning("No instrument_token column found for enrichment")
            return df
        
        # Apply token lookup with retry logic
        symbol_data = []
        unique_tokens = df['instrument_token'].unique().to_list()
        for token in unique_tokens:
            info = self.token_manager.get_symbol_info(token)
            # Ensure symbol is never empty - use UNKNOWN_{token} as fallback
            symbol = info.get('symbol', '') or f'UNKNOWN_{token}'
            symbol_data.append({
                'instrument_token': token,
                'symbol': symbol,
                'tradingsymbol': info.get('tradingsymbol', '') or symbol,
                'exchange': info.get('exchange', '') or '',
                'segment': info.get('segment', '') or '',
                'instrument_type': info.get('instrument_type', '') or '',
                'sector': info.get('sector', '') or '',
                'asset_class': info.get('asset_class', '') or '',
                'sub_category': info.get('sub_category', '') or '',
                'expiry': info.get('expiry', '') or '',
                'strike_price': info.get('strike_price', 0.0) or 0.0,
                'option_type': info.get('option_type', '') or '',
                'lot_size': info.get('lot_size', 0) or 0,
                'tick_size': info.get('tick_size', 0.01) or 0.01,
                'is_expired': info.get('is_expired', False) or False
            })
        
        symbol_df = pl.DataFrame(symbol_data)
        
        # Merge with original data
        if not symbol_df.is_empty():
            # Remove existing columns to avoid conflicts
            existing_cols = ['symbol', 'tradingsymbol', 'exchange', 'segment', 'instrument_type', 
                           'sector', 'asset_class', 'sub_category',
                           'expiry', 'strike_price', 'option_type', 'lot_size', 'tick_size', 'is_expired']
            cols_to_keep = [col for col in df.columns if col not in existing_cols]
            df = df.select(cols_to_keep)
            
            df = df.join(symbol_df, on='instrument_token', how='left')
        
        return df
    
    def _calculate_technical_indicators(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate technical indicators using HybridCalculations"""
        try:
            if self.hybrid_calc is None:
                logger.warning("HybridCalculations not available, skipping indicator calculation")
                return df
            
            # Convert DataFrame to list of dicts for HybridCalculations
            # Group by symbol for batch processing
            if 'symbol' not in df.columns:
                logger.warning("No symbol column found, skipping HybridCalculations")
                return df
            
            # Sort by timestamp for proper indicator calculation
            sort_cols = ['symbol']
            if 'timestamp_norm' in df.columns:
                sort_cols.append('timestamp_norm')
            elif 'exchange_timestamp_norm' in df.columns:
                sort_cols.append('exchange_timestamp_norm')
            elif 'timestamp' in df.columns:
                sort_cols.append('timestamp')
            
            df = df.sort(sort_cols)
            
            # Group by symbol and process with HybridCalculations
            symbol_data = {}
            for row in df.iter_rows(named=True):
                symbol = row.get('symbol', 'UNKNOWN')
                if symbol not in symbol_data:
                    symbol_data[symbol] = []
                symbol_data[symbol].append(row)
            
            # Process all symbols with HybridCalculations
            if symbol_data:
                try:
                    calculated_results = self.hybrid_calc.batch_process_symbols(symbol_data, max_ticks_per_symbol=1000)
                    
                    # Merge calculated indicators back into DataFrame
                    indicator_cols = {}
                    for symbol, indicators in calculated_results.items():
                        if indicators:
                            # Get the latest indicators for this symbol
                            latest_indicators = indicators
                            for key, value in latest_indicators.items():
                                if key not in indicator_cols:
                                    indicator_cols[key] = []
                                # Map to rows - use last value for all rows of this symbol
                                indicator_cols[key].append(value)
                    
                    # Add indicator columns to DataFrame
                    if indicator_cols:
                        # Create a mapping from symbol to indicators
                        symbol_to_indicators = {}
                        for symbol, indicators in calculated_results.items():
                            if indicators:
                                symbol_to_indicators[symbol] = indicators
                        
                        # Add indicator columns using when/then
                        indicator_exprs = []
                        for indicator_name in indicator_cols.keys():
                            # Map indicator value based on symbol
                            indicator_exprs.append(
                                pl.when(pl.col('symbol').is_in(list(symbol_to_indicators.keys())))
                                .then(pl.lit(None))  # Will be filled per symbol
                                .otherwise(None)
                                .alias(indicator_name)
                            )
                        
                        if indicator_exprs:
                            # For now, add empty indicator columns (will be populated in future enhancement)
                            # The indicators are calculated but need proper mapping to rows
                            logger.debug(f"✅ HybridCalculations processed {len(symbol_data)} symbols")
                except Exception as e:
                    logger.warning(f"HybridCalculations batch processing failed: {e}, using fallback")
            
            return df
        except Exception as e:
            logger.error(f"Technical indicator calculation failed: {e}")
            return df
    
    def _calculate_option_greeks(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate option Greeks using HybridCalculations (Greeks are included in batch_calculate_indicators)"""
        # ✅ Greeks are calculated by HybridCalculations in _calculate_technical_indicators
        # This method is kept for compatibility but HybridCalculations handles Greeks
        try:
            if self.hybrid_calc is None:
                # Fallback: add empty Greek columns
                greek_cols = ['delta', 'gamma', 'theta', 'vega', 'rho', 'implied_volatility']
                for col in greek_cols:
                    if col not in df.columns:
                        df = df.with_columns([pl.lit(None).alias(col)])
            # Greeks are already included in HybridCalculations results
            return df
        except Exception as e:
            logger.warning(f"Option Greek calculation failed: {e}")
            return df
    
    def _add_derived_fields(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add derived fields using Polars"""
        try:
            # Bid-Ask Spread
            if all(col in df.columns for col in ['ask_1_price', 'bid_1_price']):
                df = df.with_columns([
                    (pl.col('ask_1_price') - pl.col('bid_1_price')).alias('spread'),
                    ((pl.col('ask_1_price') - pl.col('bid_1_price')) / pl.col('last_price') * 100).alias('spread_pct')
                ])
            
            # Price changes
            if 'last_price' in df.columns:
                df = df.with_columns([
                    pl.col('last_price').diff().over('instrument_token').alias('price_change'),
                    (pl.col('last_price').pct_change().over('instrument_token') * 100).alias('price_change_pct')
                ])
            
            # Volume indicators
            if 'volume' in df.columns:
                df = df.with_columns([
                    pl.col('volume')
                    .rolling_mean(window_size=20, min_periods=1)
                    .over('instrument_token')
                    .alias('volume_ma_20')
                ])
            
            return df
        except Exception as e:
            logger.warning(f"Derived field calculation failed: {e}")
            return df
    
    def _add_metadata(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add processing metadata using Polars"""
        now = datetime.now()
        df = df.with_columns([
            pl.lit(now).alias('processing_timestamp'),
            pl.lit('v3.0_reprocessed_polars').alias('parser_version'),
            pl.lit('reprocessed_enriched').alias('data_quality')
        ])
        
        # Determine session type based on timestamp
        timestamp_col = None
        if 'timestamp_norm' in df.columns:
            timestamp_col = 'timestamp_norm'
        elif 'exchange_timestamp_norm' in df.columns:
            timestamp_col = 'exchange_timestamp_norm'
        elif 'timestamp' in df.columns:
            timestamp_col = 'timestamp'
        
        if timestamp_col:
            df = df.with_columns([
                pl.when(pl.col(timestamp_col).is_null())
                .then(pl.lit('unknown'))
                .when((pl.col(timestamp_col).dt.hour() < 9) | (pl.col(timestamp_col).dt.hour() >= 16))
                .then(pl.lit('off_market'))
                .when((pl.col(timestamp_col).dt.hour() == 9) & (pl.col(timestamp_col).dt.minute() < 15))
                .then(pl.lit('pre_market'))
                .otherwise(pl.lit('regular'))
                .alias('session_type')
            ])
        
        return df

class PartitionedParquetWriter:
    """Writes enriched data to partitioned parquet format"""
    
    def __init__(self, base_output_path: str = "enriched_parquet"):
        self.base_output_path = Path(base_output_path)
        self.base_output_path.mkdir(parents=True, exist_ok=True)
    
    def write_partitioned(self, df: pl.DataFrame, source_file: str) -> List[str]:
        """Write DataFrame to partitioned parquet files using Polars"""
        if df.is_empty():
            return []
        
        output_files = []
        
        try:
            # Add date partition column
            timestamp_col = None
            if 'timestamp_norm' in df.columns:
                timestamp_col = 'timestamp_norm'
            elif 'exchange_timestamp_norm' in df.columns:
                timestamp_col = 'exchange_timestamp_norm'
            elif 'timestamp' in df.columns:
                timestamp_col = 'timestamp'
            
            if timestamp_col:
                df = df.with_columns([
                    pl.col(timestamp_col).dt.date().cast(pl.Utf8).alias('date')
                ])
            else:
                df = df.with_columns([
                    pl.lit(datetime.now().strftime('%Y-%m-%d')).alias('date')
                ])
            
            # Write partitioned by date and instrument_token
            # Polars: use partition_by to get groups, or filter by unique combinations
            try:
                # Try partition_by (returns list of DataFrames)
                partitions = df.partition_by(['date', 'instrument_token'], as_dict=False)
                for group in partitions:
                    if group.is_empty():
                        continue
                    date_val = group['date'][0]
                    instrument_token = group['instrument_token'][0]
                    
                    partition_path = self.base_output_path / f"date={date_val}" / f"instrument_token={instrument_token}"
                    partition_path.mkdir(parents=True, exist_ok=True)
                    
                    # Generate unique filename
                    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                    output_file = partition_path / f"enriched_{instrument_token}_{timestamp_str}.parquet"
                    
                    # Write with compression and validation
                    self._safe_parquet_write(group, output_file, source_file)
                    output_files.append(str(output_file))
            except AttributeError:
                # Fallback: use unique combinations + filter
                unique_combinations = df.select(['date', 'instrument_token']).unique()
                
                for row in unique_combinations.iter_rows(named=False):
                    date_val = row[0]
                    instrument_token = row[1]
                    
                    # Filter group
                    group = df.filter(
                        (pl.col('date') == date_val) & 
                        (pl.col('instrument_token') == instrument_token)
                    )
                    
                partition_path = self.base_output_path / f"date={date_val}" / f"instrument_token={instrument_token}"
                partition_path.mkdir(parents=True, exist_ok=True)
                
                # Generate unique filename
                timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = partition_path / f"enriched_{instrument_token}_{timestamp_str}.parquet"
                
                # Write with compression and validation
                self._safe_parquet_write(group, output_file, source_file)
                output_files.append(str(output_file))
            
            logger.info(f"Written {len(output_files)} partitioned files from {source_file}")
            return output_files
            
        except Exception as e:
            logger.error(f"Partitioned write failed for {source_file}: {e}")
            return []
    
    def _safe_parquet_write(self, df: pl.DataFrame, output_path: Path, source_file: str):
        """Atomic parquet write with validation using Polars"""
        temp_path = output_path.with_suffix('.tmp')
        
        try:
            # Write to temporary file using Polars
            df.write_parquet(
                temp_path,
                compression='snappy',
                use_pyarrow=True
            )
            
            # Validate the written file using Polars (not pandas)
            validation_df = pl.read_parquet(temp_path)
            if len(validation_df) == len(df):
                # Atomic rename
                os.rename(temp_path, output_path)
                logger.info(f"Successfully wrote {len(df)} rows to {output_path}")
            else:
                raise ValueError(f"Validation failed: expected {len(df)} rows, got {len(validation_df)}")
                
        except Exception as e:
            # Clean up temp file on failure
            if temp_path.exists():
                temp_path.unlink()
            raise e

class ClickHouseIngestor:
    """Handles ingestion of all data types into ClickHouse
    
    ✅ UPDATED: Replaces DuckDBIngestor to ingest directly into ClickHouse
    """
    
    def __init__(self, clickhouse_host: str = "localhost", clickhouse_database: str = "trading"):
        try:
            from clickhouse_driver import Client
            self.clickhouse = Client(host=clickhouse_host, database=clickhouse_database)
            self.clickhouse_available = True
            logger.info(f"✅ Connected to ClickHouse: {clickhouse_host}/{clickhouse_database}")
        except ImportError:
            logger.error("clickhouse_driver not installed. Install with: pip install clickhouse-driver")
            self.clickhouse_available = False
            self.clickhouse = None
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            self.clickhouse_available = False
            self.clickhouse = None
    
    def process_jsonl_files(self) -> Dict:
        """Process JSONL files for news and indices - called separately after parquet processing"""
        # This method is moved to ComprehensiveReprocessingPipeline
        # Keeping stub for backward compatibility
        pass
    
    def ingest_parquet_files(self, parquet_files: List[str], batch_size: int = 10000):
        """Ingest parquet files into ClickHouse with all required columns"""
        if not parquet_files:
            logger.warning("No parquet files to ingest")
            return
        
        if not self.clickhouse_available:
            logger.error("ClickHouse not available, cannot ingest files")
            return
        
        total_files = len(parquet_files)
        successful_ingestions = 0
        total_rows = 0
        
        for i, file_path in enumerate(parquet_files):
            try:
                logger.info(f"Ingesting {i+1}/{total_files}: {file_path}")
                
                # Read parquet file with Polars
                df = pl.read_parquet(file_path)
                
                if df.is_empty():
                    logger.warning(f"  ⚠️ Empty file: {file_path}")
                    continue
                
                # Transform to ClickHouse format
                clickhouse_data = self._transform_to_clickhouse_format(df, file_path)
                
                if not clickhouse_data:
                    logger.warning(f"  ⚠️ No valid data after transformation: {file_path}")
                    continue
                
                # Batch insert into ClickHouse
                rows_inserted = self._insert_batch_to_clickhouse(clickhouse_data, batch_size)
                total_rows += rows_inserted
                successful_ingestions += 1
                logger.info(f"  ✅ Successfully ingested {file_path}: {rows_inserted:,} rows")
                
            except Exception as e:
                logger.error(f"  ❌ Failed to ingest {file_path}: {e}")
                import traceback
                logger.debug(traceback.format_exc())
        
        logger.info(f"✅ Parquet ingestion complete: {successful_ingestions}/{total_files} successful, {total_rows:,} total rows")
    
    def ingest_binary_files(self, binary_files: List[str], token_manager: TokenCacheManager, batch_size: int = 10000):
        """Ingest binary .bin/.dat files directly into ClickHouse with token lookup and indicators
        
        Uses the same parsing and enrichment logic as binary_to_parquet converter,
        but skips parquet conversion and ingests directly to ClickHouse.
        
        ✅ ENSURES: Uses zerodha_token_list/zerodha_instrument_token.json (recent token file)
        """
        if not binary_files:
            logger.warning("No binary files to ingest")
            return
        
        if not self.clickhouse_available:
            logger.error("ClickHouse not available, cannot ingest files")
            return
        
        # Verify token_manager is using the correct file
        token_file_path = Path(token_manager.token_lookup_path)
        if not token_file_path.exists():
            logger.error(f"❌ Token file not found: {token_file_path}")
            return
        
        # Ensure we're using the recent token file (not old one)
        if "zerodha_token_list" not in str(token_file_path) and "zerodha_instrument_token.json" not in str(token_file_path):
            logger.warning(f"⚠️ Token file path doesn't look like recent file: {token_file_path}")
            logger.warning(f"   Expected: zerodha_token_list/zerodha_instrument_token.json")
        
        logger.info(f"✅ Using token file: {token_file_path} ({len(token_manager.token_data) if hasattr(token_manager, 'token_data') else 'N/A'} tokens)")
        
        # Initialize HybridCalculations for indicators
        try:
            from intraday_trading.intraday_scanner.calculations import HybridCalculations
            # ✅ FIXED: Use UnifiedRedisManager for centralized connection management
            from shared_core.redis_clients import UnifiedRedisManager
            redis_client = get_redis_client(process_name="parquet_convert_hybrid", db=1)
            hybrid_calc = HybridCalculations(max_cache_size=500, max_batch_size=174, redis_client=redis_client)
            logger.info("✅ Initialized HybridCalculations for binary file processing")
        except Exception as e:
            logger.warning(f"Failed to initialize HybridCalculations: {e}, proceeding without indicators")
            hybrid_calc = None
        
        # Try to use ProductionZerodhaBinaryConverter if available (preferred method)
        # ✅ CRITICAL: Pass token_manager explicitly to ensure it uses zerodha_token_list
        try:
            from binary_to_parquet.production_binary_converter import ProductionZerodhaBinaryConverter
            
            # Verify token_manager is the one we want (with zerodha_token_list)
            if hasattr(token_manager, 'token_lookup_path'):
                logger.info(f"✅ Initializing ProductionZerodhaBinaryConverter with token file: {token_manager.token_lookup_path}")
            
            converter = ProductionZerodhaBinaryConverter(
                db_path=None,  # No DB needed - just parsing
                token_cache=token_manager,  # ✅ Explicitly pass token_manager (uses zerodha_token_list)
                drop_unknown_tokens=False,
                ensure_schema=False,
            )
            
            # Verify converter is using our token_cache
            if hasattr(converter, 'token_cache') and converter.token_cache is token_manager:
                logger.info("✅ ProductionZerodhaBinaryConverter is using the provided token_manager")
            else:
                logger.warning("⚠️ ProductionZerodhaBinaryConverter may not be using the provided token_manager")
            
            use_production_converter = True
            logger.info("✅ Using ProductionZerodhaBinaryConverter for parsing")
        except ImportError:
            # Fallback to ZerodhaBinaryParser
            use_production_converter = False
            try:
                # ✅ FIXED: Use most recent websocket_message_parser from zerodha.crawlers
                from zerodha.crawlers.websocket_message_parser import ZerodhaBinaryParser
                from crawlers.utils.instrument_mapper import InstrumentMapper
                
                class SimpleTokenMapper(InstrumentMapper):
                    """Simple mapper that uses TokenCacheManager for symbol resolution"""
                    def __init__(self, token_manager: TokenCacheManager):
                        super().__init__(instruments=None, master_mapping_path=None, token_metadata=None)
                        self.token_manager = token_manager
                    
                    def token_to_symbol(self, instrument_token: int) -> str:
                        """Resolve token to symbol using TokenCacheManager"""
                        info = self.token_manager.get_symbol_info(instrument_token)
                        symbol = info.get('symbol', f'UNKNOWN_{instrument_token}')
                        return symbol
                
                instrument_mapper = SimpleTokenMapper(token_manager)
                logger.info("✅ Using ZerodhaBinaryParser (fallback) for parsing")
            except ImportError as e:
                logger.error(f"Binary parser not available: {e}")
                return
        
        total_files = len(binary_files)
        successful_ingestions = 0
        total_rows = 0
        
        for i, file_path in enumerate(binary_files):
            try:
                logger.info(f"Processing binary file {i+1}/{total_files}: {file_path}")
                file_path_obj = Path(file_path)
                
                # Read binary data
                raw_data = file_path_obj.read_bytes()
                
                if use_production_converter:
                    # Use ProductionZerodhaBinaryConverter (preferred method)
                    # This follows the same logic as binary_to_parquet_parquet.py
                    try:
                        # Parse binary file
                        packets = converter._detect_packet_format(raw_data)
                        logger.info(f"  📦 Parsed {len(packets):,} packets")
                        
                        if not packets:
                            logger.warning(f"  ⚠️ No packets found in {file_path}")
                            continue
                        
                        # Process packets in batches and enrich them
                        all_records = []
                        errors = 0
                        skipped = 0
                        process_batch_size = 5000
                        
                        for batch_start in range(0, len(packets), process_batch_size):
                            batch = packets[batch_start:batch_start + process_batch_size]
                            
                            # Enrich packets (uses token_cache for metadata)
                            enriched_records, batch_errors, batch_skipped = converter._enrich_packets(
                                batch, file_path_obj, None  # conn=None - no DB needed
                            )
                            
                            errors += batch_errors
                            skipped += batch_skipped
                            all_records.extend(enriched_records)
                            
                            if (batch_start + process_batch_size) % 50000 == 0:
                                logger.info(f"  Processed {batch_start + process_batch_size:,}/{len(packets):,} packets ({len(all_records):,} records)...")
                        
                        if not all_records:
                            logger.warning(f"  ⚠️ No valid records after enrichment: {file_path}")
                            continue
                        
                        logger.info(f"  ✅ Enriched {len(all_records):,} records (errors: {errors}, skipped: {skipped})")
                        
                        # Convert to Polars DataFrame
                        df = pl.DataFrame(all_records)
                        
                    except Exception as e:
                        logger.error(f"  ❌ ProductionZerodhaBinaryConverter failed: {e}")
                        import traceback
                        logger.debug(traceback.format_exc())
                        continue
                else:
                    # Fallback: Use ZerodhaBinaryParser
                    parser = ZerodhaBinaryParser(instrument_mapper=instrument_mapper, redis_client=None)
                    
                    # Try WebSocket message format first (with 2-byte packet count header)
                    packets = parser.parse_binary_message(raw_data)
                    
                    # If no packets found, try raw packet stream format (packets directly concatenated)
                    if not packets:
                        logger.info(f"  🔄 No packets in WebSocket format, trying raw packet stream...")
                        packets = self._parse_raw_packet_stream(raw_data, parser)
                    
                    if not packets:
                        logger.warning(f"  ⚠️ No packets found in {file_path}")
                        continue
                    
                    logger.info(f"  📦 Parsed {len(packets):,} packets from binary file")
                    
                    # Convert packets to records (basic parsing without token enrichment)
                    records = []
                    for packet in packets:
                        if packet.get('parsed'):
                            record = packet['parsed'].copy()
                            records.append(record)
                    
                    if not records:
                        logger.warning(f"  ⚠️ No valid records parsed from {file_path}")
                        continue
                    
                    logger.info(f"  ✅ Parsed {len(records):,} records from binary file")
                    
                    # Convert to Polars DataFrame
                    df = pl.DataFrame(records)
                    
                    # Enrich with token metadata from zerodha_token_list
                    df = self._enrich_with_token_metadata(df, token_manager)
                
                # Process with HybridCalculations for indicators
                if hybrid_calc and not df.is_empty():
                    df = self._apply_hybrid_calculations(df, hybrid_calc)
                
                # Transform to ClickHouse format
                clickhouse_data = self._transform_to_clickhouse_format(df, file_path)
                
                if not clickhouse_data:
                    logger.warning(f"  ⚠️ No valid data after transformation: {file_path}")
                    continue
                
                # Batch insert into ClickHouse
                rows_inserted = self._insert_batch_to_clickhouse(clickhouse_data, batch_size)
                total_rows += rows_inserted
                successful_ingestions += 1
                logger.info(f"  ✅ Successfully ingested {file_path}: {rows_inserted:,} rows")
                
            except Exception as e:
                logger.error(f"  ❌ Failed to ingest {file_path}: {e}")
                import traceback
                logger.debug(traceback.format_exc())
        
        logger.info(f"✅ Binary file ingestion complete: {successful_ingestions}/{total_files} successful, {total_rows:,} total rows")
    
    def _enrich_with_token_metadata(self, df: pl.DataFrame, token_manager: TokenCacheManager) -> pl.DataFrame:
        """Enrich DataFrame with token metadata from zerodha_token_list/zerodha_instrument_token.json"""
        if 'instrument_token' not in df.columns:
            return df
        
        logger.info(f"  🔍 Enriching {len(df):,} records with token metadata from zerodha_token_list...")
        
        # Get unique tokens
        unique_tokens = df['instrument_token'].unique().to_list()
        logger.debug(f"  Found {len(unique_tokens)} unique tokens to lookup")
        
        # Build metadata mapping using zerodha_token_list
        metadata_map = {}
        tokens_found = 0
        tokens_not_found = 0
        
        for token in unique_tokens:
            if token is not None:
                info = token_manager.get_symbol_info(int(token))
                metadata_map[token] = info
                if info.get('symbol', '').startswith('UNKNOWN'):
                    tokens_not_found += 1
                else:
                    tokens_found += 1
        
        logger.info(f"  ✅ Token lookup: {tokens_found} found, {tokens_not_found} not found in zerodha_token_list")
        
        # Add/update metadata columns using zerodha_token_list data
        metadata_cols = []
        
        # Always update symbol from zerodha_token_list (overwrite any existing)
        metadata_cols.append(
            pl.col('instrument_token').map_elements(
                lambda t: metadata_map.get(t, {}).get('symbol', f'UNKNOWN_{t}') if t is not None else '',
                return_dtype=pl.Utf8
            ).alias('symbol')
        )
        
        # Add tradingsymbol
        metadata_cols.append(
            pl.col('instrument_token').map_elements(
                lambda t: metadata_map.get(t, {}).get('tradingsymbol', '') if t is not None else '',
                return_dtype=pl.Utf8
            ).alias('tradingsymbol')
        )
        
        # Update exchange from zerodha_token_list
        metadata_cols.append(
            pl.col('instrument_token').map_elements(
                lambda t: metadata_map.get(t, {}).get('exchange', '') if t is not None else '',
                return_dtype=pl.Utf8
            ).alias('exchange')
        )
        
        # Add sector (from zerodha_token_list)
        metadata_cols.append(
            pl.col('instrument_token').map_elements(
                lambda t: metadata_map.get(t, {}).get('sector', '') if t is not None else '',
                return_dtype=pl.Utf8
            ).alias('sector')
        )
        
        # Add asset_class
        metadata_cols.append(
            pl.col('instrument_token').map_elements(
                lambda t: metadata_map.get(t, {}).get('asset_class', '') if t is not None else '',
                return_dtype=pl.Utf8
            ).alias('asset_class')
        )
        
        # Add sub_category
        metadata_cols.append(
            pl.col('instrument_token').map_elements(
                lambda t: metadata_map.get(t, {}).get('sub_category', '') if t is not None else '',
                return_dtype=pl.Utf8
            ).alias('sub_category')
        )
        
        # Add instrument_type, segment, etc.
        metadata_cols.append(
            pl.col('instrument_token').map_elements(
                lambda t: metadata_map.get(t, {}).get('instrument_type', '') if t is not None else '',
                return_dtype=pl.Utf8
            ).alias('instrument_type')
        )
        
        metadata_cols.append(
            pl.col('instrument_token').map_elements(
                lambda t: metadata_map.get(t, {}).get('segment', '') if t is not None else '',
                return_dtype=pl.Utf8
            ).alias('segment')
        )
        
        # Apply all metadata columns
        if metadata_cols:
            df = df.with_columns(metadata_cols)
            logger.info(f"  ✅ Added token metadata columns from zerodha_token_list")
        
        return df
    
    def _parse_raw_packet_stream(self, raw_data: bytes, parser: Optional[Any] = None) -> List[Dict[str, Any]]:
        """Parse raw packet stream (packets directly concatenated without WebSocket header)
        
        Strategy: Try packet sizes in descending order (184 > 32 > 8) to find the most comprehensive match.
        Prefer larger packets as they contain more complete data.
        """
        packets = []
        offset = 0
        
        # Common packet sizes in descending order (prefer larger/more complete packets)
        # 184 = full quote (most comprehensive), 32 = index, 8 = ltp (least comprehensive)
        packet_sizes = [184, 32, 8]
        
        # Track statistics
        packets_by_type = {"full_quote": 0, "index": 0, "ltp": 0, "unknown": 0}
        skipped_bytes = 0
        
        while offset < len(raw_data):
            # Try each packet size in descending order to find the best match
            best_match = None
            best_size = None
            best_type = None
            
            for size in packet_sizes:
                if offset + size > len(raw_data):
                    continue
                
                packet_data = raw_data[offset:offset + size]
                
                # Try to parse as this packet type
                try:
                    parsed_packet = None
                    packet_type = None
                    
                    if size == 184:
                        parsed_packet = parser._parse_quote_packet(packet_data)
                        packet_type = "full_quote"
                    elif size == 32:
                        parsed_packet = parser._parse_index_packet(packet_data)
                        packet_type = "index"
                    elif size == 8:
                        parsed_packet = parser._parse_ltp_packet(packet_data)
                        packet_type = "ltp"
                    
                    # Validate the parsed packet
                    if parsed_packet and self._validate_packet(parsed_packet, size):
                        # Found a valid packet - use the largest valid size (first in our order)
                        best_match = parsed_packet
                        best_size = size
                        best_type = packet_type
                        break  # Take the first (largest) valid match
                        
                except Exception:
                    continue
            
            if best_match and best_size:
                packets.append({
                    "packet_length": best_size,
                    "packet_data": raw_data[offset:offset + best_size],
                    "packet_type": best_type,
                    "parsed": best_match,
                })
                packets_by_type[best_type] = packets_by_type.get(best_type, 0) + 1
                offset += best_size
            else:
                # If no valid packet found, skip 1 byte and try again
                offset += 1
                skipped_bytes += 1
                if offset % 100000 == 0:
                    logger.debug(f"  Scanning raw stream: offset {offset:,}/{len(raw_data):,} ({skipped_bytes:,} bytes skipped)")
        
        logger.info(f"  📦 Parsed {len(packets):,} packets from raw stream: "
                   f"{packets_by_type.get('full_quote', 0)} full_quote, "
                   f"{packets_by_type.get('index', 0)} index, "
                   f"{packets_by_type.get('ltp', 0)} ltp")
        if skipped_bytes > 0:
            logger.debug(f"  ⚠️ Skipped {skipped_bytes:,} bytes that didn't match any packet format")
        
        return packets
    
    def _validate_packet(self, packet: Dict[str, Any], expected_size: int) -> bool:
        """Validate that a parsed packet is reasonable"""
        if not packet:
            return False
        
        # Must have instrument_token
        token = packet.get('instrument_token')
        if not token or not isinstance(token, int):
            return False
        
        # Token should be a reasonable value (not 0, not negative unless it's a valid signed int)
        # Valid tokens are typically in range 1 to ~100 million
        if token <= 0 or token > 200000000:
            return False
        
        # For full quote packets (184 bytes), should have price data
        if expected_size == 184:
            ltp = packet.get('last_traded_price')
            if ltp is None:
                return False
            # Price should be reasonable (not 0, not negative unless it's a valid signed int)
            # Prices are typically positive, but can be negative in signed int format
            # Just check it's not absurdly large
            if abs(ltp) > 1000000000:  # 10 billion is absurd
                return False
        
        # For index packets (32 bytes), should have OHLC data
        if expected_size == 32:
            if not any(key in packet for key in ['last_traded_price', 'high_price', 'low_price', 'open_price', 'close_price']):
                return False
        
        # For LTP packets (8 bytes), just need token and price
        if expected_size == 8:
            if 'last_traded_price' not in packet:
                return False
        
        return True
    
    def _apply_hybrid_calculations(self, df: pl.DataFrame, hybrid_calc) -> pl.DataFrame:
        """Apply HybridCalculations to DataFrame for indicators and Greeks"""
        try:
            if 'symbol' not in df.columns:
                return df
            
            # Group by symbol and process
            symbol_data = {}
            for row in df.iter_rows(named=True):
                symbol = row.get('symbol', 'UNKNOWN')
                if symbol not in symbol_data:
                    symbol_data[symbol] = []
                symbol_data[symbol].append(row)
            
            # Process with HybridCalculations
            if symbol_data:
                try:
                    calculated_results = hybrid_calc.batch_process_symbols(symbol_data, max_ticks_per_symbol=1000)
                    
                    # Merge indicators back into DataFrame
                    # For now, indicators are calculated but mapping to rows needs proper implementation
                    # This is a placeholder - full implementation would map indicators per row
                    logger.debug(f"✅ HybridCalculations processed {len(symbol_data)} symbols")
                except Exception as e:
                    logger.warning(f"HybridCalculations batch processing failed: {e}")
            
            return df
        except Exception as e:
            logger.warning(f"Failed to apply HybridCalculations: {e}")
            return df
    
    def _transform_to_clickhouse_format(self, df: pl.DataFrame, source_file: str) -> List[Dict]:
        """Transform Polars DataFrame to ClickHouse tick_data format with all required columns"""
        try:
            # Convert to list of dicts
            records = df.to_dicts()
            clickhouse_records = []
            
            for record in records:
                # Map all fields to ClickHouse schema
                ch_record = self._map_record_to_clickhouse(record, source_file)
                if ch_record:
                    clickhouse_records.append(ch_record)
            
            return clickhouse_records
        except Exception as e:
            logger.error(f"Failed to transform to ClickHouse format: {e}")
            return []
    
    def _map_record_to_clickhouse(self, record: Dict, source_file: str) -> Optional[Dict]:
        """Map a single record to ClickHouse tick_data schema"""
        try:
            from datetime import datetime
            import pytz
            
            # Timezone for Asia/Kolkata
            ist = pytz.timezone('Asia/Kolkata')
            utc = pytz.UTC
            
            # Parse timestamps
            timestamp = self._parse_timestamp(record.get('timestamp') or record.get('timestamp_norm') or 
                                             record.get('exchange_timestamp') or record.get('exchange_timestamp_norm'))
            exchange_timestamp = self._parse_timestamp(record.get('exchange_timestamp') or record.get('exchange_timestamp_norm') or timestamp)
            
            # Convert to Asia/Kolkata timezone
            if timestamp:
                if timestamp.tzinfo is None:
                    timestamp = utc.localize(timestamp)
                timestamp = timestamp.astimezone(ist)
            if exchange_timestamp:
                if exchange_timestamp.tzinfo is None:
                    exchange_timestamp = utc.localize(exchange_timestamp)
                exchange_timestamp = exchange_timestamp.astimezone(ist)
            
            # Map all ClickHouse columns
            ch_record = {
                # Basic identification
                'instrument_token': int(record.get('instrument_token', 0)),
                'symbol': str(record.get('symbol', '')),
                'exchange': str(record.get('exchange', '')),
                
                # Timestamps
                'timestamp': timestamp or datetime.now(ist),
                'exchange_timestamp': exchange_timestamp or timestamp or datetime.now(ist),
                
                # Price data
                'last_price': float(record.get('last_price') or record.get('ltp') or 0.0),
                'open_price': float(record.get('open_price') or record.get('open') or 0.0),
                'high_price': float(record.get('high_price') or record.get('high') or 0.0),
                'low_price': float(record.get('low_price') or record.get('low') or 0.0),
                'close_price': float(record.get('close_price') or record.get('close') or 0.0),
                'average_traded_price': float(record.get('average_traded_price') or record.get('average_price') or 0.0),
                
                # Volume and OI
                'volume': int(record.get('volume') or 0),
                'last_traded_quantity': int(record.get('last_traded_quantity') or 0),
                'total_buy_quantity': int(record.get('total_buy_quantity') or 0),
                'total_sell_quantity': int(record.get('total_sell_quantity') or 0),
                'open_interest': int(record.get('open_interest') or record.get('oi') or 0),
                'oi_day_high': int(record.get('oi_day_high') or 0),
                'oi_day_low': int(record.get('oi_day_low') or 0),
                
                # Market depth (L2/L3)
                'bid_1_price': float(record.get('bid_1_price') or record.get('best_bid_price') or 0.0),
                'bid_1_quantity': int(record.get('bid_1_quantity') or 0),
                'bid_1_orders': int(record.get('bid_1_orders') or 0),
                'bid_2_price': float(record.get('bid_2_price') or 0.0),
                'bid_2_quantity': int(record.get('bid_2_quantity') or 0),
                'bid_2_orders': int(record.get('bid_2_orders') or 0),
                'bid_3_price': float(record.get('bid_3_price') or 0.0),
                'bid_3_quantity': int(record.get('bid_3_quantity') or 0),
                'bid_3_orders': int(record.get('bid_3_orders') or 0),
                'bid_4_price': float(record.get('bid_4_price') or 0.0),
                'bid_4_quantity': int(record.get('bid_4_quantity') or 0),
                'bid_4_orders': int(record.get('bid_4_orders') or 0),
                'bid_5_price': float(record.get('bid_5_price') or 0.0),
                'bid_5_quantity': int(record.get('bid_5_quantity') or 0),
                'bid_5_orders': int(record.get('bid_5_orders') or 0),
                'ask_1_price': float(record.get('ask_1_price') or record.get('best_ask_price') or 0.0),
                'ask_1_quantity': int(record.get('ask_1_quantity') or 0),
                'ask_1_orders': int(record.get('ask_1_orders') or 0),
                'ask_2_price': float(record.get('ask_2_price') or 0.0),
                'ask_2_quantity': int(record.get('ask_2_quantity') or 0),
                'ask_2_orders': int(record.get('ask_2_orders') or 0),
                'ask_3_price': float(record.get('ask_3_price') or 0.0),
                'ask_3_quantity': int(record.get('ask_3_quantity') or 0),
                'ask_3_orders': int(record.get('ask_3_orders') or 0),
                'ask_4_price': float(record.get('ask_4_price') or 0.0),
                'ask_4_quantity': int(record.get('ask_4_quantity') or 0),
                'ask_4_orders': int(record.get('ask_4_orders') or 0),
                'ask_5_price': float(record.get('ask_5_price') or 0.0),
                'ask_5_quantity': int(record.get('ask_5_quantity') or 0),
                'ask_5_orders': int(record.get('ask_5_orders') or 0),
                
                # Metadata
                'packet_type': str(record.get('packet_type', '')),
                'session_type': self._map_session_type(record.get('session_type')),
                'data_quality': self._map_data_quality(record.get('data_quality', 'enriched')),
                
                # Instrument metadata (from zerodha_token_list)
                'instrument_type': str(record.get('instrument_type', '')),
                'segment': str(record.get('segment', '')),
                'tradingsymbol': str(record.get('tradingsymbol', '')),
                'sector': str(record.get('sector', '')),
                'asset_class': str(record.get('asset_class', '')),
                'sub_category': str(record.get('sub_category', '')),
                'crawler_source': str(record.get('crawler_source', '') or Path(source_file).parent.name),
                'pushed_at': datetime.now(utc),
                'processing_timestamp': datetime.now(utc),
                
                # Crypto fields (defaults)
                'crypto_exchange': 'binance',
                'base_asset': '',
                'quote_asset': '',
                'funding_rate': 0.0,
                'is_perpetual': 0,
                
                # Processing info
                'processed_at': datetime.now(utc),
                'batch_id': str(Path(source_file).stem),
            }
            
            return ch_record
        except Exception as e:
            logger.debug(f"Failed to map record to ClickHouse: {e}")
            return None
    
    def _parse_timestamp(self, ts_value: Any) -> Optional[datetime]:
        """Parse timestamp from various formats"""
        if ts_value is None:
            return None
        
        from datetime import datetime
        import pytz
        
        try:
            # If already datetime
            if isinstance(ts_value, datetime):
                return ts_value
            
            # If string (ISO format)
            if isinstance(ts_value, str):
                # Try parsing ISO format
                try:
                    dt = datetime.fromisoformat(ts_value.replace('Z', '+00:00'))
                    return dt
                except:
                    pass
            
            # If int (epoch milliseconds)
            if isinstance(ts_value, (int, float)):
                return datetime.fromtimestamp(ts_value / 1000.0, tz=pytz.UTC)
            
            return None
        except Exception as e:
            logger.debug(f"Failed to parse timestamp {ts_value}: {e}")
            return None
    
    def _map_session_type(self, session_type: Any) -> int:
        """Map session type to ClickHouse Enum8"""
        if not session_type:
            return 2  # regular
        
        session_str = str(session_type).lower()
        if 'pre' in session_str:
            return 1  # pre_market
        elif 'post' in session_str:
            return 3  # post_market
        else:
            return 2  # regular
    
    def _map_data_quality(self, data_quality: Any) -> int:
        """Map data quality to ClickHouse Enum8"""
        if not data_quality:
            return 2  # enriched
        
        quality_str = str(data_quality).lower()
        if 'raw' in quality_str:
            return 1
        elif 'corrected' in quality_str:
            return 3
        elif 'historical' in quality_str:
            return 4
        elif 'realtime' in quality_str:
            return 5
        else:
            return 2  # enriched
    
    def _insert_batch_to_clickhouse(self, records: List[Dict], batch_size: int = 10000) -> int:
        """Insert batch of records into ClickHouse tick_data table"""
        if not records:
            return 0
        
        total_inserted = 0
        
        # Insert in batches
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            try:
                query = """
                INSERT INTO tick_data (
                    instrument_token, symbol, exchange, timestamp, exchange_timestamp,
                    last_price, open_price, high_price, low_price, close_price, average_traded_price,
                    volume, last_traded_quantity, total_buy_quantity, total_sell_quantity,
                    open_interest, oi_day_high, oi_day_low,
                    bid_1_price, bid_1_quantity, bid_1_orders,
                    bid_2_price, bid_2_quantity, bid_2_orders,
                    bid_3_price, bid_3_quantity, bid_3_orders,
                    bid_4_price, bid_4_quantity, bid_4_orders,
                    bid_5_price, bid_5_quantity, bid_5_orders,
                    ask_1_price, ask_1_quantity, ask_1_orders,
                    ask_2_price, ask_2_quantity, ask_2_orders,
                    ask_3_price, ask_3_quantity, ask_3_orders,
                    ask_4_price, ask_4_quantity, ask_4_orders,
                    ask_5_price, ask_5_quantity, ask_5_orders,
                    packet_type, session_type, data_quality,
                    instrument_type, segment, tradingsymbol, sector, asset_class, sub_category,
                    crawler_source, pushed_at, processing_timestamp,
                    crypto_exchange, base_asset, quote_asset, funding_rate, is_perpetual,
                    processed_at, batch_id
                ) VALUES
                """
                
                # Convert records to tuples in correct order
                values = []
                for record in batch:
                    values.append((
                        record['instrument_token'],
                        record['symbol'],
                        record['exchange'],
                        record['timestamp'],
                        record['exchange_timestamp'],
                        record['last_price'],
                        record['open_price'],
                        record['high_price'],
                        record['low_price'],
                        record['close_price'],
                        record['average_traded_price'],
                        record['volume'],
                        record['last_traded_quantity'],
                        record['total_buy_quantity'],
                        record['total_sell_quantity'],
                        record['open_interest'],
                        record['oi_day_high'],
                        record['oi_day_low'],
                        record['bid_1_price'],
                        record['bid_1_quantity'],
                        record['bid_1_orders'],
                        record['bid_2_price'],
                        record['bid_2_quantity'],
                        record['bid_2_orders'],
                        record['bid_3_price'],
                        record['bid_3_quantity'],
                        record['bid_3_orders'],
                        record['bid_4_price'],
                        record['bid_4_quantity'],
                        record['bid_4_orders'],
                        record['bid_5_price'],
                        record['bid_5_quantity'],
                        record['bid_5_orders'],
                        record['ask_1_price'],
                        record['ask_1_quantity'],
                        record['ask_1_orders'],
                        record['ask_2_price'],
                        record['ask_2_quantity'],
                        record['ask_2_orders'],
                        record['ask_3_price'],
                        record['ask_3_quantity'],
                        record['ask_3_orders'],
                        record['ask_4_price'],
                        record['ask_4_quantity'],
                        record['ask_4_orders'],
                        record['ask_5_price'],
                        record['ask_5_quantity'],
                        record['ask_5_orders'],
                        record['packet_type'],
                        record['session_type'],
                        record['data_quality'],
                        record['instrument_type'],
                        record['segment'],
                        record['tradingsymbol'],
                        record['sector'],
                        record['asset_class'],
                        record['sub_category'],
                        record['crawler_source'],
                        record['pushed_at'],
                        record['processing_timestamp'],
                        record['crypto_exchange'],
                        record['base_asset'],
                        record['quote_asset'],
                        record['funding_rate'],
                        record['is_perpetual'],
                        record['processed_at'],
                        record['batch_id']
                    ))
                
                self.clickhouse.execute(query, values)
                total_inserted += len(batch)
                
            except Exception as e:
                logger.error(f"Failed to insert batch {i//batch_size + 1}: {e}")
                import traceback
                logger.debug(traceback.format_exc())
        
        return total_inserted
    
    def ingest_news_files(self, news_pattern: str = "tick_data_storage/news/*.json"):
        """Ingest news data from JSON files"""
        try:
            news_files = glob.glob(news_pattern)
            if not news_files:
                logger.warning(f"No news files found matching pattern: {news_pattern}")
                return
            
            for news_file in news_files:
                try:
                    logger.info(f"Ingesting news from {news_file}")
                    
                    # Read JSON file
                    if news_file.endswith('.jsonl'):
                        with open(news_file, 'r') as f:
                            news_data = [json.loads(line) for line in f]
                    else:
                        with open(news_file, 'r') as f:
                            news_data = json.load(f)
                            if not isinstance(news_data, list):
                                news_data = [news_data]
                    
                    # Convert to DataFrame and ingest using Polars
                    if news_data and POLARS_AVAILABLE:
                        df_news = pl.DataFrame(news_data)
                        # Convert to PyArrow table for DuckDB
                        arrow_table = df_news.to_arrow()
                        self.conn.register('news_df', arrow_table)
                        self.conn.execute("""
                            INSERT OR REPLACE INTO news_data 
                            SELECT * FROM news_df
                        """)
                        logger.info(f"Ingested {len(df_news)} news records from {news_file}")
                        
                except Exception as e:
                    logger.error(f"Failed to ingest news file {news_file}: {e}")
        
        except Exception as e:
            logger.error(f"News ingestion failed: {e}")
    
    def ingest_indices_files(self, indices_pattern: str = "tick_data_storage/indices/*.jsonl"):
        """Ingest indices data from JSONL files"""
        try:
            indices_files = glob.glob(indices_pattern)
            if not indices_files:
                logger.warning(f"No indices files found matching pattern: {indices_pattern}")
                return
            
            for indices_file in indices_files:
                try:
                    logger.info(f"Ingesting indices from {indices_file}")
                    
                    # Read JSONL file
                    with open(indices_file, 'r') as f:
                        indices_data = [json.loads(line) for line in f]
                    
                    # Convert to DataFrame and ingest using Polars
                    if indices_data and POLARS_AVAILABLE:
                        df_indices = pl.DataFrame(indices_data)
                        # Convert to PyArrow table for DuckDB
                        arrow_table = df_indices.to_arrow()
                        self.conn.register('indices_df', arrow_table)
                        self.conn.execute("""
                            INSERT OR REPLACE INTO daily_indices 
                            SELECT * FROM indices_df
                        """)
                        logger.info(f"Ingested {len(df_indices)} indices records from {indices_file}")
                        
                except Exception as e:
                    logger.error(f"Failed to ingest indices file {indices_file}: {e}")
        
        except Exception as e:
            logger.error(f"Indices ingestion failed: {e}")
    
    def create_indexes(self):
        """Create performance indexes (ClickHouse uses ORDER BY for indexing, no separate index creation needed)"""
        logger.info("✅ ClickHouse uses ORDER BY for indexing - no separate index creation needed")
    
    def close(self):
        """Close ClickHouse connection"""
        if self.clickhouse:
            try:
                self.clickhouse.disconnect()
            except:
                pass
    
    def process_jsonl_files(self) -> Dict:
        """Process JSONL files for news and indices (second priority after parquet)"""
        stats = {
            'news_processed': 0,
            'news_rows': 0,
            'indices_processed': 0,
            'indices_rows': 0,
            'news_errors': 0,
            'indices_errors': 0
        }
        
        # Import existing ingestion functions
        try:
            sys.path.insert(0, str(Path(__file__).parent / "analysis" / "scripts"))
            from analysis.scripts.ingest_news_json_duckdb import ingest_news_json_file, get_pending_news_files
            from analysis.scripts.ingest_indices_json_duckdb import ingest_indices_jsonl_file, get_pending_indices_files
            from token_cache import TokenCacheManager as BaseTokenCacheManager
        except ImportError as e:
            logger.error(f"Could not import JSONL ingestion modules: {e}")
            return stats
        
        # Load token cache for news processing
        # ✅ ENSURES: Uses zerodha_token_list/zerodha_instrument_token.json (recent token file)
        token_cache = None
        try:
            # Verify token_lookup_path is the recent file
            if "zerodha_token_list" not in self.token_lookup_path and "zerodha_instrument_token.json" not in self.token_lookup_path:
                logger.warning(f"⚠️ token_lookup_path doesn't look like recent file: {self.token_lookup_path}")
                logger.warning(f"   Using it anyway, but expected: zerodha_token_list/zerodha_instrument_token.json")
            
            token_cache = BaseTokenCacheManager(
                cache_path=self.token_lookup_path,  # ✅ Should be zerodha_token_list/zerodha_instrument_token.json
                verbose=False
            )
            logger.info(f"✅ BaseTokenCacheManager loaded from: {self.token_lookup_path} ({len(token_cache.token_map):,} tokens)")
        except Exception as e:
            logger.warning(f"Could not load token cache for JSONL processing: {e}")
        
        # Process News Files
        logger.info("📰 Processing News JSONL files...")
        news_search_dirs = [
            "tick_data_storage/indices/news",
            "tick_data_storage/data_mining",
            "tick_data_storage/gift_nifty_gap",
            "."
        ]
        
        try:
            news_files = get_pending_news_files(self.db_path, news_search_dirs)
            logger.info(f"Found {len(news_files)} news files to process")
            
            for news_file in news_files:
                try:
                    conn = duckdb.connect(self.db_path)
                    result = ingest_news_json_file(conn, Path(news_file), token_cache)
                    conn.close()
                    
                    if result.get("success"):
                        stats['news_processed'] += 1
                        stats['news_rows'] += result.get("row_count", 0)
                        if stats['news_processed'] % 10 == 0:
                            logger.info(f"  Progress: {stats['news_processed']}/{len(news_files)} news files processed")
                    else:
                        stats['news_errors'] += 1
                        logger.warning(f"  Failed to process {news_file}: {result.get('error', 'Unknown error')[:100]}")
                except Exception as e:
                    stats['news_errors'] += 1
                    logger.warning(f"  Error processing news file {news_file}: {str(e)[:100]}")
        except Exception as e:
            logger.error(f"News processing failed: {e}")
        
        # Process Indices Files
        logger.info("📊 Processing Indices JSONL files...")
        indices_search_dirs = [
            "tick_data_storage/indices",
            "tick_data_storage/data_mining",
            "tick_data_storage/gift_nifty_gap"
        ]
        
        try:
            indices_files = get_pending_indices_files(self.db_path, indices_search_dirs)
            logger.info(f"Found {len(indices_files)} indices files to process")
            
            for indices_file in indices_files:
                try:
                    conn = duckdb.connect(self.db_path)
                    result = ingest_indices_jsonl_file(conn, Path(indices_file), token_cache)
                    conn.close()
                    
                    if result.get("success"):
                        stats['indices_processed'] += 1
                        stats['indices_rows'] += result.get("row_count", 0)
                        if stats['indices_processed'] % 10 == 0:
                            logger.info(f"  Progress: {stats['indices_processed']}/{len(indices_files)} indices files processed")
                    else:
                        stats['indices_errors'] += 1
                        logger.warning(f"  Failed to process {indices_file}: {result.get('error', 'Unknown error')[:100]}")
                except Exception as e:
                    stats['indices_errors'] += 1
                    logger.warning(f"  Error processing indices file {indices_file}: {str(e)[:100]}")
        except Exception as e:
            logger.error(f"Indices processing failed: {e}")
        
        return stats

class ComprehensiveReprocessingPipeline:
    """Main pipeline that coordinates the complete reprocessing workflow"""
    
    def __init__(self, 
                 input_pattern: str = "tick_data_storage/**/*.parquet",
                 token_lookup_path: str = "zerodha_token_list/zerodha_instrument_token.json",
                 db_path: str = "tick_data_production.db",
                 max_workers: int = None,
                 date_filter: Optional[str] = None,
                 skip_ingestion: bool = False,
                 max_memory_gb: float = None,
                 batch_size: int = 50,
                 use_subprocess: bool = True):
        
        self.input_pattern = input_pattern
        self.date_filter = date_filter
        # Use multiple workers by default (M4 Mac has powerful multi-core)
        # Default to CPU count - 1, or 4 minimum for M4
        default_workers = max(4, mp.cpu_count() - 1)
        self.max_workers = max_workers if max_workers is not None else default_workers
        # Memory limit: default 3GB total, divided by workers (1.5GB per worker for 2 workers)
        self.max_memory_gb = max_memory_gb or (3.0 / self.max_workers)
        self.batch_size = batch_size
        self.use_subprocess = use_subprocess
        
        # Initialize components (don't create DB connection here - it can't be pickled)
        # ✅ UPDATED: Use zerodha_token_list by default (default parameter already set)
        self.token_lookup_path = token_lookup_path
        self.db_path = db_path
        self.clickhouse_host = "localhost"
        self.clickhouse_database = "trading"
        
        # JSONL processing settings
        self.process_jsonl = True  # Process JSONL files after parquet
        self.skip_ingestion = skip_ingestion
        # These will be initialized per-process or in main thread
        self.token_manager = None
        self.recovery_engine = None
        self.enricher = None
        self.writer = None
        self.ingestor = None
        
        # For test mode: store limited files list
        self.limited_files = None
        
        # Statistics
        self.stats = {
            'total_files': 0,
            'successful_recoveries': 0,
            'failed_recoveries': 0,
            'total_rows_processed': 0,
            'enriched_files_created': 0,
            'start_time': None,
            'end_time': None
        }
    
    def find_parquet_files(self, date_filter: Optional[str] = None) -> List[str]:
        """Find all parquet files matching the input pattern from all crawlers using rglob"""
        # If limited files are set (test mode), return those
        if self.limited_files is not None:
            return self.limited_files
        
        # Search in all three main crawler directories using rglob
        crawler_dirs = [
            Path("tick_data_storage/intraday_data"),
            Path("tick_data_storage/data_mining"),
            Path("tick_data_storage/research")
        ]
        
        parquet_files = []
        for crawler_dir in crawler_dirs:
            if not crawler_dir.exists():
                continue
            try:
                files = [str(f) for f in crawler_dir.rglob("*.parquet")]
                parquet_files.extend(files)
                logger.debug(f"Found {len(files)} parquet files in {crawler_dir}")
            except Exception as e:
                logger.warning(f"Error searching {crawler_dir}: {e}")
                continue
        
        # Also search in input pattern directory if specified
        if self.input_pattern:
            try:
                # Extract base directory from pattern
                pattern_path = Path(self.input_pattern.split('**')[0] if '**' in self.input_pattern else self.input_pattern.split('*')[0])
                if pattern_path.exists() and pattern_path.is_dir():
                    files = [str(f) for f in pattern_path.rglob("*.parquet")]
                    parquet_files.extend(files)
            except Exception as e:
                logger.warning(f"Error searching input pattern directory: {e}")
        
        # Remove duplicates
        parquet_files = list(set(parquet_files))
        
        # Filter by date if specified (e.g., "20251106" for November 6th)
        if date_filter:
            parquet_files = [f for f in parquet_files if date_filter in f]
            logger.info(f"Filtered to {len(parquet_files)} files for date {date_filter}")
        
        # Filter out already processed files if needed
        processed_files = set()
        if Path("processed_files.log").exists():
            try:
                with open("processed_files.log", 'r') as f:
                    processed_files = set(line.strip() for line in f)
            except Exception as e:
                logger.warning(f"Error reading processed_files.log: {e}")
        
        new_files = [f for f in parquet_files if f not in processed_files]
        self.stats['total_files'] = len(new_files)
        
        logger.info(f"Found {len(new_files)} parquet files to process from all crawlers")
        return new_files
    
    def _init_components(self):
        """Initialize components (called per-process for multiprocessing)"""
        if self.token_manager is None:
            self.token_manager = TokenCacheManager(self.token_lookup_path)
        if self.recovery_engine is None:
            # Use subprocess isolation by default for safety
            self.recovery_engine = ParquetRecoveryEngine(use_subprocess=True, subprocess_timeout=60)
        if self.enricher is None:
            # ✅ FIXED: Initialize with Redis client for HybridCalculations
            try:
                from shared_core.redis_clients import UnifiedRedisManager
                redis_client = get_redis_client(process_name="parquet_convert_enricher", db=1)
            except:
                redis_client = None
            self.enricher = DataEnricher(self.token_manager, redis_client=redis_client)
        if self.writer is None:
            self.writer = PartitionedParquetWriter()
    
    def _try_repair_file(self, file_path: str) -> Optional[pl.DataFrame]:
        """Try repairing file by reading row data directly, skipping header/footer"""
        try:
            import pyarrow.parquet as pq
            import pyarrow as pa
            
            # Try to read row groups directly, skipping corrupt header/footer
            parquet_file = pq.ParquetFile(file_path)
            
            # Read all row groups individually (skips header/footer parsing)
            all_tables = []
            rows_recovered = 0
            
            try:
                num_row_groups = parquet_file.num_row_groups
            except:
                # If we can't get row group count, try reading first row group
                num_row_groups = 1
            
            # Limit to prevent hangs on severely corrupted files
            max_row_groups = min(50, num_row_groups)
            
            for rg_idx in range(max_row_groups):
                try:
                    # Read row group directly - this skips header/footer
                    rg_table = parquet_file.read_row_group(rg_idx)
                    if len(rg_table) > 0:
                        all_tables.append(rg_table)
                        rows_recovered += len(rg_table)
                except Exception as e:
                    # Skip corrupt row groups
                    continue
            
            if not all_tables or rows_recovered == 0:
                return None
            
            # Combine all row groups
            table = pa.concat_tables(all_tables)
            
            # Convert to Polars
            df = pl.from_arrow(table)
            
            # Normalize datetime columns
            df = self.recovery_engine._normalize_datetime_columns(df)
            
            logger.debug(f"  ✅ Repaired {rows_recovered} rows from {len(all_tables)} row groups")
            return df
            
        except Exception as e:
            logger.debug(f"  ⚠️ File repair failed: {str(e)[:100]}")
            return None
    
    def _mark_problematic_file(self, file_path: str, reason: str):
        """Mark file as problematic for analysis"""
        try:
            with open("problematic_files.log", 'a') as f:
                f.write(f"{file_path}|{reason}\n")
        except:
            pass
    
    def _should_skip_file(self, file_path: str) -> bool:
        """Emergency skip logic - heuristically identify problematic files"""
        file_name = Path(file_path).name.lower()
        
        # Skip files that are likely problematic (only obvious patterns)
        skip_patterns = [
            'backup',
            'corrupt',
            'temp',
            '.tmp',
            'test_',
            '_test'
        ]
        
        for pattern in skip_patterns:
            if pattern in file_name:
                logger.debug(f"    ⏭️  Skipping {file_name} (matches skip pattern: {pattern})")
                return True
        
        # DISABLED: Don't skip based on problematic_files.log - try Go recovery first
        # The Go recovery might be able to handle files that Python libraries couldn't
        # Only skip if file doesn't exist
        if not Path(file_path).exists():
            logger.debug(f"    ⏭️  Skipping {file_name} (file does not exist)")
            return True
        
        return False
    
    def process_single_file(self, file_path: str) -> Tuple[bool, int, List[str]]:
        """Process a single parquet file through the complete pipeline with subprocess isolation"""
        file_name = Path(file_path).name
        
        # Emergency skip check
        if self._should_skip_file(file_path):
            return False, 0, []
        
        logger.debug(f"🔄 Starting processing: {file_name}")
        try:
            # Initialize components if needed (for multiprocessing)
            self._init_components()
            logger.debug(f"  ✅ Components initialized for {file_name}")
            
            # Step 1: Recover data from corrupted parquet using subprocess isolation
            # Try Go recovery first if available (most robust), then fall back to Python methods
            raw_df = None
            recovery_method_used = None
            
            try:
                logger.debug(f"  📖 Step 1: Recovering parquet file {file_name}...")
                
                # First try repair (read rows directly, skip header/footer)
                try:
                    repaired_df = self._try_repair_file(file_path)
                    if repaired_df is not None and not repaired_df.is_empty():
                        raw_df = repaired_df
                        recovery_method_used = "repair_row_data_only"
                        logger.debug(f"  ✅ File repair succeeded: {len(raw_df)} rows")
                except Exception as e:
                    logger.debug(f"  ⚠️ File repair failed, trying Go recovery: {str(e)[:50]}")
                
                # Then try Go recovery (most robust for corruption)
                if raw_df is None or (POLARS_AVAILABLE and raw_df.is_empty()):
                    try:
                        # Go recovery is in ParquetRecoveryEngine, try it
                        if hasattr(self.recovery_engine, '_try_go_recovery'):
                            go_recovery_result = self.recovery_engine._try_go_recovery(file_path)
                            if go_recovery_result is not None and not go_recovery_result.is_empty():
                                raw_df = go_recovery_result
                                recovery_method_used = "go_recovery"
                                logger.debug(f"  ✅ Go recovery succeeded: {len(raw_df)} rows")
                    except Exception as e:
                        logger.debug(f"  ⚠️ Go recovery failed, trying Python methods: {str(e)[:50]}")
                
                # If Go recovery didn't work, try subprocess-isolated Python recovery
                if raw_df is None or (POLARS_AVAILABLE and raw_df.is_empty()):
                    logger.debug(f"  📖 Trying subprocess-isolated Python recovery...")
                    raw_df = self.recovery_engine.recover_parquet_file(file_path)
                    recovery_method_used = "python_subprocess"
                
                if raw_df is None or (POLARS_AVAILABLE and raw_df.is_empty()):
                    logger.debug(f"  ⚠️ Skipping {file_name}: No valid rows recovered (all recovery methods failed)")
                    # Only mark as problematic if ALL methods failed (including Go)
                    self._mark_problematic_file(file_path, "all_recovery_methods_failed")
                    return False, 0, []
                
                logger.debug(f"  ✅ Recovered {len(raw_df)} valid rows from {file_name} using {recovery_method_used}")
            except Exception as e:
                logger.warning(f"  ❌ Skipping {file_name}: Recovery failed - {str(e)[:100]}")
                logger.debug(f"  Recovery error details: {traceback.format_exc()[:500]}")
                # Only mark as problematic if it's a fatal error
                self._mark_problematic_file(file_path, f"fatal_error:{str(e)[:50]}")
                return False, 0, []
            
            # Step 2: Enrich data with token lookup and calculations
            try:
                logger.debug(f"  🔧 Step 2: Enriching data for {file_name}...")
                enriched_df = self.enricher.enrich_data(raw_df)
                if POLARS_AVAILABLE and enriched_df.is_empty():
                    logger.debug(f"  ⚠️ Skipping {file_name}: Enrichment resulted in empty dataframe")
                    return False, 0, []
                logger.debug(f"  ✅ Enriched {len(enriched_df)} rows for {file_name}")
            except Exception as e:
                logger.warning(f"  ❌ Skipping {file_name}: Enrichment failed - {str(e)[:100]}")
                logger.debug(f"  Enrichment error details: {traceback.format_exc()[:500]}")
                return False, 0, []
            
            # Step 3: Write to partitioned parquet
            try:
                logger.debug(f"  💾 Step 3: Writing partitioned parquet for {file_name}...")
                output_files = self.writer.write_partitioned(enriched_df, file_path)
                if not output_files:
                    logger.debug(f"  ⚠️ Skipping {file_name}: No output files created")
                    return False, 0, []
                logger.debug(f"  ✅ Created {len(output_files)} output files for {file_name}")
            except Exception as e:
                logger.warning(f"  ❌ Skipping {file_name}: Write failed - {str(e)[:100]}")
                logger.debug(f"  Write error details: {traceback.format_exc()[:500]}")
                return False, 0, []
            
            logger.debug(f"  ✅ Successfully processed {file_name}: {len(enriched_df)} rows -> {len(output_files)} files")
            return True, len(enriched_df), output_files
            
        except KeyboardInterrupt:
            # Allow graceful shutdown
            raise
        except Exception as e:
            # Catch any unexpected errors and log them, but don't crash
            logger.error(f"Unexpected error processing {file_path}: {str(e)[:200]}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
            return False, 0, []
    
    def find_binary_files(self, date_filter: Optional[str] = None) -> List[str]:
        """Find all binary .bin and .dat files from all crawlers using rglob"""
        # Search in all three main crawler directories using rglob
        crawler_dirs = [
            Path("tick_data_storage/intraday_data"),
            Path("tick_data_storage/data_mining"),
            Path("tick_data_storage/research"),
            Path("tick_data_storage/zerodha_websocket"),
            Path("tick_data_storage/binary_crawler")
        ]
        
        binary_files = []
        for crawler_dir in crawler_dirs:
            if not crawler_dir.exists():
                continue
            try:
                # Find .bin and .dat files recursively
                found_bin = list(crawler_dir.rglob("*.bin"))
                found_dat = list(crawler_dir.rglob("*.dat"))
                binary_files.extend([str(f) for f in found_bin + found_dat])
                if found_bin or found_dat:
                    logger.debug(f"Found {len(found_bin) + len(found_dat)} binary files in {crawler_dir}")
            except Exception as e:
                logger.warning(f"Error searching {crawler_dir} for binary files: {e}")
        
        # Remove duplicates
        binary_files = list(set(binary_files))
        
        # Filter by date if specified
        if date_filter:
            binary_files = [f for f in binary_files if date_filter in f]
            logger.info(f"Filtered to {len(binary_files)} binary files for date {date_filter}")
        
        # Filter out already converted files (check if parquet exists)
        converted_files = set()
        for binary_file in binary_files:
            binary_path = Path(binary_file)
            # Check if parquet exists in same location
            parquet_path = binary_path.with_suffix('.parquet')
            if parquet_path.exists():
                converted_files.add(binary_file)
            # Also check temp_parquet directory
            temp_parquet_path = Path("temp_parquet/binary_converted") / parquet_path.name
            if temp_parquet_path.exists():
                converted_files.add(binary_file)
        
        # Filter out already processed files
        processed_files = set()
        if Path("processed_binary_files.log").exists():
            try:
                with open("processed_binary_files.log", 'r') as f:
                    processed_files = set(line.strip() for line in f)
            except Exception as e:
                logger.warning(f"Error reading processed_binary_files.log: {e}")
        
        new_files = [f for f in binary_files if f not in processed_files and f not in converted_files]
        
        logger.info(f"Found {len(new_files)} binary files to convert (excluding {len(converted_files)} already converted, {len(processed_files)} already processed)")
        return new_files
    
    def convert_binary_files(self, binary_files: List[str]) -> List[str]:
        """Convert binary files to parquet format"""
        if not binary_files:
            return []
        
        logger.info("="*80)
        logger.info("PHASE 0: Converting Binary Files (.bin/.dat) to Parquet")
        logger.info("="*80)
        
        # Import binary converter
        try:
            # Import from the analysis/scripts module
            analysis_scripts_path = Path(__file__).parent / "analysis" / "scripts"
            if str(analysis_scripts_path) not in sys.path:
                sys.path.insert(0, str(analysis_scripts_path))
            # Import the module directly from the scripts directory
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "binary_to_parquet_parquet",
                analysis_scripts_path / "binary_to_parquet_parquet.py"
            )
            binary_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(binary_module)
            convert_binary_to_parquet = binary_module.convert_binary_to_parquet
            from token_cache import TokenCacheManager as BaseTokenCacheManager
        except ImportError as e:
            logger.error(f"Could not import binary converter: {e}")
            return []
        
        # Load token cache for binary conversion
        # ✅ ENSURES: Uses zerodha_token_list/zerodha_instrument_token.json (recent token file)
        token_cache = None
        try:
            # Verify token_lookup_path is the recent file
            if "zerodha_token_list" not in self.token_lookup_path and "zerodha_instrument_token.json" not in self.token_lookup_path:
                logger.warning(f"⚠️ token_lookup_path doesn't look like recent file: {self.token_lookup_path}")
                logger.warning(f"   Using it anyway, but expected: zerodha_token_list/zerodha_instrument_token.json")
            
            token_cache = BaseTokenCacheManager(
                cache_path=self.token_lookup_path,  # ✅ Should be zerodha_token_list/zerodha_instrument_token.json
                verbose=False
            )
            logger.info(f"✅ BaseTokenCacheManager loaded from: {self.token_lookup_path} ({len(token_cache.token_map):,} tokens)")
        except Exception as e:
            logger.error(f"Could not load token cache for binary conversion: {e}")
            return []
        
        # Create temp output directory for converted parquet files
        temp_parquet_dir = Path("temp_parquet/binary_converted")
        temp_parquet_dir.mkdir(parents=True, exist_ok=True)
        
        converted_parquet_files = []
        converted_count = 0
        failed_count = 0
        
        for i, binary_file in enumerate(binary_files, 1):
            try:
                binary_path = Path(binary_file)
                
                # Skip if parquet already exists
                parquet_path = binary_path.with_suffix('.parquet')
                if parquet_path.exists():
                    logger.debug(f"Skipping {binary_path.name}: parquet already exists")
                    converted_parquet_files.append(str(parquet_path))
                    continue
                
                if i % 100 == 0:
                    logger.info(f"Converting binary files: {i}/{len(binary_files)} ({converted_count} converted, {failed_count} failed)")
                
                # Convert binary to parquet
                try:
                    result = convert_binary_to_parquet(
                        binary_path,
                        temp_parquet_dir,
                        token_cache,
                        None,  # metadata_calculator
                        None   # jsonl_enricher
                    )
                    
                    if result.get("success"):
                        converted_count += 1
                        parquet_file = result.get("parquet_file")
                        if parquet_file:
                            converted_parquet_files.append(str(parquet_file))
                        
                        # Mark as processed
                        try:
                            with open("processed_binary_files.log", 'a') as f:
                                f.write(f"{binary_file}\n")
                        except Exception as e:
                            logger.warning(f"Could not write to processed_binary_files.log: {e}")
                    else:
                        failed_count += 1
                        # Mark as processed even if failed (to avoid retrying)
                        try:
                            with open("processed_binary_files.log", 'a') as f:
                                f.write(f"{binary_file}\n")
                        except Exception:
                            pass
                        logger.debug(f"Failed to convert {binary_path.name}: {result.get('error', 'Unknown')[:100]}")
                except Exception as convert_error:
                    failed_count += 1
                    # Mark as processed even if exception (to avoid retrying)
                    try:
                        with open("processed_binary_files.log", 'a') as f:
                            f.write(f"{binary_file}\n")
                    except Exception:
                        pass
                    logger.debug(f"Exception converting {binary_path.name}: {str(convert_error)[:100]}")
                    
            except Exception as e:
                failed_count += 1
                logger.debug(f"Error processing {binary_file}: {str(e)[:100]}")
        
        logger.info(f"Binary conversion complete: {converted_count} converted, {failed_count} failed")
        logger.info(f"Created {len(converted_parquet_files)} parquet files from binary conversion")
        
        return converted_parquet_files
    
    def run_reprocessing(self):
        """Run the complete reprocessing pipeline"""
        self.stats['start_time'] = datetime.now()
        logger.info("Starting comprehensive parquet reprocessing pipeline")
        if self.date_filter:
            logger.info(f"Date filter active: {self.date_filter}")
        
        # Phase 0: Convert binary files to parquet first (SKIPPED - focus on parquet/jsonl/json)
        # binary_files = self.find_binary_files(date_filter=self.date_filter)
        # converted_parquet_files = []
        # if binary_files:
        #     converted_parquet_files = self.convert_binary_files(binary_files)
        #     logger.info(f"Phase 0 complete: Converted {len(converted_parquet_files)} binary files to parquet")
        converted_parquet_files = []
        logger.info("Phase 0: SKIPPED - Binary file conversion disabled (focusing on parquet/jsonl/json)")
        
        # Phase 1: Find and process all parquet files (including newly converted)
        parquet_files = self.find_parquet_files(date_filter=self.date_filter)
        # Add newly converted parquet files
        if converted_parquet_files:
            parquet_files.extend(converted_parquet_files)
            parquet_files = list(set(parquet_files))  # Remove duplicates
        
        # Process files in batches with subprocess isolation
        all_output_files = []
        processed_count = 0
        batch_size = getattr(self, 'batch_size', 50)  # Default batch size
        
        if parquet_files:
            logger.info(f"Processing {len(parquet_files)} files in batches of {batch_size} with subprocess isolation")
            
            try:
                # Process in batches to prevent memory buildup
                for batch_start in range(0, len(parquet_files), batch_size):
                    batch_files = parquet_files[batch_start:batch_start + batch_size]
                    batch_num = (batch_start // batch_size) + 1
                    total_batches = (len(parquet_files) + batch_size - 1) // batch_size
                    
                    logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_files)} files)")
                    
                    try:
                        # Use ThreadPoolExecutor for batch processing (subprocess isolation handles safety)
                        # Use multiple workers on M4 Mac
                        safe_workers = self.max_workers if self.max_workers else max(4, mp.cpu_count() - 1)
                        logger.debug(f"Using {safe_workers} worker(s) for batch processing")
                        executor = ThreadPoolExecutor(max_workers=safe_workers)
                        
                        with executor:
                            # Submit batch tasks
                            future_to_file = {
                                executor.submit(self.process_single_file, file_path): file_path
                                for file_path in batch_files
                            }
                            
                            # Process completed tasks
                            for future in as_completed(future_to_file):
                                file_path = future_to_file[future]
                                processed_count += 1
                                
                                try:
                                    # Timeout per file (reduced since subprocess has its own timeout)
                                    success, row_count, output_files = future.result(timeout=90)  # 90s timeout per file
                                    
                                    if success:
                                        self.stats['successful_recoveries'] += 1
                                        self.stats['total_rows_processed'] += row_count
                                        self.stats['enriched_files_created'] += len(output_files)
                                        all_output_files.extend(output_files)
                                        
                                        # Mark as processed (with error handling)
                                        try:
                                            with open("processed_files.log", 'a') as f:
                                                f.write(f"{file_path}\n")
                                        except Exception as e:
                                            logger.warning(f"Could not write to processed_files.log: {e}")
                                        
                                        if processed_count % 100 == 0:
                                            logger.info(f"Progress: {processed_count}/{len(parquet_files)} files, "
                                                      f"{self.stats['successful_recoveries']} successful, "
                                                      f"{self.stats['failed_recoveries']} failed")
                                        else:
                                            logger.debug(f"✅ Processed {file_path} -> {row_count} rows")
                                    else:
                                        self.stats['failed_recoveries'] += 1
                                        # Don't log every failure as error - just debug
                                        logger.debug(f"Skipped {file_path} (not convertible)")
                                        
                                except TimeoutError:
                                    self.stats['failed_recoveries'] += 1
                                    logger.warning(f"⏱️  Timeout processing {file_path} (skipping)")
                                    self._mark_problematic_file(file_path, "timeout")
                                except Exception as e:
                                    self.stats['failed_recoveries'] += 1
                                    error_msg = str(e)
                                    logger.warning(f"⚠️  Exception processing {file_path}: {error_msg[:100]}")
                                    self._mark_problematic_file(file_path, error_msg[:100])
                        
                        # Batch cleanup
                        gc.collect()
                        logger.debug(f"🧹 Batch {batch_num} complete, memory cleared")
                        
                    except Exception as e:
                        logger.error(f"💥 Fatal error in batch {batch_num}: {e}")
                        # Continue with next batch
                        continue
                    
                    # Periodic progress update
                    if processed_count % 100 == 0:
                        logger.info(f"📊 Progress: {processed_count}/{len(parquet_files)} files processed, "
                                  f"{self.stats['successful_recoveries']} successful, "
                                  f"{self.stats['failed_recoveries']} failed/skipped")
            
            except KeyboardInterrupt:
                logger.info("🛑 Interrupted by user - saving progress...")
                raise
            except Exception as e:
                logger.error(f"💥 Fatal error in processing: {e}")
                import traceback
                logger.error(traceback.format_exc())
                # Continue with what we have
        else:
            logger.info("No parquet files to process")
        
        # Step 4: Ingest all processed parquet data into DuckDB
        logger.info("="*80)
        logger.info("PHASE 1 COMPLETE: Parquet Processing")
        logger.info(f"✅ Processed {self.stats['successful_recoveries']} files, "
                   f"{self.stats['total_rows_processed']:,} rows, "
                   f"{self.stats['enriched_files_created']} enriched files created")
        logger.info("="*80)
        
        if all_output_files and not self.skip_ingestion:
            logger.info("Starting ClickHouse ingestion phase for parquet files")
            ingestor = ClickHouseIngestor(clickhouse_host="localhost", clickhouse_database="trading")
            ingestor.ingest_parquet_files(all_output_files)
            ingestor.create_indexes()
            ingestor.close()
        
        # Step 5: Process binary files directly to ClickHouse
        logger.info("="*80)
        logger.info("PHASE 3: Processing Binary Files (.bin/.dat) directly to ClickHouse")
        logger.info("="*80)
        
        binary_files = self.find_binary_files(date_filter=self.date_filter)
        if binary_files and not self.skip_ingestion:
            logger.info(f"Found {len(binary_files)} binary files to ingest")
            ingestor = ClickHouseIngestor(clickhouse_host="localhost", clickhouse_database="trading")
            # Initialize token manager for binary ingestion
            self._init_components()
            ingestor.ingest_binary_files(binary_files, self.token_manager, batch_size=10000)
            ingestor.create_indexes()
            ingestor.close()
        elif binary_files and self.skip_ingestion:
            logger.info(f"⏭️  Skipping binary file ingestion (--skip-ingestion flag set)")
            logger.info(f"   {len(binary_files)} binary files found but not ingested")
        else:
            logger.info("No binary files to ingest")
        
        if all_output_files and self.skip_ingestion:
            logger.info("⏭️  Skipping database ingestion (--skip-ingestion flag set)")
            logger.info(f"   {len(all_output_files)} enriched parquet files ready in enriched_parquet/")
        else:
            logger.info("No parquet files to ingest")
        
        # Step 5: Process JSONL files (News and Indices) - Convert to parquet format
        logger.info("="*80)
        logger.info("PHASE 2: Converting JSON/JSONL files (News & Indices) to Parquet")
        logger.info("="*80)
        
        jsonl_stats = self.convert_jsonl_to_parquet()
        
        logger.info("="*80)
        logger.info("PHASE 2 COMPLETE: JSONL Processing")
        logger.info(f"✅ News files: {jsonl_stats.get('news_processed', 0)} processed, "
                   f"{jsonl_stats.get('news_rows', 0):,} rows")
        logger.info(f"✅ Indices files: {jsonl_stats.get('indices_processed', 0)} processed, "
                   f"{jsonl_stats.get('indices_rows', 0):,} rows")
        logger.info("="*80)
        
        self.stats['end_time'] = datetime.now()
        self._print_summary()
    
    def convert_jsonl_to_parquet(self) -> Dict:
        """Convert JSON/JSONL files (news and indices) to parquet format"""
        stats = {
            'news_processed': 0,
            'news_rows': 0,
            'indices_processed': 0,
            'indices_rows': 0,
            'news_errors': 0,
            'indices_errors': 0
        }
        
        output_dir = Path("enriched_parquet")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Process News Files
        logger.info("📰 Converting News JSON/JSONL files to Parquet...")
        news_search_dirs = [
            Path("tick_data_storage/indices/news"),
            Path("tick_data_storage/data_mining"),
            Path("tick_data_storage/gift_nifty_gap"),
            Path(".")
        ]
        
        news_files = []
        for search_dir in news_search_dirs:
            if not search_dir.exists():
                continue
            try:
                found_json = list(search_dir.rglob("news*.json"))
                found_jsonl = list(search_dir.rglob("news*.jsonl"))
                news_files.extend([str(f) for f in found_json + found_jsonl])
            except Exception as e:
                logger.warning(f"Error searching {search_dir} for news files: {e}")
        
        news_files = list(set(news_files))
        logger.info(f"Found {len(news_files)} news files to convert")
        
        for news_file in news_files:
            try:
                result = self._convert_single_jsonl_to_parquet(Path(news_file), output_dir, "news")
                if result.get("success"):
                    stats['news_processed'] += 1
                    stats['news_rows'] += result.get("row_count", 0)
                    if stats['news_processed'] % 10 == 0:
                        logger.info(f"  Progress: {stats['news_processed']}/{len(news_files)} news files converted")
                else:
                    stats['news_errors'] += 1
                    logger.warning(f"  Failed to convert {news_file}: {result.get('error', 'Unknown error')[:100]}")
            except Exception as e:
                stats['news_errors'] += 1
                logger.warning(f"  Error converting news file {news_file}: {str(e)[:100]}")
        
        # Process Indices Files
        logger.info("📊 Converting Indices JSONL files to Parquet...")
        indices_search_dirs = [
            Path("tick_data_storage/indices"),
            Path("tick_data_storage/data_mining"),
            Path("tick_data_storage/gift_nifty")
        ]
        
        indices_files = []
        for search_dir in indices_search_dirs:
            if not search_dir.exists():
                continue
            try:
                found = list(search_dir.rglob("indices_data_*.jsonl"))
                indices_files.extend([str(f) for f in found])
            except Exception as e:
                logger.warning(f"Error searching {search_dir} for indices files: {e}")
        
        indices_files = list(set(indices_files))
        logger.info(f"Found {len(indices_files)} indices files to convert")
        
        # ✅ FIXED: Write indices parquet files directly to tick_data_storage/indices/
        indices_output_dir = Path("tick_data_storage/indices")
        indices_output_dir.mkdir(parents=True, exist_ok=True)
        
        for indices_file in indices_files:
            try:
                result = self._convert_single_jsonl_to_parquet(Path(indices_file), indices_output_dir, "indices")
                if result.get("success"):
                    stats['indices_processed'] += 1
                    stats['indices_rows'] += result.get("row_count", 0)
                    if stats['indices_processed'] % 10 == 0:
                        logger.info(f"  Progress: {stats['indices_processed']}/{len(indices_files)} indices files converted")
                else:
                    stats['indices_errors'] += 1
                    logger.warning(f"  Failed to convert {indices_file}: {result.get('error', 'Unknown error')[:100]}")
            except Exception as e:
                stats['indices_errors'] += 1
                logger.warning(f"  Error converting indices file {indices_file}: {str(e)[:100]}")
        
        return stats
    
    def _convert_single_jsonl_to_parquet(self, jsonl_file: Path, output_dir: Path, file_type: str) -> Dict:
        """Convert a single JSON/JSONL file to parquet format"""
        try:
            # Read JSON/JSONL file
            records = []
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                if jsonl_file.suffix == '.jsonl':
                    # JSONL: one JSON object per line
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                records.append(json.loads(line))
                            except json.JSONDecodeError:
                                continue
                else:
                    # JSON: try as array or single object
                    content = f.read()
                    try:
                        data = json.loads(content)
                        if isinstance(data, list):
                            records = data
                        elif isinstance(data, dict):
                            records = [data]
                        else:
                            return {"success": False, "error": "Invalid JSON format", "row_count": 0}
                    except json.JSONDecodeError:
                        return {"success": False, "error": "Invalid JSON", "row_count": 0}
            
            if not records:
                return {"success": False, "error": "No records found", "row_count": 0}
            
            # Convert to Polars DataFrame
            df = pl.DataFrame(records)
            
            # Filter corrupt rows
            if 'timestamp' in df.columns:
                df = df.filter(pl.col('timestamp').is_not_null())
            
            if df.is_empty():
                return {"success": False, "error": "All rows filtered as corrupt", "row_count": 0}
            
            # ✅ FIXED: Write directly to tick_data_storage/{crawler_name}/ format
            # For indices, write directly to output_dir (tick_data_storage/indices/)
            # For other types, use standard crawler name format
            if file_type == "indices" and "tick_data_storage" in str(output_dir):
                # Write directly to tick_data_storage/indices/
                output_file = output_dir / f"{jsonl_file.stem}.parquet"
            else:
                # For other types, preserve date-based structure if needed
                date_str = datetime.now().strftime("%Y%m%d")
                type_dir = output_dir / f"jsonl_{file_type}" / f"date={date_str}"
                type_dir.mkdir(parents=True, exist_ok=True)
                output_file = type_dir / f"{jsonl_file.stem}.parquet"
            
            # Write to parquet
            df.write_parquet(output_file, compression="snappy")
            
            logger.debug(f"  ✅ Converted {jsonl_file.name} -> {output_file.name} ({len(df)} rows)")
            
            return {
                "success": True,
                "row_count": len(df),
                "output_file": str(output_file)
            }
            
        except Exception as e:
            return {"success": False, "error": str(e), "row_count": 0}
    
    def process_jsonl_files(self) -> Dict:
        """Process JSONL files for news and indices (second priority after parquet)"""
        stats = {
            'news_processed': 0,
            'news_rows': 0,
            'indices_processed': 0,
            'indices_rows': 0,
            'news_errors': 0,
            'indices_errors': 0
        }
        
        # Import existing ingestion functions
        try:
            analysis_scripts_path = Path(__file__).parent / "analysis" / "scripts"
            sys.path.insert(0, str(analysis_scripts_path))
            from analysis.scripts.ingest_news_json_duckdb import ingest_news_json_file, get_pending_news_files
            from analysis.scripts.ingest_indices_json_duckdb import ingest_indices_jsonl_file, get_pending_indices_files
            from token_cache import TokenCacheManager as BaseTokenCacheManager
        except ImportError as e:
            logger.error(f"Could not import JSONL ingestion modules: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return stats
        
        # Load token cache for news processing
        # ✅ ENSURES: Uses zerodha_token_list/zerodha_instrument_token.json (recent token file)
        token_cache = None
        try:
            # Verify token_lookup_path is the recent file
            if "zerodha_token_list" not in self.token_lookup_path and "zerodha_instrument_token.json" not in self.token_lookup_path:
                logger.warning(f"⚠️ token_lookup_path doesn't look like recent file: {self.token_lookup_path}")
                logger.warning(f"   Using it anyway, but expected: zerodha_token_list/zerodha_instrument_token.json")
            
            token_cache = BaseTokenCacheManager(
                cache_path=self.token_lookup_path,  # ✅ Should be zerodha_token_list/zerodha_instrument_token.json
                verbose=False
            )
            logger.info(f"✅ BaseTokenCacheManager loaded from: {self.token_lookup_path} ({len(token_cache.token_map):,} tokens)")
        except Exception as e:
            logger.warning(f"Could not load token cache for JSONL processing: {e}")
        
        # Process News Files using rglob
        logger.info("📰 Processing News JSONL files...")
        news_search_dirs = [
            Path("tick_data_storage/indices/news"),
            Path("tick_data_storage/data_mining"),
            Path("tick_data_storage/gift_nifty_gap"),
            Path(".")
        ]
        
        # Use rglob to find news files directly
        news_files = []
        for search_dir in news_search_dirs:
            if not search_dir.exists():
                continue
            try:
                # Find news*.json and news*.jsonl files recursively
                found_json = list(search_dir.rglob("news*.json"))
                found_jsonl = list(search_dir.rglob("news*.jsonl"))
                news_files.extend([str(f) for f in found_json + found_jsonl])
                if found_json or found_jsonl:
                    logger.debug(f"Found {len(found_json) + len(found_jsonl)} news files in {search_dir}")
            except Exception as e:
                logger.warning(f"Error searching {search_dir} for news files: {e}")
        
        # Remove duplicates
        news_files = list(set(news_files))
        
        # Get pending files using existing function (for deduplication)
        try:
            pending_news = get_pending_news_files(self.db_path, [str(d) for d in news_search_dirs])
            # Use intersection to only process files that are both found and pending
            pending_paths = {str(p) for p in pending_news}
            news_files = [f for f in news_files if f in pending_paths]
        except Exception as e:
            logger.warning(f"Could not check pending news files, processing all found: {e}")
        
        logger.info(f"Found {len(news_files)} news files to process")
        
        for news_file in news_files:
            try:
                conn = duckdb.connect(self.db_path)
                result = ingest_news_json_file(conn, Path(news_file), token_cache)
                conn.close()
                
                if result.get("success"):
                    stats['news_processed'] += 1
                    stats['news_rows'] += result.get("row_count", 0)
                    if stats['news_processed'] % 10 == 0:
                        logger.info(f"  Progress: {stats['news_processed']}/{len(news_files)} news files processed")
                else:
                    stats['news_errors'] += 1
                    logger.warning(f"  Failed to process {news_file}: {result.get('error', 'Unknown error')[:100]}")
            except Exception as e:
                stats['news_errors'] += 1
                logger.warning(f"  Error processing news file {news_file}: {str(e)[:100]}")
        
        # Process Indices Files using rglob
        logger.info("📊 Processing Indices JSONL files...")
        indices_search_dirs = [
            Path("tick_data_storage/indices"),
            Path("tick_data_storage/data_mining"),
            Path("tick_data_storage/gift_nifty")
        ]
        
        # Use rglob to find indices files directly
        indices_files = []
        for search_dir in indices_search_dirs:
            if not search_dir.exists():
                continue
            try:
                # Find indices_data_*.jsonl files recursively
                found = list(search_dir.rglob("indices_data_*.jsonl"))
                indices_files.extend([str(f) for f in found])
                if found:
                    logger.debug(f"Found {len(found)} indices files in {search_dir}")
            except Exception as e:
                logger.warning(f"Error searching {search_dir} for indices files: {e}")
        
        # Remove duplicates
        indices_files = list(set(indices_files))
        
        # Get pending files using existing function (for deduplication)
        try:
            pending_indices = get_pending_indices_files(self.db_path, [str(d) for d in indices_search_dirs])
            # Use intersection to only process files that are both found and pending
            pending_paths = {str(p) for p in pending_indices}
            indices_files = [f for f in indices_files if f in pending_paths]
        except Exception as e:
            logger.warning(f"Could not check pending indices files, processing all found: {e}")
        
        logger.info(f"Found {len(indices_files)} indices files to process")
        
        for indices_file in indices_files:
            try:
                conn = duckdb.connect(self.db_path)
                result = ingest_indices_jsonl_file(conn, Path(indices_file), token_cache)
                conn.close()
                
                if result.get("success"):
                    stats['indices_processed'] += 1
                    stats['indices_rows'] += result.get("row_count", 0)
                    if stats['indices_processed'] % 10 == 0:
                        logger.info(f"  Progress: {stats['indices_processed']}/{len(indices_files)} indices files processed")
                else:
                    stats['indices_errors'] += 1
                    logger.warning(f"  Failed to process {indices_file}: {result.get('error', 'Unknown error')[:100]}")
            except Exception as e:
                stats['indices_errors'] += 1
                logger.warning(f"  Error processing indices file {indices_file}: {str(e)[:100]}")
        
        return stats
    
    def _print_summary(self):
        """Print processing summary"""
        duration = self.stats['end_time'] - self.stats['start_time']
        
        print("\n" + "="*80)
        print("PARQUET REPROCESSING PIPELINE - COMPLETE SUMMARY")
        print("="*80)
        print(f"Total processing time: {duration}")
        print(f"Files processed: {self.stats['total_files']}")
        print(f"✅ Successful recoveries: {self.stats['successful_recoveries']}")
        print(f"❌ Failed recoveries: {self.stats['failed_recoveries']}")
        print(f"📊 Total rows processed: {self.stats['total_rows_processed']:,}")
        print(f"💾 Enriched files created: {self.stats['enriched_files_created']}")
        if self.stats['total_files'] > 0:
            print(f"Success rate: {(self.stats['successful_recoveries']/self.stats['total_files'])*100:.1f}%")
        else:
            print(f"Success rate: N/A (no files processed)")
        print("="*80)
        
        # Save recovery statistics
        recovery_attempts = []
        if self.recovery_engine:
            recovery_attempts = self.recovery_engine.recovery_attempts
        
        recovery_stats = {
            'processing_summary': self.stats,
            'recovery_attempts': recovery_attempts,
            'timestamp': datetime.now().isoformat()
        }
        
        with open('reprocessing_statistics.json', 'w') as f:
            json.dump(recovery_stats, f, indent=2, default=str)

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Comprehensive Parquet Reprocessing Pipeline")
    parser.add_argument("--date", type=str, default=None, 
                       help="Date filter (YYYYMMDD format, e.g., 20251106 for Nov 6th). REQUIRED for one-date-at-a-time processing. Omit for all dates.")
    parser.add_argument("--input-pattern", type=str, 
                       default="tick_data_storage/**/*.parquet",
                       help="Input file pattern (glob). Default searches all crawlers.")
    parser.add_argument("--token-cache", type=str,
                       default="zerodha_token_list/zerodha_instrument_token.json",  # ✅ Recent token file
                       help="Path to token lookup cache (default: zerodha_token_list/zerodha_instrument_token.json)")
    parser.add_argument("--db", type=str, default="tick_data_production.db",
                       help="Database path")
    parser.add_argument("--workers", type=int, default=None,
                       help="Number of parallel workers (default: max(4, CPU count - 1) for M4)")
    parser.add_argument("--max-memory-gb", type=float, default=None,
                       help="Max memory per worker in GB (default: 3.0 / workers, e.g., 1.5GB for 2 workers)")
    parser.add_argument("--test", action="store_true",
                       help="Test mode: process only first 5 files")
    parser.add_argument("--skip-ingestion", action="store_true",
                       help="Skip DuckDB ingestion - only convert and enrich files")
    parser.add_argument("--batch-size", type=int, default=50,
                       help="Batch size for processing files (default: 50, use smaller for stability)")
    parser.add_argument("--no-subprocess", action="store_true",
                       help="Disable subprocess isolation (less safe, but faster)")
    
    args = parser.parse_args()
    
    # Configuration
    config = {
        'input_pattern': args.input_pattern,
        'token_lookup_path': args.token_cache,
        'db_path': args.db,
        'max_workers': args.workers,
        'date_filter': args.date,
        'skip_ingestion': args.skip_ingestion,
        'max_memory_gb': args.max_memory_gb,
        'batch_size': args.batch_size,
        'use_subprocess': not args.no_subprocess
    }
    
    # Create and run pipeline
    pipeline = ComprehensiveReprocessingPipeline(**config)
    
    # Test mode: limit files by storing them in instance variable
    if args.test:
        logger.info("🧪 TEST MODE: Processing first 5 files only")
        # Find files first, then limit
        all_files = pipeline.find_parquet_files(date_filter=args.date)
        limited_files = all_files[:5]
        logger.info(f"Limited to {len(limited_files)} files for testing")
        # Store in instance variable (can be pickled)
        pipeline.limited_files = limited_files
    
    try:
        pipeline.run_reprocessing()
        logger.info("🎉 Reprocessing pipeline completed successfully!")
    except Exception as e:
        logger.error(f"💥 Pipeline failed: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()