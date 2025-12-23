# shared_core/config_utils/timestamp_normalizer.py
"""
Timestamp Normalizer Utility

Centralized timestamp normalization for all services.
Converts various timestamp formats to epoch milliseconds.
"""
import datetime
from typing import Union, Optional, List

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None

class TimestampNormalizer:
    """
    ONE-TIME FIX: Convert all timestamp formats to epoch milliseconds
    Supports both single value conversion and Polars DataFrame operations
    """
    
    IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    
    @staticmethod
    def _ensure_timezone(dt: datetime.datetime) -> datetime.datetime:
        """Treat naive timestamps as IST to avoid timezone drift on non-IST hosts."""
        if dt.tzinfo is None:
            return dt.replace(tzinfo=TimestampNormalizer.IST)
        return dt
    
    @staticmethod
    def to_epoch_ms(timestamp: Union[str, int, float, datetime.datetime]) -> int:
        """Convert ANY timestamp format to epoch milliseconds"""
        if timestamp is None:
            return int(datetime.datetime.now().timestamp() * 1000)
        
        # Already epoch milliseconds
        if isinstance(timestamp, int) and timestamp > 1000000000000:
            return timestamp
        
        # Already epoch seconds  
        if isinstance(timestamp, (int, float)) and timestamp < 1000000000000:
            return int(timestamp * 1000)
        
        # String formats
        if isinstance(timestamp, str):
            # Zerodha format: "2025-10-17T09:25:30Z"
            if timestamp.endswith('Z'):
                dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1000)
            
            # Exchange format: "2025-10-17 09:25:30.123456" or "2025-11-10 09:22:55"
            if ':' in timestamp:
                # Try format with microseconds: "2025-10-17 09:25:30.123456"
                if '.' in timestamp:
                    parts = timestamp.split('.')
                    try:
                        dt = datetime.datetime.fromisoformat(parts[0])
                        dt = TimestampNormalizer._ensure_timezone(dt)
                        microseconds = int(parts[1][:6].ljust(6, '0'))
                        dt = dt.replace(microsecond=microseconds)
                        return int(dt.timestamp() * 1000)
                    except (ValueError, IndexError):
                        pass
                # Try format without microseconds: "2025-11-10 09:22:55"
                try:
                    dt = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                    dt = TimestampNormalizer._ensure_timezone(dt)
                    return int(dt.timestamp() * 1000)
                except ValueError:
                    pass
            
            # Slash format: "17/10/2025 09:25:30"
            if '/' in timestamp:
                try:
                    dt = datetime.datetime.strptime(timestamp, "%d/%m/%Y %H:%M:%S")
                    dt = TimestampNormalizer._ensure_timezone(dt)
                    return int(dt.timestamp() * 1000)
                except ValueError:
                    pass
            
            # Try ISO format as last resort
            try:
                dt = datetime.datetime.fromisoformat(timestamp)
                dt = TimestampNormalizer._ensure_timezone(dt)
                return int(dt.timestamp() * 1000)
            except ValueError:
                pass
        
        # datetime object
        if isinstance(timestamp, datetime.datetime):
            dt = TimestampNormalizer._ensure_timezone(timestamp)
            return int(dt.timestamp() * 1000)
        
        # Fallback to current time
        return int(datetime.datetime.now().timestamp() * 1000)

    @staticmethod
    def to_iso_string(epoch_ms: int, tz: datetime.timezone = None) -> str:
        """Convert epoch milliseconds to ISO string in the desired timezone (default IST)."""
        tz = tz or TimestampNormalizer.IST
        dt = datetime.datetime.fromtimestamp(epoch_ms / 1000.0, tz=datetime.timezone.utc)
        return dt.astimezone(tz).isoformat()
    
    @staticmethod
    def normalize_dataframe_timestamps(df: 'pl.DataFrame', 
                                      timestamp_columns: Optional[List[str]] = None,
                                      output_column: str = 'timestamp_normalized') -> 'pl.DataFrame':
        """
        Normalize timestamp columns in a Polars DataFrame using Polars native methods.
        Similar to PolarsCache._create_normalized_timestamp() but as a standalone utility.
        
        Args:
            df: Polars DataFrame
            timestamp_columns: List of timestamp column names to normalize. If None, auto-detect.
            output_column: Name of the output normalized timestamp column
            
        Returns:
            DataFrame with normalized timestamp column added
        """
        if not POLARS_AVAILABLE:
            raise ImportError("polars is required for normalize_dataframe_timestamps")
        
        if df.is_empty():
            return df
        
        timestamp_exprs = []
        
        # Auto-detect timestamp columns if not provided
        if timestamp_columns is None:
            timestamp_columns = [
                col for col in df.columns 
                if any(keyword in col.lower() for keyword in ['timestamp', 'time', 'date'])
            ]
        
        # Priority-based timestamp conversion (same logic as PolarsCache)
        if 'exchange_timestamp' in df.columns:
            dtype = df['exchange_timestamp'].dtype
            if dtype == pl.Utf8:
                # ISO string - parse it
                timestamp_exprs.append(
                    pl.col('exchange_timestamp')
                    .str.strptime(pl.Datetime(time_unit="ms"), format=None)
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
                pl.col('exchange_timestamp_ms')
                .cast(pl.Datetime(time_unit="ms"))
                .alias('exchange_timestamp_ms_norm')
            )
        
        if 'timestamp_ms' in df.columns:
            timestamp_exprs.append(
                pl.col('timestamp_ms')
                .cast(pl.Datetime(time_unit="ms"))
                .alias('timestamp_ms_norm')
            )
        
        if 'timestamp' in df.columns:
            dtype = df['timestamp'].dtype
            if dtype == pl.Utf8:
                # ISO string - parse it
                timestamp_exprs.append(
                    pl.col('timestamp')
                    .str.strptime(pl.Datetime(time_unit="ms"), format=None)
                    .alias('timestamp_norm')
                )
            elif dtype in [pl.Int64, pl.Int32]:
                # Epoch milliseconds - convert using from_epoch
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('timestamp'), time_unit="ms")
                    .alias('timestamp_norm')
                )
        
        if 'exchange_timestamp_epoch' in df.columns:
            timestamp_exprs.append(
                pl.col('exchange_timestamp_epoch')
                .cast(pl.Int64)
                .cast(pl.Datetime(time_unit="s"))
                .alias('epoch_norm')
            )
        
        if 'processed_timestamp' in df.columns:
            timestamp_exprs.append(
                (pl.col('processed_timestamp') * 1000)
                .cast(pl.Int64)
                .cast(pl.Datetime(time_unit="ms"))
                .alias('processed_norm')
            )
        
        # Add all normalized columns
        if timestamp_exprs:
            df = df.with_columns(timestamp_exprs)
            
            # Create master normalized timestamp using coalesce
            norm_cols = [col for col in df.columns if col.endswith('_norm')]
            if norm_cols:
                df = df.with_columns(
                    pl.coalesce(norm_cols).alias(output_column)
                )
        
        return df
    
    @staticmethod
    def convert_to_epoch_ms_column(df: 'pl.DataFrame', 
                                   timestamp_col: str,
                                   output_col: Optional[str] = None) -> 'pl.DataFrame':
        """
        Convert a timestamp column to epoch milliseconds (Int64).
        Useful when you need epoch milliseconds instead of Datetime.
        
        Args:
            df: Polars DataFrame
            timestamp_col: Name of the timestamp column to convert
            output_col: Name of output column (default: {timestamp_col}_epoch_ms)
            
        Returns:
            DataFrame with new epoch milliseconds column
        """
        if not POLARS_AVAILABLE:
            raise ImportError("polars is required for convert_to_epoch_ms_column")
        
        if timestamp_col not in df.columns:
            return df
        
        if output_col is None:
            output_col = f"{timestamp_col}_epoch_ms"
        
        dtype = df[timestamp_col].dtype
        
        if str(dtype).startswith('Datetime'):
            # Datetime -> epoch milliseconds
            df = df.with_columns(
                pl.col(timestamp_col).dt.timestamp('ms').cast(pl.Int64).alias(output_col)
            )
        elif dtype == pl.Utf8:
            # String -> parse to Datetime -> epoch milliseconds
            df = df.with_columns(
                pl.col(timestamp_col)
                .str.strptime(pl.Datetime(time_unit="ms"), format=None)
                .dt.timestamp('ms')
                .cast(pl.Int64)
                .alias(output_col)
            )
        elif dtype in [pl.Int64, pl.Int32]:
            # Already epoch (assume milliseconds)
            df = df.with_columns(
                pl.col(timestamp_col).cast(pl.Int64).alias(output_col)
            )
        
        return df

# USAGE: One-time normalization when storing data
# normalized_ts = TimestampNormalizer.to_epoch_ms("2025-10-17T09:25:30Z")
# Result: 1737011130000 (ALWAYS epoch milliseconds)

# USAGE: Polars DataFrame normalization
# df_normalized = TimestampNormalizer.normalize_dataframe_timestamps(df)
# df_with_epoch = TimestampNormalizer.convert_to_epoch_ms_column(df, 'timestamp')

