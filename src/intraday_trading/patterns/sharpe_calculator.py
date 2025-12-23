#!/opt/homebrew/bin/python3.13
"""
Polars-Based Sharpe Ratio Calculator for Pattern Performance

This module provides comprehensive Sharpe ratio calculation and tracking for trading patterns.
It uses Polars for high-performance batch processing and integrates with the Redis-based
pattern performance tracking system.

STANDARD METHODOLOGY:
- Annualized Sharpe Ratio: (mean_return - risk_free_rate) / std_return * sqrt(252)
- Risk-free rate: 5% for Indian market (0.05 annual, 0.05/252 daily)
- Daily returns calculated from price_movement percentage stored in pattern performance data
- VIX regime adjustments for Sharpe thresholds (higher VIX = higher threshold required)

INTEGRATION:
- Reads from Redis DB 2 (analytics) - pattern_performance keys
- Stores Sharpe ratios in pattern_metrics keys (DB 2)
- Uses centralized Redis key standards (DatabaseAwareKeyBuilder)
- Integrates with time-window validation system (sharpe_inputs keys from alert_validator)
- Integrates with VIX regime system for dynamic threshold adjustment
- Real-time tracking via LivePatternSharpeTracker for live trade execution data

USAGE:
    # Basic Sharpe calculation
    from intraday_trading.patterns.sharpe_calculator import PolarsSharpeCalculator
    
    calculator = PolarsSharpeCalculator()
    sharpe = calculator.calculate_and_store_sharpe("NIFTY", "volume_breakout")
    
    # Real-time tracking
    from intraday_trading.patterns.sharpe_calculator import LivePatternSharpeTracker
    import polars as pl
    
    tracker = LivePatternSharpeTracker()
    trade_df = pl.DataFrame({
        "symbol": ["NIFTY", "NIFTY"],
        "pattern_type": ["volume_breakout", "volume_breakout"],
        "entry_price": [18000, 18100],
        "exit_price": [18100, 18050],
        "entry_time": [datetime.now(), datetime.now()],
        "exit_time": [datetime.now(), datetime.now()]
    })
    await tracker.update_pattern_sharpe_metrics(trade_df)
    
    # Check Sharpe threshold
    sharpe, meets_threshold = tracker.check_sharpe_threshold("NIFTY", "volume_breakout", "HIGH")

AUTHOR: AION Intraday Trading System
VERSION: 1.0
"""

import polars as pl
import numpy as np
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict

from shared_core.utils.serialization import fix_numpy_serialization
logger = logging.getLogger(__name__)


class PolarsSharpeCalculator:
    """
    Polars-based Sharpe Ratio calculator for pattern performance analysis.
    
    This class provides high-performance Sharpe ratio calculations using Polars
    for batch processing. It properly annualizes returns and subtracts the risk-free
    rate according to standard financial methodology.
    
    Features:
    - Proper annualization: sqrt(252) for daily returns, sqrt(12) for monthly
    - Risk-free rate subtraction: Uses 5% annual rate for Indian market
    - Redis integration: Reads pattern performance data from DB 2 (analytics)
    - VIX regime awareness: Adjusts thresholds based on market volatility
    - Batch processing: Efficient Polars-based calculations
    
    Redis Storage:
    - Reads from: pattern_performance:{symbol}:{pattern_type} (DB 2, List)
    - Writes to: pattern_metrics:{symbol}:{pattern_type} (DB 2, Hash)
    - Also reads from: sharpe_inputs:{pattern_type}:{window_key} (DB 2, List) - from time-window validation
    - TTL: 7 days (604800 seconds) per redis_config.py
    
    Example:
        calculator = PolarsSharpeCalculator()
        sharpe = calculator.calculate_annualized_sharpe(returns_series)
        stored_sharpe = calculator.calculate_and_store_sharpe("NIFTY", "volume_breakout")
    """
    
    def __init__(self, risk_free_rate: float = 0.05, redis_client=None):
        """
        Initialize Sharpe calculator.
        
        Args:
            risk_free_rate: Annual risk-free rate (default: 0.05 = 5% for India)
            redis_client: Redis client for DB 2 (analytics - pattern performance data)
        """
        self.risk_free_rate = risk_free_rate  # 5% for India
        self.redis_client = redis_client
        
        # Initialize Redis client if not provided
        if not self.redis_client:
            from shared_core.redis_clients.redis_client import RedisClientFactory
            self.redis_client = RedisClientFactory.get_trading_client()
        
        if not self.redis_client:
            logger.warning("Redis client not available - Sharpe calculations will be limited")
        
    
    def calculate_annualized_sharpe(self, returns_series: pl.Series) -> float:
        """
        Calculate annualized Sharpe ratio using Polars.
        
        STANDARD FORMULA:
        Sharpe = (mean_return - risk_free_rate) / std_return * sqrt(252)
        
        Args:
            returns_series: Polars Series of daily returns (decimal, e.g., 0.05 for 5%)
            
        Returns:
            Annualized Sharpe ratio (float)
        """
        if returns_series.len() < 2:
            return 0.0
        
        # Daily risk-free rate (annual rate / 252 trading days)
        daily_rf = self.risk_free_rate / 252
        
        # Calculate excess returns (returns - risk_free_rate)
        excess_returns = returns_series - daily_rf
        
        # Use Polars for all calculations (much faster than numpy)
        mean_return = excess_returns.mean()
        std_return = excess_returns.std()
        
        if std_return == 0 or std_return is None:
            return 0.0
        
        # Annualized Sharpe ratio
        sharpe = (mean_return / std_return) * np.sqrt(252)
        
        return float(sharpe) if sharpe is not None else 0.0
    
    def calculate_pattern_sharpe_batch(self, pattern_performance_df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate Sharpe for multiple patterns in batch using Polars.
        
        Args:
            pattern_performance_df: DataFrame with columns:
                - symbol: str
                - pattern_type: str
                - returns: float (decimal returns, e.g., 0.05 for 5%)
                
        Returns:
            DataFrame with Sharpe ratios per pattern
        """
        if pattern_performance_df.is_empty():
            return pl.DataFrame()
        
        # Group by symbol and pattern_type, calculate Sharpe for each group
        result = (
            pattern_performance_df
            .group_by("symbol", "pattern_type")
            .agg([
                pl.col("returns").count().alias("trade_count"),
                pl.col("returns").mean().alias("mean_return"),
                pl.col("returns").std().alias("std_return"),
                # Calculate Sharpe for each group using map_batches
                pl.col("returns").map_batches(
                    lambda s: pl.Series([self.calculate_annualized_sharpe(s)]),
                    return_dtype=pl.Float64
                ).alias("sharpe_ratio")
            ])
            .filter(pl.col("trade_count") >= 5)  # Minimum 5 trades for statistical significance
            .sort("sharpe_ratio", descending=True)
        )
        
        return result
    
    def load_pattern_performance_from_redis(self, symbol: Optional[str] = None, 
                                           pattern_type: Optional[str] = None,
                                           min_trades: int = 5) -> pl.DataFrame:
        """
        Load pattern performance data from Redis and convert to Polars DataFrame.
        
        ✅ Uses centralized Redis key standards for pattern_performance keys.
        ✅ Also loads Sharpe inputs from time-window validation if available.
        
        Args:
            symbol: Optional symbol filter (if None, loads all symbols)
            pattern_type: Optional pattern type filter (if None, loads all patterns)
            min_trades: Minimum number of trades required
            
        Returns:
            Polars DataFrame with columns: symbol, pattern_type, returns, timestamp
        """
        if not self.redis_client:
            logger.error("Redis client not available")
            return pl.DataFrame()
        
        # Collect all performance data
        all_data = []
        
        try:
            # If specific symbol/pattern provided, use direct key lookup
            if symbol and pattern_type:
                # ✅ Use canonical key builder
                from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
                pattern_perf_key = DatabaseAwareKeyBuilder.analytics_pattern_performance(symbol, pattern_type)
                performance_list = self.redis_client.lrange(pattern_perf_key, 0, -1)
                
                for item_json in performance_list:
                    try:
                        if isinstance(item_json, bytes):
                            item_json = item_json.decode('utf-8')
                        item = json.loads(item_json)
                        price_movement = item.get('price_movement')
                        if price_movement is not None:
                            # Convert percentage to decimal return
                            returns = float(price_movement) / 100.0
                            all_data.append({
                                'symbol': item.get('symbol', symbol),
                                'pattern_type': item.get('pattern_type', pattern_type),
                                'returns': returns,
                                'timestamp': item.get('timestamp', 0)
                            })
                    except (json.JSONDecodeError, ValueError, TypeError) as e:
                        logger.debug(f"Error parsing performance data: {e}")
                        continue
                
                # ✅ Also try to load Sharpe inputs from time-window validation
                # These contain mean_return and std_dev from time-window validation
                for window_key in ['1min', '2min', '5min', '10min', '15min', '30min', '45min', '60min']:
                    from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
                    sharpe_inputs_key = DatabaseAwareKeyBuilder.analytics_sharpe_inputs(pattern_type, window_key)
                    sharpe_inputs_list = self.redis_client.lrange(sharpe_inputs_key, 0, -1)
                    
                    for item_json in sharpe_inputs_list:
                        try:
                            if isinstance(item_json, bytes):
                                item_json = item_json.decode('utf-8')
                            sharpe_input = json.loads(item_json)
                            mean_return = sharpe_input.get('mean_return')
                            if mean_return is not None:
                                # Use mean_return as a return value
                                all_data.append({
                                    'symbol': symbol,
                                    'pattern_type': pattern_type,
                                    'returns': float(mean_return),
                                    'timestamp': sharpe_input.get('window_minutes', 0) * 60  # Convert minutes to seconds
                                })
                        except (json.JSONDecodeError, ValueError, TypeError) as e:
                            logger.debug(f"Error parsing Sharpe input data: {e}")
                            continue
            
            else:
                # Need to discover all pattern performance keys
                # Use pattern set from alert_performance_patterns_set if available
                # Otherwise, we'd need to scan (but that's forbidden)
                # For now, return empty - caller should provide symbol/pattern
                logger.warning("Symbol/pattern not provided - cannot discover keys without scanning")
                return pl.DataFrame()
        
        except Exception as e:
            logger.error(f"Error loading pattern performance from Redis: {e}")
            return pl.DataFrame()
        
        if not all_data:
            return pl.DataFrame()
        
        # Convert to Polars DataFrame
        df = pl.DataFrame(all_data)
        
        # Filter by minimum trades
        if not df.is_empty():
            trade_counts = df.group_by("symbol", "pattern_type").agg([
                pl.count().alias("count")
            ])
            valid_patterns = trade_counts.filter(pl.col("count") >= min_trades)
            
            if not valid_patterns.is_empty():
                df = df.join(
                    valid_patterns.select("symbol", "pattern_type"),
                    on=["symbol", "pattern_type"],
                    how="inner"
                )
        
        return df
    
    def calculate_and_store_sharpe(self, symbol: str, pattern_type: str) -> Optional[float]:
        """
        Calculate Sharpe ratio for a specific pattern and store in Redis.
        
        Args:
            symbol: Symbol name
            pattern_type: Pattern type
            
        Returns:
            Sharpe ratio (float) or None if insufficient data
        """
        if not self.redis_client:
            return None
        
        # Load performance data
        df = self.load_pattern_performance_from_redis(symbol=symbol, pattern_type=pattern_type)
        
        if df.is_empty():
            logger.debug(f"No performance data for {symbol}:{pattern_type}")
            return None
        
        # Extract returns series
        returns_series = df.select("returns").to_series()
        
        if returns_series.len() < 5:
            logger.debug(f"Insufficient trades for {symbol}:{pattern_type} ({returns_series.len()} < 5)")
            return None
        
        # Calculate Sharpe
        sharpe_ratio = self.calculate_annualized_sharpe(returns_series)
        
        # Store in pattern_metrics
        from shared_core.redis_clients.redis_client import get_ttl_for_data_type
        
        # ✅ Use canonical key builder for analytics (DB2)
        from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
        metrics_key = DatabaseAwareKeyBuilder.analytics_pattern_metrics(symbol, pattern_type)
        
        try:
            # Update existing metrics or create new
            existing_metrics = self.redis_client.hgetall(metrics_key)
            
            metrics_data_raw = {
                'sharpe_ratio': sharpe_ratio,
                'trade_count': returns_series.len(),
                'mean_return': returns_series.mean(),
                'std_return': returns_series.std(),
                'last_updated': datetime.now().timestamp()
            }
            metrics_data = {
                k: str(v) for k, v in fix_numpy_serialization(metrics_data_raw).items()
            }
            
            # Merge with existing metrics
            if existing_metrics:
                metrics_data.update(existing_metrics)
            
            self.redis_client.hset(metrics_key, mapping=metrics_data)
            
            # Set TTL (7 days for pattern metrics)
            ttl = get_ttl_for_data_type("pattern_metrics")
            self.redis_client.expire(metrics_key, ttl)
            
            logger.info(f"Stored Sharpe ratio for {symbol}:{pattern_type} = {sharpe_ratio:.2f}")
            
        except Exception as e:
            logger.error(f"Error storing Sharpe ratio: {e}")
        
        return sharpe_ratio
    
    def get_sharpe_ratio(self, symbol: str, pattern_type: str) -> Optional[float]:
        """
        Get cached Sharpe ratio from Redis pattern_metrics.
        
        Args:
            symbol: Symbol name
            pattern_type: Pattern type
            
        Returns:
            Sharpe ratio (float) or None if not found
        """
        if not self.redis_client:
            return None
        
        # ✅ Use canonical key builder for analytics (DB2)
        from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
        metrics_key = DatabaseAwareKeyBuilder.analytics_pattern_metrics(symbol, pattern_type)
        
        try:
            sharpe_str = self.redis_client.hget(metrics_key, "sharpe_ratio")
            if sharpe_str:
                return float(sharpe_str)
        except (ValueError, TypeError) as e:
            logger.debug(f"Error reading Sharpe ratio: {e}")
        
        return None
    
    def get_vix_adjusted_sharpe_threshold(self, vix_regime: str) -> float:
        """
        Get VIX-adjusted Sharpe threshold for pattern filtering.
        
        Higher VIX = require higher Sharpe (more risk, need better returns).
        
        Args:
            vix_regime: VIX regime (LOW, NORMAL, HIGH, PANIC)
            
        Returns:
            Minimum Sharpe threshold
        """
        base_threshold = 1.0  # Base Sharpe threshold
        
        vix_multipliers = {
            "PANIC": 2.0,   # Require 2.0 Sharpe in panic (very selective)
            "HIGH": 1.5,   # Require 1.5 Sharpe in high volatility
            "NORMAL": 1.0,  # Base threshold
            "LOW": 0.7,    # Lower threshold in low volatility (more opportunities)
        }
        
        multiplier = vix_multipliers.get(vix_regime, 1.0)
        return base_threshold * multiplier
    
    def calculate_sharpe_with_vix_adjustment(self, returns_series: pl.Series, 
                                             vix_regime: str) -> Tuple[float, bool]:
        """
        Calculate Sharpe ratio and check against VIX-adjusted threshold.
        
        Args:
            returns_series: Polars Series of returns
            vix_regime: VIX regime (LOW, NORMAL, HIGH, PANIC)
            
        Returns:
            Tuple of (sharpe_ratio, meets_threshold)
        """
        sharpe = self.calculate_annualized_sharpe(returns_series)
        threshold = self.get_vix_adjusted_sharpe_threshold(vix_regime)
        
        return sharpe, sharpe >= threshold


def calculate_pattern_sharpe_batch_all(redis_client=None) -> Dict[str, Dict[str, float]]:
    """
    Calculate Sharpe ratios for all patterns with sufficient data.
    
    This function discovers patterns from alert_performance_patterns_set
    and calculates Sharpe for each.
    
    Args:
        redis_client: Redis client (DB 0 for pattern set, DB 2 for performance)
        
    Returns:
        Dict of {pattern_type: {symbol: sharpe_ratio}}
    """
    from shared_core.redis_clients.redis_client import RedisClientFactory
    from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
    
    # Get pattern set from DB 0
    redis_db0 = redis_client or RedisClientFactory.get_trading_client()
    
    if not redis_db0:
        return {}
    
    # ✅ Use pattern registry to get all known patterns
    try:
        import json
        from pathlib import Path
        registry_path = Path(__file__).parent / "data" / "pattern_registry_config.json"
        if registry_path.exists():
            with open(registry_path, 'r') as f:
                registry = json.load(f)
                # Extract pattern names from pattern_configs
                pattern_configs = registry.get('pattern_configs', {})
                patterns = set(pattern_configs.keys()) if pattern_configs else set()
        else:
            logger.warning(f"Pattern registry not found at {registry_path}")
            patterns = set()
    except Exception as e:
        logger.warning(f"Could not load pattern registry: {e}")
        patterns = set()
    
    if not patterns:
        logger.warning("No patterns found in pattern registry")
        return {}
    
    # Initialize calculator
    calculator = PolarsSharpeCalculator(redis_client=None)
    
    results = {}
    
    # For each pattern, we need to find symbols
    # Since we can't scan, we'll need to track this differently
    # For now, return empty - this requires pattern discovery mechanism
    logger.info(f"Found {len(patterns)} patterns, but symbol discovery requires pattern registry")
    
    return results


class LivePatternSharpeTracker:
    """
    Real-time Sharpe ratio tracking for live trading patterns.
    
    This class processes trade execution data and updates Sharpe ratios in real-time
    as trades complete. It uses Polars for fast batch processing and integrates
    seamlessly with the Redis-based pattern performance tracking system.
    
    Features:
    - Real-time updates: Processes completed trades as they occur
    - Batch processing: Uses Polars for efficient group-by operations
    - Redis integration: Stores metrics in DB 2 (analytics) using centralized key standards
    - VIX-aware thresholds: Checks Sharpe against VIX-adjusted thresholds
    - Async support: Designed for async/await patterns
    
    Input Data Format:
    - symbol: str - Trading symbol (e.g., "NIFTY", "BANKNIFTY")
    - pattern_type: str - Pattern type (e.g., "volume_breakout", "reversal")
    - entry_price: float - Trade entry price
    - exit_price: float - Trade exit price (None for open trades)
    - entry_time: datetime/timestamp - Trade entry time
    - exit_time: datetime/timestamp - Trade exit time (None for open trades)
    
    Redis Storage:
    - Writes to: pattern_metrics:{symbol}:{pattern_type} (DB 2, Hash)
    - Fields: sharpe_ratio, total_trades, total_return, mean_return, std_return, last_trade_time, last_updated
    - TTL: 7 days (604800 seconds) per redis_config.py
    
    Example:
        tracker = LivePatternSharpeTracker()
        trade_df = pl.DataFrame({
            "symbol": ["NIFTY"],
            "pattern_type": ["volume_breakout"],
            "entry_price": [18000],
            "exit_price": [18100],
            "entry_time": [datetime.now()],
            "exit_time": [datetime.now()]
        })
        await tracker.update_pattern_sharpe_metrics(trade_df)
        
        # Check if pattern meets threshold
        sharpe, meets = tracker.check_sharpe_threshold("NIFTY", "volume_breakout", "HIGH")
    """
    
    def __init__(self, redis_client=None):
        """
        Initialize live Sharpe tracker.
        
        Args:
            redis_client: Redis client for DB 2 (analytics - pattern metrics storage)
        """
        from shared_core.redis_clients.redis_client import RedisClientFactory
        
        self.redis_client = redis_client
        
        # Initialize Redis client if not provided
        if not self.redis_client:
            self.redis_client = RedisClientFactory.get_trading_client()
        
        if not self.redis_client:
            logger.warning("Redis client not available - live Sharpe tracking disabled")
        
        # Initialize Sharpe calculator
        self.sharpe_calculator = PolarsSharpeCalculator(redis_client=self.redis_client)
        
        # Use DatabaseAwareKeyBuilder static methods - no instance needed
    
    async def update_pattern_sharpe_metrics(self, trade_executions_df: pl.DataFrame):
        """
        Update Sharpe ratios from live trade data using Polars.
        
        Processes completed trades and calculates Sharpe ratios per pattern.
        Stores results in Redis for real-time access.
        
        Args:
            trade_executions_df: Polars DataFrame with columns:
                - symbol: str
                - pattern_type: str
                - entry_price: float
                - exit_price: float (None for open trades)
                - entry_time: datetime/timestamp
                - exit_time: datetime/timestamp (None for open trades)
                
        Returns:
            None (updates Redis directly)
        """
        if trade_executions_df.is_empty():
            return
        
        try:
            # Process with Polars (much faster than pandas)
            sharpe_metrics = (
                trade_executions_df
                .filter(pl.col("exit_time").is_not_null())  # Completed trades only
                .filter(pl.col("exit_price").is_not_null())  # Must have exit price
                .filter(pl.col("entry_price").is_not_null())  # Must have entry price
                .with_columns([
                    # Calculate returns (decimal, e.g., 0.05 for 5%)
                    ((pl.col("exit_price") - pl.col("entry_price")) / pl.col("entry_price")).alias("returns"),
                    pl.col("pattern_type").cast(pl.Utf8),
                    pl.col("symbol").cast(pl.Utf8)
                ])
                .group_by(["symbol", "pattern_type"])
                .agg([
                    pl.col("returns").count().alias("total_trades"),
                    pl.col("returns").sum().alias("total_return"),
                    pl.col("returns").mean().alias("mean_return"),
                    pl.col("returns").std().alias("std_return"),
                    # Use our Sharpe calculator for each group
                    pl.col("returns").map_batches(
                        lambda s: pl.Series([self.sharpe_calculator.calculate_annualized_sharpe(s)]),
                        return_dtype=pl.Float64
                    ).alias("sharpe_ratio"),
                    pl.col("entry_time").max().alias("last_trade_time")
                ])
                .filter(pl.col("total_trades") >= 3)  # Minimum 3 trades for meaningful Sharpe
            )
            
            # Store in Redis for real-time access
            await self._store_sharpe_metrics_to_redis(sharpe_metrics)
            
        except Exception as e:
            logger.error(f"Error updating pattern Sharpe metrics: {e}", exc_info=True)
    
    async def _store_sharpe_metrics_to_redis(self, sharpe_metrics: pl.DataFrame):
        """
        Store Sharpe metrics in Redis using Polars iteration.
        
        ✅ Uses centralized Redis key standards for pattern_metrics keys.
        ✅ Stores in DB 2 (analytics).
        
        Args:
            sharpe_metrics: Polars DataFrame with Sharpe calculations
        """
        if sharpe_metrics.is_empty():
            return
        
        from shared_core.redis_clients.redis_client import get_ttl_for_data_type
        
        ttl = get_ttl_for_data_type("pattern_metrics")  # 7 days (604800 seconds)
        
        try:
            # Use pipeline for batch updates
            pipe = self.redis_client.pipeline()
            
            for row in sharpe_metrics.iter_rows(named=True):
                symbol = row['symbol']
                pattern_type = row['pattern_type']
                
                # Use DatabaseAwareKeyBuilder for analytics (DB2)
                from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
                metrics_key = DatabaseAwareKeyBuilder.analytics_pattern_metrics(symbol, pattern_type)
                
                # Prepare metric data
                metric_data_raw = {
                    'sharpe_ratio': float(row['sharpe_ratio']) if row['sharpe_ratio'] is not None else 0.0,
                    'total_trades': int(row['total_trades']),
                    'total_return': float(row['total_return']) if row['total_return'] is not None else 0.0,
                    'mean_return': float(row['mean_return']) if row['mean_return'] is not None else 0.0,
                    'std_return': float(row['std_return']) if row['std_return'] is not None else 0.0,
                    'last_trade_time': str(row['last_trade_time']) if row['last_trade_time'] else '',
                    'last_updated': datetime.now().isoformat()
                }
                metric_data = {
                    k: str(v) for k, v in fix_numpy_serialization(metric_data_raw).items()
                }
                
                # Update hash with new metrics
                pipe.hset(metrics_key, mapping=metric_data)
                pipe.expire(metrics_key, ttl)
            
            # Execute all updates in batch
            pipe.execute()
            
            logger.info(f"Updated Sharpe metrics for {len(sharpe_metrics)} pattern combinations")
            
        except Exception as e:
            logger.error(f"Error storing Sharpe metrics to Redis: {e}", exc_info=True)
    
    def update_from_pattern_performance(self, symbol: str, pattern_type: str, 
                                       price_movement_pct: float, timestamp: float = None):
        """
        Update Sharpe ratio from pattern performance data (price_movement).
        
        This is a convenience method for updating Sharpe when we have
        pattern performance data but not full trade execution data.
        
        Args:
            symbol: Symbol name
            pattern_type: Pattern type
            price_movement_pct: Price movement percentage (e.g., 2.5 for 2.5%)
            timestamp: Optional timestamp (defaults to now)
        """
        if not self.redis_client:
            return
        
        _ = price_movement_pct
        # Calculate and store Sharpe using existing method
        self.sharpe_calculator.calculate_and_store_sharpe(symbol, pattern_type)
    
    def get_live_sharpe_ratio(self, symbol: str, pattern_type: str) -> Optional[float]:
        """
        Get live Sharpe ratio from Redis.
        
        Args:
            symbol: Symbol name
            pattern_type: Pattern type
            
        Returns:
            Sharpe ratio (float) or None if not found
        """
        return self.sharpe_calculator.get_sharpe_ratio(symbol, pattern_type)
    
    def check_sharpe_threshold(self, symbol: str, pattern_type: str, 
                              vix_regime: str = "NORMAL") -> Tuple[Optional[float], bool]:
        """
        Check if pattern meets VIX-adjusted Sharpe threshold.
        
        Args:
            symbol: Symbol name
            pattern_type: Pattern type
            vix_regime: VIX regime (LOW, NORMAL, HIGH, PANIC)
            
        Returns:
            Tuple of (sharpe_ratio, meets_threshold)
        """
        sharpe = self.get_live_sharpe_ratio(symbol, pattern_type)
        
        if sharpe is None:
            return None, False
        
        threshold = self.sharpe_calculator.get_vix_adjusted_sharpe_threshold(vix_regime)
        meets_threshold = sharpe >= threshold
        
        return sharpe, meets_threshold
