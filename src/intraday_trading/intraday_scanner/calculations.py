"""
Calculations Module - High-Performance Hybrid Calculations
Uses Polars for maximum performance with exact Zerodha field names
Replaces all legacy calculation methods with optimized implementations
Updated: 2025-12-02 #Lokesh Gupta (Always update the date when you make any changes)
"""

import sys
import time
import json
import logging
import os
import threading
from datetime import datetime, timedelta, date
from typing import List, Dict, Union, Optional, Tuple, Any, TYPE_CHECKING
import math
import statistics
from collections import defaultdict, deque
from pathlib import Path
import re
from enum import Enum
from functools import wraps

from shared_core.redis_clients.redis_client import get_redis_client
# Polars for high-performance DataFrame operations
try:
    import polars as pl
    POLARS_AVAILABLE = True
    # Create type alias for DataFrame when Polars is available
    PolarsDataFrame = pl.DataFrame
except ImportError:
    POLARS_AVAILABLE = False
    pl = None  # type: ignore
    # Create a dummy class for type hints when Polars is not available
    class PolarsDataFrame:  # type: ignore
        pass

# Type hints for Polars (only used during type checking)
if TYPE_CHECKING:
    from polars import DataFrame
else:
    DataFrame = Any  # type: ignore

# TA-Lib for technical indicators (preferred - fastest and most accurate)
try:
    import talib
    TALIB_AVAILABLE = True
    logger = logging.getLogger(__name__)
    logger.info("✅ TA-Lib loaded (preferred for technical indicators - fastest and most accurate)")
except ImportError as e:
    TALIB_AVAILABLE = False
    talib = None  # type: ignore
    logger = logging.getLogger(__name__)
    logger.warning(f"⚠️ TA-Lib not available: {e}")

# Add parent directories to path for redis_files imports
# CRITICAL: Prioritize intraday_trading so it finds intraday_trading/redis_files first
script_dir = Path(__file__).parent.parent  # intraday_trading
parent_root = script_dir.parent  # aion_algo_trading
# Remove if already present to avoid duplicates
if str(script_dir) in sys.path:
    sys.path.remove(str(script_dir))
if str(parent_root) in sys.path:
    sys.path.remove(str(parent_root))
# Insert project_root FIRST so alerts module is found from repo root
# Then intraday_trading so it finds intraday_trading/redis_files
sys.path.insert(0, str(parent_root))
sys.path.insert(1, str(script_dir))

from shared_core.volume_files.volume_computation_manager import VolumeComputationManager, get_volume_computation_manager
from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
from shared_core.redis_clients.symbol_guard import get_symbol_guard
from shared_core.redis_clients import UnifiedRedisManager
from shared_core.utils.serialization import fix_numpy_serialization


def identify_option_type(symbol: Optional[str]) -> Optional[str]:
    """
    Extract option type from Indian option symbols.
    Examples:
    - NSEB'NIFTY25DEC25300PE → 'PE'
    - NFOBANKNIFTY26JAN60000CE → 'CE'
    """
    if not symbol:
        return None

    normalized = str(symbol).strip().upper().replace("'", "").replace('"', '')
    if normalized.endswith("PE"):
        return "PE"
    if normalized.endswith("CE"):
        return "CE"
    if normalized.endswith("P"):
        return "PE"
    if normalized.endswith("C"):
        return "CE"
    return None


class SchemaEnforcer:
    """Ensure consistent schemas across crawler outputs and scanner windows."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.expected_schema = {
            'timestamp': pl.Int64 if POLARS_AVAILABLE and pl else None,
            'exchange_timestamp_ms': pl.Int64 if POLARS_AVAILABLE and pl else None,
            'last_price': pl.Float64 if POLARS_AVAILABLE and pl else None,
            'volume': pl.Int64 if POLARS_AVAILABLE and pl else None,
            'open': pl.Float64 if POLARS_AVAILABLE and pl else None,
            'high': pl.Float64 if POLARS_AVAILABLE and pl else None,
            'low': pl.Float64 if POLARS_AVAILABLE and pl else None,
            'close': pl.Float64 if POLARS_AVAILABLE and pl else None,
            'volume_ratio': pl.Float64 if POLARS_AVAILABLE and pl else None,
            'gamma': pl.Float64 if POLARS_AVAILABLE and pl else None,
            'theta': pl.Float64 if POLARS_AVAILABLE and pl else None,
            'vega': pl.Float64 if POLARS_AVAILABLE and pl else None,
            'delta': pl.Float64 if POLARS_AVAILABLE and pl else None,
            'iv': pl.Float64 if POLARS_AVAILABLE and pl else None,
        }

    def enforce_schema(self, df: Optional["pl.DataFrame"]) -> Optional["pl.DataFrame"]:
        if not POLARS_AVAILABLE or pl is None:
            return df
        if df is None or df.is_empty():
            return df

        for col, expected_type in self.expected_schema.items():
            if expected_type is None or col not in df.columns:
                continue
            current_type = df[col].dtype
            if current_type != expected_type:
                try:
                    df = df.with_columns(pl.col(col).cast(expected_type, strict=False))
                except Exception as exc:
                    self.logger.warning(
                        f"⚠️ [SCHEMA_ENFORCE] Failed to cast {col} from {current_type} to {expected_type}: {exc}"
                    )
                    df = df.drop(col)
        return df

    def get_schema_differences(
        self, df1: Optional["pl.DataFrame"], df2: Optional["pl.DataFrame"]
    ) -> Dict[str, Dict[str, Any]]:
        diffs: Dict[str, Dict[str, Any]] = {}
        if not POLARS_AVAILABLE or pl is None or df1 is None or df2 is None:
            return diffs

        for col in set(df1.columns) & set(df2.columns):
            if df1[col].dtype != df2[col].dtype:
                diffs[col] = {
                    'df1_type': df1[col].dtype,
                    'df2_type': df2[col].dtype,
                }
        return diffs


def _ensure_float64(value: Any, default: float = 0.0) -> float:
    """Convert any numeric-like value to a stable float64 for Polars compatibility."""
    try:
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (TypeError, ValueError):
        return default


def _normalize_macd_payload(payload: Dict[str, Any]) -> Dict[str, float]:
    return {
        'macd': _ensure_float64(payload.get('macd', 0.0)),
        'signal': _ensure_float64(payload.get('signal', 0.0)),
        'histogram': _ensure_float64(payload.get('histogram', 0.0)),
    }


class FixedAdvancedGreeks:
    """
    Advanced Greeks calculation using finite differences around BSM greeks.
    """

    def calculate_advanced_greeks(
        self,
        spot: float,
        strike: float,
        time_to_expiry_years: float,
        risk_free_rate: float,
        volatility: float,
        option_type: str,
    ) -> Dict[str, float]:
        metrics = {
            "vanna": 0.0,
            "charm": 0.0,
            "vomma": 0.0,
            "speed": 0.0,
            "zomma": 0.0,
            "color": 0.0,
        }

        if spot <= 0 or strike <= 0 or time_to_expiry_years <= 0 or volatility <= 0:
            return metrics

        flag = "c" if str(option_type).lower() in ("ce", "call") else "p"
        eps_vol = max(0.0005, volatility * 0.01)
        eps_price = max(0.1, spot * 0.001)
        eps_time = min(time_to_expiry_years / 10.0, 1.0 / 365.0)

        try:
            delta_plus = black_scholes_delta(flag, spot, strike, time_to_expiry_years, risk_free_rate, volatility + eps_vol)
            delta_minus = black_scholes_delta(flag, spot, strike, time_to_expiry_years, risk_free_rate, max(volatility - eps_vol, 1e-6))
            metrics["vanna"] = (delta_plus - delta_minus) / (2 * eps_vol)

            delta_time_plus = black_scholes_delta(flag, spot, strike, time_to_expiry_years + eps_time, risk_free_rate, volatility)
            delta_time_minus = black_scholes_delta(flag, spot, strike, max(time_to_expiry_years - eps_time, 1e-6), risk_free_rate, volatility)
            metrics["charm"] = (delta_time_plus - delta_time_minus) / (2 * eps_time)

            vega_plus = black_scholes_vega(flag, spot, strike, time_to_expiry_years, risk_free_rate, volatility + eps_vol)
            vega_minus = black_scholes_vega(flag, spot, strike, time_to_expiry_years, risk_free_rate, max(volatility - eps_vol, 1e-6))
            metrics["vomma"] = (vega_plus - vega_minus) / (2 * eps_vol)

            gamma_price_plus = black_scholes_gamma(flag, spot + eps_price, strike, time_to_expiry_years, risk_free_rate, volatility)
            gamma_price_minus = black_scholes_gamma(flag, max(spot - eps_price, 1e-6), strike, time_to_expiry_years, risk_free_rate, volatility)
            metrics["speed"] = (gamma_price_plus - gamma_price_minus) / (2 * eps_price)

            gamma_vol_plus = black_scholes_gamma(flag, spot, strike, time_to_expiry_years, risk_free_rate, volatility + eps_vol)
            gamma_vol_minus = black_scholes_gamma(flag, spot, strike, time_to_expiry_years, risk_free_rate, max(volatility - eps_vol, 1e-6))
            metrics["zomma"] = (gamma_vol_plus - gamma_vol_minus) / (2 * eps_vol)

            gamma_time_plus = black_scholes_gamma(flag, spot, strike, time_to_expiry_years + eps_time, risk_free_rate, volatility)
            gamma_time_minus = black_scholes_gamma(flag, spot, strike, max(time_to_expiry_years - eps_time, 1e-6), risk_free_rate, volatility)
            metrics["color"] = (gamma_time_plus - gamma_time_minus) / (2 * eps_time)
        except Exception as calc_exc:
            logger.warning(f"⚠️ [ADVANCED_GREEKS] Calculation failed: {calc_exc}")

        return metrics

from shared_core.volume_files import VolumeComputationManager as FixedVolumeComputationManager  # UPDATED: Use unified module

# Canonical yaml field loader (shared across services)
from shared_core.config_utils.yaml_field_loader import apply_field_mapping, normalize_session_record
from shared_core.redis_clients.redis_key_standards import get_symbol_parser
# Get singleton parser - use get_underlying and parse methods
_symbol_parser = get_symbol_parser()
extract_underlying_from_symbol = lambda symbol: _symbol_parser.get_underlying(symbol) or symbol
parse_derivative_parts = lambda symbol: _symbol_parser.parse(symbol)


from market_data import InterestRateService

# get_underlying_price is now a method on UniversalSymbolParser
def get_underlying_price(symbol, redis_client=None):
    """Get underlying price using the singleton parser."""
    return _symbol_parser.get_underlying_price(symbol, redis_client)

# Backward compatibility aliases
resolve_underlying_price = get_underlying_price
INDEX_PRICE_KEY_OVERRIDES = {}  # Now handled internally by UniversalSymbolParser


def _extract_volume_ratio(source: Dict[str, Any]) -> float:
    """
    ✅ SINGLE SOURCE OF TRUTH: Read pre-computed volume ratio from incoming data.
    
    Volume ratio is calculated by websocket_parser using:
    - VolumeComputationManager.calculate_incremental() for incremental volume (stored in Redis DB 1: volume_state:{token})
    - VolumeComputationManager.calculate_volume_metrics() for volume ratio (stored in tick_data)
    
    This function ONLY reads pre-computed values - NEVER calculates.
    Returns 0.0 if volume_ratio is not found (do NOT override volume manager's calculations).
    """
    if not isinstance(source, dict):
        return 0.0
    for field in ("volume_ratio", "volume_ratio_enhanced", "normalized_volume"):
        value = source.get(field)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return 0.0


class VolumeDataValidator:
    """
    Validates that volume data flow is correct and calculations.py 
    never overrides Volume Manager computations.
    
    ✅ SINGLE SOURCE OF TRUTH: Ensures volume_ratio from websocket_parser
    (via VolumeComputationManager and VolumeComputationManager) is never modified.
    """
    
    def __init__(self):
        """Initialize volume data validator with tracking counters."""
        self.volume_override_attempts = 0
        self.missing_volume_data = 0
        self.logger = logging.getLogger(__name__)
        
    def validate_tick_data(self, tick_data: Dict[str, Any], token: int) -> bool:
        """
        Validate that tick_data has proper volume structure from Volume Manager.
        
        Args:
            tick_data: Tick data dictionary with volume fields
            token: Instrument token for logging
            
        Returns:
            bool: True if volume data is valid and from source of truth
        """
        required_volume_fields = ['volume', 'volume_ratio']
        
        # Check for presence of volume data
        has_volume_data = all(field in tick_data for field in required_volume_fields)
        
        if not has_volume_data:
            self.missing_volume_data += 1
            self.logger.warning(f"⚠️ Missing volume data for token {token}: missing fields {[f for f in required_volume_fields if f not in tick_data]}")
            return False
            
        # Check if volume_ratio looks calculated (sanity check)
        volume_ratio = tick_data.get('volume_ratio')
        if volume_ratio is not None and not isinstance(volume_ratio, (int, float)):
            self.logger.error(f"❌ Invalid volume_ratio type for token {token}: {type(volume_ratio)} (expected int/float)")
            return False
            
        return True
    
    def detect_override_attempts(self, tick_data_before: Dict[str, Any], tick_data_after: Dict[str, Any]) -> bool:
        """
        Detect if volume_ratio was modified in calculations.py (should never happen).
        
        Args:
            tick_data_before: Tick data before processing
            tick_data_after: Tick data after processing
            
        Returns:
            bool: True if override was detected, False otherwise
        """
        ratio_before = tick_data_before.get('volume_ratio')
        ratio_after = tick_data_after.get('volume_ratio')
        
        # If ratio was modified, it's an override attempt
        if ratio_before != ratio_after:
            self.volume_override_attempts += 1
            self.logger.critical(
                f"🚨 VOLUME OVERRIDE DETECTED: "
                f"Before: {ratio_before}, After: {ratio_after} "
                f"(token: {tick_data_before.get('instrument_token', 'unknown')})"
            )
            return True
            
        return False
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get validation statistics.
        
        Returns:
            dict: Statistics about volume data validation
        """
        return {
            'volume_override_attempts': self.volume_override_attempts,
            'missing_volume_data': self.missing_volume_data
        }


class MCXCrossAssetCalculator:
    """Track USDINR and MCX metal relationships for cross-asset analytics."""

    def __init__(self, maxlen: int = 200, redis_client=None):
        """
        Initialize cross-asset calculator.
        
        Args:
            maxlen: Maximum length of price history deque
            redis_client: Optional Redis client for fetching USD:INR from DB1 index_hash
        """
        self.usdinr_prices: deque[float] = deque(maxlen=maxlen)
        self.gold_prices: deque[float] = deque(maxlen=maxlen)
        self.silver_prices: deque[float] = deque(maxlen=maxlen)
        self.redis_client = redis_client
        self.logger = logging.getLogger(__name__)

    def update_cross_asset_data(self, symbol: str, price: float) -> None:
        if not symbol:
            return
        try:
            price_val = float(price)
        except (TypeError, ValueError):
            return

        symbol_upper = symbol.upper()
        if 'USDINR' in symbol_upper:
            self.usdinr_prices.append(price_val)
        elif 'GOLD' in symbol_upper:
            self.gold_prices.append(price_val)
        elif 'SILVER' in symbol_upper:
            self.silver_prices.append(price_val)

    def get_latest_usdinr_rate(self) -> float:
        """
        Get latest USD:INR rate.
        
        Priority:
        1. In-memory deque (from tick data)
        2. Redis DB1 index_hash (published by gift_nifty_gap script)
        
        Returns:
            USD:INR rate (float), or 0.0 if not available
        """
        # Priority 1: Check in-memory deque (from tick data)
        if self.usdinr_prices:
            return float(self.usdinr_prices[-1])
        
        # Priority 2: Fetch from Redis DB1 index_hash (published by gift_nifty_gap script)
        if self.redis_client:
            try:
                # Get DB1 client
                db1_client = None
                if hasattr(self.redis_client, 'get_client'):
                    db1_client = self.redis_client.get_client(1)  # DB1
                elif hasattr(self.redis_client, 'redis'):
                    db1_client = self.redis_client.redis
                else:
                    db1_client = self.redis_client
                
                if db1_client:
                    # Try multiple possible key formats (as published by gift_nifty_gap script)
                    usdinr_keys = [
                        "index_hash:USDINR",  # Primary key format
                        "index_hash:NSE:USDINR",  # NSE format
                        "index_hash:MCX:USDINR",  # MCX format
                        "index:USDINR",  # JSON string fallback
                        "index:NSE:USDINR",  # NSE JSON fallback
                    ]
                    
                    for key in usdinr_keys:
                        try:
                            # Try Redis Hash first (fast, no JSON parsing)
                            if key.startswith("index_hash:"):
                                last_price = db1_client.hget(key, 'last_price')
                                if last_price:
                                    if isinstance(last_price, bytes):
                                        last_price = last_price.decode('utf-8')
                                    rate = float(last_price)
                                    if rate > 0:
                                        # Update in-memory deque for future use
                                        self.usdinr_prices.append(rate)
                                        self.logger.debug(
                                            f"✅ [USDINR] Fetched from Redis DB1 {key}: {rate:.4f}"
                                        )
                                        return rate
                            else:
                                # Try JSON string fallback
                                data_str = db1_client.get(key)
                                if data_str:
                                    import json
                                    if isinstance(data_str, bytes):
                                        data_str = data_str.decode('utf-8')
                                    data = json.loads(data_str)
                                    last_price = data.get('last_price') or data.get('price')
                                    if last_price:
                                        rate = float(last_price)
                                        if rate > 0:
                                            self.usdinr_prices.append(rate)
                                            self.logger.debug(
                                                f"✅ [USDINR] Fetched from Redis DB1 {key} (JSON): {rate:.4f}"
                                            )
                                            return rate
                        except Exception as e:
                            self.logger.debug(f"⚠️ [USDINR] Failed to fetch from {key}: {e}")
                            continue
            except Exception as e:
                self.logger.debug(f"⚠️ [USDINR] Error accessing Redis DB1: {e}")
        
        return 0.0

    def _get_series(self, asset: str) -> deque:
        if asset == 'GOLD':
            return self.gold_prices
        if asset == 'SILVER':
            return self.silver_prices
        return deque()

    def _get_reference_price(self, asset: str, fallback: float) -> float:
        series = self._get_series(asset)
        if series:
            return float(series[-1])
        return float(fallback)

    def calculate_usdinr_sensitivity(
        self,
        asset: str,
        commodity_price: float,
        usdinr_rate: float,
    ) -> Dict[str, float]:
        asset = asset.upper()
        if asset not in ('GOLD', 'SILVER'):
            return {}

        price_series = list(self._get_series(asset))
        usdinr_series = list(self.usdinr_prices)

        reference_price = self._get_reference_price(asset, commodity_price)
        theoretical = reference_price * usdinr_rate if reference_price and usdinr_rate else 0.0
        basis = commodity_price - theoretical if theoretical else 0.0

        if (
            not NUMPY_AVAILABLE
            or np is None
            or len(price_series) < 20
            or len(usdinr_series) < 20
        ):
            return {
                'usdinr_sensitivity': 0.0,
                'usdinr_correlation': 0.0,
                'theoretical_price': theoretical,
                'basis': basis,
            }

        try:
            correlation = float(np.corrcoef(usdinr_series, price_series)[0, 1]) if usdinr_series and price_series else 0.0
        except Exception:
            correlation = 0.0

        try:
            usdinr_returns = np.diff(np.log(usdinr_series))
            asset_returns = np.diff(np.log(price_series)) if price_series else []
            min_len = min(len(usdinr_returns), len(asset_returns))
            if min_len <= 1:
                beta = 0.0
            else:
                covariance = np.cov(usdinr_returns[:min_len], asset_returns[:min_len])[0, 1]
                variance = np.var(usdinr_returns[:min_len]) if usdinr_returns[:min_len] else 0.0
                beta = float(covariance / variance) if variance > 0 else 0.0
        except Exception:
            beta = 0.0

        return {
            'usdinr_sensitivity': beta,
            'usdinr_correlation': correlation,
            'theoretical_price': theoretical,
            'basis': basis,
        }
# Note: polars and talib are now imported at the top of the file

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    np = None  # Define np for type hints even if not available

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# Configure logging
logger = logging.getLogger(__name__)

# Fast EMA Classes - Stateful EMA calculation for O(1) per-tick updates
class FastEMA:
    """Fast EMA calculation using stateful updates (O(1) per tick, optimized for real-time trading)"""
    
    def __init__(self, period: int):
        self.period = period
        self.alpha = 2.0 / (period + 1.0)
        self.ema = None
        self.is_initialized = False
        
    def update_fast(self, new_value: float) -> float:
        """Fast EMA update - optimized for single tick processing"""
        if not self.is_initialized:
            self.ema = new_value
            self.is_initialized = True
        else:
            self.ema = (new_value * self.alpha) + (self.ema if self.ema is not None else 0.0 * (1 - self.alpha))
        return self.ema
    
    def update_batch(self, values):
        """Process batch of values - much faster than loop"""
        if len(values) == 0:
            return np.array([]) if NUMPY_AVAILABLE and np is not None else []
            
        if not self.is_initialized:
            self.ema = values[0]
            self.is_initialized = True
            result = [self.ema]
        else:
            result = []
        
        # Vectorized EMA calculation
        alpha = self.alpha
        for value in values[len(result):]:
            self.ema = (value * alpha) + (self.ema if self.ema is not None else 0.0 * (1 - alpha))
            result.append(self.ema)
            
        return np.array(result) if NUMPY_AVAILABLE and np is not None else []

class CircularEMA:
    """Pre-allocated circular buffer EMA for maximum performance"""
    
    def __init__(self, period: int, max_points: int = 10000):
        self.period = period
        self.alpha = 2.0 / (period + 1.0)
        # ✅ FIXED: Check if numpy is available before using np
        if not NUMPY_AVAILABLE or np is None:
            # Fallback if numpy is not available
            self.buffer = [] if max_points is None else [0.0] * max_points
            self.emas = [] if max_points is None else [0.0] * max_points
        else:
            # Type assertion: np is not None when NUMPY_AVAILABLE is True
            assert np is not None, "numpy should be available"
            # type: ignore[union-attr] - np is guaranteed to be not None after the check above
            self.buffer = np.zeros(max_points) if max_points is not None else np.array([], dtype=np.float64)  # type: ignore[union-attr]
            self.emas = np.zeros(max_points) if max_points is not None else np.array([])  # type: ignore[union-attr]
        self.index = 0
        self.ema = 0.0
        self.is_initialized = False
        
    def add_value(self, value: float) -> float:
        """Add single value - O(1) operation"""
        if not self.is_initialized:
            self.ema = value
            self.is_initialized = True
        else:
            self.ema = (value * self.alpha) + (self.ema if self.ema is not None else 0.0 * (1 - self.alpha))
        
        self.buffer[self.index] = value
        self.emas[self.index] = self.ema
        self.index = (self.index + 1) % len(self.buffer)
        
        return self.ema
    
    def get_current_ema(self) -> float:
        return self.ema
    
# Note: TA-Lib is now imported at the top of the file

# pandas_ta - Pure Python alternative to TA-Lib (optional fallback)
# Note: pandas_ta requires numba which doesn't support Python 3.14+
# This is handled gracefully - code works without it (TA-Lib and Polars are preferred)
try:
    import pandas_ta as pta  # type: ignore[import-untyped]
    PANDAS_TA_AVAILABLE = True
    logger.info("✅ pandas_ta loaded successfully (pure Python technical analysis)")
except ImportError as e:
    PANDAS_TA_AVAILABLE = False
    # Not critical - TA-Lib and Polars are preferred calculation methods

# Using Polars for all optimizations instead

# Greek calculations imports
try:
    from scipy.stats import norm
    SCIPY_AVAILABLE = True
    from py_vollib.black_scholes import black_scholes
    from py_vollib.black_scholes.implied_volatility import implied_volatility
    
    # 🚀 VECTORIZED VOLLIB: 1000x faster for batch IV calculations
    try:
        from py_vollib_vectorized import vectorized_implied_volatility as vec_iv
        from py_vollib_vectorized import vectorized_delta, vectorized_gamma, vectorized_theta, vectorized_vega
        VECTORIZED_VOLLIB_AVAILABLE = True
        logger.info("✅ py_vollib_vectorized loaded - using vectorized Greek calculations")
    except ImportError:
        VECTORIZED_VOLLIB_AVAILABLE = False
        logger.warning("⚠️ py_vollib_vectorized not available - using standard vollib")
    # Use py_vollib for standardized Greek calculations
    from py_vollib.black_scholes.greeks.analytical import (
        delta as black_scholes_delta, gamma as black_scholes_gamma, theta as black_scholes_theta, vega as black_scholes_vega, rho as black_scholes_rho
    )
    PY_VOLLIB_AVAILABLE = True
    GREEK_CALCULATIONS_AVAILABLE = True
    # ✅ FIXED: Create aliases for convenience (delta, gamma, etc. for backward compatibility)
    # ✅ CRITICAL: Ensure aliases are always defined, even if black_scholes_* functions fail
    try:
        delta = black_scholes_delta
        gamma = black_scholes_gamma
        theta = black_scholes_theta
        vega = black_scholes_vega
        rho = black_scholes_rho
    except NameError:
        # If black_scholes_* functions are not defined, create stub functions
        def delta(*args, **kwargs): return 0.0
        def gamma(*args, **kwargs): return 0.0
        def theta(*args, **kwargs): return 0.0
        def vega(*args, **kwargs): return 0.0
        def rho(*args, **kwargs): return 0.0
        logger.warning("⚠️ black_scholes_* functions not available, using stubs for delta/gamma/theta/vega/rho")
    logger.info("✅ Greek calculation libraries loaded successfully (py_vollib + scipy)")
except ImportError as e:
    PY_VOLLIB_AVAILABLE = False
    GREEK_CALCULATIONS_AVAILABLE = False
    implied_volatility = None  # Stub for when vollib not available
    # ✅ FIXED: Define stub functions to prevent NameError if py_vollib is not available
    def delta(*args, **kwargs): return 0.0
    def gamma(*args, **kwargs): return 0.0
    def theta(*args, **kwargs): return 0.0
    def vega(*args, **kwargs): return 0.0
    def rho(*args, **kwargs): return 0.0
    # ✅ CRITICAL FIX: Also define black_scholes_* functions (used in _calculate_higher_order_greeks)
    black_scholes_delta = delta
    black_scholes_gamma = gamma
    black_scholes_theta = theta
    black_scholes_vega = vega
    black_scholes_rho = rho
    def black_scholes(*args, **kwargs): return 0.0
    logger.warning(f"⚠️ Greek calculation libraries not available: {e}")

# ✅ CONSOLIDATED: Calendar caching moved to shared_core.market_calendar
# Import get_cached_calendar from the consolidated module (single source of truth)
try:
    from shared_core.market_calendar import get_cached_calendar, MarketCalendar
    MARKET_CALENDAR_AVAILABLE = True
except ImportError:
    MARKET_CALENDAR_AVAILABLE = False
    get_cached_calendar = None
    MarketCalendar = None

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
        # ✅ FIXED: Define timestamp schema only if polars is available
        if POLARS_AVAILABLE and pl is not None:
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
        else:
            self.timestamp_schema = {}

    def _optimize_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Optimize DataFrame for trading data storage.
        """
        # ✅ FIXED: Check if polars is available before using it
        if TYPE_CHECKING:
            import polars as pl
            POLARS_AVAILABLE = True
        else:
            try:
                import polars as pl
                POLARS_AVAILABLE = True
            except ImportError:
                POLARS_AVAILABLE = False
        if not POLARS_AVAILABLE or pl is None:
            return df
        
        # Apply timestamp schema for known columns (only for non-string types)
        # String timestamps will be parsed by _create_normalized_timestamp()
        schema_to_apply = {}
        for col, dtype in self.timestamp_schema.items():
            if col in df.columns:
                # Only cast if column is NOT a string (Utf8)
                # String timestamps need to be parsed, not cast
                if df[col].dtype != pl.Utf8:
                    schema_to_apply[col] = dtype
        
        if schema_to_apply:
            df = df.cast(schema_to_apply)
        
        # Create normalized timestamp if possible (handles string parsing)
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

    def _create_normalized_timestamp(self, df: DataFrame) -> DataFrame:
        """
        Create unified timestamp column from various timestamp sources.
        """
        # ✅ FIXED: Check if polars is available before using it
        if not POLARS_AVAILABLE or pl is None:
            return df
        
        timestamp_exprs = []
        
        # Priority-based timestamp conversion
        # ✅ PRIMARY: exchange_timestamp_ms (Int64 epoch milliseconds) - from intraday crawler
        if 'exchange_timestamp_ms' in df.columns:
            dtype = df['exchange_timestamp_ms'].dtype
            if dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]:
                # Epoch milliseconds - convert using from_epoch
                timestamp_exprs.append(
                    pl.col('exchange_timestamp_ms')
                    .cast(pl.Int64)
                    .cast(pl.Datetime(time_unit="ms"))
                    .alias('exchange_timestamp_ms_norm')
                )
            elif str(dtype).startswith('Datetime'):
                # Already Datetime - remove timezone if present
                timestamp_exprs.append(
                    pl.col('exchange_timestamp_ms')
                    .dt.replace_time_zone(None)
                    .alias('exchange_timestamp_ms_norm')
                )
        
        # ✅ SECONDARY: exchange_timestamp (ISO string) - from intraday crawler
        if 'exchange_timestamp' in df.columns:
            dtype = df['exchange_timestamp'].dtype
            if dtype == pl.Utf8:
                # String (ISO format) - parse it to Datetime
                # Format from crawler: "2025-11-10T09:57:30" or "2025-11-10 09:57:30"
                # ✅ FIXED: Explicitly handle timezone by removing it before parsing, or use eager parsing
                timestamp_exprs.append(
                    pl.col('exchange_timestamp')
                    .str.strip_chars('"')  # Strip quotes first
                    .str.replace(r'[+-]\d{2}:\d{2}$', '')  # Remove timezone suffix if present (e.g., +05:30)
                    .str.replace(r'Z$', '')  # Remove Z suffix if present
                    .str.strptime(pl.Datetime(time_unit="ms"), format="%Y-%m-%dT%H:%M:%S", strict=False)  # Explicit format
                    .alias('exchange_timestamp_norm')
                )
            elif str(dtype).startswith('Datetime'):
                # Already Datetime - remove timezone if present
                timestamp_exprs.append(
                    pl.col('exchange_timestamp')
                    .dt.replace_time_zone(None)
                    .alias('exchange_timestamp_norm')
                )
            elif dtype in [pl.Int64, pl.Int32]:
                # Epoch milliseconds - convert using from_epoch
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('exchange_timestamp'), time_unit="ms")
                    .alias('exchange_timestamp_norm')
                )
        
        # ✅ FALLBACK: timestamp_ms (Int64 epoch milliseconds)
        if 'timestamp_ms' in df.columns:
            dtype = df['timestamp_ms'].dtype
            if dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]:
                # Epoch milliseconds - convert using from_epoch
                timestamp_exprs.append(
                    pl.col('timestamp_ms')
                    .cast(pl.Int64)
                    .cast(pl.Datetime(time_unit="ms"))
                    .alias('timestamp_ms_norm')
                )
            elif str(dtype).startswith('Datetime'):
                # Already Datetime - remove timezone if present
                timestamp_exprs.append(
                    pl.col('timestamp_ms')
                    .dt.replace_time_zone(None)
                    .alias('timestamp_ms_norm')
                )
        
        # ✅ FALLBACK: timestamp (ISO string)
        if 'timestamp' in df.columns:
            dtype = df['timestamp'].dtype
            if dtype == pl.Utf8:
                # String (ISO format) - parse it to Datetime
                # Format from crawler: "2025-11-10T09:57:30" or "2025-11-10 09:57:30"
                # ✅ FIXED: Explicitly handle timezone by removing it before parsing
                timestamp_exprs.append(
                    pl.col('timestamp')
                    .str.strip_chars('"')  # Strip quotes first
                    .str.replace(r'[+-]\d{2}:\d{2}$', '')  # Remove timezone suffix if present (e.g., +05:30)
                    .str.replace(r'Z$', '')  # Remove Z suffix if present
                    .str.strptime(pl.Datetime(time_unit="ms"), format="%Y-%m-%dT%H:%M:%S", strict=False)  # Explicit format
                    .alias('timestamp_norm')
                )
            elif str(dtype).startswith('Datetime'):
                # Already Datetime - remove timezone if present
                timestamp_exprs.append(
                    pl.col('timestamp')
                    .dt.replace_time_zone(None)
                    .alias('timestamp_norm')
                )
            elif dtype in [pl.Int64, pl.Int32]:
                # Epoch milliseconds - convert using from_epoch
                timestamp_exprs.append(
                    pl.from_epoch(pl.col('timestamp'), time_unit="ms")
                    .alias('timestamp_norm')
                )
        
        # ✅ FIXED: Epoch seconds - robust handling for string/numeric mixed types
        # WebSocket data often comes as strings, JSON serialization can convert numbers to strings
        if 'exchange_timestamp_epoch' in df.columns:
            dtype = df['exchange_timestamp_epoch'].dtype
            if dtype == pl.Utf8:
                # Already string - clean and convert
                timestamp_exprs.append(
                    pl.col('exchange_timestamp_epoch')
                    .str.replace(r'[^\d.-]', '')  # Clean non-numeric chars
                    .cast(pl.Float64, strict=False)  # Convert to float, don't fail on errors
                    .fill_nan(0)  # Handle NaN
                    .fill_null(0)  # Handle nulls
                    .cast(pl.Int64)  # Convert to integer
                    .mul(1000)  # Convert seconds → milliseconds
                    .cast(pl.Datetime(time_unit="ms"))
                    .alias('epoch_norm')
                )
            else:
                # Already numeric - direct conversion
                timestamp_exprs.append(
                    pl.col('exchange_timestamp_epoch')
                    .cast(pl.Float64, strict=False)  # Convert to float, don't fail on errors
                    .fill_nan(0)  # Handle NaN
                    .fill_null(0)  # Handle nulls
                    .cast(pl.Int64)  # Convert to integer
                    .mul(1000)  # Convert seconds → milliseconds
                    .cast(pl.Datetime(time_unit="ms"))
                    .alias('epoch_norm')
                )
        
        # ✅ FIXED: Float seconds - robust handling for string/numeric mixed types
        if 'processed_timestamp' in df.columns:
            dtype = df['processed_timestamp'].dtype
            if dtype == pl.Utf8:
                # Already string - clean and convert
                timestamp_exprs.append(
                    pl.from_epoch(
                        pl.col('processed_timestamp')
                        .str.replace(r'[^\d.-]', '')  # Clean non-numeric chars
                        .cast(pl.Float64, strict=False)  # Convert to float, don't fail on errors
                        .fill_nan(0)  # Handle NaN
                        .fill_null(0)  # Handle nulls
                        .mul(1000)  # Convert seconds → milliseconds
                        .cast(pl.Int64),  # Convert to integer
                        time_unit='ms'
                    )
                    .alias('processed_norm')
                )
            else:
                # Already numeric - direct conversion
                timestamp_exprs.append(
                    pl.from_epoch(
                        pl.col('processed_timestamp')
                        .cast(pl.Float64, strict=False)  # Convert to float, don't fail on errors
                        .fill_nan(0)  # Handle NaN
                        .fill_null(0)  # Handle nulls
                        .mul(1000)  # Convert seconds → milliseconds
                        .cast(pl.Int64),  # Convert to integer
                        time_unit='ms'
                    )
                    .alias('processed_norm')
                )
        
        # Add all normalized columns
        if timestamp_exprs:
            df = df.with_columns(timestamp_exprs)
            
            # Create master normalized timestamp using coalesce
            norm_cols = [col for col in df.columns if col.endswith('_norm')]
            if norm_cols:
                # ✅ FIXED: Ensure all normalized columns have same timezone (or no timezone) before coalesce
                # Convert all to datetime[ms] without timezone to avoid supertype errors
                normalized_exprs = []
                for col in norm_cols:
                    # Check if column is Datetime type before using .dt methods
                    col_dtype = df[col].dtype
                    if str(col_dtype).startswith('Datetime'):
                        # Remove timezone if present, keep as datetime[ms]
                        normalized_exprs.append(
                            pl.col(col).dt.replace_time_zone(None).alias(f"{col}_no_tz")
                        )
                    else:
                        # If not Datetime, try to cast it first (shouldn't happen, but defensive)
                        try:
                            normalized_exprs.append(
                                pl.col(col).cast(pl.Datetime(time_unit="ms")).dt.replace_time_zone(None).alias(f"{col}_no_tz")
                            )
                        except Exception:
                            # Skip this column if we can't convert it
                            continue
                
                if normalized_exprs:
                    df = df.with_columns(normalized_exprs)
                    # Coalesce the timezone-free columns
                    no_tz_cols = [col for col in df.columns if col.endswith('_no_tz')]
                    if no_tz_cols:
                        df = df.with_columns(
                            pl.coalesce(no_tz_cols).alias('timestamp_normalized')
                        )
                        # Drop intermediate columns
                        df = df.drop([col for col in df.columns if col.endswith('_no_tz')])
                else:
                    # Fallback: direct coalesce (might fail if timezone mismatch)
                    try:
                        df = df.with_columns(
                            pl.coalesce(norm_cols).alias('timestamp_normalized')
                        )
                    except Exception:
                        # If coalesce fails due to timezone, use first available column
                        if norm_cols:
                            first_col = norm_cols[0]
                            col_dtype = df[first_col].dtype
                            if str(col_dtype).startswith('Datetime'):
                                df = df.with_columns(
                                    pl.col(first_col).dt.replace_time_zone(None).alias('timestamp_normalized')
                                )
                            else:
                                # If not Datetime, just use it as-is
                                df = df.with_columns(
                                    pl.col(first_col).alias('timestamp_normalized')
                                )
        
        return df

    def _compress_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Apply compression techniques to reduce memory usage.
        """
        if not self.enable_compression:
            return df
        
        # Use more efficient data types
        schema_changes = {}
        
        for col in df.columns:
            dtype = df[col].dtype
            
            # Downcast integers where possible
            if dtype == pl.Int64:  # type: ignore[attr-defined]
                min_val = df[col].min()
                max_val = df[col].max()
                if min_val is not None and max_val is not None:
                    if -128 <= min_val and max_val <= 127:  # type: ignore[operator]
                        schema_changes[col] = pl.Int8  # type: ignore[attr-defined]
                    elif -32768 <= min_val and max_val <= 32767:  # type: ignore[operator]
                        schema_changes[col] = pl.Int16  # type: ignore[attr-defined]
                    elif -2147483648 <= min_val and max_val <= 2147483647:  # type: ignore[operator]
                        schema_changes[col] = pl.Int32  # type: ignore[attr-defined]
            
            # ✅ FIXED: Don't downcast Float64 to Float32 - causes type mismatch errors when concatenating
            # Keep Float64 to ensure type consistency across all operations
            # elif dtype == pl.Float64:
            #     schema_changes[col] = pl.Float32
        
        if schema_changes:
            df = df.cast(schema_changes)
        
        return df

    def get(self, key: str) -> Optional[PolarsDataFrame]:
        """
        Get cached DataFrame with LRU update and expiration check.
        
        Args:
            key: Cache key
            
        Returns:
            Optimized Polars DataFrame or None if not found/expired
        """
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

    def set(self, key: str, df: "PolarsDataFrame", optimize: bool = True):
        """
        Store and optimize DataFrame in cache.
        
        Args:
            key: Cache key
            df: Polars DataFrame to cache
            optimize: Apply trading-specific optimizations
        """
        with self._lock:
            # Evict if cache is full
            if len(self.cache) >= self.max_size:
                self._evict_oldest()
            
            # Apply optimizations
            if optimize:
                df = self._optimize_dataframe(df)
                df = self._compress_dataframe(df)
            
            current_time = time.time()
            
            # Store optimized entry
            self.cache[key] = {
                'df': df,
                'timestamp': current_time,
                'access_time': current_time,
                'access_count': 0,
                'row_count': len(df),
                'memory_usage': df.estimated_size()
            }
            
            self._update_access_order(key)

    def query_time_range(self, key: str, start_time: datetime, end_time: datetime, 
                        timestamp_col: str = 'timestamp_normalized') -> Optional[PolarsDataFrame]:
        """
        Query cached data within a specific time range.
        Optimized for time-series trading data.
        """
        df = self.get(key)
        if df is None or timestamp_col not in df.columns:
            return None
        
        return df.filter(
            (pl.col(timestamp_col) >= start_time) & 
            (pl.col(timestamp_col) <= end_time)
        )

    def get_stats(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get statistics for a cached DataFrame.
        """
        with self._lock:
            if key not in self.cache:
                return None
            
            entry = self.cache[key]
            df = entry['df']
            
            return {
                'key': key,
                'row_count': entry['row_count'],
                'memory_usage_mb': entry['memory_usage'] / 1024 / 1024,
                'access_count': entry['access_count'],
                'age_seconds': time.time() - entry['timestamp'],
                'columns': df.columns,
                'schema': dict(df.schema),
                'time_range': self._get_session_time_range(df) if 'timestamp_normalized' in df.columns else None
            }

    def _get_session_time_range(self, df: "PolarsDataFrame") -> Dict[str, datetime]:
        """Get time range from DataFrame with normalized timestamp."""
        if df is None or 'timestamp_normalized' not in df.columns:
            return {}
        
        time_col = df['timestamp_normalized']
        return {
            'start': time_col.min(),
            'end': time_col.max()
        }

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

    def size(self) -> int:
        """Get current cache size."""
        with self._lock:
            return len(self.cache)

    def cleanup_expired(self) -> int:
        """Remove expired entries and return count cleaned."""
        with self._lock:
            current_time = time.time()
            expired_keys = [
                key for key, entry in self.cache.items()
                if current_time - entry['timestamp'] >= self.ttl
            ]
            
            for key in expired_keys:
                self._remove_key(key)
                
            return len(expired_keys)

    def get_memory_usage(self) -> float:
        """Get total memory usage in MB."""
        with self._lock:
            total_memory = sum(entry['memory_usage'] for entry in self.cache.values())
            return total_memory / 1024 / 1024

    def list_keys(self) -> List[str]:
        """Get list of all cache keys."""
        with self._lock:
            return list(self.cache.keys())


class HybridCalculations:
    """
    High-performance calculations using Polars for data processing
    Uses exact Zerodha field names from optimized_field_mapping.yaml
    Replaces legacy calculation methods with optimized implementations
    """
    
    def __init__(self, max_cache_size: int = 1000, max_batch_size: int = 174, redis_client=None):
        """Initialize with field mapping support
        
        Args:
            max_cache_size: Maximum cache size for indicators
            max_batch_size: Maximum batch size for processing
            redis_client: Optional Redis client for fetching underlying prices, historical data, etc.
        """
        # Import field mapping utilities from the shared helper
        from shared_core.config_utils.yaml_field_loader import (
            get_field_mapping_manager,
            resolve_calculated_field as resolve_indicator_field,
            resolve_session_field,
        )
        
        self.field_mapping_manager = get_field_mapping_manager()
        self.resolve_session_field = resolve_session_field
        self.resolve_indicator_field = resolve_indicator_field
        
        # Initialize with standard parameters
        self.max_cache_size = max_cache_size
        self.max_batch_size = max_batch_size
        # ✅ Store client instance - use provided or create pooled one for process-wide sharing
        self.redis_client = redis_client or get_redis_client(
            process_name="hybrid_calculations"  # ✅ UnifiedRedisManager will share process pool
        )
        self.rolling_windows = {}  # symbol -> DataFrame (for scanner batch operations)
        self.cache = {}  # Cache for scanner batch indicator calculations
        self.min_batch_size = 20

        # ✅ VIX cache initialization
        self.vix_utils = None
        self._cached_vix_snapshot: Optional[Dict[str, Any]] = None
        self._last_vix_fetch: float = 0.0
        self.vix_refresh_interval: int = 5
        try:
            from shared_core.utils.vix_utils import get_vix_utils
            if redis_client:
                self.vix_utils = get_vix_utils(redis_client=redis_client)
            else:
                self.vix_utils = get_vix_utils()
        except Exception as vix_exc:
            logger.debug(f"⚠️ [VIX_INIT] Unable to initialize VIX utils: {vix_exc}")
            self.vix_utils = None
        
        # ✅ SESSION-BASED: Initialize session-based data structures
        self._symbol_sessions = {}  # symbol -> session-based data (in-memory Polars DataFrame)
        self._last_session_volume = {}  # symbol -> last seen bucket_cumulative_volume
        
        # ✅ NEW: Initialize HistoricalTickQueries for historical data access
        try:
            from shared_core.redis_clients.redis_storage import HistoricalTickQueries
            self.historical_queries = HistoricalTickQueries(redis_client=redis_client)
        except Exception as e:
            logger.warning(f"HistoricalTickQueries not available: {e}")
            self.historical_queries = None
        
        # ✅ FAST EMA INSTANCES: Initialize all EMA windows for ultra-fast calculations
        self.fast_emas = {
            'ema_5': FastEMA(5),
            'ema_10': FastEMA(10),
            'ema_20': FastEMA(20),
            'ema_50': FastEMA(50),
            'ema_100': FastEMA(100),
            'ema_200': FastEMA(200)
        }
        
        # ✅ CIRCULAR EMA BUFFERS: For maximum performance with large datasets
        self.circular_emas = {
            'ema_5': CircularEMA(5),
            'ema_10': CircularEMA(10),
            'ema_20': CircularEMA(20),
            'ema_50': CircularEMA(50),
            'ema_100': CircularEMA(100),
            'ema_200': CircularEMA(200)
        }
        
        # ✅ ENHANCED: Polars cache with TTL and LRU eviction
        self._polars_cache = PolarsCache(max_size=max_cache_size, ttl=300)  # 5 minutes TTL
        self._session_window_size = 100  # Default session-based window size (in-memory Polars DataFrame)
        
        # Cache for scanner batch indicator calculations (not used by parser - parser uses WriteFirstCache)
        self._cache = {}  # {symbol_indicators: {data, timestamp, data_hash}}
        self._cache_ttl = 300  # 5 minutes TTL
        self._last_calculation_time = {}  # Track when each symbol was last calculated
        self._fresh_calculations = set()  # Track which symbols had fresh calculations (not cached)
        self._microstructure_state: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {'bid_size': 0.0, 'ask_size': 0.0, 'cvd': 0.0}
        )
        # ✅ THREAD SAFETY: Dedicated lock for microstructure state to prevent race conditions
        # Protects read-modify-write operations on _microstructure_state dictionary
        self._microstructure_lock = threading.RLock()
        self._gamma_exposure_cache: Dict[str, Dict[str, float]] = {}
        self._gamma_cache_ttl = 30.0
        # ✅ NEW: Pass Redis client to MCXCrossAssetCalculator for DB1 index_hash access
        # Note: self.redis_client is set earlier in __init__, so we can use it directly
        self._cross_asset_calculator = MCXCrossAssetCalculator(redis_client=self.redis_client)
        
        self._volume_calculator = None  # Lazy-initialized VolumeComputationManager
        try:
            self._advanced_greeks_fallback = FixedAdvancedGreeks()
        except Exception as exc:
            logger.warning(f"⚠️ [ADVANCED_GREEKS] Failed to initialize fallback calculator: {exc}")
            self._advanced_greeks_fallback = None
        try:
            self._fixed_volume_manager = FixedVolumeComputationManager(redis_client=self.redis_client)
        except Exception as exc:
            logger.warning(f"⚠️ [VOLUME_FALLBACK] Failed to initialize FixedVolumeComputationManager: {exc}")
            self._fixed_volume_manager = None
        self._volume_ratio_fallback_hits = 0
        self._min_positive_float = sys.float_info.min
        self._atomic_storage_enabled = True  # Allow callers to disable internal Redis writes when needed
        self._session_zscore_lookback = 20  # Default session-based window for z-score calculations
        
        # ✅ ENHANCED: Initialize multiple ExpiryCalculators for different exchanges (uses pandas_market_calendars)
        # This ensures DTE calculations use the correct market calendar for each exchange
        try:
            # Create multiple ExpiryCalculators for different exchanges
            self.expiry_calculators = {
                'NSE': ExpiryCalculator('NSE'),
                'NFO': ExpiryCalculator('NSE'),  # NFO uses NSE calendar
                'MCX': ExpiryCalculator('MCX'),  # MCX has its own calendar
                'BSE': ExpiryCalculator('BSE'),
            }
            
            # Default to NSE for backward compatibility
            self._expiry_calc = self.expiry_calculators['NSE']
            
            logger.debug("✅ [CALCULATIONS] Initialized ExpiryCalculators for multiple exchanges")
        except Exception as e:
            logger.warning(f"⚠️ [CALCULATIONS] Failed to initialize ExpiryCalculators: {e}")
            self.expiry_calculators = {}
            self._expiry_calc = None
        # ✅ PERFORMANCE FIX: Comprehensive caching for intraday optimization
        # IV Cache - uses compound key for precision
        self._iv_cache = {}  # Format: {(symbol, strike, expiry_date): (iv, timestamp)}
        self._volatility_cache = {}  # Simple symbol -> (iv, timestamp) fallback
        self._volatility_cache_ttl = 2.0  # 2 seconds (optimizes for intraday)
        
        # Risk-free rate cache (rates change slowly)
        self._risk_free_rate_cache = None
        self._risk_free_rate_timestamp = 0
        self._risk_free_rate_ttl = 3600.0  # 1 hour
        
        # DTE cache (time changes continuously but DTE in trading days doesn't change intraday)
        self._dte_cache = {}  # {(symbol, expiry_date): (dte_info, timestamp)}
        self._dte_cache_ttl = 60.0  # 60 seconds (DTE doesn't change within a minute)
        
        # Greek result cache (for identical inputs - very short TTL for high-frequency)
        self._greek_cache = {}  # {cache_key: (greeks_dict, timestamp)}
        self._greek_cache_ttl = 5.0  # 5 seconds - Greeks don't change fast for same option

    def set_atomic_storage_enabled(self, enabled: bool) -> None:
        """Enable/disable automatic indicator storage inside batch_calculate_indicators."""
        self._atomic_storage_enabled = bool(enabled)

    # ==================== VECTORIZED BATCH GREEK CALCULATIONS ====================
    
    def calculate_greeks_batch_vectorized(self, ticks_df: "pl.DataFrame") -> "pl.DataFrame":
        """
        Batch calculate Greeks using vectorized vollib (1000x faster).
        
        ⚡ Uses py_vollib_vectorized for massive speedup on batch option pricing.
        Falls back to scalar calculation if vectorized library not available.
        
        Args:
            ticks_df: Polars DataFrame with columns: 
                      underlying_price, strike_price, dte_years, 
                      implied_volatility, option_type
            
        Returns:
            DataFrame with delta, gamma, theta, vega columns added
        """
        if not POLARS_AVAILABLE or pl is None:
            logger.warning("⚠️ Polars not available for batch Greek calculation")
            return ticks_df
            
        if ticks_df.is_empty():
            return ticks_df
        
        if not VECTORIZED_VOLLIB_AVAILABLE:
            logger.debug("⚠️ Vectorized vollib not available, using scalar calculation")
            return self._calculate_greeks_scalar(ticks_df)
        
        try:
            import numpy as np
            
            # Log performance metrics
            n_options = len(ticks_df)
            start_time = time.time()
            
            # Extract numpy arrays - handle missing columns gracefully
            required_cols = ['underlying_price', 'strike_price', 'dte_years', 'implied_volatility', 'option_type']
            for col in required_cols:
                if col not in ticks_df.columns:
                    logger.warning(f"⚠️ Missing column {col} for batch Greeks")
                    return self._calculate_greeks_scalar(ticks_df)
            
            S = ticks_df['underlying_price'].to_numpy().astype(np.float64)
            K = ticks_df['strike_price'].to_numpy().astype(np.float64)
            T = ticks_df['dte_years'].to_numpy().astype(np.float64)
            
            # Risk-free rate (5% annualized)
            r = np.full_like(S, 0.05, dtype=np.float64)
            
            # Implied volatility (already as decimal, e.g., 0.25 for 25%)
            sigma = ticks_df['implied_volatility'].to_numpy().astype(np.float64)
            
            # Map option_type to vollib format ('c' for call, 'p' for put)
            flags = []
            for opt_type in ticks_df['option_type'].to_list():
                if opt_type and isinstance(opt_type, str):
                    if opt_type.upper() in ['CE', 'C', 'CALL']:
                        flags.append('c')
                    else:
                        flags.append('p')
                else:
                    flags.append('p')  # Default to put
            
            flags_np = np.array(flags)
            
            # ⚡ Vectorized calculations (1000x faster than scalar)
            delta_vals = vectorized_delta(flags_np, S, K, T, r, sigma)
            gamma_vals = vectorized_gamma(flags_np, S, K, T, r, sigma)
            theta_vals = vectorized_theta(flags_np, S, K, T, r, sigma)
            vega_vals = vectorized_vega(flags_np, S, K, T, r, sigma)
            
            # Handle NaN values
            delta_vals = np.nan_to_num(delta_vals, nan=0.0)
            gamma_vals = np.nan_to_num(gamma_vals, nan=0.0)
            theta_vals = np.nan_to_num(theta_vals, nan=0.0)
            vega_vals = np.nan_to_num(vega_vals, nan=0.0)
            
            # Add columns to DataFrame
            result_df = ticks_df.with_columns([
                pl.Series('delta', delta_vals),
                pl.Series('gamma', gamma_vals),
                pl.Series('theta', theta_vals),
                pl.Series('vega', vega_vals),
            ])
            
            # Performance logging
            elapsed_ms = (time.time() - start_time) * 1000
            ops_per_ms = n_options / max(elapsed_ms, 0.001)
            
            logger.debug(f"⚡ [VECTORIZED_GREEKS] Calculated {n_options} options in {elapsed_ms:.1f}ms "
                        f"({ops_per_ms:.1f} options/ms)")
            
            return result_df
            
        except Exception as e:
            logger.error(f"❌ Vectorized Greek calculation failed: {e}")
            # Fallback to scalar calculation
            return self._calculate_greeks_scalar(ticks_df)

    def _calculate_greeks_scalar(self, ticks_df: "pl.DataFrame") -> "pl.DataFrame":
        """
        Scalar fallback for Greek calculations.
        Used when vectorized vollib is not available.
        """
        if not POLARS_AVAILABLE or pl is None or ticks_df.is_empty():
            return ticks_df
        
        n_options = len(ticks_df)
        start_time = time.time()
        
        deltas = []
        gammas = []
        thetas = []
        vegas = []
        
        for row in ticks_df.iter_rows(named=True):
            try:
                # Extract parameters
                S = float(row.get('underlying_price', 0) or 0)
                K = float(row.get('strike_price', 0) or 0)
                T = float(row.get('dte_years', 0) or 0)
                sigma = float(row.get('implied_volatility', 0) or 0)
                opt_type = str(row.get('option_type', 'CE') or 'CE').upper()
                
                # Skip invalid inputs
                if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
                    deltas.append(0.0)
                    gammas.append(0.0)
                    thetas.append(0.0)
                    vegas.append(0.0)
                    continue
                
                # Calculate Greeks using scalar method
                flag = 'c' if opt_type in ['CE', 'C', 'CALL'] else 'p'
                r = 0.05  # Risk-free rate
                
                if PY_VOLLIB_AVAILABLE:
                    d = black_scholes_delta(flag, S, K, T, r, sigma)
                    g = black_scholes_gamma(flag, S, K, T, r, sigma)
                    t = black_scholes_theta(flag, S, K, T, r, sigma)
                    v = black_scholes_vega(flag, S, K, T, r, sigma)
                else:
                    d, g, t, v = 0.0, 0.0, 0.0, 0.0
                
                deltas.append(d)
                gammas.append(g)
                thetas.append(t)
                vegas.append(v)
                
            except Exception as e:
                logger.debug(f"⚠️ Scalar Greek calc failed for row: {e}")
                deltas.append(0.0)
                gammas.append(0.0)
                thetas.append(0.0)
                vegas.append(0.0)
        
        # Performance logging
        elapsed_ms = (time.time() - start_time) * 1000
        logger.debug(f"🐌 [SCALAR_GREEKS] Calculated {n_options} options in {elapsed_ms:.1f}ms")
        
        # Add columns to DataFrame
        return ticks_df.with_columns([
            pl.Series('delta', deltas),
            pl.Series('gamma', gammas),
            pl.Series('theta', thetas),
            pl.Series('vega', vegas),
        ])

    def process_option_batch_vectorized(self, options_data: List[Dict]) -> List[Dict]:
        """
        Process a batch of options with vectorized Greek calculations.
        
        ⚡ High-level API for batch option processing.
        Accepts list of dicts, returns list of dicts with Greeks added.
        
        Args:
            options_data: List of dicts, each containing option data
            
        Returns:
            List of dicts with calculated Greeks
        """
        if not options_data:
            return []
        
        if not POLARS_AVAILABLE or pl is None:
            logger.warning("⚠️ Polars not available for batch option processing")
            return options_data
        
        try:
            # Convert to Polars DataFrame
            df = pl.DataFrame(options_data)
            
            # Ensure required columns exist with defaults
            required_cols = {
                'underlying_price': 0.0,
                'strike_price': 0.0,
                'dte_years': 0.0,
                'implied_volatility': 0.0,
                'option_type': 'CE'
            }
            
            for col, default in required_cols.items():
                if col not in df.columns:
                    df = df.with_columns(pl.lit(default).alias(col))
            
            # Calculate Greeks in batch
            df_with_greeks = self.calculate_greeks_batch_vectorized(df)
            
            # Convert back to list of dicts
            return df_with_greeks.to_dicts()
            
        except Exception as e:
            logger.error(f"❌ Batch option processing failed: {e}")
            return options_data

    def calculate_advanced_greeks_batch(self, ticks_df: "pl.DataFrame") -> "pl.DataFrame":
        """
        Batch calculate advanced Greeks (vanna, charm, vomma, speed, zomma, color).
        
        ⚡ Uses FixedAdvancedGreeks for higher-order Greeks calculation in batch.
        
        Args:
            ticks_df: Polars DataFrame with columns:
                      underlying_price, strike_price, dte_years,
                      implied_volatility, option_type
            
        Returns:
            DataFrame with vanna, charm, vomma, speed, zomma, color columns added
        """
        if not POLARS_AVAILABLE or pl is None or ticks_df.is_empty():
            return ticks_df
        
        n_options = len(ticks_df)
        start_time = time.time()
        
        # Ensure advanced Greeks calculator is available
        if not hasattr(self, '_advanced_greeks_fallback') or self._advanced_greeks_fallback is None:
            self._advanced_greeks_fallback = FixedAdvancedGreeks()
        
        # Initialize result lists
        vannas = []
        charms = []
        vommas = []
        speeds = []
        zommas = []
        colors = []
        
        r = 0.05  # Risk-free rate
        
        for row in ticks_df.iter_rows(named=True):
            try:
                S = float(row.get('underlying_price', 0) or 0)
                K = float(row.get('strike_price', 0) or 0)
                T = float(row.get('dte_years', 0) or 0)
                sigma = float(row.get('implied_volatility', 0) or 0)
                opt_type = str(row.get('option_type', 'CE') or 'CE')
                
                if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
                    vannas.append(0.0)
                    charms.append(0.0)
                    vommas.append(0.0)
                    speeds.append(0.0)
                    zommas.append(0.0)
                    colors.append(0.0)
                    continue
                
                # Calculate advanced Greeks using FixedAdvancedGreeks
                adv = self._advanced_greeks_fallback.calculate_advanced_greeks(
                    spot=S, strike=K, time_to_expiry_years=T,
                    risk_free_rate=r, volatility=sigma, option_type=opt_type
                )
                
                vannas.append(adv.get('vanna', 0.0))
                charms.append(adv.get('charm', 0.0))
                vommas.append(adv.get('vomma', 0.0))
                speeds.append(adv.get('speed', 0.0))
                zommas.append(adv.get('zomma', 0.0))
                colors.append(adv.get('color', 0.0))
                
            except Exception as e:
                logger.debug(f"⚠️ Advanced Greek calc failed: {e}")
                vannas.append(0.0)
                charms.append(0.0)
                vommas.append(0.0)
                speeds.append(0.0)
                zommas.append(0.0)
                colors.append(0.0)
        
        # Performance logging
        elapsed_ms = (time.time() - start_time) * 1000
        logger.debug(f"📊 [ADVANCED_GREEKS_BATCH] Calculated {n_options} options in {elapsed_ms:.1f}ms")
        
        # Add columns to DataFrame
        return ticks_df.with_columns([
            pl.Series('vanna', vannas),
            pl.Series('charm', charms),
            pl.Series('vomma', vommas),
            pl.Series('speed', speeds),
            pl.Series('zomma', zommas),
            pl.Series('color', colors),
        ])

    def calculate_all_greeks_batch(self, ticks_df: "pl.DataFrame") -> "pl.DataFrame":
        """
        Calculate ALL Greeks (basic + advanced) in batch.
        
        ⚡ Combines vectorized basic Greeks (delta, gamma, theta, vega) and
        advanced Greeks (vanna, charm, vomma, speed, zomma, color).
        
        Args:
            ticks_df: Polars DataFrame with option parameters
            
        Returns:
            DataFrame with all Greek columns added
        """
        if not POLARS_AVAILABLE or pl is None or ticks_df.is_empty():
            return ticks_df
        
        # Calculate basic Greeks (vectorized if available)
        df_with_basic = self.calculate_greeks_batch_vectorized(ticks_df)
        
        # Calculate advanced Greeks
        df_with_all = self.calculate_advanced_greeks_batch(df_with_basic)
        
        return df_with_all

    def store_volume_ratio_to_redis(self, symbol: str, volume_ratio: float,
                                     timestamp_ms: int = None) -> bool:
        """
        Store volume ratio to Redis for a specific symbol.
        
        ✅ Stores to dedicated indicator hash (ind:{symbol}) with consistent key format.
        
        Args:
            symbol: Canonical symbol (e.g., "NFONIFTY25DEC25300CE")
            volume_ratio: Computed volume ratio value
            timestamp_ms: Optional timestamp in milliseconds
            
        Returns:
            True if stored successfully, False otherwise
        """
        if not self.redis_client:
            logger.debug(f"⚠️ No Redis client for storing volume_ratio of {symbol}")
            return False
        
        try:
            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
            
            # Get canonical indicator key
            indicator_key = RedisKeyStandards.indicator_key(symbol)
            
            # Prepare data
            data = {
                'volume_ratio': str(volume_ratio),
                'volume_ratio_ts': str(timestamp_ms or int(time.time() * 1000))
            }
            
            # Store to Redis hash
            self.redis_client.hset(indicator_key, mapping=fix_numpy_serialization(data))
            
            logger.debug(f"✅ [VOLUME_RATIO] Stored {symbol}: {volume_ratio:.2f} -> {indicator_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to store volume_ratio for {symbol}: {e}")
            return False

    def store_volume_ratios_batch(self, volume_data: List[Dict[str, Any]]) -> int:
        """
        Batch store volume ratios to Redis using pipeline for efficiency.
        
        ⚡ Uses Redis pipeline for reduced round-trips.
        
        Args:
            volume_data: List of dicts with 'symbol' and 'volume_ratio' keys
            
        Returns:
            Number of successfully stored items
        """
        if not self.redis_client or not volume_data:
            return 0
        
        try:
            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
            
            stored_count = 0
            pipe = self.redis_client.pipeline()
            timestamp_ms = int(time.time() * 1000)
            
            for item in volume_data:
                symbol = item.get('symbol')
                volume_ratio = item.get('volume_ratio')
                
                if not symbol or volume_ratio is None:
                    continue
                
                try:
                    indicator_key = RedisKeyStandards.indicator_key(symbol)
                    data = {
                        'volume_ratio': str(float(volume_ratio)),
                        'volume_ratio_ts': str(timestamp_ms)
                    }
                    pipe.hset(indicator_key, mapping=fix_numpy_serialization(data))
                    stored_count += 1
                except Exception:
                    continue
            
            # Execute pipeline
            if stored_count > 0:
                pipe.execute()
                logger.debug(f"⚡ [VOLUME_RATIO_BATCH] Stored {stored_count} volume ratios")
            
            return stored_count
            
        except Exception as e:
            logger.error(f"❌ Batch volume ratio storage failed: {e}")
            return 0

    def _get_risk_free_rate(self, use_cache: bool = True) -> float:
        """Get risk-free rate with caching (1 hour TTL).
        
        ✅ PERFORMANCE FIX: Caches risk-free rate to avoid repeated lookups.
        Rate changes slowly so 1 hour cache is appropriate.
        """
        current_time = time.time()
        
        # Check cache first
        if use_cache and self._risk_free_rate_cache is not None:
            if current_time - self._risk_free_rate_timestamp < self._risk_free_rate_ttl:
                return self._risk_free_rate_cache
        
        # Default risk-free rate (RBI repo rate / T-bill rate)
        # Could be enhanced to fetch from API/Redis in future
        rate = 0.065  # 6.5% - approximate current RBI repo rate
        
        # Update cache
        self._risk_free_rate_cache = rate
        self._risk_free_rate_timestamp = current_time
        
        return rate

    def _get_cached_dte(self, expiry_date, symbol: str = None, use_cache: bool = True) -> Dict[str, Any]:
        """Get DTE with caching (60s TTL).
        
        ✅ PERFORMANCE FIX: Caches DTE calculation to avoid repeated 
        pandas_market_calendars calls. DTE doesn't change within a minute.
        """
        current_time = time.time()
        
        # Create cache key
        expiry_key = str(expiry_date) if expiry_date else 'none'
        cache_key = (symbol, expiry_key)
        
        # Check cache first
        if use_cache and cache_key in self._dte_cache:
            cached_dte, cached_timestamp = self._dte_cache[cache_key]
            if current_time - cached_timestamp < self._dte_cache_ttl:
                return cached_dte
        
        # Calculate using ExpiryCalculator (uses pandas_market_calendars)
        expiry_calc = self._ensure_expiry_calculator(symbol=symbol)
        dte_info = expiry_calc.calculate_dte(expiry_date, symbol)
        
        # Update cache
        self._dte_cache[cache_key] = (dte_info, current_time)
        
        # Cache pruning (prevent memory leak)
        if len(self._dte_cache) > 500:
            cutoff = current_time - 120.0  # Keep last 2 minutes
            self._dte_cache = {k: v for k, v in self._dte_cache.items() if v[1] > cutoff}
        
        return dte_info

    def _get_volume_calculator(self) -> VolumeComputationManager:
        """Return shared volume calculator wired to centralized baselines."""
        if self._volume_calculator is None:
            self._volume_calculator = get_volume_computation_manager(redis_client=getattr(self, 'redis_client', None))
        return self._volume_calculator
    
    def _resolve_volume_ratio(self, symbol: str, tick_data: Optional[Dict[str, Any]]) -> float:
        """
        Ensure volume_ratio is never 0.0 by falling back to the fixed manager.
        """
        base_ratio = _extract_volume_ratio(tick_data or {})
        if base_ratio > 0:
            logger.info(
                f"📈 [VOLUME_RATIO] {symbol} - using crawler ratio {base_ratio:.3f}"
            )
            return float(base_ratio)
        if not tick_data or not self._fixed_volume_manager:
            return float(base_ratio)
        try:
            volume_metrics = self._fixed_volume_manager.calculate_volume_metrics(symbol, tick_data)
            fallback_ratio = float(volume_metrics.get('volume_ratio', 0.0) or 0.0)
            if fallback_ratio > 0:
                self._volume_ratio_fallback_hits += 1
                if self._volume_ratio_fallback_hits % 100 == 1:
                    logger.info(
                        f"✅ [VOLUME_FALLBACK] {symbol} - restored volume_ratio={fallback_ratio:.3f} "
                        f"(total fixes: {self._volume_ratio_fallback_hits})"
                    )
                tick_data['volume_ratio'] = fallback_ratio
                return float(fallback_ratio)
        except Exception as exc:
            logger.warning(f"⚠️ [VOLUME_FALLBACK] Failed to rebuild volume_ratio for {symbol}: {exc}")
        return 0.0
    
    @staticmethod
    def _has_non_zero_values(values: Optional[Dict[str, float]]) -> bool:
        if not values:
            return False
        try:
            return any(abs(float(val)) > 1e-9 for val in values.values())
        except Exception:
            return False
    
    def _enforce_min_float(self, value: Optional[float], default_sign: float = 1.0) -> Optional[float]:
        """
        Guard against underflow from py_vollib when Greeks become extremely small.
        Ensures we never return an exact 0.0 so downstream detectors can still see
        a directional signal.
        """
        if value is None:
            return None
        try:
            sign = math.copysign(1.0, value)
        except Exception:
            sign = 1.0 if default_sign >= 0 else -1.0
        if value == 0.0:
            sign = 1.0 if default_sign >= 0 else -1.0
            return math.copysign(self._min_positive_float, sign)
        if 0 < abs(value) < self._min_positive_float:
            return math.copysign(self._min_positive_float, sign)
        return value

    def _normalize_delta_sign(self, delta_value: Optional[Any], option_type: Optional[str]) -> Optional[float]:
        """Ensure puts carry negative delta and calls positive."""
        if delta_value is None:
            return None
        try:
            delta_float = float(delta_value)
        except (TypeError, ValueError):
            return None
        if not option_type:
            return delta_float
        opt = option_type.lower()
        if opt in ('put', 'pe', 'p'):
            return -abs(delta_float)
        if opt in ('call', 'ce', 'c'):
            return abs(delta_float)
        return delta_float

    def _is_index_symbol_fast(self, symbol: str) -> bool:
        """
        Fast check if symbol is an index (not a tradeable instrument).
        Indices should be skipped for indicator calculations.
        """
        if not symbol:
            return False
        # ✅ FIX: Use replace() to remove ALL quotes (strip only removes edge quotes)
        symbol_upper = str(symbol).upper().replace("'", "").replace('"', "").replace("`", "").strip()
        
        # Known pure index symbols (no CE/PE/FUT suffix)
        pure_indices = {"NIFTY50", "NIFTYBANK", "SENSEX", "INDIAVIX", "FINNIFTY", "MIDCPNIFTY"}
        if symbol_upper in pure_indices:
            return True
        
        # Check with exchange prefix
        for prefix in ("NSE", "BSE", "NFO", "BFO", "NSEB", "BSEB", "BFOB"):
            for idx in pure_indices:
                if symbol_upper == f"{prefix}{idx}":
                    return True
        
        # Check for INDEX: prefix (Redis key format)
        if symbol_upper.startswith("INDEX:") or "INDIAVIX" in symbol_upper:
            return True
            
        return False

    def _decode_all_bytes(self, data: Dict) -> Dict:
        """
        Recursively decode ALL bytes values in a dictionary to strings,
        then convert numeric strings to floats/ints.
        This prevents Polars "casting from BinaryView" and "String incompatible with Float64" errors.
        """
        if not isinstance(data, dict):
            return data
        
        decoded = {}
        for key, value in data.items():
            # Decode key if bytes
            if isinstance(key, bytes):
                key = key.decode('utf-8', errors='ignore')
            
            # Decode value based on type
            if isinstance(value, bytes):
                str_value = value.decode('utf-8', errors='ignore')
                # Try to convert to numeric
                decoded[key] = self._try_numeric_conversion(str_value)
            elif isinstance(value, str):
                # Also convert string values to numeric if possible
                decoded[key] = self._try_numeric_conversion(value)
            elif isinstance(value, dict):
                decoded[key] = self._decode_all_bytes(value)
            elif isinstance(value, list):
                decoded[key] = [
                    self._try_numeric_conversion(item.decode('utf-8', errors='ignore')) 
                    if isinstance(item, bytes) 
                    else self._try_numeric_conversion(item) if isinstance(item, str) else item
                    for item in value
                ]
            else:
                decoded[key] = value
        
        return decoded
    
    def _try_numeric_conversion(self, value: str):
        """Try to convert a string to int or float, return original if not numeric."""
        if not isinstance(value, str):
            return value
        value = value.strip()
        if not value:
            return value
        try:
            # Try int first (for timestamps, volumes, etc.)
            if '.' not in value and 'e' not in value.lower():
                return int(value)
            # Try float
            return float(value)
        except (ValueError, TypeError):
            return value

    def _update_cross_asset_state(self, symbol: Optional[str], tick_data: Dict[str, Any]) -> None:
        if not symbol or not self._cross_asset_calculator:
            return
        price = tick_data.get('last_price') or tick_data.get('last_traded_price')
        if price is None:
            return
        try:
            price_val = float(price)
        except (TypeError, ValueError):
            return
        self._cross_asset_calculator.update_cross_asset_data(symbol, price_val)

    def _calculate_cross_asset_metrics(self, symbol: Optional[str], price: float) -> Dict[str, float]:
        if not symbol or not self._cross_asset_calculator:
            return {}
        symbol_upper = symbol.upper()
        asset = None
        if 'GOLD' in symbol_upper:
            asset = 'GOLD'
        elif 'SILVER' in symbol_upper:
            asset = 'SILVER'
        if not asset:
            return {}
        metrics = self._cross_asset_calculator.calculate_usdinr_sensitivity(
            asset=asset,
            commodity_price=float(price or 0.0),
            usdinr_rate=self._cross_asset_calculator.get_latest_usdinr_rate(),
        )
        if not metrics:
            return {}
        result = {
            'usdinr_sensitivity': metrics.get('usdinr_sensitivity', 0.0),
            'usdinr_correlation': metrics.get('usdinr_correlation', 0.0),
            'usdinr_theoretical_price': metrics.get('theoretical_price', 0.0),
            'usdinr_basis': metrics.get('basis', 0.0),
        }
        logger.info(
            "🌐 [CROSS_ASSET] %s asset=%s sensitivity=%s basis=%s theoretical_price=%s",
            symbol,
            asset,
            result['usdinr_sensitivity'],
            result['usdinr_basis'],
            result['usdinr_theoretical_price'],
        )
        return result

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        """Safely convert value to float, handling strings, integers, bytes, None, and percentages."""
        if value is None:
            return default
            
        # ✅ FIX: Handle bytes from Redis
        if isinstance(value, bytes):
            try:
                value = value.decode('utf-8', errors='ignore')
            except Exception:
                return default
                
        if isinstance(value, str):
            # Remove percentage signs and whitespace
            cleaned = value.strip().replace('%', '')
            # Handle "N/A", "null", "None"
            if cleaned.lower() in ['', 'na', 'null', 'none', 'nan']:
                return default
            try:
                val = float(cleaned)
                # If it was a percentage (but not just % symbol), divide by 100? 
                # Actually, usually "50%" -> 50.0 or 0.5 depending on context. 
                # Standard practice matches user request: just strip % and convert to float.
                return val
            except (ValueError, TypeError):
                return default
                
        if isinstance(value, (int, float)):
            return float(value)
            
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def _safe_int(self, value: Any, default: int = 0) -> int:
        """Safely convert value to int, handling strings, bytes, and None."""
        if value is None:
            return default
        # ✅ FIX: Handle bytes from Redis
        if isinstance(value, bytes):
            try:
                value = value.decode('utf-8', errors='ignore')
            except Exception:
                return default
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            try:
                return int(float(value))  # Convert via float to handle "123.0" strings
            except (ValueError, TypeError):
                return default
        return default

    # ✅ REMOVED: _ensure_numeric_types() - Parser already provides numeric types from binary parsing
    # ✅ REMOVED: _normalize_volume_at_ingestion() - Volume already normalized by intraday_websocket_parser.py
    # Parser handles: volume normalization, numeric type conversion, OHLC flattening

    def _normalize_session_window_schema(self, df: "PolarsDataFrame") -> "PolarsDataFrame": 
        """Ensure session-based window frames use the canonical numeric schema."""
        if not POLARS_AVAILABLE or df is None:
            return df
        try:
            schema_defaults = {
                'timestamp': (pl.Int64, 0),
                'last_price': (pl.Float64, 0.0),
                'high': (pl.Float64, 0.0),
                'low': (pl.Float64, 0.0),
                'open': (pl.Float64, 0.0),
                'close': (pl.Float64, 0.0),
                'average_price': (pl.Float64, 0.0),
                'bucket_incremental_volume': (pl.Int64, 0),
                'bucket_cumulative_volume': (pl.Int64, 0),
                'zerodha_cumulative_volume': (pl.Int64, 0),
                'zerodha_last_traded_quantity': (pl.Int64, 0),
                'volume_ratio': (pl.Float64, 0.0),
                'volume': (pl.Int64, 0),
            }
            normalized_df = df
            for col, (dtype, default_value) in schema_defaults.items():
                if col in normalized_df.columns:
                    # ✅ CRITICAL FIX: Handle string-to-numeric conversion robustly
                    # First check if column is string type, then convert to numeric before casting
                    current_dtype = normalized_df[col].dtype
                    if current_dtype == pl.Utf8 or str(current_dtype) == 'Utf8':
                        # Column is string - try to convert to numeric first
                        if dtype == pl.Float64:
                            # Try to convert string to float, use default if fails
                            expr = pl.col(col).str.strip_chars().cast(pl.Float64, strict=False).fill_nan(default_value).fill_null(default_value)
                        elif dtype == pl.Int64:
                            # Try to convert string to int, use default if fails
                            expr = pl.col(col).str.strip_chars().cast(pl.Int64, strict=False).fill_null(default_value)
                        else:
                            expr = pl.col(col).cast(dtype, strict=False).fill_null(default_value)
                    else:
                        # Column is already numeric - safe to cast
                        expr = pl.col(col).cast(dtype, strict=False)
                        if dtype == pl.Float64:
                            expr = expr.fill_nan(default_value).fill_null(default_value)
                        else:
                            expr = expr.fill_null(default_value)
                    normalized_df = normalized_df.with_columns(expr.alias(col))
                else:
                    normalized_df = normalized_df.with_columns(
                        pl.lit(default_value).cast(dtype).alias(col)
                    )
            desired_cols = list(schema_defaults.keys())
            if 'symbol' in normalized_df.columns:
                desired_cols.append('symbol')
            normalized_df = normalized_df.select(
                [col for col in desired_cols if col in normalized_df.columns]
            )
            return normalized_df
        except Exception:
            return df

    # ✅ REMOVED: _parse_and_flatten_ohlc() - OHLC already flattened by intraday_websocket_parser.py
    # Parser extracts OHLC from binary packets and sets high/low/open/close as top-level fields
    # For historical Redis data, OHLC should already be flattened if stored by parser

    def _to_polars_dataframe(self, data: Union[Dict, List[Dict]], symbol: Optional[str] = None) -> DataFrame:
        """
        Convert data to Polars DataFrame with optimized schema using Zerodha field names.
        
        ✅ ENHANCED: Uses PolarsCache with automatic timestamp normalization and memory optimization.
        ✅ REMOVED: OHLC parsing - already flattened by parser before reaching calculations.py
        No pandas fallback - pure Polars implementation.
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required for HybridCalculations")
        
        # ✅ REMOVED: _parse_and_flatten_ohlc() call - OHLC already flattened by parser
            
        # ✅ ENHANCED: Check cache first (with TTL and LRU)
        cache_key = f"{symbol}_{hash(str(data))}" if symbol else None
        if cache_key:
            cached_df = self._polars_cache.get(cache_key)
            if cached_df is not None:
                return cached_df
        
        # Handle both Dict and List[Dict] inputs
        if isinstance(data, list):
            # Convert list of dicts to Polars DataFrame with explicit schema for type safety
            # ✅ FIXED: Use explicit schema to ensure proper Polars types (prevents string dtype errors)
            # ✅ FIXED: Ensure all required columns exist with defaults to prevent "not found" errors
            schema = {
                'last_price': pl.Float64,
                'high': pl.Float64,
                'low': pl.Float64,
                'open': pl.Float64,
                'close': pl.Float64,
                'average_price': pl.Float64,
                # ✅ ADDED: Alternative field names from tick data
                'open_price': pl.Float64,
                'high_price': pl.Float64,
                'low_price': pl.Float64,
                'close_price': pl.Float64,
                'average_traded_price': pl.Float64,
                'bucket_incremental_volume': pl.Int64,
                'bucket_cumulative_volume': pl.Int64,
                'zerodha_cumulative_volume': pl.Int64,
                'zerodha_last_traded_quantity': pl.Int64,
                'volume': pl.Int64,
                'volume_ratio': pl.Float64,
                'exchange_timestamp_ms': pl.Int64,
                'timestamp_ms': pl.Int64,
                'timestamp': pl.Int64,
                'symbol': pl.Utf8,
            }
            
            # ✅ FIXED: Ensure all required columns exist in data before creating DataFrame
            # Add missing columns with default values to prevent "not found" errors
            normalized_data = []
            for item in data:
                normalized_item = item.copy() if isinstance(item, dict) else {}
                # Add missing required columns with defaults
                for col, dtype in schema.items():
                    if col not in normalized_item or normalized_item[col] is None:
                        if dtype == pl.Int64:
                            normalized_item[col] = 0
                        elif dtype == pl.Float64:
                            normalized_item[col] = 0.0
                        elif dtype == pl.Utf8:
                            normalized_item[col] = symbol if col == 'symbol' and symbol else ''
                        else:
                            normalized_item[col] = 0
                normalized_data.append(normalized_item)
            
            # Create DataFrame and cast to schema (strict=False allows missing columns)
            df = pl.DataFrame(normalized_data)
            # Cast numeric columns to proper types
            for col, dtype in schema.items():
                if col in df.columns:
                    df = df.with_columns(pl.col(col).cast(dtype, strict=False))
                else:
                    # ✅ FIXED: Add missing column with default value
                    if dtype == pl.Int64:
                        df = df.with_columns(pl.lit(0).cast(pl.Int64).alias(col))
                    elif dtype == pl.Float64:
                        df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
                    elif dtype == pl.Utf8:
                        default_val = symbol if col == 'symbol' and symbol else ''
                        df = df.with_columns(pl.lit(default_val).cast(pl.Utf8).alias(col))
        else:
            # Convert dict with lists to Polars DataFrame using explicit schema
            # ✅ FIXED: Use explicit schema to ensure proper Polars types (prevents string dtype errors)
            # ✅ REMOVED: _parse_and_flatten_ohlc() - high/low already available from parser
            
            # Build data dict with proper defaults
            data_dict = {}
            list_length = len(data.get('last_price', [])) if data.get('last_price') else 0
            
            # Define schema for type safety
            schema = {
                'exchange_timestamp_ms': pl.Int64,
                'timestamp_ns_ms': pl.Int64,
                'timestamp_ms': pl.Int64,
                'last_price': pl.Float64,
                'zerodha_cumulative_volume': pl.Int64,
                'zerodha_last_traded_quantity': pl.Int64,
                'bucket_incremental_volume': pl.Int64,
                'bucket_cumulative_volume': pl.Int64,
                'high': pl.Float64,
                'low': pl.Float64,
                'open': pl.Float64,
                'close': pl.Float64,
                'average_price': pl.Float64,
                'exchange_timestamp': pl.Utf8,  # May be string, will be normalized
                'timestamp': pl.Utf8,  # May be string, will be normalized
                'timestamp_ns': pl.Utf8,  # May be string, will be normalized
                'last_trade_time': pl.Utf8,  # May be string
                'last_trade_time_ms': pl.Int64,
                'processed_timestamp_ms': pl.Int64,
                'exchange_timestamp_epoch': pl.Int64,
                'processed_timestamp': pl.Float64,
                'symbol': pl.Utf8,
            }
            
            # Populate data dict with defaults or actual values
            for col, dtype in schema.items():
                if col == 'symbol':
                    data_dict[col] = [symbol] * list_length if symbol and list_length > 0 else []
                else:
                    raw_value = data.get(col, [])
                    if not raw_value and list_length > 0:
                        # Create empty list with proper type default
                        if dtype in [pl.Int64]:
                            data_dict[col] = [0] * list_length
                        elif dtype in [pl.Float64]:
                            data_dict[col] = [0.0] * list_length
                        else:
                            data_dict[col] = [''] * list_length
                    else:
                        data_dict[col] = raw_value
            
            # Create DataFrame with explicit schema
            df = pl.DataFrame(data_dict, schema=schema, strict=False)
        
        # ✅ ENHANCED: Store in cache with optimization enabled (timestamp normalization, compression, etc.)
        if cache_key:
            # optimize=True enables timestamp normalization, compression, categorical encoding
            self._polars_cache.set(cache_key, df, optimize=True)
            # Retrieve optimized version
            df = self._polars_cache.get(cache_key)
        else:
            # If no cache key, still apply optimizations manually
            df = self._polars_cache._optimize_dataframe(df)
            
        return df
    
    def calculate_atr(self, highs: List[float], lows: List[float], 
                     closes: List[float], period: int = 14, symbol: str = None) -> float:
        """ATR using TA-Lib (preferred) with Polars fallback and Redis caching"""
        
        logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} calculate_atr called: highs_len={len(highs)}, lows_len={len(lows)}, closes_len={len(closes)}, period={period}, TALIB_AVAILABLE={TALIB_AVAILABLE}")
        
        # First try Redis fallback
        if hasattr(self, 'redis_client') and self.redis_client:
            redis_atr = get_indicator_from_redis(symbol or "SYMBOL", "atr", self.redis_client)
            if redis_atr is not None:
                logger.debug(f"✅ [TA_LIB_DEBUG] {symbol} ATR retrieved from Redis: {redis_atr}")
                return _ensure_float64(redis_atr)
            else:
                logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} ATR not found in Redis, calculating fresh")
        
        # ✅ PREFERRED: Try TA-Lib first (fastest and most accurate)
        if TALIB_AVAILABLE and len(highs) >= period:
            try:
                np_highs = np.array(highs, dtype=np.float64)
                np_lows = np.array(lows, dtype=np.float64)
                np_closes = np.array(closes, dtype=np.float64)
                
                logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} Attempting TA-Lib ATR calculation: shapes=({np_highs.shape}, {np_lows.shape}, {np_closes.shape}), period={period}")
                atr = talib.ATR(np_highs, np_lows, np_closes, timeperiod=period)
                result = float(atr[-1]) if len(atr) > 0 and not np.isnan(atr[-1]) else 0.0
                if result > 0:
                    return _ensure_float64(result)
                else:
                    logger.debug(f"⚠️ [TA_LIB_DEBUG] {symbol} TA-Lib ATR returned 0 or NaN")
            except Exception as e:
                logger.warning(f"❌ [TA_LIB_DEBUG] {symbol} TA-Lib ATR failed, using fallback: {e}")
                import traceback
                logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} TA-Lib ATR traceback: {traceback.format_exc()}")
        else:
            if not TALIB_AVAILABLE:
                logger.debug(f"⚠️ [TA_LIB_DEBUG] {symbol} TA-Lib not available for ATR")
            if len(highs) < period:
                logger.debug(f"⚠️ [TA_LIB_DEBUG] {symbol} Insufficient data for ATR: {len(highs)} < {period}")
        
        # ✅ FALLBACK 1: Use Polars (pure Python, fast)
        if POLARS_AVAILABLE and len(highs) >= period + 1:
            try:
                df = pl.DataFrame({
                    'high': highs,
                    'low': lows, 
                    'close': closes
                })
                
                # True Range calculation in Polars
                df = df.with_columns([
                    (pl.col('high') - pl.col('low')).alias('tr1'),
                    (pl.col('high') - pl.col('close').shift(1)).abs().alias('tr2'),
                    (pl.col('low') - pl.col('close').shift(1)).abs().alias('tr3')
                ]).with_columns([
                    pl.max_horizontal('tr1', 'tr2', 'tr3').alias('tr')
                ]).with_columns([
                    # ✅ FIXED: ATR as rolling mean (Polars DataFrame operation)
                    pl.col('tr').rolling_mean(window_size=period).alias('atr')
                ])
                
                result = float(df['atr'].tail(1)[0]) if len(df) > 0 else 0.0
                if result > 0:
                    return _ensure_float64(result)
            except Exception as e:
                pass
        
        # ✅ FALLBACK 2: Use pandas_ta (pure Python) - convert Polars to pandas if needed
        if PANDAS_TA_AVAILABLE and len(highs) >= period:
            try:
                # Convert to pandas DataFrame for pandas_ta compatibility
                import pandas as pd
                df = pd.DataFrame({
                    'high': highs,
                    'low': lows,
                    'close': closes
                })
                atr = pta.atr(df['high'], df['low'], df['close'], length=period)
                result = float(atr.iloc[-1]) if len(atr) > 0 and not pd.isna(atr.iloc[-1]) else 0.0
                if result > 0:
                    return _ensure_float64(result)
            except Exception as e:
                pass
        
        # Simple fallback
        return _ensure_float64(self._fallback_atr(highs, lows, closes, period))
    
    def calculate_ema(self, prices: List[float], period: int = 20) -> float:
        """EMA using TA-Lib (preferred) with Polars fallback and Redis caching"""
        # First try Redis fallback
        if hasattr(self, 'redis_client') and self.redis_client:
            redis_ema = get_indicator_from_redis("SYMBOL", f"ema_{period}", self.redis_client)
            if redis_ema is not None:
                return _ensure_float64(redis_ema)
        
        # ✅ PREFERRED: Try TA-Lib first (fastest and most accurate)
        if TALIB_AVAILABLE and len(prices) >= period:
            try:
                np_prices = np.array(prices, dtype=np.float64) if NUMPY_AVAILABLE and np is not None else np.array([], dtype=np.float64)  # type: ignore[union-attr]
                ema = talib.EMA(np_prices, timeperiod=period)
                result = float(ema[-1]) if len(ema) > 0 and not np.isnan(ema[-1]) else float(prices[-1]) if prices else 0.0 if NUMPY_AVAILABLE and np is not None else 0.0  # type: ignore[union-attr]
                if result > 0:
                    return _ensure_float64(result)
            except Exception as e:
                logger.warning(f"TA-Lib EMA failed, using fallback: {e}")
        
        # ✅ FALLBACK 1: Use Polars (pure Python, fast)
        if POLARS_AVAILABLE and len(prices) >= period:
            try:
                series = pl.Series('last_price', prices)
                ema = series.ewm_mean(span=period).tail(1)[0]
                result = float(ema) if ema is not None else float(prices[-1]) if prices else 0.0
                if result > 0:
                    return _ensure_float64(result)
            except Exception as e:
                pass
        
        # ✅ FALLBACK 2: Use pandas_ta (pure Python) - convert Polars to pandas if needed
        if PANDAS_TA_AVAILABLE and len(prices) >= period:
            try:
                # Convert to pandas Series for pandas_ta compatibility
                import pandas as pd
                series = pd.Series(prices)
                ema = pta.ema(series, length=period)
                result = float(ema.iloc[-1]) if len(ema) > 0 and not pd.isna(ema.iloc[-1]) else float(prices[-1]) if prices else 0.0
                if result > 0:
                    return _ensure_float64(result)
            except Exception as e:
                pass
        
        # Simple fallback
        return _ensure_float64(self._fallback_ema(prices, period))
    
    def calculate_ema_5(self, prices: List[float]) -> float:
        """EMA 5 - Short-term trend"""
        return self.calculate_ema(prices, 5)
    
    def calculate_ema_10(self, prices: List[float]) -> float:
        """EMA 10 - Short-term trend"""
        return self.calculate_ema(prices, 10)
    
    def calculate_ema_20(self, prices: List[float]) -> float:
        """EMA 20 - Medium-term trend"""
        return self.calculate_ema(prices, 20)
    
    def calculate_ema_50(self, prices: List[float]) -> float:
        """EMA 50 - Long-term trend"""
        return self.calculate_ema(prices, 50)
    
    def calculate_ema_100(self, prices: List[float]) -> float:
        """EMA 100 - Long-term trend"""
        return self.calculate_ema(prices, 100)
    
    def calculate_ema_200(self, prices: List[float]) -> float:
        """EMA 200 - Very long-term trend"""
        return self.calculate_ema(prices, 200)
    
    def calculate_ema_crossover(self, prices: List[float]) -> Dict:
        """Calculate multiple EMAs for crossover analysis"""
        return {
            'ema_5': self.calculate_ema_5(prices),
            'ema_10': self.calculate_ema_10(prices),
            'ema_20': self.calculate_ema_20(prices),
            'ema_50': self.calculate_ema_50(prices),
            'ema_100': self.calculate_ema_100(prices),
            'ema_200': self.calculate_ema_200(prices)
        }
    
    # ✅ FAST EMA METHODS: Ultra-fast EMA calculations using Numba optimization
    def calculate_ema_fast(self, prices: List[float], period: int = 20) -> float:
        """Ultra-fast EMA calculation using FastEMA class"""
        if not prices:
            return 0.0
            
        # Use FastEMA for single tick processing
        ema_key = f'ema_{period}'
        if ema_key in self.fast_emas:
            fast_ema = self.fast_emas[ema_key]
            if len(prices) == 1:
                return fast_ema.update_fast(prices[0])
            else:
                # Process batch
                np_prices = np.array(prices)
                result = fast_ema.update_batch(np_prices)
                return float(result[-1]) if len(result) > 0 else 0.0
        
        # Fallback to original method
        return self.calculate_ema(prices, period)
    
    def calculate_ema_circular(self, prices: List[float], period: int = 20) -> float:
        """Ultra-fast EMA using circular buffer for maximum performance"""
        if not prices:
            return 0.0
            
        ema_key = f'ema_{period}'
        if ema_key in self.circular_emas:
            circular_ema = self.circular_emas[ema_key]
            for price in prices:
                circular_ema.add_value(price)
            return circular_ema.get_current_ema()
        
        # Fallback to original method
        return self.calculate_ema(prices, period)
    
    def update_ema_for_tick(self, symbol: str, price: float, period: int = 20) -> float:
        """Update EMA for a single tick - O(1) operation"""
        ema_key = f'ema_{period}'
        if ema_key in self.fast_emas:
            return self.fast_emas[ema_key].update_fast(price)
        
        # Fallback: get current window and recalculate
        if symbol in self._symbol_sessions:
            window = self._symbol_sessions[symbol]
            prices = window['last_price'].to_list()
            prices.append(price)
            return self.calculate_ema(prices, period)
        
        return price  # First tick
    
    def get_all_emas_fast(self, symbol: str, price: float) -> Dict:
        """Get all EMA windows for a single tick - ultra-fast"""
        return {
            'ema_5': self.update_ema_for_tick(symbol, price, 5),
            'ema_10': self.update_ema_for_tick(symbol, price, 10),
            'ema_20': self.update_ema_for_tick(symbol, price, 20),
            'ema_50': self.update_ema_for_tick(symbol, price, 50),
            'ema_100': self.update_ema_for_tick(symbol, price, 100),
            'ema_200': self.update_ema_for_tick(symbol, price, 200)
        }
    
    def _should_recalculate(self, symbol: str, current_time: float, new_data: List[Dict]) -> bool:
        """Determine if we need fresh calculations (not from cache)"""
        cache_key = f"{symbol}_indicators"
        
        # No cache? Recalculate
        if cache_key not in self._cache:
            return True
        
        cached = self._cache[cache_key]
        
        # Time expired? Recalculate
        if current_time - cached['timestamp'] >= self._cache_ttl:
            return True
        
        # Data changed significantly? Recalculate
        new_hash = hash(str(new_data))
        if new_hash != cached['data_hash']:
            return True
        
        # Otherwise use cache
        return False
    
    def calculate_rsi(self, prices: List[float], period: int = 14, symbol: str = None) -> float:
        """RSI using TA-Lib (preferred) with Polars fallback and Redis caching"""
        
        logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} calculate_rsi called: prices_len={len(prices)}, period={period}, TALIB_AVAILABLE={TALIB_AVAILABLE}")
        
        # First try Redis fallback
        if hasattr(self, 'redis_client') and self.redis_client:
            redis_rsi = get_indicator_from_redis(symbol or "SYMBOL", "rsi", self.redis_client)
            if redis_rsi is not None:
                logger.debug(f"✅ [TA_LIB_DEBUG] {symbol} RSI retrieved from Redis: {redis_rsi}")
                return _ensure_float64(redis_rsi, default=50.0)
            else:
                logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} RSI not found in Redis, calculating fresh")
        
        # Try TA-Lib first (fastest and most accurate)
        if TALIB_AVAILABLE and len(prices) >= period + 1:
            try:
                np_prices = np.array(prices, dtype=np.float64)
                logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} Attempting TA-Lib RSI calculation: prices_shape={np_prices.shape}, period={period}")
                rsi = talib.RSI(np_prices, timeperiod=period)
                result = float(rsi[-1]) if len(rsi) > 0 and not np.isnan(rsi[-1]) else 50.0
                # TA-Lib RSI calculation successful
                return _ensure_float64(result, default=50.0)
            except Exception as e:
                logger.warning(f"❌ [TA_LIB_DEBUG] {symbol} TA-Lib RSI failed, using fallback: {e}")
                import traceback
                logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} TA-Lib RSI traceback: {traceback.format_exc()}")
        else:
            if not TALIB_AVAILABLE:
                logger.debug(f"⚠️ [TA_LIB_DEBUG] {symbol} TA-Lib not available for RSI")
            if len(prices) < period + 1:
                logger.debug(f"⚠️ [TA_LIB_DEBUG] {symbol} Insufficient data for RSI: {len(prices)} < {period + 1}")
        
        # Fallback to Polars
        if POLARS_AVAILABLE and len(prices) >= period + 1:
            try:
                df = pl.DataFrame({'last_price': prices})
                rsi = df.with_columns([
                    pl.col('last_price').diff().alias('delta')
                ]).with_columns([
                    pl.when(pl.col('delta') > 0)
                      .then(pl.col('delta'))
                      .otherwise(0)
                      .alias('gain'),
                    pl.when(pl.col('delta') < 0) 
                      .then(-pl.col('delta'))
                      .otherwise(0)
                      .alias('loss')
                ]).with_columns([
                    pl.col('gain').rolling_mean(period).alias('avg_gain'),
                    pl.col('loss').rolling_mean(period).alias('avg_loss')
                ]).with_columns([
                    (100 - (100 / (1 + (pl.col('avg_gain') / pl.col('avg_loss'))))).alias('rsi')
                ])
                
                return _ensure_float64(rsi['rsi'].tail(1)[0], default=50.0)
            except Exception as e:
                logger.warning(f"Polars RSI failed, using simple fallback: {e}")
        
        # Simple fallback
        return _ensure_float64(self._fallback_rsi(prices, period), default=50.0)
    
    def calculate_vwap(self, prices: List[float], volumes: List[float]) -> float:
        """Calculate VWAP from price and volume lists"""
        if not prices or not volumes or len(prices) != len(volumes):
            return 0.0
        
        # Note: TA-Lib doesn't have VWAP function, using optimized fallback
        # TA-Lib VWAP is not available in version 0.6.8
        
        # Fallback to simple calculation
        total_volume = 0.0
        weighted_sum = 0.0
        
        for price, volume in zip(prices, volumes):
            if price > 0 and volume > 0:
                total_volume += volume
                weighted_sum += price * volume
        
        return weighted_sum / total_volume if total_volume > 0 else 0.0
    
    def calculate_vwap_batch(self, tick_data: List[Dict]) -> float:
        """VWAP using Polars (preferred) with manual calculation fallback"""
        if not tick_data:
            return 0.0
        
        # ✅ PREFERRED: Try Polars first (fastest and most efficient)
        # Note: TA-Lib does not have VWAP function, so Polars is the primary method
        if POLARS_AVAILABLE:
            try:
                # ✅ REMOVED: _parse_and_flatten_ohlc() - OHLC already flattened by parser
                # ✅ FIXED: Use _to_polars_dataframe to ensure proper Polars types
                df = self._to_polars_dataframe(tick_data)
                # Use Polars timestamp normalization to properly parse timestamps
                df = self._polars_cache._create_normalized_timestamp(df) if self._polars_cache is not None else None
                if df is None:
                    return 0.0

                # VWAP calculation using available volume fields
                # ✅ FIXED: Ensure numeric types before sum operations (prevents string dtype errors)
                volume_col = None
                for col_name in [
                    "zerodha_cumulative_volume",
                    "bucket_incremental_volume",
                    "bucket_cumulative_volume",
                ]:
                    if col_name in df.columns:
                        # ✅ FIXED: Cast to Int64 if string type before sum operation
                        try:
                            if df[col_name].dtype == pl.Utf8:
                                df = df.with_columns(pl.col(col_name).cast(pl.Int64, strict=False)) if df is not None else None
                            # Check if sum > 0 (only works on numeric types)
                            if df[col_name].dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]:
                                if df[col_name].sum() > 0 if df is not None else 0:
                                    volume_col = col_name
                                    break
                        except Exception:
                            continue

                if volume_col:
                    # ✅ FIXED: Ensure last_price is numeric before multiplication
                    if 'last_price' in df.columns if df is not None else False and df['last_price'].dtype == pl.Utf8:
                        df = df.with_columns(pl.col('last_price').cast(pl.Float64, strict=False)) if df is not None else None
                    
                    vwap = df.select([(pl.col("last_price") * pl.col(volume_col)).sum() / pl.col(volume_col).sum()]) if df is not None else 0.0
                else:
                    # Fallback: use simple average if no volume data
                    # ✅ FIXED: Ensure last_price is numeric before mean
                    if df is None:
                        return 0.0
                    if 'last_price' in df.columns and df['last_price'].dtype == pl.Utf8:
                        df = df.with_columns(pl.col('last_price').cast(pl.Float64, strict=False)) if df is not None else None
                    vwap = df["last_price"].mean() if df is not None else 0.0

                return float(vwap) if vwap is not None and isinstance(vwap, float) else 0.0
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Polars VWAP failed, using manual calculation fallback: %s", exc)
        
        # ✅ FALLBACK: Manual calculation (if Polars not available or failed)
        # Note: TA-Lib does not have VWAP function, so we use manual calculation
        if len(tick_data) >= 20:
            try:
                prices: List[float] = []
                volumes: List[float] = []

                for tick in tick_data:
                    price = tick.get("last_price", 0.0)
                    volume = (
                        tick.get("zerodha_cumulative_volume", 0)
                        or tick.get("bucket_incremental_volume", 0)
                        or tick.get("bucket_cumulative_volume", 0)
                        or 0
                    )
                    if price > 0 and volume > 0:
                        prices.append(float(price))
                        volumes.append(float(volume))

                if prices and volumes:
                    weighted_sum = sum(p * v for p, v in zip(prices, volumes))
                    total_volume = sum(volumes)
                    if total_volume > 0:
                        return weighted_sum / total_volume
            except Exception as exc:  # pragma: no cover - defensive
                pass

        # Simple fallback
        return self._fallback_vwap(tick_data)
    
    def _validate_bucket_columns(self, df: "pl.DataFrame") -> "pl.DataFrame":
        """Ensure all required bucket columns exist.
        
        ✅ FIXED: Prevents "bucket_incremental_volume" not found errors
        by ensuring all required columns exist with appropriate defaults.
        """
        required_columns = [
            'bucket_incremental_volume', 
            'bucket_cumulative_volume',
        ]
        
        for col in required_columns:
            if col not in df.columns:
                if col == 'bucket_incremental_volume':
                    # ✅ ALIGNED: Per optimized_field_mapping.yaml, bucket_incremental_volume is pattern INCREMENTAL
                    # Priority: volume (maps to incremental) → incremental_volume → zerodha_last_traded_quantity → 0
                    if 'volume' in df.columns:
                        # Per field mapping: volume maps to bucket_incremental_volume (incremental)
                        df = df.with_columns(
                            pl.col('volume').cast(pl.Int64).alias('bucket_incremental_volume')
                        )
                    elif 'incremental_volume' in df.columns:
                        # Explicit incremental field (canonical)
                        df = df.with_columns(
                            pl.col('incremental_volume').cast(pl.Int64).alias('bucket_incremental_volume')
                        )
                    elif 'zerodha_last_traded_quantity' in df.columns:
                        # Use last traded quantity as fallback (per-tick quantity)
                        df = df.with_columns(
                            pl.col('zerodha_last_traded_quantity').cast(pl.Int64).alias('bucket_incremental_volume')
                        )
                    else:
                        # Default to 0 if no volume data available
                        df = df.with_columns(pl.lit(0).cast(pl.Int64).alias('bucket_incremental_volume'))
                elif col == 'bucket_cumulative_volume':
                    # ✅ ALIGNED: Per optimized_field_mapping.yaml, bucket_cumulative_volume is session cumulative
                    # Priority: zerodha_cumulative_volume (source truth) → calculate from incremental → 0
                    if 'zerodha_cumulative_volume' in df.columns:
                        # Use Zerodha cumulative as bucket cumulative (per field mapping standard)
                        df = df.with_columns(
                            pl.col('zerodha_cumulative_volume').cast(pl.Int64).alias('bucket_cumulative_volume')
                        )
                    elif 'volume_traded_for_the_day' in df.columns:
                        # Legacy alias for zerodha_cumulative_volume
                        df = df.with_columns(
                            pl.col('volume_traded_for_the_day').cast(pl.Int64).alias('bucket_cumulative_volume')
                        )
                    elif 'cumulative_volume' in df.columns:
                        # Legacy cumulative alias
                        df = df.with_columns(
                            pl.col('cumulative_volume').cast(pl.Int64).alias('bucket_cumulative_volume')
                        )
                    elif 'bucket_incremental_volume' in df.columns:
                        # Calculate cumulative from incremental if available (fallback)
                        # Note: This is a fallback - ideally bucket_cumulative_volume should come from websocket parser
                        df = df.with_columns(
                            pl.col('bucket_incremental_volume').cum_sum().over('symbol' if 'symbol' in df.columns else None).alias('bucket_cumulative_volume')
                        )
                    else:
                        # Default to 0 if no cumulative data available
                        df = df.with_columns(pl.lit(0).cast(pl.Int64).alias('bucket_cumulative_volume'))
        
        return df

    def calculate_volume_profile(self, tick_data: Dict, price_bins: int = 20) -> Dict:
        """
        ✅ FIXED: Use correct incremental volume field for display visualization.
        
        ⚠️ IMPORTANT: This is for DISPLAY VISUALIZATION ONLY (bins/volumes for charts).
        For POC (Point of Control) and Value Area calculations, use VolumeProfileManager:
        - Access via: VolumeComputationManager.get_volume_profile_data(symbol)
        - Central source: patterns/volume_profile_manager.py
        
        This method processes a single tick and returns volume profile data that can be cached
        and sent downstream with other indicators, Greeks, and volume data.
        
        Args:
            tick_data: Single tick data dictionary with exact field names
            price_bins: Number of price bins for distribution (default: 20)
            
        Returns:
            Dict with:
            - 'volume': bucket_incremental_volume from tick
            - 'price': last_price from tick
            - 'bins': List of price bins (for display)
            - 'volumes': List of volumes per bin (for display)
            - 'poc_price': POC price (from VolumeProfileManager if available)
            - 'poc_volume': POC volume (from VolumeProfileManager if available)
            - 'symbol': Normalized symbol
        """
        # ✅ SAFEGUARD: Prevent misuse - this method should NOT compute full profile
        assert 'volume_profile_bins' not in tick_data, (
            "volume_profile should NOT be computed here — use VolumeProfileManager.get_volume_profile_data()"
        )
        try:
            # ✅ Use exact field names from tick data
            symbol = tick_data.get('symbol', '')
            if not symbol:
                # Try to get from instrument_token if symbol not available
                instrument_token = tick_data.get('instrument_token')
                if instrument_token:
                    symbol = f"TOKEN_{instrument_token}"
            
            # ✅ CENTRALIZED: Use canonical_symbol for symbol normalization (section 3)
            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
            normalized_symbol = RedisKeyStandards.canonical_symbol(symbol)
            
            # ✅ CORRECT: Use bucket_incremental_volume instead of zerodha_cumulative_volume
            volume_field = resolve_session_field('bucket_incremental_volume') if FIELD_MAPPING_AVAILABLE and resolve_session_field is not None else 'bucket_incremental_volume'
            volume = float(tick_data.get(volume_field) or tick_data.get('bucket_incremental_volume', 0))
            
            # ✅ Use exact field name for price
            price_field = resolve_session_field('last_price') if FIELD_MAPPING_AVAILABLE and resolve_session_field is not None else 'last_price'
            price = float(tick_data.get(price_field) or tick_data.get('last_price', 0))
            
            # Validate data
            if price <= 0 or volume < 0:
                return {
                    'volume': 0.0,
                    'price': 0.0,
                    'bins': [],
                    'volumes': [],
                    'poc_price': 0.0,
                    'poc_volume': 0.0,
                    'symbol': normalized_symbol
                }
            
            # ✅ Get POC data from VolumeProfileManager if available
            poc_price = 0.0
            poc_volume = 0.0
            
            try:
                # Try to get POC from VolumeProfileManager via VolumeComputationManager
                if hasattr(self, 'redis_client') and self.redis_client:
                    from shared_core.volume_files.volume_computation_manager import get_volume_computation_manager
                    volume_manager = get_volume_computation_manager(redis_client=self.redis_client)
                    if volume_manager and volume_manager.volume_profile_manager:
                        volume_nodes = volume_manager.get_volume_profile_data(normalized_symbol)
                        if volume_nodes:
                            poc_price = float(volume_nodes.get('poc_price', 0))
                            poc_volume = float(volume_nodes.get('poc_volume', 0))
            except Exception as e:
                logger.debug(f"Could not get POC from VolumeProfileManager for {normalized_symbol}: {e}")
            
            # For single tick, create simple bin structure
            # The actual volume profile bins are accumulated over time
            # This returns the current tick's contribution
            volume_bins = [price]  # Single price level for this tick
            volume_volumes = [volume]  # Volume at this price level
            
            return {
                'volume': volume,
                'price': price,
                'bins': volume_bins,
                'volumes': volume_volumes,
                'poc_price': poc_price,
                'poc_volume': poc_volume,
                'symbol': normalized_symbol
            }
            
        except Exception as e:
            logger.error(f"Volume profile calculation failed: {e}")
            return {
                'volume': 0.0,
                'price': 0.0,
                'bins': [],
                'volumes': [],
                'poc_price': 0.0,
                'poc_volume': 0.0,
                # ✅ CENTRALIZED: Use canonical_symbol for symbol normalization (section 3)
                'symbol': RedisKeyStandards.canonical_symbol(tick_data.get('symbol', '')) if tick_data and tick_data.get('symbol') else ''
            }
    
    def calculate_macd(self, prices: List[float], fast_period: int = 12, 
                      slow_period: int = 26, signal_period: int = 9, symbol: str = None) -> Dict:
        """MACD using TA-Lib (preferred) with Polars fallback and Redis caching"""
        
        logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} calculate_macd called: prices_len={len(prices)}, periods=({fast_period}, {slow_period}, {signal_period}), TALIB_AVAILABLE={TALIB_AVAILABLE}")
        
        # First try Redis fallback
        if hasattr(self, 'redis_client') and self.redis_client:
            redis_macd = get_indicator_from_redis(symbol or "SYMBOL", "macd", self.redis_client)
            if redis_macd is not None:
                logger.debug(f"✅ [TA_LIB_DEBUG] {symbol} MACD retrieved from Redis: {redis_macd}")
                if isinstance(redis_macd, dict):
                    return _normalize_macd_payload(redis_macd)
                else:
                    return _normalize_macd_payload({'macd': redis_macd, 'signal': 0.0, 'histogram': 0.0})
            else:
                logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} MACD not found in Redis, calculating fresh")
        
        # Try TA-Lib first (fastest and most accurate)
        if TALIB_AVAILABLE and len(prices) >= slow_period:
            try:
                np_prices = np.array(prices, dtype=np.float64)
                logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} Attempting TA-Lib MACD calculation: prices_shape={np_prices.shape}, periods=({fast_period}, {slow_period}, {signal_period})")
                macd, macd_signal, macd_hist = talib.MACD(np_prices, 
                                                         fastperiod=fast_period,
                                                         slowperiod=slow_period, 
                                                         signalperiod=signal_period)
                
                result = _normalize_macd_payload({
                    'macd': float(macd[-1]) if len(macd) > 0 and not np.isnan(macd[-1]) else 0.0,
                    'signal': float(macd_signal[-1]) if len(macd_signal) > 0 and not np.isnan(macd_signal[-1]) else 0.0,
                    'histogram': float(macd_hist[-1]) if len(macd_hist) > 0 and not np.isnan(macd_hist[-1]) else 0.0
                })
                # TA-Lib MACD calculation successful
                return result
            except Exception as e:
                logger.warning(f"❌ [TA_LIB_DEBUG] {symbol} TA-Lib MACD failed, using fallback: {e}")
                import traceback
                logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} TA-Lib MACD traceback: {traceback.format_exc()}")
        else:
            if not TALIB_AVAILABLE:
                logger.debug(f"⚠️ [TA_LIB_DEBUG] {symbol} TA-Lib not available for MACD")
            if len(prices) < slow_period:
                logger.debug(f"⚠️ [TA_LIB_DEBUG] {symbol} Insufficient data for MACD: {len(prices)} < {slow_period}")
        
        # Fallback to Polars
        if POLARS_AVAILABLE and len(prices) >= slow_period:
            try:
                df = pl.DataFrame({'last_price': prices})
                
                # Calculate EMAs
                ema_fast = df['last_price'].ewm_mean(span=fast_period)
                ema_slow = df['last_price'].ewm_mean(span=slow_period)
                
                # MACD line
                macd_line = ema_fast - ema_slow
                
                # Signal line (EMA of MACD)
                signal_line = macd_line.ewm_mean(span=signal_period)
                
                # Histogram
                histogram = macd_line - signal_line
                
                return _normalize_macd_payload({
                    'macd': float(macd_line.tail(1)[0]) if macd_line.tail(1)[0] is not None else 0.0,
                    'signal': float(signal_line.tail(1)[0]) if signal_line.tail(1)[0] is not None else 0.0,
                    'histogram': float(histogram.tail(1)[0]) if histogram.tail(1)[0] is not None else 0.0
                })
            except Exception as e:
                logger.warning(f"Polars MACD failed, using simple fallback: {e}")
        
        # Simple fallback
        return _normalize_macd_payload(self._fallback_macd(prices, fast_period, slow_period, signal_period))
    
    def calculate_bollinger_bands(self, prices: List[float], period: int = 20, 
                                 std_dev: float = 2.0, symbol: str = '') -> Dict:
        """Bollinger Bands using TA-Lib (preferred) with Polars fallback and Redis caching"""
        # First try Redis fallback
        if hasattr(self, 'redis_client') and self.redis_client:
            redis_bb = get_indicator_from_redis(symbol or "SYMBOL", "bollinger_bands", self.redis_client)
            if redis_bb is not None:
                if isinstance(redis_bb, dict):
                    return {
                        'upper': _ensure_float64(redis_bb.get('upper', 0.0)),
                        'middle': _ensure_float64(redis_bb.get('middle', 0.0)),
                        'lower': _ensure_float64(redis_bb.get('lower', 0.0)),
                    }
                else:
                    return {
                        'upper': _ensure_float64(redis_bb),
                        'middle': 0.0,
                        'lower': 0.0,
                    }
        
        # Try TA-Lib first (fastest and most accurate)
        if TALIB_AVAILABLE and len(prices) >= period:
            try:
                np_prices = np.array(prices, dtype=np.float64)
                upper, middle, lower = talib.BBANDS(np_prices, timeperiod=period, 
                                                  nbdevup=std_dev, nbdevdn=std_dev, 
                                                  matype=0)
                
                return {
                    'upper': _ensure_float64(upper[-1] if len(upper) > 0 else 0.0),
                    'middle': _ensure_float64(middle[-1] if len(middle) > 0 else 0.0),
                    'lower': _ensure_float64(lower[-1] if len(lower) > 0 else 0.0),
                }
            except Exception as e:
                logger.warning(f"TA-Lib Bollinger Bands failed, using fallback: {e}")
        
        # Fallback to Polars
        if POLARS_AVAILABLE and len(prices) >= period:
            try:
                df = pl.DataFrame({'last_price': prices})
                
                # Calculate moving average and standard deviation
                middle_band = df['last_price'].rolling_mean(period)
                std = df['last_price'].rolling_std(period)
                
                # Bollinger Bands
                upper_band = middle_band + (std * std_dev)
                lower_band = middle_band - (std * std_dev)
                
                return {
                    'upper': _ensure_float64(upper_band.tail(1)[0]),
                    'middle': _ensure_float64(middle_band.tail(1)[0]),
                    'lower': _ensure_float64(lower_band.tail(1)[0]),
                }
            except Exception as e:
                logger.warning(f"Polars Bollinger Bands failed, using simple fallback: {e}")
        
        # Simple fallback
        fallback_result = self._fallback_bollinger_bands(prices, period, std_dev)
        return {
            'upper': _ensure_float64(fallback_result.get('upper', 0.0)),
            'middle': _ensure_float64(fallback_result.get('middle', 0.0)),
            'lower': _ensure_float64(fallback_result.get('lower', 0.0)),
        }
    
    # ✅ SINGLE SOURCE OF TRUTH: Use _get_volume_ratio() which reads pre-computed volume_ratio from tick_data
    # Volume ratio is calculated by websocket_parser using VolumeComputationManager and stored in tick_data

    def calculate_volume_statistics(self, volumes: List[float]) -> Dict[str, float]:
        """
        Calculate robust volume statistics for real-world incremental volume data
        
        Handles the actual characteristics of volume data:
        - Extreme variance: 35 to 14,668,800 (400,000x difference)
        - Many zero values (no trading activity)
        - Uses percentiles instead of mean/std for robustness
        - Spike detection based on 90th percentile threshold
        
        Args:
            volumes: List of historical incremental volume values
            
        Returns:
            Dictionary with median, percentiles, spike detection, and confidence
        """
        if not volumes or len(volumes) < 2:
            return {
                'median_volume': 0.0,
                'percentile_75': 0.0,
                'percentile_90': 0.0,
                'is_spike': False,
                'spike_ratio': 1.0,
                'confidence_level': 0.0
            }
        
        # Filter out zeros and extreme outliers for robust statistics
        non_zero_volumes = [v for v in volumes if v > 0]
        if not non_zero_volumes:
            return {
                'median_volume': 0.0,
                'percentile_75': 0.0,
                'percentile_90': 0.0,
                'is_spike': False,
                'spike_ratio': 1.0,
                'confidence_level': 0.0
            }
        
        # Sort for percentile calculations
        sorted_volumes = sorted(non_zero_volumes)
        n = len(sorted_volumes)
        
        # Calculate robust statistics (percentiles instead of mean/std)
        median_volume = sorted_volumes[n // 2]
        percentile_75 = sorted_volumes[int(n * 0.75)]
        percentile_90 = sorted_volumes[int(n * 0.90)]
        
        # Current volume (last in list)
        current_volume = volumes[-1]
        
        # Spike detection using percentile-based thresholds
        # Spike if current volume > 90th percentile
        is_spike = current_volume > percentile_90
        
        # Calculate spike ratio relative to median
        spike_ratio = current_volume / median_volume if median_volume > 0 else 1.0
        
        # Confidence level based on how far above median
        if current_volume > percentile_90:
            confidence_level = 0.9
        elif current_volume > percentile_75:
            confidence_level = 0.7
        elif current_volume > median_volume:
            confidence_level = 0.5
        else:
            confidence_level = 0.3
        
        return {
            'median_volume': median_volume,
            'percentile_75': percentile_75,
            'percentile_90': percentile_90,
            'is_spike': is_spike,
            'spike_ratio': spike_ratio,
            'confidence_level': confidence_level
        }

    def calculate_volume_spike_detection(self, symbol: str, current_volume: float, 
                                       historical_volumes: List[float], 
                                       price_change_pct: float = 0.0,
                                       confidence_threshold: float = 0.95,
                                       min_price_move: float = 0.005) -> Dict[str, any]:
        """
        ✅ FIXED: Volume spike detection requires BOTH high volume AND significant price movement
        
        Uses percentile-based thresholds instead of Z-scores due to:
        - Extreme variance in volume values (35 to 14M+)
        - Many zero values affecting statistical calculations
        - Percentiles more robust than mean/std for this data type
        
        Args:
            symbol: Symbol name
            current_volume: Current incremental volume value
            historical_volumes: List of historical incremental volumes
            price_change_pct: Current price change percentage (REQUIRED for spike detection)
            confidence_threshold: Confidence level (0.95 = 90th percentile, 0.90 = 75th percentile)
            min_price_move: Minimum price movement required (default 0.005 = 0.5%)
            
        Returns:
            Dictionary with spike analysis using percentile-based methods
        """
        if not historical_volumes or len(historical_volumes) < 5:
            # Not enough data for analysis
            avg_volume = sum(historical_volumes) / len(historical_volumes) if historical_volumes else 0
            volume_condition = current_volume > avg_volume * 3.0 if avg_volume > 0 else False
            price_condition = abs(price_change_pct) >= min_price_move
            
            return {
                'is_spike': volume_condition and price_condition,  # ✅ BOTH conditions required
                'confidence': 0.5,
                'method': 'simple_threshold',
                'spike_ratio': current_volume / avg_volume if avg_volume > 0 else 1.0,
                'statistical_significance': 'low',
                'price_change_pct': price_change_pct,
                'price_condition_met': price_condition
            }
        
        # Calculate robust statistics
        stats = self.calculate_volume_statistics(historical_volumes + [current_volume])
        
        # Determine volume spike based on percentile thresholds
        if confidence_threshold >= 0.95:
            # High confidence: must be above 90th percentile
            volume_spike = current_volume > stats['percentile_90']
        elif confidence_threshold >= 0.90:
            # Medium confidence: must be above 75th percentile
            volume_spike = current_volume > stats['percentile_75']
        else:
            # Low confidence: must be above median
            volume_spike = current_volume > stats['median_volume']
        
        # ✅ CRITICAL: Check price movement requirement
        price_condition = abs(price_change_pct) >= min_price_move
        
        # ✅ BOTH conditions must be true for a valid spike
        is_spike = volume_spike and price_condition
        
        if not price_condition:
            logger.debug(f"⚠️ [VOLUME_SPIKE] {symbol} - Volume spike detected but insufficient price movement: {price_change_pct:.4f}% < {min_price_move:.4f}%")
        
        # Determine statistical significance based on spike ratio
        if stats['spike_ratio'] > 10.0:
            significance = 'very_high'
        elif stats['spike_ratio'] > 5.0:
            significance = 'high'
        elif stats['spike_ratio'] > 2.0:
            significance = 'medium'
        else:
            significance = 'low'
        
        return {
            'is_spike': is_spike,  # ✅ BOTH volume AND price conditions required
            'volume_condition_met': volume_spike,
            'price_condition_met': price_condition,
            'confidence': confidence_threshold,
            'method': 'percentile_based',
            'spike_ratio': stats['spike_ratio'],
            'median_volume': stats['median_volume'],
            'percentile_90': stats['percentile_90'],
            'statistical_significance': significance,
            'price_change_pct': price_change_pct,
            'min_price_move': min_price_move
            # ❌ REMOVED: 'volume_ratio': stats['spike_ratio'] - WRONG!
            # spike_ratio = current_volume / median_volume (for spike detection)
            # volume_ratio = current_volume / baseline_volume (from VolumeComputationManager)
            # These are DIFFERENT calculations - don't override volume_ratio!
        }

    @staticmethod
    def _compute_microprice(bid: float, ask: float, bid_size: float, ask_size: float) -> float:
        """Weighted mid-price using liquidity imbalance."""
        bid_size = float(bid_size or 0.0)
        ask_size = float(ask_size or 0.0)
        if bid_size + ask_size == 0:
            return (float(bid or 0.0) + float(ask or 0.0)) / 2.0
        total = bid_size + ask_size
        return ((bid_size / total) * float(ask or 0.0)) + ((ask_size / total) * float(bid or 0.0))

    @staticmethod
    def _compute_order_book_imbalance(bid_size: float, ask_size: float) -> float:
        """Normalized order book imbalance (-1 to 1)."""
        bid_size = float(bid_size or 0.0)
        ask_size = float(ask_size or 0.0)
        if bid_size + ask_size == 0:
            return 0.0
        return (bid_size - ask_size) / (bid_size + ask_size)

    def _calculate_microstructure_metrics(self, symbol: Optional[str], tick_data: Dict[str, Any]) -> Dict[str, float]:
        """
        ✅ INSTITUTIONAL-GRADE: Thread-safe microstructure calculation.
        
        Public method that handles validation and acquires lock before state access.
        Delegates actual calculation to internal method that assumes lock is held.
        
        Microstructure metrics include:
        - microprice, order_flow_imbalance (OFI), order_book_imbalance (OBI)
        - spread_absolute, spread_bps, depth_imbalance
        - cumulative_volume_delta (CVD), best_bid_size, best_ask_size
        - total_bid_depth, total_ask_depth
        """
        if not symbol:
            return {}
        
        # ✅ CHECK: If microstructure metrics already exist from websocket_parser, return them
        existing_metrics = {
            'microprice': tick_data.get('microprice'),
            'order_flow_imbalance': tick_data.get('order_flow_imbalance'),
            'order_book_imbalance': tick_data.get('order_book_imbalance'),
            'spread_absolute': tick_data.get('spread_absolute'),
            'spread_bps': tick_data.get('spread_bps'),
            'depth_imbalance': tick_data.get('depth_imbalance'),
            'cumulative_volume_delta': tick_data.get('cumulative_volume_delta'),
            'best_bid_size': tick_data.get('best_bid_size'),
            'best_ask_size': tick_data.get('best_ask_size'),
            'total_bid_depth': tick_data.get('total_bid_depth'),
            'total_ask_depth': tick_data.get('total_ask_depth'),
        }
        
        # Check if critical microstructure metrics are present (microprice and OFI are key indicators)
        critical_metrics_present = (
            existing_metrics['microprice'] is not None and
            existing_metrics['order_flow_imbalance'] is not None and
            existing_metrics['order_book_imbalance'] is not None
        )
        
        if critical_metrics_present:
            logger.debug(f"✅ [MICROSTRUCTURE] {symbol} Using pre-computed microstructure metrics from websocket_parser")
            # Return all existing metrics, filling in missing ones with calculated values if needed
            result = {}
            for key, value in existing_metrics.items():
                if value is not None:
                    try:
                        result[key] = float(value)
                    except (TypeError, ValueError):
                        pass
            
            # If some metrics are missing, calculate only those (but this is rare)
            if len(result) < 8:  # If less than 8 metrics present, calculate missing ones
                logger.debug(f"⚠️ [MICROSTRUCTURE] {symbol} Some microstructure metrics missing, calculating missing ones")
            else:
                return result
        
        # ✅ THREAD-SAFE: Acquire lock only when we need to access/modify state
        # All validation and early returns happen before lock acquisition for better performance
        with self._microstructure_lock:
            return self._calculate_microstructure_metrics_internal(symbol, tick_data, existing_metrics)

    def _calculate_microstructure_metrics_internal(self, symbol: str, tick_data: Dict[str, Any], existing_metrics: Dict[str, Any]) -> Dict[str, float]:
        """
        Internal implementation - assumes caller holds _microstructure_lock.
        
        Performs all microstructure calculations with state tracking.
        This method should only be called while holding the lock.
        
        Args:
            symbol: Trading symbol (guaranteed non-None by caller)
            tick_data: Raw tick data dictionary
            existing_metrics: Pre-extracted existing metrics for fallback
        
        Returns:
            Dictionary of calculated microstructure metrics
        """
        # Only calculate if missing - continue with calculation logic below
        depth = tick_data.get('depth') or {}
        buy_side = depth.get('buy') or []
        sell_side = depth.get('sell') or []
        if not buy_side or not sell_side:
            # Return existing metrics if available, even if depth is missing
            if existing_metrics.get('microprice') is not None:
                return {k: float(v) for k, v in existing_metrics.items() if v is not None}
            return {}

        best_bid = buy_side[0].get('price')
        best_ask = sell_side[0].get('price')
        best_bid_size = float(buy_side[0].get('quantity', 0.0))
        best_ask_size = float(sell_side[0].get('quantity', 0.0))
        if best_bid is None or best_ask is None:
            return {}

        microprice = self._compute_microprice(best_bid, best_ask, best_bid_size, best_ask_size)
        
        # ✅ STATE ACCESS: Lock is held by caller, safe to access/modify state
        state = self._microstructure_state[symbol]
        prev_bid = state.get('bid_size', 0.0)
        prev_ask = state.get('ask_size', 0.0)
        order_flow_imbalance = (best_bid_size - prev_bid) - (best_ask_size - prev_ask)

        order_book_imbalance = self._compute_order_book_imbalance(best_bid_size, best_ask_size)

        spread_absolute = float(best_ask) - float(best_bid)
        spread_bps = ((spread_absolute / float(best_bid)) * 10000.0) if best_bid else 0.0

        total_bid_depth = float(sum(level.get('quantity', 0.0) for level in buy_side[:5]))
        total_ask_depth = float(sum(level.get('quantity', 0.0) for level in sell_side[:5]))
        depth_total = total_bid_depth + total_ask_depth
        depth_imbalance = ((total_bid_depth - total_ask_depth) / depth_total) if depth_total else 0.0

        last_price = float(
            tick_data.get('last_price')
            or tick_data.get('last_traded_price')
            or tick_data.get('close')
            or 0.0
        )
        last_quantity = float(
            tick_data.get('zerodha_last_traded_quantity')
            or tick_data.get('last_traded_quantity')
            or tick_data.get('last_quantity')
            or 0.0
        )
        current_cvd = float(state.get('cvd', 0.0))
        if last_price >= microprice:
            current_cvd += last_quantity
        else:
            current_cvd -= last_quantity

        # ✅ ATOMIC UPDATE: All state updates happen within the lock (held by caller)
        state['bid_size'] = best_bid_size
        state['ask_size'] = best_ask_size
        state['cvd'] = current_cvd

        return {
            'microprice': float(microprice),
            'order_flow_imbalance': float(order_flow_imbalance),
            'order_book_imbalance': float(order_book_imbalance),
            'spread_absolute': float(spread_absolute),
            'spread_bps': float(spread_bps),
            'depth_imbalance': float(depth_imbalance),
            'cumulative_volume_delta': float(current_cvd),
            'best_bid_size': float(best_bid_size),
            'best_ask_size': float(best_ask_size),
            'total_bid_depth': float(total_bid_depth),
            'total_ask_depth': float(total_ask_depth),
        }

    def _calculate_advanced_greeks(self, tick_data: Dict[str, Any], base_greeks: Dict[str, Any]) -> Dict[str, float]:
        """Calculate higher order Greeks (vanna, charm, vomma, speed, zomma, color)."""
        symbol = tick_data.get('symbol', '')
        if not symbol or not self._is_option_symbol(symbol):
            logger.debug(f"⚠️ [ADVANCED_GREEKS] {symbol} - Not an option symbol, skipping")
            return {}

        spot = float(tick_data.get('underlying_price') or tick_data.get('last_price') or base_greeks.get('last_price', 0.0) or 0.0)
        strike = float(tick_data.get('strike_price') or 0.0)
        dte_years = float(
            base_greeks.get('dte_years')
            or tick_data.get('dte_years')
            or 0.0
        )
        iv = float(
            tick_data.get('implied_volatility')
            or tick_data.get('iv')
            or tick_data.get('volatility')
            or base_greeks.get('implied_volatility', 0.2)
            or 0.2
        )
        option_type = str(tick_data.get('option_type') or tick_data.get('instrument_type') or 'call')
        risk_free_rate = 0.05

        # ✅ CRITICAL: Log inputs for debugging
        logger.debug(
            f"🔍 [ADVANCED_GREEKS] {symbol} - Inputs: spot={spot:.2f}, strike={strike:.2f}, "
            f"dte_years={dte_years:.6f}, iv={iv:.4f}, option_type={option_type}"
        )

        if spot <= 0 or strike <= 0 or dte_years <= 0 or iv <= 0:
            logger.warning(
                f"⚠️ [ADVANCED_GREEKS] {symbol} - Invalid inputs, returning empty: "
                f"spot={spot:.2f}, strike={strike:.2f}, dte_years={dte_years:.6f}, iv={iv:.4f}"
            )
            return {}

        result = self._calculate_higher_order_greeks(
            spot=spot,
            strike=strike,
            dte_years=dte_years,
            risk_free_rate=risk_free_rate,
            volatility=iv,
            option_type=option_type
        )
        # ✅ FALLBACK: If vollib path fails, compute using deterministic fallback
        if (not self._has_non_zero_values(result)) and getattr(self, "_advanced_greeks_fallback", None):
            try:
                fallback = self._advanced_greeks_fallback.calculate_advanced_greeks(
                    spot=spot,
                    strike=strike,
                    time_to_expiry_years=dte_years,
                    risk_free_rate=risk_free_rate,
                    volatility=iv,
                    option_type=option_type,
                )
                if self._has_non_zero_values(fallback):
                    logger.info(f"✅ [ADVANCED_GREEKS] {symbol} - Fallback calculator restored values")
                    result = fallback
            except Exception as fallback_exc:
                logger.warning(f"⚠️ [ADVANCED_GREEKS] Fallback calculator failed for {symbol}: {fallback_exc}")
        
        # ✅ Log result summary
        if self._has_non_zero_values(result):
            logger.info(f"✅ [ADVANCED_GREEKS] {symbol} - Calculated: {list(result.keys())}")
        else:
            logger.warning(f"⚠️ [ADVANCED_GREEKS] {symbol} - All values are 0.0")
        
        return result

    def _calculate_higher_order_greeks(
        self,
        spot: float,
        strike: float,
        dte_years: float,
        risk_free_rate: float,
        volatility: float,
        option_type: str,
    ) -> Dict[str, float]:
        """Numerically approximate high-order Greeks using py_vollib primitives."""
        metrics = {
            'vanna': 0.0,
            'charm': 0.0,
            'vomma': 0.0,
            'speed': 0.0,
            'zomma': 0.0,
            'color': 0.0,
        }
        if not GREEK_CALCULATIONS_AVAILABLE:
            logger.warning(f"⚠️ [ADVANCED_GREEKS] GREEK_CALCULATIONS_AVAILABLE=False, returning zeros")
            return metrics

        # ✅ CRITICAL: Validate inputs before calculation
        if spot <= 0 or strike <= 0 or dte_years <= 0 or volatility <= 0:
            logger.warning(
                f"⚠️ [ADVANCED_GREEKS] Invalid inputs: spot={spot:.2f}, strike={strike:.2f}, "
                f"dte_years={dte_years:.6f}, volatility={volatility:.4f} - returning zeros"
            )
            return metrics

        try:
            # ✅ CRITICAL FIX: Ensure black_scholes_* functions are defined before use
            # Check both module-level and local scope
            if not PY_VOLLIB_AVAILABLE:
                logger.error(f"❌ [ADVANCED_GREEKS] PY_VOLLIB_AVAILABLE=False, returning zeros")
                return metrics
            
            # Verify functions exist (handle both import success and failure cases)
            try:
                # Try to access the function to verify it exists
                _ = black_scholes_delta
            except NameError:
                logger.error(f"❌ [ADVANCED_GREEKS] black_scholes_delta not in scope, returning zeros")
                return metrics
            
            flag = 'c' if str(option_type).lower() in ('ce', 'call') else 'p'
            eps_vol = max(0.0005, volatility * 0.01)
            eps_price = max(0.1, spot * 0.001)
            eps_time = min(dte_years / 10.0, 1.0 / 365.0)

            # ✅ FIXED: Use black_scholes_delta, black_scholes_gamma, black_scholes_vega (imported from py_vollib)
            # These are the actual function names when py_vollib is available
            # ✅ Calculate Vanna: dDelta/dVol
            delta_plus = black_scholes_delta(flag, spot, strike, dte_years, risk_free_rate, volatility + eps_vol)
            delta_minus = black_scholes_delta(flag, spot, strike, dte_years, risk_free_rate, max(volatility - eps_vol, 1e-6))
            metrics['vanna'] = (delta_plus - delta_minus) / (2 * eps_vol)

            # ✅ Calculate Charm: dDelta/dTime
            delta_time_plus = black_scholes_delta(flag, spot, strike, dte_years + eps_time, risk_free_rate, volatility)
            delta_time_minus = black_scholes_delta(flag, spot, strike, max(dte_years - eps_time, 1e-6), risk_free_rate, volatility)
            metrics['charm'] = (delta_time_plus - delta_time_minus) / (2 * eps_time)

            # ✅ Calculate Vomma: dVega/dVol
            vega_plus = black_scholes_vega(flag, spot, strike, dte_years, risk_free_rate, volatility + eps_vol)
            vega_minus = black_scholes_vega(flag, spot, strike, dte_years, risk_free_rate, max(volatility - eps_vol, 1e-6))
            metrics['vomma'] = (vega_plus - vega_minus) / (2 * eps_vol)

            # ✅ Calculate Speed: dGamma/dPrice
            gamma_price_plus = black_scholes_gamma(flag, spot + eps_price, strike, dte_years, risk_free_rate, volatility)
            gamma_price_minus = black_scholes_gamma(flag, max(spot - eps_price, 1e-6), strike, dte_years, risk_free_rate, volatility)
            metrics['speed'] = (gamma_price_plus - gamma_price_minus) / (2 * eps_price)

            # ✅ Calculate Zomma: dGamma/dVol
            gamma_vol_plus = black_scholes_gamma(flag, spot, strike, dte_years, risk_free_rate, volatility + eps_vol)
            gamma_vol_minus = black_scholes_gamma(flag, spot, strike, dte_years, risk_free_rate, max(volatility - eps_vol, 1e-6))
            metrics['zomma'] = (gamma_vol_plus - gamma_vol_minus) / (2 * eps_vol)

            # ✅ Calculate Color: dGamma/dTime
            gamma_time_plus = black_scholes_gamma(flag, spot, strike, dte_years + eps_time, risk_free_rate, volatility)
            gamma_time_minus = black_scholes_gamma(flag, spot, strike, max(dte_years - eps_time, 1e-6), risk_free_rate, volatility)
            metrics['color'] = (gamma_time_plus - gamma_time_minus) / (2 * eps_time)
            
            # ✅ Log successful calculation with values
            for key in metrics:
                metrics[key] = self._enforce_min_float(metrics[key])
            logger.info(
                f"✅ [ADVANCED_GREEKS] Calculated successfully: "
                f"vanna={metrics['vanna']:.6f}, charm={metrics['charm']:.6f}, vomma={metrics['vomma']:.6f}, "
                f"speed={metrics['speed']:.6f}, zomma={metrics['zomma']:.6f}, color={metrics['color']:.6f}"
            )
        except Exception as exc:
            logger.error(
                f"❌ [ADVANCED_GREEKS] Calculation failed: {exc} | "
                f"Inputs: spot={spot:.2f}, strike={strike:.2f}, dte_years={dte_years:.6f}, "
                f"volatility={volatility:.4f}, option_type={option_type}, risk_free_rate={risk_free_rate}",
                exc_info=True
            )

        return metrics

    def _get_option_chain_from_redis(self, underlying: str) -> List[Dict[str, Any]]:
        """Fetch option chain snapshot for an underlying symbol from Redis."""
        if not underlying:
            return []
        client = getattr(self, 'redis_client', None)
        if client is None:
            return []
        redis_conn = client.get_client(1) if hasattr(client, 'get_client') else client
        if redis_conn is None:
            return []

        key_variants = [
            f"option_chain:{underlying}",
            f"option_chain:{underlying.upper()}",
            f"ind:option_chain:{underlying}",
            f"live:option_chain:{underlying}",
        ]

        for key in key_variants:
            try:
                if hasattr(redis_conn, 'get'):
                    raw = redis_conn.get(key)
                else:
                    raw = redis_conn.execute_command('GET', key) if hasattr(redis_conn, 'execute_command') else None
                if not raw:
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode('utf-8')
                data = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    for candidate in ('options', 'chain', 'data'):
                        value = data.get(candidate)
                        if isinstance(value, list):
                            return value
            except Exception as exc:
                logger.debug(f"Option chain fetch failed for {underlying}: {exc}")
                continue

        return []

    def _ensure_expiry_calculator(self, exchange: str = None, symbol: str = None):
        """
        Ensure ExpiryCalculator is initialized for the correct exchange.
    
        Args:
            exchange: Exchange name (NSE, NFO, MCX, BSE). If None, will try to infer from symbol.
            symbol: Symbol to infer exchange from if exchange is not provided.
    
        Returns:
            ExpiryCalculator instance for the specified exchange, or default (NSE) if not found.
        """
        # If exchange is provided, use it directly
        if exchange:
            exchange_upper = exchange.upper()
            if hasattr(self, 'expiry_calculators') and exchange_upper in self.expiry_calculators:
                return self.expiry_calculators[exchange_upper]
    
        # Try to infer exchange from symbol
        if symbol and not exchange:
            # Extract exchange prefix from symbol (e.g., "NFO:NIFTY..." -> "NFO")
            if ':' in symbol:
                exchange_prefix = symbol.split(':', 1)[0].upper()
                # Map exchange prefixes to calculator keys
                exchange_map = {
                    'NSE': 'NSE',
                    'NFO': 'NFO',  # NFO uses NSE calendar but we have a separate entry
                    'BSE': 'BSE',
                    'BFO': 'BSE',  # BFO uses BSE calendar
                    'MCX': 'MCX',
                    'CDS': 'NSE',  # CDS uses NSE calendar
                }
                mapped_exchange = exchange_map.get(exchange_prefix, 'NSE')
                if hasattr(self, 'expiry_calculators') and mapped_exchange in self.expiry_calculators:
                    return self.expiry_calculators[mapped_exchange]
    
        # Fallback: ensure default calculator exists
        if not hasattr(self, '_expiry_calc') or self._expiry_calc is None:
            if hasattr(self, 'expiry_calculators') and 'NSE' in self.expiry_calculators:
                self._expiry_calc = self.expiry_calculators['NSE']
            else:
                self._expiry_calc = ExpiryCalculator('NSE')
    
        return self._expiry_calc

    # ✅ MERGED: Greek calculation methods from EnhancedGreekCalculator and UnifiedGreeksCalculator

    def calculate_greeks_for_tick_data_fast(self, tick_data: dict) -> dict:
        """
        🚨 EMERGENCY: Fast Greek calculation without vollib.
        Uses analytic BSM formulas with fast IV approximation.
        """
        import time
        import math
        
        start = time.perf_counter_ns()
        
        # Extract parameters
        S = tick_data.get('underlying_price', tick_data.get('spot_price', 0))
        K = tick_data.get('strike_price', tick_data.get('strike', 0))
        T = tick_data.get('dte_years', tick_data.get('time_to_expiry', 0.1))
        r = 0.05  # Fixed 5% for emergency
        option_price = tick_data.get('last_price', tick_data.get('option_price', 0))
        option_type = tick_data.get('option_type', 'call').lower()
        symbol = tick_data.get('symbol', 'UNKNOWN')
        
        is_call = option_type in ['call', 'ce', 'c']
        
        # 🚨 FAST IV CALCULATION
        iv = self._calculate_iv_approximation(option_price, S, K, T, option_type, symbol)
        if iv is None:
            iv = 0.3  # Default
        
        # 🚨 FAST GREEK CALCULATION (analytic BSM)
        if S <= 0 or K <= 0 or T <= 0 or iv <= 0:
            return {'delta': 0.5 if is_call else -0.5, 'gamma': 0, 'theta': 0, 'vega': 0, 'iv': 0.3, '_emergency': True}
        
        sqrt_T = math.sqrt(T)
        d1 = (math.log(S / K) + (r + 0.5 * iv**2) * T) / (iv * sqrt_T)
        d2 = d1 - iv * sqrt_T
        
        # Standard normal CDF (approximation)
        def norm_cdf(x):
            return 1.0 / (1.0 + math.exp(-0.07056 * x**3 - 1.5976 * x))
        
        N_d1 = norm_cdf(d1)
        N_d2 = norm_cdf(d2)
        n_d1 = math.exp(-0.5 * d1**2) / math.sqrt(2 * math.pi)  # PDF
        
        # Calculate Greeks
        if is_call:
            delta = N_d1
            gamma = n_d1 / (S * iv * sqrt_T) if S > 0 else 0
            theta = (-(S * n_d1 * iv) / (2 * sqrt_T) - r * K * math.exp(-r * T) * N_d2) / 365.0
        else:  # put
            delta = N_d1 - 1
            gamma = n_d1 / (S * iv * sqrt_T) if S > 0 else 0
            theta = (-(S * n_d1 * iv) / (2 * sqrt_T) + r * K * math.exp(-r * T) * (1 - N_d2)) / 365.0
        
        vega = S * n_d1 * sqrt_T / 100.0  # Per 1% change in IV
        
        normalized_delta = self._normalize_delta_sign(delta, 'call' if is_call else 'put')
        greeks = {
            'delta': round(normalized_delta if normalized_delta is not None else delta, 4),
            'gamma': round(gamma, 6),
            'theta': round(theta, 4),
            'vega': round(vega, 4),
            'iv': round(iv, 4),
            '_emergency': True,
            '_calc_time_us': (time.perf_counter_ns() - start) / 1000
        }
        
        total_time = (time.perf_counter_ns() - start) / 1000000  # ms
        if total_time > 10:
            logger.debug(f"🚨 [FAST_GREEKS] {symbol}: {total_time:.2f}ms")
        
        return greeks

    def calculate_greeks_for_tick_data(self, tick_data: dict, risk_free_rate: float = 0.05, 
                                    volatility: float = 0.2) -> dict:
        """Calculate Greeks from tick data - merged from EnhancedGreekCalculator"""

        symbol = tick_data.get('symbol', 'UNKNOWN')

        try:
            """EMERGENCY: Log cache hits/misses with timing."""
            import time
            start = time.perf_counter_ns()
            
            # EXTRACT KEY PARAMETERS
            symbol = tick_data.get('symbol', 'UNKNOWN')
            underlying = tick_data.get('underlying_price')
            strike = tick_data.get('strike_price')
            
            # LOG FOR EVERY TICK (temporarily)
            # print(f"🔍 [GREEK_DIAG] {symbol}: U={underlying}, K={strike}")
            
            # CHECK CACHE STATS
            # if hasattr(self, '_iv_cache'):
            #     print(f"   IV Cache size: {len(self._iv_cache)}")
            # if hasattr(self, '_greek_cache'):
            #     print(f"   Greek Cache size: {len(self._greek_cache)}")

            # ✅ PERFORMANCE FIX: Check Greek cache first (5 seconds TTL)
            current_time = time.time()
            option_price = tick_data.get('last_price') or tick_data.get('ltp') or 0.0
            
            # ✅ CRITICAL FIX: Cache key should NOT include option_price
            # It changes every tick, preventing cache hits
            # Greeks don't change dramatically within 500ms just because price changed
            greek_cache_key = (
                symbol,
                round(tick_data.get('strike_price', 0.0), 0),  # Strike doesn't change
                str(tick_data.get('expiry_date', '')),
                tick_data.get('option_type', '')
            )
            
            if greek_cache_key in self._greek_cache:
                cached_greeks, cache_timestamp = self._greek_cache[greek_cache_key]
                if current_time - cache_timestamp < self._greek_cache_ttl:
                    cached_greeks['_cached'] = 1  # Use int instead of bool for Redis
                    cached_greeks['_cache_age_ms'] = (current_time - cache_timestamp) * 1000
                    logger.debug(f"✅ [GREEK_CACHE_HIT] {symbol} - Using cached Greeks (age: {cached_greeks['_cache_age_ms']:.1f}ms)")
                    return cached_greeks

            spot_price = tick_data.get('underlying_price', 0.0)
            strike_price = tick_data.get('strike_price', 0.0)
            
            # ✅ FIX: Convert to float to prevent "str <= int" comparison errors
            spot_price = self._safe_float(spot_price, 0.0)
            strike_price = self._safe_float(strike_price, 0.0)
            
            # ✅ CRITICAL FIX: Don't default to 'call' - check if option_type is actually present and valid
            option_type = tick_data.get('option_type')
            if not option_type or option_type not in ('call', 'put', 'CE', 'PE', 'c', 'p'):
                option_type = None  # Mark as missing so extraction will run
            expiry_date = tick_data.get('expiry_date')
        
            logger.info(f"🔍 [GREEKS_DEEP_DEBUG] {symbol} - calculate_greeks_for_tick_data called")
            logger.info(f"📊 [GREEKS_DEEP_DEBUG] {symbol} - Initial inputs: spot_price={spot_price}, strike_price={strike_price}, option_type={option_type}, volatility={volatility}, risk_free_rate={risk_free_rate}, expiry_date={expiry_date}")
        
            # ✅ CRITICAL FIX: Extract strike_price and option_type from symbol if missing
            # MUST happen BEFORE validation check
            if not strike_price or strike_price <= 0 or not option_type:
                logger.info(f"🔍 [GREEKS_DEEP_DEBUG] {symbol} - Attempting to extract strike_price/option_type from symbol: {symbol}")
                parsed_strike, parsed_type = self._extract_strike_and_type_from_symbol(symbol)
                logger.info(f"🔍 [GREEKS_DEEP_DEBUG] {symbol} - Parser returned: strike={parsed_strike}, type={parsed_type}")
                if parsed_strike and parsed_strike > 0:
                    if not strike_price or strike_price <= 0:
                        strike_price = parsed_strike
                        tick_data['strike_price'] = strike_price
                        logger.info(f"✅ [GREEKS_DEEP_DEBUG] {symbol} - Extracted strike_price={strike_price} from symbol")
                    if not option_type or option_type not in ('call', 'put'):
                        if parsed_type:  # Only use parsed_type if it's not None
                            option_type = parsed_type
                            tick_data['option_type'] = option_type
                            logger.info(f"✅ [GREEKS_DEEP_DEBUG] {symbol} - Extracted option_type={option_type} from symbol")
                        else:
                            logger.warning(f"⚠️ [GREEKS_DEEP_DEBUG] {symbol} - Parser returned None for option_type, cannot extract")
                else:
                    logger.warning(f"⚠️ [GREEKS_DEEP_DEBUG] {symbol} - Failed to extract strike/type from symbol. Parser returned: strike={parsed_strike}, type={parsed_type}")

            if not option_type:
                suffix_type = identify_option_type(symbol)
                if suffix_type == 'CE':
                    option_type = 'call'
                elif suffix_type == 'PE':
                    option_type = 'put'
                if option_type:
                    tick_data['option_type'] = option_type

            # ✅ CRITICAL FIX: Normalize option_type (CE/PE -> call/put)
            if option_type:
                option_type_lower = option_type.lower()
                if option_type_lower in ('ce', 'c'):
                    option_type = 'call'
                elif option_type_lower in ('pe', 'p'):
                    option_type = 'put'
                tick_data['option_type'] = option_type
        
            logger.info(f"📊 [GREEKS_DEEP_DEBUG] {symbol} - After extraction: spot_price={spot_price}, strike_price={strike_price}, option_type={option_type}")
        
            # ✅ ARCHITECTURAL FIX: Trust underlying_price from tick_data (provided by CalculationService)
            # CalculationService stores underlying price in cache BEFORE calling this method
            # Only use Redis fallback if absolutely necessary (should rarely happen)
            if not spot_price or spot_price <= 0:
                logger.warning(
                    f"⚠️ [GREEKS_DEEP_DEBUG] {symbol} - Invalid or missing spot_price "
                    f"({spot_price}) in tick_data. This should not happen with write-first cache."
                )
                # Last resort: try Redis (but this indicates architectural issue)
                fallback_price = None
                underlying_symbol = extract_underlying_from_symbol(symbol)
                if underlying_symbol and self.redis_client:
                    fallback_price = self._get_underlying_price_from_redis(underlying_symbol, symbol)
                if fallback_price and fallback_price > 0:
                    spot_price = fallback_price
                    tick_data['underlying_price'] = spot_price
                    tick_data['spot_price'] = spot_price
                    logger.warning(
                        f"⚠️ [GREEKS_DEEP_DEBUG] {symbol} - Resolved spot_price via Redis fallback: {spot_price} "
                        f"(This should be provided by CalculationService)"
                    )
                else:
                    logger.error(
                        f"❌ [GREEKS_DEEP_DEBUG] {symbol} - Cannot calculate Greeks, "
                        f"spot_price remains invalid ({spot_price}) after all fallbacks"
                    )
                    return self._get_default_greeks()
        
            if strike_price <= 0:
                logger.warning(f"⚠️ [GREEKS_DEEP_DEBUG] {symbol} - Invalid inputs: spot_price={spot_price}, strike_price={strike_price}, returning default Greeks")
                return self._get_default_greeks()
        
            # ✅ FIX: Extract or CALCULATE IV (don't use hardcoded 0.2)
            if volatility == 0.2:
                # Try to extract IV from tick_data first
                iv = tick_data.get('iv') or tick_data.get('implied_volatility') or tick_data.get('volatility')
                if iv:
                    try:
                        iv = float(iv)
                        if 0 < iv <= 5.0:
                            volatility = iv
                            logger.info(f"✅ [GREEKS_DEEP_DEBUG] {symbol} - Using IV from tick_data: {volatility:.4f}")
                    except (ValueError, TypeError):
                        pass
            
                # ✅ NEW: If still default, CALCULATE IV from market price
                if volatility == 0.2:
                    market_price = tick_data.get('last_price') or tick_data.get('ltp') or tick_data.get('current_price')
                    if market_price and market_price > 0:
                        logger.info(f"🔍 [GREEKS_DEEP_DEBUG] {symbol} - No IV in tick_data, calculating from market_price={market_price}")
                    
                        # Get expiry_date for DTE calculation (needed for IV calc)
                        if not expiry_date:
                            expiry_date = tick_data.get('expiry_date')
                            if isinstance(expiry_date, str):
                                try:
                                    expiry_date = datetime.fromisoformat(expiry_date.replace('Z', '+00:00'))
                                except Exception:
                                    expiry_date = None
                    
                        # ✅ FIX: Extract expiry from symbol via ExpiryCalculator if still None
                        if not expiry_date:
                            try:
                                expiry_calc = self._ensure_expiry_calculator(symbol=symbol)
                                parsed_expiry = expiry_calc.parse_expiry_from_symbol(symbol)
                                if parsed_expiry:
                                    expiry_date = parsed_expiry
                                    # ✅ FIXED: Handle both datetime and string expiry_date
                                    if isinstance(expiry_date, datetime):
                                        expiry_date_str = expiry_date.date().isoformat()
                                    elif isinstance(expiry_date, str):
                                        expiry_date_str = expiry_date
                                    else:
                                        expiry_date_str = str(expiry_date)
                                    logger.info(f"✅ [GREEKS_DEEP_DEBUG] {symbol} - Extracted expiry from symbol for IV calc: {expiry_date_str}")
                            except Exception as parse_err:
                                logger.warning(f"⚠️ [GREEKS_DEEP_DEBUG] {symbol} - Failed to parse expiry for IV calc: {parse_err}")
                    
                        # Calculate DTE
                        if expiry_date:
                            expiry_calc = self._ensure_expiry_calculator(symbol=symbol)
                            dte_info = expiry_calc.calculate_dte(expiry_date, symbol)
                            dte_years = dte_info['trading_dte'] / 365.0
                        
                            logger.info(f"🔍 [GREEKS_DEEP_DEBUG] {symbol} - DTE for IV calc: trading_dte={dte_info['trading_dte']}, dte_years={dte_years:.4f}")
                        
                            # ✅ FIX: Ensure all required fields are in tick_data for _calculate_iv_from_option_price
                            # Store expiry and DTE
                            tick_data['expiry_date'] = expiry_date
                            tick_data['trading_dte'] = dte_info['trading_dte']
                            tick_data['dte_years'] = dte_years
                            
                            # ✅ CRITICAL: Ensure option_price is in tick_data (function looks for last_price, ltp, or close_price)
                            if market_price and market_price > 0:
                                if 'last_price' not in tick_data or not tick_data.get('last_price'):
                                    tick_data['last_price'] = market_price
                                if 'ltp' not in tick_data or not tick_data.get('ltp'):
                                    tick_data['ltp'] = market_price
                            
                            # ✅ CRITICAL: Ensure underlying_price and spot_price are in tick_data
                            if spot_price and spot_price > 0:
                                tick_data['underlying_price'] = spot_price
                                tick_data['spot_price'] = spot_price
                            
                            # ✅ CRITICAL: Ensure strike_price and option_type are in tick_data
                            if strike_price and strike_price > 0:
                                tick_data['strike_price'] = strike_price
                            if option_type:
                                tick_data['option_type'] = option_type
                            
                            logger.debug(f"🔍 [GREEKS_DEEP_DEBUG] {symbol} - tick_data keys before IV calc: {list(tick_data.keys())}")
                            logger.debug(f"🔍 [GREEKS_DEEP_DEBUG] {symbol} - IV calc inputs: option_price={market_price}, spot={spot_price}, strike={strike_price}, dte={dte_years:.4f}")
                        
                            # ✅ VALIDATION: Ensure we have all required inputs before attempting IV calculation
                            if not market_price or market_price <= 0:
                                logger.warning(f"⚠️ [GREEKS_DEEP_DEBUG] {symbol} - Cannot calculate IV: missing or invalid market_price={market_price}")
                            elif not spot_price or spot_price <= 0:
                                logger.warning(f"⚠️ [GREEKS_DEEP_DEBUG] {symbol} - Cannot calculate IV: missing or invalid spot_price={spot_price}")
                            elif not strike_price or strike_price <= 0:
                                logger.warning(f"⚠️ [GREEKS_DEEP_DEBUG] {symbol} - Cannot calculate IV: missing or invalid strike_price={strike_price}")
                            elif dte_years <= 0:
                                logger.warning(f"⚠️ [GREEKS_DEEP_DEBUG] {symbol} - Cannot calculate IV: invalid dte_years={dte_years}")
                            elif dte_years > 0:
                                # Try Method 1: Use existing IV calculation function
                                calculated_iv = self._calculate_iv_from_option_price(tick_data, symbol)
                                if calculated_iv and 0 < calculated_iv <= 5.0:
                                    volatility = calculated_iv
                                    # ✅ CRITICAL: Store IV in tick_data so it's included in Greeks result
                                    tick_data['iv'] = calculated_iv
                                    tick_data['implied_volatility'] = calculated_iv
                                    logger.info(f"✅ [GREEKS_DEEP_DEBUG] {symbol} - Calculated IV (vollib): {volatility:.4f}")
                                else:
                                    # Method 2: Use Newton-Raphson directly
                                    try:
                                        calculated_iv = self.calculate_implied_volatility(
                                            market_price=market_price,
                                            spot_price=spot_price,
                                            strike_price=strike_price,
                                            dte_years=dte_years,
                                            risk_free_rate=risk_free_rate,
                                            option_type=option_type
                                        )
                                        if calculated_iv and 0 < calculated_iv <= 5.0:
                                            volatility = calculated_iv
                                            # ✅ CRITICAL: Store IV in tick_data so it's included in Greeks result
                                            tick_data['iv'] = calculated_iv
                                            tick_data['implied_volatility'] = calculated_iv
                                            logger.info(f"✅ [GREEKS_DEEP_DEBUG] {symbol} - Calculated IV (Newton): {volatility:.4f}")
                                    except Exception as calc_err:
                                        logger.warning(f"⚠️ [GREEKS_DEEP_DEBUG] {symbol} - IV calculation failed: {calc_err}, using default 0.2")
        
            # Convert expiry_date if it's a string
            if isinstance(expiry_date, str):
                try:
                    expiry_date = datetime.fromisoformat(expiry_date.replace('Z', '+00:00'))
                except Exception:
                    expiry_date = None
        
            # ✅ FIX: Extract expiry from symbol via ExpiryCalculator if still None
            if not expiry_date:
                try:
                    expiry_calc = self._ensure_expiry_calculator(symbol=symbol)
                    parsed_expiry = expiry_calc.parse_expiry_from_symbol(symbol)
                    if parsed_expiry:
                        expiry_date = parsed_expiry
                        # ✅ FIXED: Handle both datetime and string expiry_date
                        if isinstance(expiry_date, datetime):
                            expiry_date_str = expiry_date.date().isoformat()
                        elif isinstance(expiry_date, str):
                            expiry_date_str = expiry_date
                        else:
                            expiry_date_str = str(expiry_date)
                        logger.info(f"✅ [GREEKS_DEEP_DEBUG] {symbol} - Extracted expiry from symbol: {expiry_date_str}")
                except Exception as parse_err:
                    logger.warning(f"⚠️ [GREEKS_DEEP_DEBUG] {symbol} - Failed to parse expiry: {parse_err}")
        
            logger.info(f"🔍 [GREEKS_DEEP_DEBUG] {symbol} - Calling calculate_greeks_with_dte with expiry_date={expiry_date}")
            greeks_result = self.calculate_greeks_with_dte(
                spot_price=spot_price,
                strike_price=strike_price,
                expiry_date=expiry_date,
                symbol=symbol,
                risk_free_rate=risk_free_rate,
                volatility=volatility,
                option_type=option_type
            )
            normalized_delta_main = self._normalize_delta_sign(
                greeks_result.get('delta'),
                option_type or tick_data.get('option_type')
            )
            if normalized_delta_main is not None:
                greeks_result['delta'] = normalized_delta_main
            
            # ✅ CRITICAL: Ensure IV is included in greeks_result (from tick_data if calculated)
            if tick_data.get('iv') or tick_data.get('implied_volatility'):
                iv_value = tick_data.get('iv') or tick_data.get('implied_volatility')
                # ✅ FIX: Convert to float to prevent format code error
                iv_value = self._safe_float(iv_value, 0.0)
                greeks_result['iv'] = iv_value
                greeks_result['implied_volatility'] = iv_value
                # ✅ CRITICAL: Store IV to Redis immediately after calculation
                self._store_iv_to_redis(symbol, iv_value, ttl=300)
                logger.info(f"✅ [GREEKS_DEEP_DEBUG] {symbol} - IV stored to Redis: {iv_value:.4f}")
            elif volatility and volatility != 0.2:  # If we calculated IV but didn't store it in tick_data
                # ✅ FIX: Ensure volatility is float
                volatility = self._safe_float(volatility, 0.2)
                greeks_result['iv'] = volatility
                greeks_result['implied_volatility'] = volatility
                # ✅ CRITICAL: Store IV to Redis immediately after calculation
                self._store_iv_to_redis(symbol, volatility, ttl=300)
                logger.info(f"✅ [GREEKS_DEEP_DEBUG] {symbol} - IV stored to Redis: {volatility:.4f}")
            
            logger.info(f"✅ [GREEKS_DEEP_DEBUG] {symbol} - calculate_greeks_with_dte returned: delta={greeks_result.get('delta')}, gamma={greeks_result.get('gamma')}, theta={greeks_result.get('theta')}, vega={greeks_result.get('vega')}, rho={greeks_result.get('rho')}, iv={greeks_result.get('iv')}, implied_volatility={greeks_result.get('implied_volatility')}")
        
            # ✅ FIXED: Calculate and include advanced Greeks (vanna, charm, vomma, speed, zomma, color)
            # Create a tick_data dict for _calculate_advanced_greeks
            advanced_greeks_tick_data = {
                'symbol': symbol,
                'underlying_price': spot_price,
                'strike_price': strike_price,
                'option_type': option_type,
                'expiry_date': expiry_date,
                'dte_years': greeks_result.get('dte_years', greeks_result.get('calendar_dte', 0) / 365.0),
                'implied_volatility': greeks_result.get('iv') or volatility,
                'iv': greeks_result.get('iv') or volatility
            }
        
            advanced_greeks = self._calculate_advanced_greeks(advanced_greeks_tick_data, greeks_result)
            if advanced_greeks:
                greeks_result.update(advanced_greeks)
                logger.info(f"✅ [GREEKS_DEEP_DEBUG] {symbol} - Advanced Greeks calculated: {list(advanced_greeks.keys())}")
        
            # ✅ FIXED: Calculate and include gamma_exposure for options
            if symbol and self._is_option_symbol(symbol):
                gamma_exposure = self._calculate_gamma_exposure(symbol)
                if gamma_exposure is not None:
                    greeks_result['gamma_exposure'] = gamma_exposure
                    logger.info(f"✅ [GREEKS_DEEP_DEBUG] {symbol} - Gamma exposure calculated: {gamma_exposure:.2f}")
        
            # ✅ PERFORMANCE FIX: Cache full Greek result before returning
            greeks_result['_cached'] = 0  # Use int instead of bool for Redis
            greeks_result['_cache_timestamp'] = current_time
            self._greek_cache[greek_cache_key] = (greeks_result.copy(), current_time)
            
            # Cache pruning (prevent memory leak)
            if len(self._greek_cache) > 500:
                cutoff = current_time - 5.0  # Keep last 5 seconds
                self._greek_cache = {k: v for k, v in self._greek_cache.items() if v[1] > cutoff}
            
            # DIAGNOSTIC TIMING
            total_time = (time.perf_counter_ns() - start) / 1000000  # ms
            if total_time > 50:
                print(f"   🚨 TOTAL GREEK TIME: {total_time:.2f}ms")
            
            return greeks_result
        
        except Exception as e:
            # DIAGNOSTIC TIMING (Exception path)
            total_time = (time.perf_counter_ns() - start) / 1000000  # ms
            print(f"   ❌ FAILED in {total_time:.2f}ms: {e}")

            logger.error(f"❌ [GREEKS_DEEP_DEBUG] {symbol} - Greek calculation from tick data failed: {e}")
            import traceback
            logger.error(f"❌ [GREEKS_DEEP_DEBUG] {symbol} - Traceback: {traceback.format_exc()}")
            return self._get_default_greeks()

    def calculate_usdinr_indicators(self, symbol: str, tick_data: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate USDINR-related indicators for commodities (GOLD, SILVER).

        ✅ ARCHITECTURAL FIX: Uses tick_data directly, no Redis reads for current price.
        CalculationService provides tick_data with all required fields.

        Args:
            symbol: Trading symbol (should contain GOLD or SILVER)
            tick_data: Tick data dictionary with last_price and other fields

        Returns:
            Dictionary with USDINR sensitivity, correlation, theoretical price, and basis
        """
        if not symbol or not tick_data:
            return {}

        symbol_upper = symbol.upper()
        if 'USDINR' not in symbol_upper and 'GOLD' not in symbol_upper and 'SILVER' not in symbol_upper:
            return {}

        try:
            # Get price from tick_data (provided by CalculationService)
            price = tick_data.get('last_price') or tick_data.get('last_traded_price') or tick_data.get('ltp')
            if not price:
                return {}

            price_val = float(price)

            # Update cross-asset state with current tick
            self._update_cross_asset_state(symbol, tick_data)

            # Calculate cross-asset metrics (includes USDINR sensitivity)
            metrics = self._calculate_cross_asset_metrics(symbol, price_val)

            return metrics

        except Exception as e:
            logger.debug(f"⚠️ USDINR indicators calculation error for {symbol}: {e}")
            return {}

    def calculate_greeks_with_dte(self, spot_price: float, strike_price: float,
                                  expiry_date: datetime = None, symbol: str = None,
                                  risk_free_rate: float = 0.05, volatility: float = 0.2,
                                  option_type: str = 'call') -> dict:
        """Calculate Greeks with proper DTE handling - merged from EnhancedGreekCalculator"""
        if not GREEK_CALCULATIONS_AVAILABLE:
            return self._get_default_greeks()

        # ✅ FIXED: Use _ensure_expiry_calculator() helper method with symbol for exchange inference
        expiry_calc = self._ensure_expiry_calculator(symbol=symbol)

        # Calculate proper DTE
        dte_info = expiry_calc.calculate_dte(expiry_date, symbol)
        calendar_dte = dte_info['calendar_dte']
        trading_dte = dte_info['trading_dte']

        # Use trading DTE for more accurate calculations
        effective_dte = trading_dte / 365.0

        greeks = self.black_scholes_greeks(
            spot_price, strike_price, effective_dte,
            risk_free_rate, volatility, option_type
        )

        # ✅ CRITICAL: Add IV to Greeks dict (it's calculated separately but needs to be included)
        # IV is the volatility parameter used for calculations, store it as both 'iv' and 'implied_volatility'
        return {
            **greeks,
            'iv': volatility,  # Store the volatility used for calculation
            'implied_volatility': volatility,  # Alias for compatibility
            'calendar_dte': calendar_dte,
            'trading_dte': trading_dte,
            'expiry_date': dte_info['expiry_date'],
            'expiry_series': dte_info.get('expiry_series', 'UNKNOWN')
        }

    def black_scholes_greeks(self, spot: float, strike: float, dte_years: float,
                             risk_free: float, volatility: float, option_type: str) -> dict:
        """Optimized Black-Scholes Greek calculations using py_vollib - merged from EnhancedGreekCalculator"""
        if dte_years <= 0 or volatility <= 0:
            return self._get_default_greeks()

        if not PY_VOLLIB_AVAILABLE:
            return self._fallback_manual_greeks(spot, strike, dte_years, risk_free, volatility, option_type)

        try:
            # ✅ CRITICAL FIX: Verify delta/gamma/theta/vega/rho are defined before use
            if 'delta' not in globals() or not callable(globals().get('delta')):
                logger.warning("⚠️ delta function not available, using fallback")
                return self._fallback_manual_greeks(spot, strike, dte_years, risk_free, volatility, option_type)
            
            flag = 'c' if option_type.lower() in ['call', 'ce'] else 'p'
            option_price = black_scholes(flag, spot, strike, dte_years, risk_free, volatility)
            raw_delta_val = delta(flag, spot, strike, dte_years, risk_free, volatility)
            normalized_delta_val = self._normalize_delta_sign(raw_delta_val, option_type)
            delta_val = normalized_delta_val if normalized_delta_val is not None else raw_delta_val
            gamma_val = gamma(flag, spot, strike, dte_years, risk_free, volatility)
            theta_val = theta(flag, spot, strike, dte_years, risk_free, volatility)
            vega_val = vega(flag, spot, strike, dte_years, risk_free, volatility)
            rho_val = rho(flag, spot, strike, dte_years, risk_free, volatility)
            is_call = option_type.lower() in ['call', 'ce']
            return {
                'delta': self._enforce_min_float(delta_val, default_sign=1.0 if is_call else -1.0),
                'gamma': self._enforce_min_float(gamma_val, default_sign=1.0),
                'theta': self._enforce_min_float(theta_val, default_sign=-1.0 if is_call else 1.0),
                'vega': self._enforce_min_float(vega_val, default_sign=1.0),
                'rho': self._enforce_min_float(rho_val, default_sign=1.0 if is_call else -1.0),
                'dte_years': dte_years,
                'option_price': option_price
            }
        except (NameError, Exception) as e:
            logger.debug(f"py_vollib Greek calculation failed, using fallback: {e}")
            return self._fallback_manual_greeks(spot, strike, dte_years, risk_free, volatility, option_type)

    def _fallback_manual_greeks(self, spot: float, strike: float, dte_years: float,
                                risk_free: float, volatility: float, option_type: str) -> dict:
        """Manual Greek calculation fallback - merged from EnhancedGreekCalculator"""
        from math import log, sqrt, exp
        try:
            from scipy.stats import norm
        except ImportError:
            # Fallback to basic approximation if scipy not available
            logger.warning("scipy.stats not available, using basic Greek approximations")
            return self._get_default_greeks()

        d1 = (log(spot / strike) + (risk_free + 0.5 * volatility**2) * dte_years) / (volatility * sqrt(dte_years))
        d2 = d1 - volatility * sqrt(dte_years)

        is_call = option_type.lower() in ['call', 'ce']

        if is_call:
            raw_delta_val = norm.cdf(d1)
            theta_val = (-spot * norm.pdf(d1) * volatility / (2 * sqrt(dte_years))
                         - risk_free * strike * exp(-risk_free * dte_years) * norm.cdf(d2)) / 365
            option_price = (spot * norm.cdf(d1)) - (strike * exp(-risk_free * dte_years) * norm.cdf(d2))
        else:  # put
            raw_delta_val = norm.cdf(d1) - 1
            theta_val = (-spot * norm.pdf(d1) * volatility / (2 * sqrt(dte_years))
                         + risk_free * strike * exp(-risk_free * dte_years) * norm.cdf(-d2)) / 365
            option_price = (strike * exp(-risk_free * dte_years) * norm.cdf(-d2)) - (spot * norm.cdf(-d1))

        normalized_delta_val = self._normalize_delta_sign(raw_delta_val, option_type)
        delta_val = normalized_delta_val if normalized_delta_val is not None else raw_delta_val

        gamma_val = norm.pdf(d1) / (spot * volatility * sqrt(dte_years))
        vega_val = spot * norm.pdf(d1) * sqrt(dte_years) / 100
        rho_val = (strike * dte_years * exp(-risk_free * dte_years)
                   * (norm.cdf(d2) if is_call else -norm.cdf(-d2))) / 100

        return {
            'delta': delta_val,
            'gamma': gamma_val,
            'theta': theta_val,
            'vega': vega_val,
            'rho': rho_val,
            'dte_years': dte_years,
            'option_price': option_price
        }

    def _get_default_greeks(self) -> dict:
        """Return default Greek values when calculation fails."""
        return {
            'delta': 0.0,
            'gamma': 0.0,
            'theta': 0.0,
            'vega': 0.0,
            'rho': 0.0,
            'dte_years': 0.0,
            'trading_dte': 0,
            'expiry_series': 'UNKNOWN',
            'option_price': 0.0,
            'iv': 0.2,  # ✅ FIXED: Default IV to 0.2 (20%) instead of 0.0
            'implied_volatility': 0.2  # ✅ FIX: Add implied_volatility for consistency
        }

    def calculate_implied_volatility(self, market_price: float, spot_price: float, strike_price: float,
                                     dte_years: float, risk_free_rate: float, option_type: str,
                                     max_iterations: int = 100, tolerance: float = 0.0001) -> float:
        """
        Calculate Implied Volatility using Newton-Raphson method.

        ✅ PROPER IV CALCULATION: Solves for volatility that makes Black-Scholes price = market price

        Args:
            market_price: Market LTP of the option
            spot_price: Underlying spot price
            strike_price: Option strike price
            dte_years: Days to expiry in years
            risk_free_rate: Annualized risk-free rate (default 5%)
            option_type: 'call' or 'put'
            max_iterations: Maximum Newton iterations
            tolerance: Acceptable pricing error

        Returns:
            Calculated implied volatility
        """
        if market_price <= 0 or spot_price <= 0 or strike_price <= 0 or dte_years <= 0:
            return 0.2  # Return default IV

        # Initial guess: use market conditions to set starting volatility
        initial_guess = 0.2
        intrinsic_value = max(0.0, spot_price - strike_price) if option_type.lower() in ['call', 'ce'] else max(0.0, strike_price - spot_price)
        if market_price < intrinsic_value:
            market_price = intrinsic_value  # Option price can't be below intrinsic value

        # Use a bounded Newton-Raphson method
        low_vol = 0.0001
        high_vol = 5.0
        vol = initial_guess

        for _ in range(max_iterations):
            price, vega_val = self._calculate_price_and_vega_manual(
                spot_price, strike_price, dte_years, risk_free_rate, vol, option_type
            )

            if vega_val == 0:
                break

            price_diff = price - market_price
            if abs(price_diff) < tolerance:
                return max(low_vol, min(high_vol, vol))

            vol -= price_diff / vega_val
            vol = max(low_vol, min(high_vol, vol))

        return max(low_vol, min(high_vol, vol))

    def _calculate_price_and_vega_manual(self, spot: float, strike: float, dte_years: float,
                                         risk_free: float, volatility: float, option_type: str) -> Tuple[float, float]:
        """Helper for manual price and vega calculation."""
        from math import log, sqrt, exp
        from scipy.stats import norm

        d1 = (log(spot / strike) + (risk_free + 0.5 * volatility**2) * dte_years) / (volatility * sqrt(dte_years))
        d2 = d1 - volatility * sqrt(dte_years)

        if option_type.lower() in ['call', 'ce']:
            price = spot * norm.cdf(d1) - strike * exp(-risk_free * dte_years) * norm.cdf(d2)
            vega_val = spot * norm.pdf(d1) * sqrt(dte_years) / 100
        else:
            price = strike * exp(-risk_free * dte_years) * norm.cdf(-d2) - spot * norm.cdf(-d1)
            vega_val = spot * norm.pdf(d1) * sqrt(dte_years) / 100

        return price, vega_val

    def _get_underlying_price_from_redis(self, underlying_symbol: str, option_symbol: str = None) -> float:
        """
        ⚠️ FALLBACK ONLY: Get underlying price from Redis.

        This should rarely be called. In the new architecture:
        - CalculationService provides underlying_price in tick_data
        - This is only used as last resort if tick_data is missing underlying_price
        - Also used by GammaExposureCalculator (scanner batch operations)

        For GOLD/SILVER options, ensures we try:
        1. GOLD/SILVER future price (e.g., GOLD25DECFUT)
        2. GOLD/SILVER spot price
        3. Standard underlying price resolution
        """
        # ✅ ENHANCED: For GOLD/SILVER, ensure we try future prices first
        if underlying_symbol and ('GOLD' in underlying_symbol.upper() or 'SILVER' in underlying_symbol.upper()):
            if option_symbol and self.redis_client:
                try:
                    # Try to get future price first (most accurate for commodities)
                    # Note: derive_future_variants_from_option was removed - using simple variant generation
                    future_variants = [option_symbol.replace('CE', 'FUT').replace('PE', 'FUT')]

                    db1_client = None
                    if hasattr(self.redis_client, 'get_client'):
                        db1_client = self.redis_client.get_client(1)
                    elif hasattr(self.redis_client, 'redis'):
                        db1_client = self.redis_client.redis
                    else:
                        db1_client = self.redis_client

                    if db1_client and future_variants:
                        for future_variant in future_variants:
                            try:
                                # Try multiple key formats for future price
                                price_keys = [
                                    f"price:latest:{future_variant}",
                                    f"price:{future_variant}",
                                ]

                                # Also try tick latest
                                from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
                                tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(future_variant)

                                for price_key in price_keys:
                                    try:
                                        price_val = db1_client.get(price_key)
                                        if price_val:
                                            if isinstance(price_val, bytes):
                                                price_val = price_val.decode('utf-8')
                                            price = float(price_val)
                                            if price > 0:
                                                logger.info(f"✅ [UNDERLYING_PRICE] {option_symbol} - Found {underlying_symbol} future price from {price_key}: {price:.2f}")
                                                return price
                                    except Exception:
                                        continue

                                # Try tick hash
                                if db1_client.exists(tick_key):
                                    tick_data = db1_client.hgetall(tick_key)
                                    price_val = (
                                        tick_data.get(b"last_price") or tick_data.get("last_price") or
                                        tick_data.get(b"close") or tick_data.get("close")
                                    )
                                    if price_val:
                                        if isinstance(price_val, bytes):
                                            price_val = price_val.decode('utf-8')
                                        price = float(price_val)
                                        if price > 0:
                                            logger.info(f"✅ [UNDERLYING_PRICE] {option_symbol} - Found {underlying_symbol} future price from tick hash: {price:.2f}")
                                            return price
                            except Exception:
                                continue
                except Exception as e:
                    logger.debug(f"⚠️ [UNDERLYING_PRICE] Future price lookup failed for {option_symbol}: {e}")

        # ✅ GENERAL FALLBACK: Use standard underlying price resolution
        try:
            # ✅ FIXED: Use existing pooled redis_client
            redis_conn = (
                self.redis_client.get_client(1)
                if hasattr(self.redis_client, 'get_client')
                else self.redis_client
            )

            if not redis_conn:
                return 0.0

            # ✅ FIXED: Use UnifiedDataStorage for indicator lookups (HGETALL on ind:{symbol} hash)
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
            from shared_core.redis_clients.unified_data_storage import get_unified_storage
            
            canonical_underlying = RedisKeyStandards.canonical_symbol(underlying_symbol)
            
            # ✅ PRIORITY 1: Try UnifiedDataStorage.get_indicators() (ind:{symbol} hash)
            try:
                storage = get_unified_storage()
                indicators = storage.get_indicators(canonical_underlying, ['underlying_price', 'last_price', 'close'])
                if indicators:
                    price = indicators.get('underlying_price') or indicators.get('last_price') or indicators.get('close')
                    if price and float(price) > 0:
                        logger.debug(f"✅ [UNDERLYING_PRICE] {option_symbol or 'N/A'} - Found from ind:{canonical_underlying} hash: {float(price):.2f}")
                        return float(price)
            except Exception as e:
                logger.debug(f"⚠️ [UNDERLYING_PRICE] UnifiedDataStorage lookup failed: {e}")

            # ✅ PRIORITY 2: Try tick latest hash (HGETALL - already correct pattern)
            tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(canonical_underlying)
            if redis_conn.exists(tick_key):
                tick_data = redis_conn.hgetall(tick_key)
                price_val = (
                    tick_data.get(b"last_price") or tick_data.get("last_price") or
                    tick_data.get(b"close") or tick_data.get("close")
                )
                if price_val:
                    if isinstance(price_val, bytes):
                        price_val = price_val.decode('utf-8')
                    price = float(price_val)
                    if price > 0:
                        logger.debug(f"✅ [UNDERLYING_PRICE] {option_symbol or 'N/A'} - Found from tick hash {tick_key}: {price:.2f}")
                        return price

        except Exception as e:
            logger.debug(f"⚠️ [UNDERLYING_PRICE] Fallback failed for {underlying_symbol}: {e}")

        return 0.0

    def _calculate_iv_from_option_price(self, last_tick: Dict, symbol: str) -> Optional[float]:
        """
        Calculate implied volatility from option price using vollib.
        
        ✅ ENHANCED: Includes input validation and price adjustment for intrinsic value violations.

        Args:
            last_tick: Tick data dictionary with required fields
            symbol: Option symbol
        """
        try:
            # ✅ PERFORMANCE FIX: Check IV cache first (avoid expensive recalculation)
            current_time = time.time()
            
            # Try simple symbol cache first (fast path)
            if symbol in self._volatility_cache:
                cached_iv, cached_timestamp = self._volatility_cache[symbol]
                if current_time - cached_timestamp < self._volatility_cache_ttl:
                    logger.debug(f"✅ [IV_CACHE_HIT] {symbol} - Using cached IV: {cached_iv:.4f} (age: {current_time - cached_timestamp:.1f}s)")
                    return cached_iv
            
            # Extract strike/expiry for compound key lookup
            strike_price = last_tick.get('strike_price')
            expiry_date = last_tick.get('expiry_date')
            if strike_price and expiry_date:
                compound_key = (symbol, strike_price, str(expiry_date))
                if compound_key in self._iv_cache:
                    cached_iv, cached_timestamp = self._iv_cache[compound_key]
                    if current_time - cached_timestamp < self._volatility_cache_ttl:
                        logger.debug(f"✅ [IV_CACHE_HIT] {symbol} - Using compound-key cached IV: {cached_iv:.4f}")
                        return cached_iv
            
            option_price = last_tick.get('last_price') or last_tick.get('ltp') or last_tick.get('close_price')
            if not option_price or option_price <= 0:
                logger.debug(f"⚠️ [IV_CALC] {symbol} - Missing or invalid option_price: {option_price} (available keys: {list(last_tick.keys())[:10]})")
                return None

            spot_price = last_tick.get('underlying_price') or last_tick.get('spot_price')
            if not spot_price or spot_price <= 0:
                underlying_symbol = extract_underlying_from_symbol(symbol)
                spot_price = self._get_underlying_price_from_redis(underlying_symbol, symbol)
                if not spot_price:
                    logger.debug(f"⚠️ [IV_CALC] {symbol} - Missing underlying_price (tried Redis for {underlying_symbol})")
                    return None

            strike_price = last_tick.get('strike_price')
            if not strike_price or strike_price <= 0:
                strike_price, option_type = self._extract_strike_and_type_from_symbol(symbol)
            else:
                option_type = last_tick.get('option_type')

            if not option_type:
                _, option_type = self._extract_strike_and_type_from_symbol(symbol)

            if not strike_price or not option_type:
                logger.debug(f"⚠️ [IV_CALC] {symbol} - Missing strike_price={strike_price} or option_type={option_type}")
                return None

            option_type = option_type.lower()
            if option_type not in ('call', 'put', 'ce', 'pe'):
                option_type = 'call'

            expiry_date = last_tick.get('expiry_date')
            if expiry_date:
                if isinstance(expiry_date, str):
                    try:
                        expiry_date = datetime.fromisoformat(expiry_date.replace('Z', '+00:00'))
                    except Exception:
                        expiry_date = None
            if not expiry_date:
                expiry_calc = self._ensure_expiry_calculator(symbol=symbol)
                expiry_date = expiry_calc.parse_expiry_from_symbol(symbol)

            if not expiry_date:
                logger.debug(f"⚠️ [IV_CALC] {symbol} - Missing expiry_date (could not parse from symbol)")
                return None

            expiry_calc = self._ensure_expiry_calculator(symbol=symbol)
            dte_info = expiry_calc.calculate_dte(expiry_date, symbol)
            dte_years = max(dte_info['trading_dte'] / 365.0, 1e-6)
            
            # ✅ ADDED: Validate DTE is reasonable
            if dte_years <= 0 or dte_years > 10:
                logger.debug(f"⚠️ [IV_CALC] {symbol} - Invalid DTE: {dte_years} years (trading_dte={dte_info.get('trading_dte')})")
                return None

            # ✅ VALIDATE INPUTS FIRST (check for None before numeric comparisons)
            time_to_expiry = dte_years
            if option_price is None or spot_price is None or strike_price is None or time_to_expiry is None:
                logger.warning(f"⚠️ [IV_CALC] {symbol} - Missing inputs for IV calculation: "
                              f"option_price={option_price}, spot_price={spot_price}, "
                              f"strike_price={strike_price}, time_to_expiry={time_to_expiry}")
                return None
            
            if not all([option_price > 0, spot_price > 0, strike_price > 0, time_to_expiry > 0]):
                logger.warning(f"⚠️ [IV_CALC] {symbol} - Invalid inputs for IV calculation: "
                              f"option_price={option_price}, spot_price={spot_price}, "
                              f"strike_price={strike_price}, time_to_expiry={dte_years}")
                return None
            
            # ✅ ENSURE OPTION PRICE IS VALID (must be >= intrinsic value)
            option_type_normalized = option_type.lower()
            if option_type_normalized in ('put', 'pe'):
                intrinsic = max(strike_price - spot_price, 0)
                min_price = intrinsic + 0.01  # Add epsilon
                if option_price < min_price:
                    logger.warning(f"⚠️ [IV_CALC] {symbol} - Option price {option_price:.2f} < intrinsic {intrinsic:.2f}, "
                                  f"using {min_price:.2f} for IV calculation")
                    option_price = min_price
            else:  # CALL or CE
                intrinsic = max(spot_price - strike_price, 0)
                min_price = intrinsic + 0.01
                if option_price < min_price:
                    logger.warning(f"⚠️ [IV_CALC] {symbol} - Option price {option_price:.2f} < intrinsic {intrinsic:.2f}, "
                                  f"using {min_price:.2f} for IV calculation")
                    option_price = min_price

            # ✅ ADDED: Log input parameters for debugging
            logger.debug(f"🔍 [IV_CALC] {symbol} - Inputs: option_price={option_price:.2f}, spot={spot_price:.2f}, strike={strike_price:.2f}, dte={dte_years:.4f}, type={option_type}")

            try:
                flag = 'c' if option_type_normalized in ['call', 'ce'] else 'p'
                # ✅ FIXED: implied_volatility is already the function, not a module
                iv = implied_volatility(
                    price=option_price,
                    S=spot_price,
                    K=strike_price,
                    t=dte_years,
                    r=0.05,
                    flag=flag
                )
                if iv and 0 < iv <= 5.0:
                    # ✅ PERFORMANCE FIX: Store IV in comprehensive cache
                    cache_time = time.time()
                    
                    # Simple symbol cache (fast path)
                    self._volatility_cache[symbol] = (iv, cache_time)
                    
                    # Comprehensive cache key (precise path)
                    comprehensive_key = (
                        symbol,
                        round(strike_price, 2),
                        round(dte_years, 4),
                        option_type_normalized,
                        round(spot_price, 2),
                        round(option_price, 2)
                    )
                    self._iv_cache[comprehensive_key] = (iv, cache_time)
                    
                    # Cache pruning to prevent memory leak
                    if len(self._iv_cache) > 1000:
                        cutoff = cache_time - 10.0
                        self._iv_cache = {k: v for k, v in self._iv_cache.items() if v[1] > cutoff}
                    
                    self._store_iv_to_redis(symbol, iv, ttl=300)
                    logger.debug(f"✅ [IV_CALC] {symbol} - Calculated IV: {iv:.4f} (cached)")
                    return iv
                else:
                    logger.debug(f"⚠️ [IV_CALC] {symbol} - vollib returned invalid IV: {iv}")
            except Exception as e:
                logger.error(f"❌ [IV_CALC] {symbol} - IV calculation failed: {e}")
                return None

            # 🚀 FAST FALLBACK: Skip slow Newton-Raphson, use approximation instead
            try:
                # Use fast approximation instead of slow vollib Newton-Raphson
                approx_iv = self._calculate_iv_approximation(option_price, spot_price, strike_price, dte_years, option_type, symbol)
                if approx_iv and 0 < approx_iv <= 2.0:
                    self._store_iv_to_redis(symbol, approx_iv, ttl=300)
                    logger.debug(f"✅ [IV_CALC] {symbol} - Fast approx IV: {approx_iv:.4f}")
                    return approx_iv
                
                # Original Newton-Raphson (only if fast approx fails)
                from py_vollib.black_scholes import black_scholes as bs_price
                from py_vollib.black_scholes.greeks import vega as bs_vega

                flag = 'c' if option_type in ['call', 'ce'] else 'p'
                vol = 0.2
                
                # ✅ ADDED: Start timing and iteration count
                # import time # Removed to check if it fixes the UnboundLocalError
                nr_start = time.perf_counter_ns()
                
                for iteration in range(100):  # Increased max_iters to 100 for safety (was 20)
                    theoretical_price = bs_price(flag, spot_price, strike_price, dte_years, 0.05, vol)
                    vega_val = bs_vega(flag, spot_price, strike_price, dte_years, 0.05, vol)
                    
                    if vega_val == 0:
                        logger.debug(f"⚠️ [IV_CALC] {symbol} - Newton-Raphson: vega=0 at iteration {iteration}")
                        break
                    
                    price_diff = theoretical_price - option_price
                    if abs(price_diff) < 0.0001:
                        # ✅ PERFORMANCE FIX: Store IV in comprehensive cache
                        nr_time = (time.perf_counter_ns() - nr_start) / 1000000
                        if nr_time > 10:
                            logger.warning(f"🚨 [IV_CALC] {symbol} - Slow IV calc: {nr_time:.2f}ms, {iteration} iterations")
                            
                        cache_time = time.time()
                        self._volatility_cache[symbol] = (vol, cache_time)
                        
                        # Comprehensive cache key
                        comprehensive_key = (
                            symbol,
                            round(strike_price, 2),
                            round(dte_years, 4),
                            option_type_normalized if 'option_type_normalized' in dir() else option_type,
                            round(spot_price, 2),
                            round(option_price, 2)
                        )
                        self._iv_cache[comprehensive_key] = (vol, cache_time)
                        
                        self._store_iv_to_redis(symbol, vol, ttl=300)
                        logger.debug(f"✅ [IV_CALC] {symbol} - Calculated IV via Newton-Raphson: {vol:.4f} (cached)")
                        return vol
                        
                    vol -= price_diff / vega_val
                    
                    # Safety check for runaway values
                    if vol <= 0 or vol > 5.0:
                        logger.debug(f"⚠️ [IV_CALC] {symbol} - Newton-Raphson: vol out of range ({vol}) at iteration {iteration}")
                        break
                        
                logger.debug(f"⚠️ [IV_CALC] {symbol} - Newton-Raphson did not converge after 100 iterations")
            except Exception as e:
                logger.debug(f"⚠️ [IV_CALC] {symbol} - Newton fallback failed: {e}")

            # ✅ ADDED: Fallback 3 - Simple approximation for ATM options
            try:
                approx_iv = self._calculate_iv_approximation(
                    option_price, spot_price, strike_price, dte_years, option_type
                )
                if approx_iv and 0 < approx_iv <= 5.0:
                    self._store_iv_to_redis(symbol, approx_iv, ttl=300)
                    logger.debug(f"✅ [IV_CALC] {symbol} - Calculated IV via approximation: {approx_iv:.4f}")
                    return approx_iv
            except Exception as e:
                logger.debug(f"⚠️ [IV_CALC] {symbol} - Approximation fallback failed: {e}")

            # ✅ IMPROVED: Include input parameters in warning for debugging
            logger.warning(f"⚠️ [IV_CALC] {symbol} - Could not calculate IV from option price (option={option_price:.2f}, spot={spot_price:.2f}, strike={strike_price:.2f}, dte={dte_years:.4f})")
            return None

        except Exception as e:
            logger.warning(f"⚠️ [IV_CALC] {symbol} - Error in IV calculation: {e}")
            import traceback
            logger.debug(f"🔍 [IV_CALC] {symbol} - Full traceback: {traceback.format_exc()}")
            return None

    def _calculate_iv_approximation(self, option_price: float, underlying: float, strike: float,
                                    dte_years: float, option_type: str, symbol: str = None) -> Optional[float]:
        """
        🚨 EMERGENCY: Fast IV calculation bypassing slow vollib.
        Uses analytic approximations instead of iterative methods.
        - ATM: Brenner-Subrahmanyam approximation
        - Near-money: Corrado-Miller extension
        - Deep OTM/ITM: Simple ratio approximation
        """
        import time
        import math
        
        start = time.perf_counter_ns()
        
        # 🚨 SAFETY CHECKS
        if not all([underlying > 0, strike > 0, dte_years > 0.0001, option_price >= 0]):
            return 0.3  # Default 30% IV
        
        # Normalize option type
        opt_type = option_type.lower()
        is_call = opt_type in ['call', 'ce', 'c']
        
        # 🚨 1. ATM OPTIONS: Use Brenner-Subrahmanyam approximation (FASTEST)
        moneyness = underlying / strike
        
        if 0.98 < moneyness < 1.02:  # ATM (±2%)
            # σ ≈ C * √(2π) / (S * √T)
            iv = option_price * math.sqrt(2 * math.pi) / (underlying * math.sqrt(dte_years))
            iv = max(0.15, min(0.60, iv))  # Bound: 15-60% for ATM
        
        # 🚨 2. NEAR-MONEY: Corrado-Miller extension
        elif 0.90 < moneyness < 1.10:  # Near-money (±10%)
            sqrt_T = math.sqrt(dte_years)
            
            # Corrado-Miller formula
            Q = option_price - (underlying - strike) / 2
            sqrt_term = ((underlying - strike) ** 2) / math.pi
            denominator = underlying * sqrt_T / math.sqrt(2 * math.pi)
            
            if denominator > 0 and Q > 0:
                iv = (math.sqrt(2 * math.pi) / (underlying * sqrt_T)) * \
                     (Q + math.sqrt(max(0, Q**2 - sqrt_term)))
                iv = max(0.10, min(0.80, iv))  # Bound: 10-80%
            else:
                iv = 0.3
        
        # 🚨 3. DEEP OTM/ITM: Use simple ratio approximation
        else:
            if is_call:
                iv = (option_price / underlying) * (1.0 / math.sqrt(dte_years)) * 3.0
            else:  # put
                iv = (option_price / strike) * (1.0 / math.sqrt(dte_years)) * 2.5
            
            iv = max(0.05, min(1.20, iv))  # Bound: 5-120%
        
        # 🚨 4. ADD VOLATILITY SMILE/SKEW ADJUSTMENT
        # NIFTY/BANKNIFTY typically have negative skew (OTM puts > OTM calls)
        if not is_call and moneyness < 0.95:  # OTM puts
            iv = iv * 1.15  # Add 15% for put skew
        
        # Ensure reasonable bounds
        iv = max(0.05, min(1.50, iv))
        
        elapsed = (time.perf_counter_ns() - start) / 1000  # microseconds
        if elapsed > 1000 and symbol:  # > 1ms is too slow
            logger.debug(f"⚠️ [IV_EMERGENCY] {symbol}: {elapsed:.2f}μs, moneyness={moneyness:.3f}, IV={iv:.3f}")
        
        return iv


    def _store_iv_to_redis(self, symbol: str, iv_value: float, ttl: int = 300) -> None:
        """
        Store calculated IV in Redis using standardized keys.
        ✅ FIXED: Uses DatabaseAwareKeyBuilder.live_greeks for ind:greeks:{symbol}:iv
        Also stores as 'implied_volatility' for compatibility.
        """
        if not self.redis_client or not symbol or not iv_value:
            return

        try:
            redis_conn = (
                self.redis_client.get_client(1)
                if hasattr(self.redis_client, 'get_client')
                else self.redis_client
            )
            if not redis_conn:
                return

            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            
            # ✅ FIXED: Use live_greeks_hash to avoid "DUPLICATE_GREEK_KEY" violations (String vs Hash)
            # Was: ind:greeks:{symbol}:iv (String) -> Now: ind:greeks:{symbol} (Hash Field: iv)
            greeks_hash_key = DatabaseAwareKeyBuilder.live_greeks_hash(canonical_symbol)
            
            # Store both 'iv' and 'implied_volatility' for compatibility in the Hash
            redis_conn.hset(
                greeks_hash_key,
                mapping=fix_numpy_serialization({
                    'iv': str(iv_value),
                    'implied_volatility': str(iv_value)
                }),
            )
            redis_conn.expire(greeks_hash_key, ttl)
            
            logger.debug(f"✅ [IV_STORAGE] {symbol} - Stored IV={iv_value:.4f} to Redis Hash (TTL={ttl}s): {greeks_hash_key}")
        except Exception as e:
            logger.warning(f"⚠️ [IV_STORAGE] Failed to store IV for {symbol}: {e}")

    def _extract_strike_and_type_from_symbol(self, symbol: str) -> Tuple[float, str]:
        """Enhanced symbol parsing for strike and option type"""
        try:
            symbol_upper = symbol.upper().replace('NFO:', '').replace('NSE:', '')

            # Pattern for NIFTY/BANKNIFTY options: NIFTY25NOV25950CE
            pattern1 = r'([A-Z]+)(\d{2}[A-Z]{3})(\d+)(CE|PE)'
            match1 = re.search(pattern1, symbol_upper)
            if match1:
                strike = float(match1.group(3))
                option_type = 'call' if match1.group(4) == 'CE' else 'put'
                return strike, option_type

            # Pattern for stock options: RELIANCE25NOV2500CE
            pattern2 = r'([A-Z]+[-A-Z]*)(\d{2}[A-Z]{3})(\d+)(CE|PE)'
            match2 = re.search(pattern2, symbol_upper)
            if match2:
                strike = float(match2.group(3))
                option_type = 'call' if match2.group(4) == 'CE' else 'put'
                return strike, option_type

            # Fallback: simple CE/PE detection
            if symbol_upper.endswith('CE'):
                strike_match = re.search(r'(\d+)(CE)$', symbol_upper)
                if strike_match:
                    return float(strike_match.group(1)), 'call'
            elif symbol_upper.endswith('PE'):
                strike_match = re.search(r'(\d+)(PE)$', symbol_upper)
                if strike_match:
                    return float(strike_match.group(1)), 'put'

            logger.warning(f"⚠️ [SYMBOL_PARSE] Could not parse strike/type from: {symbol}")
            return 0.0, None

        except Exception as e:
            logger.error(f"💥 [SYMBOL_PARSE] Error parsing symbol {symbol}: {e}")
            return 0.0, None

    def _is_option_symbol(self, symbol: str) -> bool:
        """Robust check if symbol is an options contract"""
        if not symbol or not isinstance(symbol, str):
            return False

        symbol_upper = symbol.upper()

        option_patterns = [
            r'^(NFO:)?([A-Z]+)(\d{2}[A-Z]{3})(\d+)(CE|PE)$',  # NIFTY25DEC25800CE
            r'^(NFO)?([A-Z]+)(\d{2}[A-Z]{3})(\d+)(CE|PE)$',
            r'^([A-Z]+)(\d{2}[A-Z]{3})(\d+)(CE|PE)$',
            r'^([A-Z]+)[-]([A-Z]+)(\d{2}[A-Z]{3})(\d+)(CE|PE)$',
        ]

        for pattern in option_patterns:
            if re.match(pattern, symbol_upper):
                return True

        return symbol_upper.endswith('CE') or symbol_upper.endswith('PE')

    def _is_future_symbol(self, symbol: str) -> bool:
        """Check if symbol is an F&O future with robust parsing"""
        if not symbol or not isinstance(symbol, str):
            return False

        symbol_upper = symbol.upper()

        # Normalize away optional prefixes (NFO, NFO:)
        if symbol_upper.startswith('NFO:'):
            suffix = symbol_upper[4:]
        elif symbol_upper.startswith('NFO'):
            suffix = symbol_upper[3:]
        else:
            suffix = symbol_upper

        if not suffix.endswith('FUT'):
            return False

        before_fut = suffix[:-3]
        expiry_pattern = r'\d{2}[A-Z]{3}$'
        return re.search(expiry_pattern, before_fut) is not None

    def _is_fno_symbol(self, symbol: str) -> bool:
        """Check if symbol is any F&O instrument (option or future)"""
        return self._is_option_symbol(symbol) or self._is_future_symbol(symbol)

    def _calculate_gamma_exposure(self, symbol: str) -> Optional[float]:
        """
        Calculate gamma exposure for an option symbol using cached GammaExposureCalculator data.
        """
        if not symbol:
            return None

        now = time.time()
        cached = self._gamma_exposure_cache.get(symbol)
        if cached and (now - cached.get('timestamp', 0)) < self._gamma_cache_ttl:
            return cached.get('value')

        underlying_symbol = extract_underlying_from_symbol(symbol) or symbol
        if not underlying_symbol or not self.redis_client:
            return None

        if not hasattr(self, '_gamma_calculator'):
            self._gamma_calculator = GammaExposureCalculator(redis_client=self.redis_client)

        try:
            gex_data = self._gamma_calculator.calculate_total_gex(underlying_symbol) or {}
            value = gex_data.get('net_gex')
            self._gamma_exposure_cache[symbol] = {'value': value, 'timestamp': now}
            return value
        except Exception as exc:
            logger.debug(f"⚠️ [GAMMA_EXPOSURE] Failed for {symbol}: {exc}")
            return None

    def _fallback_atr(self, highs: List[float], lows: List[float], closes: List[float], period: int) -> float:
        """Fallback ATR calculation when Polars is not available"""
        if len(highs) < period + 1:
            return 0.0

        tr_values = []
        for i in range(1, len(highs)):
            tr1 = highs[i] - lows[i]
            tr2 = abs(highs[i] - closes[i - 1])
            tr3 = abs(lows[i] - closes[i - 1])
            tr_values.append(max(tr1, tr2, tr3))

        return sum(tr_values[-period:]) / period if tr_values else 0.0

    def _fallback_ema(self, prices: List[float], period: int) -> float:
        """Fallback EMA calculation when Polars is not available"""
        if len(prices) < period:
            return float(prices[-1]) if prices else 0.0

        alpha = 2.0 / (period + 1)
        ema = prices[0]
        for last_price in prices[1:]:
            ema = alpha * last_price + (1 - alpha) * ema

        return float(ema)

    def _fallback_rsi(self, prices: List[float], period: int) -> float:
        """Fallback RSI calculation when Polars is not available"""
        if len(prices) < period + 1:
            return 50.0

        gains = []
        losses = []

        for i in range(1, len(prices)):
            change = prices[i] - prices[i - 1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(-change)

        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return float(rsi)

    def _fallback_vwap(self, tick_data: List[Dict]) -> float:
        """Fallback VWAP calculation when Polars is not available"""
        if not tick_data:
            return 0.0

        total_volume = 0
        total_price_volume = 0

        for tick in tick_data:
            last_price = tick.get('last_price', 0)
            # Try different volume fields in order of preference
            volume = (
                tick.get('zerodha_cumulative_volume', 0)
                or tick.get('bucket_incremental_volume', 0)
                or tick.get('bucket_cumulative_volume', 0)
                or 0
            )

            if volume > 0 and last_price > 0:
                total_volume += volume
                total_price_volume += last_price * volume

        if total_volume > 0:
            return total_price_volume / total_volume
        else:
            # Fallback: use simple average if no volume data
            prices = [tick.get('last_price', 0) for tick in tick_data if tick.get('last_price', 0) > 0]
            return sum(prices) / len(prices) if prices else 0.0

    def _fallback_macd(self, prices: List[float], fast_period: int, slow_period: int, signal_period: int) -> Dict:
        """Fallback MACD calculation when Polars is not available"""
        if len(prices) < slow_period:
            return {'macd': 0.0, 'signal': 0.0, 'histogram': 0.0}

        ema_fast = self._fallback_ema(prices, fast_period)
        ema_slow = self._fallback_ema(prices, slow_period)
        macd_line = ema_fast - ema_slow

        # Simplified signal line calculation
        signal_line = macd_line * 0.9  # Approximation

        return {
            'macd': macd_line,
            'signal': signal_line,
            'histogram': macd_line - signal_line
        }

    def _fallback_bollinger_bands(self, prices: List[float], period: int, std_dev: float) -> Dict:
        """Fallback Bollinger Bands calculation when Polars is not available"""
        if len(prices) < period:
            return {'upper': 0.0, 'middle': 0.0, 'lower': 0.0}

        recent_prices = prices[-period:]
        middle_band = sum(recent_prices) / len(recent_prices)

        # Calculate standard deviation
        variance = sum((p - middle_band) ** 2 for p in recent_prices) / len(recent_prices)
        std = variance ** 0.5

        upper_band = middle_band + (std * std_dev)
        lower_band = middle_band - (std * std_dev)

        return {
            'upper': upper_band,
            'middle': middle_band,
            'lower': lower_band
        }

    def _get_vix_snapshot(self) -> Optional[Dict[str, Any]]:
        """Fetch VIX snapshot with simple caching to avoid Redis spam."""
        if not self.vix_utils:
            return None
        now = time.time()
        if (
            self._cached_vix_snapshot
            and now - self._last_vix_fetch < max(self.vix_refresh_interval, 5)
        ):
            return self._cached_vix_snapshot
        try:
            snapshot = self.vix_utils.get_current_vix()
            if snapshot:
                self._cached_vix_snapshot = snapshot
                self._last_vix_fetch = now
            return snapshot
        except Exception as exc:
            logger.debug(f"⚠️ [VIX_CACHE] Failed to fetch VIX snapshot: {exc}")
            return self._cached_vix_snapshot

    def _fix_dataframe_types(self, df: Optional["pl.DataFrame"], symbol: str = "") -> Optional["pl.DataFrame"]:
        if not POLARS_AVAILABLE or df is None:
            return df
        for col in df.columns:
            dtype = df[col].dtype
            if isinstance(dtype, pl.List):
                logger.warning(f"⚠️ [LIST_FIX] {symbol} - Column {col} is List type: {dtype}")
                try:
                    lengths = df[col].list.lengths()
                    max_len = lengths.max()
                    min_len = lengths.min()
                except Exception:
                    max_len = min_len = None
                if max_len == 1 and (min_len == 1 or min_len is None):
                    try:
                        df = df.with_columns(pl.col(col).list.first().alias(col))
                    except Exception:
                        df = df.with_columns(pl.lit(None).alias(col))
                else:
                    df = df.with_columns(pl.lit(None).alias(col))
        return df

    def _clean_tick_dataframe(self, tick_list: List[Dict], symbol: str = "") -> "pl.DataFrame":
        if not POLARS_AVAILABLE or pl is None:
            raise RuntimeError("Polars is required for batch calculations")
        df = pl.DataFrame(tick_list)
        df = self._fix_dataframe_types(df, symbol) or df
        required_cols = {
            'last_price': pl.Float64,
            'volume': pl.Float64,
            'timestamp': pl.Int64,
        }
        for col, dtype in required_cols.items():
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(dtype, strict=False).alias(col))
            else:
                current_dtype = df[col].dtype
                if current_dtype != dtype:
                    try:
                        df = df.with_columns(pl.col(col).cast(dtype, strict=False))
                    except Exception:
                        df = df.with_columns(pl.lit(None).cast(dtype, strict=False).alias(col))
        return df


# ⬇️ GammaExposureCalculator MOVED TO END OF FILE
# It was accidentally inserted here, breaking HybridCalculations scope.

   
    def calculate_price_change(self, last_price: float, previous_price: float) -> float:
        """
        ✅ FIXED: Price change percentage with proper decimal precision
        
        Uses Decimal for high-precision calculations to avoid rounding errors.
        """
        try:
            # Handle zero or None prices
            if not last_price or not previous_price or previous_price <= 0:
                logger.debug(f"⚠️ [PRICE_CHANGE] Invalid prices: current={last_price}, previous={previous_price}")
                return 0.0
            
            # Convert to float with explicit conversion
            current_price = float(last_price)
            previous_price_float = float(previous_price)
            
            # ✅ FIXED: Use Decimal for high-precision calculations
            from decimal import Decimal, getcontext
            getcontext().prec = 10  # Set high precision (10 decimal places)
            
            # Calculate with Decimal to avoid floating-point errors
            current_decimal = Decimal(str(current_price))
            previous_decimal = Decimal(str(previous_price_float))
            
            if previous_decimal == 0:
                return 0.0
            
            # Calculate percentage change with high precision
            price_change_decimal = ((current_decimal - previous_decimal) / previous_decimal) * Decimal('100.0')
            price_change_pct = float(price_change_decimal)
            
            # Debug logging for precision issues
            raw_change = current_price - previous_price_float
            if abs(price_change_pct) < 0.001 and abs(raw_change) > 0.0001:
                logger.debug(f"🔢 [PRICE_CHANGE_PRECISION] Raw change: {raw_change:.8f}, "
                           f"Percentage: {price_change_pct:.8f}%, "
                           f"Current: {current_price:.8f}, Previous: {previous_price_float:.8f}")
            
            return price_change_pct
            
        except (ValueError, TypeError, ZeroDivisionError) as e:
            logger.debug(f"⚠️ [PRICE_CHANGE] Error calculating price change: {e}, "
                        f"current={last_price}, previous={previous_price}")
            return 0.0
        except Exception as e:
            logger.error(f"❌ [PRICE_CHANGE] Unexpected error: {e}")
            return 0.0
    
    def calculate_net_change(self, last_price: float, close_price: float) -> float:
        """Net change calculation using Zerodha field names"""
        return float(last_price - close_price)
    
    def calculate_session_metrics(self, session_data: Dict) -> Dict:
        """Calculate session metrics using Zerodha field names"""
        return {
            'session_date': session_data.get('session_date', ''),
            'update_count': session_data.get('update_count', 0),
            'last_price': session_data.get('last_price', 0.0),
            'high': session_data.get('high', 0.0),
            'low': session_data.get('low', 0.0),
            'zerodha_cumulative_volume': session_data.get('zerodha_cumulative_volume', 0),
            'bucket_cumulative_volume': session_data.get('bucket_cumulative_volume', 0),
            'bucket_incremental_volume': session_data.get('bucket_incremental_volume', 0),
            'last_update_timestamp': session_data.get('last_update_timestamp', 0)
        }
    
    def calculate_derivatives_metrics(self, derivatives_data: Dict) -> Dict:
        """Calculate derivatives metrics using Zerodha field names"""
        return {
            'oi': derivatives_data.get('oi', 0),
            'oi_day_high': derivatives_data.get('oi_day_high', 0),
            'oi_day_low': derivatives_data.get('oi_day_low', 0),
            'strike_price': derivatives_data.get('strike_price', 0.0),
            'expiry': derivatives_data.get('expiry', ''),
            'option_type': derivatives_data.get('option_type', ''),
            'underlying': derivatives_data.get('underlying', ''),
            'net_change': derivatives_data.get('net_change', 0.0)
        }
    
    def calculate_market_depth_metrics(self, depth_data: Dict) -> Dict:
        """Calculate market depth metrics using Zerodha field names"""
        return {
            'buy_quantity': depth_data.get('buy_quantity', 0),
            'sell_quantity': depth_data.get('sell_quantity', 0),
            'lower_circuit_limit': depth_data.get('lower_circuit_limit', 0.0),
            'upper_circuit_limit': depth_data.get('upper_circuit_limit', 0.0),
            'depth': depth_data.get('depth', {})
        }
    
    def calculate_ohlc_metrics(self, ohlc_data: Dict) -> Dict:
        """Calculate OHLC metrics using Zerodha field names"""
        return {
            'last_price': ohlc_data.get('last_price', 0.0),
            'average_price': ohlc_data.get('average_price', 0.0),
            'ohlc': ohlc_data.get('ohlc', {}),
            'net_change': ohlc_data.get('net_change', 0.0),
            'mode': ohlc_data.get('mode', 'unknown'),
            'tradable': ohlc_data.get('tradable', True)
        }
    
    
    
    def process_tick(self, symbol: str, tick_data: Dict) -> Dict:
        """
        Process single tick - Wrapper for batch_calculate_indicators
        
        Args:
            symbol: Trading symbol
            tick_data: Tick dictionary
            
        Returns:
            Calculated indicators dictionary
        """
        try:
            # Ensure tick_data has symbol
            if 'symbol' not in tick_data:
                tick_data['symbol'] = symbol
                
            # Process as batch of 1
            results = self.batch_calculate_indicators([tick_data], symbol)
            
            # Extract result
            if results:
                # If result is keyed by symbol (typical batch response)
                if symbol in results:
                    return results[symbol]
                
                # If result is keyed by symbol in a list (sometimes happens)
                if isinstance(results, list) and len(results) > 0:
                    first = results[0]
                    if isinstance(first, dict):
                        if symbol in first:
                            return first[symbol]
                        return first
                
                # If result is the indicator dict itself
                if isinstance(results, dict) and ('volume_ratio' in results or 'rsi' in results):
                    return results
                    
            return {}
        except Exception as e:
            # logger.error(f"Error in process_tick for {symbol}: {e}")
            return {}
            
    def batch_calculate_indicators(self, tick_data: List[Dict], symbol: str = None) -> Dict:
        """
        ✅ ENHANCED: Batch calculate all indicators with guaranteed volume_ratio access.
        
        Now uses session-based bucket data to ensure volume_ratio is available for all indicators.
        """
        logger.debug(f"🔍 [CALC_DEBUG] batch_calculate_indicators called for {symbol} with {len(tick_data) if tick_data else 0} ticks")
        
        if not POLARS_AVAILABLE:
            return self._fallback_batch_calculate(tick_data, symbol)
        
        if not tick_data:
            return {}

        self._log_batch_input_snapshot(symbol, tick_data)

        if len(tick_data) < getattr(self, "min_batch_size", 20):
            logger.debug(f"📉 [SMALL_BATCH] {symbol} - Only {len(tick_data)} ticks, using real-time calc")
            fallback_tick = tick_data[-1] if tick_data else {}
            window = self._symbol_sessions.get(symbol)
            if window is None:
                window = pl.DataFrame(tick_data) if POLARS_AVAILABLE else []
            return self._calculate_realtime_indicators(symbol, window, fallback_tick)
        
        # ✅ ENHANCED: Update session-based window for each tick to ensure volume_ratio is available
        if symbol:
            for tick in tick_data:
                try:
                    # Update session-based window with current tick (includes volume_ratio)
                    self.process_tick(symbol, tick)
                except Exception as e:
                    logger.debug(f"🔍 [CALC_DEBUG] {symbol} Failed to update session-based window: {e}")
        
        # ✅ OPTIMIZED: Convert to Polars DataFrame and use Polars-based calculations
        df = self._clean_tick_dataframe(tick_data, symbol)
        if df.is_empty():
            return {}

        schema_enforcer = getattr(self, "_schema_enforcer", None)
        if schema_enforcer is None:
            schema_enforcer = SchemaEnforcer(getattr(self, 'logger', logger))
            self._schema_enforcer = schema_enforcer
        enforced_df = schema_enforcer.enforce_schema(df)
        if enforced_df is not None:
            df = enforced_df
        
        # ✅ OPTIMIZED: Get last price from DataFrame
        prices_list = df['last_price'].to_list() if 'last_price' in df.columns else []
        last_price = float(prices_list[-1]) if prices_list else 0.0
        last_price = self._get_optimized_last_price([last_price], symbol) if last_price else 0.0
        logger.debug(f"🔍 [CALC_DEBUG] {symbol} Last price: {last_price}")
        
        # ✅ OPTIMIZED: Batch EMA calculation - SKIP if already computed by pipeline
        last_tick = tick_data[-1] if tick_data else {}
        existing_ema_5 = last_tick.get('ema_5') if last_tick else None
        
        if existing_ema_5 and existing_ema_5 != 0.0:
            # EMAs already computed by pipeline - use them directly
            fast_emas = {
                'ema_5': last_tick.get('ema_5', 0.0),
                'ema_10': last_tick.get('ema_10', 0.0),
                'ema_20': last_tick.get('ema_20', 0.0),
                'ema_50': last_tick.get('ema_50', 0.0),
                'ema_100': last_tick.get('ema_100', 0.0),
                'ema_200': last_tick.get('ema_200', 0.0),
            }
            logger.debug(f"⏭️ [CALC_SKIP] {symbol} EMAs from pipeline: ema_5={existing_ema_5}")
        else:
            # No pre-computed EMAs - calculate locally
            fast_emas = self._batch_calculate_emas(prices_list, last_price)
            logger.debug(f"🔍 [CALC_DEBUG] {symbol} EMAs calculated locally: {list(fast_emas.keys())}")
        
        # ✅ ENHANCED: Get historical data with volume_ratio for volume-based indicators
        last_tick = tick_data[-1] if tick_data else {}
        # ✅ DEBUG: Log volume_ratio in last_tick immediately after extraction
        volume_ratio_in_last_tick = last_tick.get('volume_ratio') if last_tick else None
        logger.info(f"🔍 [VOLUME_RATIO_FLOW] {symbol} - volume_ratio in last_tick after extraction: {volume_ratio_in_last_tick}, last_tick keys: {list(last_tick.keys())[:15] if last_tick else []}")
        historical_data = None
        
        # Combine historical session-based window with current DataFrame
        if symbol and symbol in self._symbol_sessions:
            historical_data = self._get_latest_ticks(symbol, self._session_window_size)
            if historical_data is not None and not historical_data.is_empty():
                enforced_hist = schema_enforcer.enforce_schema(historical_data)
                if enforced_hist is not None:
                    historical_data = enforced_hist
                diffs = schema_enforcer.get_schema_differences(historical_data, df)
                if diffs:
                    logger.warning(f"⚠️ [SCHEMA_DIFFS] {symbol} - Schema differences: {diffs}")
                # ✅ FIXED: Align columns before concatenating to prevent width mismatch
                # Get union of all columns from both DataFrames
                all_columns = set(historical_data.columns) | set(df.columns)
                
                # Add missing columns to historical_data with default values
                for col in all_columns:
                    if col not in historical_data.columns:
                        if col == 'symbol':
                            historical_data = historical_data.with_columns(pl.lit(symbol if symbol else '').cast(pl.Utf8).alias(col))
                        elif col in ['open', 'close', 'average_price', 'volume_ratio']:
                            historical_data = historical_data.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
                        elif col in ['exchange_timestamp_ms', 'timestamp_ms', 'timestamp', 'exchange_timestamp_epoch', 'last_trade_time_ms', 'processed_timestamp_ms']:
                            # ✅ FIXED: timestamp is Int64 (epoch milliseconds) in session-based window
                            historical_data = historical_data.with_columns(pl.lit(0).cast(pl.Int64).alias(col))
                        elif col in ['zerodha_cumulative_volume', 'zerodha_last_traded_quantity', 'bucket_incremental_volume', 'bucket_cumulative_volume', 'volume']:
                            historical_data = historical_data.with_columns(pl.lit(0).cast(pl.Int64).alias(col))
                        elif col in ['exchange_timestamp', 'timestamp_ns', 'last_trade_time']:
                            historical_data = historical_data.with_columns(pl.lit('').cast(pl.Utf8).alias(col))
                        else:
                            # Default to Float64 for unknown numeric columns
                            historical_data = historical_data.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
                
                # Add missing columns to df with default values
                for col in all_columns:
                    if col not in df.columns:
                        if col == 'symbol':
                            df = df.with_columns(pl.lit(symbol if symbol else '').cast(pl.Utf8).alias(col))
                        elif col in ['open', 'close', 'average_price', 'volume_ratio']:
                            df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
                        elif col in ['exchange_timestamp_ms', 'timestamp_ms', 'timestamp', 'exchange_timestamp_epoch', 'last_trade_time_ms', 'processed_timestamp_ms']:
                            # ✅ FIXED: timestamp should be Int64 (epoch milliseconds) to match session-based window
                            df = df.with_columns(pl.lit(0).cast(pl.Int64).alias(col))
                        elif col in ['zerodha_cumulative_volume', 'zerodha_last_traded_quantity', 'bucket_incremental_volume', 'bucket_cumulative_volume', 'volume']:
                            df = df.with_columns(pl.lit(0).cast(pl.Int64).alias(col))
                        elif col in ['exchange_timestamp', 'timestamp_ns', 'last_trade_time']:
                            df = df.with_columns(pl.lit('').cast(pl.Utf8).alias(col))
                        else:
                            # Default to Float64 for unknown numeric columns
                            df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
                
                # Ensure columns are in the same order
                column_order = sorted(all_columns)
                historical_data = historical_data.select(column_order)
                df = df.select(column_order)
                
                # ✅ FIXED: Cast all integer columns to Int64 and float columns to Float64 before concatenation
                # This prevents "type Int8 is incompatible with expected type Int64" and "type Float32 is incompatible with expected type Float64" errors
                int64_cols = [col for col in column_order if historical_data[col].dtype in [pl.Int8, pl.Int16, pl.Int32, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64]]
                float64_cols = [col for col in column_order if historical_data[col].dtype in [pl.Float32]]
                
                if int64_cols:
                    historical_data = historical_data.with_columns([pl.col(col).cast(pl.Int64, strict=False) for col in int64_cols])
                    df = df.with_columns([pl.col(col).cast(pl.Int64, strict=False) for col in int64_cols])
                
                if float64_cols:
                    historical_data = historical_data.with_columns([pl.col(col).cast(pl.Float64, strict=False) for col in float64_cols])
                    df = df.with_columns([pl.col(col).cast(pl.Float64, strict=False) for col in float64_cols])
                
                # ✅ CRITICAL FIX: Handle String vs Float64 mismatch (common with Redis decode_responses)
                # Find columns where types differ between historical_data and df
                for col in column_order:
                    hist_dtype = historical_data[col].dtype
                    df_dtype = df[col].dtype
                    
                    # If one is String/Utf8 and other is numeric, cast both to Float64
                    if hist_dtype != df_dtype:
                        if hist_dtype in [pl.Utf8, pl.String] and df_dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
                            # Historical is String, df is numeric - cast historical to Float64
                            historical_data = historical_data.with_columns(pl.col(col).cast(pl.Float64, strict=False))
                            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
                        elif df_dtype in [pl.Utf8, pl.String] and hist_dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
                            # df is String, historical is numeric - cast df to Float64
                            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
                            historical_data = historical_data.with_columns(pl.col(col).cast(pl.Float64, strict=False))
                
                # BEFORE concat, enforce schema on overlapping columns
                common_columns = set(historical_data.columns) & set(df.columns)
                for col in common_columns:
                    expected_dtype = historical_data[col].dtype
                    df = df.with_columns(pl.col(col).cast(expected_dtype, strict=False))

                # Combine historical + current for Polars calculations
                combined_window = pl.concat([historical_data, df], how='diagonal')
            else:
                combined_window = df
        else:
            combined_window = df
        
        # ✅ FIXED: Use Polars-based calculations directly on DataFrame
        indicators = {
            'symbol': symbol,
            'last_price': last_price,
            'timestamp': last_tick.get('exchange_timestamp_ms', last_tick.get('timestamp', 0)),
            **fast_emas,  # ✅ OPTIMIZED: Spread all EMAs directly
            'ema_crossover': fast_emas,  # ✅ OPTIMIZED: Reference same object (memory efficient)
        }
        
        # ✅ DIAGNOSTIC 1: RSI CALCULATION - SKIP if already computed by pipeline
        existing_rsi = last_tick.get('rsi') if last_tick else None
        if existing_rsi and existing_rsi != 0.0 and existing_rsi != 50.0:
            indicators['rsi'] = float(existing_rsi)
            logger.debug(f"⏭️ [CALC_SKIP] {symbol} RSI from pipeline: {existing_rsi}")
        else:
            logger.debug(f"📊 [RSI_DEBUG] {symbol} - Calculating RSI locally")
            try:
                if combined_window.height >= 15:  # RSI14 needs 15+ periods
                    rsi_value = self._calculate_rsi_polars(combined_window, 14, symbol)
                    indicators['rsi'] = rsi_value
                    logger.debug(f"✅ [RSI_DEBUG] {symbol} - RSI calculated: {rsi_value}")
                else:
                    indicators['rsi'] = 50.0
            except Exception as e:
                logger.debug(f"❌ [RSI_DEBUG] {symbol} - RSI calculation failed: {e}")
                indicators['rsi'] = 50.0
        
        # ✅ DIAGNOSTIC 2: ATR CALCULATION (Polars)
        logger.info(f"📊 [ATR_DEBUG] {symbol} - Starting ATR calculation (Polars)")
        try:
            if combined_window.height >= 15:
                atr_value = self._calculate_atr_polars(combined_window, 14, symbol)
                indicators['atr'] = atr_value
                logger.info(f"✅ [ATR_DEBUG] {symbol} - ATR calculated: {atr_value}")
            else:
                logger.warning(f"⚠️ [ATR_DEBUG] {symbol} - Insufficient data for ATR, using default 0.0")
                indicators['atr'] = 0.0
        except Exception as e:
            logger.error(f"❌ [ATR_DEBUG] {symbol} - ATR calculation failed: {e}")
            indicators['atr'] = 0.0
        
        # ✅ DIAGNOSTIC 3: MACD CALCULATION (Polars)
        logger.info(f"📊 [MACD_DEBUG] {symbol} - Starting MACD calculation (Polars)")
        try:
            if combined_window.height >= 26:
                macd_value = self._calculate_macd_polars(combined_window, 12, 26, 9, symbol)
                indicators['macd'] = macd_value
                logger.info(f"✅ [MACD_DEBUG] {symbol} - MACD calculated: {macd_value}")
            else:
                logger.warning(f"⚠️ [MACD_DEBUG] {symbol} - Insufficient data for MACD ({combined_window.height} rows, need 26+), using default")
                indicators['macd'] = {'macd': 0, 'signal': 0, 'histogram': 0}
        except Exception as e:
            logger.error(f"❌ [MACD_DEBUG] {symbol} - MACD calculation failed: {e}")
            indicators['macd'] = {'macd': 0, 'signal': 0, 'histogram': 0}
        
        # ✅ DIAGNOSTIC 4: VWAP CALCULATION (Polars)
        logger.info(f"📊 [VWAP_DEBUG] {symbol} - Starting VWAP calculation (Polars)")
        try:
            vwap_value = self._calculate_vwap_polars(combined_window, symbol, getattr(self, 'redis_client', None)) if combined_window is not None else 0.0
            indicators['vwap'] = vwap_value if vwap_value is not None and isinstance(vwap_value, float) else 0.0
            logger.info(f"✅ [VWAP_DEBUG] {symbol} - VWAP calculated: {vwap_value}")
        except Exception as e:
            logger.error(f"❌ [VWAP_DEBUG] {symbol} - VWAP calculation failed: {e}")
            indicators['vwap'] = last_price  # Fallback to last price
        
        # ✅ DIAGNOSTIC 5: BOLLINGER BANDS CALCULATION (Polars)
        logger.info(f"📊 [BOLLINGER_DEBUG] {symbol} - Starting Bollinger Bands calculation (Polars)")
        try:
            if combined_window.height >= 20:
                bb_value = self._calculate_bollinger_bands_polars(combined_window, 20, 2.0, symbol)
                indicators['bollinger_bands'] = bb_value
                logger.info(f"✅ [BOLLINGER_DEBUG] {symbol} - Bollinger Bands calculated: {bb_value}")
            else:
                logger.warning(f"⚠️ [BOLLINGER_DEBUG] {symbol} - Insufficient data for Bollinger ({combined_window.height} rows, need 20+), using default")
                indicators['bollinger_bands'] = {'upper': 0, 'middle': 0, 'lower': 0}
        except Exception as e:
            logger.error(f"❌ [BOLLINGER_DEBUG] {symbol} - Bollinger Bands calculation failed: {e}")
            indicators['bollinger_bands'] = {'upper': 0, 'middle': 0, 'lower': 0}
        
        # ✅ DIAGNOSTIC 6: VOLUME PROFILE CALCULATION
        logger.info(f"📊 [VOLUME_PROFILE_DEBUG] {symbol} - Starting volume profile calculation")
        try:
            volumes_list = df['bucket_incremental_volume'].to_list() if 'bucket_incremental_volume' in df.columns else []
            volume_profile_value = self._calculate_volume_profile_optimized(prices_list, volumes_list, symbol=symbol)
            # ✅ FIXED: Ensure volume_profile is always a dict, never None
            if volume_profile_value is None or not isinstance(volume_profile_value, dict):
                logger.warning(f"⚠️ [VOLUME_PROFILE_DEBUG] {symbol} - Volume profile returned invalid value: {type(volume_profile_value)}, using empty dict")
                volume_profile_value = {}
            indicators['volume_profile'] = volume_profile_value
            logger.info(f"✅ [VOLUME_PROFILE_DEBUG] {symbol} - Volume profile calculated: {type(volume_profile_value).__name__}")
            if isinstance(volume_profile_value, dict):
                logger.info(f"📊 [VOLUME_PROFILE_DEBUG] {symbol} - Volume profile keys: {list(volume_profile_value.keys())[:10]}")
        except Exception as e:
            logger.error(f"❌ [VOLUME_PROFILE_DEBUG] {symbol} - Volume profile calculation failed: {e}", exc_info=True)
            indicators['volume_profile'] = {}
        finally:
            # ✅ FIXED: Ensure volume_profile is always present in indicators dict, even if calculation failed
            if 'volume_profile' not in indicators:
                logger.warning(f"⚠️ [VOLUME_PROFILE_DEBUG] {symbol} - volume_profile missing after calculation, setting empty dict")
                indicators['volume_profile'] = {}
        
        # ✅ DIAGNOSTIC 7: Z-SCORE CALCULATION
        try:
            if POLARS_AVAILABLE:
                z_score_value = self._calculate_zscore_polars(combined_window if combined_window is not None else pl.DataFrame(), self._session_zscore_lookback, symbol) if combined_window is not None else 0.0
            else:
                z_score_value = self._calculate_zscore_list(prices_list if prices_list is not None and isinstance(prices_list, list) and len(prices_list) > 0 and isinstance(prices_list[-1], float) else [], self._session_zscore_lookback, symbol) if prices_list is not None and isinstance(prices_list, list) and len(prices_list) > 0 and isinstance(prices_list[-1], float) else 0.0
            indicators['z_score'] = z_score_value if z_score_value is not None and isinstance(z_score_value, float) else 0.0
            logger.info(f"✅ [ZSCORE_DEBUG] {symbol} - Z-score calculated: {z_score_value:.4f}")
        except Exception as e:
            logger.error(f"❌ [ZSCORE_DEBUG] {symbol} - Z-score calculation failed: {e}", exc_info=True)
            indicators['z_score'] = 0.0
        
        # ✅ DIAGNOSTIC 8: VOLUME RATIO - READ ONLY (never recalculate)
        # ✅ SINGLE SOURCE OF TRUTH: volume_ratio is already calculated by websocket_parser and stored in Redis
        # This function ONLY reads pre-computed values - NEVER calculates
        try:
            indicators['volume_ratio'] = self._resolve_volume_ratio(symbol, last_tick or {})
        except Exception as e:
            logger.error(f"❌ Volume ratio read failed for {symbol}: {e}", exc_info=True)
            fallback_ratio = (last_tick or {}).get('volume_ratio') or 0.0
            try:
                indicators['volume_ratio'] = float(fallback_ratio) if fallback_ratio is not None else 0.0
            except (ValueError, TypeError):
                indicators['volume_ratio'] = 0.0
        
        # ✅ DIAGNOSTIC 9: PRICE CHANGE CALCULATION (with high precision)
        try:
            # ✅ FIXED: Calculate with high precision
            # ✅ FIXED: Use sophisticated calculate_price_change() directly (no wrapper needed)
            if len(prices_list) >= 2:
                price_change_value = self.calculate_price_change(prices_list[-1] if prices_list is not None and isinstance(prices_list, list) and len(prices_list) > 0 and isinstance(prices_list[-1], float) else 0.0, prices_list[-2] if prices_list is not None and isinstance(prices_list, list) and len(prices_list) >= 2 and isinstance(prices_list[-2], float) else 0.0) if prices_list is not None and isinstance(prices_list, list) and len(prices_list) >= 2 and isinstance(prices_list[-1], float) and isinstance(prices_list[-2], float) else 0.0
            else:
                price_change_value = 0.0
            
            # ✅ FIXED: Store with sufficient precision (6 decimal places for calculations)
            indicators['price_change'] = round(price_change_value, 6) if price_change_value is not None and isinstance(price_change_value, float) else 0.0  # Keep 6 decimal places for calculations
            indicators['price_change_display'] = round(price_change_value, 2) if price_change_value is not None and isinstance(price_change_value, float) else 0.0  # Display with 4 decimal places
            
            # ✅ FIXED: Also add price_change_pct in decimal format (0.003 = 0.3%) for pattern detectors
            indicators['price_change_pct'] = round(price_change_value / 100.0, 8) if price_change_value != 0 else 0.0
            
            # Enhanced debug logging
            if len(prices_list) >= 2:
                current_price = prices_list[-1] if prices_list else 0
                previous_price = prices_list[-2] if len(prices_list) >= 2 else 0
                raw_change = float(current_price) - float(previous_price) if current_price and previous_price else 0
                logger.info(f"✅ [PRICE_CHANGE_DEBUG] {symbol} - "
                          f"Price change: {price_change_value:.6f}% (display: {indicators['price_change_display']:.2f}%), "
                          f"Raw change: {raw_change:.8f}, "
                          f"Current: {current_price:.8f}, Previous: {previous_price:.8f}")
            else:
                logger.info(f"✅ [PRICE_CHANGE_DEBUG] {symbol} - Price change calculated: {price_change_value:.6f}% (pct: {indicators['price_change_pct']:.8f})")
                
        except Exception as e:
            logger.error(f"❌ [PRICE_CHANGE_DEBUG] {symbol} - Price change calculation failed: {e}", exc_info=True)
            indicators['price_change'] = 0.0
            indicators['price_change_pct'] = 0.0
            indicators['price_change_display'] = 0.0  # ✅ FIXED: Also set display value on error
        
        logger.info(f"🔍 [CALC_DEBUG] {symbol} Base indicators calculated: {list(indicators.keys())[:15]}")
        logger.info(f"🔍 [CALC_DEBUG] {symbol} RSI: {indicators.get('rsi')}, volume_ratio: {indicators.get('volume_ratio')}")
        
        # ✅ ENHANCED: Calculate volume-based indicators with access to volume_ratio history
        if historical_data is not None and not historical_data.is_empty():
            volume_indicators = self._calculate_indicators_with_volume(last_tick, historical_data)
            if volume_indicators:
                indicators.update(volume_indicators)
                logger.debug(f"🔍 [CALC_DEBUG] {symbol} Volume indicators added: {list(volume_indicators.keys())}")
        
        # ✅ DIAGNOSTIC 9: GREEKS CALCULATION
        logger.info(f"📊 [GREEKS_DEBUG] {symbol} - Starting Greeks calculation")
        is_option = symbol and self._is_option_symbol(symbol)
        is_future = symbol and self._is_future_symbol(symbol)
        logger.info(f"📊 [GREEKS_DEBUG] {symbol} - Symbol type check: is_option={is_option}, is_future={is_future}, GREEK_CALCULATIONS_AVAILABLE={GREEK_CALCULATIONS_AVAILABLE}")
        
        # ✅ CRITICAL: Skip Greeks calculation for futures (futures don't have Greeks)
        if is_future:
            logger.debug(f"⏭️ [GREEKS_DEBUG] {symbol} - Skipping Greeks calculation (futures don't have Greeks)")
            return {}
        
        try:
            if is_option and GREEK_CALCULATIONS_AVAILABLE:
                logger.info(f"📊 [GREEKS_DEBUG] {symbol} - Processing as option - tick_data keys: {list(last_tick.keys())[:15]}")
                
                # ✅ FAST PATH: Trust pre-calculated Greeks from crawler if they exist
                precomputed_fields = [
                    'delta', 'gamma', 'theta', 'vega', 'rho',
                    'iv', 'implied_volatility', 'dte_years',
                    'trading_dte', 'expiry_series', 'option_price'
                ]
                precomputed_greeks = {}
                for field in precomputed_fields:
                    raw_value = last_tick.get(field)
                    if raw_value is None:
                        continue
                    if field in ['expiry_series']:
                        precomputed_greeks[field] = raw_value
                        continue
                    try:
                        precomputed_greeks[field] = float(raw_value)
                    except (TypeError, ValueError):
                        continue
                
                core_greek_fields = ['delta', 'gamma', 'theta', 'vega']
                has_precomputed_core = any(
                    field in precomputed_greeks and precomputed_greeks[field] not in (None, 0.0)
                    for field in core_greek_fields
                )
                
                if has_precomputed_core:
                    greeks = precomputed_greeks
                    indicators.update(precomputed_greeks)
                    iv_value = (
                        precomputed_greeks.get('implied_volatility') 
                        or precomputed_greeks.get('iv')
                    )
                    if iv_value is not None:
                        indicators['implied_volatility'] = iv_value
                        last_tick['iv'] = iv_value
                        last_tick['implied_volatility'] = iv_value
                        last_tick['_iv_precalculated'] = True
                    logger.info(f"✅ [GREEKS_DEBUG] {symbol} - Using pre-computed Greeks from tick_data (crawler)")
                else:
                    # ✅ FIX: Calculate IV from option price using vollib inverse Black-Scholes
                    # First try to extract IV from tick data, if not available, calculate from option price
                    iv = last_tick.get('iv') or last_tick.get('implied_volatility') or last_tick.get('volatility')
                    if iv:
                        try:
                            iv = float(iv)
                            if iv <= 0 or iv > 5.0:  # Sanity check: IV should be between 0 and 500% (5.0)
                                logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} - IV out of range ({iv}), will calculate from option price")
                                iv = None  # Will calculate below
                            else:
                                logger.info(f"✅ [GREEKS_DEBUG] {symbol} - IV extracted from tick data: {iv}")
                        except (ValueError, TypeError):
                            logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} - Could not parse IV ({iv}), will calculate from option price")
                            iv = None
                    
                    # If IV not in tick data, calculate it from option price using vollib
                    if not iv or iv is None:
                        iv = self._calculate_iv_from_option_price(last_tick, symbol)
                        if iv:
                            logger.info(f"✅ [GREEKS_DEBUG] {symbol} - IV calculated from option price using vollib: {iv:.4f}")
                        else:
                            iv = 0.2  # Fallback default if calculation fails
                            logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} - Could not calculate IV from option price, using default 0.2")
                    
                    # ✅ FIXED: Store IV in last_tick so _calculate_greeks_for_last_tick can use it (avoids recalculating)
                    # Store in multiple field names for compatibility
                    # ✅ CRITICAL: Mark IV as pre-calculated to prevent recalculation in _calculate_greeks_for_last_tick
                    last_tick['iv'] = iv
                    last_tick['implied_volatility'] = iv
                    last_tick['volatility'] = iv
                    last_tick['_iv_precalculated'] = True  # Flag to prevent recalculation
                    
                    # Add IV to indicators dict (required by patterns)
                    indicators['implied_volatility'] = iv
                    indicators['iv'] = iv  # ✅ Also add 'iv' field for consistency
                    
                    # ✅ CRITICAL: Store live IV to Redis immediately after calculation (before Greeks calculation)
                    # This ensures IV is available to downstream consumers even if Greeks calculation fails
                    if iv and iv > 0:
                        try:
                            self._store_iv_to_redis(symbol, iv, ttl=300)  # 5 minutes TTL
                            logger.debug(f"✅ [IV_STORAGE] {symbol} - Stored live IV={iv:.4f} to Redis (pre-Greeks)")
                        except Exception as iv_store_err:
                            logger.warning(f"⚠️ [IV_STORAGE] {symbol} - Failed to store live IV: {iv_store_err}")
                    
                    logger.info(f"📊 [GREEKS_DEBUG] {symbol} - Calling _calculate_greeks_for_last_tick() with IV={iv:.4f} (pre-calculated)")
                    logger.info(f"🔍 [GREEKS_DEEP_DEBUG] {symbol} - Starting Greeks calculation")
                    logger.info(f"📊 [GREEKS_DEEP_DEBUG] {symbol} - Input tick data keys: {list(last_tick.keys())[:15]}")
                    logger.info(f"📊 [GREEKS_DEEP_DEBUG] {symbol} - Critical parameters: underlying_price={last_tick.get('underlying_price')}, strike_price={last_tick.get('strike_price')}, option_type={last_tick.get('option_type')}, option_price={last_tick.get('last_price')}, iv={iv}")
                    greeks = self._calculate_greeks_for_last_tick(last_tick)
                
                # ✅ ENHANCED: Check if Greeks are all zero (indicates calculation failure)
                if greeks:
                    delta = greeks.get('delta', 0.0)
                    gamma = greeks.get('gamma', 0.0)
                    theta = greeks.get('theta', 0.0)
                    vega = greeks.get('vega', 0.0)
                    
                    # Check if all Greeks are zero (calculation failure)
                    if delta == 0.0 and gamma == 0.0 and theta == 0.0 and vega == 0.0:
                        logger.warning(f"🚨 [BATCH_GREEKS_DEBUG] {symbol} - ALL GREEKS ARE 0.0! This will block option patterns!")
                        logger.warning(f"🚨 [BATCH_GREEKS_DEBUG] {symbol} - Attempting emergency fallback calculation...")
                        
                        # Try emergency fallback
                        emergency_greeks = self._calculate_greeks_emergency_fallback(symbol, last_tick, indicators)
                        if emergency_greeks:
                            # Check if emergency fallback produced non-zero Greeks
                            if (emergency_greeks.get('delta', 0.0) != 0.0 or 
                                emergency_greeks.get('gamma', 0.0) != 0.0 or 
                                emergency_greeks.get('theta', 0.0) != 0.0):
                                logger.info(f"✅ [BATCH_GREEKS_DEBUG] {symbol} - Emergency fallback succeeded: delta={emergency_greeks.get('delta'):.3f}")
                                greeks = emergency_greeks
                            else:
                                logger.error(f"❌ [BATCH_GREEKS_DEBUG] {symbol} - Emergency fallback also returned zeros!")
                        else:
                            logger.error(f"❌ [BATCH_GREEKS_DEBUG] {symbol} - Emergency fallback returned None!")
                    
                    indicators.update(greeks)
                    
                    # ✅ ENHANCED: Calculate and add advanced Greeks (vanna, charm, vomma, speed, zomma, color)
                    advanced_greeks = self._calculate_advanced_greeks(last_tick, greeks)
                    if advanced_greeks:
                        indicators.update(advanced_greeks)
                        logger.debug(f"✅ [ADVANCED_GREEKS] {symbol} - Advanced Greeks calculated: {list(advanced_greeks.keys())}")
                    
                    # ✅ CRITICAL: Explicitly store live IV to Redis before sending downstream
                    # IV is needed by downstream consumers (patterns, scanner) and must be available immediately
                    calculated_iv = greeks.get('iv') or greeks.get('implied_volatility') or iv
                    if calculated_iv and calculated_iv > 0:
                        try:
                            # Store IV explicitly using _store_iv_to_redis (creates ind:greeks:{symbol}:iv and ind:greeks:{symbol}:implied_volatility)
                            self._store_iv_to_redis(symbol, calculated_iv, ttl=300)  # 5 minutes TTL (matches TA indicators)
                            logger.debug(f"✅ [IV_STORAGE] {symbol} - Stored live IV={calculated_iv:.4f} to Redis before sending downstream")
                        except Exception as iv_store_err:
                            logger.warning(f"⚠️ [IV_STORAGE] {symbol} - Failed to store live IV: {iv_store_err}")
                    
                    logger.info(f"✅ [GREEKS_DEBUG] {symbol} - Greeks calculated successfully: Delta={greeks.get('delta')}, Gamma={greeks.get('gamma')}, Theta={greeks.get('theta')}, Vega={greeks.get('vega')}, Rho={greeks.get('rho')}, IV={calculated_iv}")
                else:
                    # Fallback: add zero Greeks if calculation returned None
                    logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} - Greeks calculation returned None/empty, trying emergency fallback...")
                    emergency_greeks = self._calculate_greeks_for_last_tick(last_tick) if last_tick is not None else None
                    if emergency_greeks:
                        indicators.update(emergency_greeks)
                        logger.info(f"✅ [BATCH_GREEKS_DEBUG] {symbol} - Emergency fallback succeeded: delta={emergency_greeks.get('delta'):.3f}")
                    else:
                        logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} - Emergency fallback failed, adding zero Greeks")
                        indicators.update({
                            'delta': 0.0,
                            'gamma': 0.0,
                            'theta': 0.0,
                            'vega': 0.0,
                            'rho': 0.0,
                            'iv': 0.0,  # ✅ FIX: Add iv field
                            'implied_volatility': 0.0,  # ✅ FIX: Add IV in emergency fallback
                            'dte_years': 0.0,
                            'trading_dte': 0,
                            'expiry_series': 'UNKNOWN',
                            'option_price': 0.0
                        })
            elif is_future:
                logger.info(f"📊 [GREEKS_DEBUG] {symbol} - Processing as future (delta=1.0)")
                # Futures don't have classical Greeks, but we still want meaningful DTE info.
                try:
                    # ✅ FIXED: Use _ensure_expiry_calculator() helper method with symbol for exchange inference
                    expiry_calc = self._ensure_expiry_calculator(symbol=symbol)
                    dte_info = expiry_calc.calculate_dte(symbol=symbol)
                    trading_dte = dte_info.get('trading_dte', 0)
                    dte_years = trading_dte / 365.0 if trading_dte else 0.0
                    indicators.update({
                        'delta': 1.0,            # Futures move 1:1 with underlying
                        'gamma': 0.0,
                        'theta': 0.0,
                        'vega': 0.0,
                        'rho': 0.0,
                        'dte_years': dte_years,
                        'trading_dte': trading_dte,
                        'expiry_series': dte_info.get('expiry_series', 'UNKNOWN'),
                        'option_price': 0.0
                    })
                    logger.info(f"✅ [GREEKS_DEBUG] {symbol} - Future Greeks set: delta=1.0, trading_dte={trading_dte}")
                except Exception as e:
                    logger.error(f"❌ [GREEKS_DEBUG] {symbol} - Future DTE calculation failed: {e}")
                    indicators.update({
                        'delta': 1.0,
                        'gamma': 0.0,
                        'theta': 0.0,
                        'vega': 0.0,
                        'rho': 0.0,
                        'dte_years': 0.0,
                        'trading_dte': 0,
                        'expiry_series': 'FUT',
                        'option_price': 0.0
                    })
            else:
                logger.info(f"📊 [GREEKS_DEBUG] {symbol} - Processing as equity (zero Greeks)")
                # ✅ FIX: Always add zero Greeks for non-option symbols so they're present in indicators dict
                # ✅ FIX: Detect asset class to set correct expiry_series (EQUITY, FUT, etc.)
                expiry_series_value = 'UNKNOWN'
                try:
                    from shared_core.redis_clients.redis_key_standards import get_symbol_parser
                    parser = get_symbol_parser()
                    parsed = parser.parse_symbol(symbol)
                    if parsed.asset_class == 'equity':
                        expiry_series_value = 'EQUITY'
                    elif parsed.asset_class == 'index':
                        expiry_series_value = 'INDEX'
                    elif parsed.asset_class == 'futures':
                        expiry_series_value = 'FUT'
                    elif parsed.asset_class == 'currency':
                        expiry_series_value = 'CDS'
                    elif parsed.asset_class == 'commodity':
                        expiry_series_value = 'MCX'
                    else:
                        expiry_series_value = parsed.asset_class.upper() if parsed.asset_class else 'UNKNOWN'
                    logger.info(f"✅ [GREEKS_DEBUG] {symbol} - Asset class detected: {parsed.asset_class}, expiry_series={expiry_series_value}")
                except Exception as e:
                    logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} - Could not parse symbol for expiry_series: {e}")
                    # Fallback: try simple detection
                    symbol_upper = str(symbol).upper()
                    if 'NIFTY' in symbol_upper or 'SENSEX' in symbol_upper or 'BANK' in symbol_upper:
                        expiry_series_value = 'INDEX'
                    elif symbol_upper.startswith('NSE:') or (not any(x in symbol_upper for x in ['CE', 'PE', 'FUT', 'NFO:', 'CDS:', 'MCX:'])):
                        expiry_series_value = 'EQUITY'
                
                indicators.update({
                    'delta': 0.0,
                    'gamma': 0.0,
                    'theta': 0.0,
                    'vega': 0.0,
                    'rho': 0.0,
                    'implied_volatility': 0.0,  # ✅ FIX: Add IV for equities (0.0 since equities don't have IV)
                    'dte_years': 0.0,
                    'trading_dte': 0,
                    'expiry_series': expiry_series_value,
                    'option_price': 0.0
                })
                logger.info(f"✅ [GREEKS_DEBUG] {symbol} - Zero Greeks added for equity, expiry_series={expiry_series_value}")
        except Exception as e:
            logger.error(f"❌ [GREEKS_DEBUG] {symbol} - Greeks calculation failed: {e}", exc_info=True)
            # Fallback: add zero Greeks on error
            indicators.update({
                'delta': 0.0,
                'gamma': 0.0,
                'theta': 0.0,
                'vega': 0.0,
                'rho': 0.0,
                'implied_volatility': 0.0,  # ✅ FIX: Add IV in error fallback
                'dte_years': 0.0,
                'trading_dte': 0,
                'expiry_series': 'UNKNOWN',
                'option_price': 0.0
            })
        
        advanced_greeks = self._calculate_advanced_greeks(last_tick, indicators)
        if advanced_greeks:
            indicators.update(advanced_greeks)
        if symbol and self._is_option_symbol(symbol):
            gamma_exposure = self._calculate_gamma_exposure(symbol)
            # ✅ FIX: Always store gamma_exposure even if 0.0 (patterns need to know it's calculated)
            if gamma_exposure is not None:
                indicators['gamma_exposure'] = gamma_exposure
                if gamma_exposure != 0.0:
                    logger.debug(f"✅ [GAMMA_EXPOSURE] {symbol} - Calculated and stored: {gamma_exposure:.2f}")
                else:
                    logger.debug(f"⚠️ [GAMMA_EXPOSURE] {symbol} - Calculated GEX is 0.0 (option chain may be empty)")
            else:
                logger.debug(f"❌ [GAMMA_EXPOSURE] {symbol} - Calculation failed or returned None")

        # ✅ ADDED: Calculate buy_pressure (market microstructure/order flow indicator)
        # buy_pressure is NOT a volume profile calculation - it's an order flow indicator
        # It measures the ratio of buy vs sell orders, not volume profile metrics
        if 'buy_pressure' not in indicators and tick_data and len(tick_data) > 0:
            try:
                # tick_data is List[Dict], get latest tick
                latest_tick = tick_data[-1] if isinstance(tick_data, list) else tick_data
                total_buy = float(latest_tick.get('total_buy_quantity', 0) or 0)
                total_sell = float(latest_tick.get('total_sell_quantity', 0) or 0)
                total_volume = total_buy + total_sell
                if total_volume > 0:
                    indicators['buy_pressure'] = total_buy / total_volume
                else:
                    indicators['buy_pressure'] = 0.5  # Neutral if no volume data
            except Exception as e:
                logger.debug(f"⚠️ [CALC_DEBUG] {symbol} Could not calculate buy_pressure: {e}")
                indicators['buy_pressure'] = 0.5  # Default neutral
        
        # ✅ FIXED: Always add vix_level (for ICT patterns) - fetch from Redis, NO HARDCODED FALLBACK
        # Calculated here in calculations.py (not in data_pipeline - data_pipeline only merges/passes data)
        if 'vix_level' not in indicators:
            vix_value = tick_data.get('vix_level') or tick_data.get('vix')
            if vix_value is None:
                snapshot = self._get_vix_snapshot()
                if snapshot:
                    vix_value = snapshot.get('value')
                    indicators.setdefault('vix_regime', snapshot.get('regime'))
                    indicators.setdefault('vix_timestamp', snapshot.get('timestamp'))
            if vix_value is not None and vix_value > 0:
                indicators['vix_level'] = float(vix_value)
                indicators.setdefault('vix', float(vix_value))
                logger.debug(f"✅ [VIX_LEVEL] {symbol} - Added vix_level={vix_value}")
            else:
                indicators.setdefault('vix_level', None)
        
        # ✅ PHASE 1: FINAL SUMMARY
        logger.info(f"🔍 [BATCH_CALC_EXIT] {symbol} - Indicator batch calculation complete")
        logger.info(f"📊 [BATCH_CALC_EXIT] {symbol} - Results: {len(indicators)} indicators calculated")
        logger.info(f"📊 [BATCH_CALC_EXIT] {symbol} - Indicator keys: {list(indicators.keys())[:20]}")
        
        # ✅ DEBUG: Log ALL indicators with their values (WARNING level so it shows up)
        logger.warning(f"🔍 [BATCH_CALC_DEBUG] {symbol} - ALL CALCULATED INDICATORS:")
        for ind_name, ind_value in indicators.items():
            if isinstance(ind_value, dict):
                logger.warning(f"   ✅ {ind_name}: {type(ind_value).__name__} with keys: {list(ind_value.keys())[:5]}")
            elif isinstance(ind_value, (int, float)):
                logger.warning(f"   ✅ {ind_name}: {ind_value}")
            else:
                logger.warning(f"   ✅ {ind_name}: {type(ind_value).__name__} = {str(ind_value)[:50]}")
        
        # Log critical indicators
        critical_indicators = ['rsi', 'macd', 'atr', 'vwap', 'volume_ratio', 'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200', 'delta', 'gamma', 'theta', 'volume_profile', 'buy_pressure', 'vix_level']
        logger.warning(f"📊 [BATCH_CALC_EXIT] {symbol} - Critical indicators status:")
        for ind_name in critical_indicators:
            if ind_name in indicators:
                value = indicators[ind_name]
                if isinstance(value, dict):
                    logger.warning(f"   ✅ {ind_name}: {type(value).__name__} with {len(value)} keys")
                else:
                    logger.warning(f"   ✅ {ind_name}: {value}")
            else:
                logger.warning(f"   ❌ {ind_name}: MISSING")
        
        logger.debug(f"🔍 [CALC_DEBUG] {symbol} Final indicators count: {len(indicators)}, keys: {list(indicators.keys())[:20]}")
        
        # ✅ ENHANCED: Calculate microstructure metrics (microprice, OFI, OBI, spread, depth, CVD)
        # Calculate BEFORE storage so they're included in the stored indicators
        microstructure_metrics = self._calculate_microstructure_metrics(symbol, last_tick)
        if microstructure_metrics:
            indicators.update(microstructure_metrics)
            logger.debug(f"✅ [MICROSTRUCTURE] {symbol} - Microstructure metrics calculated: {list(microstructure_metrics.keys())}")
        else:
            logger.debug(f"⚠️ [MICROSTRUCTURE] {symbol} - No microstructure metrics (missing depth data)")

        # ✅ ENHANCED: Calculate cross-asset metrics (USD:INR sensitivity, correlation, theoretical price, basis)
        cross_metrics = self._calculate_cross_asset_metrics(symbol, last_price)
        if cross_metrics:
            indicators.update(cross_metrics)
            logger.debug(f"✅ [CROSS_ASSET] {symbol} - Cross-asset metrics calculated: {list(cross_metrics.keys())}")

        # ✅ PERMANENT FIX: Store results IMMEDIATELY after calculation (atomic operation)
        # Now includes microstructure and cross-asset metrics
        if indicators and self._atomic_storage_enabled:
            # ✅ COMPREHENSIVE INDICATOR LOGGING: Log all indicators including IV and VWAP
            self._log_all_indicators(symbol, indicators)
            
            self._store_indicators_atomic(symbol, indicators)

        return indicators
    
    def calculate_usdinr_indicators(self, symbol: str, tick_data: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate USDINR-related indicators for commodities (GOLD, SILVER).
        
        ✅ ARCHITECTURAL FIX: Uses tick_data directly, no Redis reads for current price.
        CalculationService provides tick_data with all required fields.
        
        Args:
            symbol: Trading symbol (should contain GOLD or SILVER)
            tick_data: Tick data dictionary with last_price and other fields
            
        Returns:
            Dictionary with USDINR sensitivity, correlation, theoretical price, and basis
        """
        if not symbol or not tick_data:
            return {}
        
        symbol_upper = symbol.upper()
        if 'USDINR' not in symbol_upper and 'GOLD' not in symbol_upper and 'SILVER' not in symbol_upper:
            return {}
        
        try:
            # Get price from tick_data (provided by CalculationService)
            price = tick_data.get('last_price') or tick_data.get('last_traded_price') or tick_data.get('ltp')
            if not price:
                return {}
            
            price_val = float(price)
            
            # Update cross-asset state with current tick
            self._update_cross_asset_state(symbol, tick_data)
            
            # Calculate cross-asset metrics (includes USDINR sensitivity)
            metrics = self._calculate_cross_asset_metrics(symbol, price_val)
            
            return metrics
            
        except Exception as e:
            logger.debug(f"⚠️ USDINR indicators calculation error for {symbol}: {e}")
            return {}
    
    def _log_batch_input_snapshot(self, symbol: Optional[str], ticks: List[Dict[str, Any]]) -> None:
        """Log the last tick that enters indicator calculations (volume_ratio, Greeks, VIX)."""
        if not ticks:
            return
        latest = ticks[-1]
        symbol_to_log = symbol or latest.get('symbol') or latest.get('tradingsymbol') or "UNKNOWN"
        snapshot = {
            'volume_ratio': _extract_volume_ratio(latest),
            'delta': latest.get('delta'),
            'gamma': latest.get('gamma'),
            'gamma_exposure': latest.get('gamma_exposure'),
            'oi': latest.get('open_interest') or latest.get('oi'),
            'order_flow_imbalance': latest.get('order_flow_imbalance'),
            'vix_level': latest.get('vix_level'),
            'last_price': latest.get('last_price'),
        }
        summary = ", ".join(
            f"{field}={snapshot[field]}"
            for field in snapshot
            if snapshot[field] not in (None, "")
        )
        logger.info(
            "🧮 [BATCH_INPUT] %s ticks=%s window=%s %s",
            symbol_to_log,
            len(ticks),
            getattr(self, "_session_window_size", "unknown"),
            summary,
        )

    def _log_all_indicators(self, symbol: str, indicators: dict):
        """✅ COMPREHENSIVE: Log all indicators including IV, VWAP, and all technical indicators"""
        try:
            # Core technical indicators
            tech_indicators = {
                'rsi': indicators.get('rsi'),
                'macd': indicators.get('macd'),
                'atr': indicators.get('atr'),
                'vwap': indicators.get('vwap'),
                'z_score': indicators.get('z_score'),
            }
            
            # EMAs
            ema_indicators = {
                'ema_5': indicators.get('ema_5'),
                'ema_10': indicators.get('ema_10'),
                'ema_20': indicators.get('ema_20'),
                'ema_50': indicators.get('ema_50'),
                'ema_100': indicators.get('ema_100'),
                'ema_200': indicators.get('ema_200'),
            }
            
            # Bollinger Bands
            bb = indicators.get('bollinger_bands', {})
            if isinstance(bb, dict):
                bb_indicators = {
                    'bb_upper': bb.get('upper'),
                    'bb_middle': bb.get('middle'),
                    'bb_lower': bb.get('lower'),
                }
            else:
                bb_indicators = {'bollinger_bands': bb}
            
            # Greeks (for options)
            greek_indicators = {
                'delta': indicators.get('delta'),
                'gamma': indicators.get('gamma'),
                'theta': indicators.get('theta'),
                'vega': indicators.get('vega'),
                'gamma_exposure': indicators.get('gamma_exposure'),
                'rho': indicators.get('rho'),
            }
            
            # IV (Implied Volatility) - CRITICAL
            iv_indicators = {
                'iv': indicators.get('iv'),
                'implied_volatility': indicators.get('implied_volatility'),
            }
            
            # Volume indicators
            volume_indicators = {
                'volume_ratio': indicators.get('volume_ratio'),
                'price_change': indicators.get('price_change'),
                'price_change_pct': indicators.get('price_change_pct'),
            }
            
            microstructure_indicators = {
                'order_flow_imbalance': indicators.get('order_flow_imbalance'),
                'depth_imbalance': indicators.get('depth_imbalance'),
                'microprice': indicators.get('microprice'),
                'cumulative_volume_delta': indicators.get('cumulative_volume_delta'),
            }
            
            cross_asset_indicators = {
                'usdinr_sensitivity': indicators.get('usdinr_sensitivity'),
                'usdinr_correlation': indicators.get('usdinr_correlation'),
                'usdinr_theoretical_price': indicators.get('usdinr_theoretical_price'),
                'usdinr_basis': indicators.get('usdinr_basis'),
            }
            
            # Format MACD if it's a dict
            macd_str = tech_indicators['macd']
            if isinstance(macd_str, dict):
                macd_str = f"macd={macd_str.get('macd', 0):.4f}, signal={macd_str.get('signal', 0):.4f}, hist={macd_str.get('histogram', 0):.4f}"
            else:
                macd_str = f"macd={macd_str}"
            
            # Log all indicators in organized sections
            rsi_val = tech_indicators['rsi'] if tech_indicators['rsi'] is not None else 0.0
            atr_val = tech_indicators['atr'] if tech_indicators['atr'] is not None else 0.0
            vwap_val = tech_indicators['vwap'] if tech_indicators['vwap'] is not None else 0.0
            z_score_val = tech_indicators['z_score'] if tech_indicators['z_score'] is not None else 0.0
            logger.info(f"📊 [INDICATORS] {symbol} - Technical: RSI={rsi_val:.2f}, {macd_str}, ATR={atr_val:.2f}, VWAP={vwap_val:.2f}, Z-Score={z_score_val:.2f}")
            
            # Log EMAs
            ema_str = ", ".join([f"{k}={v:.2f}" for k, v in ema_indicators.items() if v is not None])
            if ema_str:
                logger.info(f"📊 [INDICATORS] {symbol} - EMAs: {ema_str}")
            
            # Log Bollinger Bands
            if isinstance(bb_indicators, dict) and any(v is not None for v in bb_indicators.values()):
                bb_str = ", ".join([f"{k}={v:.2f}" for k, v in bb_indicators.items() if v is not None])
                logger.info(f"📊 [INDICATORS] {symbol} - Bollinger: {bb_str}")
            
            # Log IV (CRITICAL - user specifically asked for this)
            iv_value = iv_indicators.get('iv') or iv_indicators.get('implied_volatility')
            if iv_value is not None:
                logger.info(f"📊 [INDICATORS] {symbol} - IV: {iv_value:.4f} ({iv_value*100:.2f}%)")
            else:
                logger.warning(f"⚠️ [INDICATORS] {symbol} - IV: MISSING")
            
            # Log Greeks (if present - options only)
            if any(v is not None and v != 0.0 for v in greek_indicators.values()):
                greek_str = ", ".join([f"{k}={v:.4f}" for k, v in greek_indicators.items() if v is not None])
                logger.info(f"📊 [INDICATORS] {symbol} - Greeks: {greek_str}")
            
            # Log Volume indicators
            vol_str = ", ".join([f"{k}={v:.4f}" if isinstance(v, float) else f"{k}={v}" for k, v in volume_indicators.items() if v is not None])
            if vol_str:
                logger.info(f"📊 [INDICATORS] {symbol} - Volume: {vol_str}")
            
            micro_str = ", ".join([f"{k}={v:.4f}" if isinstance(v, float) else f"{k}={v}" for k, v in microstructure_indicators.items() if v is not None])
            if micro_str:
                logger.info(f"📊 [INDICATORS] {symbol} - Microstructure: {micro_str}")
            
            cross_asset_str = ", ".join([f"{k}={v:.4f}" if isinstance(v, float) else f"{k}={v}" for k, v in cross_asset_indicators.items() if v is not None])
            if cross_asset_str:
                logger.info(f"📊 [INDICATORS] {symbol} - CrossAsset: {cross_asset_str}")
                
        except Exception as e:
            logger.error(f"❌ [INDICATORS] {symbol} - Error logging indicators: {e}", exc_info=True)
    
    def _store_indicators_atomic(self, symbol: str, indicators: dict):
        """
        ✅ UNIFIED: Store indicators using RedisStorageManager (single source of truth).
        
        This ensures indicators are stored in the same atomic operation as calculation,
        preventing data loss between calculation and storage steps.
        """
        # ✅ UNIFIED: Use RedisStorageManager instead of duplicate implementation
        try:
            from shared_core.redis_clients.unified_data_storage import get_unified_storage
            
            if not hasattr(self, 'redis_client') or not self.redis_client:
                logger.warning(f"⚠️ [ATOMIC_STORE] {symbol} - No redis_client available, skipping storage")
                return
            
            storage = get_unified_storage(redis_client=self.redis_client)
            storage.store_indicators(symbol, indicators)
            logger.info(f"✅ [ATOMIC_STORE] {symbol} - Stored indicators via RedisStorageManager")
        except Exception as e:
            logger.error(f"❌ [ATOMIC_STORE] {symbol} - Storage failed: {e}", exc_info=True)
    
    def _get_latest_ticks(self, symbol: str, count: int) -> "pl.DataFrame":
        """
        ✅ ENHANCED: Get latest ticks with guaranteed volume_ratio field.
        
        Returns Polars DataFrame with volume_ratio included in the session-based window.
        Falls back to HistoricalTickQueries if session-based window is empty.
        """
        # Try session-based window first
        if symbol in self._symbol_sessions:
            window = self._symbol_sessions[symbol]
            if window.height > 0:
                result = window.tail(count)
                
                # ✅ CRITICAL: Ensure volume_ratio exists in the result
                if 'volume_ratio' not in result.columns:
                    result = result.with_columns([
                        pl.lit(1.0).cast(pl.Float64).alias('volume_ratio') if POLARS_AVAILABLE and pl is not None else pl.lit(1.0).cast(pl.Float64).alias('volume_ratio')  # type: ignore[union-attr]
                    ]) if result is not None and isinstance(result, pl.DataFrame) and POLARS_AVAILABLE and pl is not None else pl.DataFrame()  # type: ignore[union-attr]
                
                # ✅ CRITICAL: Ensure volume exists in the result
                if 'volume' not in result.columns:
                    result = result.with_columns([
                        pl.lit(0).cast(pl.Int64).alias('volume') if POLARS_AVAILABLE and pl is not None else pl.lit(0).cast(pl.Int64).alias('volume')  # type: ignore[union-attr]
                    ]) if result is not None and isinstance(result, pl.DataFrame) and POLARS_AVAILABLE and pl is not None else pl.DataFrame()  # type: ignore[union-attr]
                
                return result
        
        # ✅ FALLBACK: Use HistoricalTickQueries if session-based window is empty
        if self.historical_queries:
            try:
                import time
                end_time = int(time.time() * 1000)  # Current time in milliseconds
                start_time = end_time - (count * 1000)  # Approximate: count seconds back
                
                historical_ticks = self.historical_queries.get_historical_ticks(
                    symbol, start_time, end_time, max_ticks=count
                )
                
                if historical_ticks:
                    # Convert to Polars DataFrame
                    import polars as pl
                    return pl.DataFrame(historical_ticks) if historical_ticks is not None and isinstance(historical_ticks, list) else pl.DataFrame()
            except Exception as e:
                logger.debug(f"HistoricalTickQueries fallback failed for {symbol}: {e}")
        
        return pl.DataFrame() if POLARS_AVAILABLE and pl is not None else pl.DataFrame()  # type: ignore[union-attr]
    
    def _calculate_indicators_with_volume(self, current_tick: Dict, historical_data: "pl.DataFrame") -> Dict:
        """
        ✅ ENHANCED: Calculate indicators with access to volume_ratio in historical data.
        
        Volume-based indicators now have access to volume_ratio history from session-based window.
        """
        indicators = {}
        
        if historical_data.is_empty() or 'volume_ratio' not in historical_data.columns:
            return indicators
        
        try:
            # ✅ Volume Ratio Moving Average (5-period)
            if historical_data.height >= 5:
                volume_ratio_ma_5 = historical_data['volume_ratio'].tail(5).mean()
                indicators['volume_ratio_ma_5'] = float(volume_ratio_ma_5) if volume_ratio_ma_5 is not None and isinstance(volume_ratio_ma_5, float) else 0.0
            
            # ✅ Volume Ratio Moving Average (10-period)
            if historical_data.height >= 10:
                volume_ratio_ma_10 = historical_data['volume_ratio'].tail(10).mean()
                indicators['volume_ratio_ma_10'] = float(volume_ratio_ma_10) if volume_ratio_ma_10 is not None and isinstance(volume_ratio_ma_10, float) else 0.0
            
            # ✅ Volume Spike Detection using Z-score
            current_ratio = float(current_tick.get('volume_ratio', 1.0))
            if historical_data.height >= 10:
                volume_ratio_series = historical_data['volume_ratio'].tail(10)
                avg_ratio = float(volume_ratio_series.mean()) if volume_ratio_series is not None and isinstance(volume_ratio_series, pl.Series) and volume_ratio_series.mean() is not None and isinstance(volume_ratio_series.mean(), float) else 0.0
                std_ratio = float(volume_ratio_series.std()) if volume_ratio_series is not None and isinstance(volume_ratio_series, pl.Series) and volume_ratio_series.std() is not None and isinstance(volume_ratio_series.std(), float) else 0.0
                
                if std_ratio and std_ratio > 0:
                    z_score = (current_ratio - avg_ratio) / std_ratio
                    indicators['volume_ratio_zscore'] = float(z_score) if z_score is not None and isinstance(z_score, float) else 0.0
                    
                    # Volume spike alert (Z-score > 2.0 indicates significant spike)
                    if z_score > 2.0:
                        indicators['volume_spike_alert'] = True
                        indicators['volume_spike_severity'] = 'high' if z_score > 3.0 else 'medium'
                    elif z_score > 1.5:
                        indicators['volume_spike_alert'] = False
                        indicators['volume_spike_severity'] = 'low'
                    else:
                        indicators['volume_spike_alert'] = False
                        indicators['volume_spike_severity'] = 'normal'
            
            # ✅ Volume Ratio Trend (comparing recent vs historical average)
            if historical_data.height >= 20:
                recent_avg = float(historical_data['volume_ratio'].tail(5).mean()) if historical_data is not None and isinstance(historical_data, pl.DataFrame) and historical_data['volume_ratio'].tail(5).mean() is not None and isinstance(historical_data['volume_ratio'].tail(5).mean(), float) else 0.0 if POLARS_AVAILABLE and pl is not None else 0.0 # type: ignore[union-attr] # type: ignore[union-attr]
                historical_avg = float(historical_data['volume_ratio'].tail(20).mean()) if historical_data is not None and isinstance(historical_data, pl.DataFrame) and historical_data['volume_ratio'].tail(20).mean() is not None and isinstance(historical_data['volume_ratio'].tail(20).mean(), float) else 0.0 if POLARS_AVAILABLE and pl is not None else 0.0 # type: ignore[union-attr] # type: ignore[union-attr]
                 # type: ignore[union-attr] if POLARS_AVAILABLE and pl is not None else 0.0     
                trend_ratio = recent_avg / historical_avg if recent_avg is not None and isinstance(recent_avg, float) and historical_avg is not None and isinstance(historical_avg, float) else 0.0
                indicators['volume_ratio_trend'] = float(trend_ratio) if trend_ratio is not None and isinstance(trend_ratio, float) else 0.0 if POLARS_AVAILABLE and pl is not None else 0.0 # type: ignore[union-attr]
                
        except Exception as e:
            logger.debug(f"Error calculating volume-based indicators: {e}")
        
        return indicators
    
    # ✅ NEW HELPER METHODS (extracting existing logic for optimization)
    def _batch_calculate_emas(self, prices: List[float], last_price: float) -> Dict:
        """✅ OPTIMIZED: Batch calculate all EMAs from price series - single Polars operation"""
        if not prices:
            return {f'ema_{period}': last_price for period in [5, 10, 20, 50, 100, 200]}
        
        try:
            # ✅ OPTIMIZED: Create price series once and reuse
            price_series = pl.Series('price', prices)
            emas = {}
            
            for period in [5, 10, 20, 50, 100, 200]:
                if len(prices) >= period:
                    emas[f'ema_{period}'] = float(price_series.ewm_mean(span=period).tail(1)[0]) if price_series.ewm_mean(span=period).tail(1)[0] is not None and isinstance(price_series.ewm_mean(span=period).tail(1)[0], float) else 0.0
                else:
                    emas[f'ema_{period}'] = float(prices[-1]) if prices is not None and isinstance(prices, list) and len(prices) > 0 and isinstance(prices[-1], float) else 0.0   
            
            return emas
        except Exception as e:
            # Fallback to individual calculations
            logger.debug(f"Batch EMA calculation failed, using fallback: {e}")
            return {
                'ema_5': self.calculate_ema(prices, 5) if prices is not None and isinstance(prices, list) and len(prices) > 0 and isinstance(prices[-1], float) else 0.0,
                'ema_10': self.calculate_ema(prices, 10) if prices is not None and isinstance(prices, list) and len(prices) > 0 and isinstance(prices[-1], float) else 0.0,
                'ema_20': self.calculate_ema(prices, 20),
                'ema_50': self.calculate_ema(prices, 50) if prices is not None and isinstance(prices, list) and len(prices) > 0 and isinstance(prices[-1], float) else 0.0,
                'ema_100': self.calculate_ema(prices, 100),
                'ema_200': self.calculate_ema(prices, 200)
            }
    
    def _get_optimized_last_price(self, prices: List[float], symbol: str) -> float:
        """✅ OPTIMIZED: Optimized last price retrieval with Redis"""
        last_price = prices[-1] if prices else 0.0
        redis_client = getattr(self, 'redis_client', None)
        
        if redis_client and hasattr(redis_client, 'get_last_price') and symbol:
            try:
                redis_price = redis_client.get_last_price(symbol)
                if redis_price and redis_price > 0:
                    return redis_price
            except Exception:
                pass  # Silently fallback to calculated price
        
        return last_price
    
    def _get_cached_vwap(self, tick_data: List[Dict], symbol: str) -> float:
        """✅ OPTIMIZED: Get VWAP with caching support"""
        # Get Redis client if available
        redis_client = getattr(self, 'redis_client', None)
        # Use existing function with Redis fallback
        return calculate_vwap_with_redis_fallback(tick_data, symbol, redis_client)
    
    def _get_volume_ratio(self, last_tick: Dict, symbol: str) -> float:
        """
        ✅ SINGLE SOURCE OF TRUTH: Read pre-computed volume ratio from tick_data ONLY.
        
        Volume ratio is ALREADY calculated by websocket_parser and stored in Redis.
        This function ONLY reads pre-computed values - NEVER calculates.
        
        If volume_ratio is missing, return 0.0 (do NOT recalculate - it should be in Redis).
        """
        # ✅ ONLY read pre-computed value from tick_data - NEVER recalculate
        volume_ratio = _extract_volume_ratio(last_tick)
        if symbol and last_tick:
            self._update_cross_asset_state(symbol, last_tick)
        
        # ✅ REMOVED: Recalculation logic - volume_ratio should already be in tick_data or Redis
        # If it's missing, return 0.0 and let data_pipeline fetch it from Redis
        if not volume_ratio or volume_ratio == 0.0:
            logger.debug(f"⚠️ [VOLUME_DEBUG] {symbol} volume_ratio not found in tick_data (should be fetched from Redis by data_pipeline)")
        
        return volume_ratio if volume_ratio else 0.0
    
    def _log_volume_data_flow(self, tick_data: Dict[str, Any], token: int) -> None:
        """
        Log volume data flow for debugging and monitoring.
        
        Args:
            tick_data: Tick data dictionary with volume fields
            token: Instrument token for logging
        """
        volume_ratio = tick_data.get('volume_ratio')
        volume = tick_data.get('volume')
        
        # Use module-level logger (consistent with other methods in this class)
        logger.debug(
            f"Volume Data Flow - Token: {token}, "
            f"Volume: {volume}, "
            f"Ratio: {volume_ratio}, "
            f"Source: {'VolumeManager' if volume_ratio is not None else 'MISSING'}"
        )
    
    # ✅ REMOVED: _extract_underlying_from_symbol() - Use extract_underlying_from_symbol from shared_core.utils.symbol_parser
    # The shared utility provides underlying extraction functionality

    def _get_underlying_price_from_redis(self, underlying_symbol: str, option_symbol: str = None) -> float:
        """
        ⚠️ FALLBACK ONLY: Get underlying price from Redis.
        
        This should rarely be called. In the new architecture:
        - CalculationService provides underlying_price in tick_data
        - This is only used as last resort if tick_data is missing underlying_price
        - Also used by GammaExposureCalculator (scanner batch operations)
        
        For GOLD/SILVER options, ensures we try:
        1. GOLD/SILVER future price (e.g., GOLD25DECFUT)
        2. GOLD/SILVER spot price
        3. Standard underlying price resolution
        """
        # ✅ ENHANCED: For GOLD/SILVER, ensure we try future prices first
        if underlying_symbol and ('GOLD' in underlying_symbol.upper() or 'SILVER' in underlying_symbol.upper()):
            if option_symbol and self.redis_client:
                try:
                    # Try to get future price first (most accurate for commodities)
                    # Note: derive_future_variants_from_option was removed - using simple variant generation
                    future_variants = [option_symbol.replace('CE', 'FUT').replace('PE', 'FUT')]
                    
                    db1_client = None
                    if hasattr(self.redis_client, 'get_client'):
                        db1_client = self.redis_client.get_client(1)
                    elif hasattr(self.redis_client, 'redis'):
                        db1_client = self.redis_client.redis
                    else:
                        db1_client = self.redis_client
                    
                    if db1_client and future_variants:
                        for future_variant in future_variants:
                            try:
                                # Try multiple key formats for future price
                                price_keys = [
                                    f"price:latest:{future_variant}",
                                    f"price:{future_variant}",
                                ]
                                
                                # Also try tick latest
                                from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
                                tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(future_variant)
                                
                                for price_key in price_keys:
                                    try:
                                        price_val = db1_client.get(price_key)
                                        if price_val:
                                            if isinstance(price_val, bytes):
                                                price_val = price_val.decode('utf-8')
                                            price = float(price_val)
                                            if price > 0:
                                                logger.info(f"✅ [UNDERLYING_PRICE] {option_symbol} - Found {underlying_symbol} future price from {price_key}: {price:.2f}")
                                                return price
                                    except Exception:
                                        continue
                                
                                # Try tick hash
                                if db1_client.exists(tick_key):
                                    tick_data = db1_client.hgetall(tick_key)
                                    price_val = (
                                        tick_data.get(b"last_price") or tick_data.get("last_price") or
                                        tick_data.get(b"close") or tick_data.get("close")
                                    )
                                    if price_val:
                                        if isinstance(price_val, bytes):
                                            price_val = price_val.decode('utf-8')
                                        price = float(price_val)
                                        if price > 0:
                                            logger.info(f"✅ [UNDERLYING_PRICE] {option_symbol} - Found {underlying_symbol} future price from tick hash {tick_key}: {price:.2f}")
                                            return price
                            except Exception as e:
                                logger.debug(f"⚠️ [UNDERLYING_PRICE] {option_symbol} - Failed to fetch from future variant {future_variant}: {e}")
                                continue
                except Exception as e:
                    logger.debug(f"⚠️ [UNDERLYING_PRICE] {option_symbol} - Error in GOLD/SILVER future lookup: {e}")
        
        # Fallback to standard resolution
        return resolve_underlying_price(
            underlying_symbol,
            redis_client=getattr(self, 'redis_client', None),
            option_symbol=option_symbol,
            logger_obj=logger,
            historical_queries=getattr(self, 'historical_queries', None),
        )

    def _calculate_iv_from_option_price(self, last_tick: Dict, symbol: str) -> Optional[float]:
        """
        Calculate implied volatility from option price using vollib's inverse Black-Scholes.
        
        Args:
            last_tick: Tick data with option price, underlying price, strike, etc.
            symbol: Option symbol for logging
            
        Returns:
            Implied volatility (float) or None if calculation fails
        """
        try:
            if not PY_VOLLIB_AVAILABLE or implied_volatility is None:
                logger.debug(f"🔍 [IV_CALC] {symbol} - py_vollib not available, cannot calculate IV")
                return None
            
            # Extract required parameters
            option_price = last_tick.get('last_price') or last_tick.get('price') or last_tick.get('close')
            underlying_price = last_tick.get('underlying_price') or last_tick.get('underlying') or last_tick.get('spot_price', 0.0)
            strike_price = last_tick.get('strike_price') or last_tick.get('strike', 0.0)
            option_type = last_tick.get('option_type') or last_tick.get('option_type_raw')
            risk_free_rate = last_tick.get('risk_free_rate', 0.05)
            
            # ✅ FIXED: Extract strike_price and option_type from symbol if missing (same logic as _calculate_greeks_for_last_tick)
            if not strike_price or not option_type:
                symbol_upper = str(symbol).upper() if symbol else ''
                symbol_for_parsing = symbol_upper
                
                # Strip prefix if present
                if symbol_for_parsing.startswith('NFO:') or symbol_for_parsing.startswith('NSE:') or \
                   symbol_for_parsing.startswith('CDS:') or symbol_for_parsing.startswith('MCX:'):
                    symbol_for_parsing = symbol_for_parsing[4:]
                
                # Use regex to extract strike and option type
                import re
                strike_match = re.search(r'(\d+)(CE|PE)$', symbol_for_parsing)
                if strike_match:
                    if not strike_price:
                        strike_price = float(strike_match.group(1))
                        last_tick['strike_price'] = strike_price
                        logger.debug(f"✅ [IV_CALC] {symbol} Extracted strike_price={strike_price} from symbol")
                    if not option_type:
                        option_type = 'call' if strike_match.group(2) == 'CE' else 'put'
                        last_tick['option_type'] = option_type
                        logger.debug(f"✅ [IV_CALC] {symbol} Extracted option_type={option_type} from symbol")
            
            # ⚠️ FALLBACK: Extract underlying_price from Redis if missing (should come from tick_data in new architecture)
            if not underlying_price or underlying_price <= 0:
                underlying_symbol = extract_underlying_from_symbol(symbol)
                if underlying_symbol:
                    underlying_price = self._get_underlying_price_from_redis(underlying_symbol, symbol)
                    if underlying_price:
                        last_tick['underlying_price'] = underlying_price
                        logger.debug(f"✅ [IV_CALC] {symbol} Fetched underlying_price={underlying_price} from Redis for {underlying_symbol}")
            
            # Calculate DTE (days to expiry)
            # ✅ FIXED: Prefer trading_dte if available (more accurate for intraday calculations)
            trading_dte = last_tick.get('trading_dte')
            if trading_dte and trading_dte > 0:
                # Use trading_dte directly (already accounts for market hours)
                dte_years = float(trading_dte) / 365.0
            else:
                # Fallback to expiry_date calculation
                expiry_date = last_tick.get('expiry_date')
                if expiry_date:
                    if isinstance(expiry_date, str):
                        expiry_date = datetime.strptime(expiry_date, '%Y-%m-%d')
                    # ✅ FIXED: Use total_seconds() instead of .days to get fractional days
                    # This handles cases where expiry is < 24 hours away
                    delta = expiry_date - datetime.now()
                    if delta.total_seconds() > 0:
                        dte_days = delta.total_seconds() / 86400.0  # Convert seconds to days
                        dte_years = dte_days / 365.0
                    else:
                        dte_years = 0.0
                else:
                    # Try to parse expiry directly from symbol using ExpiryCalculator
                    try:
                        expiry_calc = self._ensure_expiry_calculator(symbol=symbol)
                        parsed_expiry = expiry_calc.parse_expiry_from_symbol(symbol)
                        if parsed_expiry:
                            expiry_date = parsed_expiry
                            last_tick['expiry_date'] = parsed_expiry.strftime('%Y-%m-%d')
                            delta = expiry_date - datetime.now()
                            if delta.total_seconds() > 0:
                                dte_days = delta.total_seconds() / 86400.0
                                dte_years = dte_days / 365.0
                            else:
                                dte_years = 0.0
                    except Exception:
                        expiry_date = None
                    if not expiry_date:
                        # Try to get from tick data
                        dte_years = last_tick.get('dte_years', 0.0)
                        if not dte_years or dte_years <= 0:
                            # Last resort: try trading_dte again (might have been set after first check)
                            trading_dte = last_tick.get('trading_dte', 0)
                            dte_years = float(trading_dte) / 365.0 if trading_dte and trading_dte > 0 else 0.0
            
            # ✅ VALIDATE INPUTS FIRST (check for None before numeric comparisons)
            if option_price is None or underlying_price is None or strike_price is None or dte_years is None:
                logger.warning(f"⚠️ [IV_CALC] {symbol} - Missing inputs for IV calculation: "
                              f"option_price={option_price}, underlying_price={underlying_price}, "
                              f"strike_price={strike_price}, dte_years={dte_years}")
                return None
            
            if not all([option_price > 0, underlying_price > 0, strike_price > 0, dte_years > 0]):
                logger.warning(f"⚠️ [IV_CALC] {symbol} - Invalid inputs for IV calculation: "
                              f"option_price={option_price}, underlying_price={underlying_price}, "
                              f"strike_price={strike_price}, dte_years={dte_years}")
                return None
            
            # ✅ ENSURE OPTION PRICE IS VALID (must be >= intrinsic value)
            option_type_normalized = option_type.lower() if option_type else 'call'
            if option_type_normalized in ('put', 'pe'):
                intrinsic = max(strike_price - underlying_price, 0)
                min_price = intrinsic + 0.01  # Add epsilon
                if option_price < min_price:
                    logger.warning(f"⚠️ [IV_CALC] {symbol} - Option price {option_price:.2f} < intrinsic {intrinsic:.2f}, "
                                  f"using {min_price:.2f} for IV calculation")
                    option_price = min_price
            else:  # CALL or CE
                intrinsic = max(underlying_price - strike_price, 0)
                min_price = intrinsic + 0.01
                if option_price < min_price:
                    logger.warning(f"⚠️ [IV_CALC] {symbol} - Option price {option_price:.2f} < intrinsic {intrinsic:.2f}, "
                                  f"using {min_price:.2f} for IV calculation")
                    option_price = min_price
            
            # Determine option flag
            flag = 'c' if option_type_normalized in ['call', 'ce', 'c'] else 'p'
            
            # Calculate IV using vollib's inverse Black-Scholes
            try:
                calculated_iv = implied_volatility(
                    price=option_price,
                    S=underlying_price,
                    K=strike_price,
                    t=dte_years,
                    r=risk_free_rate,
                    flag=flag
                )
                
                # Sanity check: IV should be between 0 and 500% (5.0)
                if 0 < calculated_iv <= 5.0:
                    # Store and return
                    self._store_iv_to_redis(symbol, calculated_iv)
                    return calculated_iv
                else:
                    logger.warning(f"⚠️ [IV_CALC] {symbol} - Calculated IV out of range: {calculated_iv}")
            except Exception as e:
                logger.error(f"❌ [IV_CALC] {symbol} - IV calculation failed: {e}")
                return None
            
            # ✅ FALLBACK 2: Newton-Raphson method (more accurate than approximation)
            try:
                from math import log, sqrt, exp
                from scipy.stats import norm
                from scipy.optimize import newton
                
                def black_scholes_price(volatility):
                    """Black-Scholes price given volatility"""
                    S, K, T, r = underlying_price, strike_price, dte_years, risk_free_rate
                    flag = 'c' if option_type.lower() in ['call', 'ce', 'c'] else 'p'
                    
                    if T <= 0 or volatility <= 0:
                        return 0.0
                    
                    d1 = (log(S / K) + (r + 0.5 * volatility**2) * T) / (volatility * sqrt(T))
                    d2 = d1 - volatility * sqrt(T)
                    
                    if flag == 'c':
                        price = S * norm.cdf(d1) - K * exp(-r * T) * norm.cdf(d2)
                    else:
                        price = K * exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
                    
                    return price
                
                def objective(volatility):
                    """Objective function: difference between market price and BS price"""
                    return black_scholes_price(volatility) - option_price
                
                # Initial guess: 20% volatility
                initial_guess = 0.2
                
                # Use Newton-Raphson to find root
                newton_iv = newton(objective, initial_guess, maxiter=100, tol=1e-6)
                if newton_iv and 0 < newton_iv <= 5.0:
                    logger.info(f"✅ [IV_CALC] {symbol} - IV via Newton-Raphson fallback: {newton_iv:.4f}")
                    self._store_iv_to_redis(symbol, newton_iv)
                    return newton_iv
            except ImportError:
                logger.debug(f"🔍 [IV_CALC] {symbol} - scipy not available for Newton-Raphson")
            except Exception as e:
                logger.debug(f"🔍 [IV_CALC] {symbol} - Newton-Raphson fallback failed: {e}")
            
            # ✅ FALLBACK 3: Simple approximation for ATM options
            try:
                approx_iv = self._calculate_iv_approximation(
                    option_price, underlying_price, strike_price, dte_years, option_type
                )
                if approx_iv and 0 < approx_iv <= 5.0:
                    logger.info(f"✅ [IV_CALC] {symbol} - IV via approximation fallback: {approx_iv:.4f}")
                    self._store_iv_to_redis(symbol, approx_iv)
                    return approx_iv
            except Exception as e:
                logger.debug(f"🔍 [IV_CALC] {symbol} - Approximation fallback failed: {e}")
            
            logger.warning(f"❌ [IV_CALC] {symbol} - All IV calculation methods failed")
            return None
                    
        except Exception as e:
            logger.warning(f"⚠️ [IV_CALC] {symbol} - Error in IV calculation: {e}")
            import traceback
            logger.debug(f"🔍 [IV_CALC] {symbol} - Full traceback: {traceback.format_exc()}")
            return None
    
    
    def _extract_strike_and_type_from_symbol(self, symbol: str) -> Tuple[float, str]:
        """Enhanced symbol parsing for strike and option type"""
        try:
            symbol_upper = symbol.upper().replace('NFO:', '').replace('NSE:', '')
            
            # Pattern for NIFTY/BANKNIFTY options: NIFTY25NOV25950CE
            pattern1 = r'([A-Z]+)(\d{2}[A-Z]{3})(\d+)(CE|PE)'
            match1 = re.search(pattern1, symbol_upper)
            if match1:
                strike = float(match1.group(3))
                option_type = 'call' if match1.group(4) == 'CE' else 'put'
                return strike, option_type
            
            # Pattern for stock options: RELIANCE25NOV2500CE  
            pattern2 = r'([A-Z]+[-A-Z]*)(\d{2}[A-Z]{3})(\d+)(CE|PE)'
            match2 = re.search(pattern2, symbol_upper)
            if match2:
                strike = float(match2.group(3))
                option_type = 'call' if match2.group(4) == 'CE' else 'put'
                return strike, option_type
            
            # Fallback: simple CE/PE detection
            if symbol_upper.endswith('CE'):
                strike_match = re.search(r'(\d+)(CE)$', symbol_upper)
                if strike_match:
                    return float(strike_match.group(1)), 'call'
            elif symbol_upper.endswith('PE'):
                strike_match = re.search(r'(\d+)(PE)$', symbol_upper)  
                if strike_match:
                    return float(strike_match.group(1)), 'put'
            
            logger.warning(f"⚠️ [SYMBOL_PARSE] Could not parse strike/type from: {symbol}")
            return 0.0, None if isinstance(None, tuple) else ('call', 0.0)  # Return None instead of defaulting to 'call'
            
        except Exception as e:
            logger.error(f"💥 [SYMBOL_PARSE] Error parsing symbol {symbol}: {e}")
            return 0.0, None if isinstance(None, tuple) else ('call', 0.0)  # Return None instead of defaulting to 'call'
    
    def _calculate_dte_from_symbol(self, symbol: str) -> float:
        """Calculate days to expiry from symbol"""
        try:
            symbol_upper = symbol.upper()
            
            # Extract expiry date from symbol (format: DDMMMYY)
            expiry_match = re.search(r'(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})', symbol_upper)
            if expiry_match:
                day = expiry_match.group(1)
                month = expiry_match.group(2)
                year = '20' + expiry_match.group(3)  # Convert YY to YYYY
                
                expiry_str = f"{day}-{month}-{year}"
                expiry_date = datetime.strptime(expiry_str, '%d-%b-%Y')
                
                # Calculate days to expiry
                today = datetime.now()
                days_to_expiry = (expiry_date - today).days
                
                # Convert to years
                dte_years = max(days_to_expiry / 365.0, 1/365.0)  # Minimum 1 day
                
                logger.info(f"📅 [DTE_CALC] {symbol} - Expiry: {expiry_date}, DTE: {days_to_expiry} days, {dte_years:.4f} years")
                return dte_years
            
            # Fallback: assume typical expiry (Thursday for NIFTY)
            logger.warning(f"⚠️ [DTE_CALC] Could not extract expiry from {symbol}, using default 7 days")
            return 7.0 / 365.0
            
        except Exception as e:
            logger.error(f"💥 [DTE_CALC] Error calculating DTE for {symbol}: {e}")
            return 7.0 / 365.0  # Default 1 week
    
    
    def _calculate_iv_approximation(self, option_price: float, underlying_price: float,
                                     strike_price: float, dte_years: float, option_type: str) -> Optional[float]:
        """
        Simple approximation for ATM options (fallback when py_vollib fails)
        """
        try:
            # Simple approximation: IV ≈ (option_price / underlying_price) / sqrt(2 * π * T)
            # This works best for ATM options
            if dte_years <= 0:
                return None
            
            moneyness = underlying_price / strike_price if strike_price > 0 else 1.0
            
            # Only use approximation for near-ATM options (0.9 < moneyness < 1.1)
            if 0.9 <= moneyness <= 1.1:
                from math import sqrt, pi
                approx_iv = (option_price / underlying_price) / sqrt(2 * pi * dte_years)
                # Cap at reasonable range
                return min(max(approx_iv, 0.01), 2.0)  # Between 1% and 200%
            
            return None
            
        except Exception as e:
            logger.debug(f"🔍 [IV_APPROX] Approximation calculation failed: {e}")
            return None
    
    def _store_iv_to_redis(self, symbol: str, iv_value: float, ttl: int = 180) -> None:
        """
        Store IV to Redis using UnifiedDataStorage (HSET on ind:{symbol} hash).
        
        Args:
            symbol: Option symbol
            iv_value: Calculated IV value
            ttl: Time to live in seconds (default: 180 seconds = 3 minutes)
        """
        try:
            # ✅ FIXED: Use UnifiedDataStorage.store_greeks() (HSET on ind:{symbol} hash)
            from shared_core.redis_clients.unified_data_storage import get_unified_storage
            storage = get_unified_storage()
            
            # Store both 'iv' and 'implied_volatility' as Greek fields
            greeks = {
                'iv': iv_value,
                'implied_volatility': iv_value
            }
            storage.store_greeks(symbol, greeks, ttl=ttl)
            logger.debug(f"✅ [IV_STORAGE] {symbol} - Stored IV={iv_value:.4f} to ind:{symbol} hash (TTL={ttl}s)")
        except Exception as store_err:
            logger.warning(f"⚠️ [IV_STORAGE] {symbol} - Failed to store IV to Redis: {store_err}")
    
    def _should_calculate_greeks(self, tick_data: Dict[str, Any], mapped_symbol: str) -> bool:
        """🛡️ Robust check for Greek calculation eligibility."""
        # ✅ ENHANCED: Validate option is not too deep OTM
        strike = tick_data.get("strike_price")
        underlying = tick_data.get("underlying_price")
        
        if strike and underlying:
            try:
                strike_float = float(strike)
                underlying_float = float(underlying)
                
                if underlying_float > 0:
                    moneyness = abs((underlying_float - strike_float) / underlying_float)
                    
                    # Skip if option is too deep OTM (>30% away)
                    if moneyness > 0.3:
                        logger.debug(f"⚠️ [GREEK_SKIP] {mapped_symbol} - Too deep OTM (moneyness={moneyness:.2%}, underlying={underlying_float}, strike={strike_float})")
                        return False
            except (ValueError, TypeError, ZeroDivisionError):
                # If we can't calculate moneyness, proceed with calculation
                pass
        
        # ✅ Additional validation: Check if we have required fields
        if not tick_data.get("strike_price") or not tick_data.get("underlying_price"):
            logger.debug(f"⚠️ [GREEK_SKIP] {mapped_symbol} - Missing strike_price or underlying_price")
            return False
        
        return True

    def _validate_greek_values(self, greeks: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        """✅ ENHANCED: Validate Greek values are realistic."""
        if not greeks:
            return greeks
        
        validated_greeks = {}
        for greek_name, value in greeks.items():
            if isinstance(value, float):
                # Check for NaN or infinity
                if math.isnan(value) or math.isinf(value):
                    validated_greeks[greek_name] = 0.0
                    logger.debug(f"⚠️ [GREEK_VALIDATION] {symbol} - {greek_name} is NaN/Inf, setting to 0.0")
                # Check for extremely small values (effectively zero)
                elif abs(value) < 1e-10:
                    validated_greeks[greek_name] = 0.0
                    if greek_name in ['gamma', 'vega']:  # These can legitimately be very small
                        logger.debug(f"⚠️ [GREEK_VALIDATION] {symbol} - {greek_name} is extremely small ({value}), setting to 0.0")
                # Check for unrealistic large values
                elif abs(value) > 1000:
                    validated_greeks[greek_name] = 0.0
                    logger.warning(f"⚠️ [GREEK_VALIDATION] {symbol} - {greek_name} is unrealistically large ({value}), setting to 0.0")
                else:
                    validated_greeks[greek_name] = value
            else:
                validated_greeks[greek_name] = value
        
        return validated_greeks

    def _calculate_greeks_for_last_tick(self, last_tick: Dict) -> Optional[Dict]:
        """
        ✅ OPTIMIZED: Calculate Greeks for last tick ONLY if not already present from websocket_parser.
        
        ✅ SINGLE SOURCE OF TRUTH: If websocket_parser already calculated Greeks, return them instead of recalculating.
        ✅ FIXED: Automatically extracts strike_price and option_type from symbol if missing.
        ✅ ENHANCED: Validates deep OTM options and unrealistic Greek values.
        ✅ FIXED: Automatically fetches underlying_price from Redis if missing for NIFTY/BANKNIFTY.
        """
        symbol = last_tick.get('symbol', 'UNKNOWN')
        try:
            # ✅ CHECK: If Greeks already exist from websocket_parser, return them
            existing_greeks = {
                'delta': last_tick.get('delta'),
                'gamma': last_tick.get('gamma'),
                'theta': last_tick.get('theta'),
                'vega': last_tick.get('vega'),
                'rho': last_tick.get('rho'),
                'iv': last_tick.get('iv') or last_tick.get('implied_volatility'),
            }
            
            # Check if all critical Greeks are present and valid (non-zero for delta/gamma/theta/vega)
            critical_greeks_present = (
                existing_greeks['delta'] is not None and existing_greeks['delta'] != 0.0 and
                existing_greeks['gamma'] is not None and
                existing_greeks['theta'] is not None and
                existing_greeks['vega'] is not None and existing_greeks['vega'] != 0.0 and
                existing_greeks['iv'] is not None and existing_greeks['iv'] > 0
            )
            
            if critical_greeks_present:
                logger.debug(f"✅ [GREEKS] {symbol} Using pre-computed Greeks from websocket_parser")
                normalized_delta = self._normalize_delta_sign(existing_greeks['delta'], last_tick.get('option_type'))
                return {
                    'delta': normalized_delta,
                    'gamma': existing_greeks['gamma'],
                    'theta': existing_greeks['theta'],
                    'vega': existing_greeks['vega'],
                    'rho': existing_greeks.get('rho', 0.0),
                    'iv': existing_greeks['iv'],
                    'implied_volatility': existing_greeks['iv'],
                    'dte_years': last_tick.get('dte_years', 0.0),
                    'trading_dte': last_tick.get('trading_dte', 0),
                    'expiry_series': last_tick.get('expiry_series', 'UNKNOWN'),
                    'option_price': last_tick.get('last_price', 0.0)
                }
            
            # Only calculate if missing - continue with calculation logic below
            
            # ✅ FIX: Use prefix (NFO, NSE, CDS, MCX) to identify asset class and skip Greeks for non-options
            symbol_upper = str(symbol).upper()
            
            # Identify asset class from prefix
            asset_class = None
            symbol_without_prefix = symbol_upper
            
            if symbol_upper.startswith('NSE:'):
                asset_class = 'EQUITY'
                symbol_without_prefix = symbol_upper[4:]
            elif symbol_upper.startswith('NFO:'):
                asset_class = 'FNO'  # F&O segment (options and futures)
                symbol_without_prefix = symbol_upper[4:]
            elif symbol_upper.startswith('CDS:'):
                asset_class = 'CDS'  # Currency derivatives
                symbol_without_prefix = symbol_upper[4:]
            elif symbol_upper.startswith('MCX:'):
                asset_class = 'MCX'  # Commodity derivatives
                symbol_without_prefix = symbol_upper[4:]
            else:
                # No prefix - try to infer from symbol pattern
                if symbol_upper.endswith('CE') or symbol_upper.endswith('PE'):
                    asset_class = 'FNO'  # Likely an option
                elif 'FUT' in symbol_upper and (symbol_upper.endswith('FUT') or symbol_upper.endswith('FUTURE')):
                    asset_class = 'FNO'  # Likely a future
                else:
                    asset_class = 'EQUITY'  # Default to equity if no clear indicator
            
            # ✅ CRITICAL FIX: Use UniversalSymbolParser from shared_core for consistent symbol parsing
            is_option = self._is_option_symbol(symbol)
            is_future = self._is_future_symbol(symbol)
            
            logger.info(f"🔍 [GREEKS_DEEP_DEBUG] {symbol} - Symbol analysis: symbol_without_prefix='{symbol_without_prefix}', is_option={is_option}, is_future={is_future}, asset_class={asset_class}")
            
            if not is_option:
                # Skip Greeks for futures, equities, and other non-option instruments
                if is_future:
                    logger.info(f"🔍 [GREEKS_DEBUG] {symbol} Skipping Greeks calculation for future contract (asset_class={asset_class}, futures don't have Greeks)")
                elif asset_class == 'EQUITY':
                    logger.info(f"🔍 [GREEKS_DEBUG] {symbol} Skipping Greeks calculation for equity stock (asset_class={asset_class}, equities don't have Greeks)")
                else:
                    logger.info(f"🔍 [GREEKS_DEBUG] {symbol} Skipping Greeks calculation for non-option instrument (asset_class={asset_class})")
                
                return {
                    'delta': 0.0,
                    'gamma': 0.0,
                    'theta': 0.0,
                    'vega': 0.0,
                    'rho': 0.0,
                    'dte_years': 0.0,
                    'trading_dte': 0,
                    'expiry_series': asset_class or 'UNKNOWN',
                    'option_price': last_tick.get('last_price', 0.0)
                }
            
            logger.info(f"🔍 [GREEKS_DEBUG] {symbol} Starting Greeks calculation - Input fields: {list(last_tick.keys())[:15]}")
            logger.info(f"🔍 [GREEKS_DEBUG] {symbol} Input values: underlying_price={last_tick.get('underlying_price')}, strike_price={last_tick.get('strike_price')}, option_type={last_tick.get('option_type')}")
            
            # ✅ FIX 1: Extract strike_price and option_type from symbol if missing
            strike_price = last_tick.get('strike_price') or last_tick.get('strike', 0)
            option_type = last_tick.get('option_type') or last_tick.get('option_type_raw')
            
            if not strike_price or not option_type:
                # ✅ FIXED: Use centralized parsing method instead of duplicate inline logic
                parsed_strike, parsed_type = self._extract_strike_and_type_from_symbol(symbol)
                if parsed_strike and parsed_strike > 0:
                    if not strike_price:
                        strike_price = parsed_strike
                        last_tick['strike_price'] = strike_price
                        logger.info(f"✅ [GREEKS_DEBUG] {symbol} Extracted strike_price={strike_price} via centralized parser")
                    if not option_type:
                        option_type = parsed_type
                        last_tick['option_type'] = option_type
                        logger.info(f"✅ [GREEKS_DEBUG] {symbol} Extracted option_type={option_type} via centralized parser")
                else:
                    logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} Centralized parser failed to extract strike/type from symbol")
            
            # ✅ FIX 2: Check both field mapping names for underlying_price
            underlying_price = last_tick.get('underlying_price') or last_tick.get('underlying') or last_tick.get('spot_price', 0)
            if isinstance(underlying_price, str):
                try:
                    underlying_price = float(underlying_price)
                except (ValueError, TypeError):
                    underlying_price = 0
            
            logger.info(f"🔍 [GREEKS_DEBUG] {symbol} - Before Redis lookup: underlying_price={underlying_price}")
            
            if not underlying_price or underlying_price <= 0:
                if symbol:
                    # Extract underlying symbol (e.g., 'NIFTY' from 'NFO:NIFTY25JAN24500CE')
                    underlying_symbol = extract_underlying_from_symbol(symbol)
                    logger.info(f"🔍 [GREEKS_DEBUG] {symbol} Extracted underlying_symbol: {underlying_symbol}")
                    if underlying_symbol:
                        # ✅ ENHANCED: Check if redis_client is available before fetching
                        if not hasattr(self, 'redis_client') or not self.redis_client:
                            logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} No redis_client available, trying direct RedisClientFactory connection")
                            # Try to get client directly from RedisClientFactory
                            try:
                                # ✅ FIXED: Use RedisClientFactory for centralized Redis access
                                from shared_core.redis_clients.redis_client import RedisClientFactory
                                temp_client = RedisClientFactory.get_trading_client()  # DB 1
                                if temp_client:
                                    # Temporarily set redis_client for this fetch
                                    original_client = getattr(self, 'redis_client', None)
                                    self.redis_client = type('obj', (object,), {
                                        'retrieve_by_data_type': lambda *args, **kwargs: None,
                                        'store_by_data_type': lambda *args, **kwargs: None,
                                        'get_client': lambda db: temp_client if db == 1 else None
                                    })()
                                    fetched_price = self._get_underlying_price_from_redis(underlying_symbol, symbol)
                                    self.redis_client = original_client
                                    if fetched_price > 0:
                                        underlying_price = fetched_price
                                        last_tick['underlying_price'] = underlying_price
                                        last_tick['underlying'] = underlying_price
                                        logger.info(f"✅ [GREEKS_DEBUG] {symbol} Set underlying_price via direct RedisClientFactory: {underlying_price}")
                                    else:
                                        logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} Could not fetch underlying_price via direct RedisClientFactory for {underlying_symbol} (index data may not be available yet)")
                            except Exception as e:
                                logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} Failed to get RedisClientFactory client: {e}")
                        else:
                            # ⚠️ FALLBACK: Fetch underlying price from Redis (should come from tick_data)
                            fetched_price = self._get_underlying_price_from_redis(underlying_symbol, symbol)
                            logger.info(f"🔍 [GREEKS_DEBUG] {symbol} Fetched underlying_price from Redis: {fetched_price}")
                            if fetched_price > 0:
                                underlying_price = fetched_price
                                # Set both field names for compatibility
                                last_tick['underlying_price'] = underlying_price
                                last_tick['underlying'] = underlying_price  # Field mapping name
                                logger.info(f"✅ [GREEKS_DEBUG] {symbol} Set underlying_price: {underlying_price} (from {underlying_symbol})")
                            else:
                                logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} Could not fetch underlying_price from Redis for {underlying_symbol} (index data may not be available yet)")
                    else:
                        logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} Could not extract underlying_symbol from symbol")
                else:
                    logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} No symbol in last_tick, cannot fetch underlying_price")
            
            # ✅ FIX 3: Check expiry_date (field mapping says 'expiry' but greek_calculator expects 'expiry_date')
            expiry_date = last_tick.get('expiry_date') or last_tick.get('expiry')
            if expiry_date and not isinstance(expiry_date, datetime):
                # Try to parse if it's a string
                try:
                    if isinstance(expiry_date, str):
                        from dateutil.parser import parse as date_parse
                        expiry_date = date_parse(expiry_date)
                    last_tick['expiry_date'] = expiry_date
                except Exception as e:
                    logger.debug(f"🔍 [GREEKS_DEBUG] {symbol} Could not parse expiry_date: {e}")
                    expiry_date = None
            
            # Ensure all required fields are set with correct names for greek_calculator
            last_tick['underlying_price'] = underlying_price  # greek_calculator expects this
            last_tick['strike_price'] = strike_price  # Ensure it's set
            last_tick['option_type'] = option_type  # Ensure it's set ('call' or 'put')
            if expiry_date:
                last_tick['expiry_date'] = expiry_date
            
            # ✅ FIX: Futures don't have strike_price or option_type - skip Greek calculation for them
            symbol_upper = symbol.upper()
            is_future = 'FUT' in symbol_upper and (symbol_upper.endswith('FUT') or symbol_upper.endswith('FUTURE'))
            if is_future:
                # Futures don't have Greeks - return early with delta=1.0
                logger.debug(f"🔍 [GREEKS_DEBUG] {symbol} - Skipping Greeks for future contract (futures don't have strike_price/option_type)")
                return {
                    'delta': 1.0,  # Futures have delta of 1.0
                    'gamma': 0.0,
                    'theta': 0.0,
                    'vega': 0.0,
                    'rho': 0.0,
                    'dte_years': 0.0,
                    'trading_dte': 0,
                    'expiry_series': 'UNKNOWN',
                    'option_price': 0.0
                }
            
            # Check if we have required fields
            required_fields = ['underlying_price', 'strike_price', 'option_type']
            missing_fields = []
            if not underlying_price or underlying_price <= 0:
                missing_fields.append('underlying_price')
            if not strike_price or strike_price <= 0:
                missing_fields.append('strike_price')
            if not option_type:
                missing_fields.append('option_type')
            
            logger.info(f"🔍 [GREEKS_DEBUG] {symbol} - Final parameters: underlying_price={underlying_price}, strike_price={strike_price}, option_type={option_type}, expiry_date={expiry_date}")
            
            if missing_fields:
                logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} Missing required fields for Greeks: {missing_fields}")
                logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} Current values: underlying_price={underlying_price}, strike_price={strike_price}, option_type={option_type}, expiry_date={expiry_date}")
                logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} last_tick keys: {list(last_tick.keys())[:20]}")
                # ✅ FIX: Return zero Greeks instead of None so they're always present
                return {
                    'delta': 0.0,
                    'gamma': 0.0,
                    'theta': 0.0,
                    'vega': 0.0,
                    'rho': 0.0,
                    'dte_years': 0.0,
                    'trading_dte': 0,
                    'expiry_series': 'UNKNOWN',
                    'option_price': 0.0
                }
            
            # ✅ ENHANCED: Check if we should calculate Greeks (skip deep OTM options)
            # This check happens after we've extracted/fetched underlying_price and strike_price
            if not self._should_calculate_greeks(last_tick, symbol):
                logger.debug(f"⚠️ [GREEKS_DEBUG] {symbol} - Skipping Greeks calculation (deep OTM or missing data)")
                return {
                    'delta': 0.0,
                    'gamma': 0.0,
                    'theta': 0.0,
                    'vega': 0.0,
                    'rho': 0.0,
                    'dte_years': 0.0,
                    'trading_dte': 0,
                    'expiry_series': 'UNKNOWN',
                    'option_price': 0.0,
                    'iv': 0.0,
                    'implied_volatility': 0.0
                }
            
            # ✅ FIX: Extract IV (implied volatility) from tick data, or calculate from option price
            # ✅ CRITICAL FIX: Check if IV was pre-calculated in main flow to avoid redundant calculation
            iv_precalculated = last_tick.get('_iv_precalculated', False)
            iv = last_tick.get('iv') or last_tick.get('implied_volatility') or last_tick.get('volatility')
            
            if iv:
                try:
                    iv = float(iv)
                    # ✅ FIXED: Accept 0.2 as valid (it's the fallback default, not an error)
                    if iv <= 0 or iv > 5.0:  # Sanity check: IV should be between 0 and 500% (5.0)
                        logger.debug(f"🔍 [GREEKS_DEBUG] {symbol} IV out of range ({iv}), will recalculate from option price")
                        iv = None  # Will calculate below
                    elif iv_precalculated:
                        # IV was pre-calculated in main flow, use it directly
                        logger.debug(f"✅ [GREEKS_DEBUG] {symbol} Using pre-calculated IV: {iv:.4f} (from main flow)")
                except (ValueError, TypeError):
                    logger.debug(f"🔍 [GREEKS_DEBUG] {symbol} Could not parse IV ({iv}), will calculate from option price")
                    iv = None
            
            # If IV not in tick data AND not pre-calculated, calculate it from option price using vollib
            if not iv or iv is None:
                if iv_precalculated:
                    # This should not happen - if pre-calculated, IV should be valid
                    logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} IV marked as pre-calculated but value is invalid, recalculating...")
                calculated_iv = self._calculate_iv_from_option_price(last_tick, symbol)
                if calculated_iv:
                    iv = calculated_iv
                    logger.info(f"✅ [GREEKS_DEBUG] {symbol} - IV calculated from option price using vollib: {iv:.4f}")
                else:
                    # ✅ NEW: Fallback to historical IV from historical ticks
                    if self.historical_queries:
                        try:
                            import time
                            end_time = int(time.time() * 1000)
                            start_time = end_time - (60 * 60 * 1000)  # Last 1 hour
                            
                            historical_ticks = self.historical_queries.get_historical_ticks(
                                symbol, start_time, end_time, max_ticks=100
                            )
                            
                            # Try to find IV in historical ticks
                            for tick in reversed(historical_ticks):  # Most recent first
                                hist_iv = tick.get('iv') or tick.get('implied_volatility') or tick.get('volatility')
                                if hist_iv:
                                    try:
                                        hist_iv_float = float(hist_iv)
                                        if 0 < hist_iv_float <= 5.0:
                                            iv = hist_iv_float
                                            logger.debug(f"✅ [GREEKS_DEBUG] {symbol} - IV from historical ticks: {iv:.4f}")
                                            break
                                    except (ValueError, TypeError):
                                        continue
                        except Exception as e:
                            logger.debug(f"🔍 [GREEKS_DEBUG] {symbol} Historical IV fallback failed: {e}")
                    
                    if not iv or iv is None:
                        iv = 0.2  # Fallback default if calculation fails
                        logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} - Could not calculate IV from option price or historical ticks, using default 0.2")
            
            # Extract risk-free rate if available
            risk_free_rate = last_tick.get('risk_free_rate', 0.05)
            if isinstance(risk_free_rate, str):
                try:
                    risk_free_rate = float(risk_free_rate)
                except (ValueError, TypeError):
                    risk_free_rate = 0.05
            
            dte_years = last_tick.get('dte_years', 0.0)
            expiry_date = last_tick.get('expiry_date', None)
            option_type = last_tick.get('option_type', None)
            strike_price = last_tick.get('strike_price', 0.0)
            underlying_price = last_tick.get('underlying_price', 0.0)
            iv = last_tick.get('iv', 0.0)
            risk_free_rate = last_tick.get('risk_free_rate', 0.05)
            
            logger.info(f"🔍 [GREEKS_DEBUG] {symbol} Calling calculate_greeks_for_tick_data with IV={iv}, risk_free_rate={risk_free_rate}")
            logger.info(f"🔍 [GREEKS_DEBUG] {symbol} Input to calculate_greeks_for_tick_data: underlying_price={underlying_price}, strike_price={strike_price}, option_type={option_type}, dte_years={dte_years}, expiry_date={expiry_date}")
            greeks = self.calculate_greeks_for_tick_data(last_tick, risk_free_rate=risk_free_rate, volatility=iv)
            
            if greeks:
                logger.info(f"✅ [GREEKS_DEBUG] {symbol} Greeks calculator returned: {list(greeks.keys())}")
                logger.info(f"✅ [GREEKS_DEBUG] {symbol} Greeks values: delta={greeks.get('delta')}, gamma={greeks.get('gamma')}, theta={greeks.get('theta')}, vega={greeks.get('vega')}, rho={greeks.get('rho')}")
                
                # ✅ ENHANCED: Validate Greek values are realistic
                greeks = self._validate_greek_values(greeks, symbol)
                normalized_delta = self._normalize_delta_sign(greeks.get('delta'), last_tick.get('option_type'))
                
                # ✅ FIX: Always return Greeks, even if they're 0 - patterns need them to be present
                returned_greeks = {
                    'delta': normalized_delta if normalized_delta is not None else greeks.get('delta', 0.0),
                    'gamma': greeks.get('gamma', 0.0),
                    'theta': greeks.get('theta', 0.0),
                    'vega': greeks.get('vega', 0.0),
                    'rho': greeks.get('rho', 0.0),
                    'dte_years': greeks.get('dte_years', 0.0),
                    'trading_dte': greeks.get('trading_dte', 0),
                    'expiry_series': greeks.get('expiry_series', 'UNKNOWN'),
                    'option_price': greeks.get('option_price', 0.0),
                    'iv': greeks.get('iv', 0.2),
                    'implied_volatility': greeks.get('implied_volatility', 0.2)
                }
                # ✅ CRITICAL: Check if all Greeks are zero - this indicates calculation failure
                if all(v == 0.0 for k, v in returned_greeks.items() if k in ['delta', 'gamma', 'theta', 'vega', 'rho']):
                    logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} ⚠️ ALL GREEKS ARE ZERO - This indicates calculation failure! Check inputs: underlying={underlying_price}, strike={strike_price}, IV={iv}, dte={dte_years}")
                return returned_greeks
            else:
                logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} greek_calculator returned None or empty - returning zero Greeks. Check inputs: underlying={underlying_price}, strike={strike_price}, IV={iv}, dte={dte_years}")
                # ✅ FIX: Return zero Greeks instead of None so they're always present
                return {
                    'delta': 0.0,
                    'gamma': 0.0,
                    'theta': 0.0,
                    'vega': 0.0,
                    'rho': 0.0,
                    'dte_years': 0.0,
                    'trading_dte': 0,
                    'expiry_series': 'UNKNOWN',
                    'option_price': 0.0,
                    # ✅ FIX: Include IV fields
                    'iv': 0.2,
                    'implied_volatility': 0.2
                }
        except Exception as e:
            logger.error(f"❌ [GREEKS_DEBUG] {symbol} Greek calculation failed: {e}")
            import traceback
            logger.debug(f"🔍 [GREEKS_DEBUG] {symbol} Traceback: {traceback.format_exc()}")
            # ✅ FIX: Return zero Greeks on error instead of None
            return {
                'delta': 0.0,
                'gamma': 0.0,
                'theta': 0.0,
                'vega': 0.0,
                'rho': 0.0,
                'dte_years': 0.0,
                'trading_dte': 0,
                'expiry_series': 'UNKNOWN',
                'option_price': 0.0
            }
        
    def _calculate_volume_profile_optimized(self, prices: List[float], volumes: List[float], 
                                          tick_count: int = None, price_bins: int = 20, symbol: str = None) -> Dict:
        """✅ OPTIMIZED: Calculate volume profile from arrays instead of full tick data
        
        This method uses extracted price and volume arrays for better performance,
        avoiding the need to pass full tick_data to volume_profile.
        
        Args:
            prices: List of price values
            volumes: List of volume values (bucket_incremental_volume)
            tick_count: Optional tick count (for compatibility)
            price_bins: Number of price bins for volume profile (default: 20)
            symbol: Optional symbol for POC lookup
        
        Returns:
            Dict with volume profile data matching calculate_volume_profile format:
            {'volume': total_volume, 'price': avg_price, 'bins': [price_levels], 'volumes': [volumes], 'poc_price': poc_price, 'poc_volume': poc_volume, 'symbol': symbol}
        """
        # ✅ CENTRALIZED: Use canonical_symbol for symbol normalization (section 3)
        from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
        
        if not prices or not volumes or len(prices) != len(volumes):
            # Fallback to empty result if arrays are invalid
            return {
                'volume': 0.0,
                'price': 0.0,
                'bins': [],
                'volumes': [],
                'poc_price': 0.0,
                'poc_volume': 0.0,
                'symbol': RedisKeyStandards.canonical_symbol(symbol) if symbol else ''
            }
        
        if not POLARS_AVAILABLE:
            # Fallback to simple calculation
            return self._calculate_volume_profile_simple(prices, volumes, price_bins, symbol)
        
        try:
            # ✅ OPTIMIZED: Use Polars for efficient binning and aggregation
            df = pl.DataFrame({
                'last_price': prices,
                'bucket_incremental_volume': volumes
            })
            
            # Filter out invalid values
            df = df.filter(
                (pl.col('last_price').is_not_nan()) & 
                (pl.col('last_price').is_not_null()) &
                (pl.col('bucket_incremental_volume') > 0)
            )
            
            if df.is_empty():
                # ✅ CENTRALIZED: Use canonical_symbol for symbol normalization (section 3)
                from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
                return {
                    'volume': 0.0,
                    'price': 0.0,
                    'bins': [],
                    'volumes': [],
                    'poc_price': 0.0,
                    'poc_volume': 0.0,
                    'symbol': RedisKeyStandards.canonical_symbol(symbol) if symbol else ''
                }
            
            # Get price range
            min_price = df['last_price'].min()
            max_price = df['last_price'].max()
            total_volume = float(df['bucket_incremental_volume'].sum())
            avg_price = float(df['last_price'].mean())
            
            if min_price == max_price:
                # Single price level
                bins = [float(min_price)]
                volumes_list = [total_volume]
            else:
                # Create bins and aggregate volumes
                bin_size = (max_price - min_price) / price_bins
                
                volume_profile = df.with_columns([
                    ((pl.col('last_price') - min_price) / bin_size).floor().cast(pl.Int64).alias('bin')
                ]).group_by('bin').agg([
                    pl.col('bucket_incremental_volume').sum().alias('total_volume'),
                    pl.col('last_price').mean().alias('avg_price')
                ]).sort('bin')
                
                bins = volume_profile['avg_price'].to_list()
                volumes_list = volume_profile['total_volume'].to_list()
            
            # ✅ Get POC data from VolumeProfileManager if available
            poc_price = 0.0
            poc_volume = 0.0
            # ✅ CENTRALIZED: Use canonical_symbol for symbol normalization (section 3)
            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol) if symbol else ''
            
            try:
                if symbol and hasattr(self, 'redis_client') and self.redis_client:
                    from shared_core.volume_files.volume_computation_manager import get_volume_computation_manager
                    volume_manager = get_volume_computation_manager(redis_client=self.redis_client)
                    if volume_manager and volume_manager.volume_profile_manager:
                        volume_nodes = volume_manager.get_volume_profile_data(canonical_symbol)
                        if volume_nodes:
                            poc_price = float(volume_nodes.get('poc_price', 0))
                            poc_volume = float(volume_nodes.get('poc_volume', 0))
            except Exception as e:
                logger.debug(f"Could not get POC from VolumeProfileManager for {canonical_symbol}: {e}")
            
            return {
                'volume': total_volume,
                'price': avg_price,
                'bins': bins,
                'volumes': volumes_list,
                'poc_price': poc_price,
                'poc_volume': poc_volume,
                'symbol': canonical_symbol
            }
            
        except Exception as e:
            logger.debug(f"Optimized volume profile calculation failed, using simple fallback: {e}")
            return self._calculate_volume_profile_simple(prices, volumes, price_bins, symbol)
    
    def _calculate_volume_profile_simple(self, prices: List[float], volumes: List[float], 
                                        price_bins: int = 20, symbol: str = None) -> Dict:
        """✅ FALLBACK: Simple volume profile calculation without Polars"""
        # ✅ CENTRALIZED: Use canonical_symbol for symbol normalization (section 3)
        from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
        
        if not prices or not volumes or len(prices) != len(volumes):
            return {
                'volume': 0.0,
                'price': 0.0,
                'bins': [],
                'volumes': [],
                'poc_price': 0.0,
                'poc_volume': 0.0,
                'symbol': RedisKeyStandards.canonical_symbol(symbol) if symbol else ''
            }
        
        # Aggregate volume by price level
        price_volume_dict = {}
        total_volume = 0.0
        for price, volume in zip(prices, volumes):
            if price > 0 and volume > 0:
                # Round to 2 decimal places for binning
                rounded_price = round(price, 2)
                price_volume_dict[rounded_price] = price_volume_dict.get(rounded_price, 0) + volume
                total_volume += volume
        
        if not price_volume_dict:
            return {
                'volume': 0.0,
                'price': 0.0,
                'bins': [],
                'volumes': [],
                'poc_price': 0.0,
                'poc_volume': 0.0,
                'symbol': RedisKeyStandards.canonical_symbol(symbol) if symbol else ''
            }
        
        # Sort by price and create bins/volumes lists
        sorted_prices = sorted(price_volume_dict.keys())
        bins = sorted_prices
        volumes_list = [price_volume_dict[price] for price in sorted_prices]
        avg_price = sum(prices) / len(prices) if prices else 0.0
        
        # Get POC if available
        poc_price = 0.0
        poc_volume = 0.0
        # ✅ CENTRALIZED: Use canonical_symbol for symbol normalization (section 3)
        from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol) if symbol else ''
        
        try:
            if symbol and hasattr(self, 'redis_client') and self.redis_client:
                from shared_core.volume_files.volume_computation_manager import get_volume_computation_manager
                volume_manager = get_volume_computation_manager(redis_client=self.redis_client)
                if volume_manager and volume_manager.volume_profile_manager:
                    volume_nodes = volume_manager.get_volume_profile_data(canonical_symbol)
                    if volume_nodes:
                        poc_price = float(volume_nodes.get('poc_price', 0))
                        poc_volume = float(volume_nodes.get('poc_volume', 0))
        except Exception as e:
            logger.debug(f"Could not get POC from VolumeProfileManager for {canonical_symbol}: {e}")
        
        return {
            'volume': total_volume,
            'price': avg_price,
            'bins': bins,
            'volumes': volumes_list,
            'poc_price': poc_price,
            'poc_volume': poc_volume,
            'symbol': canonical_symbol
        }
    
    def _fallback_atr(self, highs: List[float], lows: List[float], closes: List[float], period: int) -> float:
        """Fallback ATR calculation when Polars is not available"""
        if len(highs) < period + 1:
            return 0.0
        
        tr_values = []
        for i in range(1, len(highs)):
            tr1 = highs[i] - lows[i]
            tr2 = abs(highs[i] - closes[i-1])
            tr3 = abs(lows[i] - closes[i-1])
            tr_values.append(max(tr1, tr2, tr3))
        
        return sum(tr_values[-period:]) / period if tr_values else 0.0
    
    def _fallback_ema(self, prices: List[float], period: int) -> float:
        """Fallback EMA calculation when Polars is not available"""
        if len(prices) < period:
            return float(prices[-1]) if prices else 0.0
        
        alpha = 2.0 / (period + 1)
        ema = prices[0]
        for last_price in prices[1:]:
            ema = alpha * last_price + (1 - alpha) * ema
        
        return float(ema)
    
    def _fallback_rsi(self, prices: List[float], period: int) -> float:
        """Fallback RSI calculation when Polars is not available"""
        if len(prices) < period + 1:
            return 50.0
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(-change)
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return float(rsi)
    
    def _fallback_vwap(self, tick_data: List[Dict]) -> float:
        """Fallback VWAP calculation when Polars is not available"""
        if not tick_data:
            return 0.0
        
        total_volume = 0
        total_price_volume = 0
        
        for tick in tick_data:
            last_price = tick.get('last_price', 0)
            # Try different volume fields in order of preference
            volume = (tick.get('zerodha_cumulative_volume', 0) or 
                     tick.get('bucket_incremental_volume', 0) or 
                     tick.get('bucket_cumulative_volume', 0) or 0)
            
            if volume > 0 and last_price > 0:
                total_volume += volume
                total_price_volume += last_price * volume
        
        if total_volume > 0:
            return total_price_volume / total_volume
        else:
            # Fallback: use simple average if no volume data
            prices = [tick.get('last_price', 0) for tick in tick_data if tick.get('last_price', 0) > 0]
            return sum(prices) / len(prices) if prices else 0.0
    
    def _fallback_volume_profile(self, ticks: List[Dict], price_bins: int) -> Dict:
        """Fallback bucket_incremental_volume profile calculation when Polars is not available"""
        if not ticks:
            return {}
        
        prices = [tick.get('last_price', 0) for tick in ticks]
        volumes = [tick.get('bucket_incremental_volume', 0) for tick in ticks]
        
        min_price, max_price = min(prices), max(prices)
        bin_size = (max_price - min_price) / price_bins if max_price != min_price else 1.0
        
        bins = {}
        for last_price, bucket_incremental_volume in zip(prices, volumes):
            if bin_size == 1.0:  # All prices are the same
                bin_idx = 0
            else:
                bin_idx = int((last_price - min_price) / bin_size)
            if bin_idx not in bins:
                bins[bin_idx] = {'total_volume': 0, 'avg_price': 0, 'count': 0}
            bins[bin_idx]['total_volume'] += bucket_incremental_volume
            bins[bin_idx]['avg_price'] += last_price
            bins[bin_idx]['count'] += 1
        
        # Calculate averages
        for bin_idx in bins:
            bins[bin_idx]['avg_price'] /= bins[bin_idx]['count']
        
        return {
            'bins': [bins[i]['avg_price'] for i in sorted(bins.keys())],
            'volumes': [bins[i]['total_volume'] for i in sorted(bins.keys())]
        }
    
    def _fallback_macd(self, prices: List[float], fast_period: int, slow_period: int, signal_period: int) -> Dict:
        """Fallback MACD calculation when Polars is not available"""
        if len(prices) < slow_period:
            return {'macd': 0.0, 'signal': 0.0, 'histogram': 0.0}
        
        ema_fast = self._fallback_ema(prices, fast_period)
        ema_slow = self._fallback_ema(prices, slow_period)
        macd_line = ema_fast - ema_slow
        
        # Simplified signal line calculation
        signal_line = macd_line * 0.9  # Approximation
        
        return {
            'macd': macd_line,
            'signal': signal_line,
            'histogram': macd_line - signal_line
        }
    
    def _fallback_bollinger_bands(self, prices: List[float], period: int, std_dev: float) -> Dict:
        """Fallback Bollinger Bands calculation when Polars is not available"""
        if len(prices) < period:
            return {'upper': 0.0, 'middle': 0.0, 'lower': 0.0}
        
        recent_prices = prices[-period:]
        middle_band = sum(recent_prices) / len(recent_prices)
        
        # Calculate standard deviation
        variance = sum((p - middle_band) ** 2 for p in recent_prices) / len(recent_prices)
        std = variance ** 0.5
        
        upper_band = middle_band + (std * std_dev)
        lower_band = middle_band - (std * std_dev)
        
        return {
            'upper': upper_band,
            'middle': middle_band,
            'lower': lower_band
        }
    
    
    def _fallback_batch_calculate(self, tick_data: List[Dict], symbol: str) -> Dict:
        """Fallback batch calculation when Polars is not available"""
        if not tick_data:
            return {}
        
        # ✅ REMOVED: _parse_and_flatten_ohlc() - OHLC already flattened by parser
        # Data is already in correct format from parser
        prices = [tick.get('last_price', 0) if isinstance(tick, dict) else 0 for tick in tick_data]
        volumes = [tick.get('bucket_incremental_volume', 0) for tick in parsed_tick_data]
        highs = [tick.get('high', 0) for tick in parsed_tick_data]
        lows = [tick.get('low', 0) for tick in parsed_tick_data]
        
        # Get real last_price from Redis if available
        last_price = prices[-1] if prices else 0
        redis_client = getattr(self, 'redis_client', None)
        if redis_client and hasattr(redis_client, 'get_last_price') and symbol:
            redis_price = redis_client.get_last_price(symbol)
            if redis_price and redis_price > 0:
                last_price = redis_price
        
        fallback_payload = {
            'symbol': symbol,  # ✅ FIXED: Include symbol in fallback calculations
            'last_price': last_price,  # ✅ FIXED: Use Redis-fetched last_price
            'timestamp': tick_data[-1].get('exchange_timestamp_ms', tick_data[-1].get('timestamp', 0)) if tick_data else 0,
            # EMA calculations - comprehensive coverage
            'ema_5': self._fallback_ema(prices, 5),
            'ema_10': self._fallback_ema(prices, 10),
            'ema_20': self._fallback_ema(prices, 20),
            'ema_50': self._fallback_ema(prices, 50),
            'ema_100': self._fallback_ema(prices, 100),
            'ema_200': self._fallback_ema(prices, 200),
            'ema_crossover': {
                'ema_5': self._fallback_ema(prices, 5),
                'ema_10': self._fallback_ema(prices, 10),
                'ema_20': self._fallback_ema(prices, 20),
                'ema_50': self._fallback_ema(prices, 50),
                'ema_100': self._fallback_ema(prices, 100),
                'ema_200': self._fallback_ema(prices, 200)
            },
            
            # Other technical indicators
            'rsi': self._fallback_rsi(prices, 14),
            'atr': self._fallback_atr(highs, lows, prices, 14),
            'vwap': self._fallback_vwap(tick_data),
            'macd': self._fallback_macd(prices, 12, 26, 9),
            'bollinger_bands': self._fallback_bollinger_bands(prices, 20, 2.0),
            'volume_profile': self._fallback_volume_profile(tick_data, 20),
            # ✅ SINGLE SOURCE OF TRUTH: Read pre-computed volume_ratio from tick_data
            # NEVER calculate - volume_ratio is set by websocket_parser (Volume Manager)
            'volume_ratio': self._get_volume_ratio(tick_data[-1] if tick_data else {}, symbol),
            'price_change': self.calculate_price_change(prices[-1] if prices else 0, 
                                                      prices[-2] if len(prices) > 1 else 0)
        }
        cross_metrics = self._calculate_cross_asset_metrics(symbol, last_price)
        if cross_metrics:
            fallback_payload.update(cross_metrics)
        return fallback_payload
    
    def clear_cache(self):
        """Clear calculation cache"""
        # ✅ ENHANCED: Polars cache has its own clear method
        if hasattr(self._polars_cache, 'clear'):
            self._polars_cache.clear()
        else:
            # Fallback for old cache format
            self._polars_cache.clear() if isinstance(self._polars_cache, dict) else None
        self._cache.clear()  # ✅ SIMPLIFIED: Clear unified cache
        self._symbol_sessions.clear()
        self._fresh_calculations.clear()
    
    def get_rolling_window_stats(self) -> Dict:
        """Get statistics about session-based windows (in-memory Polars DataFrames)"""
        return {
            'total_symbols': len(self._symbol_sessions),
            'window_size': self._session_window_size,
            'symbols_with_data': len([s for s, w in self._symbol_sessions.items() if w.height > 0]),
            'average_window_length': sum(w.height for w in self._symbol_sessions.values()) / max(len(self._symbol_sessions), 1),
            'memory_usage_mb': sum(w.estimated_size() for w in self._symbol_sessions.values()) / (1024 * 1024) if POLARS_AVAILABLE else 0
        }
    
    def cleanup_old_windows(self, max_age_minutes: int = 30):
        """Clean up old session-based windows to prevent memory bloat"""
        import time
        current_time = time.time()
        cutoff_time = current_time - (max_age_minutes * 60)
        
        symbols_to_remove = []
        for symbol, window in self._symbol_sessions.items():
            if window.height > 0:
                # Get the latest timestamp from the window
                latest_timestamp = window['timestamp'].max()
                # Convert from milliseconds to seconds if needed
                if latest_timestamp > 1e10:  # Likely milliseconds
                    latest_timestamp = latest_timestamp / 1000
                
                if latest_timestamp < cutoff_time:
                    symbols_to_remove.append(symbol)
        
        for symbol in symbols_to_remove:
            del self._symbol_sessions[symbol]
        
        if symbols_to_remove:
            logger.info(f"Cleaned up {len(symbols_to_remove)} old session-based windows")
        
        return len(symbols_to_remove)
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        return {
            'polars_cache_size': self._polars_cache.size() if hasattr(self._polars_cache, 'size') else len(self._polars_cache),
            'indicator_cache_size': len(self._cache),
            'fresh_calculations_count': len(self._fresh_calculations),
            'polars_available': POLARS_AVAILABLE,
            'pandas_available': PANDAS_AVAILABLE,
            'numpy_available': NUMPY_AVAILABLE
        }
    
    def _enforce_cache_bounds(self):
        """Enforce cache size bounds using LRU eviction"""
        # ✅ ENHANCED: Polars cache handles its own bounds via PolarsCache class
        # Cleanup expired entries periodically
        if hasattr(self._polars_cache, 'cleanup_expired'):
            self._polars_cache.cleanup_expired()
        
        # Enforce bounds on unified indicator cache
        if len(self._cache) > self.max_cache_size:
            # Remove oldest entries (simple LRU based on timestamp)
            excess = len(self._cache) - self.max_cache_size
            # Sort by timestamp and remove oldest
            sorted_keys = sorted(self._cache.keys(), 
                               key=lambda k: self._cache[k].get('timestamp', 0))
            keys_to_remove = sorted_keys[:excess]
            for key in keys_to_remove:
                del self._cache[key]
    
    def batch_process_symbols(self, symbol_data: Dict[str, List[Dict]], max_ticks_per_symbol: int = 50) -> Dict[str, Dict]:
        """
        ✅ SIMPLIFIED CACHING: Process symbols with intelligent caching.
        Only recalculates when cache expired or data changed significantly.
        
        Args:
            symbol_data: Dict mapping symbol -> list of tick data
            max_ticks_per_symbol: Maximum ticks per symbol to prevent memory bloat
            
        Returns:
            Dict mapping symbol -> calculated indicators
        """
        if not POLARS_AVAILABLE:
            return self._fallback_batch_process_symbols(symbol_data, max_ticks_per_symbol)
        
        # Enforce batch size bounds
        if len(symbol_data) > self.max_batch_size:
            logger.warning(f"Batch size {len(symbol_data)} exceeds max {self.max_batch_size}, truncating")
            symbol_data = dict(list(symbol_data.items())[:self.max_batch_size])
        
        # ✅ SIMPLIFIED CACHING: Track fresh calculations
        self._fresh_calculations.clear()
        current_time = time.time()
        results = {}
        
        # ✅ OPTIMIZED: Combine all symbols into single DataFrame for efficient processing
        all_batch_data = []
        symbol_to_ticks = {}  # Track which ticks belong to which symbol
        
        for symbol, ticks in symbol_data.items():
            # Limit ticks per symbol to prevent memory bloat
            limited_ticks = ticks[-max_ticks_per_symbol:] if len(ticks) > max_ticks_per_symbol else ticks
            symbol_to_ticks[symbol] = limited_ticks
            
            # Prepare data for DataFrame
            # ✅ REMOVED: _parse_and_flatten_ohlc() - OHLC already flattened by parser
            for tick in limited_ticks:
                parsed_tick = tick if isinstance(tick, dict) else {}
                all_batch_data.append({
                    'symbol': symbol,
                    'last_price': parsed_tick.get('last_price', 0.0),
                    'zerodha_cumulative_volume': parsed_tick.get('zerodha_cumulative_volume', 0.0),
                    'bucket_incremental_volume': parsed_tick.get('bucket_incremental_volume', 0.0),
                    'high': parsed_tick.get('high', parsed_tick.get('last_price', 0.0)),
                    'low': parsed_tick.get('low', parsed_tick.get('last_price', 0.0)),
                    'timestamp': parsed_tick.get('exchange_timestamp_ms', parsed_tick.get('timestamp', 0)),
                    'volume_ratio': parsed_tick.get('volume_ratio'),  # ✅ Include pre-computed volume_ratio
                })
        
        if not all_batch_data:
            return results
        
        # ✅ OPTIMIZED: Create single DataFrame and partition by symbol
        # Let Polars infer types - _create_normalized_timestamp() will handle proper datetime parsing
        combined_df = pl.DataFrame(all_batch_data)
        symbol_groups = combined_df.partition_by("symbol", maintain_order=True)
        
        # Process each symbol group
        for symbol_group in symbol_groups:
            if symbol_group.is_empty():
                continue
                
            symbol = symbol_group['symbol'][0]  # Get symbol from first row
            limited_ticks = symbol_to_ticks.get(symbol, [])
            
            cache_key = f"{symbol}_indicators"
            
            # ✅ SIMPLIFIED CACHING: Check if we should recalculate
            should_recalculate = self._should_recalculate(symbol, current_time, limited_ticks)
            
            if should_recalculate:
                # ✅ FIX: Check session-based window size (includes historical OHLC data) in addition to current batch
                session_window_size = 0
                if symbol in self._symbol_sessions:
                    window = self._symbol_sessions[symbol]
                    if window is not None:
                        session_window_size = window.height if hasattr(window, 'height') else len(window) if hasattr(window, '__len__') else 0
                
                current_batch_size = len(limited_ticks)
                total_data_points = session_window_size + current_batch_size
                
                # Calculate fresh indicators
                # ✅ FIX: Always calculate - batch_calculate_indicators handles insufficient data gracefully (returns defaults)
                try:
                    # ✅ FIX: Use session-based window data if available (includes historical OHLC from DB1)
                    # This ensures symbols with historical OHLC data can calculate indicators even with few new ticks
                    if symbol in self._symbol_sessions and session_window_size >= 20:
                        window = self._symbol_sessions[symbol]
                        if window is not None and window.height >= 20:
                            # ✅ Use session-based window data (includes historical OHLC) for calculations
                            logger.debug(f"🔍 [CALC_DEBUG] {symbol} Using session-based window with {window.height} data points (includes historical OHLC)")
                            # Convert session-based window DataFrame to tick_data format
                            try:
                                window_tick_data = []
                                for row in window.iter_rows(named=True):
                                    window_tick_data.append({
                                        'last_price': row.get('last_price', row.get('close', 0.0)),
                                        'bucket_incremental_volume': row.get('bucket_incremental_volume', row.get('volume', 0)),
                                        'zerodha_cumulative_volume': row.get('zerodha_cumulative_volume', 0),
                                        'high': row.get('high', row.get('last_price', 0.0)),
                                        'low': row.get('low', row.get('last_price', 0.0)),
                                        'exchange_timestamp_ms': row.get('timestamp', 0),
                                        'volume_ratio': row.get('volume_ratio', None),
                                        'volume': row.get('volume', 0)
                                    })
                                # Use session-based window data for calculation
                                indicators = self.batch_calculate_indicators(window_tick_data, symbol)
                                logger.debug(f"🔍 [CALC_DEBUG] {symbol} Calculated indicators using session-based window data ({len(window_tick_data)} points)")
                            except Exception as window_err:
                                logger.debug(f"🔍 [CALC_DEBUG] {symbol} Error using session-based window data: {window_err}, falling back to batch data")
                                # Fall through to use batch data
                                indicators = self.batch_calculate_indicators(limited_ticks, symbol)
                        else:
                            # Session-based window exists but doesn't have enough data, use batch
                            indicators = self.batch_calculate_indicators(limited_ticks, symbol)
                    else:
                        # ✅ OPTIMIZED: Use partitioned DataFrame directly for calculations
                        # Extract data from symbol group
                        prices = symbol_group['last_price'].to_list()
                        volumes = symbol_group['bucket_incremental_volume'].to_list()
                        highs = symbol_group['high'].to_list()
                        lows = symbol_group['low'].to_list()
                        timestamps = symbol_group['timestamp'].to_list()
                        
                        # Convert to tick_data format for batch_calculate_indicators
                        tick_data = [
                            {
                                'last_price': p,
                                'bucket_incremental_volume': v,
                                'zerodha_cumulative_volume': symbol_group['zerodha_cumulative_volume'].to_list()[i],
                                'high': h,
                                'low': l,
                                'exchange_timestamp_ms': t,
                                'volume_ratio': symbol_group['volume_ratio'].to_list()[i] if 'volume_ratio' in symbol_group.columns else None
                            } for i, (p, v, h, l, t) in enumerate(zip(prices, volumes, highs, lows, timestamps))
                        ]
                        
                        indicators = self.batch_calculate_indicators(tick_data, symbol)
                    results[symbol] = indicators
                    
                    # ✅ SIMPLIFIED CACHING: Update cache with fresh calculation
                    self._cache[cache_key] = {
                        'data': indicators,
                        'timestamp': current_time,
                        'data_hash': hash(str(limited_ticks))
                    }
                    self._last_calculation_time[symbol] = current_time
                    self._fresh_calculations.add(symbol)  # Mark as fresh calculation
                    
                except Exception as e:
                    logger.error(f"Error calculating indicators for {symbol}: {e}")
                    results[symbol] = {}
            else:
                # ✅ SIMPLIFIED CACHING: Use cached result
                results[symbol] = self._cache[cache_key]['data']
        
        return results
    
    def _fallback_batch_process_symbols(self, symbol_data: Dict[str, List[Dict]], max_ticks_per_symbol: int) -> Dict[str, Dict]:
        """Fallback batch processing when Polars is not available"""
        results = {}
        
        for symbol, ticks in symbol_data.items():
            # Limit ticks per symbol
            limited_ticks = ticks[-max_ticks_per_symbol:] if len(ticks) > max_ticks_per_symbol else ticks
            
            if len(limited_ticks) >= 20:
                try:
                    indicators = self.batch_calculate_indicators(limited_ticks, symbol)
                    results[symbol] = indicators
                except Exception as e:
                    logger.error(f"Error processing {symbol}: {e}")
                    results[symbol] = {}
        
        return results
    
    def get_memory_usage(self) -> Dict[str, Any]:
        """Get memory usage statistics"""
        import sys
        
        # ✅ ENHANCED: Calculate memory for Polars cache (handle both dict and PolarsCache)
        if hasattr(self._polars_cache, 'cache'):
            # PolarsCache instance
            polars_memory = sum(sys.getsizeof(entry['df']) for entry in self._polars_cache.cache.values())
        else:
            # Legacy dict format
            polars_memory = sum(sys.getsizeof(v) for v in self._polars_cache.values())
        # ✅ SIMPLIFIED: Calculate memory for unified cache
        indicator_cache_memory = sum(
            sys.getsizeof(v) + sys.getsizeof(k) 
            for k, v in self._cache.items()
        )
        
        return {
            'polars_cache_memory_mb': polars_memory / (1024 * 1024),
            'indicator_cache_memory_mb': indicator_cache_memory / (1024 * 1024),
            'total_memory_mb': (polars_memory + indicator_cache_memory) / (1024 * 1024),
            'max_cache_size': self.max_cache_size,
            'max_batch_size': self.max_batch_size,
            'cache_utilization': len(self._cache) / self.max_cache_size if self.max_cache_size > 0 else 0
        }
    
    def process_tick(self, symbol: str, tick_data: Dict) -> Dict:
        """
        Process individual tick with session-based window (compatible with existing scanner)
        This method provides backward compatibility while using optimized calculations
        """
        try:
            # ✅ FIX: Use replace() to remove ALL embedded quotes (strip only removes edge quotes)
            raw_symbol = str(symbol).replace("'", "").replace('"', "").replace("`", "").strip()

            # ✅ SYMBOL GUARD + CANONICALIZATION to prevent BFOBBFO/NSEB' variants
            symbol_guard = getattr(self, "_symbol_guard", None)
            if symbol_guard is None:
                symbol_guard = get_symbol_guard()
                self._symbol_guard = symbol_guard
            guarded_symbol = symbol_guard.guard_symbol(raw_symbol, context="calculations.process_tick") or raw_symbol
            canonical_symbol = guarded_symbol
            try:
                normalized = RedisKeyStandards.canonical_symbol(guarded_symbol)
                if normalized:
                    canonical_symbol = normalized
            except Exception as canon_exc:
                logger.debug(f"⚠️ [SYMBOL_CANON] Failed to canonicalize {guarded_symbol}: {canon_exc}")

            if canonical_symbol != raw_symbol:
                logger.debug(f"🔁 [SYMBOL_CANON] {raw_symbol} -> {canonical_symbol}")

            symbol = canonical_symbol
            tick_data['symbol'] = symbol
            tick_data['tradingsymbol'] = symbol
            
            # ✅ FIX: Skip index symbols - they don't need indicator calculations
            # Indices cause Polars errors and have no volume/Greeks to calculate
            if self._is_index_symbol_fast(symbol):
                logger.debug(f"⏭️ [SKIP_INDEX] {symbol} - Index symbol, skipping calculations")
                return {
                    'symbol': symbol,
                    'is_index': True,
                    'last_price': tick_data.get('last_price', 0.0),
                    'timestamp': tick_data.get('exchange_timestamp_ms', tick_data.get('timestamp', 0)),
                }
            
            # ✅ CRITICAL FIX: Decode ALL bytes in tick_data BEFORE any Polars processing
            # This prevents "casting from BinaryView to Float64/Int64 not supported" errors
            tick_data = self._decode_all_bytes(tick_data)
            
            # ✅ REMOVED: _normalize_volume_at_ingestion() - volume already normalized by intraday_websocket_parser.py
            # Parser provides cleaned data with volume_ratio, incremental_volume, etc. already calculated
            
            self._update_cross_asset_state(symbol, tick_data)
            # ✅ CPU OPTIMIZATION: Only log if DEBUG level is enabled (avoids string formatting overhead)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"🔍 [CALC_DEBUG] process_tick called for {symbol} - tick_data keys: {list(tick_data.keys())[:15]}")
                logger.debug(f"🔍 [CALC_DEBUG] {symbol} Input: last_price={tick_data.get('last_price')}, volume_ratio={tick_data.get('volume_ratio')}, underlying_price={tick_data.get('underlying_price')}")
            
            # Update session-based window for real-time processing
            self._update_session_window(symbol, tick_data)
            
            # Get current window for this symbol
            if symbol not in self._symbol_sessions or self._symbol_sessions[symbol].height < 20:
                # Fallback to single tick processing if insufficient data
                window_height = self._symbol_sessions[symbol].height if symbol in self._symbol_sessions else 0
                logger.debug(f"🔍 [CALC_DEBUG] {symbol} Insufficient session-based window (height={window_height}), using batch calculation fallback")
                tick_list = [tick_data]
                fallback_indicators = self.batch_calculate_indicators(tick_list, symbol)
                if fallback_indicators:
                    logger.debug(f"🔍 [CALC_DEBUG] {symbol} Fallback batch calculation returned {len(fallback_indicators)} indicators")
                    # ✅ FIXED: Mark as fresh calculation so indicators get stored to Redis
                    microstructure_metrics = self._calculate_microstructure_metrics(symbol, tick_data)
                    if microstructure_metrics:
                        fallback_indicators.update(microstructure_metrics)
                    self._fresh_calculations.add(symbol)
                    return fallback_indicators
                # If batch calculation fails, return minimal indicators to prevent "No indicators calculated" warning
                logger.warning(f"🔍 [CALC_DEBUG] {symbol} Batch calculation failed, returning minimal indicators")
                fallback_payload = {
                    'symbol': symbol,
                    'last_price': tick_data.get('last_price', 0.0),
                    'timestamp': tick_data.get('exchange_timestamp_ms', tick_data.get('timestamp', 0)),
                    'volume_ratio': _extract_volume_ratio(tick_data),
                    'price_change': 0.0
                }
                microstructure_metrics = self._calculate_microstructure_metrics(symbol, tick_data)
                if microstructure_metrics:
                    fallback_payload.update(microstructure_metrics)
                cross_metrics = self._calculate_cross_asset_metrics(symbol, tick_data.get('last_price', 0.0))
                if cross_metrics:
                    fallback_payload.update(cross_metrics)
                return fallback_payload
            
            # Use session-based window for real-time indicators
            window = self._normalize_window_schema(self._symbol_sessions[symbol])
            logger.debug(f"🔍 [CALC_DEBUG] {symbol} Using session-based window (height={window.height}) for real-time indicators")
            indicators = self._calculate_realtime_indicators(symbol, window, tick_data)
            logger.debug(f"🔍 [CALC_DEBUG] {symbol} _calculate_realtime_indicators returned {len(indicators) if indicators else 0} indicators")
            
            # ✅ FIXED: Mark as fresh calculation so indicators get stored to Redis
            if indicators:
                microstructure_metrics = self._calculate_microstructure_metrics(symbol, tick_data)
                if microstructure_metrics:
                    indicators.update(microstructure_metrics)
                self._fresh_calculations.add(symbol)
            
            return indicators
            
        except Exception as e:
            # ✅ ENHANCED: Better error logging to identify Float32/Float64 type mismatches
            error_msg = str(e)
            if "Float32" in error_msg and "Float64" in error_msg:
                logger.error(f"❌ [TYPE_MISMATCH] Error processing tick for {symbol}: {e}")
                logger.error(f"   This is a Polars type mismatch - Float32 vs Float64. Check DataFrame concatenation.")
                logger.debug(f"   Tick data keys: {list(tick_data.keys())[:10]}")
                logger.debug(f"   Symbol: {symbol}")
            else:
                logger.error(f"Error processing tick for {symbol}: {e}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
            # Return minimal indicators instead of empty dict to prevent "No indicators calculated" warning
            fallback_payload = {
                'symbol': symbol,
                'last_price': tick_data.get('last_price', 0.0),
                'timestamp': tick_data.get('exchange_timestamp_ms', tick_data.get('timestamp', 0)),
                'volume_ratio': _extract_volume_ratio(tick_data),
                'price_change': 0.0
            }
            microstructure_metrics = self._calculate_microstructure_metrics(symbol, tick_data)
            if microstructure_metrics:
                fallback_payload.update(microstructure_metrics)
            cross_metrics = self._calculate_cross_asset_metrics(symbol, fallback_payload.get('last_price', 0.0))
            if cross_metrics:
                fallback_payload.update(cross_metrics)
            return fallback_payload
    
    def _normalize_window_schema(self, df):
        """Normalize Polars DataFrame schema to prevent type mismatches during concat.
        
        Ensures all columns have consistent Float64/Int64 types to avoid
        'type String is incompatible with expected type Float64' errors.
        """
        if not POLARS_AVAILABLE or df is None:
            return df
        
        # Define expected schema
        expected_schema = {
            'timestamp': pl.Int64,
            'last_price': pl.Float64,
            'zerodha_cumulative_volume': pl.Int64,
            'zerodha_last_traded_quantity': pl.Int64,
            'high': pl.Float64,
            'low': pl.Float64,
            'open': pl.Float64,
            'close': pl.Float64,
            'average_price': pl.Float64,
            'bucket_incremental_volume': pl.Int64,
            'bucket_cumulative_volume': pl.Int64,
            'volume_ratio': pl.Float64,
            'volume': pl.Int64
        }
        
        try:
            # Cast columns to expected types
            for col_name, expected_type in expected_schema.items():
                if col_name in df.columns:
                    current_type = df[col_name].dtype
                    if current_type != expected_type:
                        try:
                            # ✅ FIX: Handle String columns specially - they need str.strip() first
                            if current_type == pl.Utf8 or current_type == pl.String:
                                # Convert string to numeric via casting with strict=False
                                if expected_type in [pl.Float64, pl.Float32]:
                                    df = df.with_columns(
                                        pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False).fill_null(0.0).alias(col_name)
                                    )
                                else:
                                    df = df.with_columns(
                                        pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False).fill_null(0.0).cast(pl.Int64).alias(col_name)
                                    )
                            else:
                                df = df.with_columns(pl.col(col_name).cast(expected_type))
                        except Exception:
                            # If cast fails, replace with null/zero
                            if expected_type in [pl.Float64, pl.Float32]:
                                df = df.with_columns(pl.lit(0.0).cast(expected_type).alias(col_name))
                            else:
                                df = df.with_columns(pl.lit(0).cast(expected_type).alias(col_name))
            
            return df
        except Exception as e:
            logger.warning(f"Failed to normalize window schema: {e}")
            return df
    
    def _update_session_window(self, symbol: str, tick_data: Dict):
        """Update the per-symbol session-based window using canonical session data."""
        try:
            if not POLARS_AVAILABLE:
                return

            timestamp_ms = None
            if 'exchange_timestamp_ms' in tick_data:
                try:
                    timestamp_ms = int(tick_data['exchange_timestamp_ms'])
                except (TypeError, ValueError):
                    pass
            if timestamp_ms is None and 'exchange_timestamp_epoch' in tick_data:
                try:
                    timestamp_ms = int(tick_data['exchange_timestamp_epoch']) * 1000
                except (TypeError, ValueError):
                    pass
            if timestamp_ms is None:
                timestamp_val = tick_data.get('exchange_timestamp') or tick_data.get('timestamp')
                if timestamp_val:
                    try:
                        from shared_core.config_utils.timestamp_normalizer import TimestampNormalizer
                        timestamp_ms = TimestampNormalizer.to_epoch_ms(timestamp_val)
                    except Exception:
                        timestamp_ms = 0
                else:
                    timestamp_ms = 0
            if not isinstance(timestamp_ms, int):
                try:
                    timestamp_ms = int(float(timestamp_ms))
                except (TypeError, ValueError):
                    timestamp_ms = 0

            # ✅ REMOVED: _parse_and_flatten_ohlc() - OHLC already flattened by parser
            parsed_tick_data = tick_data if isinstance(tick_data, dict) else {}
            from shared_core.volume_files.correct_volume_calculator import VolumeResolver
            volume_ratio = VolumeResolver.get_volume_ratio(parsed_tick_data) or parsed_tick_data.get('volume_ratio', 0.0)
            if not volume_ratio or volume_ratio == 0.0:
                try:
                    # ✅ FIXED: Use existing pooled redis_client
                    redis_client = self.redis_client
                    from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
                    indicator_key = DatabaseAwareKeyBuilder.live_indicator_hash(symbol)
                    stored_ratio = redis_client.hget(indicator_key, "volume_ratio")
                    if stored_ratio is not None:
                        if isinstance(stored_ratio, bytes):
                            stored_ratio = stored_ratio.decode('utf-8', errors='ignore')
                        volume_ratio = float(stored_ratio)
                except Exception:
                    volume_ratio = 0.0
                if not volume_ratio or volume_ratio == 0.0:
                    try:
                        calculator = self._get_volume_calculator()
                        if calculator:
                            volume_metrics = calculator.calculate_volume_metrics(symbol, parsed_tick_data)
                            volume_ratio = float(volume_metrics.get('volume_ratio', 0.0))
                    except Exception:
                        volume_ratio = 0.0

            # ✅ CRITICAL FIX: Ensure ALL values are properly typed before creating DataFrame
            # This prevents "type String is incompatible with expected type Float64" errors
            new_tick_dict = {
                'timestamp': int(timestamp_ms) if timestamp_ms else 0,
                'last_price': self._safe_float(parsed_tick_data.get('last_price', 0.0)),
                'zerodha_cumulative_volume': self._safe_int(parsed_tick_data.get('zerodha_cumulative_volume', 0)),
                'zerodha_last_traded_quantity': self._safe_int(parsed_tick_data.get('zerodha_last_traded_quantity', 0)),
                'high': self._safe_float(parsed_tick_data.get('high', parsed_tick_data.get('high_price', parsed_tick_data.get('last_price', 0.0)))),
                'low': self._safe_float(parsed_tick_data.get('low', parsed_tick_data.get('low_price', parsed_tick_data.get('last_price', 0.0)))),
                'open': self._safe_float(parsed_tick_data.get('open', parsed_tick_data.get('open_price', 0.0))),
                'close': self._safe_float(parsed_tick_data.get('close', parsed_tick_data.get('close_price', parsed_tick_data.get('last_price', 0.0)))),
                'average_price': self._safe_float(parsed_tick_data.get('average_price', parsed_tick_data.get('average_traded_price', 0.0))),
                'bucket_incremental_volume': self._safe_int(parsed_tick_data.get('bucket_incremental_volume', 0)),
                'bucket_cumulative_volume': self._safe_int(parsed_tick_data.get('bucket_cumulative_volume', 0)),
                'volume_ratio': self._safe_float(volume_ratio),
                'volume': self._safe_int(parsed_tick_data.get('volume', parsed_tick_data.get('bucket_incremental_volume', 0)))
            }
            
            # ✅ CRITICAL: Create DataFrame with explicit schema to prevent type mismatches
            new_tick = pl.DataFrame([new_tick_dict], schema={
                'timestamp': pl.Int64,
                'last_price': pl.Float64,
                'zerodha_cumulative_volume': pl.Int64,
                'zerodha_last_traded_quantity': pl.Int64,
                'high': pl.Float64,
                'low': pl.Float64,
                'open': pl.Float64,
                'close': pl.Float64,
                'average_price': pl.Float64,
                'bucket_incremental_volume': pl.Int64,
                'bucket_cumulative_volume': pl.Int64,
                'volume_ratio': pl.Float64,
                'volume': pl.Int64
            })
            new_tick = self._normalize_window_schema(new_tick)

            cumulative_volume = self._safe_int(
                parsed_tick_data.get('bucket_cumulative_volume',
                                     parsed_tick_data.get('zerodha_cumulative_volume', 0))
            )
            last_volume = self._last_session_volume.get(symbol)
            if last_volume is not None and cumulative_volume < last_volume:
                logger.debug(f"🔁 [SESSION_RESET] {symbol} detected session reset; clearing session-based window")
                self._symbol_sessions.pop(symbol, None)
            self._last_session_volume[symbol] = cumulative_volume

            if symbol not in self._symbol_sessions:
                historical_data = self._bootstrap_from_redis_history(symbol, min_ticks=20)
                if historical_data is None or historical_data.height < 20:
                    historical_data = self._get_historical_bucket_data(symbol)
                if historical_data is not None and historical_data.height >= 20:
                    self._symbol_sessions[symbol] = self._normalize_window_schema(historical_data)
                    logger.warning(f"✅ [SESSION_WINDOW_INIT] {symbol} - Initialized with {historical_data.height} historical ticks (patterns ENABLED)")
                else:
                    self._symbol_sessions[symbol] = new_tick
                    logger.warning(f"⚠️ [SESSION_WINDOW_INIT] {symbol} - Started with 1 tick (need 19 more for pattern detection)")
                return

            existing_window = self._normalize_window_schema(self._symbol_sessions[symbol])
            try:
                window = pl.concat([existing_window, new_tick])
            except Exception as concat_error:
                logger.warning(
                    f"⚠️ [SESSION_WINDOW_RESET] {symbol} concat failed ({concat_error}); resetting window"
                )
                window = new_tick
            window = self._normalize_window_schema(window)
            if window.height > self._session_window_size:
                window = window.tail(self._session_window_size)
            self._symbol_sessions[symbol] = window

        except Exception as e:
            logger.error(f"Error updating session-based window for {symbol}: {e}")
    def _detect_exchange_prefix(self, symbol: str) -> tuple[bool, bool]:
        """
        Detect if symbol is MCX (commodity) or CDS (currency) based on keywords.
        
        Returns:
            tuple[bool, bool]: (is_mcx_symbol, is_cds_symbol)
        """
        symbol_upper = symbol.upper()
        mcx_keywords = ['GOLD', 'SILVER', 'CRUDE', 'NATURALGAS', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM']
        cds_keywords = ['USDINR', 'EURINR', 'GBPINR', 'JPYINR', 'CNYINR', 'AUDINR', 'CADINR', 'SGDINR', 'CHFINR', 'HKDINR']
        is_mcx_symbol = any(keyword in symbol_upper for keyword in mcx_keywords)
        is_cds_symbol = any(keyword in symbol_upper for keyword in cds_keywords)
        return is_mcx_symbol, is_cds_symbol
    
    def _bootstrap_from_redis_history(self, symbol: str, min_ticks: int = 20):
        """
        Bootstrap session-based window from Redis stream history for immediate pattern detection.
        
        This allows new symbols to start detecting patterns after first tick instead of waiting
        for 20+ ticks to accumulate in memory.
        
        Args:
            symbol: Symbol to bootstrap
            min_ticks: Minimum number of historical ticks to fetch (default: 20)
            
        Returns:
            Polars DataFrame with historical ticks, or None if insufficient data
        """
        if not self.redis_client:
            return None
        
        try:
            # Get DB1 client for stream access
            db1_client = None
            if hasattr(self.redis_client, 'get_client'):
                db1_client = self.redis_client.get_client(1)  # DB 1
            elif hasattr(self.redis_client, 'redis'):
                db1_client = self.redis_client.redis
            elif hasattr(self.redis_client, 'get'):
                db1_client = self.redis_client
            
            if not db1_client:
                logger.debug(f"⚠️ [{symbol}] No Redis client available for bootstrapping")
                return None
            
            # Use Redis key standards for stream key
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            stream_key = DatabaseAwareKeyBuilder.live_processed_stream()
            
            # Read last 2000 messages from stream (to find 20+ for this symbol)
            # With ~80+ symbols trading, need deeper history to find 20 ticks for one symbol
            # Stream has maxlen=5000, so 2000 gives us ~25 ticks per symbol on average
            messages = db1_client.xrevrange(stream_key, '+', '-', count=2000)
            
            if not messages:
                logger.debug(f"⚠️ [{symbol}] No messages in stream {stream_key}")
                return None
            
            # Filter messages for this symbol and parse
            symbol_ticks = []
            for msg_id, msg_data in messages:
                try:
                    # msg_data is a dict with field-value pairs
                    data_str = msg_data.get(b'data') or msg_data.get('data')
                    if data_str:
                        import json
                        if isinstance(data_str, bytes):
                            data_str = data_str.decode('utf-8')
                        tick = json.loads(data_str)
                        
                        # ✅ CRITICAL FIX: Check symbol with normalization (handle exchange prefixes)
                        tick_symbol = tick.get('symbol') or tick.get('tradingsymbol', '')
                        # Normalize both symbols for comparison (strip exchange prefixes)
                        normalized_tick_symbol = tick_symbol.upper().replace('NFO:', '').replace('NSE:', '').replace('MCX:', '').replace('CDS:', '')
                        normalized_target_symbol = symbol.upper().replace('NFO:', '').replace('NSE:', '').replace('MCX:', '').replace('CDS:', '')
                        
                        if normalized_tick_symbol == normalized_target_symbol or tick_symbol == symbol:
                            symbol_ticks.append(tick)
                            
                            # Stop once we have enough ticks
                            if len(symbol_ticks) >= min_ticks:
                                break
                except Exception as e:
                    logger.debug(f"Error parsing stream message: {e}")
                    continue
            
            if len(symbol_ticks) < min_ticks:
                logger.debug(f"⚠️ [{symbol}] Only found {len(symbol_ticks)} ticks in stream (need {min_ticks})")
                return None
            
            # Convert to Polars DataFrame using same schema as _update_session_window
            if not POLARS_AVAILABLE:
                return None
            
            from shared_core.volume_files.correct_volume_calculator import VolumeResolver
            
            tick_rows = []
            for tick in reversed(symbol_ticks):  # Oldest first
                volume_ratio = VolumeResolver.get_volume_ratio(tick) or tick.get('volume_ratio', 0.0)
                # ✅ FIX: Use 0.0 instead of 1.0 to avoid false positives in batch processing
                # VolumeComputationManager calculation not available in this batch context
                if not volume_ratio or volume_ratio == 0.0:
                    volume_ratio = 0.0
                
                # Parse timestamp
                timestamp_ms = tick.get('exchange_timestamp_ms')
                if not timestamp_ms:
                    timestamp_ms = tick.get('timestamp_ms', int(time.time() * 1000))
                
                # ✅ REMOVED: _parse_and_flatten_ohlc() - OHLC already flattened by parser
                parsed_tick = tick if isinstance(tick, dict) else {}
                
                tick_rows.append({
                    'timestamp': self._safe_int(timestamp_ms, int(time.time() * 1000)),
                    'last_price': self._safe_float(parsed_tick.get('last_price', 0.0)),
                    'zerodha_cumulative_volume': self._safe_int(parsed_tick.get('zerodha_cumulative_volume', 0)),
                    'zerodha_last_traded_quantity': self._safe_int(parsed_tick.get('zerodha_last_traded_quantity', 0)),
                    'high': self._safe_float(parsed_tick.get('high', parsed_tick.get('high_price', parsed_tick.get('last_price', 0.0)))),
                    'low': self._safe_float(parsed_tick.get('low', parsed_tick.get('low_price', parsed_tick.get('last_price', 0.0)))),
                    'open': self._safe_float(parsed_tick.get('open', parsed_tick.get('open_price', 0.0))),
                    'close': self._safe_float(parsed_tick.get('close', parsed_tick.get('close_price', parsed_tick.get('last_price', 0.0)))),
                    'average_price': self._safe_float(parsed_tick.get('average_price', parsed_tick.get('average_traded_price', 0.0))),
                    'bucket_incremental_volume': self._safe_int(parsed_tick.get('bucket_incremental_volume', 0)),
                    'bucket_cumulative_volume': self._safe_int(parsed_tick.get('bucket_cumulative_volume', 0)),
                    'volume_ratio': self._safe_float(volume_ratio),
                    'volume': self._safe_int(parsed_tick.get('volume', parsed_tick.get('bucket_incremental_volume', 0)))
                })
            
            # Create DataFrame with strict schema (matches _update_session_window session-based window schema)
            df = pl.DataFrame(tick_rows, schema={
                'timestamp': pl.Int64,
                'last_price': pl.Float64,
                'zerodha_cumulative_volume': pl.Int64,
                'zerodha_last_traded_quantity': pl.Int64,
                'high': pl.Float64,
                'low': pl.Float64,
                'open': pl.Float64,  # ✅ ADDED: Open price
                'close': pl.Float64,  # ✅ ADDED: Close price
                'average_price': pl.Float64,  # ✅ ADDED: Average price
                'bucket_incremental_volume': pl.Int64,
                'bucket_cumulative_volume': pl.Int64,
                'volume_ratio': pl.Float64,
                'volume': pl.Int64
            })
            
            logger.info(f"✅ [BOOTSTRAP] {symbol} - Loaded {df.height} historical ticks from Redis stream")
            return df
            
        except Exception as e:
            logger.warning(f"⚠️ [{symbol}] Bootstrap from Redis stream failed: {e}")
            return None
    
    def _get_historical_bucket_data(self, symbol: str):
        """Get historical bucket data from Redis for session-based window initialization
        
        ✅ FIXED: All data in DB1 (unified) - migration complete, DB0 removed
        - DB1: ohlc_daily:{symbol} (sorted set with 55 days of historical OHLC data) - unified structure
        - DB1: bucket_incremental_volume:history:{resolution}:{symbol} (history lists)
        - Individual bucket hash keys (bucket2) are NOT created in Redis
        - Uses direct key lookups (no SCAN) per redis_key_standards.py
        
        ✅ FIXED: Options should have their own OHLC data (volume/price differ from underlying)
        - Priority 1: Try full option symbol first (e.g., BANKNIFTY25NOV57100PE)
        - Priority 2: Fall back to base symbol (e.g., BANKNIFTY) if option-specific data not found
        """
        try:
            redis_client = getattr(self, 'redis_client', None)
            historical_buckets = None
            import json
            
            # ✅ FIXED: Options should have their own OHLC data (volume/price differ from underlying)
            # Try full symbol first, then fall back to base symbol
            base_symbol = extract_underlying_from_symbol(symbol) or symbol
            logger.debug(f"🔍 [HISTORICAL_DEBUG] {symbol} - Will try full symbol first, then base: {base_symbol} as fallback")
            
            # ✅ PRIORITY 1: Try DB1 first (unified) - ohlc_daily sorted set (55 days of historical data)
            # ✅ FIXED: Use RobustRedisClient._load_ohlc_series() method signature if available
            try:
                from shared_core.redis_clients.redis_client import RedisClientFactory
                from shared_core.redis_clients.redis_key_standards import (
                    DatabaseAwareKeyBuilder,
                    RedisKeyStandards,
                )
                
                # ✅ FIXED: Try to use RobustRedisClient._load_ohlc_series() if redis_client is RobustRedisClient
                if redis_client and hasattr(redis_client, '_load_ohlc_series'):
                    # Use RobustRedisClient's method directly (matches signature: symbol, limit, interval)
                    # ✅ FIXED: Try full symbol first (options have their own OHLC), then fall back to base symbol
                    # ✅ DISABLED: Currency and commodity handling removed - stick to NSE/NFO only
                    logger.debug(f"🔍 [HISTORICAL_DEBUG] {symbol} - Using RobustRedisClient._load_ohlc_series() - trying full symbol first")
                    try:
                        # Priority 1: Try full symbol (options should have their own OHLC data)
                        ohlc_series = redis_client._load_ohlc_series(symbol, limit=55, interval="1d")
                        
                        # Priority 2: Fall back to base symbol if full symbol not found
                        if not ohlc_series and base_symbol != symbol:
                            logger.debug(f"🔍 [HISTORICAL_DEBUG] {symbol} - Full symbol not found, trying base symbol: {base_symbol}")
                            ohlc_series = redis_client._load_ohlc_series(base_symbol, limit=55, interval="1d")
                        if ohlc_series:
                            # Convert OHLC series to bucket format (same as _convert_ohlc_to_buckets logic)
                            historical_buckets = []
                            for entry in ohlc_series:
                                try:
                                    # Entry format from _load_ohlc_series: {'o', 'h', 'l', 'c', 'v', 'd', 'timestamp'}
                                    # ✅ CRITICAL: Use _safe_float and _safe_int to handle string values from Redis
                                    timestamp_val = entry.get('timestamp', 0)
                                    if isinstance(timestamp_val, str):
                                        try:
                                            timestamp_val = float(timestamp_val)
                                        except (ValueError, TypeError):
                                            timestamp_val = 0
                                    # ✅ FIXED: Correct epoch length detection (same as intraday_crawler)
                                    if timestamp_val >= 1e15:
                                        # Nanoseconds - convert to milliseconds
                                        timestamp_ms = int(timestamp_val / 1_000_000)
                                    elif timestamp_val >= 1e10:
                                        # Already in milliseconds
                                        timestamp_ms = int(timestamp_val)
                                    else:
                                        # Seconds - convert to milliseconds
                                        timestamp_ms = int(timestamp_val * 1000)
                                    
                                    bucket = {
                                        'close': self._safe_float(entry.get('c', entry.get('close', 0.0))),
                                        'last_price': self._safe_float(entry.get('c', entry.get('close', 0.0))),
                                        'high': self._safe_float(entry.get('h', entry.get('high', 0.0))),
                                        'low': self._safe_float(entry.get('l', entry.get('low', 0.0))),
                                        'open': self._safe_float(entry.get('o', entry.get('open', 0.0))),
                                        'average_price': self._safe_float(entry.get('c', entry.get('close', 0.0))),  # ✅ ADDED: Average price
                                        'bucket_incremental_volume': self._safe_int(entry.get('v', entry.get('volume', 0))),
                                        'bucket_cumulative_volume': self._safe_int(entry.get('v', entry.get('volume', 0))),
                                        'zerodha_cumulative_volume': self._safe_int(entry.get('v', entry.get('volume', 0))),
                                        'zerodha_last_traded_quantity': 0,  # ✅ ADDED: Required field
                                        'volume_ratio': 1.0,
                                        'volume': self._safe_int(entry.get('v', entry.get('volume', 0))),
                                        'timestamp': self._safe_int(timestamp_ms),
                                        'last_timestamp': entry.get('d', ''),
                                        'date': entry.get('d', '')
                                    }
                                    historical_buckets.append(bucket)
                                except Exception as e:
                                    logger.debug(f"⚠️ [HISTORICAL_DEBUG] {symbol} - Error converting OHLC entry: {e}")
                                    continue
                            
                            if historical_buckets:
                                logger.debug(f"✅ [HISTORICAL_DEBUG] {symbol} - Got {len(historical_buckets)} historical OHLC buckets via RobustRedisClient._load_ohlc_series()")
                        else:
                            # ✅ FIXED: Try NFO prefix for index options (NIFTY/BANKNIFTY are NFO, not MCX)
                            nfo_symbol = f"NFO{base_symbol}"
                            logger.debug(f"🔍 [HISTORICAL_DEBUG] {symbol} - Trying NFO prefix: {nfo_symbol}")
                            ohlc_series = redis_client._load_ohlc_series(nfo_symbol, limit=55, interval="1d")
                            if ohlc_series:
                                historical_buckets = []
                                for entry in ohlc_series:
                                    try:
                                        # ✅ CRITICAL: Use _safe_float and _safe_int to handle string values from Redis
                                        timestamp_val = entry.get('timestamp', 0)
                                        if isinstance(timestamp_val, str):
                                            try:
                                                timestamp_val = float(timestamp_val)
                                            except (ValueError, TypeError):
                                                timestamp_val = 0
                                        # ✅ FIXED: Correct epoch length detection (same as intraday_crawler)
                                        if timestamp_val >= 1e15:
                                            # Nanoseconds - convert to milliseconds
                                            timestamp_ms = int(timestamp_val / 1_000_000)
                                        elif timestamp_val >= 1e10:
                                            # Already in milliseconds
                                            timestamp_ms = int(timestamp_val)
                                        else:
                                            # Seconds - convert to milliseconds
                                            timestamp_ms = int(timestamp_val * 1000)
                                    except Exception as e:
                                        logger.warning(f"Error processing timestamp for {symbol}: {e}")
                                        timestamp_ms = 0
                                        
                                        bucket = {
                                            'close': self._safe_float(entry.get('c', entry.get('close', 0.0))),
                                            'last_price': self._safe_float(entry.get('c', entry.get('close', 0.0))),
                                            'high': self._safe_float(entry.get('h', entry.get('high', 0.0))),
                                            'low': self._safe_float(entry.get('l', entry.get('low', 0.0))),
                                            'open': self._safe_float(entry.get('o', entry.get('open', 0.0))),
                                            'average_price': self._safe_float(entry.get('c', entry.get('close', 0.0))),  # ✅ ADDED: Average price
                                            'bucket_incremental_volume': self._safe_int(entry.get('v', entry.get('volume', 0))),
                                            'bucket_cumulative_volume': self._safe_int(entry.get('v', entry.get('volume', 0))),
                                            'zerodha_cumulative_volume': self._safe_int(entry.get('v', entry.get('volume', 0))),
                                            'zerodha_last_traded_quantity': 0,  # ✅ ADDED: Required field
                                            'volume_ratio': 1.0,
                                            'volume': self._safe_int(entry.get('v', entry.get('volume', 0))),
                                            'timestamp': self._safe_int(timestamp_ms),
                                            'last_timestamp': entry.get('d', ''),
                                            'date': entry.get('d', '')
                                        }
                                        historical_buckets.append(bucket)
                                    except Exception:
                                        continue
                                
                                if historical_buckets:
                                    logger.debug(f"✅ [HISTORICAL_DEBUG] {symbol} - Got {len(historical_buckets)} historical OHLC buckets via RobustRedisClient._load_ohlc_series() (NFO prefix)")
                    except Exception as e:
                        logger.debug(f"⚠️ [HISTORICAL_DEBUG] {symbol} - RobustRedisClient._load_ohlc_series() failed: {e}")
                else:
                    # ✅ UPDATED: Fallback: Direct DB 1 access (unified, migrated from DB 2)
                    # ✅ UPDATED: All data in DB 1 (unified, migrated from DB 2)
                    redis_db1 = None
                    if redis_client and hasattr(redis_client, 'get_client'):
                        redis_db1 = redis_client.get_client(1)
                    else:
                        # ✅ CRITICAL FIX: Use singleton for DB 1
                        # ✅ FIXED: Use RedisClientFactory for centralized Redis access
                        from shared_core.redis_clients.redis_client import RedisClientFactory
                        redis_db1 = RedisClientFactory.get_trading_client()  # DB 1
                    
                    if redis_db1:
                        # ✅ FIXED: Options should have their own OHLC data - try full symbol first, then base symbol
                        # Priority 1: Try full option symbol (e.g., BANKNIFTY25NOV57100PE)
                        # Priority 2: Fall back to base symbol (e.g., BANKNIFTY) if option-specific data not found
                        canonical_full_symbol = RedisKeyStandards.canonical_symbol(symbol)
                        canonical_base_symbol = RedisKeyStandards.canonical_symbol(base_symbol) if base_symbol else ""
                        normalized_full_symbol = canonical_full_symbol
                        normalized_base_symbol = canonical_base_symbol

                        symbol_candidates: list[str] = []

                        def _add_candidate(value: Optional[str]):
                            if not value:
                                return
                            if value not in symbol_candidates:
                                symbol_candidates.append(value)

                        # ✅ DISABLED: MCX/CDS detection removed - stick to NSE/NFO/BSE only
                        
                        # Full symbol variants first
                        _add_candidate(canonical_full_symbol)
                        _add_candidate(symbol)
                        _add_candidate(normalized_full_symbol)
                        _add_candidate(f"NFO:{symbol}")
                        _add_candidate(f"NSE:{symbol}")
                        _add_candidate(f"BSE:{symbol}")
                        _add_candidate(f"NFO{normalized_full_symbol}")
                        # ✅ DISABLED: MCX/CDS prefix handling removed - stick to NSE/NFO/BSE only

                        # Base symbol fallbacks
                        _add_candidate(canonical_base_symbol)
                        _add_candidate(base_symbol)
                        _add_candidate(normalized_base_symbol)
                        _add_candidate(f"NFO:{base_symbol}")
                        _add_candidate(f"NSE:{base_symbol}")
                        _add_candidate(f"BSE:{base_symbol}")
                        _add_candidate(f"NFO{normalized_base_symbol}")
                        # ✅ DISABLED: MCX/CDS prefix handling removed - stick to NSE/NFO/BSE only

                        # Build key variants using canonical format
                        # Use DatabaseAwareKeyBuilder for OHLC daily (DB1)
                        zset_key_variants = [DatabaseAwareKeyBuilder.live_ohlc_daily(candidate) for candidate in symbol_candidates if candidate]
                        
                        logger.debug(f"🔍 [HISTORICAL_DEBUG] {symbol} - Trying OHLC keys (full symbol first, then base): {zset_key_variants}")
                        
                        entries = None
                        zset_key_used = None
                        for zset_key in zset_key_variants:
                            logger.debug(f"🔍 [HISTORICAL_DEBUG] {symbol} - Trying ohlc_daily sorted set: {zset_key}")
                            try:
                                entries = redis_db1.zrevrange(zset_key, 0, 54, withscores=True)
                                if entries:
                                    zset_key_used = zset_key
                                    break
                            except Exception as e:
                                logger.debug(f"⚠️ [HISTORICAL_DEBUG] {symbol} - Error reading {zset_key}: {e}")
                                continue
                        
                        if entries:
                            historical_buckets = []
                            for payload, timestamp_score in reversed(entries):
                                try:
                                    if isinstance(payload, bytes):
                                        payload = payload.decode('utf-8')
                                    ohlc_data = json.loads(payload) if isinstance(payload, str) else payload
                                    # ✅ CRITICAL: Use _safe_float and _safe_int to handle string values from Redis
                                    timestamp_val = timestamp_score
                                    if isinstance(timestamp_val, str):
                                        try:
                                            timestamp_val = float(timestamp_val)
                                        except (ValueError, TypeError):
                                            timestamp_val = 0
                                    # ✅ FIXED: Correct epoch length detection (same as intraday_crawler)
                                    if timestamp_val >= 1e15:
                                        # Nanoseconds - convert to milliseconds
                                        timestamp_ms = int(timestamp_val / 1_000_000)
                                    elif timestamp_val >= 1e10:
                                        # Already in milliseconds
                                        timestamp_ms = int(timestamp_val)
                                    else:
                                        # Seconds - convert to milliseconds
                                        timestamp_ms = int(timestamp_val * 1000)
                                    
                                    bucket = {
                                        'close': self._safe_float(ohlc_data.get('c', ohlc_data.get('close', 0.0))),
                                        'last_price': self._safe_float(ohlc_data.get('c', ohlc_data.get('close', 0.0))),
                                        'high': self._safe_float(ohlc_data.get('h', ohlc_data.get('high', 0.0))),
                                        'low': self._safe_float(ohlc_data.get('l', ohlc_data.get('low', 0.0))),
                                        'open': self._safe_float(ohlc_data.get('o', ohlc_data.get('open', 0.0))),
                                        'average_price': self._safe_float(ohlc_data.get('c', ohlc_data.get('close', 0.0))),  # ✅ ADDED: Average price
                                        'bucket_incremental_volume': self._safe_int(ohlc_data.get('v', ohlc_data.get('volume', 0))),
                                        'bucket_cumulative_volume': self._safe_int(ohlc_data.get('v', ohlc_data.get('volume', 0))),
                                        'zerodha_cumulative_volume': self._safe_int(ohlc_data.get('v', ohlc_data.get('volume', 0))),
                                        'zerodha_last_traded_quantity': 0,  # ✅ ADDED: Required field
                                        'volume_ratio': 1.0,
                                        'volume': self._safe_int(ohlc_data.get('v', ohlc_data.get('volume', 0))),
                                        'timestamp': self._safe_int(timestamp_ms),
                                        'last_timestamp': ohlc_data.get('d', ''),
                                        'date': ohlc_data.get('d', '')
                                    }
                                    historical_buckets.append(bucket)
                                except Exception as e:
                                    logger.debug(f"⚠️ [HISTORICAL_DEBUG] {symbol} - Error parsing OHLC entry: {e}")
                                    continue
                            
                            if historical_buckets:
                                logger.debug(f"✅ [HISTORICAL_DEBUG] {symbol} - Got {len(historical_buckets)} historical OHLC buckets from DB1 ({zset_key_used})")
            except Exception as e:
                logger.debug(f"⚠️ [HISTORICAL_DEBUG] {symbol} - DB1 lookup failed: {e}")
            
            # ✅ PRIORITY 2: Use session-based bucket retrieval via get_rolling_window_buckets()
            # ✅ SESSION-BASED: Buckets are stored in DB1 as session-based history lists
            # History lists: bucket_incremental_volume:history:{resolution}:{symbol}
            # Uses CumulativeDataTracker.get_time_buckets() internally for proper session-based retrieval
            if not historical_buckets and redis_client and hasattr(redis_client, 'get_rolling_window_buckets'):
                logger.debug(f"🔍 [HISTORICAL_DEBUG] {symbol} - Trying DB1 via session-based get_rolling_window_buckets()")
                try:
                    # ✅ SESSION-BASED: get_rolling_window_buckets() now uses CumulativeDataTracker
                    # which properly handles session-based bucket storage in DB1
                    historical_buckets = redis_client.get_rolling_window_buckets(
                        symbol, 
                        lookback_minutes=60, 
                        session_date=None,  # Uses current session
                        max_days_back=None
                    )
                    # ✅ FIX: Handle DataFrame return type (convert to list if needed)
                    if historical_buckets is not None:
                        # Check if it's a Polars DataFrame
                        try:
                            import polars as pl
                            if isinstance(historical_buckets, pl.DataFrame):
                                # Convert DataFrame to list of dicts
                                historical_buckets = historical_buckets.to_dicts()
                                logger.debug(f"✅ [HISTORICAL_DEBUG] {symbol} - Converted DataFrame to {len(historical_buckets)} buckets")
                        except (ImportError, AttributeError):
                            pass
                        
                        if historical_buckets:
                            bucket_count = len(historical_buckets) if hasattr(historical_buckets, '__len__') else 'unknown'
                            logger.debug(f"✅ [HISTORICAL_DEBUG] {symbol} - Got {bucket_count} buckets from session-based retrieval (DB1 history lists)")
                except Exception as e:
                    logger.debug(f"⚠️ [HISTORICAL_DEBUG] {symbol} - Session-based get_rolling_window_buckets() failed: {e}")
            
            # ✅ REMOVED: DB0 fallback - migration complete, all data in DB1
            
            # ✅ FIX: Handle DataFrame return type for final check
            has_historical_data = False
            if historical_buckets is not None:
                try:
                    import polars as pl
                    if isinstance(historical_buckets, pl.DataFrame):
                        has_historical_data = not historical_buckets.is_empty()
                    else:
                        has_historical_data = len(historical_buckets) > 0 if hasattr(historical_buckets, '__len__') else bool(historical_buckets)
                except (ImportError, AttributeError):
                    has_historical_data = bool(historical_buckets) if historical_buckets is not None else False
            
            if not has_historical_data:
                # ✅ FIXED: Options may not have 55 days of historical data (they expire), so use debug level
                # Non-options should have historical data, so use warning level
                is_option = self._is_option_symbol(symbol)
                if is_option:
                    logger.debug(f"🔍 [HISTORICAL_DEBUG] {symbol} - NO HISTORICAL BUCKETS FOUND (checked DB1 ohlc_daily sorted set) - Options may not have 55-day OHLC baseline (expected for expiring instruments)")
                else:
                    logger.warning(f"⚠️ [HISTORICAL_DEBUG] {symbol} - NO HISTORICAL BUCKETS FOUND (checked DB1 ohlc_daily sorted set) - 55-day OHLC baseline indicators will NOT be available")
                return None
            
            logger.debug(f"🔍 [HISTORICAL_DEBUG] {symbol} - Got {len(historical_buckets)} buckets, processing...")
            # Convert bucket data to Polars DataFrame
            bucket_data = []
            for bucket in historical_buckets:
                try:
                    # ✅ FIXED: Use TimestampNormalizer (central logic) instead of manual parsing
                    from shared_core.config_utils.timestamp_normalizer import TimestampNormalizer
                    
                    # Parse timestamp properly - handle both integer (from ohlc_daily sorted set) and string (from session data)
                    timestamp_ms = 0
                    timestamp_val = bucket.get('timestamp')
                    
                    if timestamp_val is not None:
                        if isinstance(timestamp_val, (int, float)):
                            # ✅ FIXED: Correct epoch length detection (same as intraday_crawler)
                            if timestamp_val >= 1e15:
                                # Nanoseconds - convert to milliseconds
                                timestamp_ms = int(timestamp_val / 1_000_000)
                            elif timestamp_val >= 1e10:
                                # Already in milliseconds
                                timestamp_ms = int(timestamp_val)
                            else:
                                # Seconds - convert to milliseconds
                                timestamp_ms = int(timestamp_val * 1000)
                        elif isinstance(timestamp_val, str):
                            # String timestamp - use TimestampNormalizer
                            try:
                                timestamp_ms = TimestampNormalizer.to_epoch_ms(timestamp_val)
                            except Exception:
                                timestamp_ms = 0
                    
                    # If timestamp is still 0, try last_timestamp or date field
                    if timestamp_ms == 0:
                        timestamp_str = bucket.get('last_timestamp') or bucket.get('date', '')
                        if timestamp_str:
                            try:
                                timestamp_ms = TimestampNormalizer.to_epoch_ms(timestamp_str)
                            except Exception:
                                timestamp_ms = 0
                    
                    # ✅ FIXED: Map bucket data from session data structure per REDIS_STORAGE_SIGNATURE.md
                    # Session buckets have: close, bucket_incremental_volume, count, last_timestamp, hour, minute_bucket
                    # OHLC daily buckets have: close, high, low, open, volume, timestamp (int), date (string)
                    last_price = bucket.get('close') or bucket.get('last_price', 0.0)
                    # ✅ FIXED: Use actual high/low from OHLC data if available, otherwise use close as fallback
                    high_price = bucket.get('high', last_price)
                    low_price = bucket.get('low', last_price)
                    bucket_incremental = bucket.get('bucket_incremental_volume', 0)
                    bucket_cumulative = bucket.get('bucket_cumulative_volume') or bucket.get('zerodha_cumulative_volume', 0)
                    
                    # ✅ FIX: Extract volume_ratio from bucket if available, otherwise use 0.0
                    # Use 0.0 instead of 1.0 to avoid false positives in batch processing
                    bucket_volume_ratio = bucket.get('volume_ratio', 0.0)
                    if not bucket_volume_ratio or bucket_volume_ratio == 0.0:
                        bucket_volume_ratio = 0.0
                    
                    # ✅ CRITICAL: Use _safe_float and _safe_int to handle string values from Redis
                    bucket_data.append({
                        'timestamp': self._safe_int(timestamp_ms),
                        'last_price': self._safe_float(last_price),
                        'zerodha_cumulative_volume': self._safe_int(bucket_cumulative),
                        'zerodha_last_traded_quantity': self._safe_int(bucket.get('count', 0)),  # Use count as quantity
                        'high': self._safe_float(high_price),  # ✅ FIXED: Use actual high from OHLC data
                        'low': self._safe_float(low_price),   # ✅ FIXED: Use actual low from OHLC data
                        'open': self._safe_float(bucket.get('open', last_price)),  # ✅ ADDED: Open price
                        'close': self._safe_float(bucket.get('close', last_price)),  # ✅ ADDED: Close price
                        'average_price': self._safe_float(bucket.get('average_price', last_price)),  # ✅ ADDED: Average price
                        'bucket_incremental_volume': self._safe_int(bucket_incremental),
                        'bucket_cumulative_volume': self._safe_int(bucket_cumulative),
                        'volume_ratio': self._safe_float(bucket_volume_ratio),  # 🆕 CRITICAL: Include volume_ratio
                        'volume': self._safe_int(bucket_incremental)  # 🆕 Raw volume for flexibility
                    })
                except Exception as e:
                    continue
            
            if bucket_data:
                logger.debug(f"🔍 [HISTORICAL_DEBUG] {symbol} - Created DataFrame with {len(bucket_data)} rows")
                # ✅ FIXED: Use explicit schema to ensure timestamp is Int64 (matches new_tick schema)
                # ✅ FIXED: Ensure all required columns are present (bucket_incremental_volume might be missing)
                historical_df = pl.DataFrame(bucket_data, schema={
                    'timestamp': pl.Int64,  # Must match new_tick schema
                    'last_price': pl.Float64,
                    'zerodha_cumulative_volume': pl.Int64,
                    'zerodha_last_traded_quantity': pl.Int64,
                    'high': pl.Float64,
                    'low': pl.Float64,
                    'open': pl.Float64,  # ✅ ADDED: Open price
                    'close': pl.Float64,  # ✅ ADDED: Close price
                    'average_price': pl.Float64,  # ✅ ADDED: Average price
                    'bucket_incremental_volume': pl.Int64,
                    'bucket_cumulative_volume': pl.Int64,
                    'volume_ratio': pl.Float64,  # 🆕 CRITICAL: Volume ratio for indicators
                    'volume': pl.Int64  # 🆕 Raw volume for flexibility
                })
                # ✅ FIXED: Ensure bucket_incremental_volume and bucket_cumulative_volume exist (defensive)
                # They should be in the schema, but if bucket_data doesn't have them, add with 0
                required_cols = ['bucket_incremental_volume', 'bucket_cumulative_volume']
                for col in required_cols:
                    if col not in historical_df.columns:
                        historical_df = historical_df.with_columns(pl.lit(0).cast(pl.Int64).alias(col))
                return historical_df
            
            logger.debug(f"🔍 [HISTORICAL_DEBUG] {symbol} - No valid bucket data after processing")
            return None
            
        except Exception as e:
            logger.error(f"Error getting historical bucket data for {symbol}: {e}")
            return None
    
    def _get_buckets_from_db2(self, redis_client, symbol: str, lookback_minutes: int = 60) -> List[Dict[str, Any]]:
        """⚠️ DEPRECATED: Get real-time bucket2 keys from DB 2 (analytics)
        
        ❌ REMOVED: Per REDIS_STORAGE_SIGNATURE.md, individual bucket hash keys are NOT created in Redis.
        Buckets are stored in session data (DB 0): session:{symbol}:{date} -> time_buckets
        
        ❌ REMOVED: This method used SCAN which is FORBIDDEN per redis_key_standards.py.
        Should use RobustRedisClient.get_rolling_window_buckets() instead.
        
        This method is kept for backward compatibility but returns empty list.
        Use RobustRedisClient.get_rolling_window_buckets() for correct bucket retrieval.
        """
        # ✅ FIXED: Per REDIS_STORAGE_SIGNATURE.md, bucket2 keys don't exist
        # Buckets are stored in session data (DB 0), not as individual hash keys
        # Use session-based bucket retrieval via CumulativeDataTracker.get_time_buckets() instead
        logger.debug(f"⚠️ [HISTORICAL_DEBUG] {symbol} - _get_buckets_from_db2() is deprecated, use CumulativeDataTracker.get_time_buckets()")
        return []
    
    def _get_buckets_from_session_data(self, redis_client, symbol: str, lookback_minutes: int = 60) -> List[Dict[str, Any]]:
        """⚠️ DEPRECATED: Get buckets from session data (DB 0) - migration complete, all data in DB1
        
        This method is no longer used. All bucket data is now in DB1:
        - bucket_incremental_volume:history:{resolution}:{symbol} (history lists)
        - ohlc_daily:{symbol} (sorted set with historical OHLC data)
        
        Format: session:{symbol}:{date} -> time_buckets (nested JSON with prices/volumes)
        """
        try:
            import pytz
            import json
            
            ist = pytz.timezone("Asia/Kolkata")
            now = datetime.now(ist)
            cutoff_ts = (now - timedelta(minutes=lookback_minutes)).timestamp()
            
            # Try current date and recent dates
            dates_to_try = [
                now.strftime("%Y-%m-%d"),  # Today
                (now - timedelta(days=1)).strftime("%Y-%m-%d"),  # Yesterday
                (now - timedelta(days=2)).strftime("%Y-%m-%d"),  # Day before
            ]
            
            # Also try symbol variants (original, underlying)
            symbols_to_try = [symbol]
            # Try to extract underlying symbol
            underlying_symbol = symbol
            if '25NOV' in symbol or '25DEC' in symbol or '25JAN' in symbol:
                underlying_symbol = symbol.replace('25NOV', '').replace('25DEC', '').replace('25JAN', '')
                underlying_symbol = underlying_symbol.replace('CE', '').replace('PE', '').replace('FUT', '')
                if underlying_symbol and underlying_symbol != symbol:
                    symbols_to_try.append(underlying_symbol)
            
            # Get DB 0 client for session data
            session_client = None
            if hasattr(redis_client, 'get_client'):
                session_client = redis_client.get_client(0)
            elif hasattr(redis_client, 'redis_client'):
                session_client = redis_client.redis_client
                # Ensure we're on DB 0
                if hasattr(session_client, 'select'):
                    session_client.select(0)
            else:
                session_client = redis_client
            
            buckets_data = []
            
            # Try session data first (where buckets are actually stored)
            for try_symbol in symbols_to_try:
                for date_str in dates_to_try:
                    from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
                    session_key = DatabaseAwareKeyBuilder.live_session(try_symbol, date_str)
                    try:
                        session_data_str = session_client.get(session_key)
                        if session_data_str:
                            if isinstance(session_data_str, bytes):
                                session_data_str = session_data_str.decode('utf-8')
                            session_data = json.loads(session_data_str) if isinstance(session_data_str, str) else session_data_str
                            time_buckets = session_data.get('time_buckets', {})
                            
                            if time_buckets:
                                logger.debug(f"✅ [HISTORICAL_DEBUG] {symbol} -> Found {len(time_buckets)} buckets in {session_key}")
                                # Convert time_buckets to list of bucket data
                                for bucket_key, bucket_data in time_buckets.items():
                                    if isinstance(bucket_data, dict):
                                        # Extract timestamp from bucket - use TimestampNormalizer (central logic)
                                        last_ts_str = bucket_data.get('last_timestamp', '')
                                        if last_ts_str:
                                            try:
                                                from shared_core.config_utils.timestamp_normalizer import TimestampNormalizer
                                                bucket_ts_ms = TimestampNormalizer.to_epoch_ms(last_ts_str)
                                                bucket_ts = bucket_ts_ms / 1000.0  # Convert to seconds for comparison
                                                if bucket_ts >= cutoff_ts:
                                                    buckets_data.append(bucket_data)
                                            except Exception:
                                                pass
                                        else:
                                            # Include bucket even without timestamp (within date range)
                                            buckets_data.append(bucket_data)
                                if buckets_data:
                                    break  # Found buckets, no need to check other dates
                    except Exception as e:
                        continue
                
                if buckets_data:
                    break  # Found buckets, no need to check other symbols
            
            if buckets_data:
                logger.debug(f"✅ [HISTORICAL_DEBUG] {symbol} -> Retrieved {len(buckets_data)} buckets from session data")
                return buckets_data
            
            return []
            
        except Exception as e:
            logger.error(f"Error getting buckets from session data for {symbol}: {e}")
            return []
    
    def _calculate_realtime_indicators(self, symbol: str, window, tick_data: Dict) -> Dict:
        """Calculate real-time indicators using session-based window with Redis fallback
        
        ✅ OPTIMIZED: Uses Polars DataFrame directly (no list conversion)
        - Keeps data in Polars format for maximum performance
        - Only extracts final scalar values
        - Leverages Polars lazy evaluation and parallel processing
        """
        try:
            if POLARS_AVAILABLE:
                window = self._normalize_window_schema(window)
            if window.height < 20:
                # ✅ FIXED: Fallback to batch_calculate_indicators if insufficient window data
                # This ensures indicators are still calculated even with limited data
                logger.debug(f"Insufficient window data for {symbol} (height={window.height}), using fallback calculation")
                tick_list = [tick_data]
                fallback_indicators = self.batch_calculate_indicators(tick_list, symbol)
                if fallback_indicators:
                    return fallback_indicators
                # If fallback also fails, return minimal indicators + microstructure context
                fallback_payload = {
                    'symbol': symbol,
                    'last_price': tick_data.get('last_price', 0.0),
                    'timestamp': tick_data.get('exchange_timestamp_ms', tick_data.get('timestamp', 0)),
                    'volume_ratio': _extract_volume_ratio(tick_data),
                    'price_change': 0.0
                }
                microstructure_metrics = self._calculate_microstructure_metrics(symbol, tick_data)
                if microstructure_metrics:
                    fallback_payload.update(microstructure_metrics)
                return fallback_payload
            
            # ✅ OPTIMIZED: Keep data in Polars DataFrame format (no list conversion)
            # Only extract scalar values when needed (not entire lists)
            # ✅ FIXED: Ensure required columns exist before accessing (defensive check)
            if not POLARS_AVAILABLE:
                # Fallback to list conversion if Polars not available
                prices = window['last_price'].to_list() if 'last_price' in window.columns else []
                volumes = window['zerodha_cumulative_volume'].to_list() if 'zerodha_cumulative_volume' in window.columns else [0] * len(prices)
                highs = window['high'].to_list() if 'high' in window.columns else prices.copy()
                lows = window['low'].to_list() if 'low' in window.columns else prices.copy()
            else:
                # Keep as Polars Series for efficient operations
                # ✅ FIXED: Ensure required columns exist before accessing (defensive check)
                prices_series = window['last_price'] if 'last_price' in window.columns else pl.Series('last_price', [0.0] * window.height)
                volumes_series = window['zerodha_cumulative_volume'] if 'zerodha_cumulative_volume' in window.columns else pl.Series('zerodha_cumulative_volume', [0] * window.height)
                highs_series = window['high'] if 'high' in window.columns else prices_series
                lows_series = window['low'] if 'low' in window.columns else prices_series
                # Only convert to list when absolutely necessary (for methods that don't support Polars)
                prices = prices_series.to_list()
                volumes = volumes_series.to_list()
                highs = highs_series.to_list()
                lows = lows_series.to_list()
            
            # Get Redis client if available
            redis_client = getattr(self, 'redis_client', None)
            
            # ✅ FAST EMA CALCULATIONS: Use ultra-fast EMA methods for all windows
            # Get real price from Redis if available, fallback to tick_data
            current_price = tick_data.get('last_price', 0.0)
            if redis_client and hasattr(redis_client, 'get_last_price'):
                redis_price = redis_client.get_last_price(symbol)
                if redis_price and redis_price > 0:
                    current_price = redis_price
            fast_emas = self.get_all_emas_fast(symbol, current_price)
            
            # ✅ SINGLE SOURCE OF TRUTH: Read pre-computed volume ratio from tick_data
            # Volume ratio is calculated by websocket_parser (central source of truth)
            # NEVER recalculate - if missing, return 0.0 (do NOT override volume manager)
            volume_ratio = _extract_volume_ratio(tick_data)

            # ✅ OPTIMIZED: Use Polars directly for calculations when available
            if POLARS_AVAILABLE:
                # Calculate RSI using Polars directly on window DataFrame (tries TA-Lib first internally)
                rsi_value = self._calculate_rsi_polars(window, 14, symbol)
                # Calculate ATR using Polars directly on window DataFrame (tries TA-Lib first internally)
                atr_value = self._calculate_atr_polars(window, 14, symbol)
                # Calculate VWAP using Polars directly on window DataFrame
                vwap_value = self._calculate_vwap_polars(window, symbol, redis_client)
                z_score_value = self._calculate_zscore_polars(window, self._session_zscore_lookback, symbol)
            else:
                # Fallback to list-based calculations
                rsi_value = self.calculate_rsi(prices, 14, symbol)
                atr_value = self.calculate_atr(highs, lows, prices, 14, symbol)
                vwap_value = calculate_vwap_with_redis_fallback([
                    {'last_price': p, 'zerodha_cumulative_volume': v} 
                    for p, v in zip(prices, volumes)
                ], symbol, redis_client)
                z_score_value = self._calculate_zscore_list(prices, self._session_zscore_lookback, symbol)

            # Calculate indicators using TA-Lib (preferred) with Redis fallback
            indicators = {
                'symbol': symbol,
                'timestamp': tick_data.get('exchange_timestamp_ms', tick_data.get('timestamp', 0)),
                'last_price': current_price,
                'price_change': self.calculate_price_change(
                    current_price,
                    prices[-2] if len(prices) >= 2 else prices[-1]
                ),
                # ✅ FAST EMAs: All windows calculated ultra-fast
                'ema_5': fast_emas['ema_5'],
                'ema_10': fast_emas['ema_10'],
                'ema_20': fast_emas['ema_20'],
                'ema_50': fast_emas['ema_50'],
                'ema_100': fast_emas['ema_100'],
                'ema_200': fast_emas['ema_200'],
                'ema_crossover': {
                    'ema_5': fast_emas['ema_5'],
                    'ema_10': fast_emas['ema_10'],
                    'ema_20': fast_emas['ema_20'],
                    'ema_50': fast_emas['ema_50'],
                    'ema_100': fast_emas['ema_100'],
                    'ema_200': fast_emas['ema_200']
                },
                # ✅ OPTIMIZED: Polars-based calculations
                'rsi': rsi_value,
                'atr': atr_value,
                'vwap': vwap_value,
                'volume_ratio': volume_ratio,
                'z_score': z_score_value
            }
            
            # Add MACD and Bollinger Bands if enough data
            # ✅ FIX: MACD only needs 26 data points (slow_period), Bollinger Bands needs 20 (period)
            # Use the minimum required: max(26 for MACD, 20 for BB) = 26
            if window.height >= 26:
                if POLARS_AVAILABLE:
                    # Use Polars directly for MACD and Bollinger Bands (tries TA-Lib first internally)
                    macd = self._calculate_macd_polars(window, 12, 26, 9, symbol)
                    # Bollinger Bands needs 20 data points
                    if window.height >= 20:
                        bb = self._calculate_bollinger_bands_polars(window, 20, 2.0, symbol)
                        indicators.update(bb)
                else:
                    # Fallback to list-based calculations
                    macd = self.calculate_macd(prices, 12, 26, 9, symbol)
                    # Bollinger Bands needs 20 data points
                    if len(prices) >= 20:
                        bb = self.calculate_bollinger_bands(prices, 20, 2.0, symbol)
                        indicators.update(bb)
                
                indicators.update(macd)
            
            # 🎯 ADD GREEK CALCULATIONS FOR F&O OPTIONS ONLY
            # ✅ CRITICAL FIX: Only calculate and add Greeks for option symbols
            # Equity symbols should NOT have Greeks in indicators dict
            if self._is_option_symbol(symbol) and GREEK_CALCULATIONS_AVAILABLE:
                try:
                    # ✅ FIX: Extract IV from tick data and add to indicators dict (required by patterns)
                    iv = tick_data.get('iv') or tick_data.get('implied_volatility') or tick_data.get('volatility')
                    if iv:
                        try:
                            iv = float(iv)
                            if iv <= 0 or iv > 5.0:  # Sanity check: IV should be between 0 and 500% (5.0)
                                iv = 0.2  # Default fallback
                        except (ValueError, TypeError):
                            iv = 0.2
                    else:
                        iv = 0.2  # Default IV if not available
                    
                    # Add IV to indicators dict (required by patterns)
                    indicators['implied_volatility'] = iv
                    
                    greeks = self.calculate_greeks_for_tick_data(tick_data)
                    if greeks:
                        # ✅ FIX: Always add Greeks to indicators dict, even if they're 0
                        # Patterns need Greeks to be present (even if 0) for validation
                        indicators.update({
                            'delta': greeks.get('delta', 0.0),
                            'gamma': greeks.get('gamma', 0.0),
                            'theta': greeks.get('theta', 0.0),
                            'vega': greeks.get('vega', 0.0),
                            'rho': greeks.get('rho', 0.0),
                            'dte_years': greeks.get('dte_years', 0.0),
                            'trading_dte': greeks.get('trading_dte', 0),
                            'expiry_series': greeks.get('expiry_series', 'UNKNOWN'),
                            'option_price': greeks.get('option_price', 0.0)
                        })
                        # Log if all Greeks are zero (for debugging)
                        if not any(greeks.get(greek, 0) != 0 for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']):
                            logger.debug(f"⚠️ [GREEKS_DEBUG] {symbol} All Greeks are zero but added to indicators")
                    else:
                        logger.debug(f"⚠️ [GREEKS_DEBUG] {symbol} Greek calculation returned None/empty")
                        # ✅ FIX: Add zero Greeks if calculation returned None
                        indicators.update({
                            'delta': 0.0,
                            'gamma': 0.0,
                            'theta': 0.0,
                            'vega': 0.0,
                            'rho': 0.0,
                            'dte_years': 0.0,
                            'trading_dte': 0,
                            'expiry_series': 'UNKNOWN',
                            'option_price': 0.0
                        })
                except Exception as e:
                    logger.warning(f"⚠️ [GREEKS_DEBUG] {symbol} Error calculating Greeks in _calculate_realtime_indicators: {e}")
                    import traceback
                    logger.debug(f"🔍 [GREEKS_DEBUG] {symbol} Traceback: {traceback.format_exc()}")
                    # ✅ FIX: Add zero Greeks on error
                    indicators.update({
                        'delta': 0.0,
                        'gamma': 0.0,
                        'theta': 0.0,
                        'vega': 0.0,
                        'rho': 0.0,
                        'dte_years': 0.0,
                        'trading_dte': 0,
                        'expiry_series': 'UNKNOWN',
                        'option_price': 0.0
                    })
            else:
                # ✅ FIX: Always add zero Greeks for non-option symbols so they're present in indicators dict
                indicators.update({
                    'delta': 0.0,
                    'gamma': 0.0,
                    'theta': 0.0,
                    'vega': 0.0,
                    'rho': 0.0,
                    'dte_years': 0.0,
                    'trading_dte': 0,
                    'expiry_series': 'UNKNOWN',
                    'option_price': 0.0
                })
                logger.debug(f"🔍 [GREEKS_DEBUG] {symbol} Not an option symbol - added zero Greeks")

            advanced_greeks = self._calculate_advanced_greeks(tick_data, indicators)
            if advanced_greeks:
                indicators.update(advanced_greeks)
            if self._is_option_symbol(symbol):
                gamma_exposure = self._calculate_gamma_exposure(symbol)
                if gamma_exposure:
                    indicators['gamma_exposure'] = gamma_exposure

            microstructure_metrics = self._calculate_microstructure_metrics(symbol, tick_data)
            if microstructure_metrics:
                indicators.update(microstructure_metrics)

            cross_metrics = self._calculate_cross_asset_metrics(symbol, current_price)
            if cross_metrics:
                indicators.update(cross_metrics)

            return indicators
            
        except Exception as e:
            logger.error(f"Error calculating real-time indicators for {symbol}: {e}")
            return {}
    
    def _calculate_rsi_polars(self, window: "pl.DataFrame", period: int = 14, symbol: str = None) -> float:
        """✅ OPTIMIZED: Calculate RSI using Polars DataFrame directly
        
        ✅ CONSISTENT: Maintains same priority as calculate_rsi():
        1. TA-Lib (preferred - fastest and most accurate)
        2. Polars (fallback)
        3. Simple fallback
        """
        try:
            if window.height < period + 1:
                return 50.0
            
            # ✅ CONSISTENT: Try TA-Lib first (same priority as calculate_rsi())
            if TALIB_AVAILABLE:
                try:
                    prices = window['last_price'].to_list()
                    np_prices = np.array(prices, dtype=np.float64)
                    logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} _calculate_rsi_polars attempting TA-Lib: prices_shape={np_prices.shape}, period={period}")
                    rsi = talib.RSI(np_prices, timeperiod=period)
                    result = float(rsi[-1]) if len(rsi) > 0 and not np.isnan(rsi[-1]) else 50.0
                    if result > 0:
                        # TA-Lib RSI calculation successful
                        return result
                except Exception as e:
                    logger.warning(f"❌ [TA_LIB_DEBUG] {symbol} TA-Lib RSI failed in _calculate_rsi_polars, using Polars: {e}")
                    import traceback
                    logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} _calculate_rsi_polars TA-Lib traceback: {traceback.format_exc()}")
            
            # ✅ FALLBACK: Use Polars expressions for RSI calculation
            rsi_df = window.with_columns([
                pl.col('last_price').diff().alias('delta')
            ]).with_columns([
                pl.when(pl.col('delta') > 0).then(pl.col('delta')).otherwise(0).alias('gain'),
                pl.when(pl.col('delta') < 0).then(-pl.col('delta')).otherwise(0).alias('loss')
            ]).with_columns([
                pl.col('gain').rolling_mean(window_size=period).alias('avg_gain'),
                pl.col('loss').rolling_mean(window_size=period).alias('avg_loss')
            ]).with_columns([
                (100 - (100 / (1 + (pl.col('avg_gain') / (pl.col('avg_loss') + 1e-10))))).alias('rsi')
            ])
            
            rsi_value = rsi_df['rsi'].tail(1)[0]
            return float(rsi_value) if rsi_value is not None and not (isinstance(rsi_value, float) and (rsi_value != rsi_value)) else 50.0
        except Exception as e:
            logger.debug(f"Polars RSI calculation failed, using fallback: {e}")
            # Fallback to list-based calculation (which will try TA-Lib again)
            prices = window['last_price'].to_list()
            return self.calculate_rsi(prices, period, symbol)
    
    def _calculate_atr_polars(self, window: "pl.DataFrame", period: int = 14, symbol: str = None) -> float:
        """✅ OPTIMIZED: Calculate ATR using Polars DataFrame directly
        
        ✅ CONSISTENT: Maintains same priority as calculate_atr():
        1. TA-Lib (preferred - fastest and most accurate)
        2. Polars (fallback)
        3. pandas_ta (fallback 2)
        4. Simple fallback
        """
        try:
            if window.height < period:
                return 0.0
            
            # ✅ CONSISTENT: Try TA-Lib first (same priority as calculate_atr())
            if TALIB_AVAILABLE:
                try:
                    highs = window['high'].to_list()
                    lows = window['low'].to_list()
                    closes = window['last_price'].to_list()
                    
                    np_highs = np.array(highs, dtype=np.float64)
                    np_lows = np.array(lows, dtype=np.float64)
                    np_closes = np.array(closes, dtype=np.float64)
                    
                    logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} _calculate_atr_polars attempting TA-Lib: shapes=({np_highs.shape}, {np_lows.shape}, {np_closes.shape}), period={period}")
                    atr = talib.ATR(np_highs, np_lows, np_closes, timeperiod=period)
                    result = float(atr[-1]) if len(atr) > 0 and not np.isnan(atr[-1]) else 0.0
                    if result > 0:
                        # TA-Lib ATR calculation successful
                        return result
                except Exception as e:
                    logger.warning(f"❌ [TA_LIB_DEBUG] {symbol} TA-Lib ATR failed in _calculate_atr_polars, using Polars: {e}")
                    import traceback
                    logger.debug(f"🔍 [TA_LIB_DEBUG] {symbol} _calculate_atr_polars TA-Lib traceback: {traceback.format_exc()}")
            
            # ✅ FALLBACK: Use Polars expressions for ATR calculation
            atr_df = window.with_columns([
                (pl.col('high') - pl.col('low')).alias('tr1'),
                (pl.col('high') - pl.col('last_price').shift(1)).abs().alias('tr2'),
                (pl.col('low') - pl.col('last_price').shift(1)).abs().alias('tr3')
            ]).with_columns([
                pl.max_horizontal('tr1', 'tr2', 'tr3').alias('tr')
            ]).with_columns([
                pl.col('tr').rolling_mean(window_size=period).alias('atr')
            ])
            
            atr_value = atr_df['atr'].tail(1)[0]
            return float(atr_value) if atr_value is not None and atr_value > 0 else 0.0
        except Exception as e:
            logger.debug(f"Polars ATR calculation failed, using fallback: {e}")
            # Fallback to list-based calculation (which will try TA-Lib again)
            prices = window['last_price'].to_list()
            highs = window['high'].to_list()
            lows = window['low'].to_list()
            return self.calculate_atr(highs, lows, prices, period, symbol)
    
    def _calculate_vwap_polars(self, window: "pl.DataFrame", symbol: str, redis_client=None) -> float:
        """✅ OPTIMIZED: Calculate VWAP using Polars DataFrame directly"""
        try:
            if window.height == 0:
                return 0.0
            
            # Use Polars expressions for VWAP calculation
            # Note: cumsum() renamed to cum_sum() in Polars 0.19+
            vwap_df = window.with_columns([
                (pl.col('last_price') * pl.col('zerodha_cumulative_volume')).alias('price_volume')
            ]).with_columns([
                pl.col('price_volume').cum_sum().alias('cumulative_pv'),
                pl.col('zerodha_cumulative_volume').cum_sum().alias('cumulative_volume')
            ]).with_columns([
                (pl.col('cumulative_pv') / (pl.col('cumulative_volume') + 1e-10)).alias('vwap')
            ])
            
            vwap_value = vwap_df['vwap'].tail(1)[0]
            return float(vwap_value) if vwap_value is not None and vwap_value > 0 else 0.0
        except Exception as e:
            logger.debug(f"Polars VWAP calculation failed, using fallback: {e}")
            # Fallback to list-based calculation
            prices = window['last_price'].to_list()
            volumes = window['zerodha_cumulative_volume'].to_list()
            return calculate_vwap_with_redis_fallback([
                {'last_price': p, 'zerodha_cumulative_volume': v} 
                for p, v in zip(prices, volumes)
            ], symbol, redis_client)

    def _calculate_zscore_polars(self, window: "pl.DataFrame", lookback: int = 20, symbol: str = None) -> float:
        """Calculate rolling z-score using Polars expressions (preferred path)."""
        if not POLARS_AVAILABLE or lookback <= 1:
            return 0.0
        try:
            if window.is_empty() or window.height < lookback or 'last_price' not in window.columns:
                return 0.0

            mean_expr = pl.col("last_price").rolling_mean(window_size=lookback)
            std_expr = pl.col("last_price").rolling_std(window_size=lookback, ddof=0)

            z_expr = (
                pl.when(std_expr == 0)
                .then(0.0)
                .otherwise((pl.col("last_price") - mean_expr) / std_expr)
                .alias("z_internal")
            )

            z_df = window.select(z_expr)
            value = z_df['z_internal'].to_list()[-1]
            if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
                return 0.0
            return float(value)
        except Exception as exc:
            logger.debug(f"⚠️ [ZSCORE_DEBUG] {symbol} - Polars z-score failed: {exc}")
            return 0.0

    def _calculate_zscore_list(self, prices: List[float], lookback: int = 20, symbol: str = None) -> float:
        """Fallback z-score calculation using pure Python lists/statistics."""
        if lookback <= 1 or not prices or len(prices) < lookback:
            return 0.0
        try:
            recent = prices[-lookback:]
            mean_val = statistics.fmean(recent)
            std_val = statistics.pstdev(recent)
            if std_val == 0:
                return 0.0
            return (recent[-1] - mean_val) / std_val
        except Exception as exc:
            logger.debug(f"⚠️ [ZSCORE_DEBUG] {symbol} - List z-score failed: {exc}")
            return 0.0
    
    def _calculate_macd_polars(self, window: "pl.DataFrame", fast_period: int = 12, 
                               slow_period: int = 26, signal_period: int = 9, symbol: str = None) -> Dict:
        """✅ OPTIMIZED: Calculate MACD using Polars DataFrame directly
        
        ✅ CONSISTENT: Maintains same priority as calculate_macd():
        1. TA-Lib (preferred - fastest and most accurate)
        2. Polars (fallback)
        3. Simple fallback
        """
        try:
            if window.height < slow_period:
                return {'macd': 0.0, 'signal': 0.0, 'histogram': 0.0}
            
            # ✅ CONSISTENT: Try TA-Lib first (same priority as calculate_macd())
            if TALIB_AVAILABLE:
                try:
                    prices = window['last_price'].to_list()
                    np_prices = np.array(prices, dtype=np.float64)
                    macd, macd_signal, macd_hist = talib.MACD(np_prices, 
                                                             fastperiod=fast_period,
                                                             slowperiod=slow_period, 
                                                             signalperiod=signal_period)
                    
                    result = {
                        'macd': float(macd[-1]) if len(macd) > 0 and not np.isnan(macd[-1]) else 0.0,
                        'signal': float(macd_signal[-1]) if len(macd_signal) > 0 and not np.isnan(macd_signal[-1]) else 0.0,
                        'histogram': float(macd_hist[-1]) if len(macd_hist) > 0 and not np.isnan(macd_hist[-1]) else 0.0
                    }
                    # Return if we got valid results
                    if result['macd'] != 0.0 or result['signal'] != 0.0 or result['histogram'] != 0.0:
                        return result
                except Exception as e:
                    logger.debug(f"TA-Lib MACD failed in _calculate_macd_polars, using Polars: {e}")
            
            # ✅ FALLBACK: Use Polars expressions for MACD calculation
            macd_df = window.with_columns([
                pl.col('last_price').ewm_mean(span=fast_period).alias('ema_fast'),
                pl.col('last_price').ewm_mean(span=slow_period).alias('ema_slow')
            ]).with_columns([
                (pl.col('ema_fast') - pl.col('ema_slow')).alias('macd_line')
            ]).with_columns([
                pl.col('macd_line').ewm_mean(span=signal_period).alias('signal_line')
            ]).with_columns([
                (pl.col('macd_line') - pl.col('signal_line')).alias('histogram')
            ])
            
            macd_value = macd_df['macd_line'].tail(1)[0]
            signal_value = macd_df['signal_line'].tail(1)[0]
            histogram_value = macd_df['histogram'].tail(1)[0]
            
            return {
                'macd': float(macd_value) if macd_value is not None else 0.0,
                'signal': float(signal_value) if signal_value is not None else 0.0,
                'histogram': float(histogram_value) if histogram_value is not None else 0.0
            }
        except Exception as e:
            logger.debug(f"Polars MACD calculation failed, using fallback: {e}")
            # Fallback to list-based calculation (which will try TA-Lib again)
            prices = window['last_price'].to_list()
            return self.calculate_macd(prices, fast_period, slow_period, signal_period, symbol)
    
    def _calculate_bollinger_bands_polars(self, window: "pl.DataFrame", period: int = 20, 
                                         std_dev: float = 2.0, symbol: str = None) -> Dict:
        """✅ OPTIMIZED: Calculate Bollinger Bands using Polars DataFrame directly
        
        ✅ CONSISTENT: Maintains same priority as calculate_bollinger_bands():
        1. TA-Lib (preferred - fastest and most accurate)
        2. Polars (fallback)
        3. Simple fallback
        """
        try:
            if window.height < period:
                return {'upper': 0.0, 'middle': 0.0, 'lower': 0.0}
            
            # ✅ CONSISTENT: Try TA-Lib first (same priority as calculate_bollinger_bands())
            if TALIB_AVAILABLE:
                try:
                    prices = window['last_price'].to_list()
                    np_prices = np.array(prices, dtype=np.float64)
                    upper, middle, lower = talib.BBANDS(np_prices, timeperiod=period, 
                                                      nbdevup=std_dev, nbdevdn=std_dev, 
                                                      matype=0)
                    
                    result = {
                        'upper': float(upper[-1]) if len(upper) > 0 and not np.isnan(upper[-1]) else 0.0,
                        'middle': float(middle[-1]) if len(middle) > 0 and not np.isnan(middle[-1]) else 0.0,
                        'lower': float(lower[-1]) if len(lower) > 0 and not np.isnan(lower[-1]) else 0.0
                    }
                    # Return if we got valid results
                    if result['upper'] != 0.0 or result['middle'] != 0.0 or result['lower'] != 0.0:
                        return result
                except Exception as e:
                    logger.debug(f"TA-Lib Bollinger Bands failed in _calculate_bollinger_bands_polars, using Polars: {e}")
            
            # ✅ FALLBACK: Use Polars expressions for Bollinger Bands calculation
            bb_df = window.with_columns([
                pl.col('last_price').rolling_mean(window_size=period).alias('middle_band'),
                pl.col('last_price').rolling_std(window_size=period).alias('std_band')
            ]).with_columns([
                (pl.col('middle_band') + (pl.col('std_band') * std_dev)).alias('upper_band'),
                (pl.col('middle_band') - (pl.col('std_band') * std_dev)).alias('lower_band')
            ])
            
            upper_value = bb_df['upper_band'].tail(1)[0]
            middle_value = bb_df['middle_band'].tail(1)[0]
            lower_value = bb_df['lower_band'].tail(1)[0]
            
            return {
                'upper': float(upper_value) if upper_value is not None else 0.0,
                'middle': float(middle_value) if middle_value is not None else 0.0,
                'lower': float(lower_value) if lower_value is not None else 0.0
            }
        except Exception as e:
            logger.debug(f"Polars Bollinger Bands calculation failed, using fallback: {e}")
            # Fallback to list-based calculation (which will try TA-Lib again)
            prices = window['last_price'].to_list()
            return self.calculate_bollinger_bands(prices, period, std_dev, symbol)
    
    def _is_option_symbol(self, symbol: str) -> bool:
        """
        Check if symbol is an option using UniversalSymbolParser.
        
        ✅ FIXED: Uses UniversalSymbolParser to check instrument_type == 'OPT'
        This includes NFO, MCX, CDS options (not just NFO).
        Reuses same logic as _extract_underlying_from_symbol() for consistency.
        """
        if not symbol:
            return False
        # Ensure symbol is a string (sometimes it's an int like instrument_token)
        if not isinstance(symbol, str):
            return False
        
        try:
            from shared_core.redis_clients.redis_key_standards import get_symbol_parser
            parser = get_symbol_parser()
            parsed = parser.parse_symbol(symbol)
            # Check if instrument_type is 'OPT' (includes MCX/CDS options)
            return parsed.instrument_type == 'OPT'
        except (ValueError, Exception):
            # Fallback to legacy check for backward compatibility
            symbol_upper = symbol.upper()
            has_ce_pe = symbol_upper.endswith('CE') or symbol_upper.endswith('PE')
            has_nfo_prefix = symbol_upper.startswith('NFO:')
            is_nifty_option = ('NIFTY' in symbol_upper or 'BANKNIFTY' in symbol_upper) and has_ce_pe
            return has_ce_pe and (has_nfo_prefix or is_nifty_option)
    
    def _is_future_symbol(self, symbol: str) -> bool:
        """Check if symbol is an F&O future with robust parsing"""
        if not symbol or not isinstance(symbol, str):
            return False
        
        symbol_upper = symbol.upper()
        
        # Normalize away optional prefixes (NFO, NFO:)
        if symbol_upper.startswith('NFO:'):
            suffix = symbol_upper[4:]
        elif symbol_upper.startswith('NFO'):
            suffix = symbol_upper[3:]
        else:
            suffix = symbol_upper
        
        if not suffix.endswith('FUT'):
            return False
        
        before_fut = suffix[:-3]
        expiry_pattern = r'\d{2}[A-Z]{3}$'
        return re.search(expiry_pattern, before_fut) is not None
    
    def _is_fno_symbol(self, symbol: str) -> bool:
        """Check if symbol is any F&O instrument (option or future)"""
        return self._is_option_symbol(symbol) or self._is_future_symbol(symbol)
    
    # ❌ REMOVED: Duplicate _ensure_expiry_calculator and calculate_greeks_for_tick_data methods
    # These are now inside HybridCalculations class (lines 2614-2659)
    # Keeping this comment to mark where duplicates were removed 

    def calculate_greeks_with_dte(self, spot_price: float, strike_price: float, 
                                    expiry_date: datetime = None, symbol: str = None,
                                    risk_free_rate: float = 0.05, volatility: float = 0.2,
                                    option_type: str = 'call') -> dict:
        """Calculate Greeks with proper DTE handling - merged from EnhancedGreekCalculator"""
        if not GREEK_CALCULATIONS_AVAILABLE:
            return self._get_default_greeks()
    
        # ✅ FIXED: Use _ensure_expiry_calculator() helper method with symbol for exchange inference
        expiry_calc = self._ensure_expiry_calculator(symbol=symbol)
    
        # Calculate proper DTE
        dte_info = expiry_calc.calculate_dte(expiry_date, symbol)
        calendar_dte = dte_info['calendar_dte']
        trading_dte = dte_info['trading_dte']
    
        # Use trading DTE for more accurate calculations
        effective_dte = trading_dte / 365.0
    
        greeks = self.black_scholes_greeks(
            spot_price, strike_price, effective_dte, 
            risk_free_rate, volatility, option_type
        )
    
        # ✅ CRITICAL: Add IV to Greeks dict (it's calculated separately but needs to be included)
        # IV is the volatility parameter used for calculations, store it as both 'iv' and 'implied_volatility'
        return {
            **greeks,
            'iv': volatility,  # Store the volatility used for calculation
            'implied_volatility': volatility,  # Alias for compatibility
            'calendar_dte': calendar_dte,
            'trading_dte': trading_dte,
            'expiry_date': dte_info['expiry_date'],
            'expiry_series': dte_info.get('expiry_series', 'UNKNOWN')
        }

    def black_scholes_greeks(self, spot: float, strike: float, dte_years: float,
                            risk_free: float, volatility: float, option_type: str) -> dict:
        """Optimized Black-Scholes Greek calculations using py_vollib - merged from EnhancedGreekCalculator"""
        if dte_years <= 0 or volatility <= 0:
            return self._get_default_greeks()
    
        if not PY_VOLLIB_AVAILABLE:
            return self._fallback_manual_greeks(spot, strike, dte_years, risk_free, volatility, option_type)
    
        try:
            # ✅ CRITICAL FIX: Verify delta/gamma/theta/vega/rho are defined before use
            if 'delta' not in globals() or not callable(globals().get('delta')):
                logger.warning("⚠️ delta function not available, using fallback")
                return self._fallback_manual_greeks(spot, strike, dte_years, risk_free, volatility, option_type)
            
            flag = 'c' if option_type.lower() in ['call', 'ce'] else 'p'
            option_price = black_scholes(flag, spot, strike, dte_years, risk_free, volatility)
            delta_val = delta(flag, spot, strike, dte_years, risk_free, volatility)
            gamma_val = gamma(flag, spot, strike, dte_years, risk_free, volatility)
            theta_val = theta(flag, spot, strike, dte_years, risk_free, volatility)
            vega_val = vega(flag, spot, strike, dte_years, risk_free, volatility)
            rho_val = rho(flag, spot, strike, dte_years, risk_free, volatility)
            is_call = option_type.lower() in ['call', 'ce']
            return {
                'delta': self._enforce_min_float(delta_val, default_sign=1.0 if is_call else -1.0),
                'gamma': self._enforce_min_float(gamma_val, default_sign=1.0),
                'theta': self._enforce_min_float(theta_val, default_sign=-1.0 if is_call else 1.0),
                'vega': self._enforce_min_float(vega_val, default_sign=1.0),
                'rho': self._enforce_min_float(rho_val, default_sign=1.0 if is_call else -1.0),
                'dte_years': dte_years,
                'option_price': option_price
            }
        except (NameError, Exception) as e:
            logger.debug(f"py_vollib Greek calculation failed, using fallback: {e}")
            return self._fallback_manual_greeks(spot, strike, dte_years, risk_free, volatility, option_type)

    def _fallback_manual_greeks(self, spot: float, strike: float, dte_years: float,
                                    risk_free: float, volatility: float, option_type: str) -> dict:
        """Manual Greek calculation fallback - merged from EnhancedGreekCalculator"""
        from math import log, sqrt, exp
        try:
            from scipy.stats import norm
        except ImportError:
            # Fallback to basic approximation if scipy not available
            logger.warning("scipy.stats not available, using basic Greek approximations")
            return self._get_default_greeks()
    
        d1 = (log(spot / strike) + (risk_free + 0.5 * volatility**2) * dte_years) / (volatility * sqrt(dte_years))
        d2 = d1 - volatility * sqrt(dte_years)
    
        is_call = option_type.lower() in ['call', 'ce']
    
        if is_call:
            delta_val = norm.cdf(d1)
            theta_val = (-spot * norm.pdf(d1) * volatility / (2 * sqrt(dte_years)) 
                    - risk_free * strike * exp(-risk_free * dte_years) * norm.cdf(d2)) / 365
            option_price = (spot * norm.cdf(d1)) - (strike * exp(-risk_free * dte_years) * norm.cdf(d2))
        else:  # put
            delta_val = norm.cdf(d1) - 1
            theta_val = (-spot * norm.pdf(d1) * volatility / (2 * sqrt(dte_years)) 
                    + risk_free * strike * exp(-risk_free * dte_years) * norm.cdf(-d2)) / 365
            option_price = (strike * exp(-risk_free * dte_years) * norm.cdf(-d2)) - (spot * norm.cdf(-d1))
    
        gamma_val = norm.pdf(d1) / (spot * volatility * sqrt(dte_years))
        vega_val = spot * norm.pdf(d1) * sqrt(dte_years) / 100
        rho_val = (strike * dte_years * exp(-risk_free * dte_years) * 
                (norm.cdf(d2) if is_call else -norm.cdf(-d2))) / 100
    
        return {
            'delta': delta_val,
            'gamma': gamma_val,
            'theta': theta_val,
            'vega': vega_val,
            'rho': rho_val,
            'dte_years': dte_years,
            'option_price': option_price
        }

    def _get_default_greeks(self) -> dict:
        """Return default Greek values when calculation fails."""
        return {
            'delta': 0.0,
            'gamma': 0.0,
            'theta': 0.0,
            'vega': 0.0,
            'rho': 0.0,
            'dte_years': 0.0,
            'trading_dte': 0,
            'expiry_series': 'UNKNOWN',
            'option_price': 0.0,
            'iv': 0.2,  # ✅ FIXED: Default IV to 0.2 (20%) instead of 0.0
            'implied_volatility': 0.2  # ✅ FIX: Add implied_volatility for consistency
        }

    def calculate_implied_volatility(self, market_price: float, spot_price: float, strike_price: float,
                                    dte_years: float, risk_free_rate: float, option_type: str,
                                    max_iterations: int = 100, tolerance: float = 0.0001) -> float:
        """
        Calculate Implied Volatility using Newton-Raphson method.
    
        ✅ PROPER IV CALCULATION: Solves for volatility that makes Black-Scholes price = market price
    
        Args:
            market_price: Market LTP of the option
            spot_price: Underlying spot price
            strike_price: Option strike price
            dte_years: Days to expiry in years
            risk_free_rate: Risk-free interest rate
            option_type: 'call' or 'put'
            max_iterations: Maximum Newton-Raphson iterations
            tolerance: Convergence tolerance
    
        Returns:
            Calculated IV (decimal, e.g., 0.18 for 18% IV)
        """
        try:
            # Input validation
            if market_price <= 0 or spot_price <= 0 or strike_price <= 0 or dte_years <= 0:
                logger.debug(f"IV calc invalid inputs: market={market_price}, spot={spot_price}, strike={strike_price}, dte={dte_years}")
                return 0.2  # Default fallback
        
            # Intrinsic value check
            is_call = option_type.lower() in ['call', 'ce', 'c']
            intrinsic = max(0, (spot_price - strike_price) if is_call else (strike_price - spot_price))
        
            if market_price < intrinsic:
                logger.debug(f"IV calc: market_price ({market_price}) < intrinsic ({intrinsic}), using fallback")
                return 0.2
        
            # 🚀 TRY FAST APPROXIMATION FIRST (avoid slow Newton-Raphson)
            if hasattr(self, '_calculate_iv_approximation'):
                fast_iv = self._calculate_iv_approximation(market_price, spot_price, strike_price, dte_years, option_type)
                if fast_iv and 0.05 < fast_iv < 2.0:
                    logger.debug(f"✅ Fast IV approximation: {fast_iv:.4f}")
                    return fast_iv
            
            # Newton-Raphson iteration (fallback)
            iv = 0.3  # Initial guess: 30% IV
        
            for i in range(max_iterations):
                # Calculate theoretical price and vega at current IV estimate
                try:
                    if not PY_VOLLIB_AVAILABLE:
                        # Fallback to manual calculation
                        theoretical_price, vega_val = self._calculate_price_and_vega_manual(
                            spot_price, strike_price, dte_years, risk_free_rate, iv, option_type
                        )
                    else:
                        from py_vollib.black_scholes import black_scholes
                        from py_vollib.black_scholes.greeks.analytical import vega
                    
                        flag = 'c' if is_call else 'p'
                        theoretical_price = black_scholes(flag, spot_price, strike_price, dte_years, risk_free_rate, iv)
                        vega_val = vega(flag, spot_price, strike_price, dte_years, risk_free_rate, iv)
                
                    # Check convergence
                    price_diff = market_price - theoretical_price
                
                    if abs(price_diff) < tolerance:
                        logger.debug(f"IV converged: {iv:.4f} ({iv*100:.2f}%) in {i+1} iterations")
                        return max(0.05, min(iv, 2.0))  # Clamp between 5% and 200%
                
                    # Newton-Raphson update: IV_new = IV_old + (market - theoretical) / vega
                    if vega_val > 0:
                        iv = iv + (price_diff / vega_val)
                        iv = max(0.01, min(iv, 3.0))  # Keep within reasonable bounds
                    else:
                        break
            
                except Exception as e:
                    logger.debug(f"IV iteration {i} failed: {e}")
                    break
        
            # If didn't converge, return best estimate
            final_iv = max(0.05, min(iv, 2.0))
            logger.debug(f"IV did not fully converge, returning: {final_iv:.4f} ({final_iv*100:.2f}%)")
            return final_iv
        
        except Exception as e:
            logger.warning(f"IV calculation failed: {e}, using fallback 0.2")
            return 0.2  # Safe fallback

    def _calculate_price_and_vega_manual(self, spot: float, strike: float, dte_years: float,
                                        risk_free: float, volatility: float, option_type: str) -> tuple:
        """Manual Black-Scholes price and vega calculation (fallback when py_vollib unavailable)"""
        from math import log, sqrt, exp
        try:
            from scipy.stats import norm
        except ImportError:
            return (0.0, 0.0)
    
        d1 = (log(spot / strike) + (risk_free + 0.5 * volatility**2) * dte_years) / (volatility * sqrt(dte_years))
        d2 = d1 - volatility * sqrt(dte_years)
    
        is_call = option_type.lower() in ['call', 'ce', 'c']
    
        if is_call:
            price = (spot * norm.cdf(d1)) - (strike * exp(-risk_free * dte_years) * norm.cdf(d2))
        else:
            price = (strike * exp(-risk_free * dte_years) * norm.cdf(-d2)) - (spot * norm.cdf(-d1))
    
        vega_val = spot * norm.pdf(d1) * sqrt(dte_years) / 100
    
        return (price, vega_val)

# ❌ REMOVED: get_tick_processor() method - Duplicate of standalone function below, never called


def get_tick_processor(redis_client=None, config=None, use_lifecycle_manager=False):
        """
        Factory function to create TickProcessor-compatible instance
        Provides backward compatibility with existing scanner

        Args:
        redis_client: Redis client instance
        config: Optional configuration dict
        use_lifecycle_manager: If True, use IndicatorLifecycleManager instead of direct HybridCalculations
                                This provides caching, fallbacks, and symbol-aware indicator requirements

        Returns:
        TickProcessorWrapper or IndicatorLifecycleWrapper
        """
        if use_lifecycle_manager:
            # ✅ DEPRECATED: IndicatorLifecycleManager uses legacy StorageEngine
            # This code path is not actively used - UnifiedDataStorage is used directly in _store_indicators_atomic
            # If needed in future, update IndicatorLifecycleManager to accept UnifiedDataStorage
            logger = logging.getLogger(__name__)
            logger.warning("⚠️ [DEPRECATED] use_lifecycle_manager=True is deprecated. Use UnifiedDataStorage directly.")
            # Fall through to default TickProcessorWrapper

        # Original implementation (default)
        # Create HybridCalculations instance with bounds and redis_client
        hybrid_calc = HybridCalculations(max_cache_size=500, max_batch_size=174, redis_client=redis_client)

        return TickProcessorWrapper(hybrid_calc, redis_client, config)


class TickProcessorWrapper:
    def __init__(self, hybrid_calc, redis_client=None, config=None):
        self.hybrid_calc = hybrid_calc
        self.redis_client = redis_client
        self.config = config or {}
        self.debug_enabled = False
        self.stats = {
            "ticks_processed": 0,
            "indicators_computed": 0,
            "errors": 0,
        }
        # ✅ FIXED: Pass Redis client to HybridCalculations
        self.hybrid_calc.redis_client = redis_client

    # Compatibility attributes

    def process_tick(self, symbol: str, tick_data: Dict) -> Dict:
        """Process individual tick using HybridCalculations"""
        try:
            # ✅ FIXED: Ensure redis_client is always set on hybrid_calc before processing
            if not hasattr(self.hybrid_calc, 'redis_client') or not self.hybrid_calc.redis_client:
                self.hybrid_calc.redis_client = self.redis_client
            
            # Defensive check: if process_tick missing, use batch_calculate_indicators directly
            if hasattr(self.hybrid_calc, 'process_tick'):
                indicators = self.hybrid_calc.process_tick(symbol, tick_data)
            else:
                # Fallback: process as batch of 1
                batch_result = self.hybrid_calc.batch_calculate_indicators([tick_data], symbol)
                if batch_result and symbol in batch_result:
                    indicators = batch_result[symbol]
                else:
                    indicators = batch_result or {}
            
            self.stats["ticks_processed"] += 1
            
            if indicators:
                self.stats["indicators_computed"] += 1
                return indicators
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Error in TickProcessorWrapper: {e}")
        return {}
    
    def get_rolling_window_stats(self) -> Dict:
        """Get rolling window statistics"""
        return self.hybrid_calc.get_rolling_window_stats()
        
    def cleanup_old_windows(self, max_age_minutes: int = 30) -> int:
        """Clean up old rolling windows"""
        return self.hybrid_calc.cleanup_old_windows(max_age_minutes)
        
    def get_history_length(self, symbol: str) -> int:

        """Get history length for symbol (compatibility method)"""
        return 50  # Default history length
        
    def get_stats(self) -> Dict:
        """Get processing statistics"""
        return self.hybrid_calc.get_stats()


# ✅ FIXED: Moved safe_format and preload_static_data to module level
# They were incorrectly defined inside TickProcessorWrapper class (missing self)
def safe_format(value, format_str: str = "{:.2f}") -> str:
    """Safely format a value with error handling."""
    try:
        if value is None or (isinstance(value, float) and (value != value or value == float('inf') or value == float('-inf'))):
            return "N/A"
        return format_str.format(value)
    except (ValueError, TypeError):
        return "N/A"


def preload_static_data():
    """Pre-load static data like market calendars and expiry calculators before scanner starts"""
    try:
        from shared_core.market_calendar import get_cached_calendar

        try:
            get_cached_calendar('NSE')
            logger.debug("✅ Pre-loaded NSE market calendar cache")
        except Exception as e:
            logger.debug(f"⚠️ Could not preload market calendar: {e}")
        
        # Initialize global expiry calculator instance (already created at module level, but ensure it's ready)
        global expiry_calculator
        
        if expiry_calculator is None:
            expiry_calculator = ExpiryCalculator('NSE')
            logger.debug("✅ Initialized global expiry calculator")
        else:
            logger.debug("✅ Global expiry calculator already initialized")
        
        logger.debug("✅ Static data preloaded successfully")
    except Exception as e:
        logger.warning(f"⚠️ Failed to preload static data: {e}")


def get_market_volume_data(symbol: str, redis_client=None) -> dict:
    """Get market bucket_incremental_volume data for a symbol"""
    try:
        # ✅ FIXED: Use UnifiedRedisManager (singleton-style)
        from shared_core.redis_clients import UnifiedRedisManager
        
        if redis_client is None:
            redis_client = get_redis_client(
                process_name="get_market_volume_data",
                db=1
            )
        
        # Get volume data from Redis
        # Volume data is stored in ticks:latest:{symbol} hash
        from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
        tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(symbol)
        
        data = redis_client.hgetall(tick_key)
        if data:
            # Extract volume-related fields
            volume_data = {}
            volume_fields = ['bucket_incremental_volume', 'volume', 'volume_ratio', 
                            'zerodha_cumulative_volume', 'bucket_cumulative_volume']
            for field in volume_fields:
                if field in data:
                    value = data[field]
                    if isinstance(value, bytes):
                        value = value.decode('utf-8')
                        logger.debug(f"Volume data for {symbol}: {value}")
                    try:
                        volume_data[field] = float(value) if '.' in str(value) else int(value)
                        logger.debug(f"Volume data for {symbol}: {volume_data[field]}")
                    except (ValueError, TypeError):
                        logger.debug(f"Volume data for {symbol}: {value}")
            return volume_data
        return {}
    except Exception as e:
        logger.warning(f"⚠️ Failed to get market volume data for {symbol}: {e}")
        return {}

def get_all_thresholds_for_regime(vix_regime: str) -> dict:
    """Get all thresholds for a VIX regime using centralized config"""
    try:
        from shared_core.config_utils.thresholds import get_thresholds_for_regime
        thresholds = get_thresholds_for_regime(vix_regime=vix_regime)
        return thresholds
    except ImportError:
        logger.warning("⚠️ Could not import get_thresholds_for_regime from shared_core.config_utils.thresholds")
        return {}


    # ============ Redis Fallback Functions ============

def get_indicator_from_redis(symbol: str, indicator_name: str, redis_client=None) -> Optional[float]:
    """
    Get indicator value from Redis cache using UnifiedDataStorage.
    
    ✅ FIXED: Uses UnifiedDataStorage.get_indicators() which reads from ind:{symbol} HASH.
    This ensures consistency with store_indicators() which writes to the same hash.
    
    Args:
        symbol: Trading symbol
        indicator_name: Name of the indicator to fetch
        redis_client: (Deprecated) - Ignored, uses UnifiedDataStorage internally
    
    Returns:
        Float value if found, None otherwise
    """
    try:
        # ✅ FIXED: Use UnifiedDataStorage (HGETALL on ind:{symbol} hash)
        from shared_core.redis_clients.unified_data_storage import get_unified_storage
        storage = get_unified_storage()
        
        indicators = storage.get_indicators(symbol, [indicator_name])
        if indicators and indicator_name in indicators:
            value = indicators[indicator_name]
            return float(value) if value is not None else None
        
        return None
        
    except Exception as e:
        logger.debug(f"Error getting indicator {indicator_name} for {symbol}: {e}")
        return None

def calculate_rsi_with_redis_fallback(prices: List[float], period: int = 14, symbol: str = None, redis_client=None) -> float:
    """Calculate RSI with Redis fallback"""
    try:
        # First try Redis fallback
        if symbol and redis_client:
            redis_rsi = get_indicator_from_redis(symbol, "rsi", redis_client)
            if redis_rsi is not None:
                return redis_rsi
        # Fallback to calculation
        return calculate_rsi_with_polars(prices, period)
    except Exception as e: 
        logger.warning(f"⚠️ Failed to calculate RSI for {symbol}: {e}")
        return calculate_rsi_with_polars(prices, period)

def calculate_rsi_with_polars(prices: List[float], period: int = 14) -> float:
    """Calculate RSI with Polars"""
    try:
        if len(prices) < period + 1:
            return 50.0
        df = pl.DataFrame({'last_price': prices})
        rsi = df.with_columns([
            pl.col('last_price').diff().alias('delta')
        ]).with_columns([
            pl.when(pl.col('delta') > 0).then(pl.col('delta')).otherwise(0).alias('gain'),
            pl.when(pl.col('delta') < 0).then(-pl.col('delta')).otherwise(0).alias('loss')
        ]).with_columns([
            pl.col('gain').rolling_mean(period).alias('avg_gain'),
            pl.col('loss').rolling_mean(period).alias('avg_loss')
        ]).with_columns([
            (100 - (100 / (1 + (pl.col('avg_gain') / (pl.col('avg_loss') + 1e-10))))).alias('rsi')
        ])
        return float(rsi['rsi'].tail(1)[0]) if rsi['rsi'].tail(1)[0] is not None else 50.0
    except Exception as e:
        return 50.0

def calculate_atr_with_redis_fallback(highs: List[float], lows: List[float], closes: List[float], 
                                    period: int = 14, symbol: str = None, redis_client=None) -> float:
    """Calculate ATR with Redis fallback"""
    try:
        # First try Redis fallback
        if symbol and redis_client:
            redis_atr = get_indicator_from_redis(symbol, "atr", redis_client)
            if redis_atr is not None:
                return redis_atr
        
        # Fallback to calculation
        if not POLARS_AVAILABLE:
            # Use HybridCalculations fallback
            hybrid_calc = HybridCalculations(max_cache_size=500, max_batch_size=174, redis_client=redis_client)
            return hybrid_calc.calculate_atr(highs, lows, closes, period)
        
        if len(highs) < period + 1:
            return 0.0
        
        # Use Polars for rolling window calculations
        df = pl.DataFrame({
            'high': highs,
            'low': lows, 
            'close': closes
        })
        
        # True Range calculation in Polars
        df = df.with_columns([
            (pl.col('high') - pl.col('low')).alias('tr1'),
            (pl.col('high') - pl.col('close').shift(1)).abs().alias('tr2'),
            (pl.col('low') - pl.col('close').shift(1)).abs().alias('tr3')
        ]).with_columns([
            pl.max_horizontal(['tr1', 'tr2', 'tr3']).alias('tr')
        ])
        
        # ATR as rolling mean
        atr = df['tr'].tail(period).mean()
        return float(atr) if atr is not None else 0.0
    except Exception as e:
        return 0.0

def calculate_ema_with_redis_fallback(prices: List[float], period: int = 20, symbol: str = None, redis_client=None) -> float:
    """Calculate EMA with Redis fallback"""
    try:
        # First try Redis fallback
        if symbol and redis_client:
            redis_ema = get_indicator_from_redis(symbol, f"ema_{period}", redis_client)
            if redis_ema is not None:
                return redis_ema
        # Fallback to calculation
        return calculate_ema_with_polars(prices, period)
    except Exception as e:
        logger.warning(f"⚠️ Failed to calculate EMA for {symbol}: {e}")
        return calculate_ema_with_polars(prices, period)

def calculate_ema_with_polars(prices: List[float], period: int = 20) -> float:
    """Calculate EMA with Polars"""
    if not POLARS_AVAILABLE:
        # Use HybridCalculations fallback
        hybrid_calc = HybridCalculations()
        return hybrid_calc.calculate_ema(prices, period)
    
    if len(prices) < period:
        return float(prices[-1]) if prices else 0.0
    
    try:
        series = pl.Series('last_price', prices)
        # Polars exponential moving average
        ema = series.ewm_mean(span=period).tail(1)[0]
        return float(ema)
    except Exception as e:
        return float(prices[-1]) if prices else 0.0

def calculate_vwap_with_redis_fallback(tick_data: List[Dict], symbol: str = None, redis_client=None) -> float:
    """Calculate VWAP with Redis fallback using TA-Lib (preferred)"""
    try:
        # First try Redis fallback
        if symbol and redis_client:
            redis_vwap = get_indicator_from_redis(symbol, "vwap", redis_client)
            if redis_vwap is not None:
                return redis_vwap
        
        # Try TA-Lib first (fastest and most accurate) using price/volume weighting
        if TALIB_AVAILABLE and len(tick_data) >= 20:
            try:
                prices: List[float] = []
                volumes: List[float] = []
                for tick in tick_data:
                    price = tick.get("last_price", 0.0)
                    volume = (
                        tick.get("zerodha_cumulative_volume", 0)
                        or tick.get("bucket_incremental_volume", 0)
                        or tick.get("bucket_cumulative_volume", 0)
                        or 0
                    )
                    if price > 0 and volume > 0:
                        prices.append(float(price))
                        volumes.append(float(volume))
                if prices and volumes:
                    weighted_sum = sum(p * v for p, v in zip(prices, volumes))
                    total_volume = sum(volumes)
                    if total_volume > 0:
                        return weighted_sum / total_volume
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(f"⚠️ Failed to calculate VWAP for {symbol}: {exc}")
        return 0.0
    except Exception as e:
        logger.warning(f"⚠️ Failed to calculate VWAP for {symbol}: {e}")
        return 0.0

def calculate_macd_with_redis_fallback(prices: List[float], fast_period: int = 12,
                                        slow_period: int = 26, signal_period: int = 9, symbol: str = None, redis_client=None) -> Dict:
    """Calculate MACD with Redis fallback"""
    try:
        # First try Redis fallback
        if symbol and redis_client:
            redis_macd = get_indicator_from_redis(symbol, "macd", redis_client)
            if redis_macd is not None:
                return redis_macd
        # Fallback to calculation   
        if not POLARS_AVAILABLE:
            # Use HybridCalculations fallback
            hybrid_calc = HybridCalculations(max_cache_size=500, max_batch_size=174, redis_client=redis_client)
            return hybrid_calc.calculate_macd(prices, fast_period, slow_period, signal_period)
        
        if len(prices) < slow_period:
            return {'macd': 0.0, 'signal': 0.0, 'histogram': 0.0}
        
        df = pl.DataFrame({'last_price': prices})
        return calculate_macd_with_polars(df, fast_period, slow_period, signal_period)
    except Exception as e:
        logger.warning(f"⚠️ Failed to calculate MACD for {symbol}: {e}")
        return {'macd': 0.0, 'signal': 0.0, 'histogram': 0.0}
    # ============ Enhanced Greek Calculations ============

class ExpiryCalculator:
    """
    Exchange-aware expiry calculations.
    
    ✅ CONSOLIDATED: Now uses MarketCalendar from shared_core.market_calendar as single source of truth.
    All calendar logic is delegated to MarketCalendar to avoid duplication.
    """
    
    def __init__(self, exchange='NSE'):
        self.exchange = exchange
        # ✅ CONSOLIDATED: Use MarketCalendar instead of direct calendar access
        from shared_core.market_calendar import MarketCalendar
        self.market_calendar = MarketCalendar(exchange)
    
    @staticmethod
    def parse_expiry_from_symbol(symbol: str) -> Optional[datetime]:
        """Parse expiry date from option symbol - delegates to MarketCalendar."""
        from shared_core.market_calendar import MarketCalendar
        
        exchange = 'NSE'
        upper = (symbol or '').upper()
        if 'SENSEX' in upper or 'BFO' in upper:
            exchange = 'BSE'
        elif 'MCX' in upper:
            exchange = 'MCX'
        
        calendar = MarketCalendar(exchange)
        return calendar.parse_expiry_from_symbol(symbol)
    
    def get_next_expiry(self, current_date: datetime = None) -> Optional[datetime]:
        """Get next Thursday expiry date considering trading holidays"""
        return self.market_calendar.get_next_weekly_expiry(current_date)
    
    def is_trading_day(self, date: datetime) -> bool:
        """Check if date is a trading day - delegates to MarketCalendar"""
        return self.market_calendar.is_trading_day(date)
    
    def calculate_dte(self, expiry_date: datetime = None, symbol: str = None, 
                     current_date: datetime = None) -> Dict[str, Any]:
        """
        Calculate days to expiry with multiple input methods.
        
        Returns:
            Dict with 'calendar_dte', 'trading_dte', 'expiry_date', 'is_weekly'
        """
        if current_date is None:
            current_date = datetime.now()
        
        # Get expiry date from symbol if provided
        if expiry_date is None and symbol:
            expiry_date = self.parse_expiry_from_symbol(symbol)
        
        # If still no expiry, use next weekly expiry
        if expiry_date is None:
            expiry_date = self.get_next_expiry(current_date)
        
        # ✅ FIXED: Convert expiry_date to datetime if it's a string
        if expiry_date is not None and isinstance(expiry_date, str):
            try:
                from datetime import datetime as dt
                # Try parsing common date formats
                for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S%z']:
                    try:
                        expiry_date = dt.strptime(expiry_date, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    # If all formats fail, try pandas/dateutil parser
                    try:
                        from dateutil import parser
                        expiry_date = parser.parse(expiry_date)
                    except Exception:
                        logger.warning(f"⚠️ Failed to parse expiry_date string: {expiry_date}")
                        expiry_date = None
            except Exception as e:
                logger.warning(f"⚠️ Error converting expiry_date to datetime: {e}")
                expiry_date = None
        
        if expiry_date is None:
            return {
                'calendar_dte': 0.0,
                'trading_dte': 0,
                'expiry_date': None,
                'is_weekly': False,
                'expiry_series': 'UNKNOWN'
            }
        
        # ✅ FIXED: Ensure expiry_date is a datetime object (not date)
        if hasattr(expiry_date, 'date') and not isinstance(expiry_date, datetime):
            # If it's a date object, convert to datetime
            from datetime import datetime as dt, time
            if isinstance(expiry_date, date):
                expiry_date = dt.combine(expiry_date, time(15, 30))  # Market close time
        
        # Calculate trading days using MarketCalendar
        trading_days = self.market_calendar.calculate_trading_dte(expiry_date, current_date)
        calendar_days = max(0.0, (expiry_date - current_date).total_seconds() / (24 * 60 * 60))
        
        # ✅ FIX: Calculate expiry_series using MarketCalendar (WEEKLY or MONTHLY)
        expiry_series = self.get_expiry_series(expiry_date)
        
        return {
            'calendar_dte': calendar_days,
            'trading_dte': trading_days,
            'expiry_date': expiry_date,
            'is_weekly': self.market_calendar.is_weekly_expiry(expiry_date),
            'expiry_series': expiry_series
        }
    
    def calculate_trading_dte(self, expiry_date: datetime, current_date: datetime = None) -> int:
        """Calculate trading days to expiry - delegates to MarketCalendar"""
        return self.market_calendar.calculate_trading_dte(expiry_date, current_date)
    
    def is_weekly_expiry(self, expiry_date: datetime) -> bool:
        """Check if expiry is weekly - delegates to MarketCalendar"""
        return self.market_calendar.is_weekly_expiry(expiry_date)
    
    def get_expiry_series(self, expiry_date: datetime) -> str:
        """Get expiry series - delegates to MarketCalendar"""
        return self.market_calendar.get_expiry_series(expiry_date)
    
    def get_all_upcoming_expiries(self, days_lookahead: int = 30) -> List[datetime]:
        """Get all upcoming expiry dates - delegates to MarketCalendar"""
        return self.market_calendar.get_all_upcoming_expiries(days_lookahead)
    # Global instances for easy access
expiry_calculator = ExpiryCalculator('NSE')


# ✅ MOVED CLASS DEFINITION
class GammaExposureCalculator:
    """
    Professional Gamma Exposure calculation for options trading.
    
    ✅ FIXED: Uses SCAN instead of KEYS (non-blocking)
    ✅ FIXED: Caches option chains with 60-second TTL
    ✅ FIXED: Proper sign handling (calls vs puts)
    ✅ FIXED: Includes underlying price (S² term)
    ✅ FIXED: Only updates when underlying price changes significantly (±0.5%)
    ✅ FIXED: Calculates net GEX (calls - puts)
    ✅ FIXED: Generates gamma profiles by strike
    """
    
    def __init__(self, redis_client=None):
        self.redis_client = redis_client
        self.logger = logging.getLogger(__name__)
        
    def calculate_total_gex(self, underlying_symbol: str) -> Dict[str, float]:
        """
        Calculate comprehensive gamma exposure metrics.
        
        ✅ FIXED: Uses proper GEX formula with underlying price (S² term)
        ✅ FIXED: Proper sign handling (calls positive, puts positive but different delta hedge effect)
        ✅ FIXED: Only updates when underlying price changes significantly (±0.5%)
        
        Returns:
            {
                'total_gex': float,      # Total GEX (all strikes)
                'call_gex': float,       # GEX from calls only
                'put_gex': float,        # GEX from puts only
                'net_gex': float,        # Call GEX - Put GEX
                'spot_level': float,     # Current underlying price
                'max_pain': float,       # Strike with max OI
                'gamma_flip': float,     # Price where gamma flips sign
            }
        """
        if not underlying_symbol or not self.redis_client:
            return {}
        
        # Get underlying price (fallback only - should come from tick_data in new architecture)
        underlying_price = self._get_underlying_price_from_redis(underlying_symbol) if hasattr(self, '_get_underlying_price_from_redis') else 0.0
        if not underlying_price or underlying_price <= 0:
            return {}
        
        # Get option chain
        option_chain = self._build_option_chain_from_redis(underlying_symbol) if hasattr(self, '_build_option_chain_from_redis') else []
        if not option_chain:
            return {}
        
        # Calculate GEX components
        total_gex = 0.0
        call_gex = 0.0
        put_gex = 0.0
        
        for option in option_chain:
            strike = option.get('strike', 0.0)
            oi = option.get('oi', 0.0)
            gamma = option.get('gamma', 0.0)
            option_type = option.get('type', '')
            
            if strike <= 0 or oi <= 0 or gamma == 0:
                continue
            
            # GEX formula: 0.5 * gamma * S² * OI * multiplier
            option_gex = 0.5 * gamma * (underlying_price ** 2) * oi * self._get_multiplier(underlying_symbol)
            
            total_gex += option_gex
            
            if option_type.lower() in ['call', 'ce']:
                call_gex += option_gex
            elif option_type.lower() in ['put', 'pe']:
                put_gex += option_gex
        
        return {
            'total_gex': total_gex,
            'call_gex': call_gex,
            'put_gex': put_gex,
            'net_gex': call_gex - put_gex,  # Positive = call-heavy, Negative = put-heavy
            'spot_level': underlying_price,
            'gex_ratio': call_gex / put_gex if put_gex != 0 else float('inf'),
        }
    
    def calculate_gamma_profile(self, underlying_symbol: str) -> Dict:
        """
        Calculate gamma exposure by strike for profile visualization.
        
        Returns gamma exposure at each strike level.
        """
        # Get underlying price (fallback only - used by scanner batch operations)
        underlying_price = self._get_underlying_price_from_redis(underlying_symbol) if hasattr(self, '_get_underlying_price_from_redis') else 0.0
        if not underlying_price:
            return {}
        
        option_chain = self._build_option_chain_from_redis(underlying_symbol) if hasattr(self, '_build_option_chain_from_redis') else []
        if not option_chain:
            return {}
        
        gamma_profile = {}
        
        for option in option_chain:
            strike = option.get('strike', 0.0)
            oi = option.get('oi', 0.0)
            gamma = option.get('gamma', 0.0)
            
            if strike <= 0 or oi <= 0 or gamma == 0:
                continue
            
            strike_key = f"{strike:.0f}"
            option_gex = 0.5 * gamma * (underlying_price ** 2) * oi * (self._get_multiplier_from_redis(underlying_symbol) if hasattr(self, '_get_multiplier_from_redis') else 1.0)
            
            if strike_key in gamma_profile:
                gamma_profile[strike_key] += option_gex
            else:
                gamma_profile[strike_key] = option_gex
        
        return gamma_profile
    
    def _build_option_chain_from_redis(self, underlying: str) -> List[Dict[str, Any]]:
        """Build option chain from stored tick data (OI) and Greeks (gamma) in Redis."""
        if not underlying or not self.redis_client:
            return []
        
        try:
            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
            # ✅ FIXED: Use existing pooled redis_client
            redis_conn = self.redis_client
            if redis_conn is None:
                return []
            gamma_keys = RedisKeyStandards.get_gamma_keys(underlying) if hasattr(RedisKeyStandards, 'get_gamma_keys') else []
            
            if not gamma_keys:
                return []
            
            # Build option chain from stored data
            option_chain = []
            for gamma_key in gamma_keys:
                try:
                    # Extract symbol from key: ind:greeks:NFONIFTY30DEC26550PE:gamma -> NFONIFTY30DEC26550PE
                    symbol = gamma_key.split(':')[-2] if ':' in gamma_key else gamma_key.replace('ind:greeks:', '').replace(':gamma', '')
                    
                    # Get gamma from Redis
                    gamma_val = redis_conn.get(gamma_key)
                    if gamma_val:
                        if isinstance(gamma_val, str):
                            gamma_val = float(gamma_val)
                        else:
                            gamma_val = float(gamma_val)
                    else:
                        continue
                    
                    # Get OI from ticks:latest first (if available), otherwise fall back to ind:{symbol}
                    oi = None
                    from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
                    tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(symbol)
                    indicator_key = DatabaseAwareKeyBuilder.live_indicator_hash(symbol)
                    try:
                        def _hget(conn, key, field):
                            if hasattr(conn, 'hget'):
                                return conn.hget(key, field)
                            if hasattr(conn, 'execute_command'):
                                return conn.execute_command('HGET', key, field)
                            return None

                        oi = _hget(redis_conn, tick_key, 'open_interest')
                        if oi is None:
                            oi = _hget(redis_conn, tick_key, 'oi')
                        if oi is None:
                            oi = _hget(redis_conn, indicator_key, 'open_interest')
                        if oi is None:
                            oi = _hget(redis_conn, indicator_key, 'oi')

                        if oi is None:
                            logger.debug(
                                "⚠️ [OI_MISSING] %s tick_keys missing open_interest (tick=%s, ind=%s)",
                                symbol,
                                tick_key,
                                indicator_key,
                            )
                            continue

                        if isinstance(oi, bytes):
                            oi = oi.decode('utf-8', errors='ignore')
                        if isinstance(oi, str):
                            oi = oi.strip()
                            if not oi:
                                logger.debug(
                                    "⚠️ [OI_EMPTY] %s key=%s returned empty string",
                                    symbol,
                                    tick_key,
                                )
                                continue

                        oi = float(oi)
                        if oi <= 0.0:
                            logger.debug(
                                "⚠️ [OI_NONPOSITIVE] %s key=%s oi=%s",
                                symbol,
                                tick_key,
                                oi,
                            )
                            continue
                    except Exception as read_exc:
                        logger.debug(f"Failed to read/parse OI for {symbol} (tick={tick_key} ind={indicator_key}): {read_exc}")
                        continue

                    if gamma_val == 0.0:
                        logger.debug(f"⚠️ [GAMMA_ZERO] {symbol} gamma={gamma_val}")
                        continue

                    option_chain.append({
                        'symbol': symbol,
                        'oi': oi,
                        'open_interest': oi,
                        'gamma': gamma_val
                    })
                except Exception as e:
                    logger.debug(f"Failed to build option chain entry for {gamma_key}: {e}")
                    continue
            
            return option_chain
            
        except Exception as e:
            logger.debug(f"Failed to build option chain from Redis for {underlying}: {e}")
            return []
