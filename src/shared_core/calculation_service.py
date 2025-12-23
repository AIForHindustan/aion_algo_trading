# shared_core/calculation_service.py
"""
Calculation Service
===================

Pure calculation logic extracted from HybridCalculations for multiprocess pipeline.
Contains NO Redis writes - only pure computations.

Designed for use in CalculationWorker processes where:
- Parser Process sends parsed ticks
- Calculation Workers compute indicators/Greeks
- Storage Process writes to Redis

Usage:
    service = CalculationService()
    result = service.execute_pipeline(tick_data, price_service)
"""

from __future__ import annotations

import logging
import math
import os
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from shared_core.index_manager import index_manager

logger = logging.getLogger(__name__)

# Try to import py_vollib for accurate Greeks
PY_VOLLIB_AVAILABLE = False
try:
    from py_vollib.black_scholes import black_scholes
    from py_vollib.black_scholes.implied_volatility import implied_volatility
    from py_vollib.black_scholes.greeks.analytical import (
        delta as bs_delta,
        gamma as bs_gamma,
        theta as bs_theta,
        vega as bs_vega,
    )
    PY_VOLLIB_AVAILABLE = True
except ImportError:
    pass

# Try to import vectorized vollib for batch calculations
PY_VOLLIB_VECTORIZED_AVAILABLE = False
try:
    from py_vollib_vectorized import vectorized_implied_volatility as vec_iv
    from py_vollib_vectorized import vectorized_delta, vectorized_gamma, vectorized_theta, vectorized_vega
    PY_VOLLIB_VECTORIZED_AVAILABLE = True
except ImportError:
    pass

# Try to import TA-Lib for technical indicators
TALIB_AVAILABLE = False
try:
    import talib
    TALIB_AVAILABLE = True
except ImportError:
    pass

# Try to import Polars for DataFrame operations
POLARS_AVAILABLE = False
try:
    import polars as pl
    POLARS_AVAILABLE = True
    
    # Standard tick schema with all timestamp formats
    TICK_SCHEMA = {
        # Timestamps (multiple formats supported)
        'exchange_timestamp_ms': pl.Int64,
        'timestamp_ms': pl.Int64,
        'timestamp_ns_ms': pl.Int64,
        'last_trade_time_ms': pl.Int64,
        'processed_timestamp_ms': pl.Int64,
        'exchange_timestamp_epoch': pl.Int64,
        'processed_timestamp': pl.Float64,
        'exchange_timestamp': pl.Utf8,
        'timestamp': pl.Utf8,
        'timestamp_ns': pl.Utf8,
        'last_trade_time': pl.Utf8,
        # Price data
        'last_price': pl.Float64,
        'high': pl.Float64,
        'low': pl.Float64,
        'open': pl.Float64,
        'close': pl.Float64,
        'average_price': pl.Float64,
        # Volume data
        'zerodha_cumulative_volume': pl.Int64,
        'zerodha_last_traded_quantity': pl.Int64,
        'bucket_incremental_volume': pl.Int64,
        'bucket_cumulative_volume': pl.Int64,
        'volume': pl.Int64,
        # Option data
        'underlying_price': pl.Float64,
        'strike_price': pl.Float64,
        'dte_years': pl.Float64,
        'option_type': pl.Utf8,
        # Greeks (output)
        'delta': pl.Float64,
        'gamma': pl.Float64,
        'theta': pl.Float64,
        'vega': pl.Float64,
        'iv': pl.Float64,
        'implied_volatility': pl.Float64,
        # Microstructure
        'bid_price': pl.Float64,
        'ask_price': pl.Float64,
        'bid_quantity': pl.Int64,
        'ask_quantity': pl.Int64,
        'microprice': pl.Float64,
        'order_book_imbalance': pl.Float64,
        'cvd': pl.Float64,
        # Metadata
        'symbol': pl.Utf8,
    }
except ImportError:
    pl = None
    TICK_SCHEMA = {}

# Try to import numpy
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    np = None
    NUMPY_AVAILABLE = False

# Try to import Numba for JIT acceleration
NUMBA_AVAILABLE = False
try:
    from numba import njit, prange
    NUMBA_AVAILABLE = True
    
    @njit(cache=True)
    def _norm_cdf_numba(x: float) -> float:
        """Fast normal CDF approximation (Abramowitz & Stegun)."""
        a1 = 0.254829592
        a2 = -0.284496736
        a3 = 1.421413741
        a4 = -1.453152027
        a5 = 1.061405429
        p = 0.3275911
        
        sign = 1.0 if x >= 0 else -1.0
        x = abs(x)
        t = 1.0 / (1.0 + p * x)
        y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x)
        return 0.5 * (1.0 + sign * y)
    
    @njit(cache=True)
    def _norm_pdf_numba(x: float) -> float:
        """Fast normal PDF."""
        return np.exp(-0.5 * x * x) / np.sqrt(2.0 * np.pi)
    
    @njit(parallel=True, cache=True)
    def _calculate_greeks_numba_batch(
        S: np.ndarray,
        K: np.ndarray,
        T: np.ndarray,
        r: float,
        iv: np.ndarray,
        is_call: np.ndarray
    ) -> tuple:
        """
        Numba-accelerated parallel Greeks calculation.
        
        Processes 1000+ options in parallel using all CPU cores.
        """
        n = len(S)
        delta = np.zeros(n, dtype=np.float64)
        gamma = np.zeros(n, dtype=np.float64)
        theta = np.zeros(n, dtype=np.float64)
        vega = np.zeros(n, dtype=np.float64)
        
        for i in prange(n):
            if S[i] <= 0 or K[i] <= 0 or T[i] <= 0 or iv[i] <= 0:
                delta[i] = 0.5 if is_call[i] else -0.5
                gamma[i] = 0.0
                theta[i] = 0.0
                vega[i] = 0.0
                continue
            
            sqrt_T = np.sqrt(T[i])
            d1 = (np.log(S[i] / K[i]) + (r + 0.5 * iv[i]**2) * T[i]) / (iv[i] * sqrt_T)
            d2 = d1 - iv[i] * sqrt_T
            
            N_d1 = _norm_cdf_numba(d1)
            N_d2 = _norm_cdf_numba(d2)
            n_d1 = _norm_pdf_numba(d1)
            
            if is_call[i]:
                delta[i] = N_d1
                theta[i] = (-(S[i] * n_d1 * iv[i]) / (2 * sqrt_T) - 
                           r * K[i] * np.exp(-r * T[i]) * N_d2) / 365.0
            else:
                delta[i] = N_d1 - 1
                theta[i] = (-(S[i] * n_d1 * iv[i]) / (2 * sqrt_T) + 
                           r * K[i] * np.exp(-r * T[i]) * (1 - N_d2)) / 365.0
            
            gamma[i] = n_d1 / (S[i] * iv[i] * sqrt_T)
            vega[i] = S[i] * n_d1 * sqrt_T / 100.0
        
        return delta, gamma, theta, vega
    
    @njit(parallel=True, cache=True)
    def _approximate_iv_batch_numba(
        prices: np.ndarray,
        S: np.ndarray,
        K: np.ndarray,
        T: np.ndarray
    ) -> np.ndarray:
        """Numba-accelerated batch IV approximation."""
        n = len(prices)
        iv = np.zeros(n, dtype=np.float64)
        
        for i in prange(n):
            if S[i] <= 0 or T[i] <= 0 or prices[i] <= 0:
                iv[i] = 0.20
                continue
            
            # Brenner-Subrahmanyam approximation
            iv_est = prices[i] * np.sqrt(2.0 * np.pi / T[i]) / S[i]
            
            # Adjust for moneyness
            if K[i] > 0:
                moneyness = S[i] / K[i]
                if moneyness < 0.95 or moneyness > 1.05:
                    iv_est *= 1.1
            
            # Clamp to reasonable range
            iv[i] = max(0.05, min(iv_est, 2.0))
        
        return iv
    
    logger.info("✅ Numba JIT acceleration enabled for batch Greeks calculation")
    
except ImportError:
    logger.info("⚠️ Numba not available - using standard Python for Greeks")



@dataclass
class CalculationResult:
    """Result of calculation pipeline."""
    symbol: str
    tick_data: Dict[str, Any]
    indicators: Dict[str, float]
    greeks: Dict[str, float]
    microstructure: Dict[str, float]
    calculation_time_us: float


class CalculationPipeline:
    """Composable calculation pipeline with per-stage observability."""

    def __init__(self, service: "CalculationService", price_service=None):
        self.service = service
        self.price_service = price_service
        self.logger = logging.getLogger(__name__)
        self.stages = OrderedDict([
            ("parse", self._parse_stage),
            ("volume", self._volume_stage),
            ("greeks", self._greeks_stage),
            ("indicators", self._indicators_stage),
            ("classification", self._classification_stage),
        ])

    def process(self, tick_data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        stage_results: Dict[str, Any] = {}
        errors: List[str] = []
        for stage_name, stage_func in self.stages.items():
            try:
                result = stage_func(tick_data, stage_results)
                stage_results[stage_name] = result
                self.logger.debug("[PIPELINE_STAGE] %s - %s: SUCCESS", tick_data.get("symbol"), stage_name)
            except Exception as exc:
                errors.append(f"{stage_name}: {exc}")
                self.logger.error(
                    "[PIPELINE_STAGE] %s - %s: FAILED - %s",
                    tick_data.get("symbol"),
                    stage_name,
                    exc,
                )
                if stage_name == "greeks":
                    fallback = self.service._get_default_greeks(tick_data.get("option_type"))
                    tick_data.update(fallback)
                    stage_results[stage_name] = fallback
        if errors:
            self.logger.warning("[PIPELINE_ERRORS] %s - Errors: %s", tick_data.get("symbol"), errors)
        return stage_results, errors

    def _parse_stage(self, tick_data: Dict[str, Any], _: Dict[str, Any]) -> Dict[str, Any]:
        self.service._ensure_underlying_price(tick_data, self.price_service)
        return {
            "symbol": tick_data.get("symbol"),
            "underlying_price": tick_data.get("underlying_price"),
        }

    def _volume_stage(self, tick_data: Dict[str, Any], _: Dict[str, Any]) -> Dict[str, Any]:
        if tick_data.get("volume_ratio") is None:
            tick_data["volume_ratio"] = 0.0
        return {"volume_ratio": tick_data.get("volume_ratio", 0.0)}

    def _greeks_stage(self, tick_data: Dict[str, Any], _: Dict[str, Any]) -> Dict[str, Any]:
        return self.service._run_greeks_stage(tick_data)

    def _indicators_stage(self, tick_data: Dict[str, Any], _: Dict[str, Any]) -> Dict[str, Any]:
        return self.service._run_indicator_stage(tick_data)

    def _classification_stage(self, tick_data: Dict[str, Any], _: Dict[str, Any]) -> Dict[str, Any]:
        return self.service._run_classification_stage(tick_data)


class CalculationService:
    """
    Pure calculation service for multiprocess pipeline.
    
    Features:
    - Greeks calculation (BSM, fast approximation)
    - Technical indicators (RSI, MACD, Bollinger)
    - Microstructure metrics (microprice, imbalance, CVD)
    - VWAP calculation
    
    All methods are stateless and thread-safe.
    """
    
    def __init__(self, risk_free_rate: float = 0.065, redis_client=None):
        """
        Initialize calculation service.
        
        Args:
            risk_free_rate: Risk-free rate for Greeks calculation (default 6.5%)
            redis_client: Optional Redis client for edge calculations
        """
        self.risk_free_rate = risk_free_rate
        self.redis_client = redis_client
        self.logger = logging.getLogger(__name__)
        self._observability_interval = float(os.environ.get("CALC_SERVICE_LOG_INTERVAL", "0.5"))
        self._stage_log_times: Dict[str, float] = {}
        
        # Microstructure state (per-symbol CVD tracking)
        self._cvd_state: Dict[str, float] = {}
        
        # IV cache (short TTL)
        self._iv_cache: Dict[Tuple, Tuple[float, float]] = {}
        self._iv_cache_ttl = 2.0  # 2 seconds
        
        # ✅ Edge calculator (lazy-loaded per symbol)
        self._edge_calculators: Dict[str, Any] = {}
        self._edge_fields = [
            'dhdi', 'motive', 'hedge_pressure_ma',
            'gamma_stress', 'charm_ratio', 'lci',
            'transition_probability', 'time_compression', 'latency_collapse',
            'regime_id', 'should_trade'
        ]

    def _should_log_stage(self, stage: str) -> bool:
        interval = getattr(self, "_observability_interval", 0.0)
        if interval <= 0:
            return False
        now = time.time()
        last_logged = self._stage_log_times.get(stage, 0.0)
        if now - last_logged < interval:
            return False
        self._stage_log_times[stage] = now
        return True

    def _log_pipeline_snapshot(
        self,
        stage: str,
        symbol: str,
        payload: Optional[Dict[str, Any]],
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not payload or not self._should_log_stage(stage):
            return
        try:
            snapshot_fields = {
                "volume_ratio": payload.get("volume_ratio"),
                "delta": payload.get("delta"),
                "gamma": payload.get("gamma"),
                "gamma_exposure": payload.get("gamma_exposure"),
                "order_flow_imbalance": payload.get("order_flow_imbalance"),
                "microprice": payload.get("microprice"),
                "underlying_price": payload.get("underlying_price"),
            }
            if extra:
                snapshot_fields.update(extra)
            snapshot = ", ".join(
                f"{key}={snapshot_fields[key]}"
                for key in snapshot_fields
                if snapshot_fields[key] not in (None, "")
            )
            self.logger.info(
                "🧮 [CALC_OBSERVABILITY] stage=%s symbol=%s %s",
                stage,
                symbol or "UNKNOWN",
                snapshot,
            )
        except Exception as exc:
            self.logger.debug(f"Observability log failed: {exc}")

    def _log_greek_snapshot(
        self,
        symbol: str,
        tick_data: Dict[str, Any],
        greeks: Dict[str, Any],
        inputs: Dict[str, Any],
    ) -> None:
        extra_fields = {
            "underlying_price": inputs.get("underlying_price"),
            "strike_price": inputs.get("strike_price"),
            "dte_years": inputs.get("dte_years"),
            "iv": greeks.get("iv"),
            "gamma_exposure": greeks.get("gamma_exposure"),
        }
        self._log_pipeline_snapshot("greeks", symbol, greeks, extra_fields)
    
    def _run_greeks_stage(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute Greek calculations with fallback defaults."""
        if not self._is_option(tick_data.get('symbol', ''), tick_data):
            return {}
        try:
            greeks = self.calculate_greeks_fast(tick_data)
            tick_data.update(greeks)
            self._log_greek_snapshot(
                tick_data.get('symbol', ''),
                tick_data,
                greeks,
                {
                    "underlying_price": tick_data.get('underlying_price'),
                    "strike_price": tick_data.get('strike_price', tick_data.get('strike')),
                    "dte_years": tick_data.get('dte_years'),
                },
            )
            return greeks
        except Exception as exc:
            self.logger.error("Greeks stage failed for %s: %s", tick_data.get('symbol'), exc)
            fallback = self._get_default_greeks(tick_data.get('option_type'), reason=str(exc))
            tick_data.update(fallback)
            return fallback
    
    def _run_indicator_stage(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """Run indicator/microstructure stage."""
        symbol = tick_data.get('symbol', '')
        result: Dict[str, Any] = {}
        try:
            microstructure = self.calculate_microstructure(symbol, tick_data)
            tick_data.update(microstructure)
            result.update(microstructure)
            
            if self._is_option(symbol, tick_data) and tick_data.get('delta') is not None:
                edge_indicators = self.calculate_edge_indicators(tick_data)
                if edge_indicators:
                    tick_data.update(edge_indicators)
                    result.update(edge_indicators)
            
            if result:
                self._log_pipeline_snapshot("indicators", symbol, result)
        except Exception as exc:
            self.logger.error("Indicators stage failed for %s: %s", symbol, exc)
        return result
    
    def _run_classification_stage(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """Classification placeholder for future ML stages."""
        payload = {
            "volume_ratio": tick_data.get("volume_ratio"),
            "delta": tick_data.get("delta"),
            "gamma": tick_data.get("gamma"),
        }
        self._log_pipeline_snapshot("classification", tick_data.get('symbol', ''), payload)
        return payload
    
    def execute_pipeline(
        self, 
        tick_data: Dict[str, Any],
        price_service=None
    ) -> Dict[str, Any]:
        """
        Execute full calculation pipeline on a tick.
        
        Args:
            tick_data: Parsed tick data
            price_service: UnderlyingPriceService for spot price lookup
            
        Returns:
            Enriched tick_data with indicators, greeks, microstructure, edge
        """
        start_time = time.perf_counter_ns()
        
        symbol = tick_data.get('symbol', '')
        token = tick_data.get('instrument_token')
        
        # ✅ [WORKER_COORD] Checkpoint
        pid = os.getpid()
        self.logger.debug(f"[WORKER_COORD] Worker-{pid} processing {symbol}")
        
        pipeline = CalculationPipeline(self, price_service)
        _, stage_errors = pipeline.process(tick_data)
        
        # Add calculation metadata
        tick_data['_calc_time_us'] = (time.perf_counter_ns() - start_time) / 1000

        self._log_pipeline_snapshot("pipeline", symbol, tick_data)
        if stage_errors:
            self.logger.warning("[PIPELINE_ERRORS] %s - %s", symbol, stage_errors)
        
        return tick_data
    
    def get_edge_calculator(self, symbol: str):
        """Get or create edge calculator for symbol (lazy-loaded)."""
        if symbol not in self._edge_calculators:
            try:
                from intraday_trading.intraday_scanner.calculations_edge import get_edge_calculator
                self._edge_calculators[symbol] = get_edge_calculator(
                    symbol=symbol,
                    redis_client=self.redis_client
                )
                self.logger.debug(f"✅ Edge calculator created for {symbol}")
            except ImportError as e:
                self.logger.warning(f"⚠️ Edge calculator not available: {e}")
                self._edge_calculators[symbol] = None
            except Exception as e:
                self.logger.error(f"❌ Edge calculator init error: {e}")
                self._edge_calculators[symbol] = None
        return self._edge_calculators.get(symbol)
    
    def calculate_edge_indicators(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate intent/hedge/constraint indicators.
        
        Uses RedisStatefulEdgeCalculator for stateful calculations.
        
        Returns:
            Dict with edge_ prefixed indicators
        """
        symbol = tick_data.get('symbol', '')
        if not symbol:
            return {}
        
        edge_calc = self.get_edge_calculator(symbol)
        if edge_calc is None:
            return {}
        
        try:
            # Call edge calculator main method
            enriched = edge_calc.calculate_and_store_edge_indicators(
                tick_data=tick_data,
                options_chain=None,  # Will fetch from Redis if available
                orderbook=tick_data.get('depth') or tick_data.get('order_book')
            )
            
            # Extract edge fields with prefix
            result = {}
            for key in ['edge_intent', 'edge_constraint', 'edge_transition', 'edge_regime', 'edge_should_trade']:
                if key in enriched:
                    result[key] = enriched[key]
            
            # Also flatten common edge indicators for easy access
            if 'edge_intent' in enriched:
                intent = enriched['edge_intent']
                result['edge_dhdi'] = intent.get('dhdi')
                result['edge_motive'] = intent.get('motive')
                result['edge_hedge_pressure'] = intent.get('hedge_pressure_ma')
            
            if 'edge_constraint' in enriched:
                constraint = enriched['edge_constraint']
                result['edge_gamma_stress'] = constraint.get('gamma_stress')
                result['edge_charm_ratio'] = constraint.get('charm_ratio')
                result['edge_lci'] = constraint.get('lci')
            
            if 'edge_regime' in enriched:
                regime = enriched['edge_regime']
                result['edge_regime_id'] = regime.get('regime_id')
                result['edge_regime_label'] = regime.get('regime_label')
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Edge calculation error for {symbol}: {e}")
            return {}
    
    def execute_pipeline_polars(
        self, 
        df: "pl.DataFrame",
        price_service=None
    ) -> "pl.DataFrame":
        """
        Execute calculation pipeline on a Polars DataFrame.
        
        Optimized for batch processing with vectorized operations.
        
        Args:
            df: Polars DataFrame with tick data (using TICK_SCHEMA)
            price_service: UnderlyingPriceService for spot price lookup
            
        Returns:
            DataFrame enriched with Greeks and microstructure metrics
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required for execute_pipeline_polars")
        
        if df is None or df.is_empty():
            return df
        
        start_time = time.perf_counter_ns()
        
        # Get required columns
        has_underlying = 'underlying_price' in df.columns
        has_strike = 'strike_price' in df.columns or 'strike' in df.columns
        has_option_type = 'option_type' in df.columns
        
        # Extract data as numpy arrays for batch calculation
        n = len(df)
        
        # Option Greeks calculation
        if has_strike and has_option_type and n > 0:
            # Extract arrays
            S = df['underlying_price'].to_numpy() if has_underlying else np.zeros(n)
            K = (df['strike_price'] if 'strike_price' in df.columns else df['strike']).to_numpy()
            T = df['dte_years'].to_numpy() if 'dte_years' in df.columns else np.full(n, 0.1)
            prices = df['last_price'].to_numpy() if 'last_price' in df.columns else np.zeros(n)
            option_types = df['option_type'].to_list()
            is_call = np.array([ot.lower() in ('call', 'ce', 'c') if ot else True for ot in option_types])
            
            # Calculate Greeks using Numba if available
            if NUMBA_AVAILABLE and n >= 10:
                iv_arr = _approximate_iv_batch_numba(prices, S, K, T)
                delta_arr, gamma_arr, theta_arr, vega_arr = _calculate_greeks_numba_batch(
                    S, K, T, self.risk_free_rate, iv_arr, is_call
                )
            else:
                # Fallback to manual calculation
                iv_arr = np.zeros(n)
                delta_arr = np.zeros(n)
                gamma_arr = np.zeros(n)
                theta_arr = np.zeros(n)
                vega_arr = np.zeros(n)
                for i in range(n):
                    iv_arr[i] = self._approximate_iv(prices[i], S[i], K[i], T[i], is_call[i])
                    d, g, t, v = self._calculate_greeks_manual(S[i], K[i], T[i], self.risk_free_rate, iv_arr[i], is_call[i])
                    delta_arr[i], gamma_arr[i], theta_arr[i], vega_arr[i] = d, g, t, v
            
            # Add Greeks columns to DataFrame
            df = df.with_columns([
                pl.Series('delta', delta_arr),
                pl.Series('gamma', gamma_arr),
                pl.Series('theta', theta_arr),
                pl.Series('vega', vega_arr),
                pl.Series('iv', iv_arr),
                pl.Series('implied_volatility', iv_arr),
            ])
        
        # Microstructure metrics (vectorized)
        if 'bid_price' in df.columns and 'ask_price' in df.columns:
            # Microprice
            df = df.with_columns([
                ((pl.col('bid_price') * pl.col('ask_quantity') + 
                  pl.col('ask_price') * pl.col('bid_quantity')) / 
                 (pl.col('bid_quantity') + pl.col('ask_quantity'))).alias('microprice'),
                
                ((pl.col('bid_quantity') - pl.col('ask_quantity')) /
                 (pl.col('bid_quantity') + pl.col('ask_quantity'))).alias('order_book_imbalance'),
            ])
        
        # Add calculation time
        calc_time_us = (time.perf_counter_ns() - start_time) / 1000
        df = df.with_columns(pl.lit(calc_time_us / n).alias('_calc_time_us'))
        
        return df
    
    def to_polars_dataframe(self, ticks: List[Dict[str, Any]]) -> "pl.DataFrame":
        """
        Convert list of tick dicts to Polars DataFrame with proper schema.
        
        Args:
            ticks: List of tick data dicts
            
        Returns:
            Polars DataFrame with TICK_SCHEMA types
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required for to_polars_dataframe")
        
        if not ticks:
            return pl.DataFrame()
        
        # Create DataFrame with explicit schema
        schema = {k: v for k, v in TICK_SCHEMA.items()}
        
        # Only include columns that exist in the data
        available_cols = set()
        for tick in ticks[:10]:  # Sample first 10 for performance
            available_cols.update(tick.keys())
        
        filtered_schema = {k: v for k, v in schema.items() if k in available_cols}
        
        try:
            df = pl.DataFrame(ticks, schema=filtered_schema, strict=False)
        except Exception:
            # Fallback: create without schema, then cast
            df = pl.DataFrame(ticks)
            for col, dtype in filtered_schema.items():
                if col in df.columns:
                    try:
                        df = df.with_columns(pl.col(col).cast(dtype, strict=False))
                    except Exception:
                        pass
        
        return df
    
    def _is_option(self, symbol: str, tick_data: Dict[str, Any]) -> bool:
        """Check if symbol is an option."""
        if not symbol:
            return False
        upper = symbol.upper()
        if upper.endswith('CE') or upper.endswith('PE'):
            return True
        option_type = tick_data.get('option_type', '')
        return option_type in ('CE', 'PE', 'call', 'put', 'c', 'p')

    def _get_default_greeks(self, option_type: Optional[str] = None, reason: str = "fallback") -> Dict[str, float]:
        """Provide safe default Greeks when calculation fails."""
        opt = (option_type or "call").lower()
        is_call = opt in ('call', 'ce', 'c')
        return {
            'delta': 0.5 if is_call else -0.5,
            'gamma': 0.0,
            'theta': 0.0,
            'vega': 0.0,
            'charm': 0.0,
            'vanna': 0.0,
            'volga': 0.0,
            'speed': 0.0,
            'color': 0.0,
            'zomma': 0.0,
            'vomma': 0.0,
            'gamma_exposure': 0.0,
            'iv': 0.20,
            '_greeks_error': reason,
        }
    
    def _ensure_underlying_price(self, tick_data: Dict[str, Any], price_service=None) -> None:
        """Resolve underlying price using parser data, index manager, and price service."""
        symbol = tick_data.get('symbol', '')
        token = tick_data.get('instrument_token')
        underlying_price = tick_data.get('underlying_price')
        
        if underlying_price:
            return
        
        if token:
            index_symbol = index_manager.get_index_for_token(token)
            if index_symbol:
                underlying_price = index_manager.get_spot_price(index_symbol)
                if not underlying_price and price_service:
                    try:
                        underlying_price = price_service.get_price(index_symbol)
                    except Exception:
                        pass
        
        if not underlying_price and symbol:
            # ✅ FIXED: Strip exchange prefixes (BFO, NSE, NFO) before matching
            # BFOSENSEX2610185600PE → SENSEX2610185600PE
            # NSENIFTY25D2326200CE → NIFTY25D2326200CE
            symbol_clean = symbol
            for prefix in ['BFO', 'NSE', 'NFO', 'MCX', 'CDS']:
                if symbol.startswith(prefix) and len(symbol) > len(prefix):
                    symbol_clean = symbol[len(prefix):]
                    break
            
            for known_index in ['SENSEX', 'BANKNIFTY', 'NIFTY', 'USD:INR', 'INDIAVIX']:
                if symbol_clean.startswith(known_index) or known_index in symbol:
                    underlying_price = index_manager.get_spot_price(known_index)
                    if underlying_price:
                        self.logger.debug(
                            "[UNDERLYING_LOOKUP] %s - Symbol-based fallback hit: %s=%s",
                            symbol,
                            known_index,
                            underlying_price,
                        )
                        break
        
        if not underlying_price and price_service and 'underlying_price' not in tick_data:
            underlying = price_service.get_underlying_for_option(symbol)
            if underlying:
                try:
                    underlying_price = price_service.get_price(underlying)
                except Exception:
                    pass
        
        if underlying_price:
            tick_data['underlying_price'] = underlying_price
            self.logger.debug("[UNDERLYING_LOOKUP] %s - Found price: %s", symbol, underlying_price)
        else:
            self.logger.debug(
                "[UNDERLYING_LOOKUP] %s - Price lookup FAILED (Checked Parser Inject, IndexManager, and legacy PriceService)",
                symbol,
            )
    
    # ========================================================================
    # GREEKS CALCULATIONS
    # ========================================================================
    
    def calculate_greeks_fast(self, tick_data: Dict[str, Any]) -> Dict[str, float]:
        """
        Fast Greek calculation using analytic BSM formulas.
        
        Computes both first-order Greeks (delta, gamma, theta, vega) and
        second-order Greeks (charm, vanna, volga) for advanced analysis.
        
        Falls back to approximation if py_vollib not available.
        """
        symbol = tick_data.get('symbol', '')
        # Extract parameters
        S = float(tick_data.get('underlying_price', 0) or 0)
        K = float(tick_data.get('strike_price', tick_data.get('strike', 0)) or 0)
        T = float(tick_data.get('dte_years', tick_data.get('time_to_expiry', 0.1)) or 0.1)
        r = self.risk_free_rate
        option_price = float(tick_data.get('last_price', 0) or 0)
        option_type_raw = tick_data.get('option_type', '')
        option_type = str(option_type_raw).lower() if option_type_raw else ''
        
        # ✅ FIXED: Parse option_type from symbol if not in tick_data
        # NSENIFTY25DEC26000PE → PE (put), NSENIFTY25DEC26000CE → CE (call)
        if not option_type:
            symbol_upper = symbol.upper()
            if symbol_upper.endswith('PE') or symbol_upper.endswith('P'):
                option_type = 'put'
            elif symbol_upper.endswith('CE') or symbol_upper.endswith('C'):
                option_type = 'call'
            else:
                option_type = 'call'  # Default to call only if truly unknown
        
        is_call = option_type in ('call', 'ce', 'c')
        
        # Guard against invalid inputs
        if S <= 0 or K <= 0 or T <= 0:
            return self._get_default_greeks(option_type_raw, reason="invalid_inputs")
        
        # Calculate IV
        iv = self._calculate_iv(option_price, S, K, T, is_call)
        if iv is None or iv <= 0:
            iv = 0.20  # Default 20% IV
        
        # Calculate Greeks using analytic BSM
        try:
            if PY_VOLLIB_AVAILABLE and iv > 0:
                # Use py_vollib for accurate Greeks
                flag = 'c' if is_call else 'p'
                delta = bs_delta(flag, S, K, T, r, iv)
                gamma = bs_gamma(flag, S, K, T, r, iv)
                theta = bs_theta(flag, S, K, T, r, iv)
                vega = bs_vega(flag, S, K, T, r, iv)
            else:
                # Fallback to manual BSM
                delta, gamma, theta, vega = self._calculate_greeks_manual(S, K, T, r, iv, is_call)
        except Exception as e:
            logger.debug(f"Greeks calculation failed: {e}")
            delta, gamma, theta, vega = self._calculate_greeks_manual(S, K, T, r, iv, is_call)
        
        # ====================================================================
        # ADVANCED GREEKS: Charm, Vanna, Volga, Speed, Color, Zomma, Vomma
        # Using BSM d1/d2 partial derivatives
        # ====================================================================
        charm = 0.0
        vanna = 0.0
        volga = 0.0
        speed = 0.0
        color = 0.0
        zomma = 0.0
        vomma = 0.0
        gamma_exposure = 0.0
        
        try:
            sqrt_T = math.sqrt(T)
            d1 = (math.log(S / K) + (r + 0.5 * iv**2) * T) / (iv * sqrt_T)
            d2 = d1 - iv * sqrt_T
            
            # Standard normal PDF
            n_d1 = math.exp(-0.5 * d1**2) / math.sqrt(2 * math.pi)
            
            # Standard normal CDF approximation
            def norm_cdf(x):
                return 1.0 / (1.0 + math.exp(-0.07056 * x**3 - 1.5976 * x))
            
            N_d1 = norm_cdf(d1)
            N_d2 = norm_cdf(d2)
            
            # ==================== FIRST-ORDER SENSITIVITIES ====================
            
            # Charm (Delta decay): dDelta/dT
            if T > 0.001 and iv > 0.01:
                charm_numerator = 2 * r * T - d2 * iv * sqrt_T
                charm = -n_d1 * charm_numerator / (2 * T * iv * sqrt_T)
                charm = charm / 365.0  # Per-day charm
            
            # Vanna: dDelta/dIV = dVega/dS
            if iv > 0.01:
                vanna = -n_d1 * d2 / iv
            
            # Volga (Vomma): dVega/dIV
            if iv > 0.01 and vega != 0:
                volga = vega * (d1 * d2) / iv
                vomma = volga  # Vomma is same as Volga
            
            # ==================== SECOND-ORDER SENSITIVITIES ====================
            
            # Speed: dGamma/dS (rate of gamma change with spot)
            if S > 0.01 and iv > 0.01 and sqrt_T > 0:
                speed = -gamma * (1 + d1 / (iv * sqrt_T)) / S
            
            # Zomma: dGamma/dIV (rate of gamma change with volatility)
            if iv > 0.01 and gamma != 0:
                zomma = gamma * (d1 * d2 - 1) / iv
            
            # Color: dGamma/dT (gamma decay)
            if T > 0.001 and iv > 0.01 and sqrt_T > 0:
                term1 = 2 * r * T - d2 * iv * sqrt_T
                color = -n_d1 / (2 * S * T * iv * sqrt_T) * (term1 / (iv * sqrt_T) + 1)
                color = color / 365.0  # Per-day color
            
            # ==================== GAMMA EXPOSURE (GEX) ====================
            # GEX = Gamma * OI * Contract_Multiplier * Spot^2 / 100
            oi = float(tick_data.get('open_interest', tick_data.get('oi', 0)) or 0)
            lot_size = float(tick_data.get('lot_size', 50) or 50)  # Default NIFTY lot
            if gamma != 0 and oi > 0:
                # Positive for calls, negative for puts
                sign = 1 if is_call else -1
                gamma_exposure = sign * gamma * oi * lot_size * (S ** 2) / 100
                
        except Exception as e:
            logger.debug(f"Advanced Greeks calculation failed: {e}")
        
        greeks_payload = {
            'delta': round(delta, 4),
            'gamma': round(gamma, 6),
            'theta': round(theta, 4),
            'vega': round(vega, 4),
            'charm': round(charm, 6),
            'vanna': round(vanna, 4),
            'volga': round(volga, 4),
            'speed': round(speed, 6),
            'color': round(color, 6),
            'zomma': round(zomma, 6),
            'vomma': round(vomma, 6),
            'gamma_exposure': round(gamma_exposure, 2),
            'iv': round(iv, 4),
            'implied_volatility': round(iv, 4),
        }
        self._log_greek_snapshot(
            symbol,
            tick_data,
            greeks_payload,
            {
                'underlying_price': S,
                'strike_price': K,
                'dte_years': T,
                'option_type': option_type,
            },
        )
        return greeks_payload

    
    def calculate_greeks_batch(
        self, 
        ticks: List[Dict[str, Any]]
    ) -> List[Dict[str, float]]:
        """
        Calculate Greeks for multiple options at once using parallel execution.
        
        Priority:
        1. Numba JIT with parallel execution (fastest for 1000+ options)
        2. py_vollib_vectorized
        3. Individual calculation fallback
        
        Args:
            ticks: List of tick data dicts with underlying_price, strike_price, etc.
            
        Returns:
            List of Greek dicts corresponding to input ticks
        """
        if not ticks:
            return []
        
        n = len(ticks)
        
        # Extract arrays
        S = np.zeros(n, dtype=np.float64) if NUMPY_AVAILABLE else [0.0] * n
        K = np.zeros(n, dtype=np.float64) if NUMPY_AVAILABLE else [0.0] * n
        T = np.zeros(n, dtype=np.float64) if NUMPY_AVAILABLE else [0.1] * n
        prices = np.zeros(n, dtype=np.float64) if NUMPY_AVAILABLE else [0.0] * n
        is_call = np.ones(n, dtype=np.bool_) if NUMPY_AVAILABLE else [True] * n
        
        for i, tick in enumerate(ticks):
            S[i] = float(tick.get('underlying_price', 0) or 0)
            K[i] = float(tick.get('strike_price', tick.get('strike', 0)) or 0)
            T[i] = float(tick.get('dte_years', tick.get('time_to_expiry', 0.1)) or 0.1)
            prices[i] = float(tick.get('last_price', 0) or 0)
            option_type = str(tick.get('option_type', 'call')).lower()
            is_call[i] = option_type in ('call', 'ce', 'c')
        
        r = self.risk_free_rate
        
        # PRIORITY 1: Numba JIT acceleration (best for 100+ options - JIT compile overhead)
        if NUMBA_AVAILABLE and NUMPY_AVAILABLE and n >= 100:
            try:
                # Approximate IV using Numba
                iv_arr = _approximate_iv_batch_numba(prices, S, K, T)
                
                # Calculate Greeks using Numba parallel execution
                delta_arr, gamma_arr, theta_arr, vega_arr = _calculate_greeks_numba_batch(
                    S, K, T, r, iv_arr, is_call
                )
                
                # Convert to list of dicts
                results = []
                for i in range(n):
                    results.append({
                        'delta': round(float(delta_arr[i]), 4),
                        'gamma': round(float(gamma_arr[i]), 6),
                        'theta': round(float(theta_arr[i]), 4),
                        'vega': round(float(vega_arr[i]), 4),
                        'iv': round(float(iv_arr[i]), 4),
                        'implied_volatility': round(float(iv_arr[i]), 4),
                        '_numba': True,
                    })
                return results
                
            except Exception as e:
                logger.debug(f"Numba batch Greeks failed: {e}")
        
        # PRIORITY 2: py_vollib_vectorized
        if PY_VOLLIB_VECTORIZED_AVAILABLE and NUMPY_AVAILABLE:
            try:
                flags = np.array(['c' if c else 'p' for c in is_call])
                r_arr = np.full(n, r, dtype=np.float64)
                
                # Vectorized IV calculation
                try:
                    iv_arr = vec_iv(prices, S, K, T, r_arr, flags, return_nan=True)
                except Exception:
                    iv_arr = np.full(n, 0.20)
                
                iv_arr = np.nan_to_num(iv_arr, nan=0.20)
                iv_arr = np.clip(iv_arr, 0.01, 5.0)
                
                # Vectorized Greeks
                delta_arr = vectorized_delta(flags, S, K, T, r_arr, iv_arr)
                gamma_arr = vectorized_gamma(flags, S, K, T, r_arr, iv_arr)
                theta_arr = vectorized_theta(flags, S, K, T, r_arr, iv_arr)
                vega_arr = vectorized_vega(flags, S, K, T, r_arr, iv_arr)
                
                results = []
                for i in range(n):
                    results.append({
                        'delta': round(float(delta_arr[i]), 4),
                        'gamma': round(float(gamma_arr[i]), 6),
                        'theta': round(float(theta_arr[i]), 4),
                        'vega': round(float(vega_arr[i]), 4),
                        'iv': round(float(iv_arr[i]), 4),
                        'implied_volatility': round(float(iv_arr[i]), 4),
                        '_vollib_vectorized': True,
                    })
                return results
                
            except Exception as e:
                logger.debug(f"Vectorized Greeks failed: {e}")
        
        # PRIORITY 3: Individual calculation fallback
        return [self.calculate_greeks_fast(tick) for tick in ticks]
    
    def _calculate_iv(
        self, 
        price: float, 
        S: float, 
        K: float, 
        T: float, 
        is_call: bool
    ) -> Optional[float]:
        """Calculate implied volatility with caching."""
        if price <= 0 or S <= 0 or K <= 0 or T <= 0:
            return None
        
        # Check cache
        cache_key = (round(S, 0), round(K, 0), round(T * 100, 0), is_call)
        current_time = time.time()
        
        if cache_key in self._iv_cache:
            cached_iv, cached_time = self._iv_cache[cache_key]
            if current_time - cached_time < self._iv_cache_ttl:
                return cached_iv
        
        try:
            if PY_VOLLIB_AVAILABLE:
                flag = 'c' if is_call else 'p'
                iv = implied_volatility(price, S, K, T, self.risk_free_rate, flag)
            else:
                iv = self._approximate_iv(price, S, K, T, is_call)
            
            # Validate IV range
            if iv and 0.01 < iv < 5.0:
                self._iv_cache[cache_key] = (iv, current_time)
                return iv
        except Exception:
            pass
        
        # Fallback approximation
        return self._approximate_iv(price, S, K, T, is_call)
    
    def _approximate_iv(
        self, 
        price: float, 
        S: float, 
        K: float, 
        T: float, 
        is_call: bool
    ) -> float:
        """
        Fast IV approximation for ATM options.
        
        Uses Brenner-Subrahmanyam approximation:
        IV ≈ price * sqrt(2 * pi / T) / S
        """
        if S <= 0 or T <= 0:
            return 0.20
        
        # Brenner-Subrahmanyam approximation
        iv = price * math.sqrt(2 * math.pi / T) / S
        
        # Adjust for moneyness
        moneyness = S / K if K > 0 else 1.0
        if moneyness < 0.95 or moneyness > 1.05:
            # OTM/ITM adjustment
            iv *= 1.1
        
        # Clamp to reasonable range
        return max(0.05, min(iv, 2.0))
    
    def _calculate_greeks_manual(
        self, 
        S: float, 
        K: float, 
        T: float, 
        r: float, 
        iv: float, 
        is_call: bool
    ) -> Tuple[float, float, float, float]:
        """Manual Black-Scholes Greeks calculation."""
        if iv <= 0 or T <= 0:
            return (0.5 if is_call else -0.5, 0.0, 0.0, 0.0)
        
        sqrt_T = math.sqrt(T)
        d1 = (math.log(S / K) + (r + 0.5 * iv**2) * T) / (iv * sqrt_T)
        d2 = d1 - iv * sqrt_T
        
        # Standard normal CDF approximation
        def norm_cdf(x):
            return 1.0 / (1.0 + math.exp(-0.07056 * x**3 - 1.5976 * x))
        
        def norm_pdf(x):
            return math.exp(-0.5 * x**2) / math.sqrt(2 * math.pi)
        
        N_d1 = norm_cdf(d1)
        N_d2 = norm_cdf(d2)
        n_d1 = norm_pdf(d1)
        
        # Greeks
        if is_call:
            delta = N_d1
            theta = (-(S * n_d1 * iv) / (2 * sqrt_T) - r * K * math.exp(-r * T) * N_d2) / 365.0
        else:
            delta = N_d1 - 1
            theta = (-(S * n_d1 * iv) / (2 * sqrt_T) + r * K * math.exp(-r * T) * (1 - N_d2)) / 365.0
        
        gamma = n_d1 / (S * iv * sqrt_T) if S > 0 else 0.0
        vega = S * n_d1 * sqrt_T / 100.0
        
        return (delta, gamma, theta, vega)
    
    # ========================================================================
    # MICROSTRUCTURE CALCULATIONS
    # ========================================================================
    
    def calculate_microstructure(
        self, 
        symbol: str, 
        tick_data: Dict[str, Any]
    ) -> Dict[str, float]:
        """
        Calculate microstructure metrics.
        
        Returns:
            microprice, order_book_imbalance, cvd
        """
        result = {}
        
        # Extract bid/ask data
        bid = float(tick_data.get('bid_price', tick_data.get('best_bid_price', 0)) or 0)
        ask = float(tick_data.get('ask_price', tick_data.get('best_ask_price', 0)) or 0)
        bid_size = float(tick_data.get('bid_quantity', tick_data.get('best_bid_quantity', 0)) or 0)
        ask_size = float(tick_data.get('ask_quantity', tick_data.get('best_ask_quantity', 0)) or 0)
        
        # Microprice - weighted midpoint
        if bid > 0 and ask > 0 and (bid_size + ask_size) > 0:
            microprice = (bid * ask_size + ask * bid_size) / (bid_size + ask_size)
            result['microprice'] = round(microprice, 4)
        
        # Order book imbalance (-1 to +1)
        if (bid_size + ask_size) > 0:
            imbalance = (bid_size - ask_size) / (bid_size + ask_size)
            result['order_book_imbalance'] = round(imbalance, 4)
        
        # Cumulative Volume Delta (CVD)
        last_price = float(tick_data.get('last_price', 0) or 0)
        volume = float(tick_data.get('volume', tick_data.get('last_traded_quantity', 0)) or 0)
        
        if symbol and volume > 0:
            # Determine aggressor side based on price vs mid
            mid = (bid + ask) / 2 if bid > 0 and ask > 0 else last_price
            if last_price >= mid:
                delta_volume = volume  # Buy aggressor
            else:
                delta_volume = -volume  # Sell aggressor
            
            # Update CVD
            prev_cvd = self._cvd_state.get(symbol, 0.0)
            new_cvd = prev_cvd + delta_volume
            self._cvd_state[symbol] = new_cvd
            result['cvd'] = round(new_cvd, 2)
        
        return result
    
    # ========================================================================
    # TECHNICAL INDICATORS
    # ========================================================================
    
    def calculate_rsi(self, prices: List[float], period: int = 14) -> Optional[float]:
        """
        Calculate RSI using TA-Lib or manual calculation.
        """
        if len(prices) < period + 1:
            return None
        
        if TALIB_AVAILABLE and NUMPY_AVAILABLE:
            try:
                arr = np.array(prices, dtype=np.float64)
                rsi = talib.RSI(arr, timeperiod=period)
                if len(rsi) > 0 and not np.isnan(rsi[-1]):
                    return round(float(rsi[-1]), 2)
            except Exception:
                pass
        
        # Manual RSI calculation
        return self._calculate_rsi_manual(prices, period)
    
    def _calculate_rsi_manual(self, prices: List[float], period: int) -> Optional[float]:
        """Manual RSI calculation."""
        if len(prices) < period + 1:
            return None
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        if len(gains) < period:
            return None
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return round(rsi, 2)
    
    def calculate_ema(self, prices: List[float], period: int) -> Optional[float]:
        """Calculate EMA."""
        if len(prices) < period:
            return None
        
        if TALIB_AVAILABLE and NUMPY_AVAILABLE:
            try:
                arr = np.array(prices, dtype=np.float64)
                ema = talib.EMA(arr, timeperiod=period)
                if len(ema) > 0 and not np.isnan(ema[-1]):
                    return round(float(ema[-1]), 4)
            except Exception:
                pass
        
        # Manual EMA
        alpha = 2 / (period + 1)
        ema = prices[0]
        for price in prices[1:]:
            ema = alpha * price + (1 - alpha) * ema
        return round(ema, 4)
    
    def calculate_macd(
        self, 
        prices: List[float], 
        fast: int = 12, 
        slow: int = 26, 
        signal: int = 9
    ) -> Optional[Dict[str, float]]:
        """Calculate MACD using TA-Lib or manual."""
        if len(prices) < slow + signal:
            return None
        
        if TALIB_AVAILABLE and NUMPY_AVAILABLE:
            try:
                arr = np.array(prices, dtype=np.float64)
                macd, macd_signal, macd_hist = talib.MACD(arr, fast, slow, signal)
                if not np.isnan(macd[-1]):
                    return {
                        'macd': round(float(macd[-1]), 4),
                        'macd_signal': round(float(macd_signal[-1]), 4),
                        'macd_histogram': round(float(macd_hist[-1]), 4),
                    }
            except Exception:
                pass
        
        # Manual MACD
        fast_ema = self.calculate_ema(prices, fast)
        slow_ema = self.calculate_ema(prices, slow)
        
        if fast_ema is None or slow_ema is None:
            return None
        
        macd_line = fast_ema - slow_ema
        return {
            'macd': round(macd_line, 4),
            'macd_signal': 0.0,  # Would need signal EMA
            'macd_histogram': 0.0,
        }
    
    def calculate_bollinger_bands(
        self, 
        prices: List[float], 
        period: int = 20, 
        std_dev: float = 2.0
    ) -> Optional[Dict[str, float]]:
        """Calculate Bollinger Bands."""
        if len(prices) < period:
            return None
        
        if TALIB_AVAILABLE and NUMPY_AVAILABLE:
            try:
                arr = np.array(prices, dtype=np.float64)
                upper, middle, lower = talib.BBANDS(arr, timeperiod=period, nbdevup=std_dev, nbdevdn=std_dev)
                if not np.isnan(upper[-1]):
                    return {
                        'bb_upper': round(float(upper[-1]), 4),
                        'bb_middle': round(float(middle[-1]), 4),
                        'bb_lower': round(float(lower[-1]), 4),
                    }
            except Exception:
                pass
        
        # Manual calculation
        recent = prices[-period:]
        sma = sum(recent) / period
        std = (sum((x - sma) ** 2 for x in recent) / period) ** 0.5
        
        return {
            'bb_upper': round(sma + std_dev * std, 4),
            'bb_middle': round(sma, 4),
            'bb_lower': round(sma - std_dev * std, 4),
        }
    
    def calculate_vwap(
        self, 
        prices: List[float], 
        volumes: List[float]
    ) -> Optional[float]:
        """Calculate VWAP."""
        if not prices or not volumes or len(prices) != len(volumes):
            return None
        
        total_volume = sum(volumes)
        if total_volume == 0:
            return None
        
        vwap = sum(p * v for p, v in zip(prices, volumes)) / total_volume
        return round(vwap, 4)
    
    def calculate_atr(
        self, 
        highs: List[float], 
        lows: List[float], 
        closes: List[float], 
        period: int = 14
    ) -> Optional[float]:
        """Calculate ATR (Average True Range)."""
        if len(highs) < period + 1:
            return None
        
        if TALIB_AVAILABLE and NUMPY_AVAILABLE:
            try:
                h = np.array(highs, dtype=np.float64)
                l = np.array(lows, dtype=np.float64)
                c = np.array(closes, dtype=np.float64)
                atr = talib.ATR(h, l, c, timeperiod=period)
                if len(atr) > 0 and not np.isnan(atr[-1]):
                    return round(float(atr[-1]), 4)
            except Exception:
                pass
        
        # Manual ATR
        trs = []
        for i in range(1, len(highs)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
            trs.append(tr)
        
        if len(trs) < period:
            return None
        
        atr = sum(trs[-period:]) / period
        return round(atr, 4)


# Factory function
def create_calculation_service(risk_free_rate: float = 0.065) -> CalculationService:
    """Create CalculationService instance."""
    return CalculationService(risk_free_rate=risk_free_rate)


# Module-level singleton
_default_service: Optional[CalculationService] = None


def get_calculation_service() -> CalculationService:
    """Get or create default calculation service singleton."""
    global _default_service
    if _default_service is None:
        _default_service = CalculationService()
    return _default_service
