#!/opt/homebrew/bin/python3.13
"""
ICT Pattern Detector Module
Consolidated ICT pattern orchestration with centralized math engines.
Updated: 2025-10-27
"""
from __future__ import annotations

import logging
import random
import os
import sys
from typing import Dict, Any, List, Optional

# ✅ FIX: Add intraday_trading to sys.path for utils imports
_ict_file_dir = os.path.dirname(os.path.abspath(__file__))  # patterns/ict/
_patterns_dir = os.path.dirname(_ict_file_dir)  # patterns/
_intraday_trading_root = os.path.dirname(_patterns_dir)  # intraday_trading/
if _intraday_trading_root not in sys.path:
    sys.path.insert(0, _intraday_trading_root)

from ..pattern_mathematics import PatternMathematics
from intraday_trading.intraday_scanner.math_dispatcher import MathDispatcher
MATH_DISPATCHER_AVAILABLE = True
from ..utils.pattern_schema import create_pattern, validate_pattern
from ..utils.ict_helpers import ICTRiskManager, ICTRegimeFilter
from .liquidity import ICTLiquidityDetector
from .fvg import ICTFVGDetector
from .ote import ICTOTECalculator
from .killzone import ICTKillzoneDetector
from .premium_discount import ICTPremiumDiscountDetector

# ICT Pattern Classes (Consolidated from individual files)
from datetime import date, timedelta

# ✅ FIXED: Use intraday_trading.config.thresholds
# ✅ FIXED: Use shared_core.config_utils.thresholds (single source of truth)
# Note: get_volume_threshold is legacy - using get_pattern_volume_requirement instead
from shared_core.config_utils.thresholds import get_pattern_volume_requirement

# ✅ FIX: Conditional import for vix_utils
try:
    from shared_core.utils.vix_utils import get_vix_regime
    VIX_UTILS_AVAILABLE = True
except ImportError:
    # Fallback: try direct file import
    try:
        import importlib.util
        vix_utils_path = os.path.join(_intraday_trading_root, "utils", "vix_utils.py")
        if os.path.exists(vix_utils_path):
            spec = importlib.util.spec_from_file_location("vix_utils", vix_utils_path)
            vix_utils_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(vix_utils_module)
            get_vix_regime = vix_utils_module.get_vix_regime
            VIX_UTILS_AVAILABLE = True
        else:
            raise ImportError(f"vix_utils.py not found at {vix_utils_path}")
    except Exception:
        # Last resort: create dummy function
        def get_vix_regime():
            return "NORMAL"  # Default fallback
        VIX_UTILS_AVAILABLE = False

logger = logging.getLogger(__name__)


class ICTPatternDetector:
    """
    Orchestrator for ICT-based patterns. Consumes Redis buckets and current indicators
    to produce actionable ICT setups for F&O scalping.
    """

    def __init__(self, redis_client=None):
        self.redis_client = redis_client
        self.liquidity = ICTLiquidityDetector(redis_client)
        self.fvg = ICTFVGDetector()
        self.ote = ICTOTECalculator()
        self.killzone = ICTKillzoneDetector()
        self.pd = ICTPremiumDiscountDetector(redis_client)
        self.risk = ICTRiskManager()
        
        # ✅ CENTRALIZED: Initialize UnifiedAlertBuilder for algo-ready payloads
        try:
            from alerts.unified_alert_builder import get_unified_alert_builder
            self.alert_builder = get_unified_alert_builder(redis_client=redis_client)
        except ImportError as e:
            self.logger.warning(f"⚠️ UnifiedAlertBuilder not available: {e}")
            self.alert_builder = None
        # ✅ ENHANCED: Pass redis_client for VIX fetching from Redis DB1
        # ✅ FIX: Import MathDispatcher here if not available at module level
        math_dispatcher_class = MathDispatcher
        if not MATH_DISPATCHER_AVAILABLE or math_dispatcher_class is None:
            try:
                from intraday_trading.intraday_scanner.math_dispatcher import MathDispatcher as MathDispatcherClass
                math_dispatcher_class = MathDispatcherClass
            except ImportError:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning("MathDispatcher not available - ICT pattern confidence calculations may be limited")
                math_dispatcher_class = None
        
        if math_dispatcher_class is not None:
            self.math_dispatcher = math_dispatcher_class(PatternMathematics, None, redis_client=redis_client)
        else:
            self.math_dispatcher = None
            import logging
            logger = logging.getLogger(__name__)
            logger.warning("MathDispatcher not initialized for ICT patterns - using fallback")
        # Lightweight lifetime stats
        self._stats = {
            "liquidity_pools": 0,
            "fvg_zones": 0,
            "ote_opportunities": 0,
            "premium_discount_setups": 0,
            "patterns_created_schema": 0,
            "patterns_validated": 0,
            "patterns_failed_validation": 0,
            "ict_patterns_created": 0,
        }
        from collections import defaultdict as _dd

        self._setups_by_type = _dd(int)
        self.regime_filter = ICTRegimeFilter(redis_client)
        
        # ✅ CENTRALIZED: Initialize UnifiedAlertBuilder for algo-ready payloads
        try:
            from alerts.unified_alert_builder import get_unified_alert_builder
            self.alert_builder = get_unified_alert_builder(redis_client=redis_client)
        except ImportError as e:
            logger.warning(f"⚠️ UnifiedAlertBuilder not available: {e}")
            self.alert_builder = None

    def _resolve_confidence(
        self,
        pattern_type: str,
        context: Dict[str, Any],
        fallback,
    ) -> Optional[float]:
        """
        Route confidence calculations through the centralized dispatcher.

        Args:
            pattern_type: Canonical ICT pattern identifier.
            context: Payload describing the current setup environment.
            fallback: Callable or static value used when dispatcher math is unavailable.
        """
        dispatched = None
        if self.math_dispatcher:
            try:
                dispatched = self.math_dispatcher.calculate_confidence(pattern_type, context)
            except Exception as dispatch_error:
                pass

        if dispatched is not None:
            return dispatched

        return fallback() if callable(fallback) else fallback

    def _get_recent_candles(self, symbol: str, count: int = 30) -> List[Dict[str, Any]]:
        candles: List[Dict[str, Any]] = []
        try:
            if not self.redis_client:
                return candles
            
            # Try to get OHLC buckets first
            buckets = self.redis_client.get_ohlc_buckets(symbol, count=count)
            
            # If no OHLC buckets, try time buckets as fallback
            if not buckets:
                time_buckets = self.redis_client.get_time_buckets(symbol)
                if time_buckets:
                    # Convert time buckets to OHLC format
                    buckets = time_buckets[-count:] if len(time_buckets) > count else time_buckets
            
            # Normalize to candle structure expected by ICTFVGDetector
            for b in buckets:
                candles.append(
                    {
                        "open": b.get("open", b.get("close", 0)),
                        "high": b.get("high", b.get("close", 0)),
                        "low": b.get("low", b.get("close", 0)),
                        "close": b.get("close", 0),
                        "timestamp": b.get("first_timestamp", 0),
                        "volume": b.get("volume", 0),
                    }
                )
        except Exception as e:
            # Log the error for debugging
            import logging
            logger = logging.getLogger(__name__)
        return candles

    def _guess_impulse_move(
        self, candles: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        if len(candles) < 5:
            return None
        try:
            recent = candles[-5:]
            highs = [c.get("high") for c in recent]
            lows = [c.get("low") for c in recent]
            if None in highs or None in lows:
                return None
            high = max(highs)
            low = min(lows)
            direction = "up" if recent[-1]["close"] >= recent[0]["open"] else "down"
            return {"high": float(high), "low": float(low), "direction": direction}
        except Exception:
            return None

    def detect_ict_setups(
        self, symbol: str, indicators: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        # Get VIX regime for dynamic thresholds
        vix_level = indicators.get('vix_level', 0)
        vix_regime = get_vix_regime()
        setups: List[Dict[str, Any]] = []

        # Use centralized killzone detection - single source of truth
        # Get killzone from PatternDetector's centralized method
        kz = None
        if hasattr(self, 'pattern_detector') and self.pattern_detector:
            kz = self.pattern_detector._get_current_killzone()
        elif hasattr(self, '_get_current_killzone'):
            kz = self._get_current_killzone()
        
        is_high_priority_killzone = kz and kz.get("priority") == "high"
        
        # Log killzone status for debugging
        if kz:
            print(f"🔍 [ICT_KILLZONE] Current killzone: {kz.get('zone', 'unknown')} priority: {kz.get('priority', 'unknown')}")
        else:
            print(f"🔍 [ICT_KILLZONE] No killzone data available")

        last_price = float(indicators.get("last_price") or 0)
        if last_price <= 0:
            return setups

        # Use pre-calculated indicators for liquidity pools (no calculations here)
        pools = {
            "previous_day_high": indicators.get("resistance_level"),
            "previous_day_low": indicators.get("support_level"),
            "weekly_high": indicators.get("resistance_level"),  # Use same for now
            "weekly_low": indicators.get("support_level"),     # Use same for now
        }
        
        # Count discovered pools (non-empty)
        try:
            present = [pools.get(k) for k in ("previous_day_high", "previous_day_low", "weekly_high", "weekly_low")]
            self._stats["liquidity_pools"] += sum(1 for x in present if x)
        except Exception:
            pass

        # Use pre-calculated indicators for FVG detection (no calculations here)
        # FVG detection should use pre-calculated OHLC data from indicators
        candles = [{
            "open": indicators.get("open", indicators.get("last_price", 0)),
            "high": indicators.get("high", indicators.get("last_price", 0)),
            "low": indicators.get("low", indicators.get("last_price", 0)),
            "close": indicators.get("last_price", 0),
            "timestamp": indicators.get("timestamp_ms", 0)
        }]
        
        fvgs = self.fvg.detect_fvg(candles)
        try:
            self._stats["fvg_zones"] += len(fvgs)
        except Exception:
            pass

        # Use pre-calculated indicators for premium/discount (no calculations here)
        pd_input = {
            "vwap": indicators.get("vwap"),
            "last_price": last_price,
            "atr": indicators.get("atr"),
        }
        pd_zones = self.pd.calculate_premium_discount_zones(pd_input)

        # Simple confidence base - adjust based on killzone priority
        if is_high_priority_killzone:
            base_conf = 0.76
        elif kz and kz.get("priority") == "medium":
            base_conf = 0.65
        elif kz:
            base_conf = 0.55  # Low priority killzone
        else:
            base_conf = 0.45  # No killzone data
        # Use centralized mathematical integrity for volume ratio
        vr = PatternMathematics.protect_outliers(float(indicators.get("volume_ratio", 1.0)))
        dispatcher_conf = None
        if self.math_dispatcher:
            ict_context = {
                "pattern_type": "ict_core",
                "volume_ratio": vr,
                "base_confidence": base_conf,
                "killzone_priority": kz.get("priority") if kz else None,
            }
            try:
                dispatcher_conf = self.math_dispatcher.calculate_confidence('ict_core', ict_context)
            except Exception as dispatch_error:
                print(f"[ICT_PATTERN] MathDispatcher confidence error: {dispatch_error}")

        if dispatcher_conf is not None:
            conf = dispatcher_conf
        else:
            # ✅ FIXED: Use private helper for combining base confidence with bonus (special case)
            conf = PatternMathematics._calculate_bounded_confidence(
                base_conf,
                min(0.15, max(0.0, (vr - 1.0) * 0.02)),
                max_bonus=0.15,
            )

        # Debug logging (reduced frequency)
        if random.random() < 0.5:  # 50% reduction
            print(f"🔍 [ICT_DEBUG] Symbol: {symbol}, Price: {last_price}, Volume Ratio: {vr:.2f}, Base Conf: {base_conf:.2f}")
            print(f"🔍 [ICT_DEBUG] Liquidity pools: {len([k for k, v in pools.items() if v])} found")
            print(f"🔍 [ICT_DEBUG] FVGs: {len(fvgs)} found")
            print(f"🔍 [ICT_DEBUG] PD zones: {len(pd_zones)} found")
        
        # Use pre-calculated indicators for ICT patterns (no calculations here)
        # ICT Momentum patterns
        price_change = float(indicators.get("price_change", 0))
        buy_pressure = float(indicators.get("buy_pressure", 0.5))
        
        if vr >= 2.0 and price_change > 0.005 and buy_pressure > 0.55:
            momentum_context = {
                "symbol": symbol,
                "volume_ratio": vr,
                "price_change_pct": price_change,
                "buy_pressure": buy_pressure,
                "base_confidence": conf,
                "killzone_priority": kz.get("priority") if kz else None,
                "vix_regime": vix_regime,
            }
            momentum_conf = self._resolve_confidence(
                "ict_momentum_bullish",
                momentum_context,
                lambda: PatternMathematics.calculate_confidence("ict_momentum", momentum_context, redis_client=self.redis_client) or conf,
            )
            setups.append({
                "pattern": "ict_momentum_bullish",
                "confidence": momentum_conf,
                "signal": "BUY",
                "volume_ratio": vr,
                "price_change": price_change,
                "buy_pressure": buy_pressure,
                "math_context": momentum_context,
            })
            print(f"✅ [ICT_PATTERN] Found ict_momentum_bullish for {symbol}")
            
        if vr >= 2.0 and price_change < -0.005 and buy_pressure < 0.45:
            momentum_context = {
                "symbol": symbol,
                "volume_ratio": vr,
                "price_change_pct": price_change,
                "buy_pressure": buy_pressure,
                "base_confidence": conf,
                "killzone_priority": kz.get("priority") if kz else None,
                "vix_regime": vix_regime,
            }
            momentum_conf = self._resolve_confidence(
                "ict_momentum_bearish",
                momentum_context,
                lambda: PatternMathematics.calculate_confidence("ict_momentum", momentum_context, redis_client=self.redis_client) or conf,
            )
            setups.append({
                "pattern": "ict_momentum_bearish", 
                "confidence": momentum_conf,
                "signal": "SELL",
                "volume_ratio": vr,
                "price_change": price_change,
                "buy_pressure": buy_pressure,
                "math_context": momentum_context,
            })
            print(f"✅ [ICT_PATTERN] Found ict_momentum_bearish for {symbol}")
            
        # ICT Pressure patterns
        if vr >= 1.5 and buy_pressure > 0.6:
            pressure_context = {
                "symbol": symbol,
                "volume_ratio": vr,
                "buy_pressure": buy_pressure,
                "base_confidence": conf,
                "killzone_priority": kz.get("priority") if kz else None,
                "vix_regime": vix_regime,
            }
            pressure_conf = self._resolve_confidence(
                "ict_buy_pressure",
                pressure_context,
                lambda: PatternMathematics.calculate_confidence("ict_buy_pressure", pressure_context, redis_client=self.redis_client) or conf,
            )
            setups.append({
                "pattern": "ict_buy_pressure",
                "confidence": pressure_conf,
                "signal": "BUY", 
                "volume_ratio": vr,
                "buy_pressure": buy_pressure,
                "math_context": pressure_context,
            })
            print(f"✅ [ICT_PATTERN] Found ict_buy_pressure for {symbol}")
            
        if vr >= 1.5 and buy_pressure < 0.4:
            pressure_context = {
                "symbol": symbol,
                "volume_ratio": vr,
                "buy_pressure": buy_pressure,
                "base_confidence": conf,
                "killzone_priority": kz.get("priority") if kz else None,
                "vix_regime": vix_regime,
            }
            pressure_conf = self._resolve_confidence(
                "ict_sell_pressure",
                pressure_context,
                lambda: PatternMathematics.calculate_confidence("ict_sell_pressure", pressure_context, redis_client=self.redis_client) or conf,
            )
            setups.append({
                "pattern": "ict_sell_pressure",
                "confidence": pressure_conf,
                "signal": "SELL",
                "volume_ratio": vr, 
                "buy_pressure": buy_pressure,
                "math_context": pressure_context,
            })
            print(f"✅ [ICT_PATTERN] Found ict_sell_pressure for {symbol}")
        
        # Setup: Liquidity grab near previous day/weekly highs + bearish FVG retest → short
        try:
            near_high_level = None
            for key in ("previous_day_high", "weekly_high"):
                lvl = pools.get(key)
                if lvl and abs(last_price - float(lvl)) / float(lvl) < 0.002:
                    near_high_level = float(lvl)
                    break

            if near_high_level and fvgs:
                # Find nearest bearish FVG containing price
                for f in fvgs[::-1]:
                    if f.get("type") == "bearish" and self.fvg.is_price_in_fvg(
                        last_price, f
                    ):
                        # Check volume threshold for ICT short pattern
                        volume_threshold = get_pattern_volume_requirement('ict_liquidity_grab_fvg_retest_short', vix_regime=vix_regime)
                        if vr < volume_threshold:
                            return setups  # Skip this pattern if volume threshold not met
                            
                        pattern_context = {
                            "symbol": symbol,
                            "volume_ratio": vr,
                            "base_confidence": conf,
                            "liquidity_level": near_high_level,
                            "fvg_direction": f.get("type"),
                            "killzone_priority": kz.get("priority") if kz else None,
                            "vix_regime": vix_regime,
                        }
                        pattern_conf = self._resolve_confidence(
                            "ict_liquidity_grab_fvg_retest_short",
                            pattern_context,
                            lambda: conf,
                        )
                        ict_pattern = create_pattern(
                            symbol=symbol,
                            pattern_type="ict_liquidity_grab_fvg_retest_short",
                            signal="SELL",
                            confidence=pattern_conf,
                            last_price=last_price,
                            price_change=float(indicators.get("price_change", 0) or 0),
                            volume=float(indicators.get("bucket_incremental_volume", 0)),
                            volume_ratio=vr,
                            # cumulative_delta=float(indicators.get("cumulative_delta", 0) or 0),  # ❌ REDUNDANT
                            # session_cumulative=float(indicators.get("session_cumulative", 0) or 0),  # ❌ REDUNDANT
                            bucket_incremental_volume=float(indicators.get("bucket_incremental_volume", 0)),
                            bucket_cumulative_volume=float(indicators.get("bucket_cumulative_volume", 0)),
                            vix_level=float(indicators.get("vix_level", 0) or 0),
                            expected_move=0.75,
                            details={
                                "ict_concept": "liquidity_grab",
                                "market_structure": "fvg_retest",
                                "bias": "bearish",
                                "killzone": kz.get("zone"),
                                "liquidity_level": near_high_level,
                                "fvg": f,
                            },
                        )
                        self._stats["patterns_created_schema"] += 1
                        self._stats["ict_patterns_created"] += 1
                        
                        # ✅ CENTRALIZED: Add algo_alert payload using UnifiedAlertBuilder
                        ict_pattern = self._add_algo_alert_to_pattern(ict_pattern, symbol, indicators)
                        
                        # Validate pattern
                        is_valid, issues = validate_pattern(ict_pattern)
                        if is_valid:
                            ict_pattern["math_context"] = pattern_context
                            self._stats["patterns_validated"] += 1
                            setups.append(ict_pattern)
                        else:
                            self._stats["patterns_failed_validation"] += 1
                        break
        except Exception:
            pass

        # Setup: Liquidity sweep below lows + bullish FVG retest → long
        try:
            near_low_level = None
            for key in ("previous_day_low", "weekly_low"):
                lvl = pools.get(key)
                if lvl and abs(last_price - float(lvl)) / float(lvl) < 0.002:
                    near_low_level = float(lvl)
                    break

            if near_low_level and fvgs:
                for f in fvgs[::-1]:
                    if f.get("type") == "bullish" and self.fvg.is_price_in_fvg(
                        last_price, f
                    ):
                        # Check volume threshold for ICT long pattern
                        volume_threshold = get_pattern_volume_requirement('ict_liquidity_grab_fvg_retest_long', vix_regime=vix_regime)
                        if vr < volume_threshold:
                            return setups  # Skip this pattern if volume threshold not met
                            
                        pattern_context = {
                            "symbol": symbol,
                            "volume_ratio": vr,
                            "base_confidence": conf,
                            "liquidity_level": near_low_level,
                            "fvg_direction": f.get("type"),
                            "killzone_priority": kz.get("priority") if kz else None,
                            "vix_regime": vix_regime,
                        }
                        pattern_conf = self._resolve_confidence(
                            "ict_liquidity_grab_fvg_retest_long",
                            pattern_context,
                            lambda: conf,
                        )
                        ict_pattern = create_pattern(
                            symbol=symbol,
                            pattern_type="ict_liquidity_grab_fvg_retest_long",
                            signal="BUY",
                            confidence=pattern_conf,
                            last_price=last_price,
                            price_change=float(indicators.get("price_change", 0) or 0),
                            volume=float(indicators.get("bucket_incremental_volume", 0)),
                            volume_ratio=vr,
                            # cumulative_delta=float(indicators.get("cumulative_delta", 0) or 0),  # ❌ REDUNDANT
                            # session_cumulative=float(indicators.get("session_cumulative", 0) or 0),  # ❌ REDUNDANT
                            bucket_incremental_volume=float(indicators.get("bucket_incremental_volume", 0)),
                            bucket_cumulative_volume=float(indicators.get("bucket_cumulative_volume", 0)),
                            vix_level=float(indicators.get("vix_level", 0) or 0),
                            expected_move=0.75,
                            details={
                                "ict_concept": "liquidity_grab",
                                "market_structure": "fvg_retest",
                                "bias": "bullish",
                                "killzone": kz.get("zone"),
                                "liquidity_level": near_low_level,
                                "fvg": f,
                            },
                        )
                        self._stats["patterns_created_schema"] += 1
                        self._stats["ict_patterns_created"] += 1
                        
                        # ✅ CENTRALIZED: Add algo_alert payload using UnifiedAlertBuilder
                        ict_pattern = self._add_algo_alert_to_pattern(ict_pattern, symbol, indicators)
                        
                        # Validate pattern
                        is_valid, issues = validate_pattern(ict_pattern)
                        if is_valid:
                            ict_pattern["math_context"] = pattern_context
                            self._stats["patterns_validated"] += 1
                            setups.append(ict_pattern)
                        else:
                            self._stats["patterns_failed_validation"] += 1
                        break
        except Exception:
            pass

        # Additional OTE retracement setups (explicit entries)
        try:
            swings = self._identify_price_swings(candles)
            for swing in swings[-3:]:
                if swing.get("direction") == "up":
                    impulse = {
                        "high": float(
                            max(c.get("high", 0) for c in candles[-10:] or candles)
                        ),
                        "low": float(swing.get("price", 0)),
                        "direction": "up",
                    }
                else:
                    impulse = {
                        "high": float(swing.get("price", 0)),
                        "low": float(
                            min(c.get("low", 0) for c in candles[-10:] or candles)
                        ),
                        "direction": "down",
                    }
                ote = self.ote.calculate_ote_zones(impulse)
                if ote and ote.get("zone_low") <= last_price <= ote.get("zone_high"):
                    conf_fvg = self._find_confirming_fvg(fvgs, ote.get("direction"))
                    if conf_fvg:
                        pname = (
                            "ict_ote_retracement_long"
                            if ote.get("direction") == "up"
                            else "ict_ote_retracement_short"
                        )
                        
                        # Check volume threshold for ICT OTE pattern
                        volume_threshold = get_pattern_volume_requirement(pname, vix_regime=vix_regime)
                        if vr < volume_threshold:
                            return setups  # Skip this pattern if volume threshold not met
                        mtf = self._compute_mtf_confirmation(
                            symbol, last_price, ote.get("direction")
                        )
                        base_conf = 0.8 if ote.get("strength") == "strong" else 0.72
                        adj_conf = min(
                            base_conf * (1.0 + min(mtf.get("strength", 0.0), 0.2)), 0.92
                        )
                        pattern_context = {
                            "symbol": symbol,
                            "volume_ratio": vr,
                            "base_confidence": base_conf,
                            "ote_strength": ote.get("strength"),
                            "ote_direction": ote.get("direction"),
                            "mtf_strength": mtf.get("strength"),
                            "killzone_priority": kz.get("priority") if kz else None,
                            "vix_regime": vix_regime,
                        }
                        pattern_conf = self._resolve_confidence(
                            pname,
                            pattern_context,
                            lambda: adj_conf,
                        )
                        ict_pattern = create_pattern(
                            symbol=symbol,
                            pattern_type=pname,
                            signal="BUY" if ote.get("direction") == "up" else "SELL",
                            confidence=pattern_conf,
                            last_price=last_price,
                            price_change=float(indicators.get("price_change", 0) or 0),
                            volume=float(indicators.get("bucket_incremental_volume", 0)),
                            volume_ratio=vr,
                            # cumulative_delta=float(indicators.get("cumulative_delta", 0) or 0),  # ❌ REDUNDANT
                            # session_cumulative=float(indicators.get("session_cumulative", 0) or 0),  # ❌ REDUNDANT
                            bucket_incremental_volume=float(indicators.get("bucket_incremental_volume", 0)),
                            bucket_cumulative_volume=float(indicators.get("bucket_cumulative_volume", 0)),
                            vix_level=float(indicators.get("vix_level", 0) or 0),
                            expected_move=0.75,
                            details={
                                "ict_concept": "ote_retracement",
                                "market_structure": "optimal_trade_entry",
                                "killzone": kz.get("zone"),
                                "ote_zone": ote,
                                "fvg_snapshot": conf_fvg,
                                "mtf_confirmation": mtf,
                            },
                        )
                        self._stats["patterns_created_schema"] += 1
                        self._stats["ict_patterns_created"] += 1
                        # Validate pattern
                        is_valid, issues = validate_pattern(ict_pattern)
                        if is_valid:
                            ict_pattern["math_context"] = pattern_context
                            self._stats["patterns_validated"] += 1
                            setups.append(ict_pattern)
                        else:
                            self._stats["patterns_failed_validation"] += 1
        except Exception:
            pass

        # Optionally add OTE zone information (for context consumers)
        try:
            imp = self._guess_impulse_move(candles)
            if imp:
                ote = self.ote.calculate_ote_zones(imp)
                if ote:
                    self._stats["ote_opportunities"] += 1
                    for s in setups:
                        s["ote_zone"] = ote
        except Exception:
            pass
    
    def _add_algo_alert_to_pattern(self, pattern: Dict[str, Any], symbol: str, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """Add algo_alert payload using UnifiedAlertBuilder."""
        if not self.alert_builder:
            return pattern
        
        try:
            entry_price = pattern.get('last_price') or indicators.get('last_price') or indicators.get('microprice')
            if not entry_price or entry_price <= 0:
                return pattern
            
            pattern_type = pattern.get('pattern', pattern.get('pattern_type', 'ict_core'))
            signal = pattern.get('signal', 'NEUTRAL')
            confidence = pattern.get('confidence', 0.5)
            
            algo_alert = self.alert_builder.build_algo_alert(
                symbol=symbol,
                pattern_type=pattern_type,
                entry_price=entry_price,
                stop_loss=None,  # Will be calculated by UnifiedAlertBuilder
                target_price=None,  # Will be calculated by UnifiedAlertBuilder
                confidence=confidence,
                signal=signal,
                indicators=indicators,
                volume_ratio=indicators.get('volume_ratio', 0.0),
                description=f"{pattern_type} pattern detected",
                # 🚨 CRITICAL: Pass exchange_timestamp for freshness validation
                exchange_timestamp=indicators.get('exchange_timestamp'),
                exchange_timestamp_ms=indicators.get('exchange_timestamp_ms'),
            )
            pattern['algo_alert'] = algo_alert
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"❌ UnifiedAlertBuilder failed for {symbol}: {e}")
        
        return pattern


        # Premium/Discount zone setups
        try:
            if pd_zones:
                dist = float(pd_zones.get("distance_from_vwap") or 0.0)
                cz = pd_zones.get("current_zone")
                if dist >= 0.01:
                    if cz == "premium" and self._has_bearish_confirmation(
                        last_price, fvgs
                    ):
                        # Check volume threshold for ICT premium zone short
                        volume_threshold = get_pattern_volume_requirement('ict_premium_zone_short', vix_regime=vix_regime)
                        if vr < volume_threshold:
                            return setups  # Skip this pattern if volume threshold not met
                        pattern_context = {
                            "symbol": symbol,
                            "volume_ratio": vr,
                            "distance_from_vwap": dist,
                            "premium_discount_zone": cz,
                            "base_confidence": min(0.68 + min(dist * 10.0, 0.2), 0.9),
                            "killzone_priority": kz.get("priority") if kz else None,
                            "vix_regime": vix_regime,
                        }
                        pattern_conf = self._resolve_confidence(
                            "ict_premium_zone_short",
                            pattern_context,
                            lambda: pattern_context["base_confidence"],
                        )
                        ict_pattern = create_pattern(
                            symbol=symbol,
                            pattern_type="ict_premium_zone_short",
                            signal="SELL",
                            confidence=pattern_conf,
                            last_price=last_price,
                            price_change=float(indicators.get("price_change", 0) or 0),
                            volume=float(indicators.get("bucket_incremental_volume", 0)),
                            volume_ratio=vr,
                            # cumulative_delta=float(indicators.get("cumulative_delta", 0) or 0),  # ❌ REDUNDANT
                            # session_cumulative=float(indicators.get("session_cumulative", 0) or 0),  # ❌ REDUNDANT
                            bucket_incremental_volume=float(indicators.get("bucket_incremental_volume", 0)),
                            bucket_cumulative_volume=float(indicators.get("bucket_cumulative_volume", 0)),
                            vix_level=float(indicators.get("vix_level", 0) or 0),
                            expected_move=0.5,
                            details={
                                "ict_concept": "premium_zone",
                                "market_structure": "premium_discount",
                                "bias": "bearish",
                                "killzone": kz.get("zone"),
                                "premium_discount_zones": pd_zones,
                                "distance_from_vwap": dist,
                            },
                        )
                        self._stats["patterns_created_schema"] += 1
                        self._stats["ict_patterns_created"] += 1
                        # Validate pattern
                        is_valid, issues = validate_pattern(ict_pattern)
                        if is_valid:
                            ict_pattern["math_context"] = pattern_context
                            self._stats["patterns_validated"] += 1
                            setups.append(ict_pattern)
                            self._stats["premium_discount_setups"] += 1
                        else:
                            self._stats["patterns_failed_validation"] += 1
                    elif cz == "discount" and self._has_bullish_confirmation(
                        last_price, fvgs
                    ):
                        # Check volume threshold for ICT discount zone long
                        volume_threshold = get_pattern_volume_requirement('ict_discount_zone_long', vix_regime=vix_regime)
                        if vr < volume_threshold:
                            return setups  # Skip this pattern if volume threshold not met
                        pattern_context = {
                            "symbol": symbol,
                            "volume_ratio": vr,
                            "distance_from_vwap": dist,
                            "premium_discount_zone": cz,
                            "base_confidence": min(0.68 + min(dist * 10.0, 0.2), 0.9),
                            "killzone_priority": kz.get("priority") if kz else None,
                            "vix_regime": vix_regime,
                        }
                        pattern_conf = self._resolve_confidence(
                            "ict_discount_zone_long",
                            pattern_context,
                            lambda: pattern_context["base_confidence"],
                        )
                        ict_pattern = create_pattern(
                            symbol=symbol,
                            pattern_type="ict_discount_zone_long",
                            signal="BUY",
                            confidence=pattern_conf,
                            last_price=last_price,
                            price_change=float(indicators.get("price_change", 0) or 0),
                            volume=float(indicators.get("bucket_incremental_volume", 0)),
                            volume_ratio=vr,
                            # cumulative_delta=float(indicators.get("cumulative_delta", 0) or 0),  # ❌ REDUNDANT
                            # session_cumulative=float(indicators.get("session_cumulative", 0) or 0),  # ❌ REDUNDANT
                            bucket_incremental_volume=float(indicators.get("bucket_incremental_volume", 0)),
                            bucket_cumulative_volume=float(indicators.get("bucket_cumulative_volume", 0)),
                            vix_level=float(indicators.get("vix_level", 0) or 0),
                            expected_move=0.5,
                            details={
                                "ict_concept": "discount_zone",
                                "market_structure": "premium_discount",
                                "bias": "bullish",
                                "killzone": kz.get("zone"),
                                "premium_discount_zones": pd_zones,
                                "distance_from_vwap": dist,
                            },
                        )
                        self._stats["patterns_created_schema"] += 1
                        self._stats["ict_patterns_created"] += 1
                        # Validate pattern
                        is_valid, issues = validate_pattern(ict_pattern)
                        if is_valid:
                            ict_pattern["math_context"] = pattern_context
                            self._stats["patterns_validated"] += 1
                            setups.append(ict_pattern)
                            self._stats["premium_discount_setups"] += 1
                        else:
                            self._stats["patterns_failed_validation"] += 1
        except Exception:
            pass

        # Regime filtering
        try:
            index_context = self.regime_filter._get_index_context(symbol)
        except Exception:
            index_context = None

        filtered: List[Dict[str, Any]] = []
        for s in setups:
            try:
                if self.regime_filter and self.regime_filter.should_filter_setup(
                    symbol, s, index_context
                ):
                    continue
            except Exception:
                pass
            filtered.append(s)
            # Per-type counter
            try:
                self._setups_by_type[
                    s.get("pattern", s.get("pattern_type", "unknown"))
                ] += 1
            except Exception:
                pass

        return filtered

    # -------------------- Helpers --------------------
    def _identify_price_swings(
        self, candles: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        swings: List[Dict[str, Any]] = []
        if not candles or len(candles) < 10:
            return swings
        for i in range(2, len(candles) - 2):
            try:
                c = candles[i]
                if (
                    c["high"] > candles[i - 1]["high"]
                    and c["high"] > candles[i - 2]["high"]
                    and c["high"] > candles[i + 1]["high"]
                    and c["high"] > candles[i + 2]["high"]
                ):
                    swings.append(
                        {
                            "type": "high",
                            "price": float(c["high"]),
                            "timestamp": c.get("timestamp"),
                            "direction": "down",
                        }
                    )
                if (
                    c["low"] < candles[i - 1]["low"]
                    and c["low"] < candles[i - 2]["low"]
                    and c["low"] < candles[i + 1]["low"]
                    and c["low"] < candles[i + 2]["low"]
                ):
                    swings.append(
                        {
                            "type": "low",
                            "price": float(c["low"]),
                            "timestamp": c.get("timestamp"),
                            "direction": "up",
                        }
                    )
            except Exception:
                continue
        return swings

    def _find_confirming_fvg(
        self, fvgs: List[Dict[str, Any]], expected_direction: str
    ) -> Optional[Dict]:
        for f in fvgs:
            if expected_direction == "up" and f.get("type") == "bullish":
                return f
            if expected_direction == "down" and f.get("type") == "bearish":
                return f
        return None

    def _has_bearish_confirmation(
        self, last_price: float, fvgs: List[Dict[str, Any]]
    ) -> bool:
        try:
            for f in fvgs:
                if f.get("type") == "bearish" and self.fvg.is_price_in_fvg(
                    last_price, f
                ):
                    return True
            return any(f.get("type") == "bearish" for f in fvgs)
        except Exception:
            return False

    def _has_bullish_confirmation(
        self, last_price: float, fvgs: List[Dict[str, Any]]
    ) -> bool:
        try:
            for f in fvgs:
                if f.get("type") == "bullish" and self.fvg.is_price_in_fvg(
                    last_price, f
                ):
                    return True
            return any(f.get("type") == "bullish" for f in fvgs)
        except Exception:
            return False

    def get_ict_stats(self) -> Dict[str, Any]:
        """Return ICT stats including per-type breakdown and totals."""
        d = dict(self._stats)
        try:
            d["total_setups_detected"] = sum(self._setups_by_type.values())
            d["setups_by_type"] = dict(self._setups_by_type)
        except Exception:
            d["total_setups_detected"] = 0
            d["setups_by_type"] = {}
        return d

    def _compute_mtf_confirmation(
        self, symbol: str, last_price: float, direction: str
    ) -> Dict[str, Any]:
        """Compute confirmation using recent closes: Bollinger, MACD, RSI."""
        result: Dict[str, Any] = {"aligned": False, "components": {}, "strength": 0.0}
        closes: List[float] = []
        try:
            buckets = (
                self.redis_client.get_ohlc_buckets(symbol, count=30)
                if self.redis_client
                else []
            )
            closes = [
                b.get("close")
                for b in buckets
                if isinstance(b.get("close"), (int, float))
            ]
        except Exception:
            closes = []

        checks = 0
        score = 0
        try:
            # ✅ UNIFIED: Use HybridCalculations as single source of truth
            from intraday_trading.intraday_scanner.calculations import HybridCalculations
            
            hybrid_calc = HybridCalculations()
            
            if len(closes) >= 20:
                bb_data = hybrid_calc.calculate_bollinger_bands(closes, period=20, std_dev=2.0)
                if bb_data and isinstance(bb_data, dict):
                    ub = bb_data.get('upper')
                    lb = bb_data.get('lower')
                if ub and lb and ub != lb:
                    pos = (last_price - lb) / (ub - lb)
                    result["components"]["bb_position"] = pos
                    checks += 1
                    if (direction == "up" and pos >= 0.5) or (
                        direction == "down" and pos <= 0.5
                    ):
                        score += 1
            if len(closes) >= 26:
                macd_data = hybrid_calc.calculate_macd(closes, fast_period=12, slow_period=26, signal_period=9)
                if macd_data and isinstance(macd_data, dict):
                    macd_line = macd_data.get('macd', 0)
                    signal_line = macd_data.get('signal', 0)
                    hist = macd_data.get('histogram', 0)
                result["components"]["macd_hist"] = hist
                checks += 1
                if (direction == "up" and macd_line > signal_line and hist > 0) or (
                    direction == "down" and macd_line < signal_line and hist < 0
                ):
                    score += 1
            if len(closes) >= 14:
                rsi = hybrid_calc.calculate_rsi(closes, period=14)
                result["components"]["rsi"] = rsi
                checks += 1
                if (direction == "up" and rsi >= 50) or (
                    direction == "down" and rsi <= 50
                ):
                    score += 1
        except Exception:
            pass

        result["strength"] = (score / checks) if checks else 0.0
        result["aligned"] = score >= max(1, checks - 1) if checks else False
        return result


# ============================================
# ICT PATTERN CLASSES (Consolidated)
# ============================================

class ICTLiquidityDetector:
    """Detects previous day/week highs/lows and simple stop-hunt zones."""

    def __init__(self, redis_client=None):
        self.redis_client = redis_client

    def detect_liquidity_pools(self, symbol: str) -> Dict[str, Optional[float]]:
        prev_day = self._get_previous_trading_day()
        weekly_dates = self._get_recent_trading_days(5)

        pd_high = self._get_session_high(symbol, prev_day)
        pd_low = self._get_session_low(symbol, prev_day)

        # Weekly from last up to 5 sessions (excluding today)
        w_high, w_low = self._get_week_extremes(symbol, weekly_dates)

        pools = {
            "previous_day_high": pd_high,
            "previous_day_low": pd_low,
            "weekly_high": w_high,
            "weekly_low": w_low,
        }
        pools["stop_hunt_zones"] = self._calculate_stop_hunt_zones(pools)
        return pools

    def _get_previous_trading_day(self) -> str:
        # Simple: yesterday; if weekend, walk back
        d = date.today() - timedelta(days=1)
        # Walk back up to 3 days to skip weekend
        for _ in range(3):
            if d.weekday() < 5:
                break
            d = d - timedelta(days=1)
        return d.isoformat()

    def _get_recent_trading_days(self, count: int) -> List[str]:
        days = []
        d = date.today() - timedelta(days=1)
        while len(days) < count and (date.today() - d).days <= 10:
            if d.weekday() < 5:
                days.append(d.isoformat())
            d = d - timedelta(days=1)
        return days

    def _get_session_high(self, symbol: str, session_date: Optional[str]) -> Optional[float]:
        """Get previous day high from OHLC data in Redis DB1."""
        if not session_date or not self.redis_client:
            return None
        try:
            # ✅ FIXED: Use OHLC daily sorted set instead of session keys
            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards, DatabaseAwareKeyBuilder
            import json
            
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            ohlc_daily_key = DatabaseAwareKeyBuilder.live_ohlc_daily(canonical_symbol)
            
            # Get Redis client (handle wrappers)
            redis_client = self.redis_client
            if hasattr(redis_client, 'get_client'):
                redis_client = redis_client.get_client(1)  # DB1
            elif hasattr(redis_client, 'redis'):
                redis_client = redis_client.redis
            
            # ✅ OPTIMIZED: Get last 10 entries (most recent) instead of all entries
            entries = redis_client.zrevrange(ohlc_daily_key, 0, 9, withscores=True)
            for entry, score in entries:
                if isinstance(entry, bytes):
                    entry = entry.decode('utf-8')
                try:
                    data = json.loads(entry)
                    entry_date = data.get('d') or data.get('date') or data.get('session_date')
                    if entry_date == session_date:
                        high = data.get('h') or data.get('high') or data.get('high_price')
                        if high and isinstance(high, (int, float)):
                            return float(high)
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Error fetching session high for {symbol} on {session_date}: {e}")
        return None

    def _get_session_low(self, symbol: str, session_date: Optional[str]) -> Optional[float]:
        """Get previous day low from OHLC data in Redis DB1."""
        if not session_date or not self.redis_client:
            return None
        try:
            # ✅ FIXED: Use OHLC daily sorted set instead of session keys
            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards, DatabaseAwareKeyBuilder
            import json
            
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            ohlc_daily_key = DatabaseAwareKeyBuilder.live_ohlc_daily(canonical_symbol)
            
            # Get Redis client (handle wrappers)
            redis_client = self.redis_client
            if hasattr(redis_client, 'get_client'):
                redis_client = redis_client.get_client(1)  # DB1
            elif hasattr(redis_client, 'redis'):
                redis_client = redis_client.redis
            
            # ✅ OPTIMIZED: Get last 10 entries (most recent) instead of all entries
            entries = redis_client.zrevrange(ohlc_daily_key, 0, 9, withscores=True)
            for entry, score in entries:
                if isinstance(entry, bytes):
                    entry = entry.decode('utf-8')
                try:
                    data = json.loads(entry)
                    entry_date = data.get('d') or data.get('date') or data.get('session_date')
                    if entry_date == session_date:
                        low = data.get('l') or data.get('low') or data.get('low_price')
                        if low and isinstance(low, (int, float)):
                            return float(low)
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Error fetching session low for {symbol} on {session_date}: {e}")
        return None

    def _get_week_extremes(self, symbol: str, session_dates: List[str]) -> (Optional[float], Optional[float]):
        """Get weekly high/low from OHLC daily data in Redis DB1."""
        highs: List[float] = []
        lows: List[float] = []
        if not self.redis_client:
            return None, None
        try:
            # ✅ FIXED: Use OHLC daily sorted set instead of session keys
            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards, DatabaseAwareKeyBuilder
            import json
            
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            ohlc_daily_key = DatabaseAwareKeyBuilder.live_ohlc_daily(canonical_symbol)
            
            # Get Redis client (handle wrappers)
            redis_client = self.redis_client
            if hasattr(redis_client, 'get_client'):
                redis_client = redis_client.get_client(1)  # DB1
            elif hasattr(redis_client, 'redis'):
                redis_client = redis_client.redis
            
            # ✅ OPTIMIZED: Get last 10 entries (most recent) instead of all entries
            entries = redis_client.zrevrange(ohlc_daily_key, 0, 9, withscores=True)
            session_dates_set = set(session_dates)
            
            for entry, score in entries:
                if isinstance(entry, bytes):
                    entry = entry.decode('utf-8')
                try:
                    data = json.loads(entry)
                    entry_date = data.get('d') or data.get('date') or data.get('session_date')
                    if entry_date in session_dates_set:
                        high = data.get('h') or data.get('high') or data.get('high_price')
                        low = data.get('l') or data.get('low') or data.get('low_price')
                        if high and isinstance(high, (int, float)):
                            highs.append(float(high))
                        if low and isinstance(low, (int, float)):
                            lows.append(float(low))
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Error fetching week extremes for {symbol}: {e}")
        
        return (max(highs) if highs else None, min(lows) if lows else None)

    def _calculate_stop_hunt_zones(self, pools: Dict[str, Optional[float]]) -> List[float]:
        levels = [
            pools.get("previous_day_high"),
            pools.get("previous_day_low"),
            pools.get("weekly_high"),
            pools.get("weekly_low"),
        ]
        hunt_zones: List[float] = []
        for level in levels:
            try:
                if level and level > 0:
                    hunt_zones.append(level * 1.005)
                    hunt_zones.append(level * 0.995)
            except Exception:
                continue
        return hunt_zones


class ICTFVGDetector:
    """Detect Fair Value Gaps (FVG) from simple OHLC candles."""

    def detect_fvg(self, candles: List[Dict]) -> List[Dict]:
        fvgs: List[Dict] = []
        if not candles or len(candles) < 3:
            return fvgs

        # Expect each candle to have keys: open, high, low, close, timestamp
        for i in range(2, len(candles)):
            prior = candles[i - 2]
            previous = candles[i - 1]
            current = candles[i]

            try:
                # Bullish FVG: current low > prior high
                if float(current["low"]) > float(prior["high"]):
                    fvgs.append(
                        {
                            "type": "bullish",
                            "gap_low": float(prior["high"]),
                            "gap_high": float(current["low"]),
                            "strength": (float(current["low"]) - float(prior["high"]))
                            / max(1e-9, float(prior["high"])),
                            "timestamp": current.get("timestamp"),
                        }
                    )

                # Bearish FVG: current high < prior low
                elif float(current["high"]) < float(prior["low"]):
                    fvgs.append(
                        {
                            "type": "bearish",
                            "gap_low": float(current["high"]),
                            "gap_high": float(prior["low"]),
                            "strength": (float(prior["low"]) - float(current["high"]))
                            / max(1e-9, float(current["high"])),
                            "timestamp": current.get("timestamp"),
                        }
                    )
            except Exception:
                continue

        return fvgs

    def is_price_in_fvg(self, current_price: float, fvg: Dict) -> bool:
        try:
            return float(fvg["gap_low"]) <= float(current_price) <= float(fvg["gap_high"])
        except Exception:
            return False


class ICTOTECalculator:
    """Calculate Optimal Trade Entry (OTE) retracement zones."""

    def calculate_ote_zones(self, impulse_move: Dict, multi_timeframe: bool = False) -> Dict:
        base = self._calculate_base_ote_zone(impulse_move)
        if not base:
            return {}
        if not multi_timeframe:
            # Add default confidence
            base.setdefault("confidence", 0.7)
            return base
        # MTF confirmation (placeholder)
        mtf = self._get_mtf_confirmation(impulse_move)
        base["mtf_confirmation"] = mtf
        base["confidence"] = base.get("confidence", 0.7) * float(mtf.get("strength", 1.0))
        return base

    def _calculate_base_ote_zone(self, impulse_move: Dict) -> Dict:
        high = float(impulse_move.get("high", 0) or 0)
        low = float(impulse_move.get("low", 0) or 0)
        direction = impulse_move.get("direction", "up")
        if high <= 0 or low <= 0 or high == low:
            return {}
        total_move = high - low
        if direction == "up":
            ote_high = high - (total_move * 0.618)
            ote_low = high - (total_move * 0.786)
        else:
            ote_low = low + (total_move * 0.618)
            ote_high = low + (total_move * 0.786)
        zone_low = min(ote_low, ote_high)
        zone_high = max(ote_low, ote_high)
        width = abs(zone_high - zone_low)
        strength = "strong" if (width / max(1e-9, total_move)) > 0.1 else "weak"
        return {"zone_low": zone_low, "zone_high": zone_high, "direction": direction, "strength": strength}

    def _get_mtf_confirmation(self, swing: Dict) -> Dict:
        # Placeholder: in practice, use higher TF data alignment
        return {"higher_tf_aligned": True, "lower_tf_momentum": "confirming", "strength": 1.0}
