"""
MATHEMATICAL INTEGRITY ENGINE FOR ALL PATTERNS
Ensures consistent confidence bounds, risk integration, and outlier protection
"""
import logging
import time
from typing import Any, Dict, Optional, Tuple, Union
from math import isnan

logger = logging.getLogger(__name__)

# ✅ VIX Utilities - Import for Redis DB1 access
try:
    from shared_core.utils.vix_utils import get_vix_regime, get_vix_value, get_vix_utils
    VIX_UTILS_AVAILABLE = True
except ImportError:
    VIX_UTILS_AVAILABLE = False
    def get_vix_regime() -> str:
        return "UNKNOWN"
    def get_vix_value() -> Optional[float]:
        return None
    def get_vix_utils(redis_client=None):
        return None

class PatternMathematics:
    """
    Centralized mathematical integrity for all 19 patterns
    
    ✅ ENHANCED: All confidence and threshold calculations fetch VIX from Redis DB1
    """
    
    # VIX-aware RSI thresholds
    RSI_THRESHOLDS = {
        "HIGH_VIX": {"oversold": 25, "overbought": 75, "neutral_low": 40, "neutral_high": 60},
        "NORMAL": {"oversold": 30, "overbought": 70, "neutral_low": 45, "neutral_high": 65},
        "LOW_VIX": {"oversold": 35, "overbought": 65, "neutral_low": 50, "neutral_high": 60}
    }
    
    # ⚠️ DEPRECATED: VIX caching moved to thresholds.py (single source of truth)
    # This cache is kept for backward compatibility but should use thresholds.py instead
    _vix_cache = {"regime": None, "value": None, "timestamp": None, "fetch_time": 0}
    _vix_cache_ttl = 180  # ✅ Cache for 180 seconds (3 minutes) - matches thresholds.py
    
    @staticmethod
    def _get_vix_from_redis_db1(redis_client=None, force_refresh=False) -> Dict[str, Any]:
        """
        ⚠️ DEPRECATED: Use thresholds.py get_safe_vix_regime() instead (single source of truth)
        
        This method is kept for backward compatibility but delegates to thresholds.py.
        All VIX data should come from thresholds.py to ensure consistency.
        
        Args:
            redis_client: Optional Redis client (if None, uses thresholds.py default)
            force_refresh: If True, bypass cache and fetch fresh from Redis
            
        Returns:
            Dict with 'regime' and 'value' keys, or {'regime': 'UNKNOWN', 'value': None} if unavailable
        """
        try:
            # ✅ DELEGATE: Use thresholds.py for VIX data (single source of truth)
            from shared_core.config_utils.thresholds import get_safe_vix_regime, VIXCache
            
            # Get VIX regime from thresholds.py (uses cached VIXCache)
            vix_regime = get_safe_vix_regime(redis_client=redis_client)
            
            # Get VIX value from VIXCache if available
            vix_value = None
            try:
                vix_cache = VIXCache(redis_client=redis_client)
                vix_data = vix_cache.get_vix_regime()
                if isinstance(vix_data, dict):
                    vix_value = vix_data.get('value')
                elif hasattr(vix_cache, '_vix_cache'):
                    vix_value = vix_cache._vix_cache.get('value')
            except Exception:
                pass
            
            # Update local cache for backward compatibility
            PatternMathematics._vix_cache = {
                "regime": vix_regime,
                "value": vix_value,
                "timestamp": None,
                "fetch_time": time.time()
            }
            
            return {"regime": vix_regime, "value": vix_value}
            
        except ImportError:
            # Fallback: Use old vix_utils if thresholds.py not available
            logger.debug("⚠️ thresholds.py not available, using vix_utils fallback")
            if not VIX_UTILS_AVAILABLE:
                return {"regime": "UNKNOWN", "value": None}
            
            try:
                vix_utils = get_vix_utils(redis_client=redis_client)
                if not vix_utils:
                    return {"regime": "UNKNOWN", "value": None}
                
                vix_data = vix_utils.get_current_vix()
                if vix_data:
                    regime = vix_data.get('regime', 'UNKNOWN')
                    value = vix_data.get('value')
                    
                    PatternMathematics._vix_cache = {
                        "regime": regime,
                        "value": value,
                        "timestamp": vix_data.get('timestamp'),
                        "fetch_time": time.time()
                    }
                    
                    return {"regime": regime, "value": value}
                
                return {"regime": "UNKNOWN", "value": None}
                
            except Exception as e:
                logger.warning(f"⚠️ Error fetching VIX (fallback): {e}")
                return {"regime": "UNKNOWN", "value": None}
    
    @staticmethod
    def _ensure_vix_regime(context: Dict[str, Any], redis_client=None) -> str:
        """
        ✅ ENHANCED: Ensure VIX regime is available for calculations
        
        Checks context first, then fetches from Redis DB1 if missing.
        This ensures all confidence/threshold calculations have VIX data.
        
        Args:
            context: Context dictionary (may contain 'vix_regime')
            redis_client: Optional Redis client for VIX fetch
            
        Returns:
            VIX regime string: "HIGH", "LOW", "NORMAL", "PANIC", or "UNKNOWN"
        """
        # First, check if vix_regime is in context
        vix_regime = context.get("vix_regime")
        if vix_regime and vix_regime != "UNKNOWN":
            return str(vix_regime).upper()
        
        # If not in context, fetch from Redis DB1
        vix_data = PatternMathematics._get_vix_from_redis_db1(redis_client=redis_client)
        regime = vix_data.get("regime", "UNKNOWN")
        
        # Update context for future use
        context["vix_regime"] = regime
        if vix_data.get("value") is not None:
            context["vix_value"] = vix_data["value"]
        
        return regime
    
    @staticmethod
    def _calculate_bounded_confidence(base_confidence: float, strength_bonus: float, 
                                   max_bonus: float = 0.3) -> float:
        """
        ✅ PRIVATE: Internal helper for confidence bounds calculation.
        Should only be used internally by calculate_confidence().
        External code should use PatternMathematics.calculate_confidence() via math_dispatcher.
        """
        confidence = base_confidence + min(strength_bonus, max_bonus)
        return max(0.0, min(1.0, confidence))
    
    @staticmethod
    def protect_outliers(value: float, max_value: float = 10.0) -> float:
        """Protect against extreme outlier values"""
        if value is None:
            return 1.0
        if isnan(value):
            return 1.0
        return min(value, max_value)
    
    @staticmethod
    def get_vix_aware_rsi_thresholds(
        vix_regime: str = None,
        threshold_type: str = "neutral",
        redis_client=None,
    ) -> Union[float, Tuple[float, float]]:
        """
        ✅ ENHANCED: Return RSI thresholds adjusted for VIX regime from Redis DB1
        
        If vix_regime is not provided, fetches from Redis DB1.

        Args:
            vix_regime: Optional regime key (HIGH_VIX, HIGH, NORMAL, LOW_VIX, LOW, PANIC).
                       If None, fetches from Redis DB1.
            threshold_type: RSI band to resolve. Returns a float for single-value
                thresholds (e.g., "oversold", "overbought") and a (low, high)
                tuple when `"neutral"` is requested.
            redis_client: Optional Redis client for VIX fetch

        Returns:
            Either the float threshold or a tuple of neutral bounds when
            `threshold_type` is `"neutral"`. Falls back to NORMAL regime defaults.
        """
        # ✅ ENHANCED: Fetch VIX from Redis DB1 if not provided
        if not vix_regime or vix_regime == "UNKNOWN":
            vix_data = PatternMathematics._get_vix_from_redis_db1(redis_client=redis_client)
            vix_regime = vix_data.get("regime", "NORMAL")
        
        # ✅ FIX: Normalize VIX regime to match RSI_THRESHOLDS keys
        # System uses "HIGH", "LOW" but RSI_THRESHOLDS uses "HIGH_VIX", "LOW_VIX"
        normalized_regime = vix_regime.upper()
        if normalized_regime == "HIGH":
            normalized_regime = "HIGH_VIX"
        elif normalized_regime == "LOW":
            normalized_regime = "LOW_VIX"
        elif normalized_regime == "PANIC":
            # PANIC uses HIGH_VIX thresholds (more sensitive)
            normalized_regime = "HIGH_VIX"
        
        regime_thresholds = PatternMathematics.RSI_THRESHOLDS.get(
            normalized_regime,
            PatternMathematics.RSI_THRESHOLDS["NORMAL"],
        )

        if threshold_type == "neutral":
            return (
                regime_thresholds.get("neutral_low", 45),
                regime_thresholds.get("neutral_high", 65),
            )

        return regime_thresholds.get(threshold_type, 50.0)
    
    @staticmethod
    def _get_vix_confidence_multiplier(vix_regime: str) -> float:
        """Get VIX regime confidence multiplier for centralized confidence calculations."""
        vix_multipliers = {
            "HIGH": 0.8,      # Reduce confidence in high VIX (more noise)
            "NORMAL": 1.0,    # Normal confidence
            "LOW": 1.2,       # Increase confidence in low VIX (cleaner signals)
            "UNKNOWN": 1.0    # Default to normal
        }
        return vix_multipliers.get(vix_regime, 1.0)
    
    @staticmethod
    def _calculate_volume_confidence(volume_ratio: float, threshold: float, 
                                  base_confidence: float = 0.5) -> float:
        """
        ✅ PRIVATE: Internal helper for volume confidence calculation.
        Should only be used internally by calculate_confidence().
        External code should use PatternMathematics.calculate_confidence() via math_dispatcher.
        """
        # ✅ FIXED: Guard against division by zero
        if threshold <= 0:
            return base_confidence  # Return base confidence if threshold is invalid
        
        if volume_ratio < threshold:
            return 0.0
        
        safe_ratio = PatternMathematics.protect_outliers(volume_ratio)
        
        # Diminishing returns: 1.5x volume = 70% confidence, 2x = 80%, 3x = 85%
        # ✅ FIXED: Guard against division by zero (threshold already checked above)
        ratio_over_threshold = safe_ratio / threshold if threshold > 0 else 1.0
        if ratio_over_threshold >= 3.0:
            strength_bonus = 0.35  # Max 85% confidence
        elif ratio_over_threshold >= 2.0:
            strength_bonus = 0.3   # 80% confidence  
        elif ratio_over_threshold >= 1.5:
            strength_bonus = 0.2   # 70% confidence
        else:
            strength_bonus = 0.1   # 60% confidence
        
        return PatternMathematics._calculate_bounded_confidence(base_confidence, strength_bonus)
    
    @staticmethod
    def _calculate_price_confidence(price_change: float, threshold: float,
                                 base_confidence: float = 0.6) -> float:
        """
        ✅ PRIVATE: Internal helper for price confidence calculation.
        Should only be used internally by calculate_confidence().
        External code should use PatternMathematics.calculate_confidence() via math_dispatcher.
        """
        # ✅ FIX: Guard against division by zero
        if threshold <= 0:
            # If threshold is 0 or negative, use a small default to prevent division by zero
            threshold = 0.1
        
        if abs(price_change) < threshold:
            return 0.0
        
        # Diminishing returns: 1.5x threshold = 75% confidence, 2x = 80%, 3x = 85%
        ratio_over_threshold = abs(price_change) / threshold
        if ratio_over_threshold >= 3.0:
            strength_bonus = 0.25  # Max 85% confidence
        elif ratio_over_threshold >= 2.0:
            strength_bonus = 0.2   # 80% confidence
        elif ratio_over_threshold >= 1.5:
            strength_bonus = 0.15  # 75% confidence
        else:
            strength_bonus = 0.05  # 65% confidence
        
        return PatternMathematics._calculate_bounded_confidence(base_confidence, strength_bonus)
    
    @staticmethod
    def _calculate_trend_confidence(trend_strength: float, threshold: float, 
                                 base_confidence: float = 0.55) -> float:
        """
        ✅ PRIVATE: Internal helper for trend confidence calculation.
        Should only be used internally by calculate_confidence().
        External code should use PatternMathematics.calculate_confidence() via math_dispatcher.
        """
        # ✅ FIX: Guard against division by zero
        if threshold <= 0:
            # If threshold is 0 or negative, use a small default to prevent division by zero
            threshold = 0.1
        
        if trend_strength < threshold:
            return 0.0
        
        # Diminishing returns: 1.5x threshold = 70% confidence, 2x = 80%, 3x = 85%
        ratio_over_threshold = trend_strength / threshold
        if ratio_over_threshold >= 3.0:
            strength_bonus = 0.3   # Max 85% confidence
        elif ratio_over_threshold >= 2.0:
            strength_bonus = 0.25  # 80% confidence
        elif ratio_over_threshold >= 1.5:
            strength_bonus = 0.15  # 70% confidence
        else:
            strength_bonus = 0.05  # 60% confidence
        
        return PatternMathematics._calculate_bounded_confidence(base_confidence, strength_bonus)

    @staticmethod
    def calculate_confidence(pattern_type: str, context: Dict[str, Any], redis_client=None) -> Optional[float]:
        """
        ✅ ENHANCED: Aggregate confidence computation with VIX from Redis DB1
        
        Ensures VIX regime is fetched from Redis DB1 if not in context.
        """
        try:
            ptype = (pattern_type or "").lower()
            volume_ratio = float(context.get("volume_ratio", 0.0) or 0.0)
            price_change_pct = context.get("price_change_pct")
            if price_change_pct is None:
                price_change_pct = context.get("price_change")
            price_change_pct = float(price_change_pct or 0.0)
            dynamic_threshold = float(context.get("dynamic_threshold") or context.get("volume_threshold") or 1.0)
            min_price_move = float(context.get("min_price_move") or context.get("price_threshold") or 0.5)
            base_confidence = float(context.get("base_confidence") or 0.5)
            
            # ✅ ENHANCED: Ensure VIX regime is fetched from Redis DB1 if not in context
            vix_regime = PatternMathematics._ensure_vix_regime(context, redis_client=redis_client)
            vix_multiplier = PatternMathematics._get_vix_confidence_multiplier(vix_regime)

            if ptype in {"volume_spike", "volume_breakout"}:
                vol_conf = PatternMathematics._calculate_volume_confidence(
                    max(volume_ratio, 0.0), max(dynamic_threshold, 0.1), max(0.45, base_confidence)
                )
                price_conf = PatternMathematics._calculate_price_confidence(
                    abs(price_change_pct), max(min_price_move, 0.1), 0.45
                )
                confidence = max(0.0, min(0.95, vol_conf * 0.55 + price_conf * 0.45))
                return max(0.0, min(0.95, confidence * vix_multiplier))

            if ptype in {"breakout", "reversal"}:
                breakout_threshold = float(context.get("price_threshold") or min_price_move or 0.3)
                price_conf = PatternMathematics._calculate_price_confidence(
                    abs(price_change_pct), max(breakout_threshold, 0.1), 0.55
                )
                volume_conf = PatternMathematics._calculate_volume_confidence(
                    max(volume_ratio, 0.0), max(dynamic_threshold, 0.1), 0.45
                )
                confidence = max(0.0, min(0.95, price_conf * 0.6 + volume_conf * 0.4))
                return max(0.0, min(0.95, confidence * vix_multiplier))

            if ptype in {"volume_price_divergence"}:
                divergence_score = float(context.get("divergence_score") or 0.0)
                base = 0.5 + min(0.2, abs(divergence_score))
                confidence = max(0.0, min(0.85, base))
                return max(0.0, min(0.95, confidence * vix_multiplier))

            if ptype in {"upside_momentum", "downside_momentum"}:
                momentum_strength = abs(context.get("momentum_strength", price_change_pct))
                momentum_threshold = float(context.get("momentum_threshold") or min_price_move or 0.3)
                price_conf = PatternMathematics._calculate_price_confidence(
                    momentum_strength, max(momentum_threshold, 0.1), 0.6
                )
                confidence = max(0.0, min(0.9, price_conf))
                return max(0.0, min(0.95, confidence * vix_multiplier))

            confidence = max(0.0, min(0.95, base_confidence))
            return max(0.0, min(0.95, confidence * vix_multiplier))
        except Exception:
            return None

    @staticmethod
    def calculate_position_size(pattern_type: str, context: Dict[str, Any]) -> Optional[float]:
        """
        ⚠️ DEPRECATED: This method has been moved to UnifiedAlertBuilder._calculate_position_size()
        
        Position sizing is now handled by UnifiedAlertBuilder for consistent algo-ready payloads.
        This method is kept for backward compatibility but delegates to UnifiedAlertBuilder.
        
        Args:
            pattern_type: Pattern type (for logging)
            context: Context dict with entry_price, stop_loss, account_size, etc.
        
        Returns:
            Position size in shares/lots, or None if invalid
        """
        try:
            # ✅ DELEGATE: Use UnifiedAlertBuilder for position sizing
            from alerts.unified_alert_builder import get_unified_alert_builder
            
            # Get Redis client from context if available
            redis_client = context.get("redis_client")
            builder = get_unified_alert_builder(redis_client=redis_client)
            
            # Extract required fields
            symbol = context.get("symbol", "")
            entry_price = float(context.get("entry_price") or 0.0)
            stop_loss = float(context.get("stop_loss") or 0.0)
            account_size = float(context.get("account_size") or 1_000_000.0)
            
            # Call UnifiedAlertBuilder's position sizing
            position_lots = builder._calculate_position_size(
                symbol=symbol,
                entry_price=entry_price,
                stop_loss=stop_loss,
                account_size=account_size,
                pattern_type=pattern_type
            )
            
            if position_lots:
                logger.debug(f"✅ [POSITION_SIZE] {symbol} Delegated to UnifiedAlertBuilder: {position_lots} lots")
                return position_lots
            
            # Fallback: Return None if UnifiedAlertBuilder fails
            logger.warning(f"⚠️ [POSITION_SIZE] UnifiedAlertBuilder failed, returning None")
            return None
            
        except ImportError:
            logger.warning("⚠️ [POSITION_SIZE] UnifiedAlertBuilder not available, cannot calculate position size")
            return None
        except Exception as e:
            logger.warning(f"⚠️ [POSITION_SIZE] Error delegating to UnifiedAlertBuilder: {e}")
            return None

    @staticmethod
    def calculate_stop_loss(pattern_type: str, context: Dict[str, Any]) -> Optional[float]:
        """ATR-based stop loss helper."""
        try:
            entry_price = float(context.get("entry_price") or 0.0)
            atr = float(context.get("atr") or (entry_price * 0.02))
            confidence = float(context.get("confidence") or context.get("base_confidence") or 0.5)
            direction = (context.get("direction") or context.get("signal") or "BUY").upper()

            if entry_price <= 0:
                return None

            pattern = (pattern_type or "").lower()
            # ✅ UPDATED: Only 8 core patterns - ATR multipliers for stop loss calculation
            atr_multiplier_map = {
                # Advanced Pattern Detectors (5)
                "order_flow_breakout": 0.8,
                "gamma_exposure_reversal": 0.8,
                "microstructure_divergence": 1.0,
                "cross_asset_arbitrage": 1.0,
                "vix_momentum": 1.0,
                # Strategies (2)
                "kow_signal_straddle": 1.5,
                "ict_iron_condor": 1.2,
                # Game Theory (1)
                "game_theory_signal": 1.0,
            }
            atr_mult = atr_multiplier_map.get(pattern, 1.0)
            confidence_adjustment = 1.0 - (confidence - 0.5) * 0.2
            stop_distance = atr * max(0.1, atr_mult * confidence_adjustment)

            if direction in {"SELL", "SHORT"} or "downside" in pattern:
                stop_loss = entry_price + stop_distance
            else:
                stop_loss = entry_price - stop_distance

            max_stop = entry_price * 0.05
            if abs(entry_price - stop_loss) > max_stop:
                if direction in {"SELL", "SHORT"} or "downside" in pattern:
                    stop_loss = entry_price + max_stop
                else:
                    stop_loss = entry_price - max_stop

            return max(0.0, stop_loss)
        except Exception:
            return None

    @staticmethod
    def calculate_price_targets(pattern_type: str, context: Dict[str, Any]) -> Optional[Dict[str, float]]:
        """Derive entry/target/stop structure for a pattern."""
        try:
            last_price = float(context.get("last_price") or context.get("entry_price") or 0.0)
            if last_price <= 0:
                return None

            direction = (context.get("direction") or context.get("signal") or "BUY").upper()
            atr = float(context.get("atr") or (last_price * 0.02))
            base_stop = PatternMathematics.calculate_stop_loss(pattern_type, context)
            if base_stop is None:
                return None

            if direction in {"SELL", "SHORT"}:
                entry_price = last_price * 0.998
                target_price = last_price - (atr * 2.0)
            else:
                entry_price = last_price * 1.002
                target_price = last_price + (atr * 2.0)

            stop_loss = base_stop
            return {
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "target_price": target_price,
            }
        except Exception:
            return None

    @staticmethod
    def calculate_news_impact(instrument: str, news_data: Dict[str, Any]) -> Optional[float]:
        """Safe, capped news boost calculation."""
        try:
            if not news_data:
                return 0.0

            impact = (news_data.get("impact") or news_data.get("market_impact") or "LOW").upper()
            sentiment = news_data.get("sentiment")
            if sentiment is None:
                sentiment = news_data.get("sentiment_score", 0.0)

            base_boost = 0.0
            if impact == "HIGH":
                base_boost += 0.06
            elif impact == "MEDIUM":
                base_boost += 0.03

            if news_data.get("volume_trigger"):
                base_boost += 0.03

            if isinstance(sentiment, str):
                sent = sentiment.lower()
                if sent == "positive":
                    base_boost += 0.02
                elif sent == "negative":
                    base_boost -= 0.02
            else:
                score = float(sentiment)
                if score > 0.2:
                    base_boost += 0.02
                elif score < -0.2:
                    base_boost -= 0.02

            return max(-0.05, min(0.10, base_boost))
        except Exception:
            return None
