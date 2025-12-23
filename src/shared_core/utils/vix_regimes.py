#!/opt/homebrew/bin/python3.13
from __future__ import annotations

from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class VIXRegimeManager:
    """Centralized VIX regime management used across the system."""

    def __init__(self, redis_client=None):
        # ✅ FIXED: Use RedisClientFactory if no client provided
        if redis_client is None:
            try:
                from shared_core.redis_clients.redis_client import RedisClientFactory
                self.redis_client = RedisClientFactory.get_trading_client()
            except Exception as e:
                logger.warning(f"⚠️ [VIX_REGIMES] Failed to get Redis client: {e}")
                self.redis_client = None
        else:
            self.redis_client = redis_client
        # SINGLE SOURCE OF TRUTH: Use ranges from config/thresholds.py
        self.regime_definitions = {
            "LOW": {"min": 0.0, "max": 12.0, "description": "Low volatility, complacent market"},
            "NORMAL": {"min": 12.0, "max": 15.0, "description": "Normal volatility, healthy market"},
            "HIGH": {"min": 15.0, "max": 25.0, "description": "High volatility, elevated market"},
            "PANIC": {"min": 25.0, "max": 10_000.0, "description": "Extreme volatility, panic market"},
        }

    def get_current_regime(self) -> Dict[str, Any]:
        """SINGLE SOURCE OF TRUTH: Use VIX classification from config/thresholds.py"""
        try:
            # ✅ SINGLE SOURCE OF TRUTH: Use classify_indian_vix_regime from shared_core.config_utils.thresholds
            from shared_core.config_utils.thresholds import classify_indian_vix_regime
            from shared_core.utils.vix_utils import get_vix_value
            vix_value = get_vix_value() or 0.0
            regime = classify_indian_vix_regime(vix_value)
            
            return {
                "regime": regime,
                "vix_value": vix_value,
                "description": self.regime_definitions[regime]["description"],
                "thresholds": self._get_regime_thresholds(regime),
            }
        except ImportError:
            # Fallback to local classification
            vix_value = float(self._get_current_vix() or 0.0)
            for name, bounds in self.regime_definitions.items():
                if float(bounds["min"]) <= vix_value < float(bounds["max"]):
                    return {
                        "regime": name,
                        "vix_value": vix_value,
                        "description": bounds["description"],
                        "thresholds": self._get_regime_thresholds(name),
                    }
            return {
                "regime": "NORMAL",
                "vix_value": vix_value,
                "description": self.regime_definitions["NORMAL"]["description"],
                "thresholds": self._get_regime_thresholds("NORMAL"),
            }

    def _get_current_vix(self) -> float:
        """Get current VIX value from Redis DB1 using standard keys"""
        # ✅ FIXED: Use standard Redis keys and RedisClientFactory
        if not self.redis_client:
            try:
                from shared_core.redis_clients.redis_client import RedisClientFactory
                self.redis_client = RedisClientFactory.get_trading_client()
            except Exception as e:
                logger.debug(f"⚠️ [VIX_REGIMES] Failed to get Redis client: {e}")
                return 0.0
        
        # Standard VIX keys (same as vix_utils.py)
        keys_to_try = [
            "index:NSEINDIA_VIX",  # ✅ FIXED: Primary key with underscore (actual Redis key)
            "index:NSE:INDIA VIX",  # Primary key - exact match
            "index:NSEINDIAVIX",  # Canonical symbol variant
            "index:NSEINDIA VIX",  # Variant with space but no colon
            "market_data:indices:nse_india_vix",  # Legacy key with underscore
            "market_data:indices:nse_india vix",  # Legacy key with space
        ]
        
        import json
        for key in keys_to_try:
            try:
                vix_data_raw = self.redis_client.get(key)
                if vix_data_raw:
                    vix_data = json.loads(vix_data_raw) if isinstance(vix_data_raw, (str, bytes)) else vix_data_raw
                    if isinstance(vix_data, dict):
                        val = vix_data.get("last_price") or vix_data.get("close") or vix_data.get("value")
                        if val is not None:
                            return float(val)
            except Exception as e:
                logger.debug(f"⚠️ [VIX_REGIMES] Error reading key {key}: {e}")
                continue
        
        # Fallback to zero when unavailable
        return 0.0

    def _get_regime_thresholds(self, regime: str) -> Dict[str, Any]:
        """SINGLE SOURCE OF TRUTH: Use thresholds from config/thresholds.py"""
        try:
            # ✅ SINGLE SOURCE OF TRUTH: Use thresholds from shared_core.config_utils.thresholds
            from shared_core.config_utils.thresholds import VIX_REGIME_MULTIPLIERS, normalize_vix_regime
            normalized_regime = normalize_vix_regime(regime)
            multipliers = VIX_REGIME_MULTIPLIERS.get(normalized_regime, {})
            
            return {
                "momentum_multiplier": multipliers.get("upside_momentum", 1.0),
                "volume_multiplier": multipliers.get("volume_spike", 1.0),
                "expected_move_adjustment": multipliers.get("min_move", 1.0),
                "allow_breakouts": True,
                "allow_ote_setups": True,
                "allow_premium_discount": True,
            }
        except ImportError:
            # Fallback to hardcoded values if config not available
            return {
                "momentum_multiplier": 1.0,
                "volume_multiplier": 1.0,
                "expected_move_adjustment": 1.0,
                "allow_breakouts": True,
                "allow_ote_setups": True,
                "allow_premium_discount": True,
            }

    def is_pattern_type_allowed(self, pattern_type: str, regime: Dict[str, Any] | None = None) -> bool:
        if regime is None:
            regime = self.get_current_regime()
        th = regime.get("thresholds", {})
        p = (pattern_type or "").lower()
        if "breakout" in p and not th.get("allow_breakouts", True):
            return False
        if "ote" in p and not th.get("allow_ote_setups", True):
            return False
        if ("premium_zone" in p or "discount_zone" in p) and not th.get("allow_premium_discount", True):
            return False
        return True

