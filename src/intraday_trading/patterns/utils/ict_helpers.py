"""
ICT Pattern Helper Classes
==========================

Consolidated ICT pattern helper classes from individual files.
Contains killzone, premium/discount, risk management, and regime filtering.

Classes:
- ICTKillzoneDetector: Time-of-day killzone detection
- ICTPremiumDiscountDetector: Premium/discount zone computation
- ICTRiskManager: Risk management for F&O scalping
- ICTRegimeFilter: Regime-based setup filtering

Created: October 9, 2025
"""

from __future__ import annotations
from typing import Dict

from intraday_trading.patterns.ict.killzone import ICTKillzoneDetector
from intraday_trading.patterns.ict.premium_discount import ICTPremiumDiscountDetector

# Export all ICT helper classes
__all__ = [
    'ICTKillzoneDetector',
    'ICTPremiumDiscountDetector',
    'ICTRiskManager',
    'ICTRegimeFilter',
]

# Import VIX regime manager for filtering
try:
    from shared_core.utils.vix_regimes import VIXRegimeManager
except ImportError:
    class VIXRegimeManager:
        def __init__(self, redis_client):
            pass
        def get_current_regime(self):
            return {"regime": "NORMAL"}
        def is_pattern_type_allowed(self, pattern_type, regime):
            return True


class ICTRiskManager:
    """Very simple risk manager for F&O scalping sizing."""

    def calculate_position_size(self, account_size: float, risk_per_trade: float = 0.02) -> Dict:
        """
        Calculate position size based on account size and risk per trade.
        
        ✅ UNIFIED: Uses local implementation (TradingCalculations doesn't exist).
        """
        # ✅ FIXED: TradingCalculations doesn't exist, use local implementation
        risk_amount = float(account_size) * float(risk_per_trade)
        return {
            "account_size": account_size,
            "risk_per_trade": risk_amount,
            "position_size_futures": self._calc_futures_size(risk_amount),
            "position_size_options": self._calc_options_size(risk_amount),
            "daily_target": account_size * 0.01,
            "max_daily_loss": account_size * 0.02,
        }

    def _calc_futures_size(self, risk_amount: float) -> int:
        lot_risk_points = 50.0
        lots = max(1, int(risk_amount / lot_risk_points))
        return min(lots, 10)

    def _calc_options_size(self, risk_amount: float) -> int:
        per_option_risk = 20.0
        qty = max(1, int(risk_amount / per_option_risk))
        return min(qty, 1000)


class ICTRegimeFilter:
    """Filter ICT setups based on index regime and market context."""

    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.vix_regime_manager = VIXRegimeManager(redis_client)

    def should_filter_setup(self, symbol: str, setup: Dict, index_context: Dict | None = None) -> bool:
        if not index_context:
            index_context = self._get_index_context(symbol)

        setup_type = setup.get("pattern", setup.get("pattern_type", ""))
        signal = setup.get("signal", "NEUTRAL")
        index_trend = index_context.get("trend", "neutral")
        index_strength = float(index_context.get("strength", 0) or 0)

        # Filter counter-trend in strong markets
        if abs(index_strength) > 0.7:
            if index_trend == "bullish" and signal == "SHORT" and not setup_type.endswith("premium_zone_short"):
                return True
            if index_trend == "bearish" and signal == "BUY" and not setup_type.endswith("discount_zone_long"):
                return True

        # Unified regime gating via VIXRegimeManager
        regime = self.vix_regime_manager.get_current_regime()
        regime_name = regime.get("regime", "NORMAL")
        if regime_name == "PANIC" and not (
            setup_type.endswith("premium_zone_short") or setup_type.endswith("discount_zone_long")
        ):
            return True

        # OTE in very low volatility environment tends to fail
        atr_percent = float(index_context.get("atr_percent", 0) or 0)
        if "ote" in setup_type and atr_percent < 0.008:
            return True

        # Final pattern-type allowance
        if not self.vix_regime_manager.is_pattern_type_allowed(setup_type, regime):
            return True
        return False

    def _get_index_context(self, symbol: str) -> Dict:
        index_map = {
            "NIFTY": "nifty50",
            "BANKNIFTY": "niftybank",
            "FINNIFTY": "finnifty",
            "NFO:NIFTY": "nifty50",
            "NFO:BANKNIFTY": "niftybank",
            "NFO:FINNIFTY": "finnifty",
        }
        index_symbol = None
        for key, idx in index_map.items():
            if key in symbol:
                index_symbol = idx
                break
        if not index_symbol:
            return {"trend": "neutral", "strength": 0, "vix": 0, "atr_percent": 0}

        idx_data = self._get_index_data(index_symbol)
        vix_data = self._get_index_data("indiavix")

        trend = self._calculate_index_trend(idx_data)
        strength = self._calculate_trend_strength(idx_data)
        vix = (vix_data or {}).get("last_price", 0)
        atr_percent = self._calculate_atr_percent(idx_data)

        return {
            "trend": trend,
            "strength": strength,
            "vix": vix,
            "atr_percent": atr_percent,
            "index_symbol": index_symbol,
        }

    def _get_index_data(self, name: str) -> Dict | None:
        try:
            if hasattr(self.redis_client, "get_index"):
                data = self.redis_client.get_index(name)
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
        return None

    def _calculate_index_trend(self, index_data: Dict | None) -> str:
        if not index_data:
            return "neutral"
        try:
            chg = float(index_data.get("change_percent", 0))
            if chg > 0.2:
                return "bullish"
            if chg < -0.2:
                return "bearish"
        except Exception:
            pass
        return "neutral"

    def _calculate_trend_strength(self, index_data: Dict | None) -> float:
        # Placeholder: use change percent magnitude as proxy
        try:
            chg = abs(float(index_data.get("change_percent", 0)))
            return min(1.0, chg / 2.0)
        except Exception:
            return 0.5

    def _calculate_atr_percent(self, index_data: Dict | None) -> float:
        # Placeholder constant; replace with real ATR% if available
        return 0.01
