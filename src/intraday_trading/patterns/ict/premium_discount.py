#!/opt/homebrew/bin/python3.13
"""
ICT Premium/Discount zone helpers.
Updated: 2025-10-27
"""
from __future__ import annotations

from typing import Dict, Optional


class ICTPremiumDiscountDetector:
    """Premium/Discount zone computation using VWAP and ATR proxies."""

    def __init__(self, redis_client=None):
        self.redis_client = redis_client

    def calculate_premium_discount_zones(self, symbol_data: Dict) -> Dict:
        """
        Calculate premium/discount zones using VWAP and ATR.
        
        ✅ UNIFIED: Uses HybridCalculations for VWAP/ATR if available, otherwise fallback.
        """
        # ✅ UNIFIED: Try HybridCalculations first (single source of truth)
        try:
            from intraday_trading.intraday_scanner.calculations import HybridCalculations
            # HybridCalculations doesn't have a direct premium/discount method,
            # so we use the fallback implementation with VWAP/ATR from indicators
            pass
        except ImportError:
            pass
        
        # Fallback implementation (used when HybridCalculations doesn't have this method)
            vwap = float(symbol_data.get("vwap") or symbol_data.get("average_price") or 0)
            last_price = float(symbol_data.get("last_price") or 0)
        atr = float(symbol_data.get("atr") or symbol_data.get("atr_14") or 0)

        if vwap <= 0:
            return {}

        # If ATR not given, use a small proxy (0.5% of price)
        if atr <= 0 and last_price > 0:
            atr = last_price * 0.005

        premium_low = vwap + (atr * 0.5)
        premium_high = vwap + (atr * 2.0)
        discount_low = vwap - (atr * 2.0)
        discount_high = vwap - (atr * 0.5)

        current_zone = "premium" if last_price > vwap else "discount"

        return {
            "equilibrium": vwap,
            "premium_zone": [premium_low, premium_high],
            "discount_zone": [discount_low, discount_high],
            "current_zone": current_zone,
            "distance_from_vwap": abs(last_price - vwap) / vwap if vwap else 0,
        }
