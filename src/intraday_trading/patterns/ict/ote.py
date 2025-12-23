#!/opt/homebrew/bin/python3.13
"""
ICT Optimal Trade Entry calculations.
Updated: 2025-10-27
"""
from __future__ import annotations

from typing import Dict


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
