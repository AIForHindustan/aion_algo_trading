#!/opt/homebrew/bin/python3.13
"""
ENHANCED ICT Fair Value Gap Detector for VWAP-Based Straddle & Options Trading
Updated: 2025-11-22 
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import logging
import numpy as np

logger = logging.getLogger(__name__)

class EnhancedICTFVGDetector:
    """
    Enhanced FVG detector with VWAP integration for straddle and options trading.
    
    Key Features:
    - Volume-confirmed FVGs
    - VWAP relationship analysis
    - Options-specific FVG significance
    - Straddle entry/exit signals
    - Risk-adjusted position sizing
    """

    def __init__(self, vwap_lookback: int = 20, min_volume_ratio: float = 1.2):
        self.vwap_lookback = vwap_lookback
        self.min_volume_ratio = min_volume_ratio
        self.logger = logging.getLogger(__name__)

    def detect_volume_confirmed_fvg(self, candles: List[Dict], volume_data: List[Dict] = None) -> List[Dict]:
        """
        Detect FVGs with volume confirmation for higher reliability.
        
        Args:
            candles: OHLC data with potential volume
            volume_data: Separate volume data if not in candles
            
        Returns:
            List of volume-confirmed FVGs
        """
        fvgs = []
        if not candles or len(candles) < 3:
            return fvgs

        for i in range(2, len(candles)):
            try:
                prior = candles[i-2]
                previous = candles[i-1] 
                current = candles[i]

                # Extract prices with validation
                prior_high = float(prior.get("high", 0))
                prior_low = float(prior.get("low", 0))
                current_high = float(current.get("high", 0))
                current_low = float(current.get("low", 0))

                # Get volume data
                prior_volume = self._extract_volume(prior, volume_data, i-2)
                current_volume = self._extract_volume(current, volume_data, i)

                # Bullish FVG: current low > prior high
                if current_low > prior_high:
                    strength = (current_low - prior_high) / max(1e-9, prior_high)
                    volume_ratio = current_volume / max(1, prior_volume)
                    
                    if volume_ratio >= self.min_volume_ratio:
                        fvgs.append({
                            "type": "bullish",
                            "gap_low": prior_high,
                            "gap_high": current_low,
                            "strength": strength,
                            "volume_ratio": volume_ratio,
                            "timestamp": current.get("timestamp"),
                            "confirmed": True,
                            "significance": self._calculate_significance(strength, volume_ratio)
                        })

                # Bearish FVG: current high < prior low
                elif current_high < prior_low:
                    strength = (prior_low - current_high) / max(1e-9, current_high)
                    volume_ratio = current_volume / max(1, prior_volume)
                    
                    if volume_ratio >= self.min_volume_ratio:
                        fvgs.append({
                            "type": "bearish", 
                            "gap_low": current_high,
                            "gap_high": prior_low,
                            "strength": strength,
                            "volume_ratio": volume_ratio,
                            "timestamp": current.get("timestamp"),
                            "confirmed": True,
                            "significance": self._calculate_significance(strength, volume_ratio)
                        })

            except Exception as e:
                self.logger.debug(f"FVG detection error at index {i}: {e}")
                continue

        return fvgs

    def _extract_volume(self, candle: Dict, volume_data: List[Dict], index: int) -> float:
        """Extract volume from candle or separate volume data."""
        if "volume" in candle:
            return float(candle.get("volume", 0))
        elif volume_data and index < len(volume_data):
            return float(volume_data[index].get("volume", 0))
        return 0.0

    def _calculate_significance(self, strength: float, volume_ratio: float) -> str:
        """Calculate FVG significance level."""
        score = (strength * 100) + (volume_ratio * 10)
        
        if score > 30:
            return "HIGH"
        elif score > 15:
            return "MEDIUM"
        else:
            return "LOW"

    def analyze_fvg_vwap_relationship(self, fvg: Dict, vwap: float, price: float) -> Dict[str, Any]:
        """
        Analyze FVG relationship with VWAP for straddle trading signals.
        
        Args:
            fvg: FVG dictionary
            vwap: Current VWAP value
            price: Current price
            
        Returns:
            VWAP relationship analysis
        """
        try:
            gap_midpoint = (fvg["gap_low"] + fvg["gap_high"]) / 2
            distance_to_vwap = abs(gap_midpoint - vwap) / max(1e-9, vwap)
            price_in_gap = fvg["gap_low"] <= price <= fvg["gap_high"]
            
            # VWAP proximity analysis
            if distance_to_vwap <= 0.005:  # 0.5% from VWAP
                vwap_proximity = "VERY_CLOSE"
            elif distance_to_vwap <= 0.01:  # 1% from VWAP
                vwap_proximity = "CLOSE" 
            elif distance_to_vwap <= 0.02:  # 2% from VWAP
                vwap_proximity = "MODERATE"
            else:
                vwap_proximity = "FAR"
            
            # Straddle signal logic
            straddle_signal = self._generate_straddle_signal(fvg, vwap, price, price_in_gap)
            
            return {
                "vwap_proximity": vwap_proximity,
                "distance_to_vwap_pct": distance_to_vwap * 100,
                "price_in_gap": price_in_gap,
                "gap_midpoint": gap_midpoint,
                "straddle_signal": straddle_signal,
                "vwap": vwap
            }
            
        except Exception as e:
            self.logger.error(f"VWAP relationship analysis failed: {e}")
            return {}

    def _generate_straddle_signal(self, fvg: Dict, vwap: float, price: float, price_in_gap: bool) -> Dict[str, Any]:
        """
        Generate straddle trading signals based on FVG and VWAP.
        """
        gap_midpoint = (fvg["gap_low"] + fvg["gap_high"]) / 2
        gap_width = fvg["gap_high"] - fvg["gap_low"]
        gap_width_pct = (gap_width / gap_midpoint) * 100
        
        # Base signal parameters
        signal = {
            "action": "HOLD",
            "confidence": 0.0,
            "expected_move_pct": gap_width_pct,
            "timeframe": "INTRADAY"
        }
        
        # High significance FVG near VWAP
        if (fvg["significance"] == "HIGH" and 
            abs(gap_midpoint - vwap) / vwap <= 0.01):
            
            if price_in_gap:
                # Price in gap - potential reversal/straddle opportunity
                if fvg["type"] == "bullish":
                    signal.update({
                        "action": "STRADDLE_BUY",
                        "confidence": min(0.8, fvg["strength"] * 10 + fvg["volume_ratio"] * 0.1),
                        "rationale": "Bullish FVG at VWAP - expect upside breakout"
                    })
                else:  # bearish
                    signal.update({
                        "action": "STRADDLE_SELL", 
                        "confidence": min(0.8, fvg["strength"] * 10 + fvg["volume_ratio"] * 0.1),
                        "rationale": "Bearish FVG at VWAP - expect downside breakout"
                    })
            else:
                # Price approaching gap - watch for entry
                signal.update({
                    "action": "MONITOR",
                    "confidence": 0.6,
                    "rationale": "FVG near VWAP - potential straddle entry zone"
                })
        
        return signal

    def calculate_straddle_parameters(self, fvg: Dict, underlying_price: float, 
                                   iv: float, days_to_expiry: int) -> Dict[str, Any]:
        """
        Calculate options straddle parameters based on FVG analysis.
        
        Args:
            fvg: FVG analysis
            underlying_price: Current underlying price
            iv: Implied volatility
            days_to_expiry: Days to option expiry
            
        Returns:
            Straddle trading parameters
        """
        try:
            gap_width = fvg["gap_high"] - fvg["gap_low"]
            gap_width_pct = (gap_width / underlying_price) * 100
            
            # Calculate expected move using IV and time
            daily_volatility = iv / np.sqrt(252)
            expected_move = underlying_price * daily_volatility * np.sqrt(days_to_expiry)
            expected_move_pct = (expected_move / underlying_price) * 100
            
            # Straddle selection logic
            strike_selection = self._select_straddle_strikes(fvg, underlying_price, expected_move)
            position_size = self._calculate_straddle_size(fvg, expected_move_pct, gap_width_pct)
            
            return {
                "expected_move_pct": expected_move_pct,
                "fvg_width_pct": gap_width_pct,
                "strike_selection": strike_selection,
                "position_size": position_size,
                "target_profit_pct": min(50, gap_width_pct * 0.8),  # 80% of gap width
                "stop_loss_pct": max(10, gap_width_pct * 1.2),      # 120% of gap width
                "max_hold_days": min(5, days_to_expiry - 1),
                "iv_rank": iv,  # Could be enhanced with IV percentile
                "trade_viability": expected_move_pct >= gap_width_pct * 0.7
            }
            
        except Exception as e:
            self.logger.error(f"Straddle parameter calculation failed: {e}")
            return {}

    def _select_straddle_strikes(self, fvg: Dict, underlying_price: float, 
                               expected_move: float) -> Dict[str, Any]:
        """Select appropriate strikes for straddle based on FVG analysis."""
        gap_midpoint = (fvg["gap_low"] + fvg["gap_high"]) / 2
        
        # ATM straddle around gap midpoint
        atm_strike = round(gap_midpoint / 0.05) * 0.05  # Round to nearest 0.05
        
        # Adjust based on FVG type and strength
        if fvg["type"] == "bullish":
            call_strike = atm_strike
            put_strike = atm_strike
            bias = "SLIGHTLY_BULLISH"
        else:  # bearish
            call_strike = atm_strike  
            put_strike = atm_strike
            bias = "SLIGHTLY_BEARISH"
        
        return {
            "call_strike": call_strike,
            "put_strike": put_strike,
            "strike_bias": bias,
            "distance_to_underlying": abs(atm_strike - underlying_price) / underlying_price * 100
        }

    def _calculate_straddle_size(self, fvg: Dict, expected_move_pct: float, 
                               gap_width_pct: float) -> Dict[str, float]:
        """Calculate position size for straddle trade."""
        # Base size on FVG significance and expected move
        base_size = 1.0  # Normalized size
        
        # Adjust for significance
        significance_multiplier = {
            "HIGH": 1.2,
            "MEDIUM": 1.0, 
            "LOW": 0.7
        }.get(fvg["significance"], 1.0)
        
        # Adjust for expected move vs gap width
        move_ratio = expected_move_pct / max(1e-9, gap_width_pct)
        if move_ratio >= 1.5:
            move_multiplier = 1.3
        elif move_ratio >= 1.0:
            move_multiplier = 1.0
        else:
            move_multiplier = 0.7
            
        final_size = base_size * significance_multiplier * move_multiplier
        
        return {
            "normalized_size": final_size,
            "risk_unit": 0.01,  # 1% risk per unit
            "max_position_size": min(3.0, final_size * 2)  # Cap at 3x base
        }

    def get_fvg_options_strategy(self, fvg: Dict, underlying_symbol: str,
                               current_price: float, iv: float, dte: int) -> Dict[str, Any]:
        """
        Generate complete options strategy based on FVG analysis.
        """
        straddle_params = self.calculate_straddle_parameters(fvg, current_price, iv, dte)
        vwap_analysis = self.analyze_fvg_vwap_relationship(fvg, current_price, current_price)
        
        strategy = {
            "symbol": underlying_symbol,
            "fvg_type": fvg["type"],
            "fvg_significance": fvg["significance"],
            "timestamp": datetime.now().isoformat(),
            "straddle_parameters": straddle_params,
            "vwap_analysis": vwap_analysis,
            "entry_conditions": self._get_entry_conditions(fvg, vwap_analysis, straddle_params),
            "exit_conditions": self._get_exit_conditions(fvg, straddle_params),
            "risk_management": self._get_risk_management(fvg, straddle_params)
        }
        
        return strategy

    def _get_entry_conditions(self, fvg: Dict, vwap_analysis: Dict, 
                            straddle_params: Dict) -> Dict[str, Any]:
        """Define entry conditions for FVG-based straddle."""
        return {
            "price_in_gap": vwap_analysis.get("price_in_gap", False),
            "vwap_proximity": vwap_analysis.get("vwap_proximity"),
            "min_confidence": 0.65,
            "volume_confirmation": fvg.get("volume_ratio", 0) >= self.min_volume_ratio,
            "trade_viability": straddle_params.get("trade_viability", False),
            "timeframe": "15min-1h"  # Optimal entry timeframe
        }

    def _get_exit_conditions(self, fvg: Dict, straddle_params: Dict) -> Dict[str, Any]:
        """Define exit conditions for FVG-based straddle."""
        return {
            "profit_target_pct": straddle_params.get("target_profit_pct", 30),
            "stop_loss_pct": straddle_params.get("stop_loss_pct", 15),
            "time_stop_days": straddle_params.get("max_hold_days", 5),
            "gap_fill_completion": True,  # Exit when gap is filled
            "volatility_contraction": False  # Exit if IV drops significantly
        }

    def _get_risk_management(self, fvg: Dict, straddle_params: Dict) -> Dict[str, Any]:
        """Define risk management parameters."""
        position_size = straddle_params.get("position_size", {}).get("normalized_size", 1.0)
        
        return {
            "position_size": position_size,
            "max_portfolio_risk": 0.02,  # 2% max portfolio risk
            "risk_reward_ratio": 2.0,
            "iv_impact": "HIGH" if straddle_params.get("iv_rank", 0) > 0.3 else "LOW",
            "hedging_required": position_size > 2.0
        }

    def is_price_in_fvg(self, current_price: float, fvg: Dict) -> bool:
        """Check if price is within FVG range."""
        try:
            return fvg["gap_low"] <= current_price <= fvg["gap_high"]
        except Exception:
            return False

    def get_fvg_fill_status(self, current_price: float, fvg: Dict) -> str:
        """Get FVG fill status."""
        if self.is_price_in_fvg(current_price, fvg):
            gap_midpoint = (fvg["gap_low"] + fvg["gap_high"]) / 2
            if current_price >= gap_midpoint:
                return "PARTIALLY_FILLED_ABOVE_MID"
            else:
                return "PARTIALLY_FILLED_BELOW_MID"
        elif current_price > fvg["gap_high"]:
            return "FULLY_FILLED_UP"
        elif current_price < fvg["gap_low"]:
            return "FULLY_FILLED_DOWN"
        else:
            return "UNTOUCHED"

# Example usage and testing
def example_usage():
    """Example of how to use the enhanced FVG detector for straddle trading."""
    detector = EnhancedICTFVGDetector()
    
    # Sample candle data (with volume)
    candles = [
        {"high": 100, "low": 98, "open": 99, "close": 99.5, "volume": 10000, "timestamp": "2024-01-01 10:00"},
        {"high": 101, "low": 99, "open": 99.5, "close": 100.5, "volume": 15000, "timestamp": "2024-01-01 10:15"},
        {"high": 103, "low": 102, "open": 102, "close": 102.5, "volume": 20000, "timestamp": "2024-01-01 10:30"},
    ]
    
    # Detect FVGs
    fvgs = detector.detect_volume_confirmed_fvg(candles)
    
    for fvg in fvgs:
        print(f"Detected FVG: {fvg}")
        
        # Generate options strategy
        strategy = detector.get_fvg_options_strategy(
            fvg=fvg,
            underlying_symbol="NIFTY",
            current_price=101.5,
            iv=0.15,  # 15% IV
            dte=5     # 5 days to expiry
        )
        
        print(f"Options Strategy: {strategy}")

# ✅ ALIAS: Provide backward compatibility alias
ICTFVGDetector = EnhancedICTFVGDetector

if __name__ == "__main__":
    example_usage()