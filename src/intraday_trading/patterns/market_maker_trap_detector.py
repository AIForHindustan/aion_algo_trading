"""
Market Maker Trap Detector
==========================

Detects market maker manipulation patterns and traps to protect retail traders.
Integrates with options data, volume analysis, and price action.

Created: January 2025
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class MarketMakerTrapDetector:
    """
    Detects market maker manipulation patterns including:
    - Options OI concentration traps
    - Max pain manipulation
    - Block deal patterns
    - Institutional volume anomalies
    - Pin risk scenarios
    - Gamma squeeze setups
    """

    def __init__(self, redis_client=None):
        """
        Initialize market maker trap detector.

        Args:
            redis_client: Redis client for historical data access
        """
        self.redis_client = redis_client

        # Detection thresholds
        self.OI_CONCENTRATION_THRESHOLD = 0.70  # 70% OI in few strikes
        self.MAX_PAIN_PROXIMITY_THRESHOLD = 0.02  # 2% from max pain
        self.BLOCK_DEALS_THRESHOLD = 0.30  # 30% block deal volume
        self.PUT_CALL_RATIO_EXTREME_LOW = 0.40  # Extreme bullish sentiment
        self.PUT_CALL_RATIO_EXTREME_HIGH = 2.50  # Extreme bearish sentiment
        self.VWAP_DEVIATION_THRESHOLD = 0.015  # 1.5% from VWAP
        self.SUDDEN_VOLUME_SPIKE = 5.0  # 5x normal volume
        self.INSTITUTIONAL_VOLUME_THRESHOLD = 0.40  # 40% institutional

        # Stats tracking
        self.stats = {
            "total_checks": 0,
            "traps_detected": 0,
            "oi_concentration_traps": 0,
            "max_pain_traps": 0,
            "block_deal_traps": 0,
            "institutional_traps": 0,
            "pin_risk_scenarios": 0,
            "gamma_squeeze_setups": 0,
            "sentiment_extreme_traps": 0,
        }

    def detect_market_maker_trap(
        self, symbol: str, indicators: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Comprehensive market maker trap detection.

        Args:
            symbol: Trading symbol
            indicators: Indicator dictionary with price, volume, options data

        Returns:
            Dict with trap detection results
        """
        self.stats["total_checks"] += 1

        result = {
            "is_trap": False,
            "trap_types": [],
            "confidence_multiplier": 1.0,  # Multiply original confidence by this
            "severity": "none",  # none, low, medium, high, critical
            "reasons": [],
            "details": {},
        }

        # Run all detection checks
        checks = [
            self._check_oi_concentration,
            self._check_max_pain_manipulation,
            self._check_block_deals,
            self._check_institutional_volume,
            self._check_pin_risk,
            self._check_gamma_squeeze,
            self._check_sentiment_extremes,
            self._check_vwap_manipulation,
            self._check_volume_anomaly,
        ]

        for check in checks:
            try:
                check_result = check(symbol, indicators)
                if check_result["detected"]:
                    result["is_trap"] = True
                    result["trap_types"].append(check_result["type"])
                    result["reasons"].append(check_result["reason"])
                    result["details"][check_result["type"]] = check_result["details"]

                    # Accumulate confidence reduction
                    result["confidence_multiplier"] *= check_result[
                        "confidence_multiplier"
                    ]

            except Exception as e:
                logger.debug(f"Market maker check failed for {symbol}: {e}")

        # Determine overall severity
        if result["is_trap"]:
            self.stats["traps_detected"] += 1
            result["severity"] = self._calculate_severity(
                result["trap_types"], result["confidence_multiplier"]
            )

            # Log trap detection
            logger.warning(
                f"🎣 Market maker trap detected: {symbol} - "
                f"{len(result['trap_types'])} patterns - "
                f"Severity: {result['severity']} - "
                f"Confidence reduction: {(1 - result['confidence_multiplier']) * 100:.0f}%"
            )

        return result

    def _check_oi_concentration(
        self, symbol: str, indicators: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check for high options OI concentration (pin risk)"""
        result = {
            "detected": False,
            "type": "oi_concentration",
            "reason": "",
            "confidence_multiplier": 1.0,
            "details": {},
        }

        oi_concentration = indicators.get("options_oi_concentration", 0.0)

        if oi_concentration > self.OI_CONCENTRATION_THRESHOLD:
            result["detected"] = True
            result["reason"] = (
                f"High OI concentration: {oi_concentration:.1%} "
                f"(threshold: {self.OI_CONCENTRATION_THRESHOLD:.1%})"
            )
            result["confidence_multiplier"] = 0.40  # 60% reduction
            result["details"] = {
                "oi_concentration": oi_concentration,
                "threshold": self.OI_CONCENTRATION_THRESHOLD,
                "max_pain_strike": indicators.get("max_pain_strike"),
            }
            self.stats["oi_concentration_traps"] += 1

        return result

    def _check_max_pain_manipulation(
        self, symbol: str, indicators: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check if price is near max pain (likely to be pinned)"""
        result = {
            "detected": False,
            "type": "max_pain",
            "reason": "",
            "confidence_multiplier": 1.0,
            "details": {},
        }

        max_pain = indicators.get("max_pain_price", 0)
        current_price = indicators.get("last_price", 0)

        if max_pain > 0 and current_price > 0:
            distance_pct = abs(max_pain - current_price) / current_price

            if distance_pct < self.MAX_PAIN_PROXIMITY_THRESHOLD:
                result["detected"] = True
                result["reason"] = (
                    f"Price near max pain: {distance_pct:.2%} away "
                    f"(Max Pain: ₹{max_pain:.2f}, Current: ₹{current_price:.2f})"
                )
                result["confidence_multiplier"] = 0.50  # 50% reduction
                result["details"] = {
                    "max_pain_price": max_pain,
                    "current_price": current_price,
                    "distance_pct": distance_pct,
                }
                self.stats["max_pain_traps"] += 1

        return result

    def _check_block_deals(
        self, symbol: str, indicators: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check for high block deal activity (institutional manipulation)"""
        result = {
            "detected": False,
            "type": "block_deals",
            "reason": "",
            "confidence_multiplier": 1.0,
            "details": {},
        }

        block_deals_ratio = indicators.get("block_deals_ratio", 0.0)

        if block_deals_ratio > self.BLOCK_DEALS_THRESHOLD:
            result["detected"] = True
            result["reason"] = (
                f"High block deal activity: {block_deals_ratio:.1%} of volume "
                f"(threshold: {self.BLOCK_DEALS_THRESHOLD:.1%})"
            )
            result["confidence_multiplier"] = 0.60  # 40% reduction
            result["details"] = {
                "block_deals_ratio": block_deals_ratio,
                "threshold": self.BLOCK_DEALS_THRESHOLD,
                "block_deal_volume": indicators.get("block_deal_volume"),
            }
            self.stats["block_deal_traps"] += 1

        return result

    def _check_institutional_volume(
        self, symbol: str, indicators: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check for dominant institutional volume"""
        result = {
            "detected": False,
            "type": "institutional_volume",
            "reason": "",
            "confidence_multiplier": 1.0,
            "details": {},
        }

        institutional_pct = indicators.get("institutional_volume_pct", 0.0)

        if institutional_pct > self.INSTITUTIONAL_VOLUME_THRESHOLD:
            result["detected"] = True
            result["reason"] = (
                f"Institutional volume dominance: {institutional_pct:.1%} "
                f"(threshold: {self.INSTITUTIONAL_VOLUME_THRESHOLD:.1%})"
            )
            result["confidence_multiplier"] = 0.65  # 35% reduction
            result["details"] = {
                "institutional_pct": institutional_pct,
                "threshold": self.INSTITUTIONAL_VOLUME_THRESHOLD,
            }
            self.stats["institutional_traps"] += 1

        return result

    def _check_pin_risk(
        self, symbol: str, indicators: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check for pin risk scenario (price pinned to strike)"""
        result = {
            "detected": False,
            "type": "pin_risk",
            "reason": "",
            "confidence_multiplier": 1.0,
            "details": {},
        }

        # Check if near major strike with high gamma
        current_price = indicators.get("last_price", 0)
        nearest_strike = indicators.get("nearest_high_oi_strike", 0)
        gamma_exposure = indicators.get("gamma_exposure", 0)

        if current_price > 0 and nearest_strike > 0:
            distance = abs(current_price - nearest_strike) / current_price

            # High gamma + close to strike = pin risk
            if distance < 0.005 and gamma_exposure > 100000000:  # 10 crore gamma
                result["detected"] = True
                result["reason"] = (
                    f"Pin risk: Price {distance:.2%} from strike ₹{nearest_strike:.2f} "
                    f"with high gamma exposure (₹{gamma_exposure / 10000000:.1f}Cr)"
                )
                result["confidence_multiplier"] = 0.30  # 70% reduction
                result["details"] = {
                    "current_price": current_price,
                    "nearest_strike": nearest_strike,
                    "distance_pct": distance,
                    "gamma_exposure": gamma_exposure,
                }
                self.stats["pin_risk_scenarios"] += 1

        return result

    def _check_gamma_squeeze(
        self, symbol: str, indicators: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check for gamma squeeze setup (can cause violent moves)"""
        result = {
            "detected": False,
            "type": "gamma_squeeze",
            "reason": "",
            "confidence_multiplier": 1.0,
            "details": {},
        }

        # High call OI + price above strikes + high volume = gamma squeeze
        call_oi = indicators.get("total_call_oi", 0)
        put_oi = indicators.get("total_put_oi", 0)
        volume_ratio = indicators.get("volume_ratio", 0.0)  # Default to 0.0 to highlight issues

        if call_oi > 0 and put_oi > 0:
            call_put_ratio = call_oi / put_oi

            # Extreme call concentration + high volume = squeeze risk
            if call_put_ratio > 2.0 and volume_ratio > 3.0:
                result["detected"] = True
                result["reason"] = (
                    f"Gamma squeeze risk: Call/Put ratio {call_put_ratio:.2f} "
                    f"with {volume_ratio:.1f}x volume"
                )
                result["confidence_multiplier"] = 0.55  # 45% reduction
                result["details"] = {
                    "call_put_oi_ratio": call_put_ratio,
                    "volume_ratio": volume_ratio,
                    "total_call_oi": call_oi,
                    "total_put_oi": put_oi,
                }
                self.stats["gamma_squeeze_setups"] += 1

        return result

    def _check_sentiment_extremes(
        self, symbol: str, indicators: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check for extreme sentiment (contrarian indicator)"""
        result = {
            "detected": False,
            "type": "sentiment_extreme",
            "reason": "",
            "confidence_multiplier": 1.0,
            "details": {},
        }

        pcr = indicators.get("put_call_ratio", 1.0)

        # Extreme bullish sentiment (too many calls) = likely reversal
        if pcr < self.PUT_CALL_RATIO_EXTREME_LOW:
            result["detected"] = True
            result["reason"] = (
                f"Extreme bullish sentiment: PCR {pcr:.2f} "
                f"(extreme low: {self.PUT_CALL_RATIO_EXTREME_LOW})"
            )
            result["confidence_multiplier"] = 0.70  # 30% reduction
            result["details"] = {"put_call_ratio": pcr, "sentiment": "extreme_bullish"}
            self.stats["sentiment_extreme_traps"] += 1

        # Extreme bearish sentiment (too many puts) = likely reversal
        elif pcr > self.PUT_CALL_RATIO_EXTREME_HIGH:
            result["detected"] = True
            result["reason"] = (
                f"Extreme bearish sentiment: PCR {pcr:.2f} "
                f"(extreme high: {self.PUT_CALL_RATIO_EXTREME_HIGH})"
            )
            result["confidence_multiplier"] = 0.70  # 30% reduction
            result["details"] = {"put_call_ratio": pcr, "sentiment": "extreme_bearish"}
            self.stats["sentiment_extreme_traps"] += 1

        return result

    def _check_vwap_manipulation(
        self, symbol: str, indicators: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check for VWAP manipulation patterns"""
        result = {
            "detected": False,
            "type": "vwap_manipulation",
            "reason": "",
            "confidence_multiplier": 1.0,
            "details": {},
        }

        vwap = indicators.get("vwap", 0)
        current_price = indicators.get("last_price", 0)
        volume_ratio = indicators.get("volume_ratio", 0.0)  # Default to 0.0 to highlight issues

        if vwap > 0 and current_price > 0:
            deviation = abs(current_price - vwap) / vwap

            # Large deviation from VWAP + high volume = manipulation
            if deviation > self.VWAP_DEVIATION_THRESHOLD and volume_ratio > 3.0:
                result["detected"] = True
                result["reason"] = (
                    f"VWAP manipulation: {deviation:.2%} deviation "
                    f"with {volume_ratio:.1f}x volume"
                )
                result["confidence_multiplier"] = 0.75  # 25% reduction
                result["details"] = {
                    "vwap": vwap,
                    "current_price": current_price,
                    "deviation_pct": deviation,
                    "volume_ratio": volume_ratio,
                }

        return result

    def _check_volume_anomaly(
        self, symbol: str, indicators: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check for sudden volume spikes (potential manipulation)"""
        result = {
            "detected": False,
            "type": "volume_anomaly",
            "reason": "",
            "confidence_multiplier": 1.0,
            "details": {},
        }

        volume_ratio = indicators.get("volume_ratio", 0.0)  # Default to 0.0 to highlight issues
        price_change = abs(indicators.get("price_change", 0))

        # Huge volume spike without proportional price move = manipulation
        if volume_ratio > self.SUDDEN_VOLUME_SPIKE and price_change < 0.5:
            result["detected"] = True
            result["reason"] = (
                f"Volume anomaly: {volume_ratio:.1f}x volume "
                f"with only {price_change:.2f}% price change"
            )
            result["confidence_multiplier"] = 0.80  # 20% reduction
            result["details"] = {
                "volume_ratio": volume_ratio,
                "price_change": price_change,
                "threshold": self.SUDDEN_VOLUME_SPIKE,
            }

        return result

    def _calculate_severity(
        self, trap_types: List[str], confidence_multiplier: float
    ) -> str:
        """Calculate overall trap severity"""
        num_traps = len(trap_types)
        confidence_reduction = 1.0 - confidence_multiplier

        # Critical: 3+ traps OR 70%+ confidence reduction
        if num_traps >= 3 or confidence_reduction >= 0.70:
            return "critical"

        # High: 2+ traps OR 50%+ confidence reduction
        if num_traps >= 2 or confidence_reduction >= 0.50:
            return "high"

        # Medium: 1 trap with 30%+ confidence reduction
        if confidence_reduction >= 0.30:
            return "medium"

        # Low: 1 trap with less reduction
        if num_traps >= 1:
            return "low"

        return "none"

    def get_stats(self) -> Dict[str, int]:
        """Get detection statistics"""
        return self.stats.copy()


# Global instance
_trap_detector_instance: Optional[MarketMakerTrapDetector] = None


def get_trap_detector(redis_client=None) -> MarketMakerTrapDetector:
    """Get global market maker trap detector instance (singleton)"""
    global _trap_detector_instance

    if _trap_detector_instance is None:
        _trap_detector_instance = MarketMakerTrapDetector(redis_client)

    return _trap_detector_instance


# Convenience function for quick checks
def detect_market_maker_trap(
    symbol: str, indicators: Dict[str, Any], redis_client=None
) -> Dict[str, Any]:
    """
    Convenience function for market maker trap detection.

    Args:
        symbol: Trading symbol
        indicators: Indicator dictionary
        redis_client: Optional Redis client

    Returns:
        Trap detection result dictionary
    """
    detector = get_trap_detector(redis_client)
    return detector.detect_market_maker_trap(symbol, indicators)


if __name__ == "__main__":
    # Self-test
    print("=" * 80)
    print("MARKET MAKER TRAP DETECTOR - SELF TEST")
    print("=" * 80)

    detector = get_trap_detector()

    # Test 1: Clean signal (no trap)
    print("\n1️⃣  Test: Clean Signal (No Trap)")
    clean_indicators = {
        "last_price": 45000.0,
        "volume_ratio": 1.5,
        "options_oi_concentration": 0.30,
        "max_pain_price": 44000.0,
        "block_deals_ratio": 0.10,
    }
    result = detector.detect_market_maker_trap(
        "NFO:BANKNIFTY28OCTFUT", clean_indicators
    )
    print(f"   Is Trap: {result['is_trap']}")
    print(f"   Severity: {result['severity']}")

    # Test 2: High OI concentration trap
    print("\n2️⃣  Test: High OI Concentration Trap")
    oi_trap_indicators = {
        "last_price": 45000.0,
        "volume_ratio": 2.0,
        "options_oi_concentration": 0.85,  # High concentration
        "max_pain_strike": 45000,
    }
    result = detector.detect_market_maker_trap(
        "NFO:BANKNIFTY28OCTFUT", oi_trap_indicators
    )
    print(f"   Is Trap: {result['is_trap']}")
    print(f"   Trap Types: {result['trap_types']}")
    print(f"   Confidence Multiplier: {result['confidence_multiplier']:.2f}")
    print(f"   Severity: {result['severity']}")

    # Test 3: Max pain manipulation
    print("\n3️⃣  Test: Max Pain Manipulation")
    max_pain_indicators = {
        "last_price": 45000.0,
        "max_pain_price": 45050.0,  # Very close to max pain
        "volume_ratio": 2.0,
    }
    result = detector.detect_market_maker_trap(
        "NFO:BANKNIFTY28OCTFUT", max_pain_indicators
    )
    print(f"   Is Trap: {result['is_trap']}")
    print(f"   Reasons: {result['reasons']}")
    print(f"   Confidence Multiplier: {result['confidence_multiplier']:.2f}")

    # Test 4: Multiple trap indicators (critical)
    print("\n4️⃣  Test: Multiple Traps (Critical)")
    critical_indicators = {
        "last_price": 45000.0,
        "options_oi_concentration": 0.80,
        "max_pain_price": 45000.0,
        "block_deals_ratio": 0.35,
        "volume_ratio": 3.0,
        "put_call_ratio": 0.35,  # Extreme bullish
    }
    result = detector.detect_market_maker_trap(
        "NFO:BANKNIFTY28OCTFUT", critical_indicators
    )
    print(f"   Is Trap: {result['is_trap']}")
    print(f"   Trap Types: {result['trap_types']}")
    print(f"   Confidence Multiplier: {result['confidence_multiplier']:.2f}")
    print(f"   Severity: {result['severity']}")
    print(
        f"   Original 85% conf → {85 * result['confidence_multiplier']:.0f}% adjusted"
    )

    # Show stats
    print("\n📊 Detection Statistics:")
    stats = detector.get_stats()
    print(f"   Total Checks: {stats['total_checks']}")
    print(f"   Traps Detected: {stats['traps_detected']}")
    print(f"   OI Concentration Traps: {stats['oi_concentration_traps']}")
    print(f"   Max Pain Traps: {stats['max_pain_traps']}")
    print(f"   Block Deal Traps: {stats['block_deal_traps']}")

    print("\n" + "=" * 80)
    print("✅ Self-test complete!")
    print("=" * 80)
