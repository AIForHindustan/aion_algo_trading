"""
MathDispatcher - Centralized Routing Layer for Pattern Mathematics

Provides a unified interface between pattern detectors and the mathematical engine
(PatternMathematics). Acts as a thin adapter layer that enables dynamic adjustments
through context passing while maintaining backward compatibility with legacy code.

Architecture:
    Pattern Detector → MathDispatcher → PatternMathematics
                                    → Risk Calculator (optional)

Key Features:
    - Unified interface for all pattern detectors
    - Dynamic adjustments via context (VIX regime, thresholds, etc.)
    - Fallback support for legacy calculations
    - Centralized confidence, position sizing, and risk calculations

Usage:
    ```python
    from intraday_trading.patterns.pattern_mathematics import PatternMathematics
    from intraday_trading.intraday_scanner.math_dispatcher import MathDispatcher
    
    # Initialize with PatternMathematics and optional risk manager
    math_dispatcher = MathDispatcher(PatternMathematics, risk_manager)
    
    # Calculate confidence with dynamic context
    context = {
        "volume_ratio": 2.5,
        "price_change": 0.05,
        "vix_regime": "NORMAL",
        "volume_threshold": 1.5,  # Dynamic from orchestrator
        "base_confidence": 0.5
    }
    confidence = math_dispatcher.calculate_confidence("volume_spike", context)
    ```

Dynamic Adjustments:
    - VIX Regime Multiplier: HIGH_VIX (0.8x), NORMAL (1.0x), LOW_VIX (1.2x)
    - Dynamic Thresholds: Volume and price thresholds from orchestrator
    - Outlier Protection: Volume ratios capped at 10.0
    - Bounded Confidence: All values constrained to 0.0-0.95 range

See Also:
    - patterns/pattern_mathematics.py - Mathematical engine implementation
    - MATH_DISPATCHER_FLOW_TRACE.md - Complete flow documentation
"""
from __future__ import annotations

from typing import Any, Dict, Optional


class MathDispatcher:
    """
    Thin adapter that delegates to the configured math and risk engines.
    
    Provides a unified interface for pattern detectors to access mathematical
    calculations without hard-coding dependencies on PatternMathematics or risk
    calculators. Enables dynamic adjustments through context passing.
    
    ✅ ENHANCED: Now supports Redis client for VIX fetching from DB1
    
    Args:
        pattern_math: The mathematical engine (typically PatternMathematics class)
        risk_calculator: Optional risk calculator for position sizing and stop loss
            (can be None if not available)
        redis_client: Optional Redis client for VIX data fetching from DB1
    
    Attributes:
        pattern_math: Reference to the mathematical engine
        risk_calculator: Optional reference to risk calculator
        redis_client: Optional Redis client for VIX fetching
    
    Example:
        ```python
        # Initialize with PatternMathematics and Redis client
        dispatcher = MathDispatcher(PatternMathematics, None, redis_client=redis_client)
        
        # Calculate confidence with context (VIX fetched from Redis DB1 if not in context)
        context = {
            "volume_ratio": 2.0,
            "volume_threshold": 1.5
        }
        confidence = dispatcher.calculate_confidence("volume_spike", context)
        ```
    """

    def __init__(self, pattern_math: Any, risk_calculator: Any, redis_client: Any = None):
        """
        Initialize MathDispatcher with math engine, optional risk calculator, and Redis client.
        
        Args:
            pattern_math: Mathematical engine (typically PatternMathematics class)
            risk_calculator: Optional risk calculator for position sizing/stop loss
            redis_client: Optional Redis client for VIX fetching from DB1
        """
        self.pattern_math = pattern_math
        self.risk_calculator = risk_calculator
        self.redis_client = redis_client

    def calculate_position_size(self, pattern_type: str, context: Dict[str, Any]) -> Optional[float]:
        """
        Calculate position size based on risk per trade and stop loss distance.
        
        Routes to PatternMathematics for position sizing calculations, with fallback
        to risk calculator if available. Uses risk-based position sizing formula:
        position_size = (account_size * risk_per_trade) / abs(entry_price - stop_loss)
        
        Args:
            pattern_type: Type of pattern (e.g., "volume_spike", "breakout")
            context: Dictionary containing:
                - entry_price (float): Entry price for the trade
                - stop_loss (float): Stop loss price
                - account_size (float): Total account size (default: 1,000,000)
                - risk_per_trade (float): Risk percentage per trade (default: 0.01)
                - confidence (float): Pattern confidence (optional)
        
        Returns:
            Position size (number of shares/units) or None if calculation fails
        
        Example:
            ```python
            context = {
                "entry_price": 100.0,
                "stop_loss": 95.0,
                "account_size": 1_000_000,
                "risk_per_trade": 0.01
            }
            size = dispatcher.calculate_position_size("volume_spike", context)
            ```
        """
        if self.pattern_math and hasattr(self.pattern_math, "calculate_position_size"):
            return self.pattern_math.calculate_position_size(pattern_type, context)

        if self.risk_calculator and hasattr(self.risk_calculator, "calculate_position_size"):
            return self.risk_calculator.calculate_position_size(pattern_type, context)

        return None

    def calculate_confidence(self, pattern_type: str, context: Dict[str, Any]) -> Optional[float]:
        """
        Calculate pattern confidence score with dynamic adjustments.
        
        Routes to PatternMathematics for confidence calculations. Applies VIX regime
        adjustments, dynamic thresholds, and pattern-specific confidence weights.
        All confidence values are bounded to 0.0-0.95 range.
        
        Args:
            pattern_type: Type of pattern (e.g., "volume_spike", "breakout", "reversal")
            context: Dictionary containing:
                - volume_ratio (float): Current volume ratio vs baseline
                - price_change (float): Price change percentage
                - vix_regime (str): VIX regime ("HIGH_VIX", "NORMAL", "LOW_VIX")
                - volume_threshold (float): Dynamic volume threshold from orchestrator
                - price_threshold (float): Dynamic price threshold from orchestrator
                - base_confidence (float): Base confidence (default: 0.5)
                - Additional pattern-specific context fields
        
        Returns:
            Confidence score (0.0-0.95) or None if calculation fails
        
        Dynamic Adjustments:
            - VIX Regime: HIGH_VIX (0.8x), NORMAL (1.0x), LOW_VIX (1.2x)
            - Volume Confidence: Based on volume_ratio vs dynamic threshold
            - Price Confidence: Based on price_change vs dynamic threshold
            - Pattern Weights: Pattern-specific combination of volume/price confidence
        
        Example:
            ```python
            context = {
                "volume_ratio": 2.5,
                "price_change": 0.05,
                "vix_regime": "NORMAL",
                "volume_threshold": 1.5,
                "price_threshold": 0.03,
                "base_confidence": 0.5
            }
            confidence = dispatcher.calculate_confidence("volume_spike", context)
            ```
        """
        if self.pattern_math and hasattr(self.pattern_math, "calculate_confidence"):
            # ✅ ENHANCED: Pass redis_client for VIX fetching from DB1
            return self.pattern_math.calculate_confidence(pattern_type, context, redis_client=self.redis_client)

        return None

    def calculate_news_boost(self, instrument: str, news_data: Dict[str, Any]) -> Optional[float]:
        """
        Calculate news impact boost for pattern confidence.
        
        Routes to PatternMathematics for news impact calculations. Applies conservative
        boosts based on news impact level, sentiment, and volume triggers. All boosts
        are capped to prevent over-confidence.
        
        Args:
            instrument: Trading symbol (e.g., "NSE:RELIANCE")
            news_data: Dictionary containing:
                - impact (str): News impact level ("HIGH", "MEDIUM", "LOW")
                - sentiment (str/float): Sentiment ("positive", "negative") or score (-1.0 to 1.0)
                - volume_trigger (bool): Whether news triggered volume spike
                - market_impact (str): Alternative field for impact level
        
        Returns:
            News boost value (-0.05 to 0.10) or None if calculation fails
        
        Boost Values:
            - HIGH impact: +0.06
            - MEDIUM impact: +0.03
            - Volume trigger: +0.03
            - Positive sentiment: +0.02
            - Negative sentiment: -0.02
            - Maximum total boost: +0.10
            - Minimum total boost: -0.05
        
        Example:
            ```python
            news_data = {
                "impact": "HIGH",
                "sentiment": "positive",
                "volume_trigger": True
            }
            boost = dispatcher.calculate_news_boost("NSE:RELIANCE", news_data)
            # Returns: 0.11 (capped at 0.10)
            ```
        """
        if self.pattern_math and hasattr(self.pattern_math, "calculate_news_impact"):
            return self.pattern_math.calculate_news_impact(instrument, news_data)

        return None

    def calculate_stop_loss(self, pattern_type: str, context: Dict[str, Any]) -> Optional[float]:
        """
        Calculate stop loss price based on ATR and pattern type.
        
        Routes to PatternMathematics for stop loss calculations. Uses pattern-specific
        ATR multipliers and confidence adjustments. Stop loss is capped at 5% of entry price.
        
        Args:
            pattern_type: Type of pattern (e.g., "volume_spike", "breakout")
            context: Dictionary containing:
                - entry_price (float): Entry price for the trade
                - atr (float): Average True Range (default: entry_price * 0.02)
                - confidence (float): Pattern confidence (affects stop distance)
                - direction (str): Trade direction ("BUY", "SELL", "SHORT")
                - signal (str): Alternative field for direction
        
        Returns:
            Stop loss price or None if calculation fails
        
        ATR Multipliers by Pattern:
            - volume_spike: 1.5x ATR
            - volume_breakout: 1.0x ATR
            - breakout/reversal: 0.8x ATR
            - ICT patterns: 1.0-1.3x ATR
            - Straddle strategies: 1.5-2.0x ATR
        
        Example:
            ```python
            context = {
                "entry_price": 100.0,
                "atr": 2.0,
                "confidence": 0.75,
                "direction": "BUY"
            }
            stop_loss = dispatcher.calculate_stop_loss("volume_spike", context)
            ```
        """
        if self.pattern_math and hasattr(self.pattern_math, "calculate_stop_loss"):
            return self.pattern_math.calculate_stop_loss(pattern_type, context)

        if self.risk_calculator and hasattr(self.risk_calculator, "calculate_stop_loss"):
            return self.risk_calculator.calculate_stop_loss(pattern_type, context)

        return None

    def calculate_price_targets(self, pattern_type: str, context: Dict[str, Any]) -> Optional[Dict[str, float]]:
        """
        Calculate entry, target, and stop loss prices for a pattern.
        
        Routes to PatternMathematics for price target calculations. Derives entry price,
        target price (2x ATR from entry), and stop loss based on pattern type and direction.
        
        Args:
            pattern_type: Type of pattern (e.g., "volume_spike", "breakout")
            context: Dictionary containing:
                - last_price (float): Current/last price
                - entry_price (float): Entry price (optional, defaults to last_price * 1.002 for BUY)
                - atr (float): Average True Range (default: last_price * 0.02)
                - direction (str): Trade direction ("BUY", "SELL", "SHORT")
                - Additional fields for stop loss calculation
        
        Returns:
            Dictionary with keys:
                - entry_price (float): Recommended entry price
                - target_price (float): Target price (2x ATR from entry)
                - stop_loss (float): Stop loss price
            Or None if calculation fails
        
        Price Calculation:
            - BUY: entry = last_price * 1.002, target = entry + (2 * ATR)
            - SELL/SHORT: entry = last_price * 0.998, target = entry - (2 * ATR)
            - Stop loss calculated via calculate_stop_loss()
        
        Example:
            ```python
            context = {
                "last_price": 100.0,
                "atr": 2.0,
                "direction": "BUY"
            }
            targets = dispatcher.calculate_price_targets("volume_spike", context)
            # Returns: {"entry_price": 100.2, "target_price": 104.2, "stop_loss": 97.0}
            ```
        """
        if self.pattern_math and hasattr(self.pattern_math, "calculate_price_targets"):
            # ✅ ENHANCED: Pass redis_client for VIX fetching from DB1 (if needed in future)
            return self.pattern_math.calculate_price_targets(pattern_type, context)

        return None
