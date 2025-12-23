"""
Pattern Schema and Utilities
============================

Pattern data structures and validation functions.
Consolidated from existing pattern schema definitions.

Functions:
- create_pattern: Create pattern data structure
- validate_pattern: Validate pattern data
- should_send_alert: Determine if alert should be sent
- calculate_risk_reward: Calculate risk/reward ratio

Created: October 9, 2025
"""

from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Import pattern schema from consolidated schemas
try:
    from config.schemas import PATTERN_SCHEMA
    PATTERN_SCHEMA_AVAILABLE = True
except ImportError:
    PATTERN_SCHEMA_AVAILABLE = False
    PATTERN_SCHEMA = {}

# Import pattern registry configuration
try:
    import json
    from pathlib import Path
    pattern_config_path = Path(__file__).parent.parent / "data" / "pattern_registry_config.json"
    if pattern_config_path.exists():
        with open(pattern_config_path, 'r') as f:
            PATTERN_REGISTRY_CONFIG = json.load(f)
        PATTERN_CONFIG_AVAILABLE = True
    else:
        PATTERN_REGISTRY_CONFIG = {}
        PATTERN_CONFIG_AVAILABLE = False
except Exception:
    PATTERN_REGISTRY_CONFIG = {}
    PATTERN_CONFIG_AVAILABLE = False

# Pattern categories from registry
PATTERN_CATEGORIES = PATTERN_REGISTRY_CONFIG.get("categories", {}) if PATTERN_CONFIG_AVAILABLE else {}


def create_pattern(
    symbol: str,
    pattern_type: str,
    signal: str,
    timestamp: Optional[float] = None,
    timestamp_ms: Optional[float] = None,
    confidence: float = 0.5,
    last_price: float = 0.0,
    price_change: float = 0.0,
    volume: float = 0.0,
    volume_ratio: float = 1.0,
    vix_level: float = 0.0,
    market_regime: str = "NORMAL",
    pattern_category: str = "UNKNOWN",
    expected_move: float = 0.0,
    target_price: float = 0.0,
    stop_loss: float = 0.0,
    risk_reward: float = 0.0,
    action: Optional[str] = None,  # ✅ Added: Actionable trading action
    position_size: Optional[int] = None,  # ✅ Added: Calculated position size
    **kwargs
) -> Dict[str, Any]:
    """
    Create a pattern data structure following the unified schema.
    
    Args:
        symbol: Trading symbol
        pattern_type: Type of pattern detected
        signal: Trading signal (BUY/SELL/NEUTRAL)
        timestamp: Unix timestamp
        timestamp_ms: Unix timestamp in milliseconds
        confidence: Pattern confidence (0.0-1.0)
        last_price: Current price
        price_change: Price change amount
        volume: Current volume
        volume_ratio: Volume ratio vs average
        vix_level: VIX level
        market_regime: Market regime (NORMAL/COMPLACENT/PANIC)
        pattern_category: Pattern category
        expected_move: Expected price move
        target_price: Target price
        stop_loss: Stop loss price
        risk_reward: Risk/reward ratio
        **kwargs: Additional pattern-specific fields
        
    Returns:
        Pattern data dictionary
    """
    # Use current time if not provided
    if timestamp is None:
        timestamp = datetime.now().timestamp()
    if timestamp_ms is None:
        timestamp_ms = timestamp * 1000
    
    # Create base pattern structure
    pattern = {
        "symbol": symbol,
        "pattern": pattern_type,  # For backward compatibility
        "pattern_type": pattern_type,
        "timestamp": timestamp,
        "timestamp_ms": timestamp_ms,
        "signal": signal,
        "confidence": confidence,
        "last_price": last_price,
        "price_change": price_change,
        "volume": volume,
        "volume_ratio": volume_ratio,
        "vix_level": vix_level,
        "market_regime": market_regime,
        "pattern_category": pattern_category,
        "expected_move": expected_move,
        "target_price": target_price,
        "stop_loss": stop_loss,
        "risk_reward": risk_reward,
    }
    
    # ✅ Added: Include action and position_size if provided (for consistency with ICT patterns)
    if action is not None:
        pattern["action"] = action
    if position_size is not None:
        pattern["position_size"] = position_size
    
    # Add any additional fields
    pattern.update(kwargs)
    
    return pattern


def validate_pattern(pattern: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate pattern data against schema.
    
    Args:
        pattern: Pattern data dictionary
        
    Returns:
        Tuple of (is_valid, issues_list)
    """
    issues = []
    
    # Required fields
    required_fields = ["symbol", "pattern_type", "signal", "timestamp"]
    for field in required_fields:
        if field not in pattern:
            issues.append(f"Missing required field: {field}")
    
    # Validate signal values
    if "signal" in pattern:
        valid_signals = ["BUY", "SELL", "NEUTRAL", "BULLISH", "BEARISH", "LONG", "SHORT", "FLAT"]
        if pattern["signal"] not in valid_signals:
            issues.append(f"Invalid signal: {pattern['signal']}. Must be one of {valid_signals}")
    
    # Validate confidence range
    if "confidence" in pattern:
        confidence = pattern["confidence"]
        if not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
            issues.append(f"Invalid confidence: {confidence}. Must be between 0.0 and 1.0")
    
    # Validate numeric fields
    numeric_fields = ["last_price", "price_change", "volume", "volume_ratio", "vix_level"]
    for field in numeric_fields:
        if field in pattern:
            try:
                float(pattern[field])
            except (ValueError, TypeError):
                issues.append(f"Invalid numeric value for {field}: {pattern[field]}")
    
    # Validate timestamp
    if "timestamp" in pattern:
        try:
            float(pattern["timestamp"])
        except (ValueError, TypeError):
            issues.append(f"Invalid timestamp: {pattern['timestamp']}")
    
    return len(issues) == 0, issues


def should_send_alert(pattern: Dict[str, Any], min_confidence: float = 0.7) -> bool:
    """
    Determine if an alert should be sent for this pattern.
    
    Args:
        pattern: Pattern data dictionary
        min_confidence: Minimum confidence threshold
        
    Returns:
        True if alert should be sent
    """
    # Check confidence threshold
    confidence = pattern.get("confidence", 0.0)
    if confidence < min_confidence:
        return False
    
    # Check if pattern is enabled
    pattern_type = pattern.get("pattern_type", pattern.get("pattern", ""))
    if not is_pattern_enabled(pattern_type):
        return False
    
    # Check signal validity (include game theory signals: LONG/SHORT/FLAT)
    signal = pattern.get("signal", "")
    if signal not in ["BUY", "SELL", "BULLISH", "BEARISH", "LONG", "SHORT", "FLAT"]:
        return False
    
    # ✅ CRITICAL FIX: Only check volume threshold for volume-based patterns
    # Non-volume patterns (momentum, breakout, reversal) don't need volume!
    volume_ratio = pattern.get("volume_ratio", 1.0)
    
    # ✅ FIXED: Use shared_core.config_utils.thresholds (single source of truth)
    try:
        from shared_core.config_utils.thresholds import is_volume_dependent_pattern
        is_volume_dependent = is_volume_dependent_pattern(pattern_type)
    except ImportError:
        # ✅ FALLBACK: Only the 8 core patterns - volume-dependent ones from thresholds.py
        # From thresholds.py VOLUME_DEPENDENT_PATTERNS: kow_signal_straddle, ict_iron_condor, order_flow_breakout
        volume_dependent_patterns = [
            'kow_signal_straddle',      # VWAP-based but needs volume validation
            'ict_iron_condor',          # Needs volume validation
            'order_flow_breakout',      # Needs volume validation
            # Note: Other 5 patterns (gamma_exposure_reversal, microstructure_divergence,
            # cross_asset_arbitrage, vix_momentum, game_theory_signal) are NOT volume-dependent
        ]
        is_volume_dependent = pattern_type in volume_dependent_patterns
    
    if is_volume_dependent:
        # ✅ STANDARD VOLUME VALIDATION: Only for the 3 volume-dependent patterns
        # (kow_signal_straddle, ict_iron_condor, order_flow_breakout)
        # Use appropriate threshold from thresholds.py
        try:
            from shared_core.config_utils.thresholds import get_pattern_volume_requirement
            volume_threshold = get_pattern_volume_requirement(pattern_type, redis_client=None)
            if volume_threshold <= 0:
                # If threshold is 0, pattern doesn't need volume validation
                return True
        except Exception:
            # Fallback: Use default threshold from _BASIC_VOLUME_REQUIREMENTS
            volume_threshold = 1.5  # Default fallback
        
        if volume_ratio < volume_threshold:
            logger.debug(f"🔍 [ALERT_FILTER] {pattern.get('symbol', 'UNKNOWN')} {pattern_type} - BLOCKED: volume_ratio={volume_ratio:.2f} < threshold={volume_threshold:.2f}")
            return False
    # else: Non-volume patterns don't care about volume - always allow!
    
    return True


def calculate_risk_reward(
    entry_price: float,
    target_price: float,
    stop_loss: float,
    signal: str
) -> float:
    """
    Calculate risk/reward ratio for a pattern.
    
    Args:
        entry_price: Entry price
        target_price: Target price
        stop_loss: Stop loss price
        signal: Trading signal (BUY/SELL)
        
    Returns:
        Risk/reward ratio
    """
    if signal in ["BUY", "BULLISH"]:
        # Long position
        profit = target_price - entry_price
        risk = entry_price - stop_loss
    elif signal in ["SELL", "BEARISH"]:
        # Short position
        profit = entry_price - target_price
        risk = stop_loss - entry_price
    else:
        return 0.0
    
    if risk <= 0:
        return 0.0
    
    return profit / risk


def get_pattern_categories() -> Dict[str, Any]:
    """Get pattern categories from registry configuration"""
    return PATTERN_CATEGORIES


def get_pattern_category(pattern_name: str) -> str:
    """Get pattern category for a given pattern name"""
    for category, config in PATTERN_CATEGORIES.items():
        if "patterns" in config and pattern_name in config["patterns"]:
            return category
    return "UNKNOWN"


def is_pattern_enabled(pattern_name: str) -> bool:
    """Check if pattern is enabled in registry"""
    category = get_pattern_category(pattern_name)
    if category == "UNKNOWN" or category not in PATTERN_CATEGORIES:
        return False
    return PATTERN_CATEGORIES[category].get("enabled", False)


def get_pattern_schema() -> Dict[str, Any]:
    """Get pattern schema from consolidated schemas"""
    if PATTERN_SCHEMA_AVAILABLE:
        return PATTERN_SCHEMA
    return {}


# Legacy function names for backward compatibility
def create_pattern_data(*args, **kwargs):
    """Legacy function name for create_pattern"""
    return create_pattern(*args, **kwargs)


def validate_pattern_data(*args, **kwargs):
    """Legacy function name for validate_pattern"""
    return validate_pattern(*args, **kwargs)


# Wrapper functions for ICT pattern compatibility
def create_pattern_with_indicators(pattern_data: Dict[str, Any], indicators: Dict[str, Any]) -> Dict[str, Any]:
    """
    ✅ FIXED: Create pattern with ICT-compatible interface using unified create_pattern().
    
    This function now calls the modern create_pattern() to ensure consistency across all patterns.
    Legacy ICT-specific fields (action, position_size) are now part of the standard pattern structure.
    
    Args:
        pattern_data: Pattern data dictionary
        indicators: Market indicators dictionary
        
    Returns:
        Standardized pattern dictionary with consistent field structure
    """
    try:
        # Extract required fields with defaults
        symbol = pattern_data.get('symbol', indicators.get('symbol', 'UNKNOWN'))
        pattern_type = pattern_data.get('pattern', pattern_data.get('pattern_type', 'unknown'))
        signal = pattern_data.get('signal', 'NEUTRAL')
        confidence = pattern_data.get('confidence', 0.5)
        last_price = pattern_data.get('last_price', indicators.get('last_price', 0.0))
        
        # ✅ FIXED: Use modern create_pattern() for consistency
        pattern = create_pattern(
            symbol=symbol,
            pattern_type=pattern_type,
            signal=signal,
            timestamp=pattern_data.get('timestamp', indicators.get('timestamp')),
            timestamp_ms=pattern_data.get('timestamp_ms', indicators.get('timestamp_ms')),
            confidence=confidence,
            last_price=last_price,
            price_change=pattern_data.get('price_change', indicators.get('price_change', 0.0)),
            volume=pattern_data.get('volume', indicators.get('volume', 0.0)),
            volume_ratio=pattern_data.get('volume_ratio', indicators.get('volume_ratio', 1.0)),
            vix_level=pattern_data.get('vix_level', indicators.get('vix_level', 0.0)),
            market_regime=pattern_data.get('market_regime', indicators.get('market_regime', 'NORMAL')),
            pattern_category=pattern_data.get('pattern_category', pattern_data.get('pattern_type', 'UNKNOWN')),
            expected_move=pattern_data.get('expected_move', 1.0),
            target_price=pattern_data.get('target_price', 0.0),
            stop_loss=pattern_data.get('stop_loss', 0.0),
            risk_reward=pattern_data.get('risk_reward', 0.0),
            action=pattern_data.get('action', 'WATCH'),  # ✅ ICT-specific: Actionable trading action
            position_size=pattern_data.get('position_size', 1),  # ✅ ICT-specific: Calculated position size
            description=pattern_data.get('description', f'{pattern_type} pattern detected'),
            metadata=pattern_data  # Preserve original data for reference
        )
        
        # Add straddle-specific fields if present (ICT-specific extensions)
        if 'straddle' in pattern_type.lower():
            pattern.update({
                'strike': pattern_data.get('strike', 0),
                'ce_symbol': pattern_data.get('ce_symbol', ''),
                'pe_symbol': pattern_data.get('pe_symbol', ''),
                'combined_premium': pattern_data.get('combined_premium', 0.0),
                'vwap': pattern_data.get('vwap', 0.0),
                'underlying': pattern_data.get('underlying', 'NIFTY'),
                'expiry': pattern_data.get('expiry', ''),
                'entry_premium': pattern_data.get('entry_premium', 0.0),
                'strategy_type': 'straddle'
            })
        
        return pattern
        
    except Exception as e:
        logger.error(f"Error creating pattern: {e}")
        # Return minimal valid pattern using create_pattern()
        return create_pattern(
            symbol='UNKNOWN',
            pattern_type='unknown',
            signal='NEUTRAL',
            confidence=0.0,
            last_price=0.0,
            action='WATCH',
            description='Pattern creation failed'
        )


def validate_pattern_structure(pattern: Dict[str, Any]) -> bool:
    """
    Validate pattern data structure
    
    Args:
        pattern: Pattern dictionary to validate
        
    Returns:
        True if pattern is valid, False otherwise
    """
    try:
        # Check required fields
        required_fields = ['symbol', 'pattern', 'signal', 'confidence']
        for field in required_fields:
            if field not in pattern:
                logger.warning(f"Pattern missing required field: {field}")
                return False
        
        # Validate confidence range
        confidence = pattern.get('confidence', 0)
        if not isinstance(confidence, (int, float)) or confidence < 0 or confidence > 1:
            logger.warning(f"Invalid confidence value: {confidence}")
            return False
        
        # Validate signal values (include game theory signals)
        signal = pattern.get('signal', '')
        valid_signals = ['BUY', 'SELL', 'NEUTRAL', 'HOLD', 'LONG', 'SHORT', 'FLAT', 'BULLISH', 'BEARISH']
        if signal not in valid_signals:
            logger.warning(f"Invalid signal value: {signal}")
            return False
        
        # Validate action values
        action = pattern.get('action', '')
        valid_actions = ['BUY_MARKET', 'BUY_LIMIT', 'SELL_SHORT', 'SELL_LIMIT', 'EXIT_LONG', 'EXIT_SHORT', 'HEDGE_PUT', 'HEDGE_CALL', 'SELL_STRADDLE', 'SELL_STRANGLE', 'BUY_STRADDLE', 'WATCH']
        if action and action not in valid_actions:
            logger.warning(f"Invalid action value: {action}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating pattern: {e}")
        return False
