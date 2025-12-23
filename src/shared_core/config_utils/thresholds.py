# thresholds.py - FIXED VERSION
"""
Threshold Configuration Utilities - Shared Core
===============================================

Core threshold functions used by Redis clients, volume managers, and pattern detectors.
"""

import time
import logging
import json
from typing import Optional, Dict, Any, Tuple
from datetime import datetime
try:
    import redis
except ImportError:  # pragma: no cover
    redis = None

try:
    from shared_core.utils.vix_utils import get_vix_utils
except ImportError:  # pragma: no cover
    get_vix_utils = None

logger = logging.getLogger(__name__)

def classify_indian_vix_regime(vix_value: float) -> str:
    """Classify India VIX value into standard market regimes."""
    try:
        value = float(vix_value)
    except (TypeError, ValueError):
        return 'UNKNOWN'
    if value >= 25:
        return 'PANIC'
    if value >= 20:
        return 'HIGH'
    if value >= 15:
        return 'NORMAL'
    return 'LOW'

# ============================================================================
# 1. SINGLE VIX CACHE (180 seconds) - NO MORE MULTIPLE REDIS CALLS
# ============================================================================

class VIXCache:
    """Global VIX cache for entire system - 180-second TTL"""
    
    _cache = {
        'regime': 'NORMAL',
        'value': None,
        'timestamp': 0,
        'raw_data': None
    }
    
    _ttl_seconds = 180  # Cache for 3 minutes
    
    @staticmethod
    def get_vix_regime(redis_client=None) -> str:
        """Get VIX regime from cache or fetch once every 180 seconds"""
        current_time = time.time()
        
        # Return cached if valid
        if (VIXCache._cache['timestamp'] > 0 and 
            (current_time - VIXCache._cache['timestamp']) < VIXCache._ttl_seconds and
            VIXCache._cache['regime'] != 'UNKNOWN'):
            return VIXCache._cache['regime']
        
        # Fetch fresh
        vix_data = VIXCache._fetch_vix_from_redis(redis_client)
        VIXCache._cache = {
            'regime': vix_data['regime'],
            'value': vix_data['value'],
            'timestamp': current_time,
            'raw_data': vix_data
        }
        return vix_data['regime']
    
    @staticmethod
    def get_vix_value(redis_client=None) -> Optional[float]:
        """Get VIX value from cache"""
        # Refresh cache if needed
        VIXCache.get_vix_regime(redis_client)
        return VIXCache._cache['value']
    
    @staticmethod
    def _fetch_vix_from_redis(redis_client=None) -> Dict[str, Any]:
        """Fetch VIX once - used by entire system"""
        try:
            if redis_client:
                vix_client = redis_client.get_client(1) if hasattr(redis_client, 'get_client') else redis_client
                
                if get_vix_utils:
                    try:
                        vix_snapshot = get_vix_utils(redis_client=vix_client).get_current_vix()
                        if vix_snapshot and vix_snapshot.get('value') is not None:
                            regime = vix_snapshot.get('regime') or 'NORMAL'
                            value = float(vix_snapshot.get('value'))
                            logger.info(f"✅ [VIX_CACHE] Fetched VIX via VIXUtils: {value:.2f} ({regime})")
                            return {'regime': regime, 'value': value}
                    except Exception as utils_exc:
                        logger.debug(f"⚠️ [VIX_CACHE] VIXUtils fetch failed: {utils_exc}")
                
                # Legacy fallback to handle environments where VIXUtils isn't available
                vix_keys_to_try = [
                    "index_hash:NSEINDIAVIX",
                    "index_hash:NSE:INDIA VIX",
                    "index:NSEINDIA_VIX",
                    "index:NSE:INDIA VIX",
                    "index:NSEINDIAVIX",
                    "index:NSEINDIA VIX",
                    "market_data:indices:nse_india_vix",
                    "market_data:indices:nse_india vix",
                    "market_data:indices:nseindia_vix",
                ]
                
                for vix_key in vix_keys_to_try:
                    try:
                        data: Optional[Dict[str, Any]] = None
                        if vix_key.startswith("index_hash:"):
                            payload = vix_client.hgetall(vix_key)
                            if not payload:
                                continue
                            decoded = {}
                            for k, v in payload.items():
                                key_str = k.decode('utf-8') if isinstance(k, (bytes, bytearray)) else str(k)
                                if isinstance(v, (bytes, bytearray)):
                                    try:
                                        v = v.decode('utf-8')
                                    except Exception:
                                        pass
                                decoded[key_str] = v
                            data = decoded
                        else:
                            raw_value = vix_client.get(vix_key)
                            if not raw_value:
                                continue
                            if isinstance(raw_value, (bytes, bytearray)):
                                try:
                                    raw_value = raw_value.decode('utf-8')
                                except Exception:
                                    pass
                            if isinstance(raw_value, str):
                                try:
                                    data = json.loads(raw_value)
                                except json.JSONDecodeError:
                                    data = {'value': raw_value}
                            elif isinstance(raw_value, (int, float)):
                                data = {'value': float(raw_value)}
                            elif isinstance(raw_value, dict):
                                data = raw_value
                            else:
                                data = {'value': raw_value}
                        
                        if not data:
                            continue
                        last_price = (
                            data.get('last_price')
                            or data.get('close')
                            or data.get('value')
                            or data.get('ltp')
                        )
                        if last_price is None:
                            continue
                        try:
                            vix_value = float(last_price)
                        except (TypeError, ValueError):
                            continue
                        
                        regime = classify_indian_vix_regime(vix_value)
                        
                        logger.info(f"✅ [VIX_CACHE] Fetched VIX: {vix_value:.2f} ({regime}) from key: {vix_key}")
                        return {'regime': regime, 'value': vix_value}
                    except Exception as e:
                        logger.debug(f"⚠️ [VIX_CACHE] Error reading key {vix_key}: {e}")
                        continue
                    except Exception as e:
                        logger.debug(f"⚠️ [VIX_CACHE] Error reading key {vix_key}: {e}")
                        continue
        except Exception as e:
            logger.warning(f"⚠️ [VIX_CACHE] VIX fetch failed: {e}")
        
        return {'regime': 'NORMAL', 'value': None}

# ============================================================================
# 1.5. COMPATIBILITY WRAPPER - get_safe_vix_regime()
# ============================================================================

def get_safe_vix_regime(redis_client=None) -> str:
    """
    ✅ COMPATIBILITY: Wrapper for VIXCache.get_vix_regime()
    
    This function is used throughout the codebase for VIX regime access.
    It delegates to VIXCache which provides 180-second caching.
    
    Args:
        redis_client: Optional Redis client
        
    Returns:
        VIX regime string: 'HIGH', 'NORMAL', 'LOW', 'PANIC', or 'UNKNOWN'
    """
    return VIXCache.get_vix_regime(redis_client=redis_client)

# ============================================================================
# 2. INSTRUMENT CONFIGURATION - YOUR FOCUS INSTRUMENTS
# ============================================================================

INSTRUMENT_CONFIGS = {
    # NIFTY Contracts (Tuesday expiry)
    'NIFTY_WEEKLY': {
        'lot_size': 75,
        'multiplier': 50,
        'risk_per_trade': 0.01,  # 1%
        'min_move_threshold': 0.3,  # 0.3%
        'category': 'index_futures',
        'strike_selection': 'atm',
        'expiry_preference': 'weekly'
    },
    'NIFTY_MONTHLY': {
        'lot_size': 75,
        'multiplier': 50,
        'risk_per_trade': 0.015,  # 1.5%
        'min_move_threshold': 0.4,
        'category': 'index_futures',
        'strike_selection': 'atm',
        'expiry_preference': 'monthly'
    },
    # BANKNIFTY Contracts
    'BANKNIFTY_WEEKLY': {
        'lot_size': 25,
        'multiplier': 25,
        'risk_per_trade': 0.01,
        'min_move_threshold': 0.4,
        'category': 'index_futures',
        'strike_selection': 'atm',
        'expiry_preference': 'weekly'
    },
    'BANKNIFTY_MONTHLY': {
        'lot_size': 25,
        'multiplier': 25,
        'risk_per_trade': 0.015,
        'min_move_threshold': 0.5,
        'category': 'index_futures',
        'strike_selection': 'atm',
        'expiry_preference': 'monthly'
    },
    # Commodities
    'GOLD': {
        'lot_size': 1,
        'multiplier': 100,
        'risk_per_trade': 0.02,
        'min_move_threshold': 0.2,
        'category': 'commodity',
        'strike_selection': 'atm',
        'expiry_preference': 'monthly'
    },
    'SILVER': {
        'lot_size': 30,
        'multiplier': 5,
        'risk_per_trade': 0.02,
        'min_move_threshold': 0.25,
        'category': 'commodity',
        'strike_selection': 'atm',
        'expiry_preference': 'monthly'
    },
    # Currency
    'USDINR': {
        'lot_size': 1000,
        'multiplier': 1,
        'risk_per_trade': 0.01,
        'min_move_threshold': 0.1,
        'category': 'currency',
        'strike_selection': 'atm',
        'expiry_preference': 'monthly'
    }
}

def _get_asset_multiplier(symbol: str) -> float:
    """
    ✅ CRITICAL: Get contract multiplier (lot size × tick value) from asset-based logic.
    
    This matches the logic in intraday_websocket_parser.py LotSizeManager.get_multiplier()
    to ensure consistent multiplier calculations across the system.
    
    Args:
        symbol: Trading symbol (e.g., 'NIFTY', 'BANKNIFTY', 'GOLDM')
        
    Returns:
        Contract multiplier (lot_size × tick_value)
    """
    symbol_upper = symbol.upper()
    
    # Current lot sizes (will change in Jan 2026)
    current_lot_sizes = {
        "NIFTY": 75,
        "BANKNIFTY": 35,
        "FINNIFTY": 40,
        "MIDCPNIFTY": 75,
        "GOLDM": 1,
        "SILVERM": 5,
        "USDINR": 1000,
        "CRUDEOIL": 100,
    }
    
    # Future lot sizes (from Jan 2026)
    future_lot_sizes = {
        "NIFTY": 65,      # Reduced from 75
        "BANKNIFTY": 30,  # Reduced from 35
        "FINNIFTY": 40,   # No change
        "MIDCPNIFTY": 75, # No change
        "GOLDM": 1,       # No change
        "SILVERM": 5,     # No change
        "USDINR": 1000,   # No change
        "CRUDEOIL": 100,  # No change
    }
    
    # Cutoff date for lot size change
    cutoff_date = datetime(2026, 1, 1)
    current_date = datetime.now()
    lot_map = future_lot_sizes if current_date >= cutoff_date else current_lot_sizes
    
    # Get lot size
    lot_size = 1  # Default
    for pattern, size in lot_map.items():
        if pattern in symbol_upper:
            lot_size = size
            break
    
    # Tick value mappings
    tick_values = {
        "NIFTY": 0.05,
        "BANKNIFTY": 0.05,
        "FINNIFTY": 0.05,
        "MIDCPNIFTY": 0.05,
        "GOLDM": 1.0,
        "SILVERM": 1.0,
        "USDINR": 0.0025,
        "CRUDEOIL": 1.0,
    }
    
    # Get tick value
    tick_value = 1.0  # Default
    for pattern, value in tick_values.items():
        if pattern in symbol_upper:
            tick_value = value
            break
    
    return lot_size * tick_value

def get_instrument_config(symbol: str) -> Dict[str, Any]:
    """
    Get instrument config with asset-based multiplier calculation.
    
    ✅ CRITICAL: Uses dynamic multiplier calculation from intraday_websocket_parser.py
    to ensure consistency across the system.
    
    📋 INSTRUMENT POSITIONING:
    --------------------------
    This function provides instrument positioning data including lot size and multipliers.
    To get instrument positioning, import and use this function:
    
        from shared_core.config_utils.thresholds import get_instrument_config
        
        config = get_instrument_config(symbol='NIFTY')
        
        # Access positioning data:
        lot_size = config.get('lot_size')          # Lot size for the instrument
        multiplier = config.get('multiplier')      # Contract multiplier (lot_size * tick_value)
        risk_per_trade = config.get('risk_per_trade')  # Risk percentage per trade
        
        # Example usage in UnifiedAlertBuilder:
        # File: alerts/unified_alert_builder.py
        # Method: _get_instrument_config() uses this function internally
        # Method: _calculate_position_size() uses multiplier for position sizing
    
    📊 RETURNED CONFIG STRUCTURE:
    -----------------------------
    {
        'lot_size': int,              # Lot size (e.g., 75 for NIFTY, 35 for BANKNIFTY)
        'multiplier': float,          # Contract multiplier (lot_size * tick_value)
        'risk_per_trade': float,      # Risk percentage per trade (e.g., 0.01 = 1%)
        'min_move_threshold': float,  # Minimum price move threshold
        'category': str,              # Instrument category (e.g., 'index', 'equity', 'commodity')
        'strike_selection': str,      # Strike selection method (e.g., 'atm', 'otm')
        'expiry_preference': str      # Expiry preference (e.g., 'weekly', 'monthly')
    }
    
    🔗 INTEGRATION WITH UNIFIED ALERT BUILDER:
    ------------------------------------------
    The UnifiedAlertBuilder (alerts/unified_alert_builder.py) uses this
    function internally via its _get_instrument_config() method for position sizing:
    
        - Position sizing: Uses 'multiplier' and 'lot_size' to calculate position in lots
        - Risk management: Uses 'risk_per_trade' for risk-adjusted position sizing
        - VIX-aware adjustments: Combined with get_risk_adjusted_position_multiplier()
    
    📍 FULL PATH:
    ------------
    Function: shared_core.config_utils.thresholds.get_instrument_config()
    Used by: alerts/unified_alert_builder.py
    Related: get_risk_adjusted_position_multiplier() for VIX-aware position sizing
    """
    symbol_upper = symbol.upper()
    
    # Calculate multiplier using asset-based logic
    calculated_multiplier = _get_asset_multiplier(symbol)
    
    # Direct mapping for focus instruments
    for key, config in INSTRUMENT_CONFIGS.items():
        if key in symbol_upper:
            # Override multiplier with calculated value for consistency
            config_copy = config.copy()
            config_copy['multiplier'] = calculated_multiplier
            # Also update lot_size if it's different
            if 'NIFTY' in symbol_upper:
                config_copy['lot_size'] = 75 if datetime.now().year < 2026 else 65
            elif 'BANKNIFTY' in symbol_upper:
                config_copy['lot_size'] = 35 if datetime.now().year < 2026 else 30
            return config_copy
    
    # Fallback for other symbols
    if 'NIFTY' in symbol_upper:
        base_config = INSTRUMENT_CONFIGS['NIFTY_WEEKLY'].copy()
        base_config['lot_size'] = 75 if datetime.now().year < 2026 else 65
        base_config['multiplier'] = calculated_multiplier
        return base_config
    elif 'BANKNIFTY' in symbol_upper:
        base_config = INSTRUMENT_CONFIGS['BANKNIFTY_WEEKLY'].copy()
        base_config['lot_size'] = 35 if datetime.now().year < 2026 else 30
        base_config['multiplier'] = calculated_multiplier
        return base_config
    elif 'FINNIFTY' in symbol_upper:
        lot_size = 40  # No change in 2026
        base_config = {
            'lot_size': lot_size,
            'multiplier': calculated_multiplier,
            'risk_per_trade': 0.01,
            'min_move_threshold': 0.3,
            'category': 'index_futures',
            'strike_selection': 'atm',
            'expiry_preference': 'weekly'
        }
        return base_config
    elif 'GOLDM' in symbol_upper or 'GOLD' in symbol_upper:
        base_config = INSTRUMENT_CONFIGS.get('GOLD', {}).copy()
        base_config['multiplier'] = calculated_multiplier
        return base_config
    elif 'SILVERM' in symbol_upper or 'SILVER' in symbol_upper:
        base_config = INSTRUMENT_CONFIGS.get('SILVER', {}).copy()
        base_config['multiplier'] = calculated_multiplier
        return base_config
    elif 'USDINR' in symbol_upper:
        base_config = INSTRUMENT_CONFIGS.get('USDINR', {}).copy()
        base_config['multiplier'] = calculated_multiplier
        return base_config
    
    # Default for unknown
    return {
        'lot_size': 1,
        'multiplier': calculated_multiplier,
        'risk_per_trade': 0.01,
        'min_move_threshold': 0.5,
        'category': 'unknown',
        'strike_selection': 'atm',
        'expiry_preference': 'weekly'
    }

def get_greek_thresholds(symbol: str) -> Dict[str, Any]:
    """
    Get Greek thresholds (lot size and other config) for a symbol.
    
    ✅ COMPATIBILITY: Alias for get_instrument_config() for backward compatibility.
    Used by risk_manager.py and other components that expect 'get_greek_thresholds'.
    
    Args:
        symbol: Trading symbol (e.g., 'NIFTY', 'BANKNIFTY')
        
    Returns:
        Dict with 'lot_size', 'multiplier', and other instrument config fields
    """
    return get_instrument_config(symbol)

# ============================================================================
# 3. THRESHOLD CONFIGURATION (SIMPLE VERSION)
# ============================================================================

# Base thresholds for different VIX regimes
VIX_THRESHOLDS = {
    'PANIC': {  # VIX > 22
        'confidence_multiplier': 1.0,   # Same confidence requirement
        'move_multiplier': 1.5,        # Need 1.5x larger moves
        'position_size_multiplier': 0.5,  # 50% position size
        'stop_loss_multiplier': 2.0,   # Wider stops (2N)
    },
    'HIGH': {   # VIX 18-22
        'confidence_multiplier': 1.0,
        'move_multiplier': 1.2,
        'position_size_multiplier': 0.7,
        'stop_loss_multiplier': 1.5,   # Wider stops (1.5N)
    },
    'NORMAL': { # VIX 12-18
        'confidence_multiplier': 1.0,
        'move_multiplier': 1.0,
        'position_size_multiplier': 1.0,
        'stop_loss_multiplier': 1.0,   # Normal stops (1N)
    },
    'LOW': {    # VIX < 12
        'confidence_multiplier': 1.0,
        'move_multiplier': 0.8,
        'position_size_multiplier': 1.2,  # 20% larger positions
        'stop_loss_multiplier': 0.8,    # Tighter stops (0.8N)
    }
}

# ✅ COMPATIBILITY: BASE_THRESHOLDS alias for backward compatibility
# Used by alert_manager.py and other components
BASE_THRESHOLDS = VIX_THRESHOLDS

# ============================================================================
# 3.5. EDGE CALCULATION THRESHOLDS
# ============================================================================

# Edge calculation thresholds for pattern detection gating
EDGE_THRESHOLDS = {
    # Intent Layer (DHDI - Delta-Hedge Demand Index)
    'dhdi': {
        'hedge_threshold': 1.0,      # DHDI > 1.0 indicates hedging activity
        'spec_threshold': 0.3,       # DHDI < 0.3 indicates speculative activity
        'window_size': 20,           # Rolling window for hedge_pressure_ma
    },
    # Constraint Layer (LCI - Liquidity Commitment Index)
    'lci': {
        'poor_threshold': 0.5,       # LCI < 0.5 = poor liquidity (gate patterns)
        'good_threshold': 0.8,       # LCI > 0.8 = good liquidity
        'tier_weights': {
            'hft': 0.1,              # HFT tier (lowest weight)
            'mm': 0.5,               # Market Maker tier
            'insti': 1.0,            # Institutional tier (highest weight)
        },
    },
    # Constraint Layer (Gamma Stress)
    'gamma_stress': {
        'high_threshold': 0.7,       # Gamma stress > 0.7 = high stress environment
        'critical_threshold': 0.9,   # Gamma stress > 0.9 = critical (avoid trading)
    },
    # Transition Layer
    'transition': {
        'high_probability': 0.7,     # Transition prob > 0.7 = likely regime change
        'latency_collapse_threshold': 0.5,  # Latency < 50% avg = collapse signal
    },
    # Regime Layer (Gating)
    'regime_gating': {
        'min_confidence': 0.7,       # Minimum confidence to trade
        'avoid_regimes': [4],        # Regime IDs to avoid (e.g., 4 = liquidity stress)
        'reduce_size_regimes': [2],  # Regime IDs to reduce position size (e.g., 2 = pin risk)
        'size_reduction_factor': 0.5,  # Reduce position by 50% in reduce_size_regimes
    },
}


def get_edge_thresholds(category: str = None) -> Dict[str, Any]:
    """
    Get edge thresholds for pattern detection gating.
    
    Args:
        category: Optional category ('dhdi', 'lci', 'gamma_stress', 'transition', 'regime_gating')
                 If None, returns all thresholds.
    
    Returns:
        Threshold config dict
    
    Usage:
        >>> lci_thresholds = get_edge_thresholds('lci')
        >>> if lci < lci_thresholds['poor_threshold']:
        ...     return []  # Gate patterns due to poor liquidity
    """
    if category:
        return EDGE_THRESHOLDS.get(category, {})
    return EDGE_THRESHOLDS

def get_thresholds_for_regime(vix_regime: str = None, redis_client=None) -> Dict[str, Any]:
    """Get all thresholds for current VIX regime"""
    if vix_regime is None:
        vix_regime = VIXCache.get_vix_regime(redis_client)
    
    base_thresholds = VIX_THRESHOLDS.get(vix_regime, VIX_THRESHOLDS['NORMAL'])
    
    # Add current VIX value
    vix_value = VIXCache.get_vix_value(redis_client)
    base_thresholds['vix_value'] = vix_value
    base_thresholds['vix_regime'] = vix_regime
    
    return base_thresholds

def get_move_threshold(alert_type: str = None, vix_regime: str = None, redis_client=None) -> float:
    """
    Get move threshold (price movement requirement) for alert type based on VIX regime.
    
    ✅ COMPATIBILITY: Used by alert_manager.py for move-based validation.
    
    Args:
        alert_type: Optional alert type (e.g., 'volume_spike', 'breakout_confirmation')
        vix_regime: Optional VIX regime (PANIC, HIGH, NORMAL, LOW). If None, fetches from cache.
        redis_client: Optional Redis client for VIX fetching
    
    Returns:
        Move threshold multiplier (0.8 to 1.5) based on VIX regime
    
    Move multipliers by VIX Regime:
        - PANIC (VIX > 22): 1.5x (need 1.5x larger moves)
        - HIGH (VIX 18-22): 1.2x (need 1.2x larger moves)
        - NORMAL (VIX 12-18): 1.0x (normal moves)
        - LOW (VIX < 12): 0.8x (smaller moves acceptable)
    """
    try:
        # Get VIX regime if not provided
        if vix_regime is None:
            vix_regime = VIXCache.get_vix_regime(redis_client)
        
        # Get move_multiplier from VIX_THRESHOLDS
        thresholds = VIX_THRESHOLDS.get(vix_regime, VIX_THRESHOLDS['NORMAL'])
        move_multiplier = thresholds.get('move_multiplier', 1.0)
        
        logger.debug(f"✅ [MOVE_THRESHOLD] {alert_type or 'default'} in {vix_regime} regime: {move_multiplier:.2f}x")
        return move_multiplier
    except Exception as e:
        logger.warning(f"⚠️ [MOVE_THRESHOLD] Error getting threshold for {alert_type}: {e}")
        return 1.0  # Default to 1.0x (no adjustment)

# ============================================================================
# 4. UNIFIED ALERT BUILDER (IMPORT FROM CORRECT LOCATION)
# ============================================================================

"""
✅ NOTE: UnifiedAlertBuilder has been moved to alerts/unified_alert_builder.py

This is the SINGLE SOURCE OF TRUTH for all 8 patterns:
- order_flow_breakout, gamma_exposure_reversal, microstructure_divergence
- cross_asset_arbitrage, vix_momentum, game_theory_signal
- kow_signal_straddle, ict_iron_condor

To use UnifiedAlertBuilder, import from the correct location:

    from alerts.unified_alert_builder import get_unified_alert_builder
    
    builder = get_unified_alert_builder(redis_client=redis_client)
    alert = builder.build_algo_alert(
        symbol=symbol,
        pattern_type='order_flow_breakout',
        entry_price=price,
        confidence=0.85,
        signal='BUY',
        indicators=indicators,
        greeks=greeks,
        atr=atr_value,
        expected_move=1.0,
        volume_ratio=2.5,
    )

For straddle patterns, use:
    alert = builder.build_straddle_algo_payload(...)

For iron condor patterns, use:
    alert = builder.build_iron_condor_algo_payload(...)
"""


# ============================================================================
# 4.5 EXPECTED MOVE CALCULATOR
# ============================================================================


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class ExpectedMoveCalculator:
    """
    Lightweight expected move calculator that uses instrument configs and risk
    settings to derive percent move ranges for symbols/timeframes.

    The returned payload matches the format expected by PatternDetector._augment_with_expected_move.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.base_expected_move_pct = _safe_float(self.config.get("base_expected_move_pct", 0.85), 0.85)
        self.min_expected_move_pct = _safe_float(self.config.get("min_expected_move_pct", 0.25), 0.25)
        self.max_expected_move_pct = _safe_float(self.config.get("max_expected_move_pct", 5.0), 5.0)
        self.timeframe_multipliers = {
            "scalper": 0.4,
            "scalp": 0.45,
            "intraday": 0.65,
            "30min": 0.75,
            "hourly": 0.85,
            "daily": 1.0,
            "weekly": 1.35,
            "swing": 1.5,
        }
        self.category_multipliers = {
            "index_futures": 0.9,
            "index": 0.9,
            "strategy": 1.1,
            "commodity": 1.2,
            "currency": 0.35,
            "equity": 1.0,
        }
        overrides = self.config.get("symbol_overrides") or {}
        self.symbol_overrides = {
            str(symbol).upper(): _safe_float(value, self.base_expected_move_pct)
            for symbol, value in overrides.items()
            if isinstance(value, (int, float, str))
        }
        self._last_result: Dict[str, Any] = {}

    def calculate_expected_move(self, symbol: str, timeframe: str = "daily") -> Dict[str, Any]:
        symbol_upper = (symbol or "UNKNOWN").upper()
        base_pct, method = self._derive_symbol_expected_move(symbol_upper)
        tf_multiplier = self.timeframe_multipliers.get(timeframe.lower(), 1.0)
        adjusted = base_pct * tf_multiplier
        expected_move_pct = max(self.min_expected_move_pct, min(adjusted, self.max_expected_move_pct))

        band = max(0.1, expected_move_pct * 0.35)
        lower_bound = max(self.min_expected_move_pct, expected_move_pct - band)
        upper_bound = min(self.max_expected_move_pct, expected_move_pct + band)
        confidence = self._estimate_confidence(expected_move_pct, timeframe)

        result = {
            "symbol": symbol_upper,
            "timeframe": timeframe,
            "expected_move": round(expected_move_pct, 3),
            "expected_move_percent": round(expected_move_pct, 3),
            "lower_bound": round(lower_bound, 3),
            "upper_bound": round(upper_bound, 3),
            "confidence": round(confidence, 3),
            "method": method,
            "timestamp": datetime.now().isoformat(),
        }
        self._last_result = result
        return result

    def get_health_metrics(self) -> Dict[str, Any]:
        return {
            "base_expected_move_pct": self.base_expected_move_pct,
            "min_expected_move_pct": self.min_expected_move_pct,
            "max_expected_move_pct": self.max_expected_move_pct,
            "last_calculation": self._last_result,
        }

    def _derive_symbol_expected_move(self, symbol_upper: str) -> Tuple[float, str]:
        if symbol_upper in self.symbol_overrides:
            return self.symbol_overrides[symbol_upper], "symbol_override"

        instrument_config: Dict[str, Any] = {}
        try:
            instrument_config = get_instrument_config(symbol_upper)
        except Exception as e:
            logger.debug(f"⚠️ [EXPECTED_MOVE] Could not load instrument config for {symbol_upper}: {e}")

        category = instrument_config.get("category", "equity")
        category_multiplier = self.category_multipliers.get(category, 1.0)
        min_move_threshold = _safe_float(instrument_config.get("min_move_threshold", 0.0), 0.0)

        base = self.base_expected_move_pct * category_multiplier
        if min_move_threshold > 0:
            base = max(base, min_move_threshold * 1.5)

        heuristics = [
            ("BANKNIFTY", 1.2),
            ("FINNIFTY", 0.95),
            ("MIDCPNIFTY", 1.1),
            ("NIFTY", 0.85),
            ("USDINR", 0.3),
            ("GOLD", 0.75),
            ("SILVER", 0.9),
        ]
        for pattern, override in heuristics:
            if pattern in symbol_upper:
                base = max(base, override)
                break

        return base, "instrument_config"

    def _estimate_confidence(self, expected_move_pct: float, timeframe: str) -> float:
        confidence = 0.55
        if expected_move_pct >= 1.5:
            confidence += 0.15
        elif expected_move_pct >= 1.0:
            confidence += 0.08
        elif expected_move_pct <= 0.4:
            confidence -= 0.1

        timeframe = timeframe.lower()
        timeframe_adjust = {
            "scalper": -0.15,
            "intraday": -0.05,
            "daily": 0.0,
            "weekly": 0.05,
            "swing": 0.08,
        }
        confidence += timeframe_adjust.get(timeframe, 0.0)
        return max(0.35, min(0.9, confidence))


def create_expected_move_calculator(risk_config: Optional[Dict[str, Any]] = None) -> ExpectedMoveCalculator:
    """Factory helper so higher layers can request a calculator without importing the class directly."""
    return ExpectedMoveCalculator(risk_config)

# ============================================================================
# 5. MISSING FUNCTIONS - Added for compatibility with existing codebase
# ============================================================================

def get_volume_baseline(symbol: str, redis_client=None) -> float:
    """Get volume baseline for a symbol (fallback implementation)"""
    # This is a placeholder - actual implementation should fetch from Redis
    # For now, return 0.0 to indicate baseline is not available
    return 0.0

def get_pattern_volume_requirement(
    pattern_name: str,
    symbol: str = None,
    vix_regime: str = None,
    redis_client=None,
    timestamp: str = None,
) -> float:
    """
    ✅ Centralized volume requirements for patterns that need volume validation.
    
    Patterns with volume requirements:
    - kow_signal_straddle: 1.2
    - ict_iron_condor: 1.5
    - vix_momentum: 2.0 (VIX regime-based momentum requires higher volume)
    - order_flow_breakout: 1.0 (lower threshold for options)
    - gamma_exposure_reversal: 0.5 (lower threshold for options)
    
    All other patterns return 0.0 (no volume validation needed).
    
    Note: VIX regime can adjust these thresholds (higher in HIGH/PANIC, lower in LOW).
    """
    pattern_lower = (pattern_name or "").lower()
    
    # Get VIX regime if not provided
    if vix_regime is None:
        vix_regime = VIXCache.get_vix_regime(redis_client)
    
    # Base volume requirements by pattern
    base_requirements = {
        "kow_signal_straddle": 1.2,
        "ict_iron_condor": 1.5,
        "vix_momentum": 2.0,  # ✅ ADDED: Centralized threshold for vix_momentum
        "order_flow_breakout": 1.0,
        "gamma_exposure_reversal": 0.5,
    }
    
    base_requirement = base_requirements.get(pattern_lower, 0.0)
    
    # Apply VIX regime adjustments (higher VIX = need more volume confirmation)
    if base_requirement > 0:
        vix_multipliers = {
            "PANIC": 1.3,   # 30% higher in panic
            "HIGH": 1.2,    # 20% higher in high VIX
            "NORMAL": 1.0,  # Base requirement
            "LOW": 0.9,     # 10% lower in low VIX
        }
        multiplier = vix_multipliers.get(vix_regime, 1.0)
        adjusted_requirement = base_requirement * multiplier
        
        logger.debug(f"✅ [VOLUME_REQUIREMENT] {pattern_name}: base={base_requirement:.2f}, vix_regime={vix_regime}, adjusted={adjusted_requirement:.2f}")
        return adjusted_requirement
    
    # All other patterns don't need volume validation
    return 0.0

def is_volume_dependent_pattern(pattern_name: str) -> bool:
    """Check if pattern requires volume validation"""
    pattern_lower = (pattern_name or "").lower()
    return pattern_lower in ["kow_signal_straddle", "ict_iron_condor"]

def get_kow_signal_strategy_config() -> Dict[str, Any]:
    """
    Get KOW Signal Straddle strategy configuration.
    
    ✅ COMPATIBILITY: Provides strategy-specific config for kow_signal_straddle pattern.
    Used by kow_signal_straddle.py for strategy initialization.
    
    Returns:
        Dict with strategy configuration including entry_time, exit_time, confirmation_periods, etc.
    """
    return {
        'entry_time': '09:30',
        'exit_time': '15:15',
        'confirmation_periods': 1,
        'max_reentries': 3,
        'underlying_symbols': ['NIFTY', 'BANKNIFTY'],
        'strike_selection': 'atm',
        'max_risk_per_trade': 0.03,
        'confidence_threshold': 0.85,
        'telegram_routing': True,
        'premium_decay_target': 0.30,  # 30% premium decay target
        'stop_loss_per_lot': -1000,  # ₹1000 stop loss per lot
        'strategy_type': 'premium_collection_straddle',
        'expiry_preference': 'weekly'
    }

# ============================================================================
# 6. RISK-ADJUSTED POSITION MULTIPLIER
# ============================================================================

def get_risk_adjusted_position_multiplier(pattern_type: str, redis_client=None) -> float:
    """
    Get VIX-aware position size multiplier for a pattern type.
    
    ✅ Uses VIX_THRESHOLDS to adjust position size based on current VIX regime.
    This is used by UnifiedAlertBuilder to apply VIX-aware position sizing.
    
    Args:
        pattern_type: Pattern type (e.g., 'order_flow_breakout', 'kow_signal_straddle')
        redis_client: Optional Redis client for VIX fetching
    
    Returns:
        Position size multiplier (0.5 to 1.2) based on VIX regime
    
    Multipliers by VIX Regime:
        - PANIC (VIX > 22): 0.5 (50% position size)
        - HIGH (VIX 18-22): 0.7 (70% position size)
        - NORMAL (VIX 12-18): 1.0 (100% position size)
        - LOW (VIX < 12): 1.2 (120% position size)
    """
    try:
        # Get current VIX regime
        vix_regime = VIXCache.get_vix_regime(redis_client)
        
        # Get position_size_multiplier from VIX_THRESHOLDS
        thresholds = VIX_THRESHOLDS.get(vix_regime, VIX_THRESHOLDS['NORMAL'])
        multiplier = thresholds.get('position_size_multiplier', 1.0)
        
        logger.debug(f"✅ [RISK_MULTIPLIER] {pattern_type} in {vix_regime} regime: {multiplier:.2f}x")
        return multiplier
    except Exception as e:
        logger.warning(f"⚠️ [RISK_MULTIPLIER] Error getting multiplier for {pattern_type}: {e}")
        return 1.0  # Default to 1.0 (no adjustment)

# ============================================================================
# 7. CONFIDENCE THRESHOLDS
# ============================================================================

def get_confidence_threshold(alert_type: str = None, vix_regime: str = None, redis_client=None) -> float:
    """
    Get confidence threshold for alert type based on VIX regime.
    
    ✅ Used by alert_validator.py and alert_manager.py to determine minimum confidence.
    
    Args:
        alert_type: Optional alert type (e.g., 'volume_spike', 'breakout_confirmation')
        vix_regime: Optional VIX regime (PANIC, HIGH, NORMAL, LOW). If None, fetches from cache.
        redis_client: Optional Redis client for VIX fetching
    
    Returns:
        Minimum confidence threshold (0.0 to 1.0)
    
    Confidence thresholds by VIX Regime:
        - PANIC (VIX > 22): 0.95 (95% - very high confidence required)
        - HIGH (VIX 18-22): 0.90 (90% - high confidence required)
        - NORMAL (VIX 12-18): 0.85 (85% - standard confidence)
        - LOW (VIX < 12): 0.80 (80% - slightly lower threshold)
    """
    try:
        # Get VIX regime if not provided
        if vix_regime is None:
            vix_regime = VIXCache.get_vix_regime(redis_client)
        
        # Base confidence thresholds by regime
        confidence_thresholds = {
            'PANIC': 0.95,  # Very high confidence required in panic
            'HIGH': 0.90,   # High confidence required
            'NORMAL': 0.85, # Standard confidence
            'LOW': 0.80     # Slightly lower threshold in low volatility
        }
        
        # Get threshold for regime
        threshold = confidence_thresholds.get(vix_regime, 0.85)  # Default to NORMAL
        
        # Alert-type specific adjustments (if needed in future)
        if alert_type:
            alert_type_lower = alert_type.lower()
            # High-priority alerts require higher confidence
            if 'kow_signal' in alert_type_lower or 'straddle' in alert_type_lower:
                threshold = min(threshold + 0.05, 0.95)  # Add 5% but cap at 95%
        
        logger.debug(f"✅ [CONFIDENCE_THRESHOLD] {alert_type or 'default'} in {vix_regime} regime: {threshold:.2f}")
        return threshold
    except Exception as e:
        logger.warning(f"⚠️ [CONFIDENCE_THRESHOLD] Error getting threshold for {alert_type}: {e}")
        return 0.85  # Default to 85% confidence
