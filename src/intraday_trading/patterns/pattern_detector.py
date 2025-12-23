#!/opt/homebrew/bin/python3.13
"""
8-PATTERN DETECTOR - SINGLE SOURCE OF TRUTH
============================================

Focused pattern detection limited to the 8 approved patterns.
All legacy patterns stay dormant and their signals are filtered upstream.

Allowed Patterns (8 total):
- Advanced Pattern Detectors (5): order_flow_breakout, gamma_exposure_reversal, 
  microstructure_divergence, cross_asset_arbitrage, vix_momentum
- Strategies (2): kow_signal_straddle, ict_iron_condor
- Game Theory (1): game_theory_signal

Based on Zerodha WebSocket data:
- zerodha_cumulative_volume: Cumulative bucket_incremental_volume from session start
- last_traded_quantity: Per-trade quantity (incremental)
- total_buy_quantity: Total buy bucket_incremental_volume
- total_sell_quantity: Total sell bucket_incremental_volume
"""

import asyncio
import logging
import os
import math
import traceback
import statistics
import sys
import json
import random
import uuid
import inspect
import redis
from threading import Lock
from typing import Dict, Any, List, Optional, Set, Tuple
from collections import deque
from collections import defaultdict

# Registry helper (feature-flag aware) for pattern detectors
try:
    from shared_core.instrument_token.registry_helper import (
        get_pattern_detector_registry,
        resolve_symbol_with_monitoring,
    )
except Exception:
    get_pattern_detector_registry = None  # type: ignore
    resolve_symbol_with_monitoring = None  # type: ignore

# ✅ FIX: Add project root to sys.path for alerts import (alerts moved to parent root)
# CRITICAL: Prioritize intraday_trading so it finds intraday_trading/redis_files first
intraday_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # intraday_trading/
project_root = os.path.dirname(intraday_root)  # aion_algo_trading/
# Remove if already present to avoid duplicates
if project_root in sys.path:
    sys.path.remove(project_root)
if intraday_root in sys.path:
    sys.path.remove(intraday_root)
# Insert intraday_trading FIRST (highest priority) so it finds intraday_trading/redis_files
# Insert project_root FIRST so alerts module is found from repo root
# Then intraday_trading so it finds intraday_trading/redis_files
sys.path.insert(0, str(project_root))
sys.path.insert(1, str(intraday_root))

# Import new bucket_incremental_volume architecture
from shared_core.volume_files.volume_computation_manager import VolumeComputationManager, get_volume_computation_manager
from shared_core.volume_files.correct_volume_calculator import VolumeResolver  # VolumeResolver still in correct_volume_calculator
# ✅ REMOVED: TimeAwareVolumeBaseline - use UnifiedTimeAwareBaseline via volume_manager instead
from .volume_thresholds import VolumeThresholdCalculator
# Removed duplicate import - using core.data.volume_thresholds as single source of truth
from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
# ✅ REMOVED: resolve_underlying_price_helper - replaced with self.get_reliable_underlying_price()
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
import time
from functools import wraps, lru_cache


# ✅ LOG FILTER: Reduce noise from pattern detection logs
class PatternLogFilter(logging.Filter):
    """Filter out noisy pattern detection logs"""
    
    def filter(self, record):
        # Suppress ATM strike skip logs (they're DEBUG level, not INFO)
        if "does NOT match ATM strike" in record.getMessage():
            return False
        
        # Suppress combined premium missing leg warnings if they're common
        if "combined premium - missing legs" in record.getMessage():
            return False  # Or set to DEBUG level
        
        # Keep everything else
        return True


class MarketContext:
    """✅ SINGLE source of market state - fetched ONCE with 3-minute TTL cache
    
    All 8 patterns use this cached context instead of making individual calls.
    Reduces Redis calls and ensures consistency across all pattern detection.
    """
    
    def __init__(self, redis_client=None):
        # ✅ CRITICAL: Ensure self.redis is a *reusable* client with decode_responses=True
        if redis_client is None:
            from shared_core.redis_clients.redis_client import get_redis_client
            # Force DB=1 and decode_responses=True
            self.redis = get_redis_client(process_name="market_context", db=1, decode_responses=True)
        else:
            # ✅ Honor existing client if provided, but ensure we use the underlying redis object if it's a wrapper
            # This ensures we don't nest wrappers or use stale client pools
            self.redis = getattr(redis_client, 'redis', redis_client)
            
        self._cache_ttl = 180  # ✅ 3 minutes TTL (180 seconds)
        self._last_fetch = 0
        self._cached_data = None
        
    def get_context(self, force_refresh=False) -> Dict:
        """Get ALL market context ONCE per tick (cached for 3 minutes)"""
        current_time = time.time()
        
        # Use cache if within TTL
        if not force_refresh and self._cached_data and (current_time - self._last_fetch) < self._cache_ttl:
            return self._cached_data
        
        # Fetch EVERYTHING once
        vix_value, vix_regime = self._get_vix_once()
        thresholds, normalized_regime, regime_multipliers = self._get_all_thresholds_once(vix_regime)
        
        context = {
            # VIX data
            'vix_value': vix_value,
            'vix_regime': vix_regime,
            'volatility_regime': vix_regime,  # ✅ FIX: volatility_regime is same as vix_regime
            'normalized_vix_regime': normalized_regime,  # ✅ Cached normalized regime
            'regime_multipliers': regime_multipliers,     # ✅ Cached multipliers
            
            # Thresholds for ALL patterns (VIX-aware)
            'thresholds': thresholds,
            
            # Volume baselines
            'volume_baselines': {},
            
            # Sector correlations
            'correlation_groups': self._get_correlation_groups_once(),
            
            # Current timestamp
            'timestamp': current_time,
            'timestamp_ms': int(current_time * 1000),
            
            # Cached for next tick
            '_cache_time': current_time
        }
        
        self._cached_data = context
        self._last_fetch = current_time
        return context
    
    def _get_vix_once(self) -> tuple[Optional[float], str]:
        """
        ✅ FIXED: Use get_safe_vix_regime() which uses VIXCache (180-second Redis cache).
        
        No need to fetch directly - VIXCache already caches VIX in Redis.
        """
        try:
            from shared_core.config_utils.thresholds import get_safe_vix_regime
            from shared_core.utils.vix_utils import get_vix_utils
            
            # ✅ SINGLE SOURCE: Use get_safe_vix_regime() which uses VIXCache (cached in Redis)
            vix_regime = get_safe_vix_regime(redis_client=self.redis)
            
            # Get VIX value from vix_utils (also uses Redis cache)
            vix_utils = get_vix_utils(redis_client=self.redis)
            vix_value = vix_utils.get_vix_value() if vix_utils else None
            
            if vix_regime and vix_regime != "UNKNOWN":
                return vix_value, vix_regime
        except Exception:
            ...
        return None, "NORMAL"  # Default fallback
    
    def _get_all_thresholds_once(self, vix_regime: str) -> tuple[Dict, str, Dict]:
        """✅ Get VIX-aware thresholds for ALL patterns ONCE, also return normalized regime and multipliers"""
        try:
            from shared_core.config_utils.thresholds import (
                VIX_THRESHOLDS,
                get_thresholds_for_regime
            )
            
            # Normalize VIX regime (ensure it's one of the valid regimes)
            valid_regimes = ['PANIC', 'HIGH', 'NORMAL', 'LOW']
            normalized_regime = vix_regime.upper() if vix_regime.upper() in valid_regimes else 'NORMAL'
            
            # Get thresholds for the regime
            thresholds = get_thresholds_for_regime(normalized_regime, self.redis)
            regime_multipliers = VIX_THRESHOLDS.get(normalized_regime, VIX_THRESHOLDS['NORMAL'])
            
            return thresholds, normalized_regime, regime_multipliers
        except Exception:
            return {}, vix_regime, {}
    
    def _get_vix_regime_once(self) -> str:
        """Helper to get VIX regime"""
        _, vix_regime = self._get_vix_once()
        return vix_regime
    
    def _get_correlation_groups_once(self) -> Dict:
        """Get correlation groups (placeholder - can be enhanced)"""
        return {}

# Pattern schema functions (from base_detector - consolidated)
def create_pattern(pattern_data, indicators):
    """
    ✅ FIXED: Fallback pattern creation - delegates to modern create_pattern() for consistency.
    
    This is a compatibility wrapper that ensures all pattern creation uses the unified schema.
    """
    try:
        from intraday_trading.patterns.utils.pattern_schema import create_pattern as modern_create_pattern
        
        # Extract fields from pattern_data and indicators
        symbol = pattern_data.get('symbol', indicators.get('symbol', 'UNKNOWN'))
        pattern_type = pattern_data.get('pattern', pattern_data.get('pattern_type', 'unknown'))
        signal = pattern_data.get('signal', 'NEUTRAL')
        confidence = pattern_data.get('confidence', 0.5)
        
        # Use modern create_pattern() for consistency
        return modern_create_pattern(
            symbol=symbol,
            pattern_type=pattern_type,
            signal=signal,
            confidence=confidence,
            **pattern_data,  # Pass through all other fields
            **{k: v for k, v in indicators.items() if k not in pattern_data}  # Add missing fields from indicators
        )
    except Exception:
        # Fallback: return pattern_data as-is if import fails
        return pattern_data

def validate_pattern(pattern):
    """Basic pattern validation"""
    return True

# ✅ CACHED: Pattern config path (module-level constant)
_PATTERN_CONFIG_PATH = Path(__file__).parent / "data" / "pattern_registry_config.json"

@lru_cache(maxsize=1)
def _load_pattern_config_cached() -> Dict[str, Any]:
    """✅ CACHED: Load pattern configuration from JSON file (cached at module level)"""
    try:
        if _PATTERN_CONFIG_PATH.exists():
            with open(_PATTERN_CONFIG_PATH, 'r') as f:
                config = json.load(f)
                logger.debug(f"✅ Loaded pattern config from {_PATTERN_CONFIG_PATH} (cached)")
                return config
        else:
            logger.warning(f"⚠️ Pattern config not found at {_PATTERN_CONFIG_PATH}")
            return {}
    except Exception as e:
        logger.warning(f"⚠️ Could not load pattern config: {e}")
        return {}

# Import VIX utilities for volatility-aware thresholds
try:
    from shared_core.utils.vix_utils import VIXUtils
    vix_utils = VIXUtils()
except ImportError:
    vix_utils = None

from .pattern_mathematics import PatternMathematics

# Pattern module imports (relative imports since we're in the patterns package)
try:
    from .delta_analyzer import DeltaAnalyzer
    DELTA_ANALYZER_AVAILABLE = True
except ImportError:
    DeltaAnalyzer = None
    DELTA_ANALYZER_AVAILABLE = False

try:
    from .volume_profile_detector import VolumeProfileDetector
    VOLUME_PROFILE_DETECTOR_AVAILABLE = True
except ImportError:
    VolumeProfileDetector = None
    VOLUME_PROFILE_DETECTOR_AVAILABLE = False

# ✅ Default: disable kow_signal_straddle (legacy strategy) unless explicitly enabled
KOW_SIGNAL_ENABLED = False
try:
    if KOW_SIGNAL_ENABLED:
        from .strategies.kow_signal_straddle import KowSignalStraddleStrategy
        KOW_SIGNAL_AVAILABLE = True
    else:
        KowSignalStraddleStrategy = None
        KOW_SIGNAL_AVAILABLE = False
except ImportError:
    KowSignalStraddleStrategy = None
    KOW_SIGNAL_AVAILABLE = False

try:
    from .ict.ict_iron_condor import ICTIronCondor
    ICT_IRON_CONDOR_AVAILABLE = True
except ImportError:
    ICTIronCondor = None
    ICT_IRON_CONDOR_AVAILABLE = False

# ✅ FIX: Make MathDispatcher import conditional to handle path setup timing
try:
    from intraday_trading.intraday_scanner.math_dispatcher import MathDispatcher
    MATH_DISPATCHER_AVAILABLE = True
except ImportError:
    # Fallback: MathDispatcher will be imported later when paths are set up
    MathDispatcher = None
    MATH_DISPATCHER_AVAILABLE = False

try:
    from intraday_trading.patterns.pattern_mathematics import PatternMathematics
    PATTERN_MATHEMATICS_AVAILABLE = True
except ImportError:
    import intraday_trading.patterns.pattern_mathematics as PatternMathematics  # type: ignore
    PATTERN_MATHEMATICS_AVAILABLE = False

# ✅ UPDATED: Use UnifiedRedisManager for centralized Redis access
try:
    from shared_core.redis_clients import UnifiedRedisManager
    UNIFIED_REDIS_AVAILABLE = True
except ImportError:
    UnifiedRedisManager = None
    UNIFIED_REDIS_AVAILABLE = False

try:
    from .ict import (
        ICTPatternDetector,
        ICTLiquidityDetector,
        ICTFVGDetector,
        ICTOTECalculator,
        ICTPremiumDiscountDetector,
    )
    # ✅ REMOVED: Killzone detector - no longer used by 8 core patterns
    # Only game_theory_signal had "killzone_filter": true, but killzone is not actively used
    # from .ict.killzone import EnhancedICTKillzoneDetector
    ICT_MODULES_AVAILABLE = True
    KILLZONE_DETECTOR_AVAILABLE = False  # ✅ Disabled - killzone not used
except ImportError:
    ICTPatternDetector = None
    ICTLiquidityDetector = None
    ICTFVGDetector = None
    ICTOTECalculator = None
    ICTPremiumDiscountDetector = None
    ICTKillzoneDetector = None
    EnhancedICTKillzoneDetector = None
    ICT_MODULES_AVAILABLE = False
    KILLZONE_DETECTOR_AVAILABLE = False

# ✅ NEW: Advanced pattern detectors (moved to advanced/ subdirectory for consistency)
try:
    from .advanced import (
        OrderFlowBreakoutDetector,
        GammaExposureReversalDetector,
        MicrostructureDivergenceDetector,
        CrossAssetArbitrageDetector,
        VIXRegimeMomentumDetector,
    )
    ADVANCED_DETECTORS_AVAILABLE = True
except ImportError:
    OrderFlowBreakoutDetector = None
    GammaExposureReversalDetector = None
    MicrostructureDivergenceDetector = None
    CrossAssetArbitrageDetector = None
    VIXRegimeMomentumDetector = None
    ADVANCED_DETECTORS_AVAILABLE = False

# ✅ REMOVED: Risk Manager import - Risk management is handled by UnifiedAlertBuilder
# Risk management (position sizing, stop loss) is the responsibility of:
# - UnifiedAlertBuilder (intraday_trading/alerts/unified_alert_builder.py)
# - thresholds.py (shared_core/config_utils/thresholds.py)
# 
# Flow: Pattern Detection → UnifiedAlertBuilder.build_algo_alert() → Notifiers
# 
# Pattern detector should only detect patterns, not manage risk.

# Simplified imports for 8 core patterns only
# VIX utilities for dynamic thresholds
# ✅ Note: logger is already defined above (line 35)
try:
    # ✅ FIXED: Use shared_core.config_utils.thresholds (single source of truth)
    from shared_core.config_utils.thresholds import (
        is_volume_dependent_pattern, 
        get_pattern_volume_requirement,
        get_thresholds_for_regime,
        VIX_THRESHOLDS,
        VIXCache,
        get_safe_vix_regime,
        get_instrument_config
    )
    # Legacy functions not in shared_core - use fallbacks if needed
    get_volume_threshold = None  # Not used by 7 patterns
    get_sector_threshold = None  # Not used by 7 patterns
    # ✅ FIXED: VIX functions helper - robust import with fallback
    def _import_vix_utils():
        """Helper to import VIX utilities with multiple fallback strategies"""
        try:
            from shared_core.utils.vix_utils import get_vix_value, get_vix_regime, get_vix_utils
            return get_vix_value, get_vix_regime, get_vix_utils
        except ImportError:
            try:
                # Fallback: try absolute import using importlib
                import importlib.util
                vix_utils_path = os.path.join(intraday_root, "utils", "vix_utils.py")
                if os.path.exists(vix_utils_path):
                    spec = importlib.util.spec_from_file_location("vix_utils", vix_utils_path)
                    if spec and spec.loader:
                        vix_utils_module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(vix_utils_module)
                        return (vix_utils_module.get_vix_value, 
                                vix_utils_module.get_vix_regime,
                                vix_utils_module.get_vix_utils)
            except Exception:
                ...
            # Last resort: return dummy functions with correct return types
            def get_vix_value() -> Optional[float]:
                """Fallback: returns None if VIX value unavailable"""
                return None
            def get_vix_regime() -> str:
                """Fallback: returns 'NORMAL' regime if VIX unavailable"""
                return "NORMAL"
            def get_vix_utils(redis_client=None):
                # Return a dummy VIXUtils instance to match the expected return type
                try:
                    from shared_core.utils.vix_utils import VIXUtils
                    return VIXUtils(redis_client=redis_client)
                except ImportError:
                    # If VIXUtils class is not available, create a minimal dummy class
                    class DummyVIXUtils:
                        def __init__(self, redis_client=None):
                            self.redis_client = redis_client
                        def get_current_vix(self):
                            return None
                        def get_vix_value(self):
                            return None
                        def get_vix_regime(self):
                            return "NORMAL"
                    return DummyVIXUtils(redis_client=redis_client)
            return get_vix_value, get_vix_regime, get_vix_utils
    
    get_vix_value, get_vix_regime, get_vix_utils = _import_vix_utils()  # type: ignore[assignment]
    THRESHOLD_CONFIG_AVAILABLE = True
except ImportError as e:
    # Fallback: try importing from shared_core.config_utils.thresholds
    try:
        from shared_core.config_utils.thresholds import (
            is_volume_dependent_pattern, 
            get_pattern_volume_requirement,
            get_thresholds_for_regime,
            VIX_THRESHOLDS,
            get_instrument_config,
            get_safe_vix_regime,
            VIXCache
        )
        # Try to get VIX functions (use the helper if main import failed)
        import logging
        logger = logging.getLogger(__name__)
        if 'get_vix_utils' not in globals():
            get_vix_value, get_vix_regime, get_vix_utils = _import_vix_utils()  # type: ignore[assignment]
        logger.warning(f"⚠️ Using fallback imports: {e}")
    except ImportError:
        # Final fallback: define minimal functions
        def is_volume_dependent_pattern(pattern_name: str) -> bool:
            volume_dependent = ['volume_price_divergence', 'hidden_accumulation']  # ✅ REMOVED: volume_spike, volume_breakout
            return pattern_name in volume_dependent
        
        def get_pattern_volume_requirement(*args, **kwargs) -> float:
            return 0.0
        
        def get_vix_value():
            return None
        def get_vix_regime():
            return "NORMAL"
        
        get_volume_threshold = None
        get_confidence_threshold = None
        get_sector_threshold = None

# ✅ Define get_asset_aware_threshold at module level to avoid duplicate declarations
if 'get_asset_aware_threshold' not in globals():
    # Use get_pattern_volume_requirement if available, otherwise return 0.0
    if 'get_pattern_volume_requirement' in globals():
        _pattern_volume_func = get_pattern_volume_requirement
        def get_asset_aware_threshold(pattern_name: str, symbol: Optional[str] = None, 
                                     vix_regime: Optional[str] = None, 
                                     sector: Optional[str] = None, redis_client=None) -> float:
            """Asset-aware threshold wrapper that uses get_pattern_volume_requirement"""
            try:
                return _pattern_volume_func(
                    pattern_name=pattern_name,
                    symbol=symbol,
                    vix_regime=vix_regime,
                    redis_client=redis_client
                )
            except Exception:
                return 0.0
    else:
        def get_asset_aware_threshold(pattern_name: str, symbol: Optional[str] = None, 
                                     vix_regime: Optional[str] = None, 
                                     sector: Optional[str] = None, redis_client=None) -> float:
            """Fallback asset-aware threshold that returns 0.0"""
            return 0.0

if 'get_vix_value' not in locals():
    def get_vix_value():
        return None
if 'get_vix_regime' not in locals():
    def get_vix_regime():
        return "NORMAL"
    if 'get_volume_threshold' not in locals():
        get_volume_threshold = None
    if 'get_confidence_threshold' not in locals():
        get_confidence_threshold = None
    if 'get_sector_threshold' not in locals():
        get_sector_threshold = None
    THRESHOLD_CONFIG_AVAILABLE = False


logger = logging.getLogger(__name__)
# ✅ Apply log filter to reduce noise
pattern_logger = logging.getLogger('pattern_detector')
pattern_logger.addFilter(PatternLogFilter())
# Also apply to module logger
logger.addFilter(PatternLogFilter())
# Also apply to kow_signal logger
kow_logger = logging.getLogger('kow_signal')
kow_logger.addFilter(PatternLogFilter())

# Use DatabaseAwareKeyBuilder static methods directly - no instance needed
PATTERN_DEBUG_ENABLED = bool(int(os.environ.get("PATTERN_DEBUG_ENABLED", "0")))
DEBUG_TRACE_ENABLED = os.getenv("DEBUG_TRACE", "1").lower() in ("1", "true", "yes")
PATTERN_DEBUG_FIELDS = {
    'order_flow_breakout': ['order_flow_imbalance', 'cumulative_volume_delta', 'volume_ratio'],
    'gamma_exposure_reversal': ['gamma_exposure', 'gamma', 'delta', 'iv'],
    'microstructure_divergence': ['microprice', 'last_price', 'order_book_imbalance', 'volume_ratio'],
    'cross_asset_arbitrage': ['usdinr_basis', 'usdinr_sensitivity', 'underlying_price'],
    'vix_momentum': ['vix_level', 'vix_change_pct', 'implied_volatility'],
    'game_theory_signal': ['order_book_imbalance', 'depth_imbalance', 'best_bid_size', 'best_ask_size'],
}

if not PATTERN_DEBUG_ENABLED and logger.isEnabledFor(logging.DEBUG):
    PATTERN_DEBUG_ENABLED = True


def _pattern_field_snapshot(payload: Dict[str, Any]) -> Dict[str, Any]:
    greeks = payload.get('greeks') if isinstance(payload.get('greeks'), dict) else {}
    snapshot_fields = {
        'delta': payload.get('delta', greeks.get('delta')),
        'gamma': payload.get('gamma', greeks.get('gamma')),
        'gamma_exposure': payload.get('gamma_exposure', greeks.get('gamma_exposure')),
        'order_flow_imbalance': payload.get('order_flow_imbalance'),
        'vix_level': payload.get('vix_level') or payload.get('vix_regime'),
        'volume_ratio': payload.get('volume_ratio'),
    }
    return {k: v for k, v in snapshot_fields.items() if v not in (None, '')}


def identify_option_type(symbol: Optional[str]) -> Optional[str]:
    """Deduce option type from common Indian option symbol suffixes."""
    if not symbol:
        return None
    normalized = str(symbol).strip().upper().replace("'", "").replace('"', "")
    if normalized.endswith("PE"):
        return "PE"
    if normalized.endswith("CE"):
        return "CE"
    if normalized.endswith("P"):
        return "PE"
    if normalized.endswith("C"):
        return "CE"
    return None


def preprocess_delta_for_patterns(symbol: str, delta_value: Optional[Any], logger: logging.Logger) -> Optional[float]:
    """Correct delta sign based on inferred option type before pattern gating."""
    if delta_value is None:
        return None
    try:
        delta_float = float(delta_value)
    except (TypeError, ValueError):
        return None
    option_type = identify_option_type(symbol)
    if option_type == "PE" and delta_float > 0:
        corrected = -abs(delta_float)
        logger.warning(
            f"⚠️ [DELTA_CORRECTION] {symbol} - Corrected delta from {delta_float} to {corrected}"
        )
        return corrected
    if option_type == "CE" and delta_float < 0:
        corrected = abs(delta_float)
        logger.warning(
            f"⚠️ [DELTA_CORRECTION] {symbol} - Corrected delta from {delta_float} to {corrected}"
        )
        return corrected
    return delta_float


def log_option_details(
    logger: logging.Logger, symbol: str, greeks: Dict[str, Any], price_data: Dict[str, Any]
) -> None:
    option_type = identify_option_type(symbol)
    delta_val = greeks.get("delta", 0.0)
    last_price = price_data.get("last_price")
    underlying_price = price_data.get("underlying_price")
    strike_match = re.findall(r"(\d+)(CE|PE)?$", symbol.upper())
    strike_text = strike_match[0][0] if strike_match else "UNKNOWN"
    logger.info(
        f"\n🔍 [OPTION_DEBUG] {symbol}\n"
        f"  Type: {option_type or 'UNKNOWN'}\n"
        f"  Delta: {delta_val} "
        f"{'(PUT - should be negative)' if option_type == 'PE' else '(CALL - should be positive)' if option_type == 'CE' else ''}\n"
        f"  Last Price: {last_price}\n"
        f"  Strike: {strike_text}\n"
        f"  Underlying: {underlying_price}\n"
    )


# ============================================================================
# DATA QUALITY ENFORCER - Emergency Hotfix for Data Validation
# ============================================================================

class DataQualityEnforcer:
    """
    ✅ EMERGENCY HOTFIX: Data quality enforcer for pattern detection.
    
    Validates data quality before pattern detection to prevent:
    - Low volume false signals
    - Stale/corrupted EMA data
    - Invalid option Greeks
    - Invalid timestamps
    
    This is a defense-in-depth layer that runs inside pattern_detector
    even if data_pipeline validation passes.
    """
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        
        # Thresholds
        self.min_volume_ratio = 0.1  # Minimum 10% volume for reliable patterns
        self.max_identical_emas = 2  # Max identical EMA values (indicates stale data)
        
        # Stats tracking
        self.stats = {
            'total_validations': 0,
            'rejected_volume': 0,
            'rejected_stale_emas': 0,
            'rejected_options': 0,
            'rejected_timestamps': 0,
            'passed': 0
        }
    
    
    def _is_futures_contract(self, symbol: str) -> bool:
        """Check if symbol is a futures contract (no Greeks expected)"""
        if not symbol:
            return False
        try:
            from shared_core.redis_clients.redis_key_standards import get_symbol_parser
            parser = get_symbol_parser()
            canonical = RedisKeyStandards.canonical_symbol(symbol)
            parsed = parser.parse_symbol(canonical)
            return parsed.instrument_type == 'FUT'
        except Exception:
            symbol_upper = str(symbol).upper()
            base = symbol_upper.split(':', 1)[-1]
            return base.endswith('FUT')
    
    def validate_option_data(self, symbol: str, data: dict) -> bool:
        """Validate option-specific data (skip for futures)"""
        # ✅ FIX 1: Futures contracts don't need Greek validation
        if self._is_futures_contract(symbol):
            self.logger.debug(f"✅ FUTURES: {symbol} - Skipping Greek validation (futures don't have Greeks)")
            return True
        
        if not symbol.endswith(('CE', 'PE')):
            return True  # Not an option or futures
        
        issues = []
        
        # ✅ FIX: Check if Greeks are PRESENT (not just non-zero)
        # Greeks can legitimately be 0 (e.g., deep OTM options), so we check for presence, not value
        critical_greeks = ['gamma', 'vega']  # Only gamma and vega are critical for validation
        for greek in critical_greeks:
            greek_value = data.get(greek)
            # Check if Greek is missing (None) or explicitly not present
            if greek_value is None:
                # Try to fetch from Redis if we have access to PatternDetector's storage
                # Note: This is called from pre_detection_validation, which runs after enrichment
                # So if it's still None after enrichment, it's likely not in Redis
                issues.append(f"Missing {greek}")
            # Allow 0 values - they're legitimate (e.g., deep OTM options)
        
        # Delta and theta are less critical - only check if they're completely missing
        optional_greeks = ['delta', 'theta']
        for greek in optional_greeks:
            if greek not in data:
                # These are warnings, not blockers
                pass
        
        # Check DTE (already checked in pre_detection_validation, but double-check here)
        dte = data.get('trading_dte', 0)
        if dte <= 0:
            # Skip DTE check here - it's already handled in pre_detection_validation
            pass
        
        # Check IV - this is critical
        iv = data.get('implied_volatility') or data.get('iv')
        if iv is None:
            issues.append(f"Missing IV")
        elif iv <= 0 or iv > 5:  # IV should be between 0 and 5 (500%)
            issues.append(f"Invalid IV: {iv}")
        
        if issues:
            self.logger.warning(f"❌ OPTION DATA INVALID: {symbol} - {', '.join(issues)}")
            self.stats['rejected_options'] += 1
            return False
        
        return True
    
    def pre_detection_validation(self, symbol: str, data: dict) -> bool:
        """
        ✅ CRITICAL: Validate data before pattern detection.
        Returns False if data is invalid and pattern detection should be skipped.
        
        NOTE: Volume validation is handled per-pattern by the detector.
        This validates only data quality (stale EMAs, invalid timestamps, option Greeks).
        """
        self.stats['total_validations'] += 1
        
        # 1. ✅ REMOVED: Timestamp normalization - data_pipeline already handles this
        # data_pipeline._normalize_timestamps() is called before sending data to pattern_detector
        
        # Different patterns have different volume requirements (some need none!)
        # Momentum, breakout, reversal, ICT patterns should work even with low volume
        
        # 3. Check for stale EMAs (all identical = corrupted/stale data)
        ema_periods = [5, 10, 20, 50]
        ema_values = [data.get(f'ema_{period}') for period in ema_periods if data.get(f'ema_{period}')]
        
        if len(ema_values) >= 3:  # Only check if we have at least 3 EMAs
            unique_emas = len(set(ema_values))
            if unique_emas <= self.max_identical_emas:
                self.logger.warning(
                    f"🛑 REJECTED: {symbol} - stale EMA data ({unique_emas} unique values, need > {self.max_identical_emas})"
                )
                self.stats['rejected_stale_emas'] += 1
                return False
        
        # 4. Option-specific checks (early DTE check to avoid processing expired options)
        if symbol.endswith(('CE', 'PE')):
            # ✅ EARLY FILTER: Check DTE first (before expensive Greek validation)
            dte = data.get('trading_dte') or data.get('dte')
            if dte is None:
                dte_years = data.get('dte_years')
                if dte_years is not None:
                    dte = float(dte_years) * 365
            if dte is not None:
                dte = float(dte)
                if dte <= 0:
                    self.logger.debug(f"⏭️ [SKIP_EXPIRED] {symbol} - Expired option (DTE={dte}), skipping pattern detection")
                    self.stats['rejected_options'] += 1
                    return False
            
            # Only validate Greeks if DTE is valid
            if not self.validate_option_data(symbol, data):
                return False
        
        # 5. Timestamp validity (final check)
        timestamp_ms = data.get('timestamp_ms', 0)
        if timestamp_ms > 2000000000000:  # Still invalid after normalization
            self.logger.warning(f"🛑 REJECTED: {symbol} - invalid timestamp: {timestamp_ms}")
            self.stats['rejected_timestamps'] += 1
            return False
        
        self.logger.debug(f"✅ DATA QUALITY PASSED: {symbol}")
        self.stats['passed'] += 1
        return True
    
    def get_stats(self) -> dict:
        """Get validation statistics"""
        return self.stats.copy()
    
    def reset_stats(self):
        """Reset validation statistics"""
        for key in self.stats:
            self.stats[key] = 0
    
    def store_stats_to_redis(self, redis_client=None) -> bool:
        """
        ✅ Store validation statistics to Redis DB3 for 30-day retention.
        
        Key: scanner_performance:data_quality:{date}
        Storage: Redis DB3 (scanner_performance database)
        TTL: 30 days (2,592,000 seconds)
        
        Args:
            redis_client: Redis client (will get DB3 client if not provided)
            
        Returns:
            bool: True if stored successfully, False otherwise
        """
        try:
            # Get DB3 client
            if redis_client:
                # Try to get DB3 client from redis_client
                if hasattr(redis_client, 'get_client'):
                    db3_client = redis_client.get_client(3)
                elif hasattr(redis_client, 'setex'):
                    # Assume it's already a client - try to use it (may not be DB3)
                    db3_client = redis_client
                else:
                    db3_client = redis_client
            else:
                # Try to get DB3 client from RedisManager82
                try:
                    from shared_core.redis_clients.redis_client import UnifiedRedisManager
                    db3_client = get_redis_client(process_name="data_quality_stats", db=3)
                except Exception:
                    self.logger.warning("⚠️ Could not get Redis DB3 client - stats not stored")
                    return False
            
            if not db3_client:
                self.logger.warning("⚠️ Redis DB3 client not available - stats not stored")
                return False
            
            # Get current date for key
            from datetime import datetime
            date_str = datetime.now().strftime("%Y-%m-%d")
            hour_str = datetime.now().strftime("%H")
            
            # Key format: scanner_performance:data_quality:{date}:{hour}
            # This allows hourly aggregation while keeping 30-day retention
            stats_key = f"scanner_performance:data_quality:{date_str}:{hour_str}"
            
            # Prepare stats data with timestamp
            stats_data = {
                **self.stats.copy(),
                'timestamp': datetime.now().isoformat(),
                'timestamp_ms': int(datetime.now().timestamp() * 1000),
                'date': date_str,
                'hour': hour_str
            }
            
            # Store as hash in Redis DB3
            # Use HSET to update/append to hourly stats
            import json
            stats_json = json.dumps(stats_data)
            
            # Store with 30-day TTL (2,592,000 seconds)
            ttl_30_days = 30 * 24 * 60 * 60  # 2,592,000 seconds
            db3_client.setex(stats_key, ttl_30_days, stats_json)
            
            self.logger.debug(f"✅ Stored data quality stats to Redis DB3: {stats_key} (TTL: 30 days)")
            return True
            
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to store data quality stats to Redis: {e}")
            return False

# ✅ FOCUSED: Only 8 patterns enabled
ALLOWED_PATTERNS = {
    "order_flow_breakout",
    "gamma_exposure_reversal",
    "microstructure_divergence",
    "cross_asset_arbitrage",  # usdinr_arbitrage
    "vix_momentum",
    # "kow_signal_straddle",  # ✅ DISABLED: Temporarily disabled for testing other 7 patterns
    "ict_iron_condor",
    "game_theory_signal",
}

# Performance monitoring decorator
def time_pattern_method(method):
    """Decorator to time pattern detection methods and log slow ones"""
    @wraps(method)
    def timed_method(self, tick_data, *args, **kwargs):
        start_time = time.time()
        result = method(self, tick_data, *args, **kwargs)
        duration = time.time() - start_time
        
        # Log slow patterns (>100ms)
        if duration > 0.1:
            symbol = 'UNKNOWN'
            if isinstance(tick_data, dict):
                symbol = tick_data.get('symbol', 'UNKNOWN') or 'UNKNOWN'
            elif isinstance(tick_data, str):
                symbol = tick_data
            elif isinstance(tick_data, (tuple, list)) and tick_data:
                # Sometimes tick_data might be (symbol, payload)
                first = tick_data[0]
                if isinstance(first, str):
                    symbol = first
            elif isinstance(tick_data, (int, float)):
                symbol = str(tick_data)
            
            if symbol == 'UNKNOWN':
                symbol = kwargs.get('symbol') or (args[0] if args and isinstance(args[0], str) else 'UNKNOWN')
            
            logger.warning(f"SLOW PATTERN: {method.__name__} took {duration:.3f}s for {symbol}")
        
        return result
    return timed_method

# Standardized field mapping for 8 core patterns
# Using canonical field names from optimized_field_mapping.yaml
# ✅ FIX: Conditional import for yaml_field_loader
try:
    from shared_core.config_utils.yaml_field_loader import (
        get_field_mapping_manager,
        resolve_session_field,
        resolve_calculated_field as resolve_indicator_field,
    )
    FIELD_MAPPING_AVAILABLE = True
except ImportError as e:
    # Fallback: try direct file import
    try:
        import importlib.util
        yaml_field_loader_path = os.path.join(intraday_root, "utils", "yaml_field_loader.py")
        if os.path.exists(yaml_field_loader_path):
            spec = importlib.util.spec_from_file_location("yaml_field_loader", yaml_field_loader_path)
            if spec and spec.loader:
                yaml_field_loader_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(yaml_field_loader_module)
                get_field_mapping_manager = yaml_field_loader_module.get_field_mapping_manager
                resolve_session_field = yaml_field_loader_module.resolve_session_field
                resolve_indicator_field = yaml_field_loader_module.resolve_calculated_field
                FIELD_MAPPING_AVAILABLE = True
        else:
            raise ImportError(f"yaml_field_loader.py not found at {yaml_field_loader_path}")
    except Exception as e2:
        # Last resort: create dummy functions at module level
        logger.warning(f"yaml_field_loader not available: {e}, {e2}")
        def get_field_mapping_manager() -> Optional[Any]:  # type: ignore
            return None
        def resolve_session_field(field_name: str) -> str:
            return field_name
        def resolve_calculated_field(field_name: str) -> str:
            return field_name
        resolve_indicator_field = resolve_calculated_field
        FIELD_MAPPING_AVAILABLE = False

# Initialize field mapping manager
if FIELD_MAPPING_AVAILABLE:
    FIELD_MAPPING_MANAGER = get_field_mapping_manager()
else:
    FIELD_MAPPING_MANAGER = None

# Standardized helper functions for 8 core patterns
def _session_value(source: Dict[str, Any], field: str, default: float = 0.0) -> float:
    """Get session value using field mapping resolution."""
    resolved_field = resolve_session_field(field)
    return float(source.get(resolved_field, default))

def _indicator_value(source: Dict[str, Any], field: str, default: float = 0.0) -> float:
    """Get indicator value using field mapping resolution."""
    resolved_field = resolve_indicator_field(field)
    return float(source.get(resolved_field, default))


class BasePatternDetector:
    """
    ✅ CONSOLIDATED: Minimal base class for pattern detectors (consolidated from base_detector.py)
    
    Provides common initialization for detectors that don't need full PatternDetector functionality.
    PatternDetector itself is now standalone and doesn't inherit from this.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None, redis_client=None):
        """Initialize base pattern detector"""
        self.config = config or {}
        self.math_dispatcher = None
        self.redis_client = redis_client
        self.logger = logging.getLogger(__name__)
        # Feature-flag aware registry (optional)
        self.registry = None
        if get_pattern_detector_registry:
            try:
                self.registry = get_pattern_detector_registry()
            except Exception:
                self.registry = None

        
        # Load pattern configuration
        self.pattern_config = _load_pattern_config_cached()
        
        # Initialize pattern tracking
        self.pattern_counts = defaultdict(int)
        self.pattern_success_rates = defaultdict(float)
        
        # Initialize stats tracking
        self.stats = {
            "invocations": 0,
            "patterns_found": 0,
            "errors": 0,
        }
        
        # ✅ SINGLE SOURCE: Load thresholds from config (BasePatternDetector doesn't use MarketContext)
        # Note: This is a minimal base class - full PatternDetector uses MarketContext instead
        try:
            vix_regime = get_safe_vix_regime(redis_client)
            from shared_core.config_utils.thresholds import get_thresholds_for_regime
            self.thresholds = get_thresholds_for_regime(vix_regime, redis_client)
        except ImportError:
            self.thresholds = {}
        
        # Load dynamic volume baselines (20d/55d)
        self.volume_baselines = {}
        
        # Historical data tracking
        self.price_history = {}
        self.high_history = {}
        self.low_history = {}
        self.volume_history = {}
        self.price_volume = {}
        
        # Load sector correlation groups
        self.correlation_groups = self._load_correlation_groups()
        
        # Initialize VIX utilities
        self.vix_utils = None
        try:
            from shared_core.utils.vix_utils import VIXUtils
            self.vix_utils = VIXUtils(redis_client)
        except Exception as e:
            self.logger.warning(f"Could not initialize VIX utilities: {e}")
        
        # Initialize Delta Analyzer
        self.delta_analyzer = None
        try:
            from intraday_trading.patterns.delta_analyzer import DeltaAnalyzer
            self.delta_analyzer = DeltaAnalyzer(window=10)
        except Exception as e:
            self.logger.debug(f"Delta Analyzer not available: {e}")
    
    def _load_correlation_groups(self) -> Dict[str, List[str]]:
        """Load sector correlation groups from JSON file (optional - used for correlation filtering)"""
        try:
            sector_path = Path(__file__).parent.parent / "config" / "nse_sector_mapping.json"
            if sector_path.exists():
                with open(sector_path, 'r') as f:
                    return json.load(f)
            else:
                # Sector mapping file is optional - correlation filtering is disabled if not found
                self.logger.debug(f"Sector mapping file not found: {sector_path} (correlation filtering disabled)")
        except Exception as e:
            # Correlation groups are optional - only used for position correlation filtering
            self.logger.debug(f"Could not load correlation groups: {e} (correlation filtering disabled)")
        return {}
    
    def get_profile_data(self) -> Dict[str, Any]:
        """Get volume profile data (fallback implementation)"""
        return {
            'poc_price': 0.0,
            'poc_volume': 0.0,
            'value_area_high': 0.0,
            'value_area_low': 0.0,
            'total_volume': 0,
            'price_levels': 0,
            'profile_range': 0.0,
            'exchange_timestamp': '',
            'calculation_method': 'fallback',
            'support_levels': [],
            'resistance_levels': [],
            'price_volume_distribution': {}
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get detection statistics"""
        return self.stats.copy()

# ✅ DELAYED IMPORT: Avoid circular dependency when loading GameTheoryPatternDetector
GameTheoryPatternDetector = None
GAME_THEORY_DETECTOR_AVAILABLE = False
try:
    from .game_theory_pattern_detector import GameTheoryPatternDetector as _GameTheoryPatternDetector
    GameTheoryPatternDetector = _GameTheoryPatternDetector
    GAME_THEORY_DETECTOR_AVAILABLE = True
except ImportError as exc:
    logger.warning(f"⚠️ [PATTERN_INIT] GameTheoryPatternDetector unavailable: {exc}")
    GameTheoryPatternDetector = None
    GAME_THEORY_DETECTOR_AVAILABLE = False

class PatternDetector(BasePatternDetector):
    _instance = None
    _initialized = False
    _patterns_registered = False
    _shared_option_handlers: Dict[str, Any] = {}
    _shared_detector_methods: List[Any] = []
    """
    Simplified pattern detector using only 8 core patterns.
    Replaces complex pattern detection with focused, reliable patterns.
    Standalone class (consolidated from BasePatternDetector).
    
    8 Core Patterns:
    1. order_flow_breakout (OrderFlowBreakoutDetector)
    2. gamma_exposure_reversal (GammaExposureReversalDetector)
    3. microstructure_divergence (MicrostructureDivergenceDetector)
    4. cross_asset_arbitrage (CrossAssetArbitrageDetector)
    5. vix_momentum (VIXRegimeMomentumDetector)
    6. kow_signal_straddle (KowSignalStraddleStrategy)
    7. ict_iron_condor (ICTIronCondor)
    8. game_theory_signal (GameTheoryPatternDetector)
    
    ✅ VALIDATION BYPASS: Patterns that bypass all validation layers
    These patterns have their own internal validation and don't need external validation layers.
    """
    
    # ✅ CENTRALIZED: Patterns that bypass ALL validation layers
    VALIDATION_BYPASS_PATTERNS = [
        'vix_momentum',
        'gamma_exposure_reversal',
        'microstructure_divergence',
        'order_flow_breakout',
        'game_theory_signal',
        'cross_asset_arbitrage',
        'ict_iron_condor',
        # Note: 'kow_signal_straddle' is NOT in this list - it still requires validation
    ]

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        redis_client=None,
        worker_id: Optional[str] = None,
        process_type: Optional[str] = None,
    ):
        """
        Initialize simplified pattern detector with 8 core patterns - FIXED RACE CONDITION

        Args:
            config: Configuration dictionary
            redis_client: Redis client for market data
        """
        # ========== CRITICAL FIX: Early check WITHOUT lock ==========
        if getattr(self, "_initialized", False):
            logger.debug("🔄 PatternDetector already initialized; skipping re-init")
            self.option_pattern_handlers = self.__class__._shared_option_handlers
            self.all_pattern_detectors = self.__class__._shared_detector_methods
            return
        
        # ========== THREAD-SAFE INITIALIZATION (Double-Check Locking) ==========
        # Use a class-level lock to prevent concurrent initialization across workers
        if not hasattr(self.__class__, '_init_lock'):
            import threading
            self.__class__._init_lock = threading.Lock()
        
        with self.__class__._init_lock:
            # Double-check inside lock (another thread may have initialized while waiting)
            if getattr(self, "_initialized", False):
                logger.debug("🔄 PatternDetector already initialized (double-checked)")
                self.option_pattern_handlers = self.__class__._shared_option_handlers
                self.all_pattern_detectors = self.__class__._shared_detector_methods
                return
            
            # ========== MARK AS INITIALIZED FIRST! ==========
            # Set flag immediately to prevent re-entry during heavy initialization
            self._initialized = True
            
        # ========== NOW PROCEED WITH INITIALIZATION ==========
        self.worker_id = worker_id
        self.process_type = process_type
        self.initialized = False
        self.option_pattern_handlers = self.__class__._shared_option_handlers
        self.all_pattern_detectors = self.__class__._shared_detector_methods
        self.pid = os.getpid()
        self.process_name = os.environ.get('PROCESS_NAME', 'unknown')
        
        # Log who's initializing
        logger.warning(f"🎯 [PATTERN_INIT_DEBUG] Initializing in PID: {self.pid}, Process: {self.process_name}")
        
        # Log stack trace to see caller
        for line in traceback.format_stack()[-3:]:
            if 'pattern' in line.lower() or 'init' in line.lower():
                logger.debug(f"🎯 [CALLER] {line.strip()}")

        self.config = config or {}
        self.math_dispatcher = None
        
        # ✅ Pooled client (strictly enforced)
        self.redis_client = redis_client or get_redis_client(
            process_name="pattern_detector",
            db=1,
        )
        self.logger = logging.getLogger(__name__)
        
        # ✅ SINGLE SOURCE: Initialize MarketContext for cached VIX/thresholds (3-min TTL)
        self.market_context = MarketContext(redis_client=self.redis_client)
        self.logger.info("✅ MarketContext initialized (3-minute TTL cache)")
        
        # ✅ UNIFIED STORAGE: Initialize UnifiedDataStorage once and reuse for all pattern checks
        # Prevents connection pool exhaustion and ensures consistent data access
        self._unified_storage = None
        if redis_client:
            try:
                from shared_core.redis_clients.unified_data_storage import get_unified_storage
                self._unified_storage = get_unified_storage(redis_client=redis_client)
                self.logger.info("✅ UnifiedDataStorage initialized for pattern_detector (single instance, reused for all patterns)")
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to initialize UnifiedDataStorage: {e}")
                self._unified_storage = None
        
        # ✅ UNIFIED ALERT BUILDER: Initialize for enriching patterns with risk_metrics
        self.alert_builder = None
        if redis_client:
            try:
                from alerts.unified_alert_builder import get_unified_alert_builder
                self.alert_builder = get_unified_alert_builder(redis_client=redis_client)
                self.logger.info("✅ UnifiedAlertBuilder initialized for risk_metrics enrichment")
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to initialize UnifiedAlertBuilder: {e}")
                self.alert_builder = None
        
        # 🎯 Data source tracking initialization
        self.logger.info("🎯 PatternDetector initialized - setting up data source tracking")
        
        # Load pattern configuration
        self.pattern_config = self._load_pattern_config()
        
        # Initialize pattern tracking
        self.pattern_counts = defaultdict(int)
        self.pattern_success_rates = defaultdict(float)
        
        # Initialize stats tracking (base class stats)
        self.stats = {
            "invocations": 0,
            "patterns_found": 0,
            "errors": 0,
        }
        
        # All patterns use indicators.get('pattern_thresholds') from MarketContext
        
        # Load dynamic volume baselines (20d/55d)
        self.volume_baselines = self._load_volume_baselines()
        
        # Historical data tracking for Wyckoff patterns (Spring/Coil)
        self.price_history = {}  # symbol -> list of prices
        self.high_history = {}   # symbol -> list of highs  
        self.low_history = {}    # symbol -> list of lows
        self.volume_history = {} # symbol -> list of volumes
        
        # Volume profile data (required for _find_support_resistance)
        self.price_volume = {}  # price -> volume mapping for volume profile
        
        # Load sector correlation groups for position sizing
        self.correlation_groups = self._load_correlation_groups()
        
        # Initialize VIX utilities for threshold updates
        self.vix_utils = None
        try:
            from shared_core.utils.vix_utils import VIXUtils
            self.vix_utils = VIXUtils(redis_client)
        except Exception as e:
            self.logger.warning(f"Could not initialize VIX utilities: {e}")
        
        # ✅ REMOVED: base_thresholds - now using MarketContext for all thresholds
        # All thresholds come from MarketContext.get_context()['thresholds'] (cached, VIX-aware)
        
        # Initialize field mapping manager for standardized field names
        self.field_mapping_manager = get_field_mapping_manager()
        self.resolve_session_field = resolve_session_field
        self.resolve_indicator_field = resolve_indicator_field
        
        # ✅ CORE SERVICE: Initialize Delta Analyzer once
        if DELTA_ANALYZER_AVAILABLE and DeltaAnalyzer:
            try:
                self.delta_analyzer = DeltaAnalyzer(window=10)
                self.logger.info("✅ Delta Analyzer initialized")
            except Exception as e:
                self.logger.warning(f"⚠️ Delta Analyzer initialization failed: {e}")
                self.delta_analyzer = None
        else:
            self.delta_analyzer = None

        # ✅ FIX: Initialize detector attributes to None before _initialize_detector_components()
        # This ensures type checker knows these attributes exist and can be None or detector objects
        self.ict_detector: Optional[Any] = None
        self.ict_liquidity_detector: Optional[Any] = None
        self.ict_fvg_detector: Optional[Any] = None
        self.ict_ote_calculator: Optional[Any] = None
        self.ict_premium_discount_detector: Optional[Any] = None
        self.ict_killzone_detector: Optional[Any] = None
        # Advanced detectors
        self.order_flow_detector: Optional[Any] = None
        self.gamma_exposure_detector: Optional[Any] = None
        self.microstructure_detector: Optional[Any] = None
        self.cross_asset_detector: Optional[Any] = None
        self.vix_regime_detector: Optional[Any] = None

        # ✅ ORCHESTRATOR: Load pattern registry for metadata enrichment (Moved up for dependency resolution)
        self.pattern_registry = self._load_pattern_registry()
        if self.pattern_registry:
            self.logger.info(f"✅ Pattern registry loaded: {len(self.pattern_registry.get('pattern_configs', {}))} patterns")
        else:
            self.logger.warning("⚠️ Pattern registry not available")

        # Initialize detector stacks (ICT + advanced) and math dispatcher
        self._initialize_detector_components(redis_client)
        
        # ✅ CRITICAL FIX: Initialize VolumeProfileManager for dynamic threshold analysis
        try:
            from shared_core.dynamic_thresholds import VolumeProfileManager as SharedVolumeProfileManager
        except ImportError:
            SharedVolumeProfileManager = None

        VolumeProfileManagerImpl = SharedVolumeProfileManager

        if VolumeProfileManagerImpl is None:
            try:
                from intraday_trading.patterns.volume_profile_manager import VolumeProfileManager as LocalVolumeProfileManager
                VolumeProfileManagerImpl = LocalVolumeProfileManager
            except ImportError as e:
                self.logger.warning(f"⚠️ VolumeProfileManager not available: {e}")
                VolumeProfileManagerImpl = None

        if VolumeProfileManagerImpl:
            try:
                self.volume_profile_manager = VolumeProfileManagerImpl(
                    redis_client=redis_client,
                    token_resolver=None
                )
                self.logger.info("✅ VolumeProfileManager initialized for dynamic threshold analysis")
            except Exception as e:
                self.logger.warning(f"⚠️ VolumeProfileManager initialization failed: {e}")
                self.volume_profile_manager = None
        else:
            self.volume_profile_manager = None

        # Initialize alert_manager as optional (may be set by external code)
        self.alert_manager: Optional[Any] = None
        
        # ✅ Add dynamic threshold manager
        # Note: DynamicThresholdManager doesn't exist in shared_core.config_utils.thresholds
        # Using get_thresholds_for_regime instead for VIX-aware thresholds
        self.threshold_mgr = None
        
        # ✅ Add instrument config manager
        try:
            # ✅ REMOVED: config.instrument_config_manager - use thresholds.py get_instrument_config() instead
            # Instrument configs are now hardcoded in shared_core.config_utils.thresholds.INSTRUMENT_CONFIGS
            from shared_core.config_utils.thresholds import get_instrument_config
            self.get_instrument_config = get_instrument_config  # Store function reference
            self.instrument_config_manager = None  # Not needed - use get_instrument_config() directly
            self.logger.info("✅ Instrument config manager initialized")
        except Exception as e:
            self.logger.warning(f"⚠️ Instrument config manager not available: {e}")
            self.instrument_config_manager = None
        
        # ✅ REMOVED: Killzone configuration - killzone is no longer used by 8 core patterns
        # Killzone was disabled earlier - keeping minimal attributes for backward compatibility
        self.killzone_fallback_enabled: bool = False
        self.killzone_fallback_patterns: Set[str] = set()
        self.killzone_windows: List[Tuple[dt_time, dt_time]] = []
        self.killzone_respect_patterns: Set[str] = set()

        # ✅ FOCUSED: Core pattern definitions (8 patterns enabled)
        self.CORE_PATTERNS = {
            "order_flow_breakout": {
                "description": "Order flow imbalance breakout with price confirmation",
                "complexity": "high",
                "reliability": "high",
                "category": "advanced"
            },
            "gamma_exposure_reversal": {
                "description": "Gamma exposure reversal zones for options",
                "complexity": "high",
                "reliability": "high",
                "category": "advanced"
            },
            "microstructure_divergence": {
                "description": "Price vs microprice divergence and order flow absorption",
                "complexity": "high",
                "reliability": "medium",
                "category": "advanced"
            },
            "cross_asset_arbitrage": {
                "description": "USDINR-Gold arbitrage opportunities",
                "complexity": "high",
                "reliability": "high",
                "category": "advanced"
            },
            "vix_momentum": {
                "description": "VIX-regime momentum adaptation",
                "complexity": "medium",
                "reliability": "high",
                "category": "advanced"
            },
            "kow_signal_straddle": {
                "description": "VWAP-based Kow Signal Straddle strategy",
                "complexity": "high",
                "reliability": "high",
                "category": "strategy"
            },
            "ict_iron_condor": {
                "description": "ICT-based Iron Condor strategy",
                "complexity": "high",
                "reliability": "high",
                "category": "strategy"
            },
            "game_theory_signal": {
                "description": "Game Theory Signal - Nash equilibrium, FOMO, and Prisoner's Dilemma fusion",
                "complexity": "high",
                "reliability": "high",
                "category": "advanced"
            }
        }
        
        # ✅ REMOVED: VIX/threshold initialization - now using MarketContext
        # All VIX and threshold values come from MarketContext.get_context() (cached, 3-min TTL)
        # No need for self.vix_regime, self.vix_value, or self.MOMENTUM_THRESHOLD, etc.
        # All patterns use market_context.get_context()['vix_regime'] and ['thresholds']
        
        # Volume analysis
        self.volume_history = defaultdict(lambda: deque(maxlen=20))  # 20-tick rolling average
        self.price_history = defaultdict(lambda: deque(maxlen=20))
        
        # Pattern tracking
        self.pattern_counts = defaultdict(int)
        self.pattern_success_rates = defaultdict(float)
        
        # Statistics
        self.stats = {
            "total_detections": 0,
            "successful_detections": 0,
            "failed_detections": 0,
            "patterns_found": 0,
            "invocations": 0,
            # ✅ REMOVED: volume_spikes stat - legacy pattern not part of core 8 patterns
            "momentum_signals": 0,
            "breakout_signals": 0,
            "reversal_signals": 0,
            "spring_signals": 0,
            "coil_signals": 0,
            "hidden_accumulation": 0,
            "pattern_counts": defaultdict(int)
        }
        
        # Initialize unified volume manager
        self.volume_manager = get_volume_computation_manager(redis_client=self.redis_client)
        # Backward compatibility: correct_volume_calculator points to volume_manager
        self.correct_volume_calculator = self.volume_manager
        # ✅ REMOVED: UnifiedTimeAwareBaseline dependency - not needed for 6 of 8 core patterns
        # Only kow_signal_straddle and ict_iron_condor need baselines, and they handle it directly
        self.time_aware_baseline = None  # Deprecated - removed to prevent pattern detection failures
        
        
        # Mathematical volume engine
        # ✅ FIXED: UnifiedTimeAwareBaseline requires symbol, so we pass redis_client only
        # VolumeThresholdCalculator will create UnifiedTimeAwareBaseline per-symbol when needed
        try:
            # Don't import UnifiedTimeAwareBaseline here - VolumeThresholdCalculator handles it internally
            self.mathematical_calculator = VolumeThresholdCalculator(redis_client=self.redis_client)
            self.logger.info("✅ Mathematical volume engine initialized")
        except Exception as e:
            self.logger.warning(f"Failed to initialize Mathematical Calculator: {e}")
            self.mathematical_calculator = None
        
        # Volume Manager for Volume Profile Integration (already initialized above)
        # Use the unified volume_manager for all volume operations
        self.volume_state_manager = self.volume_manager  # Backward compatibility alias
        self.logger.info("✅ Volume Manager initialized for Volume Profile patterns")
        
        # Volume Profile Detector for Advanced Pattern Detection
        if VOLUME_PROFILE_DETECTOR_AVAILABLE and VolumeProfileDetector:
            try:
                self.volume_profile_detector = VolumeProfileDetector(self.redis_client)
                self.logger.info("✅ Volume Profile Detector initialized for advanced pattern detection")
            except Exception as e:
                self.logger.warning(f"Failed to initialize Volume Profile Detector: {e}")
                self.volume_profile_detector = None
        else:
            self.volume_profile_detector = None
        
        # ✅ UPDATED: VolumeThresholdCalculator now uses UnifiedTimeAwareBaseline via redis_client
        try:
            # Get DB1 client from volume_manager for UnifiedTimeAwareBaseline
            db1_client = self.volume_manager._baseline_db1_client if hasattr(self.volume_manager, '_baseline_db1_client') else None
            if not db1_client:
                # Fallback: try to get from redis_client wrapper
                db1_client = self.redis_client.get_client(1) if hasattr(self.redis_client, 'get_client') and self.redis_client else None
            self.threshold_calculator = VolumeThresholdCalculator(redis_client=db1_client) if db1_client else None
        except Exception as e:
            self.logger.warning(f"Failed to initialize VolumeThresholdCalculator: {e}")
            self.threshold_calculator = None
        
        # Unified pattern registry - Pattern detection method registry
        # Note: This registry is for legacy/backward compatibility
        # The actual detection flow uses _detect_advanced_patterns_with_data(), _detect_straddle_patterns_with_data(), etc.
        self.all_pattern_detectors = []
        
        # Add basic patterns (existing methods)
        # Note: _detect_volume_patterns doesn't exist - only _detect_volume_patterns_with_data exists
        
        # ICT patterns are integrated directly into PatternDetector
        # Using centralized methods only - no external imports
        # All ICT logic uses self._get_current_killzone(), PatternMathematics, etc.
        
        # Add Straddle Strategy (optional legacy straddle strategy)
        if KOW_SIGNAL_ENABLED and KOW_SIGNAL_AVAILABLE and KowSignalStraddleStrategy:
            try:
                self.logger.info("🔧 [PATTERN_INIT] Initializing kow_signal_straddle detector...")
                # Initialize straddle strategy with configuration from thresholds.py
                self.straddle_strategy = KowSignalStraddleStrategy(redis_client=self.redis_client)
                self.logger.info("✅ [PATTERN_INIT] kow_signal_straddle detector initialized")
                
                # Add straddle patterns to registry
                detect_method = getattr(self, '_detect_straddle_patterns', None)
                if callable(detect_method):
                    self.all_pattern_detectors.append(detect_method)
                
                self.logger.info("✅ Straddle strategy initialized and added to pattern registry")
            except Exception as e:
                self.logger.warning(f"Straddle strategy initialization failed: {e}")
                self.straddle_strategy = None
        else:
            self.straddle_strategy = None
            if not KOW_SIGNAL_ENABLED:
                self.logger.info("⏸️ [PATTERN_INIT] kow_signal_straddle disabled via feature flag")
            else:
                self.logger.info("⏸️ [PATTERN_INIT] kow_signal_straddle strategy unavailable")

        # Initialize math dispatcher if available
        if MATH_DISPATCHER_AVAILABLE and MathDispatcher is not None and PATTERN_MATHEMATICS_AVAILABLE and PatternMathematics is not None:
            try:
                self.math_dispatcher = MathDispatcher(PatternMathematics, None)
            except Exception as e:
                self.logger.warning(f"⚠️ MathDispatcher initialization failed: {e}")
                self.math_dispatcher = None
        else:
            self.math_dispatcher = None

        
        # ✅ EMERGENCY HOTFIX: Initialize data quality enforcer (defined at top of this file)
        self.data_quality_enforcer = DataQualityEnforcer(logger=self.logger)
        self.logger.info("✅ DataQualityEnforcer initialized (defense-in-depth validation)")
        
        self.detector_methods = self.all_pattern_detectors
        # Patterns will be registered via _initialize_patterns() -> _register_patterns_once()
        self.detector_methods = self.all_pattern_detectors
        # ✅ FIX: Use actual class name (PatternDetector) instead of misleading "SimplifiedPatternDetector"
        # Log accurate initialization status
        pattern_count = len(self.all_pattern_detectors)
        components = [f"{len(self.option_pattern_handlers)} option handlers"]
        if ICT_MODULES_AVAILABLE:
            components.append("ICT patterns")
        if hasattr(self, 'delta_analyzer') and self.delta_analyzer:
            components.append("Option patterns")
        components_str = ", ".join(components)
        self.logger.info(f"🎯 PatternDetector initialized with {components_str} ({pattern_count} detector methods)")
        # NOTE: self._initialized = True is now set at the BEGINNING (line 1167) inside thread lock
        self._initialize_patterns()
    
    # detector_methods now shared with all_pattern_detectors
    
    def get_available_handlers(self) -> List[str]:
        """Return all known option pattern handler names."""
        if hasattr(self, "_all_option_handlers"):
            return list(self._all_option_handlers.keys())
        return list(self.option_pattern_handlers.keys())

    def enable_handler(self, handler_name: str, enabled: bool = True) -> bool:
        """
        Enable or disable a handler dynamically without rebuilding the detector.
        Returns True if a state change occurred or handler already in desired state.
        """
        if not hasattr(self, "_all_option_handlers"):
            self._all_option_handlers = self.option_pattern_handlers.copy()
        handler = self._all_option_handlers.get(handler_name)
        if enabled:
            if not handler:
                self.logger.warning("⚠️ Handler %s unavailable; cannot enable", handler_name)
                return False
            self.option_pattern_handlers[handler_name] = handler
            return True
        # Disable path
        removed = self.option_pattern_handlers.pop(handler_name, None) is not None
        if removed:
            self.logger.info("⏸️ [HANDLER] %s disabled via enable_handler()", handler_name)
        return removed

    def _initialize_patterns(self) -> None:
        """Unified entrypoint to register detectors and handlers exactly once."""
        
        # Check flag early (fast path)
        if getattr(self, "initialized", False):
            logger.debug("🔍 _initialize_patterns: already initialized (self.initialized=True)")
            return
        
        # Thread-safe pattern registration
        if not hasattr(self.__class__, '_pattern_registration_lock'):
            import threading
            self.__class__._pattern_registration_lock = threading.Lock()
        
        with self.__class__._pattern_registration_lock:
            # Double-check inside lock
            if getattr(self, "initialized", False):
                return
            
            self.logger.info(
                "🎯 [PATTERN_INIT_START] Worker: %s, Type: %s, PID: %s",
                self.worker_id,
                self.process_type,
                os.getpid(),
            )
            self._register_patterns_once()
            self.initialized = True

    def _register_patterns_once(self) -> None:
        """Register patterns exactly once using CLASS-LEVEL storage."""
        
        # Check class-level flag (not instance-level)
        if getattr(self.__class__, '_patterns_registered', False):
            logger.debug("🔄 Patterns already registered at class level")
            
            # COPY from class attributes to instance
            if hasattr(self.__class__, '_shared_detector_methods'):
                self.detector_methods = self.__class__._shared_detector_methods.copy()
                self.all_pattern_detectors = self.__class__._shared_detector_methods.copy()
            if hasattr(self.__class__, '_shared_option_handlers'):
                self.option_pattern_handlers = self.__class__._shared_option_handlers.copy()
            
            return
        
        # Initialize instance storage first
        self.option_pattern_handlers = {}
        self.core_detectors = {}
        
        # Register patterns
        self._register_core_patterns()
        self._register_option_handlers()
        
        # Store to CLASS-LEVEL for sharing across instances
        self.__class__._shared_detector_methods = list(self.option_pattern_handlers.values())
        self.__class__._shared_option_handlers = self.option_pattern_handlers.copy()
        
        # Copy to instance
        self.detector_methods = self.__class__._shared_detector_methods.copy()
        self.all_pattern_detectors = self.__class__._shared_detector_methods.copy()
        
        # Set class-level flag
        self.__class__._patterns_registered = True
        
        logger.info(f"✅ [PATTERN_REGISTRATION_COMPLETE] Core: {len(self.core_detectors)} methods, Options: {len(self.option_pattern_handlers)} handlers")

    def _register_core_patterns(self) -> None:
        """Register all core detection methods that follow the detect_* convention."""
        for method_name in dir(self):
            if not method_name.startswith('detect_'):
                continue
            method = getattr(self, method_name, None)
            if callable(method):
                pattern_name = method_name.replace('detect_', '', 1)
                self.core_detectors[pattern_name] = method
        # Logging moved to _register_patterns_once()

    def _register_option_handlers(self) -> None:
        """Register option pattern handlers without disabling any by default."""
        handlers = {
            'vix_momentum': getattr(self, '_handle_vix_momentum', None),
            'order_flow_breakout': getattr(self, '_handle_order_flow_breakout', None),
            'gamma_exposure_reversal': getattr(self, '_handle_gamma_exposure_reversal', None),
            'microstructure_divergence': getattr(self, '_handle_microstructure_divergence', None),
            'cross_asset_arbitrage': getattr(self, '_handle_cross_asset_arbitrage', None),
            'ict_iron_condor': getattr(self, '_handle_ict_iron_condor_option', None),
            'game_theory_signal': getattr(self, '_handle_game_theory_signal', None),
        }
        if KOW_SIGNAL_ENABLED:
            handlers['kow_signal_straddle'] = getattr(self, '_handle_kow_signal_straddle', None)
        available = {name: handler for name, handler in handlers.items() if callable(handler)}
        self.option_pattern_handlers = available
        self._all_option_handlers = available.copy()
        # Logging moved to _register_patterns_once()

    def _initialize_detector_components(self, redis_client) -> None:
        """
        Initialize advanced pattern detectors with proper config injection.
        
        Each detector receives its pattern-specific config from pattern_registry,
        allowing access to thresholds like deadband_threshold, base_confidence, etc.
        
        Patterns initialized here:
        - 7 advanced detectors: vix_momentum, order_flow_breakout, gamma_exposure_reversal,
          microstructure_divergence, cross_asset_arbitrage, ict_iron_condor, game_theory_signal
        
        Note: ict_iron_condor and kow_signal_straddle are strategy-level patterns
        initialized separately (kow is currently DISABLED for testing).
        """
        # Get pattern-specific configs from registry
        pattern_configs = self.pattern_registry.get('pattern_configs', {}) if self.pattern_registry else {}
        
        # Initialize 5 advanced detectors
        if ADVANCED_DETECTORS_AVAILABLE:
            try:
                self.logger.info("🔧 [PATTERN_INIT] Initializing advanced pattern detectors...")
                
                self.order_flow_detector = OrderFlowBreakoutDetector(
                    pattern_configs.get('order_flow_breakout', {}),
                    redis_client
                ) if OrderFlowBreakoutDetector else None
                self.logger.info(f"   ✅ order_flow_breakout: {'initialized' if self.order_flow_detector else 'FAILED'}")
                
                self.gamma_exposure_detector = GammaExposureReversalDetector(
                    pattern_configs.get('gamma_exposure_reversal', {}),
                    redis_client
                ) if GammaExposureReversalDetector else None
                self.logger.info(f"   ✅ gamma_exposure_reversal: {'initialized' if self.gamma_exposure_detector else 'FAILED'}")
                
                self.microstructure_detector = MicrostructureDivergenceDetector(
                    pattern_configs.get('microstructure_divergence', {}),
                    redis_client
                ) if MicrostructureDivergenceDetector else None
                self.logger.info(f"   ✅ microstructure_divergence: {'initialized' if self.microstructure_detector else 'FAILED'}")
                
                self.cross_asset_detector = CrossAssetArbitrageDetector(
                    pattern_configs.get('cross_asset_arbitrage', {}),
                    redis_client
                ) if CrossAssetArbitrageDetector else None
                self.logger.info(f"   ✅ cross_asset_arbitrage: {'initialized' if self.cross_asset_detector else 'FAILED'}")
                
                self.vix_regime_detector = VIXRegimeMomentumDetector(
                    pattern_configs.get('vix_momentum', {}),
                    redis_client
                ) if VIXRegimeMomentumDetector else None
                self.logger.info(f"   ✅ vix_momentum: {'initialized' if self.vix_regime_detector else 'FAILED'}")
                
                self.logger.info("   ℹ️ ict_iron_condor: inline handler (no separate detector)")
                self.logger.info("   ℹ️ game_theory_signal: inline handler (no separate detector)")
                
                self.logger.info("✅ Advanced detectors initialized with config injection")
                
                # Inject math dispatcher for numerical computations
                if self.math_dispatcher:
                    for detector_obj in [
                        self.order_flow_detector,
                        self.gamma_exposure_detector,
                        self.microstructure_detector,
                        self.cross_asset_detector,
                        self.vix_regime_detector,
                    ]:
                        if detector_obj and hasattr(detector_obj, "math_dispatcher"):
                            detector_obj.math_dispatcher = self.math_dispatcher
                            
            except Exception as adv_error:
                self.logger.error(f"❌ [PATTERN_INIT] Failed to initialize advanced detectors: {adv_error}", exc_info=True)
                self.order_flow_detector = None
                self.gamma_exposure_detector = None
                self.microstructure_detector = None
                self.cross_asset_detector = None
                self.vix_regime_detector = None
        else:
            self.logger.warning("⚠️ [PATTERN_INIT] ADVANCED_DETECTORS_AVAILABLE=False - advanced detectors disabled")
        
        # Initialize game_theory_signal detector (separate from advanced.py detectors)
        if GAME_THEORY_DETECTOR_AVAILABLE and GameTheoryPatternDetector:
            try:
                self.logger.info("🔧 [PATTERN_INIT] Initializing game_theory_signal detector...")
                # Pass self as data_source so detector can access get_microstructure_data
                self.game_theory_detector = GameTheoryPatternDetector(
                    pattern_configs.get('game_theory_signal', {}),
                    redis_client,
                    data_source=self
                )
                self.logger.info("✅ [PATTERN_INIT] game_theory_signal detector initialized with data_source")
            except Exception as gt_error:
                self.logger.error(f"❌ [PATTERN_INIT] Failed to initialize Game Theory detector: {gt_error}", exc_info=True)
                self.game_theory_detector = None
        else:
            self.logger.warning(
                f"⚠️ [PATTERN_INIT] Game theory detector unavailable "
                f"(GAME_THEORY_DETECTOR_AVAILABLE={GAME_THEORY_DETECTOR_AVAILABLE})"
            )
            self.game_theory_detector = None
        
        self.logger.info("✅ [PATTERN_INIT] Detector components initialization complete (6 detectors + 1 strategy)")
    
    def _load_pattern_registry(self) -> Optional[Dict[str, Any]]:
        """
        Load pattern registry from data/pattern_registry_config.json.
        
        Returns:
            Pattern registry dict with pattern_configs and metadata, or None if not found.
        """
        try:
            registry_path = Path(__file__).parent / "data" / "pattern_registry_config.json"
            if registry_path.exists():
                with open(registry_path, 'r') as f:
                    return json.load(f)
            else:
                self.logger.warning(f"⚠️ Pattern registry not found: {registry_path}")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to load pattern registry: {e}")
        return {}

    # Add these methods to your existing PatternDetector class in pattern_detector.py


    def should_detect_patterns(self, symbol: str, tick_data: Dict[str, Any]) -> bool:
        """
        Check if we should attempt pattern detection.
        
        Filters out:
        - Symbols with no volume activity (volume_ratio <= 0)
        - Very deep ITM/OTM options (delta < 0.05 or > 0.95) - only if we have delta data
        - Symbols with insufficient tick data (< 5 ticks in session)
        
        Returns:
            bool: True if patterns should be detected, False to skip
        """
        # Skip if no volume activity
        volume_ratio = float(tick_data.get('volume_ratio', 0.0) or 0.0)
        if volume_ratio <= 0.0:
            # Allow index underlyings (NIFTY/BANKNIFTY/etc.) to bypass the volume gate
            symbol_upper = (symbol or "").upper()
            if not (
                symbol_upper.startswith("NSE")
                or symbol_upper.startswith("BSE")
                or symbol_upper.startswith("NFO")
            ):
                self.logger.debug(f"⚠️ [PATTERN_SKIP] {symbol} - No volume activity (ratio={volume_ratio})")
                return False
        
        greeks = tick_data.get('greeks', {}) or {}
        delta_raw = tick_data.get('delta', greeks.get('delta'))
        iv_raw = (
            tick_data.get('iv')
            or tick_data.get('implied_volatility')
            or greeks.get('iv')
            or greeks.get('implied_volatility')
        )
        try:
            iv = float(iv_raw) if iv_raw is not None else None
        except (TypeError, ValueError):
            iv = None
        
        normalized_delta: Optional[float] = None
        option_type = None
        symbol_upper = (symbol or "").upper()
        if delta_raw is not None:
            try:
                normalized_delta = float(delta_raw)
            except (TypeError, ValueError):
                normalized_delta = None
        normalized_delta = preprocess_delta_for_patterns(symbol, normalized_delta, self.logger) if normalized_delta is not None else None
        suffix_option_type = identify_option_type(symbol)
        if "PE" in symbol_upper and "CE" not in symbol_upper:
            option_type = "put"
        elif "CE" in symbol_upper and "PE" not in symbol_upper:
            option_type = "call"
        elif normalized_delta is not None:
            option_type = "put" if normalized_delta < 0 else "call"
        if suffix_option_type:
            option_type = 'put' if suffix_option_type == 'PE' else 'call'
        # ✅ REMOVED: Delta range check emergency patch - advanced.py handles delta validation
        # Delta validation for options is now done in individual pattern detectors
        
        if iv is not None and iv <= 0.01:
            self.logger.debug(f"⚠️ [PATTERN_SKIP] {symbol} - Very low IV ({iv:.4f})")
            return False
        
        session_key = f"session:{symbol}:window"
        session_data = None
        client = getattr(self, 'redis_client', None)
        if client and hasattr(client, 'get'):
            try:
                raw = client.get(session_key)
                if isinstance(raw, bytes):
                    raw = raw.decode('utf-8')
                if raw:
                    session_data = json.loads(raw)
            except Exception:
                session_data = None
        if isinstance(session_data, dict):
            tick_count = session_data.get('tick_count', 0)
            if tick_count < 10:
                self.logger.debug(f"⚠️ [PATTERN_SKIP] {symbol} - Insufficient data ({tick_count} ticks)")
                return False
        
        return True

    def _log_pattern_handler_invocation(self, handler_name: str, symbol: str, payload: Dict[str, Any]) -> None:
        if not PATTERN_DEBUG_ENABLED:
            return
        snapshot = _pattern_field_snapshot(payload)
        self.logger.info(
            "🎛️ [PATTERN_ENTER] %s handler=%s fields=%s",
            symbol,
            handler_name,
            snapshot,
        )

    def _log_pattern_handler_result(self, handler_name: str, symbol: str, result: Any, payload: Dict[str, Any]) -> None:
        if not PATTERN_DEBUG_ENABLED:
            return
        snapshot = _pattern_field_snapshot(payload)
        if not result:
            self.logger.info(
                "⏭️ [PATTERN_EXIT] %s handler=%s skipped fields=%s",
                symbol,
                handler_name,
                snapshot,
            )
            return
        if isinstance(result, list):
            patterns = [item.get('pattern', handler_name) for item in result if isinstance(item, dict)]
            self.logger.info(
                "🎯 [PATTERN_EXIT] %s handler=%s triggered_patterns=%s fields=%s",
                symbol,
                handler_name,
                patterns or ['unknown'],
                snapshot,
            )
            return
        pattern_name = result.get('pattern') if isinstance(result, dict) else handler_name
        confidence = result.get('confidence') if isinstance(result, dict) else None
        self.logger.info(
            "🎯 [PATTERN_EXIT] %s handler=%s pattern=%s confidence=%s fields=%s",
            symbol,
            handler_name,
            pattern_name or handler_name,
            confidence,
            snapshot,
        )

    def detect_patterns(self, indicators, use_pipelining=True, tick_data=None):
        """Main entry point for pattern detection."""
        all_alerts = []
        data = tick_data if tick_data else indicators
        symbol = data.get('symbol')
        greeks = data.get('greeks', {})
        if not symbol: return []

        _ = use_pipelining

        if PATTERN_DEBUG_ENABLED:
            log_option_details(self.logger, symbol, greeks or {}, data)

        # ✅ NEW: Skip detection for illiquid/expired/no-volume symbols
        if not self.should_detect_patterns(symbol, data):
            return []

        evaluated_patterns: List[str] = []
        triggered_patterns: List[str] = []

        if hasattr(self, 'option_pattern_handlers') and self.option_pattern_handlers:
            for name, handler in self.option_pattern_handlers.items():
                evaluated_patterns.append(name)
                try:
                    # Handlers define: (self, symbol, indicators, greeks, tick_data)
                    self._log_pattern_handler_invocation(name, symbol, indicators)
                    result = handler(symbol, indicators, greeks, data)
                    self._log_pattern_handler_result(name, symbol, result, indicators)
                    if result:
                        if isinstance(result, list):
                            all_alerts.extend(result)
                            if PATTERN_DEBUG_ENABLED:
                                for alert in result:
                                    pattern_name = alert.get('pattern') if isinstance(alert, dict) else name
                                    triggered_patterns.append(pattern_name or name)
                                    self.logger.debug(
                                        f"✅ [PATTERN_HIT] {symbol} - {pattern_name or name}"
                                    )
                        else:
                            all_alerts.append(result)
                            pattern_name = result.get('pattern') if isinstance(result, dict) else name
                            triggered_patterns.append(pattern_name or name)
                            if PATTERN_DEBUG_ENABLED:
                                self.logger.debug(
                                    f"✅ [PATTERN_HIT] {symbol} - {pattern_name or name}"
                                )
                    elif PATTERN_DEBUG_ENABLED:
                        self.logger.debug(f"⚠️ [PATTERN_SKIP_DETAIL] {symbol} - {name} produced no signal")
                except Exception as e:
                    if PATTERN_DEBUG_ENABLED:
                        self.logger.debug(f"❌ [PATTERN_ERROR] {symbol} - {name} failed: {e}")
                    continue

        if PATTERN_DEBUG_ENABLED and evaluated_patterns:
            skipped = [p for p in evaluated_patterns if p not in triggered_patterns]
            self.logger.debug(
                f"📊 [PATTERN_SUMMARY] {symbol} - evaluated={evaluated_patterns}, "
                f"triggered={triggered_patterns or ['NONE']} skipped={skipped or ['NONE']}"
            )

        return all_alerts

    def _merge_pattern_inputs(
        self,
        symbol: str,
        indicators: Optional[Dict[str, Any]],
        greeks: Optional[Dict[str, Any]],
        tick_data: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Combine indicator, greek, and tick payloads for detector delegation."""
        payload: Dict[str, Any] = {}
        for source in (tick_data, indicators, greeks):
            if isinstance(source, dict):
                payload.update(source)
        payload.setdefault("symbol", symbol)
        return payload

    def _delegate_detector(
        self,
        detector: Optional[Any],
        detector_name: str,
        symbol: str,
        indicators: Dict[str, Any],
        greeks: Dict[str, Any],
        tick_data: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Delegate to advanced detector implementations with consistent logging."""
        if not detector:
            return None
        payload = self._merge_pattern_inputs(symbol, indicators, greeks, tick_data)
        debug_trace = DEBUG_TRACE_ENABLED
        if debug_trace:
            payload_keys = [key for key in payload.keys() if key not in ('symbol', 'tradingsymbol')]
            self.logger.debug(
                "⚙️ [%s] %s → detect() with %s fields (sample: %s)",
                detector_name,
                symbol,
                len(payload_keys),
                payload_keys[:8],
            )
        try:
            result = detector.detect(symbol, payload)
            if debug_trace and result:
                self.logger.info(
                    "✅ [%s] %s → PATTERN HIT | signal=%s | conf=%s",
                    detector_name,
                    symbol,
                    result.get('signal'),
                    f"{result.get('confidence'):.3f}" if isinstance(result.get('confidence'), (int, float)) else result.get('confidence'),
                )
            elif debug_trace and not result:
                self.logger.debug(f"⏭️ [{detector_name}] {symbol} → no signal")
        except Exception as exc:
            if debug_trace:
                self.logger.exception(f"🚨 [{detector_name}] {symbol} → CRASH: {exc}")
            else:
                self.logger.debug(f"⚠️ [{detector_name}] {symbol} detector error: {exc}")
            return None

        if result:
            if PATTERN_DEBUG_ENABLED:
                self.logger.debug(
                    f"✅ [PATTERN_DETECT] {symbol} - {detector_name} triggered (confidence={result.get('confidence')})"
                )
            return result

        if PATTERN_DEBUG_ENABLED:
            debug_fields = PATTERN_DEBUG_FIELDS.get(detector_name, [])
            snapshot = {field: payload.get(field) for field in debug_fields}
            self.logger.debug(
                f"⚠️ [PATTERN_SKIP_DETAIL] {symbol} - {detector_name} returned None with inputs {snapshot}"
            )
        return None

    def _handle_kow_signal_straddle(self, symbol: str, indicators: Dict, greeks: Dict, tick_data: Dict) -> Optional[Dict]:
        """Delegate to Kow Signal straddle strategy if available."""
        if not self.straddle_strategy:
            self.logger.debug("⏸️ [KOW_SIGNAL] Strategy unavailable for %s", symbol)
            return None
        payload: Dict[str, Any] = {}
        if isinstance(tick_data, dict):
            payload.update(tick_data)
        elif isinstance(indicators, dict):
            payload.update(indicators)
        payload.setdefault('symbol', symbol)
        try:
            coroutine = self.straddle_strategy.on_tick(payload, send_alert=False)
            if asyncio.iscoroutine(coroutine):
                return self._run_async_task(coroutine)
            return coroutine
        except Exception as exc:
            self.logger.debug("⚠️ [KOW_SIGNAL] Failed to process %s: %s", symbol, exc)
            return None

    def _handle_vix_momentum(self, symbol: str, indicators: Dict, greeks: Dict, tick_data: Dict) -> Optional[Dict]:
        """Delegate to VIXRegimeMomentumDetector for VIX momentum pattern detection."""
        return self._delegate_detector(
            self.vix_regime_detector,
            "vix_momentum",
            symbol,
            indicators,
            greeks,
            tick_data,
        )
    
    def _handle_order_flow_breakout(self, symbol: str, indicators: Dict, greeks: Dict, tick_data: Dict) -> Optional[Dict]:
        """Delegate to OrderFlowBreakoutDetector for order flow breakout pattern detection."""
        return self._delegate_detector(
            self.order_flow_detector,
            "order_flow_breakout",
            symbol,
            indicators,
            greeks,
            tick_data,
        )
    
    def _handle_gamma_exposure_reversal(self, symbol: str, indicators: Dict, greeks: Dict, tick_data: Dict) -> Optional[Dict]:
        """Delegate to GammaExposureReversalDetector for gamma exposure reversal pattern detection."""
        return self._delegate_detector(
            self.gamma_exposure_detector,
            "gamma_exposure_reversal",
            symbol,
            indicators,
            greeks,
            tick_data,
        )
    
    def _handle_microstructure_divergence(self, symbol: str, indicators: Dict, greeks: Dict, tick_data: Dict) -> Optional[Dict]:
        """Delegate to MicrostructureDivergenceDetector for microstructure divergence pattern detection."""
        return self._delegate_detector(
            self.microstructure_detector,
            "microstructure_divergence",
            symbol,
            indicators,
            greeks,
            tick_data,
        )
    
    def _handle_cross_asset_arbitrage(self, symbol: str, indicators: Dict, greeks: Dict, tick_data: Dict) -> Optional[Dict]:
        """Delegate to CrossAssetArbitrageDetector for cross-asset arbitrage pattern detection."""
        return self._delegate_detector(
            self.cross_asset_detector,
            "cross_asset_arbitrage",
            symbol,
            indicators,
            greeks,
            tick_data,
        )
    

    def _handle_ict_iron_condor_option(self, symbol: str, indicators: Dict, greeks: Dict, tick_data: Dict) -> Optional[Dict]:
        """
        ICT Iron Condor Pattern
        Detects ideal conditions for iron condor strategies based on ICT methodology
        """
        try:
            # Check if this is an option
            if not self._is_option_symbol(symbol):
                return None
            
            # Get ICT-relevant metrics
            option_type = tick_data.get('option_type', '').lower()
            strike_price = tick_data.get('strike_price', 0)
            underlying_price = tick_data.get('underlying_price', tick_data.get('spot_price', 0))
            expiry_date = tick_data.get('expiry_date')
            
            if not all([option_type, strike_price, underlying_price, expiry_date]):
                return None
            
            # Calculate moneyness
            moneyness = underlying_price / strike_price if strike_price > 0 else 1.0
            
            # Get Greeks
            delta = greeks.get('delta', 0)
            theta = greeks.get('theta', 0)
            vega = greeks.get('vega', 0)
            iv = greeks.get('implied_volatility', greeks.get('iv', 0))
            
            # ICT Iron Condor conditions:
            # 1. Low delta (0.10-0.30 for calls, -0.30 to -0.10 for puts)
            # 2. Positive theta (time decay)
            # 3. Low vega (low sensitivity to volatility)
            # 4. High IV rank (selling premium)
            
            # Check delta range for iron condor wings
            if option_type == 'call':
                delta_condition = 0.10 <= delta <= 0.30
            else:  # put
                delta_condition = -0.30 <= delta <= -0.10
            
            # Theta should be positive (selling options)
            theta_condition = theta > 0
            
            # Vega should be relatively low (low volatility sensitivity)
            vega_condition = abs(vega) < 0.15
            
            # Time to expiry (ideal 30-45 days for iron condor)
            try:
                from datetime import datetime
                expiry = datetime.strptime(expiry_date, '%Y-%m-%d')
                days_to_expiry = (expiry - datetime.now()).days
                dte_condition = 30 <= days_to_expiry <= 45
            except:
                dte_condition = False
            
            # All conditions met
            if delta_condition and theta_condition and vega_condition and dte_condition:
                # Calculate confidence based on how ideal conditions are
                delta_score = 1.0 - abs(delta - 0.20) if option_type == 'call' else 1.0 - abs(abs(delta) - 0.20)
                theta_score = min(1.0, theta / 0.05)  # Normalize theta
                vega_score = 1.0 - min(1.0, abs(vega) / 0.15)
                dte_score = 1.0 - abs(days_to_expiry - 37.5) / 15  # Center at 37.5 days
                
                confidence = (delta_score * 0.3 + theta_score * 0.3 + vega_score * 0.2 + dte_score * 0.2) * 0.9
                
                return {
                    'pattern': 'ict_iron_condor',
                    'type': f'{option_type}_wing',
                    'confidence': confidence,
                    'delta': delta,
                    'theta': theta,
                    'vega': vega,
                    'iv': iv,
                    'days_to_expiry': days_to_expiry,
                    'moneyness': moneyness,
                    'signal': f'Ideal {option_type} wing for iron condor (delta: {delta:.2f}, theta: {theta:.4f})'
                }
            
            # Partial match (good for adjustment or legging in)
            conditions_met = sum([delta_condition, theta_condition, vega_condition, dte_condition])
            if conditions_met >= 3:
                confidence = conditions_met / 4 * 0.7
                
                return {
                    'pattern': 'ict_iron_condor',
                    'type': f'partial_{option_type}',
                    'confidence': confidence,
                    'delta': delta,
                    'theta': theta,
                    'vega': vega,
                    'iv': iv,
                    'conditions_met': conditions_met,
                    'signal': f'Partial iron condor conditions ({conditions_met}/4 met)'
                }
        
        except Exception as e:
            self.logger.debug(f"ICT iron condor pattern error: {e}")
        
        return None
    
    def _handle_game_theory_signal(self, symbol: str, indicators: Dict, greeks: Dict, tick_data: Dict) -> Optional[Dict]:
        """
        Game Theory Signal Pattern
        Detects Nash equilibrium and game theory patterns in order book
        """
        try:
            # Get order book and microstructure data
            order_book_imbalance = indicators.get('order_book_imbalance', 0)
            depth_imbalance = indicators.get('depth_imbalance', 0)
            best_bid_size = indicators.get('best_bid_size', 0)
            best_ask_size = indicators.get('best_ask_size', 0)
            total_bid_depth = indicators.get('total_bid_depth', 0)
            total_ask_depth = indicators.get('total_ask_depth', 0)
            
            if not all([best_bid_size, best_ask_size, total_bid_depth, total_ask_depth]):
                return None
            
            # Calculate game theory metrics
            bid_ask_ratio = best_bid_size / best_ask_size if best_ask_size > 0 else 1.0
            total_depth_ratio = total_bid_depth / total_ask_depth if total_ask_depth > 0 else 1.0
            
            # Nash equilibrium detection
            # When both sides have similar strength, market is in equilibrium
            equilibrium_threshold = 0.1  # 10% difference
            
            # Check if market is in Nash equilibrium
            in_equilibrium = (
                abs(bid_ask_ratio - 1.0) < equilibrium_threshold and
                abs(total_depth_ratio - 1.0) < equilibrium_threshold and
                abs(order_book_imbalance) < 0.2 and
                abs(depth_imbalance) < 0.2
            )
            
            # Check for prisoner's dilemma (both sides trapped)
            # High volume on both sides with small spread
            spread_bps = indicators.get('spread_bps', 0)
            volume_ratio = indicators.get('volume_ratio', 1.0)
            
            prisoners_dilemma = (
                best_bid_size > 1000 and best_ask_size > 1000 and  # High volume both sides
                spread_bps < 2.0 and  # Tight spread
                volume_ratio > 1.5  # High volume
            )
            
            # Dominant strategy detection
            # One side clearly dominating
            dominant_bid = bid_ask_ratio > 1.5 and total_depth_ratio > 1.5
            dominant_ask = bid_ask_ratio < 0.67 and total_depth_ratio < 0.67
            
            # Pattern detection
            if prisoners_dilemma:
                # Market in prisoner's dilemma - both sides trapped
                confidence = min(0.95, (min(best_bid_size, best_ask_size) / 2000) * 0.7)
                
                return {
                    'pattern': 'game_theory_signal',
                    'type': 'prisoners_dilemma',
                    'confidence': confidence,
                    'bid_ask_ratio': bid_ask_ratio,
                    'total_depth_ratio': total_depth_ratio,
                    'best_bid_size': best_bid_size,
                    'best_ask_size': best_ask_size,
                    'spread_bps': spread_bps,
                    'signal': 'Prisoner\'s dilemma - both sides trapped with high volume'
                }
            
            elif in_equilibrium:
                # Market in Nash equilibrium
                confidence = min(0.90, (1.0 - max(abs(bid_ask_ratio-1), abs(total_depth_ratio-1))) * 0.8)
                
                return {
                    'pattern': 'game_theory_signal',
                    'type': 'nash_equilibrium',
                    'confidence': confidence,
                    'order_book_imbalance': order_book_imbalance,
                    'depth_imbalance': depth_imbalance,
                    'bid_ask_ratio': bid_ask_ratio,
                    'total_depth_ratio': total_depth_ratio,
                    'signal': 'Nash equilibrium - balanced order book'
                }
            
            elif dominant_bid:
                # Dominant bid strategy
                confidence = min(0.85, (bid_ask_ratio - 1.0) * 0.6)
                
                return {
                    'pattern': 'game_theory_signal',
                    'type': 'dominant_bid_strategy',
                    'confidence': confidence,
                    'bid_ask_ratio': bid_ask_ratio,
                    'total_depth_ratio': total_depth_ratio,
                    'best_bid_size': best_bid_size,
                    'best_ask_size': best_ask_size,
                    'signal': 'Dominant bid strategy - buyers controlling market'
                }
            
            elif dominant_ask:
                # Dominant ask strategy
                confidence = min(0.85, (1.0 - bid_ask_ratio) * 0.6)
                
                return {
                    'pattern': 'game_theory_signal',
                    'type': 'dominant_ask_strategy',
                    'confidence': confidence,
                    'bid_ask_ratio': bid_ask_ratio,
                    'total_depth_ratio': total_depth_ratio,
                    'best_bid_size': best_bid_size,
                    'best_ask_size': best_ask_size,
                    'signal': 'Dominant ask strategy - sellers controlling market'
                }
            
            # Mixed strategy (randomized play)
            if abs(order_book_imbalance) > 0.5 and abs(depth_imbalance) < 0.1:
                # Large order book imbalance but balanced depth
                confidence = abs(order_book_imbalance) * 0.6
                
                return {
                    'pattern': 'game_theory_signal',
                    'type': 'mixed_strategy',
                    'confidence': confidence,
                    'order_book_imbalance': order_book_imbalance,
                    'depth_imbalance': depth_imbalance,
                    'signal': 'Mixed strategy - order book imbalance with balanced depth'
                }
        
        except Exception as e:
            self.logger.debug(f"Game theory signal pattern error: {e}")
        
        return None
    
    def _is_option_symbol(self, symbol: str) -> bool:
        """Check if symbol is an option using centralized UniversalSymbolParser."""
        if not symbol:
            return False
        try:
            from shared_core.redis_clients.universal_symbol_parser import get_symbol_parser
            parser = get_symbol_parser()
            parsed = parser.parse_symbol(symbol)
            return parsed.instrument_type in ('CE', 'PE', 'OPTION')
        except Exception:
            # Fallback: simple string check
            symbol_upper = symbol.upper()
            return symbol_upper.endswith('CE') or symbol_upper.endswith('PE')


    def _get_timeframe_data(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """
        ✅ FIXED: Get timeframe data using single source - live_ohlc_latest or live_ohlc_timeseries.
        
        No longer reconstructs from stream - uses pre-computed OHLC from Redis.
        """
        try:
            if not self.redis_client:
                return None
            
            # Use DatabaseAwareKeyBuilder for OHLC data
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
            from shared_core.redis_clients.redis_client import UnifiedRedisManager
            
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            
            # Priority 1: Try live_ohlc_latest (most recent OHLC)
            ohlc_key = DatabaseAwareKeyBuilder.live_ohlc_latest(canonical_symbol)
            ohlc_data = None
            
            if UNIFIED_REDIS_AVAILABLE and UnifiedRedisManager:
                try:
                    redis_client = get_redis_client(process_name="pattern_detector_ohlc", db=1)
                    ohlc_data = redis_client.hgetall(ohlc_key)
                except Exception:
                    ohlc_data = {}
            elif self.redis_client:
                ohlc_data = self.redis_client.hgetall(ohlc_key) if hasattr(self.redis_client, 'hgetall') else {}
            
            # Priority 2: If latest not available, try timeseries for specific timeframe
            if not ohlc_data:
                timeseries_key = DatabaseAwareKeyBuilder.live_ohlc_timeseries(canonical_symbol, timeframe)
                if UNIFIED_REDIS_AVAILABLE and UnifiedRedisManager:
                    try:
                        redis_client = get_redis_client(process_name="pattern_detector_ohlc_ts", db=1)
                        # Timeseries is a sorted set, get latest entry
                        entries_raw = redis_client.zrevrange(timeseries_key, 0, 0, withscores=True)
                        # ✅ FIX: Handle both sync and async results, ensure entries is a list
                        entries = entries_raw if isinstance(entries_raw, (list, tuple)) else []
                        if entries and len(entries) > 0:
                            import json
                            entry_tuple = entries[0] if isinstance(entries[0], (list, tuple)) else None
                            if entry_tuple and len(entry_tuple) > 0:
                                payload = entry_tuple[0]
                                if isinstance(payload, bytes):
                                    payload = payload.decode('utf-8')
                                ohlc_data = json.loads(payload) if isinstance(payload, str) else {}
                            else:
                                ohlc_data = {}
                        else:
                            ohlc_data = {}
                    except Exception:
                        ohlc_data = {}
                elif self.redis_client and hasattr(self.redis_client, 'zrevrange'):
                    try:
                        entries_raw = self.redis_client.zrevrange(timeseries_key, 0, 0, withscores=True)
                        # ✅ FIX: Handle both sync and async results, ensure entries is a list
                        entries = entries_raw if isinstance(entries_raw, (list, tuple)) else []
                        if entries and len(entries) > 0:
                            import json
                            entry_tuple = entries[0] if isinstance(entries[0], (list, tuple)) else None
                            if entry_tuple and len(entry_tuple) > 0:
                                payload = entry_tuple[0]
                                if isinstance(payload, bytes):
                                    payload = payload.decode('utf-8')
                                ohlc_data = json.loads(payload) if isinstance(payload, str) else {}
                            else:
                                ohlc_data = {}
                        else:
                            ohlc_data = {}
                    except Exception:
                        ohlc_data = {}
            
            if ohlc_data:
                # Convert bytes to strings/numbers if needed
                data = {}
                for k, v in ohlc_data.items() if isinstance(ohlc_data, dict) else {}:
                    key = k.decode('utf-8') if isinstance(k, bytes) else k
                    value = v.decode('utf-8') if isinstance(v, bytes) else v
                    try:
                        data[key] = float(value)
                    except (ValueError, TypeError):
                        data[key] = value
                
                # ✅ FIXED: Fetch price_change_pct from indicators using UnifiedDataStorage (HGETALL on ind:{symbol})
                if 'price_change_pct' not in data:
                    try:
                        from shared_core.redis_clients.unified_data_storage import get_unified_storage
                        storage = get_unified_storage()
                        indicators = storage.get_indicators(canonical_symbol, ['price_change_pct', 'price_change'])
                        
                        if indicators:
                            price_change_pct = indicators.get('price_change_pct')
                            price_change = indicators.get('price_change')
                            
                            if price_change_pct is not None:
                                data['price_change_pct'] = float(price_change_pct)
                                self.logger.debug(f"✅ [TIMEFRAME] {symbol} - Fetched price_change_pct={data['price_change_pct']:.4f}% from ind:{canonical_symbol}")
                            elif price_change is not None:
                                # price_change may be in percentage format, convert to decimal
                                data['price_change_pct'] = float(price_change) / 100.0
                                self.logger.debug(f"✅ [TIMEFRAME] {symbol} - Fetched price_change={price_change}, converted to pct={data['price_change_pct']:.4f}")
                    except Exception as e:
                        self.logger.debug(f"⚠️ [TIMEFRAME] {symbol} - Failed to fetch price_change_pct: {e}")
                
                return data
            
            self.logger.debug(f"⚠️ [TIMEFRAME] {symbol} - No OHLC data found for timeframe {timeframe}")
            return None
            
        except Exception as e:
            self.logger.debug(f"⚠️ [TIMEFRAME] {symbol} - Error fetching timeframe data: {e}")
            return None

    def _get_ohlc_from_redis(self, symbol: str, stream_entries: Optional[List[Any]] = None) -> Optional[Dict[str, float]]:
        """
        ✅ SINGLE SOURCE: Get OHLC from Redis using DatabaseAwareKeyBuilder.live_ohlc_latest().
        
        OHLC is stored in ohlc_latest:{symbol} by websocket_parser.
        This method uses DatabaseAwareKeyBuilder.live_ohlc_latest() to fetch pre-computed OHLC.
        
        Args:
            symbol: Trading symbol
            stream_entries: (Deprecated) No longer used - kept for backward compatibility
        
        Returns:
            Dict with 'open', 'high', 'low', 'close', 'volume', 'timestamp', 'symbol'
        """
        _ = stream_entries
        if not self.redis_client:
            return None
        
        try:
            # ✅ SINGLE SOURCE: Use DatabaseAwareKeyBuilder.live_ohlc_latest() from Redis
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
            from shared_core.redis_clients.redis_client import UnifiedRedisManager
            
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            ohlc_key = DatabaseAwareKeyBuilder.live_ohlc_latest(canonical_symbol)
            
            # Get OHLC from Redis DB1
            if UNIFIED_REDIS_AVAILABLE and UnifiedRedisManager:
                try:
                    redis_client = get_redis_client(process_name="pattern_detector_ohlc", db=1)
                    ohlc_data = redis_client.hgetall(ohlc_key)
                except Exception:
                    ohlc_data = {}
            elif self.redis_client:
                ohlc_data = self.redis_client.hgetall(ohlc_key) if hasattr(self.redis_client, 'hgetall') else {}
            else:
                ohlc_data = {}
            
            if ohlc_data:
                # Convert bytes to strings/numbers if needed
                data: Dict[str, Any] = {}
                for k, v in ohlc_data.items() if isinstance(ohlc_data, dict) else {}:
                    key = k.decode('utf-8') if isinstance(k, bytes) else k
                    value = v.decode('utf-8') if isinstance(v, bytes) else v
                    try:
                        data[key] = float(value)
                    except (ValueError, TypeError):
                        # Skip non-numeric values - we only need OHLC fields
                        continue
                
                # Return in expected format - ensure all values are floats
                if all(k in data for k in ['open', 'high', 'low', 'close']):
                    result: Dict[str, float] = {
                        "open": float(data.get('open', 0)),
                        "high": float(data.get('high', 0)),
                        "low": float(data.get('low', 0)),
                        "close": float(data.get('close', 0)),
                        "volume": float(data.get('volume', 0)),
                        "timestamp": float(data.get('timestamp', data.get('exchange_timestamp', 0))),
                    }
                    return result
            
            self.logger.debug(f"⚠️ [OHLC] {symbol} - OHLC not found in ohlc_latest:{canonical_symbol}")
            return None
            
        except Exception as e:
            self.logger.debug(f"⚠️ [OHLC] {symbol} - Error fetching OHLC from Redis: {e}")
            return None

    def _get_timeframe_trend(self, tf_data: Dict) -> str:
        """
        ✅ FIXED: Use pre-computed price_change_pct from indicators instead of recalculating.
        
        price_change_pct is already calculated by calculations.py and stored in Redis.
        """
        try:
            if not tf_data:
                return 'SIDEWAYS'
            
            # ✅ SINGLE SOURCE: Use price_change_pct from indicators (pre-computed)
            price_change_pct = tf_data.get('price_change_pct') or tf_data.get('price_change')
            
            if price_change_pct is not None:
                try:
                    price_change = float(price_change_pct)
                except (ValueError, TypeError):
                    price_change = 0.0
            else:
                # Fallback: Calculate only if price_change_pct is missing (should be rare)
                open_price = tf_data.get('open', 0)
                close_price = tf_data.get('close', 0)
                
                if open_price > 0:
                    from decimal import Decimal, getcontext
                    getcontext().prec = 10
                    close_decimal = Decimal(str(close_price))
                    open_decimal = Decimal(str(open_price))
                    price_change_decimal = ((close_decimal - open_decimal) / open_decimal) * Decimal('100.0')
                    price_change = float(price_change_decimal)
                else:
                    price_change = 0.0
                
                if price_change != 0.0:
                    self.logger.debug(f"⚠️ [TREND] Using fallback price_change calculation (price_change_pct not in tf_data)")
            
            # Determine trend based on price movement
            if price_change > 0.005:  # > 0.5% up
                trend = 'BULLISH'
            elif price_change < -0.005:  # > 0.5% down
                trend = 'BEARISH'
            else:
                trend = 'SIDEWAYS'
            
            return trend
            
        except Exception as e:
            self.logger.debug(f"⚠️ [TREND] Error determining trend: {e}")
            return 'SIDEWAYS'


    
    def _get_price_trend(self, indicators: Optional[Dict] = None) -> str:
        """
        ✅ FIXED: Use pre-computed price_change_pct from indicators instead of recalculating.
        
        price_change_pct is already calculated by calculations.py and stored in Redis.
        """
        try:
            if not indicators:
                return 'SIDEWAYS'
            
            # ✅ SINGLE SOURCE: Use price_change_pct from indicators (pre-computed)
            price_change_pct = indicators.get('price_change_pct') or indicators.get('price_change')
            
            if price_change_pct is not None:
                try:
                    price_change = float(price_change_pct)
                except (ValueError, TypeError):
                    price_change = 0.0
            else:
                # Fallback: Calculate only if price_change_pct is missing (should be rare)
                open_price = indicators.get('open', 0)
                close_price = indicators.get('close', 0)
                
                if open_price > 0:
                    from decimal import Decimal, getcontext
                    getcontext().prec = 10
                    close_decimal = Decimal(str(close_price))
                    open_decimal = Decimal(str(open_price))
                    price_change_decimal = ((close_decimal - open_decimal) / open_decimal) * Decimal('100.0')
                    price_change = float(price_change_decimal)
                else:
                    price_change = 0.0
                
                if price_change != 0.0:
                    self.logger.debug(f"⚠️ [TREND] Using fallback price_change calculation (price_change_pct not in indicators)")
            
            # Determine trend based on price movement
            if price_change > 0.005:  # > 0.5% up
                trend = 'BULLISH'
            elif price_change < -0.005:  # > 0.5% down
                trend = 'BEARISH'
            else:
                trend = 'SIDEWAYS'
            
            return trend
            
        except Exception as e:
            self.logger.debug(f"⚠️ [TREND] Error determining trend: {e}")
            return 'SIDEWAYS'

    def update_history(self, symbol: str, indicators: Dict[str, Any]) -> None:
        """Maintain rolling history for pattern detection with Redis persistence"""
        if symbol not in self.price_history:
            self.price_history[symbol] = deque(maxlen=50)
            self.high_history[symbol] = deque(maxlen=50)
            self.low_history[symbol] = deque(maxlen=50)
            self.volume_history[symbol] = deque(maxlen=50)
        
        # Keep last 50 periods
        max_history = 50
        last_price = indicators.get('last_price', 0)
        current_high = indicators.get('high', last_price)
        current_low = indicators.get('low', last_price)
        current_volume = indicators.get('bucket_incremental_volume', 0)
        
        # Update in-memory history
        self.price_history[symbol].append(last_price)
        self.high_history[symbol].append(current_high)
        self.low_history[symbol].append(current_low)
        self.volume_history[symbol].append(current_volume)
        
        # Trim to max_history
        for history in [self.price_history, self.high_history, self.low_history, self.volume_history]:
            if len(history[symbol]) > max_history:
                history[symbol] = history[symbol][-max_history:]
        
        # Store in Redis for persistence and cross-process access
        redis_conn = None
        if self.redis_client:
            redis_conn = getattr(self.redis_client, "redis", None)
            if redis_conn is None and hasattr(self.redis_client, "redis_client"):
                redis_conn = self.redis_client.redis_client

        if redis_conn:
            try:
                # Store last_price history in Redis
                # Use DatabaseAwareKeyBuilder instead of hardcoded key
                from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
                price_key = DatabaseAwareKeyBuilder.analytics_pattern_history("last_price", symbol, "price")
                redis_conn.lpush(price_key, last_price)
                redis_conn.ltrim(price_key, 0, max_history - 1)
                redis_conn.expire(price_key, 3600)  # 1 hour TTL
                
                # Store high history in Redis
                high_key = f"pattern_history:high:{symbol}"
                redis_conn.lpush(high_key, current_high)
                redis_conn.ltrim(high_key, 0, max_history - 1)
                redis_conn.expire(high_key, 3600)
                
                # Store low history in Redis
                low_key = f"pattern_history:low:{symbol}"
                redis_conn.lpush(low_key, current_low)
                redis_conn.ltrim(low_key, 0, max_history - 1)
                redis_conn.expire(low_key, 3600)
                
                # Store bucket_incremental_volume history in Redis
                volume_key = f"pattern_history:bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}"
                redis_conn.lpush(volume_key, current_volume)
                redis_conn.ltrim(volume_key, 0, max_history - 1)
                redis_conn.expire(volume_key, 3600)
                
            except Exception as e:
                ...

    def validate_data_quality(self, symbol: str, prices: List[float], volumes: List[float]) -> Dict[str, Any]:
        """Validate data quality before running pattern detection."""
        quality_score = 100
        issues: List[str] = []

        price_count = len(prices)
        if price_count < 15:
            quality_score -= 40
            issues.append(f"Insufficient data points: {price_count}")

        valid_prices = [p for p in prices if isinstance(p, (int, float)) and p > 0]
        if len(valid_prices) != price_count:
            quality_score -= 20
            issues.append(f"Invalid prices: {price_count - len(valid_prices)}")

        if len(valid_prices) >= 3:
            price_changes = []
            for i in range(1, len(valid_prices)):
                prev = valid_prices[i - 1]
                curr = valid_prices[i]
                if prev != 0:
                    price_changes.append(abs((curr - prev) / prev))
            if price_changes:
                avg_change = sum(price_changes) / len(price_changes)
                if 0.0190 <= avg_change <= 0.0193:
                    quality_score = 0
                    issues.append("SYNTHETIC_DATA_DETECTED: Identical ~1.919% moves")

        if volumes:
            valid_volumes = [v for v in volumes if isinstance(v, (int, float)) and v >= 0]
            if len(valid_volumes) != len(volumes):
                quality_score -= 10
                issues.append("Invalid bucket_incremental_volume entries detected")
            if sum(valid_volumes) == 0:
                quality_score -= 20
                issues.append("Zero bucket_incremental_volume data")

        quality_score = max(0, quality_score)
        if quality_score >= 80:
            quality_level = "HIGH"
        elif quality_score >= 60:
            quality_level = "MEDIUM"
        elif quality_score >= 40:
            quality_level = "LOW"
        else:
            quality_level = "REJECT"

        return {
            "score": quality_score,
            "level": quality_level,
            "issues": issues,
            "valid": quality_score >= 60,
        }

    def load_history_from_redis(self, symbol: str) -> None:
        """
        ✅ FIXED: Use HistoricalTickQueries.get_historical_data() instead of manual Redis queries.
        
        HistoricalTickQueries provides standardized access to historical tick data from Redis timeseries.
        """
        if not self.redis_client:
            return

        # ✅ SINGLE SOURCE: Use HistoricalTickQueries for historical data
        try:
            from shared_core.redis_clients.redis_storage import HistoricalTickQueries
            historical_queries = HistoricalTickQueries(redis_client=self.redis_client)
            
            # Get historical ticks for last 8 hours (HistoricalTickQueries retention)
            import time
            end_time = int(time.time() * 1000)  # Current time in milliseconds
            start_time = end_time - (8 * 60 * 60 * 1000)  # 8 hours back
            
            historical_ticks = historical_queries.get_historical_ticks(
                symbol, start_time, end_time, max_ticks=50
            )
            
            if historical_ticks:
                # Extract prices and volumes from historical ticks
                prices = []
                volumes = []
                highs = []
                lows = []
                
                for tick in historical_ticks:
                    price = tick.get('last_price') or tick.get('price') or tick.get('p')
                    volume = tick.get('bucket_incremental_volume') or tick.get('volume') or 0
                    high = tick.get('high') or price
                    low = tick.get('low') or price
                    
                    if price and price > 0:
                        prices.append(float(price))
                        volumes.append(float(volume) if volume else 0)
                        if high:
                            highs.append(float(high))
                        if low:
                            lows.append(float(low))
                
                # Update in-memory history
                if prices:
                    self.price_history[symbol] = deque(prices[-50:], maxlen=50)
                    self.volume_history[symbol] = deque(volumes[-50:], maxlen=50)
                    self.high_history[symbol] = deque(highs[-50:], maxlen=50)
                    self.low_history[symbol] = deque(lows[-50:], maxlen=50)
                    self.logger.debug(f"✅ [HISTORY] Loaded {len(prices)} historical ticks for {symbol} from HistoricalTickQueries")
                    return
        except Exception as e:
            self.logger.debug(f"⚠️ [HISTORY] HistoricalTickQueries failed for {symbol}: {e}, using fallback")

        # Fallback: Manual Redis query (only if HistoricalTickQueries unavailable)
        redis_conn = getattr(self.redis_client, "redis", None)
        if redis_conn is None and hasattr(self.redis_client, "redis_client"):
            redis_conn = self.redis_client.redis_client
        if not redis_conn:
            return

        try:
            # Use standardized Redis key patterns from optimized_field_mapping.yaml
            # Pattern: session:{symbol}:{date} for session data
            session_date = datetime.now().strftime('%Y-%m-%d')
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            session_key = DatabaseAwareKeyBuilder.live_session(symbol, session_date)
            session_data = redis_conn.hgetall(session_key)
            if session_data:
                last_price = session_data.get('last_price')
                if last_price:
                    self.price_history[symbol] = deque([float(last_price)], maxlen=50)
                
                high = session_data.get('high')
                low = session_data.get('low')
                if high:
                    self.high_history[symbol] = deque([float(high)], maxlen=50)
                if low:
                    self.low_history[symbol] = deque([float(low)], maxlen=50)
                
                zerodha_cumulative_volume = session_data.get('zerodha_cumulative_volume')
                if zerodha_cumulative_volume:
                    self.volume_history[symbol] = deque([float(zerodha_cumulative_volume)], maxlen=50)
                
        except Exception as e:
            ...

    def _load_pattern_config(self) -> Dict[str, Any]:
        """✅ CACHED: Load pattern configuration (uses module-level cache)"""
        # Use cached loader to avoid reloading on every initialization
        return _load_pattern_config_cached()

    def _load_thresholds(self) -> Dict[str, float]:
        """
        ✅ DEPRECATED: This method is no longer used by PatternDetector.
        
        MarketContext handles all thresholds (cached, VIX-aware) via get_context()['thresholds'].
        All patterns use indicators.get('pattern_thresholds') from MarketContext.
        
        Kept for backward compatibility only - delegates to MarketContext.
        """
        # ✅ SINGLE SOURCE: Get thresholds from MarketContext instead (cached, VIX-aware)
        market_context = self._get_market_context()
        return market_context.get('thresholds', {})

    def _load_volume_baselines(self) -> Dict[str, Dict[str, float]]:
        self.volume_baselines = {} # Initialize empty dict to prevent AttributeError

        """✅ FIXED: Load dynamic 20d/55d bucket_incremental_volume baselines from Redis DB1 (primary), JSON fallback only if Redis has no data"""
        baselines = {}
        
        # PRIMARY: Load from Redis DB1 (source of truth)
        redis_loaded = False
        try:
            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards, DatabaseAwareKeyBuilder
            
            if not hasattr(self, 'redis_client') or not self.redis_client:
                logger.warning("⚠️ No redis_client available, will try JSON fallback")
            else:
                # Get DB1 client
                db1_client = None
                if hasattr(self.redis_client, 'get_client'):
                    db1_client = self.redis_client.get_client(1)
                elif hasattr(self.redis_client, 'hgetall'):
                    db1_client = self.redis_client
                
                if db1_client:
                    baseline_keys = []
                    try:
                        # Logic to resolve keys (Active Set or Fallback Scan)
                        active_symbols = db1_client.smembers('active_baselines')
                         
                        if active_symbols:
                             logger.info(f"✅ [INIT] Using active_baselines SET index ({len(active_symbols)} symbols)")
                             baseline_keys = [f"vol:baseline:{s.decode('utf-8') if isinstance(s, bytes) else s}" for s in active_symbols]
                        else:
                             # Fallback scan
                             logger.warning("⚠️ active_baselines SET missing - Performing one-time SCAN to populate...")
                             unique_keys = set()
                             cursor = 0
                             while True:
                                 cursor, keys = db1_client.scan(cursor, match='vol:baseline:*', count=2000)
                                 unique_keys.update(keys)
                                 if cursor == 0:
                                     break
                             baseline_keys = list(unique_keys)
                             # Populate set
                             if baseline_keys:
                                 symbols = []
                                 for k in baseline_keys:
                                     k_str = k.decode('utf-8') if isinstance(k, bytes) else k
                                     symbols.append(k_str.replace('vol:baseline:', ''))
                                 if symbols:
                                     db1_client.sadd('active_baselines', *symbols)
                
                    except Exception as e:
                        logger.error(f"❌ Error resolving baseline keys: {e}")
                        baseline_keys = []
                
                    # Pipeline Loading
                    if baseline_keys:
                        logger.info(f"🔍 Loading {len(baseline_keys)} baselines using PIPELINE...")
                        batch_size = 500
                        loaded_count = 0
                        
                        try:
                            for i in range(0, len(baseline_keys), batch_size):
                                batch_keys = baseline_keys[i:i+batch_size]
                                pipeline = db1_client.pipeline()
                                for key in batch_keys:
                                    pipeline.hgetall(key)
                                results = pipeline.execute()
                                
                                for key, baseline_data in zip(batch_keys, results):
                                    if not baseline_data: continue
                                    key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                                    symbol = key_str.replace('vol:baseline:', '')
                                    
                                    clean_data = {}
                                    for k, v in baseline_data.items():
                                        k_str = k.decode('utf-8') if isinstance(k, bytes) else k
                                        v_str = v.decode('utf-8') if isinstance(v, bytes) else v
                                        try: 
                                            clean_data[k_str] = float(v_str)
                                        except: 
                                            clean_data[k_str] = v_str
                                    self.volume_baselines[symbol] = clean_data
                                    loaded_count += 1
                                    
                            logger.info(f"✅ Successfully loaded {loaded_count} volume baselines (Pipeline optimized)")
                        except Exception as e:
                            logger.error(f"Error loading baselines loop: {e}")

        except Exception as e:
            logger.error(f"Error in _load_volume_baselines: {e}")

        return self.volume_baselines

    def _run_async_task(self, coroutine):
        """Run coroutine in a blocking manner, handling nested event loops gracefully."""
        try:
            return asyncio.run(coroutine)
        except RuntimeError:
            new_loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(new_loop)
                return new_loop.run_until_complete(coroutine)
            finally:
                asyncio.set_event_loop(None)
                new_loop.close()
        except Exception as exc:
            self.logger.warning("⚠️ Failed to execute async task: %s", exc)
            return None


class PatternDetectorFactory:
    """THE ONE TRUE FACTORY"""

    @staticmethod
    def get_instance(
        detector_type: str = "default",
        *,
        config: Optional[Dict[str, Any]] = None,
        redis_client=None,
        caller: str = "unknown",
        **kwargs,
    ):
        """
        Get PatternDetector instance - ALWAYS returns singleton.
        detector_type is currently advisory; we still funnel everything through the singleton.
        """
        from patterns.singleton_factory import PatternDetectorSingletonFactory

        instance = PatternDetectorSingletonFactory.get_instance(
            config=config,
            redis_client=redis_client,
            caller=f"{caller}->{detector_type}",
            **kwargs,
        )

        if detector_type != "default" and hasattr(instance, "set_detector_type"):
            instance.set_detector_type(detector_type)

        return instance

    @staticmethod
    def create_detector(
        detector_type: str = "default",
        *,
        config: Optional[Dict[str, Any]] = None,
        redis_client=None,
        **kwargs,
    ):
        """DEPRECATED - maintain compatibility but route to get_instance."""
        import warnings

        warnings.warn(
            "create_detector is deprecated. Use get_instance for singleton.",
            DeprecationWarning,
        )
        return PatternDetectorFactory.get_instance(
            detector_type,
            config=config,
            redis_client=redis_client,
            caller="create_detector",
            **kwargs,
        )


def _configure_handler_states(detector: PatternDetector, handler_states: Dict[str, bool]) -> PatternDetector:
    """Apply handler enable/disable map to detector."""
    if not hasattr(detector, "enable_handler"):
        return detector
    for handler_name, should_enable in handler_states.items():
        detector.enable_handler(handler_name, should_enable)
    return detector


def _log_handler_summary(tag: str, worker_id: Optional[str], detector: PatternDetector) -> None:
    """Log handler/detector counts for visibility and ensure handlers exist."""
    handler_count = len(getattr(detector, "option_pattern_handlers", {}))
    detector_count = len(detector.detector_methods)
    logger.info(
        f"🎯 [{tag}] Worker {worker_id or 'unknown'}: "
        f"{handler_count} handlers, {detector_count} detector methods"
    )
    if handler_count == 0:
        raise RuntimeError(
            f"Pattern initialization failed for {tag}: no option handlers registered"
        )


def _initialize_worker_patterns(
    worker_type: str,
    worker_id: Optional[str],
    config: Optional[Dict[str, Any]] = None,
    redis_client=None,
    handler_overrides: Optional[Dict[str, bool]] = None,
) -> PatternDetector:
    """Internal helper that wires up a detector for a worker category."""
    detector = PatternDetectorFactory.get_instance(
        worker_id=worker_id,
        process_type=worker_type,
        config=config,
        redis_client=redis_client,
    )
    handler_states = {name: True for name in detector.get_available_handlers()}
    if handler_overrides:
        handler_states.update(handler_overrides)
    _configure_handler_states(detector, handler_states)
    _log_handler_summary(f"{worker_type.upper()}_PATTERNS", worker_id, detector)
    return detector


def initialize_scanner_patterns(
    worker_id: Optional[str],
    config: Optional[Dict[str, Any]] = None,
    redis_client=None,
) -> PatternDetector:
    """Initialize patterns for scanner workers (enable all except kow_signal_straddle)."""
    overrides = {'kow_signal_straddle': False}
    return _initialize_worker_patterns("scanner", worker_id, config, redis_client, overrides)


def initialize_crawler_patterns(
    worker_id: Optional[str],
    config: Optional[Dict[str, Any]] = None,
    redis_client=None,
) -> PatternDetector:
    """Initialize patterns for crawler workers."""
    overrides = {'kow_signal_straddle': False}
    return _initialize_worker_patterns("crawler", worker_id, config, redis_client, overrides)


def initialize_parser_patterns(
    worker_id: Optional[str],
    config: Optional[Dict[str, Any]] = None,
    redis_client=None,
) -> PatternDetector:
    """Initialize patterns for parser workers."""
    overrides = {'kow_signal_straddle': False}
    return _initialize_worker_patterns("parser", worker_id, config, redis_client, overrides)


def initialize_default_patterns(
    worker_id: Optional[str],
    config: Optional[Dict[str, Any]] = None,
    redis_client=None,
) -> PatternDetector:
    """Fallback initializer for unknown worker types."""
    overrides = {'kow_signal_straddle': False}
    return _initialize_worker_patterns("default", worker_id, config, redis_client, overrides)


def initialize_patterns_once(
    worker_type: str,
    worker_id: Optional[str],
    config: Optional[Dict[str, Any]] = None,
    redis_client=None,
) -> PatternDetector:
    """
    Unified entrypoint to initialize pattern detectors once per worker type/ID.
    Uses a filesystem lock plus the in-memory factory singleton for redundancy.
    """
    lock_file = f"/tmp/pattern_init_{worker_type}_{worker_id}.lock"

    if os.path.exists(lock_file):
        logger.info(f"🔄 [PATTERN_INIT] Patterns already initialized for {worker_type}-{worker_id}")
        return PatternDetectorFactory.get_instance(
            worker_id=worker_id,
            process_type=worker_type,
            config=config,
            redis_client=redis_client,
        )

    with open(lock_file, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))

    try:
        initializer_map = {
            "scanner": initialize_scanner_patterns,
            "crawler": initialize_crawler_patterns,
            "parser": initialize_parser_patterns,
        }
        initializer = initializer_map.get(worker_type, initialize_default_patterns)
        detector = initializer(worker_id, config=config, redis_client=redis_client)
        logger.info(f"✅ [PATTERN_INIT] Patterns initialized for {worker_type}-{worker_id}")
        return detector
    except Exception as exc:
        logger.error(f"❌ [PATTERN_INIT] Failed for {worker_type}-{worker_id}: {exc}")
        try:
            os.remove(lock_file)
        except OSError:
            pass
        raise


def debug_pattern_detection(symbol: str):
    """Print Redis state for a symbol to understand why patterns are skipped."""
    client = get_redis_client(
        process_name="debug_pattern_detection",
        db=1,
    )

    print(f"\n🔍 DEBUG PATTERN DETECTION FOR: {symbol}")
    print("=" * 60)

    hash_key = f"ind:{symbol}"
    hash_data = client.hgetall(hash_key)
    print(f"📊 HASH INDICATORS ({hash_key}):")
    if hash_data:
        for key, value in hash_data.items():
            print(f"  {key}: {value}")
    else:
        print("  ❌ No HASH data found!")

    print("\n📊 GREEK INDICATORS:")
    for greek in ['delta', 'gamma', 'theta', 'vega', 'iv']:
        key = f"ind:greeks:{symbol}:{greek}"
        value = client.get(key)
        print(f"  {greek}: {value if value else 'MISSING'}")

    volume_ratio = hash_data.get('volume_ratio') if hash_data else None
    if not volume_ratio:
        legacy_key = f"ind:volume:{symbol}:volume_ratio"
        volume_ratio = client.get(legacy_key)
    print(f"\n📈 VOLUME_RATIO: {volume_ratio}")

    session_key = f"session:{symbol}:window"
    session_data = client.get(session_key)
    print("\n🕒 SESSION WINDOW:")
    if session_data:
        try:
            session = json.loads(session_data)
        except json.JSONDecodeError:
            session = {}
        print(f"  Tick count: {session.get('tick_count', 0)}")
        print(f"  Initialized: {session.get('initialized', False)}")
        print(f"  Patterns enabled: {session.get('patterns_enabled', False)}")
    else:
        print("  ❌ No session window found!")

    print("\n🎯 PATTERN DETECTION DECISION:")
    try:
        vr_float = float(volume_ratio) if volume_ratio is not None else 0.0
    except (TypeError, ValueError):
        vr_float = 0.0
    if vr_float <= 0.0:
        print("  ❌ FAIL: volume_ratio missing or 0.0")
    else:
        print(f"  ✅ PASS: volume_ratio = {vr_float:.4f}")

    delta_val = client.get(f"ind:greeks:{symbol}:delta")
    if delta_val:
        try:
            delta_float = float(delta_val)
            print(f"  Delta: {delta_float:.3f}")
            if abs(delta_float) < 0.05 or abs(delta_float) > 0.95:
                print("  ⚠️ WARNING: Delta might be considered extreme")
            else:
                print("  ✅ Delta is within normal bounds")
        except (TypeError, ValueError):
            print("  ⚠️ WARNING: Delta value not numeric")
    else:
        print("  ⚠️ Delta not available")

    print("=" * 60)
