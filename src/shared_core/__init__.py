"""
shared_core
===========

Central location for cross-service modules (Redis utilities, volume
managers, YAML config helpers, market calendar, etc.).  This package is installable so both
the intraday scanner and dashboard backend can depend on the same source of
truth without manipulating sys.path.
"""

from .market_calendar import MarketCalendar, get_cached_calendar

__all__ = ["config_utils", "dynamic_thresholds", "interest_rate_service", "redis_clients", "volume_files", "market_calendar", "MarketCalendar", "get_cached_calendar"]
from .config_utils import apply_field_mapping, normalize_session_record
try:
    from .config_utils.timestamp_normalizer import TimestampNormalizer
    __all__.append("TimestampNormalizer")
except ImportError:
    TimestampNormalizer = None
try:
    from .dynamic_thresholds import TimeAwareBaseline, VolumeProfileManager, VolumeProfile
except ImportError:
    TimeAwareBaseline = None
    VolumeProfileManager = None
    VolumeProfile = None

try:
    from .interest_rate_service import InterestRateService
except ImportError:
    InterestRateService = None
from .redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
from .volume_files.volume_computation_manager import (
    VolumeComputationManager,
    get_volume_computation_manager,
)
from .market_calendar import MarketCalendar, get_cached_calendar

# ✅ NEW: Export VIX utilities
try:
    from .utils.vix_utils import VIXUtils, get_vix_utils, get_vix_value, get_vix_regime, get_current_vix, is_vix_panic, is_vix_complacent
    from .utils.vix_regimes import VIXRegimeManager
    __all__.extend([
        "VIXUtils", "get_vix_utils", "get_vix_value", "get_vix_regime", "get_current_vix", 
        "is_vix_panic", "is_vix_complacent", "VIXRegimeManager",
    ])
except ImportError:
    pass
