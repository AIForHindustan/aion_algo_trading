"""
Bridge module for dynamic_thresholds to expose shared_core implementations.
Redirects legacy imports to the centralized shared_core.volume_files module.
"""

# ✅ CENTRALIZED: Import from the single source of truth
from shared_core.volume_files import (
    BaselineManager as AssetTypeAwareBaseline,  # Alias for backward compatibility
    BaseBaselineCalculator as UnifiedTimeAwareBaseline, # Alias
    EquityBaselineCalculator as OHLCBaselineCalculator, # Alias
    BaselineService
)

# Define BUCKET_MULTIPLIERS for backward compatibility (derived from BaselineService)
# Callers expect a simple dict {time: multiplier}
BUCKET_MULTIPLIERS = {}
# Flatten EQUITY multipliers for compatibility
for name, (start, end, mult) in BaselineService.EQUITY_SESSION_MULTIPLIERS.items():
    key = start.strftime("%H:%M")
    BUCKET_MULTIPLIERS[key] = mult

# Alias for backward compatibility
TimeAwareBaseline = UnifiedTimeAwareBaseline

# Import VolumeProfileManager from patterns module (at root level)
try:
    from intraday_trading.patterns.volume_profile_manager import VolumeProfileManager, SymbolVolumeProfile

    VolumeProfile = SymbolVolumeProfile
except ImportError:
    VolumeProfileManager = None
    VolumeProfile = None
    SymbolVolumeProfile = None

__all__ = [
    'TimeAwareBaseline',
    'AssetTypeAwareBaseline',
    'UnifiedTimeAwareBaseline',
    'OHLCBaselineCalculator',
    'BUCKET_MULTIPLIERS',
    'VolumeProfileManager',
    'VolumeProfile',
    'SymbolVolumeProfile',
]
