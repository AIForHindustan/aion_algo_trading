# shared_core/volume_files/__init__.py
"""
Unified Volume Module - Single source of truth for all volume computations.

This module provides:
1. EXISTING EXPORTS (backward compatible - all 13 consumers keep working):
   - VolumeComputationManager
   - get_volume_computation_manager
   - VolumeResolver (legacy)

2. NEW UNIFIED EXPORTS (opt-in for new development):
   - UnifiedVolumeEngine
   - BaselineService
   - InstrumentType
   - SessionTracker
   - LegacyKeyCompatibility

Usage (existing consumers - NO CHANGES NEEDED):
    from shared_core.volume_files import VolumeComputationManager
    manager = VolumeComputationManager(redis_client)
    result = manager.calculate_volume_metrics(symbol, tick_data)

Usage (new development - optional):
    from shared_core.volume_files import UnifiedVolumeEngine, InstrumentType
    engine = UnifiedVolumeEngine(redis_client)
    result = engine.process_tick(symbol, tick_data)
"""

# =============================================================================
# EXISTING EXPORTS (backward compatible)
# =============================================================================

from .volume_computation_manager import (
    VolumeComputationManager,
    get_volume_computation_manager,
)

# Legacy VolumeResolver for backward compatibility
try:
    from .correct_volume_calculator import VolumeResolver
except ImportError:
    VolumeResolver = None

# =============================================================================
# NEW UNIFIED EXPORTS (opt-in for new development)
# =============================================================================

from .unified_engine import UnifiedVolumeEngine
from .baseline_service import BaselineService, InstrumentType
from .session_tracker import SessionTracker
from .legacy_compatibility import LegacyKeyCompatibility
from .key_usage_tracker import KeyUsageTracker, get_key_tracker

# =============================================================================
# BASELINE CLASSES (F&O vs Equity specific logic)
# =============================================================================

# Abstract base and concrete baseline calculators
from abc import ABC, abstractmethod
from datetime import datetime, time
from typing import Dict, Optional


class BaseBaselineCalculator(ABC):
    """Abstract base class for all baseline calculators"""
    
    def __init__(self, redis_client):
        self.redis = redis_client
        self.trading_minutes = 375  # 9:15-15:30
    
    @abstractmethod
    def calculate_static_baseline(self, symbol: str) -> Dict:
        """Calculate static baseline (20d/55d averages)"""
        pass
    
    @abstractmethod
    def calculate_time_aware_baseline(self, symbol: str, timestamp: datetime) -> float:
        """Calculate time-aware baseline with adjustments"""
        pass


class EquityBaselineCalculator(BaseBaselineCalculator):
    """Equity-specific baseline calculations"""
    
    def __init__(self, redis_client):
        super().__init__(redis_client)
        self.time_multipliers = {
            ("9:15", "9:45"): 1.8,
            ("9:45", "11:30"): 1.2,
            ("11:30", "13:00"): 0.8,
            ("13:00", "15:15"): 1.1,
            ("15:15", "15:30"): 1.5,
        }
    
    def calculate_static_baseline(self, symbol: str) -> Dict:
        """Get 20d/55d averages for equity"""
        service = BaselineService(self.redis)
        return service.get_baseline(symbol, InstrumentType.EQUITY)
    
    def calculate_time_aware_baseline(self, symbol: str, timestamp: datetime) -> float:
        """Get time-aware baseline for equity"""
        baseline = self.calculate_static_baseline(symbol)
        return float(baseline.get('adjusted_1min', 0))


class FNOBaselineCalculator(BaseBaselineCalculator):
    """F&O-specific baseline calculations with expiry awareness"""
    
    def __init__(self, redis_client):
        super().__init__(redis_client)
        self.time_multipliers = {
            ("9:15", "10:30"): 2.0,
            ("10:30", "13:00"): 1.3,
            ("13:00", "14:30"): 0.9,
            ("14:30", "15:30"): 1.8,
        }
    
    def calculate_static_baseline(self, symbol: str) -> Dict:
        """Get F&O baseline with expiry adjustments"""
        service = BaselineService(self.redis)
        return service.get_baseline(symbol, InstrumentType.FNO_INDEX)
    
    def calculate_time_aware_baseline(self, symbol: str, timestamp: datetime) -> float:
        """Get time-aware baseline for F&O"""
        baseline = self.calculate_static_baseline(symbol)
        return float(baseline.get('adjusted_1min', 0))


class BaselineManager:
    """Unified baseline manager with asset-type routing"""
    
    def __init__(self, redis_client):
        self.redis = redis_client
        self.calculators = {
            "equity": EquityBaselineCalculator(redis_client),
            "fno": FNOBaselineCalculator(redis_client),
        }
        self._service = BaselineService(redis_client)
    
    def _get_asset_type(self, symbol: str) -> str:
        """Determine asset type from symbol"""
        symbol_upper = symbol.upper()
        if symbol_upper.endswith('CE') or symbol_upper.endswith('PE') or symbol_upper.endswith('FUT'):
            return "fno"
        return "equity"
    
    def get_baseline(self, symbol: str, timestamp: datetime, 
                    use_cache: bool = True) -> Dict:
        """Get complete baseline for a symbol"""
        asset_type = self._get_asset_type(symbol)
        
        # Map to InstrumentType
        if asset_type == "fno":
            instrument_type = InstrumentType.FNO_INDEX
        else:
            instrument_type = InstrumentType.EQUITY
        
        baseline = self._service.get_baseline(
            symbol=symbol,
            instrument_type=instrument_type,
            timestamp=timestamp
        )
        
        # Add session baseline (first 30 mins uses different source)
        session_baseline = self._get_session_baseline(symbol, timestamp)
        
        return {
            "static": baseline,
            "time_aware": baseline.get('adjusted_1min', 0),
            "session_baseline": session_baseline,
            "asset_type": asset_type,
            "timestamp": timestamp.isoformat()
        }
    
    def _get_session_baseline(self, symbol: str, timestamp: datetime) -> float:
        """Get session baseline for early trading minutes"""
        tracker = SessionTracker(self.redis)
        session = tracker.get_session_data(symbol)
        
        minutes = float(session.get('minutes_elapsed', 0))
        cumulative = float(session.get('cumulative_volume', 0))
        
        if minutes > 0:
            return cumulative / minutes
        
        return 0.0


# =============================================================================
# PUBLIC API
# =============================================================================

__all__ = [
    # Existing exports (backward compatible)
    'VolumeComputationManager',
    'get_volume_computation_manager',
    'VolumeResolver',
    
    # New unified system (opt-in)
    'UnifiedVolumeEngine',
    'BaselineService',
    'InstrumentType',
    'SessionTracker',
    'LegacyKeyCompatibility',
    
    # Baseline calculators
    'BaseBaselineCalculator',
    'EquityBaselineCalculator',
    'FNOBaselineCalculator',
    'BaselineManager',
]