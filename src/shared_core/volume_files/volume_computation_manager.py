# shared_core/volume_files/volume_computation_manager.py
"""
Single source of truth for real-time volume computation.

This class maintains backward compatibility with all 13 downstream consumers
while internally using the new UnifiedVolumeEngine for computation.
"""

from datetime import datetime
from typing import Dict, Optional, Any
import logging

logger = logging.getLogger(__name__)


class VolumeComputationManager:
    """
    Unified Volume Manager - Single source of truth for all volume calculations.
    
    BACKWARD COMPATIBLE: All existing imports and method calls work unchanged.
    INTERNALLY ENHANCED: Uses UnifiedVolumeEngine for actual computation.
    """
    
    def __init__(self, redis_client, token_resolver=None):
        """
        Initialize VolumeComputationManager.
        
        Args:
            redis_client: Redis client instance
            token_resolver: Optional token resolver (kept for backward compatibility)
        """
        self.redis = redis_client
        self.token_resolver = token_resolver
        
        # NEW: Internal unified engine for actual computation
        from .unified_engine import UnifiedVolumeEngine
        self._unified_engine = UnifiedVolumeEngine(redis_client)
        
        # NEW: Baseline manager for easy access
        from .baseline_service import BaselineService
        self._baseline_service = BaselineService(redis_client)
    
    def calculate_volume_metrics(self, symbol: str, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate volume metrics for a tick.
        
        MAINTAINS EXACT SAME INTERFACE for downstream consumers.
        Internally routes to UnifiedVolumeEngine.
        
        Args:
            symbol: Canonical symbol
            tick_data: Tick data dict with volume, price, timestamp
            
        Returns:
            Dict with volume_ratio, per_minute_ratio, etc.
        """
        try:
            # Route to unified engine
            unified_result = self._unified_engine.process_tick(symbol, tick_data)
            
            # Transform to legacy format for backward compatibility
            ratios = unified_result.get('ratios', {})
            session = unified_result.get('session', {})
            baselines = unified_result.get('baselines', {})
            
            return {
                'volume_ratio': ratios.get('cumulative', 0.0),
                'per_minute_ratio': ratios.get('minute', 0.0),
                'cumulative_volume': float(session.get('cumulative_volume', 0)),
                'expected_cumulative': ratios.get('expected_cumulative', 0.0),
                'baseline_1min': ratios.get('baseline_1min', 0.0),
                'minutes_elapsed': float(session.get('minutes_elapsed', 0)),
                'asset_type': unified_result.get('instrument_type', 'equity'),
                'timestamp': unified_result.get('timestamp', datetime.now().isoformat())
            }

        except Exception as e:
            logger.error(f"Error in calculate_volume_metrics for {symbol}: {e}")
            return {
                'volume_ratio': 0.0,
                'per_minute_ratio': 0.0,
                'cumulative_volume': 0.0,
                'expected_cumulative': 0.0,
                'baseline_1min': 0.0,
                'minutes_elapsed': 0.0,
                'asset_type': 'equity',
                'error': str(e)
            }

    def get_volume_baseline(self, symbol: str, timestamp: Optional[datetime] = None) -> float:
        """
        Get adjusted volume baseline for a symbol.
        
        Args:
            symbol: Canonical symbol
            timestamp: Optional timestamp (defaults to now)
            
        Returns:
            Adjusted baseline_1min value
        """
        try:
            from .baseline_service import InstrumentType
            
            # Categorize instrument
            instrument_type = self._unified_engine._categorize_instrument(symbol)
            
            # Get full baseline info
            baseline = self._baseline_service.get_baseline(
                symbol=symbol,
                instrument_type=instrument_type,
                timestamp=timestamp or datetime.now()
            )
            
            return float(baseline.get('adjusted_1min', 0))
            
        except Exception as e:
            logger.error(f"Error getting volume baseline for {symbol}: {e}")
            return 1000.0  # Safe fallback
    
    def get_session_state(self, symbol: str) -> Dict[str, Any]:
        """
        Get current session state for a symbol.
        
        Args:
            symbol: Canonical symbol
            
        Returns:
            Dict with cumulative_volume, current_minute_volume, etc.
        """
        try:
            return self._unified_engine.session_tracker.get_session_data(symbol)
        except Exception as e:
            logger.error(f"Error getting session state for {symbol}: {e}")
            return {}
    
    # Legacy method aliases for backward compatibility
    def calculate_incremental(self, instrument_token: str, current_cumulative: int,
                            exchange_timestamp: datetime, symbol: Optional[str] = None) -> int:
        """
        Legacy method for calculating incremental volume.
        
        DEPRECATED: Use calculate_volume_metrics() instead.
        Kept for backward compatibility.
        """
        logger.debug(f"calculate_incremental called for {symbol or instrument_token} - legacy path")
        
        # Try to get from session tracker
        if symbol:
            session = self.get_session_state(symbol)
            return int(float(session.get('current_minute_volume', 0)))
        
        return 0


# Global instance cache for backward compatibility
_volume_computation_manager_instance: Optional[VolumeComputationManager] = None


def get_volume_computation_manager(redis_client=None, token_resolver=None) -> VolumeComputationManager:
    """
    Get or create singleton instance of VolumeComputationManager.
    
    Args:
        redis_client: Redis client (required on first call)
        token_resolver: Optional token resolver
        
    Returns:
        VolumeComputationManager instance
    """
    global _volume_computation_manager_instance
    
    if _volume_computation_manager_instance is None:
        if redis_client is None:
            # Try to get default client
            from shared_core.redis_clients import get_redis_client
            redis_client = get_redis_client(process_name="volume_manager", db=1)
        
        _volume_computation_manager_instance = VolumeComputationManager(
            redis_client=redis_client,
            token_resolver=token_resolver
        )
    
    return _volume_computation_manager_instance
