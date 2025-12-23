# shared_core/volume_files/baseline_service.py
"""
Unified baseline computation with F&O special handling.
Provides baselines for all instrument types with proper adjustments.
"""

from datetime import datetime, time
from enum import Enum
from typing import Dict, Optional, Any
import json
import logging

logger = logging.getLogger(__name__)


class InstrumentType(Enum):
    EQUITY = "equity"
    FNO_INDEX = "fno_index"
    FNO_STOCK = "fno_stock"
    FNO_OPTION = "fno_option"


class BaselineService:
    """Unified baseline computation with F&O special handling."""
    
    # Trading constants
    TRADING_MINUTES_PER_DAY = 375  # 9:15 to 15:30
    
    # Time-based multipliers for different sessions
    EQUITY_SESSION_MULTIPLIERS = {
        'pre_open': (time(9, 0), time(9, 15), 0.1),
        'opening_frenzy': (time(9, 15), time(10, 0), 1.8),
        'morning_session': (time(10, 0), time(11, 30), 1.2),
        'mid_session': (time(11, 30), time(13, 0), 0.9),
        'afternoon_session': (time(13, 0), time(15, 0), 1.1),
        'closing_auction': (time(15, 0), time(15, 30), 2.0),
    }
    
    FNO_SESSION_MULTIPLIERS = {
        'pre_open': (time(9, 0), time(9, 15), 0.1),
        'opening_momentum': (time(9, 15), time(10, 30), 2.0),
        'morning_session': (time(10, 30), time(13, 0), 1.3),
        'mid_session': (time(13, 0), time(14, 30), 0.9),
        'closing_rush': (time(14, 30), time(15, 30), 1.8),
    }
    
    def __init__(self, redis_client=None):
        """
        Initialize baseline service.
        
        Args:
            redis_client: Optional Redis client. If None, uses centralized get_redis_client().
                         Can also be a wrapper like WorkerRedisManager.
        """
        # Use centralized Redis client pattern
        if redis_client is None:
            from shared_core.redis_clients import get_redis_client
            self.redis = get_redis_client(process_name="baseline_service", db=1)
        elif hasattr(redis_client, 'get_client'):
            # It's a wrapper (e.g., WorkerRedisManager) - get the actual client
            self.redis = redis_client.get_client()
        else:
            # It's a direct Redis client
            self.redis = redis_client
    
    def get_baseline(self, symbol: str, instrument_type: InstrumentType, 
                     timestamp: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Returns baseline with proper handling for each instrument type.
        
        For F&O indices:
        - Consider expiry week adjustments
        - Account for rollover activity
        - Different time-of-day patterns vs equity
        
        For F&O options:
        - Moneyness-based adjustments
        - Expiry day special handling
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        if instrument_type == InstrumentType.EQUITY:
            return self._get_equity_baseline(symbol, timestamp)
        elif instrument_type in [InstrumentType.FNO_INDEX, InstrumentType.FNO_STOCK]:
            return self._get_fno_baseline(symbol, timestamp, instrument_type)
        elif instrument_type == InstrumentType.FNO_OPTION:
            return self._get_option_baseline(symbol, timestamp)
        else:
            # Fallback to equity
            return self._get_equity_baseline(symbol, timestamp)
    
    def _get_equity_baseline(self, symbol: str, timestamp: datetime) -> Dict[str, Any]:
        """Get baseline for equity instruments."""
        static_baseline = self._get_static_baseline(symbol)
        time_multiplier = self._get_time_multiplier(timestamp, InstrumentType.EQUITY)
        vix_multiplier = self._get_vix_multiplier()
        
        adjusted_1min = static_baseline * time_multiplier * vix_multiplier
        
        return {
            'static_1min': static_baseline,
            'adjusted_1min': adjusted_1min,
            'time_multiplier': time_multiplier,
            'vix_multiplier': vix_multiplier,
            'adjustments': {},
            'is_fno': False,
            'instrument_type': InstrumentType.EQUITY.value
        }
    
    def _get_fno_baseline(self, symbol: str, timestamp: datetime, 
                          instrument_type: InstrumentType) -> Dict[str, Any]:
        """Special baseline logic for F&O instruments."""
        
        # 1. Get static baseline from daily pre-seed
        static_baseline = self._get_static_baseline(symbol)
        
        # 2. Apply F&O specific adjustments
        adjustments = self._get_fno_adjustments(symbol, timestamp)
        
        # 3. Check if in expiry week
        if self._is_expiry_week(symbol, timestamp):
            adjustments['expiry_multiplier'] = self._get_expiry_multiplier(timestamp)
        else:
            adjustments['expiry_multiplier'] = 1.0
        
        # 4. For indices, consider rollover dates
        if instrument_type == InstrumentType.FNO_INDEX:
            adjustments['rollover_adjustment'] = self._get_rollover_adjustment(symbol, timestamp)
        else:
            adjustments['rollover_adjustment'] = 1.0
        
        # 5. Apply time-aware multiplier for Indian markets
        time_multiplier = self._get_time_multiplier(timestamp, instrument_type)
        
        # 6. VIX adjustment
        vix_multiplier = self._get_vix_multiplier()
        
        # Calculate total adjustment
        total_adjustment = (
            adjustments.get('expiry_multiplier', 1.0) *
            adjustments.get('rollover_adjustment', 1.0) *
            time_multiplier *
            vix_multiplier
        )
        
        return {
            'static_1min': static_baseline,
            'adjusted_1min': static_baseline * total_adjustment,
            'time_multiplier': time_multiplier,
            'vix_multiplier': vix_multiplier,
            'adjustments': adjustments,
            'is_fno': True,
            'instrument_type': instrument_type.value
        }
    
    def _get_option_baseline(self, symbol: str, timestamp: datetime) -> Dict[str, Any]:
        """Special baseline logic for options with moneyness adjustments."""
        base_result = self._get_fno_baseline(symbol, timestamp, InstrumentType.FNO_OPTION)
        
        # Add moneyness adjustment for options
        moneyness_mult = self._get_moneyness_multiplier(symbol)
        base_result['adjustments']['moneyness_multiplier'] = moneyness_mult
        base_result['adjusted_1min'] *= moneyness_mult
        
        return base_result
    
    def _get_static_baseline(self, symbol: str) -> float:
        """Get the pre-seeded 20-day baseline from Redis."""
        try:
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            baseline_key = DatabaseAwareKeyBuilder.live_volume_baseline(symbol)
            
            baseline_data = self.redis.hgetall(baseline_key)
            if baseline_data:
                baseline_1min = baseline_data.get('baseline_1min') or baseline_data.get(b'baseline_1min')
                if baseline_1min:
                    if isinstance(baseline_1min, bytes):
                        baseline_1min = baseline_1min.decode('utf-8')
                    return float(baseline_1min)
            
            # Fallback: Calculate from 20d average if available
            avg_20d = baseline_data.get('avg_volume_20d') or baseline_data.get(b'avg_volume_20d')
            if avg_20d:
                if isinstance(avg_20d, bytes):
                    avg_20d = avg_20d.decode('utf-8')
                return float(avg_20d) / self.TRADING_MINUTES_PER_DAY
            
            logger.warning(f"No baseline found for {symbol}, using fallback")
            return 1000.0  # Safe fallback
            
        except Exception as e:
            logger.error(f"Error getting static baseline for {symbol}: {e}")
            return 1000.0
    
    def _get_time_multiplier(self, timestamp: datetime, instrument_type: InstrumentType) -> float:
        """Get time-of-day multiplier based on market session."""
        current_time = timestamp.time()
        
        # Select appropriate multiplier set
        if instrument_type in [InstrumentType.FNO_INDEX, InstrumentType.FNO_STOCK, InstrumentType.FNO_OPTION]:
            multipliers = self.FNO_SESSION_MULTIPLIERS
        else:
            multipliers = self.EQUITY_SESSION_MULTIPLIERS
        
        for session_name, (start, end, multiplier) in multipliers.items():
            if start <= current_time < end:
                return multiplier
        
        return 1.0  # Default
    
    def _get_vix_multiplier(self) -> float:
        """Get VIX-based volatility multiplier."""
        try:
            vix_value = self.redis.get('index:NSINDIAVIX') or self.redis.get('index:INDIAVIX')
            if vix_value:
                if isinstance(vix_value, bytes):
                    vix_value = vix_value.decode('utf-8')
                vix = float(vix_value)
                
                # VIX regime multipliers (from user memory)
                if vix <= 12:  # Low/complacent
                    return 0.8
                elif vix < 20:  # Normal
                    return 1.0
                elif vix < 25:  # High
                    return 1.3
                else:  # Panic (>= 25)
                    return 1.6
        except Exception as e:
            logger.debug(f"Could not get VIX multiplier: {e}")
        
        return 1.0  # Default
    
    def _get_fno_adjustments(self, symbol: str, timestamp: datetime) -> Dict[str, float]:
        """Get F&O-specific adjustments."""
        adjustments = {}
        
        # Check day of week (Thursday typically has higher F&O volume)
        if timestamp.weekday() == 3:  # Thursday
            adjustments['thursday_effect'] = 1.15
        else:
            adjustments['thursday_effect'] = 1.0
        
        return adjustments
    
    def _is_expiry_week(self, symbol: str, timestamp: datetime) -> bool:
        """Check if current date is in expiry week."""
        # Parse expiry from symbol and compare
        # For now, simple check: is it Thursday?
        # TODO: Use MarketCalendar for accurate expiry detection
        return timestamp.weekday() == 3
    
    def _get_expiry_multiplier(self, timestamp: datetime) -> float:
        """Get expiry day/week multiplier."""
        if timestamp.weekday() == 3:  # Expiry day (Thursday)
            return 1.5
        return 1.0
    
    def _get_rollover_adjustment(self, symbol: str, timestamp: datetime) -> float:
        """Get adjustment for rollover period (last few days before expiry)."""
        # TODO: Implement proper rollover detection
        return 1.0
    
    def _get_moneyness_multiplier(self, symbol: str) -> float:
        """Get multiplier based on option moneyness (ATM options have higher volume)."""
        # TODO: Parse symbol, get underlying price, calculate moneyness
        # For now, return 1.0
        return 1.0