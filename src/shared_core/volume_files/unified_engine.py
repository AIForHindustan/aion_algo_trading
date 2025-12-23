# shared_core/volume_files/unified_engine.py
"""
Single source of truth for all volume computations.
Replaces scattered volume logic with a unified, testable engine.
"""

from datetime import datetime, date
from typing import Dict, Optional, Any
import logging

from .baseline_service import BaselineService, InstrumentType
from .session_tracker import SessionTracker
from .key_usage_tracker import get_key_tracker

logger = logging.getLogger(__name__)


class UnifiedVolumeEngine:
    """
    Single source of truth for all volume computations.
    
    This engine:
    1. Categorizes instruments (F&O vs Equity)
    2. Gets appropriate baselines with adjustments
    3. Tracks session state (cumulative, minute volumes)
    4. Computes all volume ratios
    5. Writes to both new unified keys AND legacy keys
    """
    
    def __init__(self, redis_client=None):
        """
        Initialize unified volume engine.
        
        Args:
            redis_client: Optional Redis client. If None, uses centralized get_redis_client().
                         Can also be a wrapper like WorkerRedisManager.
        """
        # Use centralized Redis client pattern
        if redis_client is None:
            # Import here to avoid circular imports
            from shared_core.redis_clients import get_redis_client
            self.redis = get_redis_client(process_name="unified_volume_engine", db=1)
        elif hasattr(redis_client, 'get_client'):
            # It's a wrapper (e.g., WorkerRedisManager) - get the actual client
            self.redis = redis_client.get_client()
        else:
            # It's a direct Redis client
            self.redis = redis_client
        
        # Use the resolved client for sub-services
        self.baseline_service = BaselineService(self.redis)
        self.session_tracker = SessionTracker(self.redis)
    
    def process_tick(self, symbol: str, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single tick - replaces all scattered computation logic.
        
        Args:
            symbol: Canonical symbol (e.g., NFOBANKNIFTY27JAN60000CE)
            tick_data: Dict with 'volume', 'price', 'timestamp' keys
            
        Returns:
            Unified result dict with baselines, session data, ratios
        """
        try:
            # 1. Categorize instrument (F&O vs Equity)
            instrument_type = self._categorize_instrument(symbol)
            
            # 2. Get or compute baseline
            timestamp = tick_data.get('timestamp')
            if isinstance(timestamp, str):
                ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            elif isinstance(timestamp, (int, float)):
                # Convert epoch timestamp to datetime
                # Handle milliseconds vs seconds
                if timestamp > 1e12:  # Milliseconds
                    ts = datetime.fromtimestamp(timestamp / 1000)
                else:  # Seconds
                    ts = datetime.fromtimestamp(timestamp)
            elif isinstance(timestamp, datetime):
                ts = timestamp
            else:
                ts = datetime.now()
            
            baseline = self.baseline_service.get_baseline(
                symbol=symbol,
                instrument_type=instrument_type,
                timestamp=ts
            )
            
            # 3. Update session state
            volume = float(tick_data.get('volume', 0))
            price = float(tick_data.get('price', 0))
            
            session_data = self.session_tracker.update(
                symbol=symbol,
                volume=volume,
                price=price,
                timestamp=ts  # Use converted datetime, not raw timestamp
            )
            
            # 4. Compute all ratios in ONE place
            ratios = self._compute_all_ratios(
                baseline=baseline,
                session_data=session_data,
                instrument_type=instrument_type
            )
            
            # 5. Build unified result
            unified_data = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'baselines': baseline,
                'session': session_data,
                'ratios': ratios,
                'instrument_type': instrument_type.value
            }
            
            # 6. Store in unified format AND legacy keys
            self._store_with_compatibility(symbol, unified_data)
            
            return unified_data
            
        except Exception as e:
            logger.error(f"Error processing tick for {symbol}: {e}")
            return {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'baselines': {},
                'session': {},
                'ratios': {'cumulative': 0.0, 'minute': 0.0}
            }
    
    def _categorize_instrument(self, symbol: str) -> InstrumentType:
        """Determine instrument type from symbol."""
        symbol_upper = symbol.upper()
        
        # Check for option suffixes
        if symbol_upper.endswith('CE') or symbol_upper.endswith('PE'):
            return InstrumentType.FNO_OPTION
        
        # Check for futures
        if symbol_upper.endswith('FUT'):
            # Check if it's an index future
            if any(idx in symbol_upper for idx in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX']):
                return InstrumentType.FNO_INDEX
            return InstrumentType.FNO_STOCK
        
        # Check for index options (contain index name but not FUT)
        if any(idx in symbol_upper for idx in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX']):
            if symbol_upper.endswith('CE') or symbol_upper.endswith('PE'):
                return InstrumentType.FNO_INDEX
        
        # Default to equity
        return InstrumentType.EQUITY
    
    def _compute_all_ratios(self, baseline: Dict, session_data: Dict, 
                           instrument_type: InstrumentType) -> Dict[str, float]:
        """Compute all volume ratios in one place - single source of truth."""
        
        cumulative_volume = float(session_data.get('cumulative_volume', 0))
        current_minute_volume = float(session_data.get('current_minute_volume', 0))
        minutes_elapsed = float(session_data.get('minutes_elapsed', 1))
        
        # Get adjusted baseline
        baseline_1min = float(baseline.get('adjusted_1min', 0))
        
        if baseline_1min <= 0:
            baseline_1min = float(baseline.get('static_1min', 1000))
        
        # Ensure we have valid denominators
        minutes_elapsed = max(1.0, minutes_elapsed)
        baseline_1min = max(1.0, baseline_1min)
        
        # CUMULATIVE RATIO (main metric)
        # Formula: cumulative_volume / (baseline_1min * minutes_elapsed)
        expected_cumulative = baseline_1min * minutes_elapsed
        cumulative_ratio = cumulative_volume / expected_cumulative if expected_cumulative > 0 else 0.0
        
        # PER-MINUTE RATIO (real-time signal)
        minute_ratio = current_minute_volume / baseline_1min if baseline_1min > 0 else 0.0
        
        # Cap ratios at reasonable values to prevent outliers
        cumulative_ratio = self._reasonable_cap(cumulative_ratio)
        minute_ratio = self._reasonable_cap(minute_ratio)
        
        return {
            'cumulative': cumulative_ratio,
            'minute': minute_ratio,
            'expected_cumulative': expected_cumulative,
            'baseline_1min': baseline_1min
        }
    
    def _reasonable_cap(self, ratio: float, max_ratio: float = 50.0) -> float:
        """Cap ratio at a reasonable maximum to prevent outliers."""
        if ratio < 0:
            return 0.0
        if ratio > max_ratio:
            return max_ratio
        return ratio
    
    def _store_with_compatibility(self, symbol: str, data: Dict[str, Any]):
        """Store data in new format AND maintain legacy keys."""
        try:
            today = date.today().isoformat()
            
            # 1. Store in NEW unified key (for future use)
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            unified_key = DatabaseAwareKeyBuilder.live_volume_session(symbol, today)
            
            # Flatten for Redis hash storage
            flat_data = {
                'symbol': symbol,
                'timestamp': data.get('timestamp', ''),
                'cumulative_ratio': str(data.get('ratios', {}).get('cumulative', 0)),
                'minute_ratio': str(data.get('ratios', {}).get('minute', 0)),
                'cumulative_volume': str(data.get('session', {}).get('cumulative_volume', 0)),
                'baseline_1min': str(data.get('ratios', {}).get('baseline_1min', 0)),
                'instrument_type': str(data.get('instrument_type', 'equity'))
            }
            
            self.redis.hset(unified_key, mapping=flat_data)
            self.redis.expire(unified_key, 86400)
            
            # Track new key write
            get_key_tracker().track_write(unified_key, is_legacy=False)
            
            # 2. CRITICAL: Also write to LEGACY keys for backward compatibility
            self._write_to_legacy_keys(symbol, data)
            
        except Exception as e:
            logger.error(f"Error storing with compatibility for {symbol}: {e}")
    
    def _write_to_legacy_keys(self, symbol: str, data: Dict[str, Any]):
        """Write to existing Redis keys that downstream consumers expect."""
        try:
            ratios = data.get('ratios', {})
            session = data.get('session', {})
            
            # Write to ind:{symbol} (indicators hash) - scanner reads this
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            ind_key = DatabaseAwareKeyBuilder.live_indicator_hash(symbol)
            
            self.redis.hset(ind_key, mapping={
                'volume_ratio': str(ratios.get('cumulative', 0)),
                'per_minute_ratio': str(ratios.get('minute', 0))
            })
            self.redis.expire(ind_key, 86400)
            
            # Track legacy key write
            get_key_tracker().track_write(ind_key, is_legacy=True)
            
            # Session data is already written by SessionTracker
            # No need to duplicate here
            
        except Exception as e:
            logger.error(f"Error writing legacy keys for {symbol}: {e}")