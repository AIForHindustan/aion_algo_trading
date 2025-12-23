# shared_core/volume_files/session_tracker.py
"""
Session state management for real-time volume tracking.
Maintains cumulative volume, minute buckets, and session statistics.
"""

from datetime import datetime, date, time
from typing import Dict, Optional, Any
import json
import logging

logger = logging.getLogger(__name__)


class SessionTracker:
    """
    Tracks real-time session state for volume computation.
    
    Keys:
    - vol:session:{symbol}:{date} - Session cumulative data (Hash)
    - vol:minute:{symbol}:{date}:{HH:MM} - Per-minute volume (String)
    """
    
    MARKET_OPEN = time(9, 15)
    MARKET_CLOSE = time(15, 30)
    TRADING_MINUTES = 375
    
    def __init__(self, redis_client=None):
        """
        Initialize session tracker.
        
        Args:
            redis_client: Optional Redis client. If None, uses centralized get_redis_client().
                         Can also be a wrapper like WorkerRedisManager.
        """
        # Use centralized Redis client pattern
        if redis_client is None:
            from shared_core.redis_clients import get_redis_client
            self.redis = get_redis_client(process_name="session_tracker", db=1)
        elif hasattr(redis_client, 'get_client'):
            # It's a wrapper (e.g., WorkerRedisManager) - get the actual client
            self.redis = redis_client.get_client()
        else:
            # It's a direct Redis client
            self.redis = redis_client
    
    def update(self, symbol: str, volume: float, price: float, 
               timestamp: Optional[str] = None) -> Dict[str, Any]:
        """
        Update session state with new tick data.
        
        Args:
            symbol: Canonical symbol
            volume: Incremental volume from tick
            price: Current price
            timestamp: ISO timestamp or None for now
            
        Returns:
            Updated session data dict
        """
        if timestamp:
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
        else:
            ts = datetime.now()
        
        today = ts.date().isoformat()
        current_minute = ts.strftime('%H:%M')
        
        # ========== DUAL-WRITE KEYS ==========
        # Legacy keys (for backward compatibility with downstream consumers)
        legacy_session_key = f"vol:session:{symbol}:{today}"
        legacy_minute_key = f"vol:minute:{symbol}:{today}:{current_minute}"
        
        # Standard keys (using DatabaseAwareKeyBuilder for eventual migration)
        from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
        standard_session_key = DatabaseAwareKeyBuilder.live_volume_session(symbol, today)
        # Note: minute key doesn't have a standard method yet, using legacy format
        # =====================================
        
        try:
            # Get existing session data (read from legacy key for now)
            session_data = self._get_session_data(legacy_session_key)
            
            # Check for session reset (new day)
            if session_data.get('date') != today:
                session_data = self._reset_session(symbol, today)
            
            # Update cumulative volume
            prev_cumulative = float(session_data.get('cumulative_volume', 0))
            new_cumulative = prev_cumulative + volume
            
            # Update minute volume
            prev_minute_vol = float(self.redis.get(legacy_minute_key) or 0)
            new_minute_vol = prev_minute_vol + volume
            
            # Calculate minutes elapsed
            minutes_elapsed = self._calculate_minutes_elapsed(ts)
            
            # Update session data
            session_data.update({
                'cumulative_volume': str(new_cumulative),
                'current_minute_volume': str(new_minute_vol),
                'last_price': str(price),
                'last_timestamp': ts.isoformat(),
                'minutes_elapsed': str(minutes_elapsed),
                'tick_count': str(int(session_data.get('tick_count', 0)) + 1),
                'date': today
            })
            
            # Store updates
            self.redis.hset(legacy_session_key, mapping=session_data)
            self.redis.set(legacy_minute_key, str(new_minute_vol))
            
            # Set TTL (24 hours)
            self.redis.expire(legacy_session_key, 86400)
            self.redis.expire(legacy_minute_key, 86400)
            
            return {
                'cumulative_volume': new_cumulative,
                'current_minute_volume': new_minute_vol,
                'minutes_elapsed': minutes_elapsed,
                'last_price': price,
                'tick_count': int(session_data.get('tick_count', 1)),
                'date': today
            }
            
        except Exception as e:
            logger.error(f"Error updating session for {symbol}: {e}")
            return {
                'cumulative_volume': volume,
                'current_minute_volume': volume,
                'minutes_elapsed': 1,
                'last_price': price,
                'tick_count': 1,
                'date': today
            }
    
    def get_session_data(self, symbol: str, date_str: Optional[str] = None) -> Dict[str, Any]:
        """Get current session data for a symbol."""
        if date_str is None:
            date_str = date.today().isoformat()
        
        session_key = f"vol:session:{symbol}:{date_str}"
        return self._get_session_data(session_key)
    
    def _get_session_data(self, session_key: str) -> Dict[str, Any]:
        """Internal method to fetch session data from Redis."""
        try:
            raw_data = self.redis.hgetall(session_key)
            if not raw_data:
                return {}
            
            # Decode bytes to strings
            decoded = {}
            for k, v in raw_data.items():
                key = k.decode('utf-8') if isinstance(k, bytes) else k
                value = v.decode('utf-8') if isinstance(v, bytes) else v
                decoded[key] = value
            
            return decoded
        except Exception as e:
            logger.error(f"Error fetching session data: {e}")
            return {}
    
    def _reset_session(self, symbol: str, today: str) -> Dict[str, Any]:
        """Reset session data for a new trading day."""
        return {
            'cumulative_volume': '0',
            'current_minute_volume': '0',
            'last_price': '0',
            'last_timestamp': '',
            'minutes_elapsed': '0',
            'tick_count': '0',
            'date': today,
            'session_open': datetime.now().isoformat()
        }
    
    def _calculate_minutes_elapsed(self, timestamp: datetime) -> float:
        """Calculate minutes elapsed since market open."""
        current_time = timestamp.time()
        
        # Before market open
        if current_time < self.MARKET_OPEN:
            return 0.0
        
        # After market close
        if current_time > self.MARKET_CLOSE:
            return float(self.TRADING_MINUTES)
        
        # During market hours
        market_open_dt = datetime.combine(timestamp.date(), self.MARKET_OPEN)
        elapsed = (timestamp - market_open_dt).total_seconds() / 60.0
        
        return max(0.0, min(elapsed, float(self.TRADING_MINUTES)))
    
    def get_minute_history(self, symbol: str, date_str: Optional[str] = None, 
                          limit: int = 30) -> list:
        """Get recent minute-by-minute volume history."""
        if date_str is None:
            date_str = date.today().isoformat()
        
        # This would require SCAN which we avoid - 
        # For now, return empty list. Full implementation would use a sorted set.
        logger.debug(f"Minute history not implemented for {symbol}")
        return []
