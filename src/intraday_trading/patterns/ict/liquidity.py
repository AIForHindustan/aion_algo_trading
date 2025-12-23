#!/opt/homebrew/bin/python3.13
"""
ICT Liquidity Detector utilities.
Updated: 2025-10-27
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, List, Optional, Any


class ICTLiquidityDetector:
    """Detects previous day/week highs/lows and simple stop-hunt zones."""

    def __init__(self, redis_client=None):
        self.redis_client = redis_client

    def detect_liquidity_pools(self, symbol: str) -> Dict[str, Optional[float]]:
        prev_day = self._get_previous_trading_day()
        weekly_dates = self._get_recent_trading_days(5)

        pd_high = self._get_session_high(symbol, prev_day)
        pd_low = self._get_session_low(symbol, prev_day)

        # Weekly from last up to 5 sessions (excluding today)
        w_high, w_low = self._get_week_extremes(symbol, weekly_dates)

        pools = {
            "previous_day_high": pd_high,
            "previous_day_low": pd_low,
            "weekly_high": w_high,
            "weekly_low": w_low,
        }
        pools["stop_hunt_zones"] = self._calculate_stop_hunt_zones(pools)
        return pools

    def _get_previous_trading_day(self) -> str:
        # Simple: yesterday; if weekend, walk back
        d = date.today() - timedelta(days=1)
        # Walk back up to 3 days to skip weekend
        for _ in range(3):
            if d.weekday() < 5:
                break
            d = d - timedelta(days=1)
        return d.isoformat()

    def _get_recent_trading_days(self, count: int) -> List[str]:
        days = []
        d = date.today() - timedelta(days=1)
        while len(days) < count and (date.today() - d).days <= 10:
            if d.weekday() < 5:
                days.append(d.isoformat())
            d = d - timedelta(days=1)
        return days

    def _get_session_high(self, symbol: str, session_date: Optional[str]) -> Optional[float]:
        """Get previous day high from OHLC data in Redis DB1."""
        if not session_date or not self.redis_client:
            return None
        try:
            # ✅ FIX: Use OHLC daily sorted set instead of session keys
            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards, DatabaseAwareKeyBuilder
            import json
            
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            ohlc_daily_key = DatabaseAwareKeyBuilder.live_ohlc_daily(canonical_symbol)
            
            # Get Redis client (handle wrappers)
            redis_client = self.redis_client
            if hasattr(redis_client, 'get_client'):
                redis_client = redis_client.get_client(1)  # DB1
            elif hasattr(redis_client, 'redis'):
                redis_client = redis_client.redis
            
            # ✅ OPTIMIZED: Get last 10 entries (most recent) instead of all entries
            # Sorted sets are ordered by timestamp, so most recent are at the end
            entries = redis_client.zrevrange(ohlc_daily_key, 0, 9, withscores=True)
            for entry, score in entries:
                if isinstance(entry, bytes):
                    entry = entry.decode('utf-8')
                try:
                    data = json.loads(entry)
                    entry_date = data.get('d') or data.get('date') or data.get('session_date')
                    if entry_date == session_date:
                        high = data.get('h') or data.get('high') or data.get('high_price')
                        if high and isinstance(high, (int, float)):
                            return float(high)
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Error fetching session high for {symbol} on {session_date}: {e}")
        return None

    def _get_session_low(self, symbol: str, session_date: Optional[str]) -> Optional[float]:
        """Get previous day low from OHLC data in Redis DB1."""
        if not session_date or not self.redis_client:
            return None
        try:
            # ✅ FIX: Use OHLC daily sorted set instead of session keys
            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards, DatabaseAwareKeyBuilder
            import json
            
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            ohlc_daily_key = DatabaseAwareKeyBuilder.live_ohlc_daily(canonical_symbol)
            
            # Get Redis client (handle wrappers)
            redis_client = self.redis_client
            if hasattr(redis_client, 'get_client'):
                redis_client = redis_client.get_client(1)  # DB1
            elif hasattr(redis_client, 'redis'):
                redis_client = redis_client.redis
            
            # ✅ OPTIMIZED: Get last 10 entries (most recent) instead of all entries
            # Sorted sets are ordered by timestamp, so most recent are at the end
            entries = redis_client.zrevrange(ohlc_daily_key, 0, 9, withscores=True)
            for entry, score in entries:
                if isinstance(entry, bytes):
                    entry = entry.decode('utf-8')
                try:
                    data = json.loads(entry)
                    entry_date = data.get('d') or data.get('date') or data.get('session_date')
                    if entry_date == session_date:
                        low = data.get('l') or data.get('low') or data.get('low_price')
                        if low and isinstance(low, (int, float)):
                            return float(low)
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Error fetching session low for {symbol} on {session_date}: {e}")
        return None

    def _get_week_extremes(self, symbol: str, session_dates: List[str]) -> (Optional[float], Optional[float]):
        """Get weekly high/low from OHLC daily data in Redis DB1."""
        highs: List[float] = []
        lows: List[float] = []
        if not self.redis_client:
            return None, None
        try:
            # ✅ FIX: Use OHLC daily sorted set instead of session keys
            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards, DatabaseAwareKeyBuilder
            import json
            
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            ohlc_daily_key = DatabaseAwareKeyBuilder.live_ohlc_daily(canonical_symbol)
            
            # Get Redis client (handle wrappers)
            redis_client = self.redis_client
            if hasattr(redis_client, 'get_client'):
                redis_client = redis_client.get_client(1)  # DB1
            elif hasattr(redis_client, 'redis'):
                redis_client = redis_client.redis
            
            # ✅ OPTIMIZED: Get last 10 entries (most recent) instead of all entries
            # Sorted sets are ordered by timestamp, so most recent are at the end
            entries = redis_client.zrevrange(ohlc_daily_key, 0, 9, withscores=True)
            session_dates_set = set(session_dates)
            
            for entry, score in entries:
                if isinstance(entry, bytes):
                    entry = entry.decode('utf-8')
                try:
                    data = json.loads(entry)
                    entry_date = data.get('d') or data.get('date') or data.get('session_date')
                    if entry_date in session_dates_set:
                        high = data.get('h') or data.get('high') or data.get('high_price')
                        low = data.get('l') or data.get('low') or data.get('low_price')
                        if high and isinstance(high, (int, float)):
                            highs.append(float(high))
                        if low and isinstance(low, (int, float)):
                            lows.append(float(low))
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Error fetching week extremes for {symbol}: {e}")
        
        return (max(highs) if highs else None, min(lows) if lows else None)

    def _calculate_stop_hunt_zones(self, pools: Dict[str, Optional[float]]) -> List[float]:
        levels = [
            pools.get("previous_day_high"),
            pools.get("previous_day_low"),
            pools.get("weekly_high"),
            pools.get("weekly_low"),
        ]
        hunt_zones: List[float] = []
        for level in levels:
            try:
                if level and level > 0:
                    hunt_zones.append(level * 1.005)
                    hunt_zones.append(level * 0.995)
            except Exception:
                continue
        return hunt_zones
