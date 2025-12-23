"""
Redis OHLC Key Utilities - Symbol Normalization & Key Construction
================================================================

Essential utilities for Redis OHLC data management, symbol normalization,
and trading day calculations. Used extensively across the trading system
for consistent Redis key patterns and data access.

Key Functions:
- normalize_symbol(): Standardizes symbols for Redis keys (removes spaces, special chars)
- ohlc_daily_zset(): Creates daily OHLC sorted set keys
- ohlc_latest_hash(): Creates latest OHLC hash keys  
- ohlc_stats_hash(): Creates OHLC statistics hash keys
- ohlc_stream(): Creates OHLC update stream keys
- get_trading_days(): Calculates trading days around anchor dates

Redis Key Patterns:
- ohlc:{symbol}:{interval} - Time series data
- ohlc_daily:{symbol} - Daily OHLC sorted sets
- ohlc_latest:{symbol} - Latest OHLC hash
- ohlc_stats:{symbol} - OHLC statistics hash
- ohlc_updates:{symbol} - OHLC update streams

Used by: Core system, pattern detection, alert management, data pipeline,
volume calculations, and scanner components.

Author: Trading System Team
Last Updated: October 26, 2025
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Final, List
from shared_core.redis_clients.redis_key_standards import RedisKeyStandards

# ✅ FIXED: Use RedisKeyStandards.canonical_symbol() instead of normalize_symbol()
def normalize_symbol(symbol: str | None) -> str:
    """
    Normalize symbols used for Redis OHLC keys.
    
    ✅ FIXED: Delegates to RedisKeyStandards.canonical_symbol() for consistency.
    """
    return RedisKeyStandards.canonical_symbol(symbol) if symbol else ""


def ohlc_timeseries_key(symbol: str, interval: str = "1d") -> str:
    return f"ohlc:{normalize_symbol(symbol)}:{interval}"


def ohlc_daily_zset(symbol: str) -> str:
    return f"ohlc_daily:{normalize_symbol(symbol)}"


def ohlc_hourly_zset(symbol: str) -> str:
    return f"ohlc_hourly:{normalize_symbol(symbol)}"


def ohlc_latest_hash(symbol: str) -> str:
    return f"ohlc_latest:{normalize_symbol(symbol)}"


def ohlc_stats_hash(symbol: str) -> str:
    return f"ohlc_stats:{normalize_symbol(symbol)}"


def ohlc_stream(symbol: str) -> str:
    return f"ohlc_updates:{normalize_symbol(symbol)}"


def _is_trading_day(date_obj: datetime) -> bool:
    """Return True if the date is considered a trading day (weekdays only)."""
    return date_obj.weekday() < 5  # Monday (0) to Friday (4)


def get_trading_days(
    anchor: datetime,
    *,
    days_back: int = 0,
    days_forward: int = 0,
    include_anchor: bool = False,
) -> List[datetime]:
    """
    Return trading days around the anchor date.

    Args:
        anchor: Reference datetime.
        days_back: Number of prior trading days to include.
        days_forward: Number of forward trading days to include.
        include_anchor: Whether to include the anchor date if it is a trading day.
    """
    anchor_date = anchor.replace(hour=0, minute=0, second=0, microsecond=0)
    results: List[datetime] = []

    # Backward search
    days_found = 0
    check_date = anchor_date
    while days_found < days_back:
        check_date -= timedelta(days=1)
        if _is_trading_day(check_date):
            results.append(check_date)
            days_found += 1

    results.sort()

    # Include anchor if requested
    if include_anchor and _is_trading_day(anchor_date):
        results.append(anchor_date)

    # Forward search
    days_found = 0
    check_date = anchor_date
    while days_found < days_forward:
        check_date += timedelta(days=1)
        if _is_trading_day(check_date):
            results.append(check_date)
            days_found += 1

    return results
