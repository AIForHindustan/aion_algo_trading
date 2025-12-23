"""Read helpers for tick and indicator data."""

from __future__ import annotations

from typing import Dict, Any, Optional, List

from shared_core.redis_clients.unified_data_storage import get_unified_storage


def _get_storage(redis_client=None):
    if redis_client:
        return get_unified_storage(redis_client=redis_client)
    return get_unified_storage()


def get_tick(symbol: str, *, redis_client=None) -> Dict[str, Any]:
    """Return the latest tick snapshot."""
    storage = _get_storage(redis_client)
    return storage.get_tick_data(symbol)


def get_indicators(symbol: str, indicator_names: Optional[List[str]] = None, *, redis_client=None) -> Dict[str, Any]:
    """Return indicator hash fields."""
    storage = _get_storage(redis_client)
    return storage.get_indicators(symbol, indicator_names=indicator_names)


def get_greeks(symbol: str, *, redis_client=None) -> Dict[str, float]:
    """Return stored Greeks."""
    storage = _get_storage(redis_client)
    return storage.get_greeks(symbol)


def get_live_price(symbol: str, *, redis_client=None) -> Optional[float]:
    """Return latest cached live price, if available."""
    storage = _get_storage(redis_client)
    return storage.get_live_price(symbol)
