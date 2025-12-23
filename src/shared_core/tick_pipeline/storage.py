"""Convenience wrappers around UnifiedDataStorage for tick writes."""

from __future__ import annotations

from typing import Dict, Any, Optional, List

from shared_core.redis_clients.unified_data_storage import (
    UnifiedDataStorage,
    get_unified_storage,
)


def _get_storage(redis_client=None):
    if redis_client:
        return get_unified_storage(redis_client=redis_client)
    return get_unified_storage()


def store_tick(symbol: str, data: Dict[str, Any], ttl: int = 3600, *, redis_client=None) -> None:
    """Persist the latest tick snapshot (ticks:latest:{symbol})."""
    storage = _get_storage(redis_client)
    storage.store_tick_data(symbol, data, ttl)


def store_indicators(symbol: str, indicators: Dict[str, Any], ttl: Optional[int] = None, *, redis_client=None) -> bool:
    """Persist indicator fields into ind:{symbol} hash."""
    if not indicators:
        return False
    storage = _get_storage(redis_client)
    return storage.store_indicators(symbol, indicators, ttl)


def store_greeks(symbol: str, greeks: Dict[str, Any], ttl: int = 86400, *, redis_client=None) -> bool:
    """Persist Greeks using the canonical live_greeks hash."""
    if not greeks:
        return False
    storage = _get_storage(redis_client)
    return storage.store_greeks(symbol, greeks, ttl)


def get_all_greek_names() -> List[str]:
    """Expose the canonical Greek list."""
    return list(UnifiedDataStorage.GREEK_NAMES)
