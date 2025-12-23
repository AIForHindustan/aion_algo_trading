"""Symbol/tick normalization helpers used across crawlers and pipelines."""

from __future__ import annotations

from typing import Dict, Any

from shared_core.redis_clients import normalize_redis_tick_data
from shared_core.redis_clients.redis_key_standards import RedisKeyStandards


def normalize_tick_data(tick: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize incoming tick payload into canonical field names."""
    return normalize_redis_tick_data(tick)


def canonical_symbol(symbol: str) -> str:
    """Return the canonical Redis symbol form."""
    return RedisKeyStandards.canonical_symbol(symbol)
