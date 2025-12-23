"""Thin wrapper exposing the shared SymbolResolver interface."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

from shared_core.redis_clients.redis_key_standards import (
    SymbolResolver as _SymbolResolver,
    get_symbol_resolver as _get_symbol_resolver,
)

SymbolResolver = _SymbolResolver
__all__ = ["SymbolResolver", "get_symbol_resolver"]


def get_symbol_resolver(metadata_path: Optional[Union[str, Path]] = None) -> SymbolResolver:
    """Return the shared SymbolResolver instance (backed by redis_key_standards)."""
    return _get_symbol_resolver(metadata_path=metadata_path)
