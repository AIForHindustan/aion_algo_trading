"""Compatibility wrapper that delegates to shared_core.core.storage.

All new code should import from `shared_core.core.storage` directly.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from shared_core.core.storage import (
    data_root as _data_root,
    parquet_root as _parquet_root,
    ensure_dirs as _ensure_dirs,
    path_for as _path_for,
    DEFAULT_PARQUET_COMPRESSION,
    DEFAULT_PARQUET_COMPRESSION_LEVEL,
)


def get_data_root() -> Path:
    return _data_root()


def get_parquet_root() -> Path:
    return _parquet_root()


def ensure_storage_dirs() -> None:
    _ensure_dirs()


def path_for(category: str, crawler: Optional[str] = None) -> Path:
    return _path_for(category, crawler)
