"""
Shared-core storage manager: single source of truth for on-disk data locations.

All producers (crawlers, mirrors, ingesters) should use this module to resolve
where to write Parquet/JSONL/etc., so ingestion can be tracked and reconciled.

Default layout under repo root:

data_store/
  parquet/
    ticks_canonical/
    alerts/
    crawler/
      zerodha/
      zerodha_websocket/
      ubuntu_crawlers/
  raw/
    jsonl/
    parquet/
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional


def _repo_root() -> Path:
    # Resolve based on this file location (shared_core/core/storage/__init__.py)
    # __file__ → storage → core → shared_core → src → repo root
    return Path(__file__).resolve().parents[4]


def data_root() -> Path:
    return _repo_root() / 'data_store'


def parquet_root() -> Path:
    return data_root() / 'parquet'


def ensure_dirs() -> None:
    base = parquet_root()
    for sub in [
        base / 'ticks_canonical',
        base / 'alerts',
        base / 'crawler' / 'zerodha',
        base / 'crawler' / 'zerodha_websocket',
        base / 'crawler' / 'ubuntu_crawlers',
    ]:
        sub.mkdir(parents=True, exist_ok=True)


def path_for(category: str, crawler: Optional[str] = None) -> Path:
    base = parquet_root()
    if crawler:
        return base / 'crawler' / crawler / category
    return base / category


DEFAULT_PARQUET_COMPRESSION = 'zstd'
DEFAULT_PARQUET_COMPRESSION_LEVEL = 19

