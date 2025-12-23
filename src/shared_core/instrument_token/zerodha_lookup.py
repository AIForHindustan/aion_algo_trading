"""
Zerodha instrument lookup helper.

Provides a cached interface to resolve Zerodha tradingsymbols into metadata
(instrument_token, exchange, lot_size, etc.). The lookup data comes from the
regularly refreshed JSON at `zerodha/core/data/token_lookup_enriched.json`.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from threading import Lock
from typing import Dict, Optional


def _default_lookup_paths() -> list[Path]:
    """Return candidate paths for the enriched lookup JSON."""
    repo_root = Path(__file__).resolve().parents[2]
    return [
        Path(
            os.environ.get(
                "ZERODHA_TOKEN_LOOKUP_PATH",
                repo_root / "zerodha/core/data/token_lookup_enriched.json",
            )
        ),
        repo_root / "shared_core/instrument_token/token_lookup_enriched.json",
    ]


def _normalize_tradingsymbol(symbol: str) -> str:
    """Normalize raw symbols (NFO prefixes, NSE:) to match Zerodha format."""
    sym = symbol.strip().upper()
    if sym.startswith("NFO"):
        sym = sym[3:]
    if sym.startswith("NSE:"):
        sym = sym[4:]
    if sym.startswith("BSE:"):
        sym = sym[4:]
    return sym.replace(" ", "").replace(":", "")


class ZerodhaInstrumentLookup:
    """Singleton-style cache for Zerodha instrument metadata."""

    _cache: Dict[str, Dict] = {}
    _loaded = False
    _lock = Lock()

    @classmethod
    def _ensure_loaded(cls) -> None:
        if cls._loaded:
            return
        with cls._lock:
            if cls._loaded:
                return
            for path in _default_lookup_paths():
                try:
                    with open(path, "r", encoding="utf-8") as fh:
                        payload = json.load(fh)
                        instruments = []
                        if isinstance(payload, list):
                             instruments = payload
                        elif isinstance(payload, dict):
                            # Check for 'instruments' or 'data' keys
                            if "instruments" in payload:
                                instruments = payload["instruments"]
                            elif "data" in payload:
                                instruments = payload["data"]
                            else:
                                # Assume it's a direct mapping of token -> details
                                # We need to convert it to a list or iterate directly
                                # The existing code iterates 'for inst in instruments'
                                # implying instruments is a list of dicts.
                                # If payload is {"123": {"tradingsymbol": "..."}}, values() gives lists of dicts.
                                instruments = list(payload.values())
                        
                        for inst in instruments:
                            tradingsymbol = inst.get("tradingsymbol")
                            if not tradingsymbol:
                                continue
                            cls._cache[_normalize_tradingsymbol(tradingsymbol)] = inst
                        cls._loaded = True
                        return
                except FileNotFoundError:
                    continue
                except Exception:
                    continue

    @classmethod
    def get_instrument(cls, symbol: str) -> Optional[Dict]:
        """Return enriched metadata for the given symbol if available."""
        cls._ensure_loaded()
        if not cls._cache:
            return None
        normalized = _normalize_tradingsymbol(symbol)
        if normalized in cls._cache:
            return cls._cache[normalized]
        alt = normalized.replace("NSE", "", 1) if normalized.startswith("NSE") else normalized
        return cls._cache.get(alt)
