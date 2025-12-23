"""
Angel One instrument lookup helper.

Uses the master instrument dump plus the intraday mapping to expose metadata
for instruments that are approved for auto-execution.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional, Set


def _intraday_path() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    return Path(
        os.environ.get("ANGEL_INTRADAY_FILE", repo_root / "aion_trading_dashboard" / "backend" / "angel_one_intraday.json")
    )


def _master_path() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    return Path(
        os.environ.get("ANGEL_MASTER_FILE", repo_root / "angel_one_instruments_master.json")
    )


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace(" ", "")


class AngelOneInstrumentLookup:
    """Singleton cache filtered to Angel intraday instruments."""

    _cache: Dict[str, Dict] = {}
    _tokens: Set[str] = set()
    _loaded = False
    _lock = Lock()

    @classmethod
    def _ensure_loaded(cls) -> None:
        if cls._loaded:
            return
        with cls._lock:
            if cls._loaded:
                return
            intraday_tokens: Set[str] = set()
            try:
                with _intraday_path().open("r", encoding="utf-8") as fh:
                    payload = json.load(fh)
                    raw_tokens = payload.get("tokens") or []
                    intraday_tokens = {str(token) for token in raw_tokens}
            except FileNotFoundError:
                intraday_tokens = set()
            except Exception:
                intraday_tokens = set()

            try:
                with _master_path().open("r", encoding="utf-8") as fh:
                    instruments = json.load(fh)
                    for inst in instruments:
                        token = str(inst.get("token") or inst.get("symboltoken") or "")
                        if intraday_tokens and token not in intraday_tokens:
                            continue
                        symbol = inst.get("symbol")
                        if not symbol:
                            continue
                        normalized = cls._normalize_instrument(inst)
                        cls._cache[_normalize_symbol(symbol)] = normalized
                        cls._tokens.add(normalized.get("symboltoken") or token)
                cls._loaded = True
            except FileNotFoundError:
                cls._cache = {}
                cls._tokens = set()
                cls._loaded = True
            except Exception:
                cls._cache = {}
                cls._tokens = set()
                cls._loaded = True

    @classmethod
    def get_instrument(cls, symbol: str) -> Optional[Dict]:
        cls._ensure_loaded()
        if not cls._cache:
            return None
        normalized = _normalize_symbol(symbol)
        inst = cls._cache.get(normalized)
        if inst:
            return inst
        # Some Angel symbols use suffixes like -EQ; try matching against that form
        alt = normalized.replace("NSE:", "").replace("BSE:", "")
        return cls._cache.get(alt)

    @classmethod
    def is_intraday_token(cls, token: str) -> bool:
        cls._ensure_loaded()
        return str(token) in cls._tokens

    @staticmethod
    def _normalize_instrument(inst: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize numeric fields (strike, lot, tick) and ensure symboltoken exists."""
        normalized = dict(inst)
        symboltoken = inst.get("symboltoken") or inst.get("token")
        if symboltoken is not None:
            normalized["symboltoken"] = str(symboltoken)
        instrument_type = (inst.get("instrumenttype") or "").upper()

        strike_val = inst.get("strike")
        try:
            strike_float = float(strike_val)
        except (TypeError, ValueError):
            strike_float = None
        if strike_float is not None:
            if instrument_type.startswith("OPT") and strike_float > 1000:
                strike_float = strike_float / 100.0
            normalized["strike"] = strike_float

        lot_val = inst.get("lotsize")
        try:
            normalized["lot_size"] = int(float(lot_val))
        except (TypeError, ValueError):
            normalized["lot_size"] = None

        tick_val = inst.get("tick_size")
        try:
            normalized["tick_size"] = float(tick_val)
        except (TypeError, ValueError):
            normalized["tick_size"] = None

        return normalized
