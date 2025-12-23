"""
Token cache manager for fast instrument metadata lookups.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple


class TokenCacheManager:
    """Load token metadata from core/data/token_lookup.json once."""

    def __init__(self, cache_path: Optional[str] = None, verbose: bool = True) -> None:
        default_path = Path("core/data/token_lookup.json")
        self.cache_path = Path(cache_path) if cache_path else default_path
        self.token_map: Dict[int, Dict] = {}
        self.symbol_map: Dict[str, int] = {}
        self.verbose = verbose
        self._load_token_cache()

    def _load_token_cache(self) -> None:
        if not self.cache_path.exists():
            print(f"⚠️ Token cache not found at {self.cache_path}, continuing without cache")
            return

        try:
            with self.cache_path.open("r", encoding="utf-8") as fh:
                raw_data = json.load(fh)

            iterator = []
            if isinstance(raw_data, dict):
                iterator = raw_data.items()
            elif isinstance(raw_data, list):
                iterator = ((entry.get("token"), entry) for entry in raw_data if isinstance(entry, dict))

            for token_key, metadata in iterator:
                if not isinstance(metadata, dict):
                    continue

                token = self._resolve_token(token_key, metadata)
                if token is None:
                    continue

                normalized, aliases = self._normalize_metadata_entry(token, metadata)
                self.token_map[token] = normalized
                for alias in aliases:
                    if alias and not alias.startswith("UNKNOWN_"):
                        self.symbol_map.setdefault(alias, token)

            if self.verbose:
                print(f"✅ Loaded {len(self.token_map)} instruments from {self.cache_path}")
        except Exception as exc:  # noqa: BLE001
            print(f"❌ Failed to load token cache ({self.cache_path}): {exc}")
            self.token_map = {}
            self.symbol_map = {}

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _resolve_token(self, token_key: Any, metadata: Dict[str, Any]) -> Optional[int]:
        token = self._safe_int(token_key)
        if token is not None and token >= 0:
            return token

        for candidate_key in ("token", "instrument_token", "tradingsymbol_token", "id"):
            candidate_value = metadata.get(candidate_key)
            token = self._safe_int(candidate_value)
            if token is not None and token >= 0:
                return token
        return None

    def _normalize_metadata_entry(self, token: int, metadata: Dict[str, Any]) -> Tuple[Dict, Iterable[str]]:
        symbol, aliases = self._extract_symbol(token, metadata)
        normalized = {
            "symbol": symbol,
            "exchange": metadata.get("exchange"),
            "segment": metadata.get("segment"),
            "instrument_type": metadata.get("instrument_type"),
            "lot_size": metadata.get("lot_size"),
            "tick_size": metadata.get("tick_size"),
            "expiry": metadata.get("expiry"),
            "strike_price": metadata.get("strike_price") or metadata.get("strike"),
            "option_type": metadata.get("option_type") or metadata.get("right"),
            "is_expired": metadata.get("is_expired"),
        }
        return normalized, aliases

    def _extract_symbol(self, token: int, metadata: Dict[str, Any]) -> Tuple[str, Iterable[str]]:
        raw_candidates = [
            metadata.get("tradingsymbol"),
            metadata.get("symbol"),
            metadata.get("name"),
        ]

        key_field = metadata.get("key")
        if key_field:
            raw_candidates.append(key_field)
            if isinstance(key_field, str) and ":" in key_field:
                raw_candidates.append(key_field.split(":")[-1])

        candidates = []
        for candidate in raw_candidates:
            if not candidate:
                continue
            cand_str = str(candidate).strip()
            if cand_str:
                candidates.append(cand_str)

        unique_candidates = list(dict.fromkeys(candidates))

        if unique_candidates:
            symbol = unique_candidates[0]
        else:
            symbol = f"UNKNOWN_{token}"
            unique_candidates = [symbol]
        return symbol, unique_candidates

    def get_instrument_info(self, instrument_token: int) -> Dict:
        """Return cached instrument metadata, or a fallback entry."""
        return self.token_map.get(
            instrument_token,
            {
                "symbol": f"UNKNOWN_{instrument_token}",
                "exchange": None,
                "segment": None,
                "instrument_type": None,
                "lot_size": None,
                "tick_size": None,
                "expiry": None,
            },
        )

    def token_to_symbol(self, instrument_token: int) -> str:
        """Return cached symbol or UNKNOWN placeholder."""
        return self.get_instrument_info(instrument_token)["symbol"]

    def symbol_to_token(self, symbol: str) -> Optional[int]:
        """Return cached token for symbol, if available."""
        return self.symbol_map.get(symbol)
