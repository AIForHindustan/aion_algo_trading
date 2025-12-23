"""Indicator lifecycle that wraps HybridCalculations + Redis storage."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

from intraday_trading.intraday_scanner.calculations import HybridCalculations, UnifiedGreeksCalculator
import re
from shared_core.redis_clients.redis_key_standards import RedisKeyStandards, DatabaseAwareKeyBuilder

# ✅ DEPRECATED: StorageEngine replaced by UnifiedDataStorage
# from ..storage.engine import StorageEngine
from shared_core.redis_clients.unified_data_storage import get_unified_storage

logger = logging.getLogger(__name__)


class IndicatorLifecycleManager:
    """Single entrypoint for indicator retrieval/calculation/storage."""

    def __init__(
        self,
        storage_engine=None,  # ✅ DEPRECATED: Accept for backward compatibility, but use UnifiedDataStorage
        *,
        redis_client=None,
        health_monitor=None,
        unified_storage=None,  # ✅ NEW: Preferred way to pass UnifiedDataStorage
    ) -> None:
        # ✅ CENTRALIZED: Use UnifiedDataStorage instead of legacy StorageEngine
        if unified_storage is not None:
            self.unified_storage = unified_storage
        elif storage_engine is not None:
            self.unified_storage = get_unified_storage(
                redis_client=redis_client or getattr(storage_engine, "redis_client", None)
            )
        else:
            self.unified_storage = get_unified_storage(redis_client=redis_client)
        
        # ✅ DEPRECATED: Keep storage_engine for backward compatibility (will be removed)
        self.storage_engine = storage_engine
        self.redis_client = redis_client or getattr(self.unified_storage, "redis_client", None)
        self.health_monitor = health_monitor

        self.hybrid_calc = HybridCalculations(redis_client=self.redis_client)
        self.greeks_calc = UnifiedGreeksCalculator("NSE")
        # ✅ FIXED: Pass HybridCalculations instance to fallback provider to avoid duplicate calculations
        self.fallback_provider = TwentyDayFallbackProvider(self.redis_client, hybrid_calc=self.hybrid_calc)

        self._load_indicator_requirements()

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    async def get_indicators(
        self,
        symbol: str,
        required_indicators: Sequence[str],
        context_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        context_data = context_data or {}
        if not required_indicators:
            return {}

        logger.debug("Fetching %s indicators for %s", len(required_indicators), symbol)
        # ✅ CENTRALIZED: Use UnifiedDataStorage instead of legacy StorageEngine
        cached = self.unified_storage.get_indicators(symbol, required_indicators) or {}

        missing = set(required_indicators) - set(cached.keys())
        if missing:
            calculated = await self._calculate_missing_indicators(symbol, missing, context_data)
            if calculated:
                # ✅ CENTRALIZED: Use UnifiedDataStorage instead of legacy StorageEngine
                self.unified_storage.store_indicators(symbol, calculated)
                cached.update(calculated)

        final = self._ensure_complete_indicator_set(
            symbol,
            cached,
            list(required_indicators),
            context_data,
        )
        quality = self._validate_indicator_quality(final)
        logger.debug("Indicator payload ready: %s items, quality %.1f%%", len(final), quality * 100)
        return final

    def get_required_indicators(self, symbol: str) -> List[str]:
        symbol_type = self._classify_symbol_type(symbol)
        return list(self.requirements.get(symbol_type, {}).get("required", []))

    def get_indicators_sync(
        self,
        symbol: str,
        required_indicators: Sequence[str],
        context_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Synchronous wrapper for get_indicators().
        Uses asyncio.run() or thread pool to execute async method.
        
        This allows IndicatorLifecycleManager to be used in synchronous contexts
        like MarketScanner and DataPipeline.
        """
        try:
            # Check if event loop is already running
            try:
                asyncio.get_running_loop()
                # Loop is running, use thread pool executor to run async code in separate thread
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(
                        lambda: asyncio.run(
                            self.get_indicators(symbol, required_indicators, context_data)
                        )
                    )
                    return future.result(timeout=5.0)
            except RuntimeError:
                # No running loop, can use asyncio.run() directly
                return asyncio.run(
                    self.get_indicators(symbol, required_indicators, context_data)
                )
        except Exception as e:
            logger.warning(f"Sync wrapper failed for {symbol}: {e}, falling back to direct calculation")
            # Fallback to direct HybridCalculations
            return self._calculate_direct_fallback(symbol, required_indicators, context_data)

    def _calculate_direct_fallback(
        self,
        symbol: str,
        required_indicators: Sequence[str],
        context_data: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Fallback to direct HybridCalculations if async fails"""
        tick_data = self._extract_tick_data(symbol, context_data or {})
        if not tick_data:
            return {}
        
        calculated = self.hybrid_calc.batch_calculate_indicators(tick_data, symbol)
        return {ind: calculated.get(ind) for ind in required_indicators if ind in calculated}

    # ------------------------------------------------------------------ #
    # Calculation helpers
    # ------------------------------------------------------------------ #
    async def _calculate_missing_indicators(
        self,
        symbol: str,
        missing: Set[str],
        context_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        categories = self._categorize_indicators(missing)
        tick_data = self._extract_tick_data(symbol, context_data)

        if "ta" in categories or "volume" in categories:
            ta_needed = categories.get("ta", set()) | categories.get("volume", set())
            hybrid_values = await self._calculate_with_hybrid(symbol, tick_data, ta_needed)
            results.update(hybrid_values)

        if "greeks" in categories:
            greek_values = await self._calculate_greeks(symbol, tick_data, context_data, categories["greeks"])
            results.update(greek_values)

        if self.health_monitor:
            self.health_monitor.record_calculation_attempt(bool(results))
        return results

    async def _calculate_with_hybrid(
        self,
        symbol: str,
        tick_data: List[Dict[str, Any]],
        indicators: Set[str],
    ) -> Dict[str, Any]:
        if not tick_data:
            logger.debug("No tick data available for %s – skipping hybrid calculation", symbol)
            return {}

        def _worker():
            calculated = self.hybrid_calc.batch_calculate_indicators(tick_data, symbol)
            payload = {}
            for indicator in indicators:
                if indicator in calculated:
                    payload[indicator] = calculated[indicator]
                elif indicator == "macd":
                    macd = calculated.get("macd")
                    if isinstance(macd, dict) and "macd" in macd:
                        payload["macd"] = macd["macd"]
            return payload

        return await asyncio.to_thread(_worker)

    async def _calculate_greeks(
        self,
        symbol: str,
        tick_data: List[Dict[str, Any]],
        context_data: Dict[str, Any],
        requested: Set[str],
    ) -> Dict[str, Any]:
        last_tick = self._extract_last_tick(symbol, tick_data, context_data)
        if not last_tick:
            return {}

        enriched_tick = self._enrich_option_tick(symbol, last_tick, context_data)

        def _worker():
            greeks = self.greeks_calc.calculate_greeks_for_tick_data(enriched_tick)
            if not greeks:
                return {}
            payload = {name: greeks.get(name) for name in requested if greeks.get(name) is not None}
            payload.setdefault("option_price", greeks.get("option_price") or last_tick.get("last_price"))
            payload.setdefault("dte_years", greeks.get("dte_years"))
            payload.setdefault("trading_dte", greeks.get("trading_dte"))
            payload.setdefault("expiry_series", greeks.get("expiry_series"))
            return {k: v for k, v in payload.items() if k in requested}

        return await asyncio.to_thread(_worker)

    # ------------------------------------------------------------------ #
    # Data preparation helpers
    # ------------------------------------------------------------------ #
    def _extract_tick_data(self, symbol: str, context_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        ticks = context_data.get("ticks")
        if isinstance(ticks, list) and ticks:
            return ticks

        fallback_ticks = self.fallback_provider.build_tick_series(symbol)
        if fallback_ticks:
            return fallback_ticks

        logger.debug("Context missing ticks for %s, using empty tick list", symbol)
        return []

    def _extract_last_tick(
        self,
        symbol: str,
        tick_data: List[Dict[str, Any]],
        context_data: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if tick_data:
            return tick_data[-1]
        fallback_tick = context_data.get("last_tick")
        if fallback_tick:
            return fallback_tick
        last_price = context_data.get("last_price")
        if last_price:
            return {"symbol": symbol, "last_price": last_price}
        fallback_close = self.fallback_provider.get_last_close(symbol)
        if fallback_close is not None:
            return {"symbol": symbol, "last_price": fallback_close}
        return None

    # ------------------------------------------------------------------ #
    # Validation / fallbacks
    # ------------------------------------------------------------------ #
    def _ensure_complete_indicator_set(
        self,
        symbol: str,
        indicators: Dict[str, Any],
        required_indicators: List[str],
        context_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        safe_indicators = dict(indicators)
        for indicator in required_indicators:
            if indicator not in safe_indicators:
                fallback = self._get_indicator_fallback(symbol, indicator, context_data)
                safe_indicators[indicator] = fallback
                logger.debug("Fallback for %s:%s -> %s", symbol, indicator, fallback)
        return safe_indicators

    def _get_indicator_fallback(
        self,
        symbol: str,
        indicator: str,
        context_data: Dict[str, Any],
    ) -> Any:
        fallback = self.fallback_provider.get_indicator_value(symbol, indicator, context_data)
        if fallback is not None:
            return fallback

        return 0.0

    def _validate_indicator_quality(self, indicators: Dict[str, Any]) -> float:
        if not indicators:
            return 0.0
        valid = 0
        for name, value in indicators.items():
            if self._is_valid_indicator_value(name, value):
                valid += 1
        return valid / len(indicators)

    def _is_valid_indicator_value(self, name: str, value: Any) -> bool:
        if value is None:
            return False
        defaults = {"rsi": 50.0, "volume_ratio": 1.0, "delta": 0.5, "macd": 0.0}
        if name in defaults and value == defaults[name]:
            return False
        if name == "rsi" and not (0 <= value <= 100):
            return False
        if name == "volume_ratio" and value < 0:
            return False
        if name == "delta" and not (-1 <= value <= 1):
            return False
        return True

    # ------------------------------------------------------------------ #
    # Classification / requirements
    # ------------------------------------------------------------------ #
    def _load_indicator_requirements(self) -> None:
        self.requirements = {
            "equity": {
                "required": ["rsi", "macd", "volume_ratio", "ema_20", "sma_20"],
                "optional": ["bollinger_upper", "bollinger_lower", "atr", "vwap"],
            },
            "option": {
                "required": [
                    "delta",
                    "gamma",
                    "theta",
                    "vega",
                    "option_price",
                    "rsi",
                    "volume_ratio",
                    "dte_years",
                    "trading_dte",
                ],
                "optional": ["rho", "implied_volatility", "historical_volatility"],
            },
            "future": {
                "required": ["rsi", "volume_ratio", "open_interest", "ema_20"],
                "optional": ["basis", "roll_yield", "term_structure"],
            },
        }

    def _categorize_indicators(self, indicators: Set[str]) -> Dict[str, Set[str]]:
        categories = {"ta": set(), "greeks": set(), "volume": set()}
        greek_indicators = {
            "delta",
            "gamma",
            "theta",
            "vega",
            "rho",
            "option_price",
            "dte_years",
            "trading_dte",
            "expiry_series",
        }
        volume_indicators = {"volume_ratio", "volume_profile", "volume_ratio_ma_5", "volume_ratio_ma_20"}
        for indicator in indicators:
            if indicator in greek_indicators:
                categories["greeks"].add(indicator)
            elif indicator in volume_indicators:
                categories["volume"].add(indicator)
            else:
                categories["ta"].add(indicator)
        return {k: v for k, v in categories.items() if v}

    def _classify_symbol_type(self, symbol: str) -> str:
        """
        Classify symbol type using prefix-aware logic (NFO, NSE, CDS, MCX).
        Uses regex pattern to correctly identify options (\\d+(CE|PE)$) to avoid false positives.
        """
        import re
        upper = symbol.upper()
        symbol_without_prefix = upper
        
        # Strip prefix if present (handle both with and without colon)
        if upper.startswith('NFO:') or upper.startswith('NSE:') or \
           upper.startswith('CDS:') or upper.startswith('MCX:'):
            symbol_without_prefix = upper[4:]
        elif upper.startswith('NFO') and len(upper) > 3 and upper[3] != ':':
            # Handle NFO prefix without colon (e.g., NFORELIANCE)
            symbol_without_prefix = upper[3:]
        elif upper.startswith('NSE') and len(upper) > 3 and upper[3] != ':':
            symbol_without_prefix = upper[3:]
        elif upper.startswith('CDS') and len(upper) > 3 and upper[3] != ':':
            symbol_without_prefix = upper[3:]
        elif upper.startswith('MCX') and len(upper) > 3 and upper[3] != ':':
            symbol_without_prefix = upper[3:]
        
        # Check for option pattern: digit(s) followed by CE or PE at the end
        # This avoids false positives like RELIANCE (contains 'CE' but isn't an option)
        if re.search(r'\d+(CE|PE)$', symbol_without_prefix):
            return "option"
        
        # Check for future
        if 'FUT' in symbol_without_prefix and (symbol_without_prefix.endswith('FUT') or symbol_without_prefix.endswith('FUTURE')):
            return "future"
        
        # Default to equity
        return "equity"

    # Option enrichment methods
    OPTION_MONTHS = {
        "JAN": 1,
        "FEB": 2,
        "MAR": 3,
        "APR": 4,
        "MAY": 5,
        "JUN": 6,
        "JUL": 7,
        "AUG": 8,
        "SEP": 9,
        "OCT": 10,
        "NOV": 11,
        "DEC": 12,
    }

    def _enrich_option_tick(self, symbol: str, tick: Dict[str, Any], context_data: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(tick, dict):
            return tick

        symbol_upper = symbol.upper()
        if not (symbol_upper.endswith("CE") or symbol_upper.endswith("PE")):
            return tick

        metadata = self._parse_option_symbol(symbol_upper)
        if not metadata:
            return tick

        enriched = dict(tick)
        enriched.setdefault("strike_price", metadata["strike"])
        enriched.setdefault("option_type", metadata["option_type"])
        enriched.setdefault("expiry_date", metadata["expiry"].isoformat())

        trading_dte = metadata["trading_dte"]
        enriched.setdefault("trading_dte", trading_dte)
        if trading_dte is not None:
            enriched.setdefault("dte_years", trading_dte / 365 if trading_dte else 0.0)

        underlying_price = (
            tick.get("underlying_price")
            or context_data.get("underlying_price")
            or self.fallback_provider.get_last_close(metadata["underlying"])
        )
        if underlying_price:
            enriched["underlying_price"] = underlying_price

        return enriched

    def _parse_option_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        day_month = re.search(r"(\d{1,2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)", symbol)
        strike_match = re.search(r"(\d+)(CE|PE)$", symbol)
        if not day_month or not strike_match:
            return None

        day = int(day_month.group(1))
        month_abbr = day_month.group(2)
        month = self.OPTION_MONTHS.get(month_abbr)
        if not month:
            return None

        strike = float(strike_match.group(1))
        option_type = "call" if strike_match.group(2) == "CE" else "put"
        underlying = symbol[: day_month.start()].rstrip("0123456789")

        now = datetime.now()
        year = now.year
        try:
            expiry = datetime(year, month, day)
        except ValueError:
            return None
        if expiry.date() < now.date():
            try:
                expiry = datetime(year + 1, month, day)
            except ValueError:
                return None

        trading_dte = (expiry.date() - now.date()).days
        return {
            "strike": strike,
            "option_type": option_type,
            "expiry": expiry,
            "trading_dte": max(trading_dte, 0),
            "underlying": underlying,
        }


async def test_lifecycle_manager():  # pragma: no cover - manual test helper
    import redis
    from shared_core.redis_clients.redis_client import RedisClientFactory, RedisKeyStandards
    from indicators.lifecycle import IndicatorLifecycleManager

    redis_client = redis_client = RedisClientFactory.get_trading_client()
    unified_storage = get_unified_storage(redis_client=redis_client)
    lifecycle = IndicatorLifecycleManager(unified_storage=unified_storage, redis_client=redis_client)
    indicators = await lifecycle.get_indicators(
        "RELIANCE",
        ["rsi", "volume_ratio", "delta", "missing_indicator"],
        {"last_price": 2500.0, "ticks": [{"last_price": 2500.0, "volume": 1000}]},
    )
    print(f"Final indicators: {list(indicators.keys())}")


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(test_lifecycle_manager())


class TwentyDayFallbackProvider:
    """Provides indicator fallbacks based on 20-day OHLC + published baselines.
    
    ✅ FIXED: Now uses HybridCalculations from calculations.py instead of duplicate implementations.
    """

    def __init__(self, redis_client=None, hybrid_calc=None):
        self.redis_client = redis_client
        # ✅ FIXED: Use HybridCalculations instance instead of reimplementing calculations
        self.hybrid_calc = hybrid_calc or HybridCalculations(redis_client=redis_client)
        self._history_cache: Dict[str, List[Dict[str, float]]] = {}
        self._tick_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._ta_cache: Dict[str, Dict[str, Any]] = {}
        self._volume_payload: Optional[Dict[str, Dict[str, Any]]] = None
        self.volume_file = Path(__file__).resolve().parents[2] / "config" / "volume_averages_20d.json"

    def build_tick_series(self, symbol: str, limit: int = 55) -> List[Dict[str, Any]]:
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        if canonical in self._tick_cache:
            return self._tick_cache[canonical]

        rows = self._fetch_ohlc_rows(symbol, limit)
        ticks: List[Dict[str, Any]] = []
        for row in rows:
            timestamp = row.get("date") or datetime.utcnow().isoformat()
            ticks.append(
                {
                    "symbol": symbol,
                    "last_price": row["close"],
                    "high": row["high"],
                    "low": row["low"],
                    "open": row["open"],
                    "volume": row["volume"],
                    "bucket_incremental_volume": row["volume"],
                    "bucket_cumulative_volume": row["volume"],
                    "timestamp": timestamp,
                }
            )
        self._tick_cache[canonical] = ticks
        return ticks

    def get_indicator_value(self, symbol: str, indicator: str, context_data: Dict[str, Any]) -> Optional[Any]:
        snapshot = self._get_ta_snapshot(symbol)
        if not snapshot:
            return None

        if indicator in snapshot and snapshot[indicator] is not None:
            return snapshot[indicator]

        if indicator == "volume_ratio":
            ratio = self._estimate_volume_ratio(symbol)
            if ratio is not None:
                return ratio

        if indicator == "option_price":
            return snapshot.get("last_close")

        if indicator == "dte_years":
            trading_dte = context_data.get("trading_dte")
            if trading_dte is not None:
                return trading_dte / 365

        if indicator == "trading_dte":
            return context_data.get("trading_dte")

        return None

    def get_last_close(self, symbol: str) -> Optional[float]:
        snapshot = self._get_ta_snapshot(symbol)
        return snapshot.get("last_close")

    def _get_ta_snapshot(self, symbol: str) -> Dict[str, Any]:
        """✅ FIXED: Use HybridCalculations instead of duplicate calculation methods."""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        if canonical in self._ta_cache:
            return self._ta_cache[canonical]

        rows = self._fetch_ohlc_rows(symbol, 55)
        if len(rows) < 5:
            self._ta_cache[canonical] = {}
            return {}

        # ✅ FIXED: Convert OHLC rows to tick data format and use HybridCalculations
        tick_data = []
        for row in rows:
            tick_data.append({
                "symbol": symbol,
                "last_price": row["close"],
                "high": row["high"],
                "low": row["low"],
                "open": row["open"],
                "close": row["close"],
                "volume": row["volume"],
                "bucket_incremental_volume": row["volume"],
                "bucket_cumulative_volume": row["volume"],
                "timestamp": row.get("date", datetime.utcnow().isoformat()),
            })

        # ✅ FIXED: Use HybridCalculations.batch_calculate_indicators() instead of duplicate methods
        calculated = self.hybrid_calc.batch_calculate_indicators(tick_data, symbol)
        
        snapshot: Dict[str, Any] = {}
        
        # Extract calculated indicators
        if "rsi" in calculated:
            snapshot["rsi"] = calculated["rsi"]
        if "macd" in calculated:
            macd_val = calculated["macd"]
            if isinstance(macd_val, dict):
                snapshot["macd"] = macd_val.get("macd", 0.0)
            else:
                snapshot["macd"] = macd_val
        if "atr" in calculated:
            snapshot["atr"] = calculated["atr"]
        if "vwap" in calculated:
            snapshot["vwap"] = calculated["vwap"]
        
        # Extract EMAs
        for period in (5, 10, 20, 50, 100, 200):
            ema_key = f"ema_{period}"
            if ema_key in calculated:
                snapshot[ema_key] = calculated[ema_key]
        
        # SMA 20 (simple calculation, no need for HybridCalculations)
        closes = [row["close"] for row in rows]
        snapshot["sma_20"] = sum(closes[-20:]) / 20 if len(closes) >= 20 else closes[-1]

        volume_ratio = self._estimate_volume_ratio(symbol)
        if volume_ratio is not None:
            snapshot["volume_ratio"] = volume_ratio
        elif "volume_ratio" in calculated:
            snapshot["volume_ratio"] = calculated["volume_ratio"]

        snapshot["last_close"] = closes[-1]
        snapshot["last_volume"] = rows[-1]["volume"]
        self._ta_cache[canonical] = snapshot
        return snapshot

    def _estimate_volume_ratio(self, symbol: str) -> Optional[float]:
        history = self._fetch_ohlc_rows(symbol, 2)
        if not history:
            return None
        last_volume = history[-1]["volume"]
        baseline = self._get_volume_baseline(symbol)
        if baseline and baseline > 0:
            ratio = last_volume / baseline
            return max(0.1, min(ratio, 10.0))
        return None

    # ✅ REMOVED: Duplicate calculation methods - now using HybridCalculations from calculations.py
    # _calculate_rsi() - Use HybridCalculations.calculate_rsi()
    # _calculate_macd() - Use HybridCalculations.calculate_macd()
    # _calculate_atr() - Use HybridCalculations.calculate_atr()
    # _calculate_vwap() - Use HybridCalculations.calculate_vwap()

    def _fetch_ohlc_rows(self, symbol: str, limit: int) -> List[Dict[str, float]]:
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        if canonical in self._history_cache:
            return self._history_cache[canonical]

        rows: List[Dict[str, float]] = []
        client = self._get_db1_client()
        if client:
            try:
                # ✅ FIXED: Use DatabaseAwareKeyBuilder static method for OHLC daily (DB1)
                key = DatabaseAwareKeyBuilder.live_ohlc_daily(canonical)
                entries = client.zrange(key, -limit, -1)
                for entry in entries:
                    if isinstance(entry, bytes):
                        entry = entry.decode("utf-8")
                    try:
                        payload = json.loads(entry)
                    except json.JSONDecodeError:
                        continue
                    rows.append(
                        {
                            "date": payload.get("date"),
                            "open": float(payload.get("o", 0)),
                            "high": float(payload.get("h", 0)),
                            "low": float(payload.get("l", 0)),
                            "close": float(payload.get("c", 0)),
                            "volume": float(payload.get("v", 0)),
                        }
                    )
            except Exception as exc:
                logger.debug("Failed to load OHLC rows for %s: %s", symbol, exc)

        self._history_cache[canonical] = rows
        return rows

    def _get_volume_baseline(self, symbol: str) -> Optional[float]:
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        client = self._get_db1_client()
        if client:
            try:
                key = f"vol:baseline:{canonical}"
                raw = client.hgetall(key)
                if raw:
                    data = self._decode_dict(raw)
                    if "avg_volume_20d" in data:
                        return float(data["avg_volume_20d"])
            except Exception as exc:
                logger.debug("vol:baseline lookup failed for %s: %s", symbol, exc)

        payload = self._load_volume_payload()
        if payload:
            data = payload.get(canonical)
            if data and data.get("avg_volume_20d"):
                return float(data["avg_volume_20d"])
        return None

    def _load_volume_payload(self) -> Optional[Dict[str, Dict[str, Any]]]:
        if self._volume_payload is not None:
            return self._volume_payload
        if not self.volume_file.exists():
            logger.debug("Volume averages file missing: %s", self.volume_file)
            self._volume_payload = None
            return None
        try:
            with self.volume_file.open("r", encoding="utf-8") as handle:
                self._volume_payload = json.load(handle)
        except Exception as exc:
            logger.warning("Failed to load volume averages: %s", exc)
            self._volume_payload = None
        return self._volume_payload

    def _get_db1_client(self):
        client = self.redis_client
        if client is None:
            return None
        if hasattr(client, "get_client"):
            try:
                return client.get_client(1)
            except Exception:
                return client
        return client

    # ✅ REMOVED: Duplicate EMA methods - now using HybridCalculations.calculate_ema() from calculations.py
    # _ema_last() - Use HybridCalculations.calculate_ema()
    # _ema_series() - Use HybridCalculations.calculate_ema() or batch_calculate_indicators()

    def _decode_dict(self, raw: Dict[Any, Any]) -> Dict[str, Any]:
        decoded = {}
        for key, value in raw.items():
            if isinstance(key, bytes):
                key = key.decode("utf-8")
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            decoded[key] = value
        return decoded
