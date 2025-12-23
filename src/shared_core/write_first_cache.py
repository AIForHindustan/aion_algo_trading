"""
Shared write-first cache utilities for the realtime pipeline.

These classes were originally embedded inside the Zerodha crawler.  They are
now shared so both the crawler and the multi-process pipeline can reuse the
same caching behaviour without duplicating code.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Optional

from cachetools import TTLCache
from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
from shared_core.redis_clients.redis_key_standards import UniversalSymbolParser, get_symbol_parser as get_parser


logger = logging.getLogger(__name__)


class WriteFirstCache:
    """
    Write-first cache that ensures calculations read the latest data even
    before Redis writes complete.
    
    ✅ UPDATED: Uses TTLCache for automatic LRU eviction and expiry.
    """

    def __init__(self, instruments_config_path: Optional[str] = None):
        # ✅ REPLACE with LRU/TTL Cache as requested
        # Ticks: 75k symbols, 5min (300s) TTL
        self._tick_cache = TTLCache(maxsize=75000, ttl=300)
        
        # Underlying: 1000 items, 5s TTL
        self._underlying_cache = TTLCache(maxsize=1000, ttl=5)
        
        # Calculations: 5000 items, 1s TTL (Flattened keys: (symbol, type))
        self._calculation_cache = TTLCache(maxsize=5000, ttl=1)
        
        self._volume_cache: Dict[str, Dict[str, Any]] = {}
        self._builder = DatabaseAwareKeyBuilder

        self._preseed_underlying_prices(instruments_config_path)

    def store_tick_for_calculations(self, symbol: str, tick_data: Dict[str, Any]) -> None:
        if not symbol:
            return
        # Store directly in TTLCache
        self._tick_cache[symbol] = {
            "last_price": tick_data.get("last_price"),
            "volume": tick_data.get("volume"),
            "timestamp": time.time(),
        }
        if tick_data.get("segment") == 9 and tick_data.get("last_price"):
            self._underlying_cache[symbol] = float(tick_data["last_price"])

    def get_underlying_price(self, symbol: str, underlying_symbol: str) -> Optional[float]:
        # Try specific underlying symbol first
        price = self._underlying_cache.get(underlying_symbol)
        if price is not None:
            return price
        # Fallback to symbol itself (if it is an index)
        return self._underlying_cache.get(symbol)

    def get_volume_state(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self._volume_cache.get(symbol)

    def store_calculation_result(self, symbol: str, calculation_type: str, result: Any) -> None:
        # Use flattened key for TTLCache
        key = f"{symbol}:{calculation_type}"
        self._calculation_cache[key] = result

    def get_calculation_result(self, symbol: str, calculation_type: str) -> Optional[Any]:
        key = f"{symbol}:{calculation_type}"
        return self._calculation_cache.get(key)
        # Timestamp check handled by TTLCache ttl=1

    def _preseed_underlying_prices(self, config_path: Optional[str] = None) -> None:
        path = config_path or os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "zerodha",
            "crawlers",
            "binary_crawler1",
            "intraday_crawler_instruments.json",
        )
        if not os.path.exists(path):
            logger.debug("WriteFirstCache instruments file not found: %s", path)
            return
        try:
            with open(path, "r", encoding="utf-8") as handle:
                config = json.load(handle)
            indices = config.get("indices", {})
            for index_name, index_data in indices.items():
                spot_price = index_data.get("spot_price")
                if not spot_price:
                    continue
                self._underlying_cache[index_name] = spot_price
                if index_name == "NIFTY":
                    self._underlying_cache["NIFTY 50"] = spot_price
                if index_name == "BANKNIFTY":
                    self._underlying_cache["NIFTY BANK"] = spot_price
                self._underlying_cache[f"NSE:{index_name}"] = spot_price
        except Exception as exc:
            logger.debug("WriteFirstCache failed to preseed prices: %s", exc)


class UnderlyingPriceServiceWithCache:
    """
    Wrapper that prioritizes WriteFirstCache and IndexManager for spot prices.
    
    ✅ UPDATED: Integrated IndexManager for unified spot price source of truth.
    """

    def __init__(self, price_service: Any, cache: WriteFirstCache):
        self.price_service = price_service
        self.cache = cache
        
        # Ensure imports
        try:
            from shared_core.index_manager import index_manager
            self.index_manager = index_manager
        except ImportError:
            self.index_manager = None
            logger.warning("UnderlyingPriceServiceWithCache could not import IndexManager")

    def get_underlying_price(self, option_symbol: str) -> Optional[float]:
        # 1. Try WriteFirstCache (fastest, pre-seeded or recently written)
        underlying_symbol = self.price_service.get_underlying(option_symbol)
        if underlying_symbol:
            cached = self.cache.get_underlying_price(str(option_symbol), str(underlying_symbol))
            if cached is not None:
                return cached
                
        # 2. Try IndexManager (Centralized Source of Truth for Indices)
        if self.index_manager:
            # Check if this token belongs to an index
            # We can try getting the spot price for the underlying symbol directly
            if underlying_symbol:
                spot_price = self.index_manager.get_spot_price(underlying_symbol)
                if spot_price and spot_price > 0:
                    return spot_price
            
            # Or try getting spot price by token if we had the token (not passed here)
            
        # 3. Fallback to legacy price service (UniversalSymbolParser)
        return self.price_service.get_underlying_price(option_symbol)

    def store_underlying_price(self, symbol: str, price: float) -> None:
        if symbol and price:
            self.cache._underlying_cache[symbol] = price  # pylint: disable=protected-access
            self.price_service.update_price(symbol, price)
            
            # Also update IndexManager if it's an index
            if self.index_manager:
                self.index_manager.update_spot_price(symbol, price)

    def get_underlying_for_option(self, option_symbol: str) -> Optional[str]:
        return self.price_service.get_underlying(option_symbol)

    def get_price(self, symbol: str) -> Optional[float]:
        # Unified price getter
        # 1. Check IndexManager first for indices
        if self.index_manager:
            price = self.index_manager.get_spot_price(symbol)
            if price and price > 0:
                return price
                
        # 2. Fallback to legacy
        return self.price_service.get_underlying_price(symbol)


# ✅ FIXED: Factory function to create price service with proper interface
def create_price_service(redis_client=None) -> UnderlyingPriceServiceWithCache:
    """
    Create a price service instance with caching and get_underlying_for_option() support.
    
    Returns:
        UnderlyingPriceServiceWithCache with get_underlying_for_option(), get_price() methods
    """
    parser = get_parser()
    cache = WriteFirstCache()
    return UnderlyingPriceServiceWithCache(price_service=parser, cache=cache)


# ✅ NEW: Alias for backward compatibility
UnderlyingPriceService = UniversalSymbolParser
