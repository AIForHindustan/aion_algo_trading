"""
Utility helpers shared across trading services.
"""

# Import from canonical location (redis_key_standards is the single source of truth for symbol parsing)
from shared_core.redis_clients.redis_key_standards import (
    UniversalSymbolParser,
    ParsedSymbol,
    get_symbol_parser as get_parser,
)

# Backward-compatible function exports using UniversalSymbolParser
_parser = get_parser()

def extract_underlying_from_symbol(symbol: str) -> str:
    """Extract underlying from symbol using UniversalSymbolParser."""
    if not symbol:
        return symbol
    return _parser.get_underlying(str(symbol)) or symbol

def parse_derivative_parts(symbol: str) -> dict:
    """Parse derivative parts using UniversalSymbolParser."""
    if not symbol:
        return {'underlying': None, 'expiry': None, 'strike': None, 'option_type': None}
    parsed = _parser.parse(str(symbol))
    return {
        'underlying': parsed.underlying,
        'expiry': parsed.expiry,
        'strike': parsed.strike,
        'option_type': parsed.option_type
    }
from .time_utils import (
    IndianMarketTimeParser,
    INDIAN_TIME_PARSER,
    get_current_ist_time,
    get_current_ist_timestamp,
    parse_ist_timestamp,
    get_market_session_key,
    is_market_hours,
    is_premarket_hours,
    is_postmarket_hours,
    get_market_session_start_end,
    format_timestamp_for_redis,
    parse_redis_timestamp,
)
from shared_core.redis_clients.redis_key_standards import get_symbol_parser
_parser = get_symbol_parser()

def resolve_underlying_price(symbol: str, redis_client=None):
    """
    Get underlying price, prioritizing IndexManager for indices.
    """
    if not symbol:
        return None
        
    # 1. Try IndexManager First
    try:
        from shared_core.index_manager import index_manager
        # Check if we have a connected redis client we can pass to index manager if needed
        # (IndexManager is singleton, but might need connection)
        if redis_client and not index_manager._redis:
             index_manager.connect_redis(redis_client)
             
        price = index_manager.get_spot_price(symbol)
        if price and price > 0:
            return price
    except ImportError:
        pass

    # 2. Fallback to Legacy Parser
    return _parser.get_underlying_price(symbol, redis_client)

INDEX_PRICE_KEY_OVERRIDES = {}
from .vix_utils import VIXUtils, get_vix_utils, get_vix_value, get_vix_regime, get_current_vix, is_vix_panic, is_vix_complacent
from .vix_regimes import VIXRegimeManager

__all__ = [
    "extract_underlying_from_symbol",
    "parse_derivative_parts",
    "IndianMarketTimeParser",
    "INDIAN_TIME_PARSER",
    "get_current_ist_time",
    "get_current_ist_timestamp",
    "parse_ist_timestamp",
    "get_market_session_key",
    "is_market_hours",
    "is_premarket_hours",
    "is_postmarket_hours",
    "get_market_session_start_end",
    "format_timestamp_for_redis",
    "parse_redis_timestamp",
    # Underlying price resolution
    "resolve_underlying_price",
    "get_underlying_price",
    "INDEX_PRICE_KEY_OVERRIDES",
    # VIX utilities
    "VIXUtils",
    "get_vix_utils",
    "get_vix_value",
    "get_vix_regime",
    "get_current_vix",
    "is_vix_panic",
    "is_vix_complacent",
    "VIXRegimeManager",
]
