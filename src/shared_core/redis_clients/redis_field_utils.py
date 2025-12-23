"""
Redis Field Utilities
=====================

Simple utility functions for field normalization and mapping.
Replaces complex redis_key_mapping.py with direct yaml_field_loader usage.
"""

import json
from typing import Dict, Any, List, Optional


def normalize_redis_tick_data(tick_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize Redis tick data to canonical field names.
    
    ✅ FIXED: Preserves ALL fields including ALL Greeks (basic + advanced) and computed indicators.
    Uses yaml_field_loader for field name mapping, but preserves all original fields.
    
    ✅ FIXED: JSON-decodes depth/order_book/tick payloads that were stored as strings.
    ✅ FIXED: Decodes bytes from Redis to strings to prevent JSON serialization errors.
    
    CRITICAL: This function does NOT drop or rename Greek fields. All Greek fields are preserved
    as-is, even if they're not in the YAML mapping.
    """
    if not isinstance(tick_data, dict):
        return tick_data
    
    # ✅ CRITICAL FIX: Decode bytes from Redis to strings
    # Redis may return bytes for some values which causes "Object of type bytes is not JSON serializable"
    decoded_tick_data = {}
    for key, value in tick_data.items():
        # Decode key if bytes
        decoded_key = key.decode('utf-8') if isinstance(key, bytes) else key
        
        # Decode value if bytes
        if isinstance(value, bytes):
            try:
                decoded_value = value.decode('utf-8')
                # Try to parse as JSON if it looks like JSON
                if decoded_value.startswith('{') or decoded_value.startswith('['):
                    try:
                        decoded_value = json.loads(decoded_value)
                    except json.JSONDecodeError:
                        pass
            except UnicodeDecodeError:
                decoded_value = value  # Keep as bytes if can't decode
        elif isinstance(value, dict):
            # Recursively decode nested dicts
            decoded_value = normalize_redis_tick_data(value)
        elif isinstance(value, list):
            # Decode list items
            decoded_value = [
                item.decode('utf-8') if isinstance(item, bytes) else item
                for item in value
            ]
        else:
            decoded_value = value
        
        decoded_tick_data[decoded_key] = decoded_value
    
    tick_data = decoded_tick_data
    
    # ✅ CRITICAL FIX: JSON-decode depth/order_book/tick payloads that were stored as strings
    # This handles cases where data was stored as JSON strings in Redis
    json_string_fields = ['depth', 'order_book', 'market_depth', 'ticks', 'tick_payload']
    for field in json_string_fields:
        if field in tick_data and isinstance(tick_data[field], str):
            try:
                decoded = json.loads(tick_data[field])
                tick_data[field] = decoded
            except (json.JSONDecodeError, TypeError):
                # If JSON parsing fails, keep original value
                pass
    
    try:
        from shared_core.config_utils.yaml_field_loader import normalize_session_record
        
        # ✅ CRITICAL: Define ALL Greek field names (basic + advanced + metadata) to ensure they're preserved
        # These fields are NOT in the YAML mapping, so they must be explicitly preserved
        all_greek_field_names = {
            # Basic Greeks (first-order)
            'delta', 'gamma', 'theta', 'vega', 'rho',
            # Volatility
            'iv', 'implied_volatility', 'volatility',
            # Advanced Greeks (higher-order)
            'vanna', 'charm', 'vomma', 'speed', 'zomma', 'color',
            # Gamma Exposure
            'gamma_exposure', 'gamma_risk', 'delta_risk', 'theta_decay',
            # Option Metadata (stored as Greeks)
            'dte_years', 'trading_dte', 'expiry_series', 'option_price',
            'underlying_price', 'spot_price', 'strike_price', 'expiry_date', 'option_type',
            'moneyness', 'intrinsic_value', 'time_value',
            # Additional option fields
            'calendar_dte', 'trading_dte', 'underlying_symbol'
        }
        
        # ✅ CRITICAL FIX: Preserve ALL fields, not just those in session_fields mapping
        # normalize_session_record with include_aliases=False only keeps mapped fields
        # We need to merge normalized fields with original to preserve computed fields (Greeks, indicators, etc.)
        
        # Step 1: Normalize known fields (maps aliases to canonical names)
        normalized = normalize_session_record(tick_data, include_aliases=False)
        
        # Step 2: Preserve ALL original fields that weren't mapped
        # This ensures ALL Greeks (basic + advanced) and other computed fields are preserved
        for key, value in tick_data.items():
            if key not in normalized and value is not None:
                # Preserve any field that wasn't in the session_fields mapping
                # This includes: ALL Greeks (basic + advanced), computed indicators, option metadata, etc.
                normalized[key] = value
        
        # ✅ CRITICAL: Explicitly preserve ALL Greek fields from original tick_data
        # This ensures that even if a Greek field was in the mapping and got renamed,
        # we still have the original field name preserved
        for greek_field in all_greek_field_names:
            if greek_field in tick_data and tick_data[greek_field] is not None:
                # Only add if not already in normalized (to avoid overwriting normalized values)
                if greek_field not in normalized:
                    normalized[greek_field] = tick_data[greek_field]
        
        return normalized
    except Exception:
        # Fallback: return as-is if normalization fails
        return tick_data


def map_tick_for_pattern(tick_data: Dict[str, Any]) -> Dict[str, Any]:
    """Map tick data for pattern detector consumption"""
    # Use normalized data - patterns expect canonical field names
    return normalize_redis_tick_data(tick_data)


def map_tick_for_scanner(tick_data: Dict[str, Any]) -> Dict[str, Any]:
    """Map tick data for scanner consumption"""
    # Use normalized data - scanners expect canonical field names
    return normalize_redis_tick_data(tick_data)


def map_tick_for_dashboard(tick_data: Dict[str, Any]) -> Dict[str, Any]:
    """Map tick data for dashboard consumption"""
    # Use normalized data - dashboards expect canonical field names
    return normalize_redis_tick_data(tick_data)


def resolve_redis_key(key_alias: str, symbol: Optional[str] = None, **kwargs) -> str:
    """
    Resolve Redis key alias to canonical key.
    
    ✅ FIXED: Uses DatabaseAwareKeyBuilder instead of redis_key_mapping.
    """
    from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
    
    # If symbol provided, use DatabaseAwareKeyBuilder methods
    if symbol:
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        
        # Try to match common key patterns
        if key_alias.startswith("ticks:") or "tick" in key_alias.lower():
            if "latest" in key_alias or "hash" in key_alias:
                return DatabaseAwareKeyBuilder.live_ticks_latest(canonical_symbol)
            elif "stream" in key_alias:
                return DatabaseAwareKeyBuilder.live_ticks_stream(canonical_symbol)
        elif key_alias.startswith("ohlc:") or "ohlc" in key_alias.lower():
            if "latest" in key_alias:
                return DatabaseAwareKeyBuilder.live_ohlc_latest(canonical_symbol)
        elif key_alias.startswith("vol:") or "volume" in key_alias.lower():
            if "baseline" in key_alias:
                return DatabaseAwareKeyBuilder.live_volume_baseline(canonical_symbol)
    
    # Fallback: return as-is (assume it's already canonical)
    result = key_alias
    if symbol and "{symbol}" in result:
        result = result.replace("{symbol}", symbol)
    if symbol and "{canonical_symbol}" in result:
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        result = result.replace("{canonical_symbol}", canonical_sym)
    
    for key, value in kwargs.items():
        placeholder = f"{{{key}}}"
        if placeholder in result:
            result = result.replace(placeholder, str(value))
    
    return result


def get_redis_key_fields(key_type: str) -> List[str]:
    """
    Get list of fields stored in a Redis key type.
    
    ✅ FIXED: Uses yaml_field_loader instead of redis_key_mapping.
    """
    try:
        from shared_core.config_utils.yaml_field_loader import get_redis_session_fields, get_calculated_indicator_fields
        
        if "session" in key_type.lower():
            return list(get_redis_session_fields().keys())
        elif "indicator" in key_type.lower() or "ind:" in key_type.lower():
            return list(get_calculated_indicator_fields().keys())
        else:
            # Default: return common tick fields
            return list(get_redis_session_fields().keys())
    except Exception:
        return []

