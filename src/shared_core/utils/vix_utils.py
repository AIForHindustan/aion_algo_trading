"""
VIX Utilities - India VIX data access for retail validation
Provides easy access to current VIX values and market regime detection.

This module is designed to be resilient in environments where the
`redis` package is not installed or a Redis server is not available.
In such cases, it degrades gracefully and simply returns None/UNKNOWN
for VIX queries rather than raising ImportError at import time.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from shared_core.redis_clients.redis_client import get_redis_client
# Import field mapping utilities for canonical field names
try:
    from shared_core.config_utils.yaml_field_loader import resolve_session_field, get_field_mapping_manager
    FIELD_MAPPING_AVAILABLE = True
except ImportError:
    FIELD_MAPPING_AVAILABLE = False
    def resolve_session_field(field_name: str) -> str:
        return field_name  # Fallback to original field name

# Optional imports: redis and duckdb may not be installed in all environments
try:
    import redis  # type: ignore
    _REDIS_AVAILABLE = True
except Exception:  # ImportError or any other failure
    redis = None  # type: ignore
    _REDIS_AVAILABLE = False

try:
    import duckdb  # type: ignore
    _DUCKDB_AVAILABLE = True
except Exception:  # ImportError or any other failure
    duckdb = None  # type: ignore
    _DUCKDB_AVAILABLE = False

logger = logging.getLogger(__name__)

class VIXUtils:
    """Utility class for accessing India VIX data"""
    
    # ✅ CLASS-LEVEL SINGLETON: Shared Redis client to avoid "Too many connections"
    _shared_redis_client = None
    _client_initialized = False
    
    def __init__(self, redis_client=None):
        """Initialize VIX utilities with optional Redis client.

        If a client is not provided and the `redis` package is unavailable or a
        connection cannot be established, the instance will operate in a
        no-Redis mode where `get_current_vix()` returns None.
        """
        # ✅ Use provided client, or class-level singleton, or create new one
        if redis_client:
            self.redis_client = redis_client
            # Update singleton if provided
            VIXUtils._shared_redis_client = redis_client
            VIXUtils._client_initialized = True
        elif VIXUtils._shared_redis_client and VIXUtils._client_initialized:
            # Reuse existing singleton
            self.redis_client = VIXUtils._shared_redis_client
        elif _REDIS_AVAILABLE:
            # Create new client only if no singleton exists
            try:
                from shared_core.redis_clients.redis_client import UnifiedRedisManager
                self.redis_client = get_redis_client(
                    process_name="vix_utils",
                    db=1
                )
                # Test connection
                self.redis_client.ping()
                # Store as singleton
                VIXUtils._shared_redis_client = self.redis_client
                VIXUtils._client_initialized = True
                logger.debug("✅ [VIX] Created singleton Redis client for VIXUtils")
            except RecursionError as e:
                logger.error(f"❌ [VIX] Recursion error connecting to Redis DB1: {e}")
                self.redis_client = None
            except Exception as e:
                logger.debug(f"⚠️ [VIX] Failed to connect to Redis DB1: {e}")
                self.redis_client = None
        else:
            self.redis_client = None

    
    def get_current_vix(self) -> Optional[Dict[str, Any]]:
        """
        Get current India VIX value and regime from Redis database
        
        Returns:
            Dict with 'value', 'regime', 'timestamp' or None if unavailable
        """
        # ✅ FIXED: Use singleton pattern - don't create new connections
        client = self.redis_client or VIXUtils._shared_redis_client
        
        if not client:
            # Only create if absolutely no singleton exists
            try:
                from shared_core.redis_clients.redis_client import UnifiedRedisManager
                client = get_redis_client(process_name="vix_utils_fallback", db=1)
                # Store as singleton for future use
                VIXUtils._shared_redis_client = client
                VIXUtils._client_initialized = True
            except Exception as e:
                logger.debug(f"⚠️ [VIX] Failed to get Redis connection: {e}")
                return None
        
        if not client:
            return None
        
        # Primary keys that gift_nifty_gap.py uses (from _publish_to_redis)
        # ✅ ENHANCED: Added more key variants based on actual Redis keys found
        # ✅ FIXED: Prioritize hash keys first (they're more commonly used)
        keys_to_try = [
            "index_hash:NSEINDIAVIX",  # ✅ PRIMARY: Hash key canonical variant (most common)
            "index_hash:NSE:INDIA VIX",  # Hash key variant with space
            "index:NSEINDIA_VIX",  # String key with underscore (actual Redis key)
            "index:NSE:INDIA VIX",  # String key - exact match (with space)
            "index:NSEINDIAVIX",  # Canonical symbol variant (no space, no colon)
            "index:NSEINDIA VIX",  # Variant with space but no colon
            "market_data:indices:nse_india_vix",  # Legacy key with underscore
            "market_data:indices:nse_india vix",  # Legacy key with space
            "market_data:indices:nseindia_vix",  # Legacy key no space
        ]
        
        vix_data = None
        key_used = None
        
        try:
            for key in keys_to_try:
                try:
                    # Check if key exists first
                    if not client.exists(key):
                        continue
                    
                    # Check key type to determine how to read it
                    key_type = client.type(key)
                    key_type_str = key_type.decode('utf-8') if isinstance(key_type, bytes) else key_type
                    
                    # Handle hash keys differently
                    if key_type_str == 'hash' or (isinstance(key_type, bytes) and key_type == b'hash'):
                        try:
                            hash_data = client.hgetall(key)
                            if hash_data:
                                # Handle bytes keys/values
                                last_price = None
                                timestamp = None
                                for k, v in list(hash_data.items() if isinstance(hash_data, dict) else []):
                                    k_str = k.decode('utf-8') if isinstance(k, bytes) else (k if isinstance(k, str) else str(k))
                                    if k_str == 'last_price':
                                        v_str = v.decode('utf-8') if isinstance(v, bytes) else v
                                        try:
                                            last_price = float(v_str) if v_str else None
                                        except (ValueError, TypeError):
                                            pass
                                    elif k_str == 'timestamp':
                                        v_str = v.decode('utf-8') if isinstance(v, bytes) else v
                                        timestamp = v_str
                                
                                if last_price is not None:
                                    # ✅ FIX: Store as dict (not JSON string) for consistent parsing
                                    vix_data = {
                                        'last_price': last_price,
                                        'timestamp': timestamp or datetime.now().isoformat()
                                    }
                                    key_used = key
                                    logger.debug(f"✅ [VIX] Found data in Redis hash key: {key}, last_price={last_price}")
                                    break
                        except Exception as hash_error:
                            logger.debug(f"⚠️ [VIX] Error reading hash key {key}: {hash_error}")
                            continue
                    
                    # Handle string keys
                    elif key_type_str == 'string' or (isinstance(key_type, bytes) and key_type == b'string'):
                        try:
                            raw_data = client.get(key)
                            
                            # Handle both bytes and string responses
                            if raw_data:
                                # Decode if bytes
                                if isinstance(raw_data, bytes):
                                    vix_data = raw_data.decode('utf-8')
                                else:
                                    vix_data = raw_data
                                
                                if vix_data and isinstance(vix_data, str) and len(vix_data) > 0:
                                    key_used = key
                                    logger.debug(f"✅ [VIX] Found data in Redis key: {key}")
                                    break
                        except Exception as get_error:
                            logger.debug(f"⚠️ [VIX] Error reading string key {key}: {get_error}")
                            continue
                            
                except Exception as e:
                    logger.debug(f"⚠️ [VIX] Error reading key {key}: {e}")
                    continue
        except Exception as e:
            logger.debug(f"⚠️ [VIX] Error in key lookup loop: {e}")
            return None
        
        # ✅ FIXED: Don't close connection - RedisClientFactory handles connection pooling
        
        if not vix_data:
            # Only log warning if we actually tried to connect (not if Redis is unavailable)
            try:
                if client:
                    logger.debug(f"⚠️ [VIX] No VIX data found in Redis DB1. Tried keys: {keys_to_try[:3]}...")
                    # Try one more time with a direct connection test
                    try:
                        test_key = "index:NSE:INDIA VIX"
                        test_exists = client.exists(test_key)
                        if test_exists:
                            logger.debug(f"⚠️ [VIX] Key {test_key} EXISTS but get() returned None - possible encoding issue")
                        else:
                            logger.debug(f"⚠️ [VIX] Key {test_key} does NOT exist in Redis DB1")
                    except Exception as test_error:
                        logger.debug(f"⚠️ [VIX] Error testing key existence: {test_error}")
            except Exception:
                pass  # Silent fail if Redis is unavailable
            
            # ✅ FALLBACK: Try reading from JSON file if Redis is unavailable
            fallback_data = self._load_vix_from_json_fallback()
            if fallback_data:
                logger.info("✅ [VIX] Loaded VIX data from JSON fallback file")
                return fallback_data
            
            return None
        
        # Parse JSON data (handle both dict and JSON string)
        try:
            if isinstance(vix_data, dict):
                data = vix_data
            elif isinstance(vix_data, str):
                data = json.loads(vix_data)
            else:
                logger.error(f"❌ [VIX] Unexpected vix_data type: {type(vix_data)}")
                return None
        except Exception as e:
            logger.error(f"❌ [VIX] Error parsing VIX data from Redis key {key_used}: {e}")
            logger.debug(f"❌ [VIX] Raw data type: {type(vix_data)}, value: {str(vix_data)[:200]}...")
            return None
        
        # Extract VIX value - gift_nifty_gap.py publishes 'last_price'
        vix_value = (
            data.get('last_price') or 
            data.get('value') or 
            data.get('close') or 
            data.get('price') or 
            0
        )
        
        if not vix_value or vix_value == 0:
            logger.warning(f"⚠️ [VIX] VIX value is 0 or None from Redis key {key_used}. Data keys: {list(data.keys())}")
            return None
        
        try:
            vix_value = float(vix_value)
        except (ValueError, TypeError) as e:
            logger.error(f"❌ [VIX] Invalid VIX value type: {type(vix_value)} = {vix_value}")
            return None
        
        regime = self._classify_vix_regime(vix_value)
        logger.debug(f"✅ [VIX] Retrieved VIX: {vix_value:.2f}, Regime: {regime} from key: {key_used}")
        
        return {
            'value': vix_value,
            'regime': regime,
            'change_pct': data.get('percent_change', data.get('change_percent', 0.0)),
            'change': data.get('net_change', data.get('change', 0.0)),
            'timestamp': data.get('timestamp', datetime.now().isoformat()),
            'source': 'redis',
            'key_used': key_used
        }
    
    def _load_vix_from_json_fallback(self) -> Optional[Dict[str, Any]]:
        """
        Load VIX data from JSON fallback file if Redis is unavailable.
        
        Checks multiple possible locations for VIX JSON files:
        - clickhouse_setup/historical_data/commodity/json/NSE_INDIA VIX.json
        - shared_core/historical_data/vix_latest.json (if exists)
        
        Returns:
            Dict with 'value', 'regime', 'timestamp' or None if file not found
        """
        from pathlib import Path
        
        # Possible fallback file locations
        possible_paths = [
            Path(__file__).parent.parent.parent / "clickhouse_setup" / "historical_data" / "commodity" / "json" / "NSE_INDIA VIX.json",
            Path(__file__).parent.parent / "historical_data" / "vix_latest.json",
            Path(__file__).parent.parent.parent / "shared_core" / "historical_data" / "vix_latest.json",
        ]
        
        for json_path in possible_paths:
            try:
                if json_path.exists():
                    with open(json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                    # Handle different JSON structures
                    if isinstance(data, dict):
                        # Check if it has ohlc_data array (historical data format)
                        if 'ohlc_data' in data and isinstance(data['ohlc_data'], list) and len(data['ohlc_data']) > 0:
                            # Use the most recent entry from ohlc_data
                            latest_entry = data['ohlc_data'][-1]
                            vix_value = latest_entry.get('close') or latest_entry.get('last_price') or 0
                            timestamp = latest_entry.get('timestamp') or latest_entry.get('date') or datetime.now().isoformat()
                        else:
                            # Extract VIX value from various possible field names
                            vix_value = (
                                data.get('last_price') or
                                data.get('value') or
                                data.get('close') or
                                data.get('price') or
                                (data.get('ohlc', {}).get('close') if isinstance(data.get('ohlc'), dict) else None) or
                                0
                            )
                            timestamp = data.get('timestamp', datetime.now().isoformat())
                        
                        if vix_value and vix_value > 0:
                            try:
                                vix_value = float(vix_value)
                                regime = self._classify_vix_regime(vix_value)
                                
                                return {
                                    'value': vix_value,
                                    'regime': regime,
                                    'timestamp': timestamp,
                                    'source': 'json_fallback',
                                    'file_path': str(json_path)
                                }
                            except (ValueError, TypeError) as e:
                                logger.debug(f"⚠️ [VIX] Invalid VIX value in JSON file {json_path}: {e}")
                                continue
                    elif isinstance(data, list) and len(data) > 0:
                        # If it's a list, use the most recent entry
                        latest_entry = data[-1] if isinstance(data[-1], dict) else data[0]
                        vix_value = (
                            latest_entry.get('last_price') or
                            latest_entry.get('value') or
                            latest_entry.get('close') or
                            0
                        )
                        
                        if vix_value and vix_value > 0:
                            try:
                                vix_value = float(vix_value)
                                regime = self._classify_vix_regime(vix_value)
                                
                                return {
                                    'value': vix_value,
                                    'regime': regime,
                                    'timestamp': latest_entry.get('timestamp', datetime.now().isoformat()),
                                    'source': 'json_fallback',
                                    'file_path': str(json_path)
                                }
                            except (ValueError, TypeError) as e:
                                logger.debug(f"⚠️ [VIX] Invalid VIX value in JSON file {json_path}: {e}")
                                continue
                    
                    logger.debug(f"⚠️ [VIX] JSON file {json_path} exists but doesn't contain valid VIX data")
            except FileNotFoundError:
                continue
            except json.JSONDecodeError as e:
                logger.debug(f"⚠️ [VIX] Error parsing JSON file {json_path}: {e}")
                continue
            except Exception as e:
                logger.debug(f"⚠️ [VIX] Error reading JSON fallback file {json_path}: {e}")
                continue
        
        logger.debug("⚠️ [VIX] No VIX JSON fallback file found in any of the checked locations")
        return None
    
    def _classify_vix_regime(self, vix_value: float) -> str:
        """Classify VIX value into market regime (4 regimes) - Uses classify_indian_vix_regime from shared_core.config_utils.thresholds"""
        try:
            # ✅ SINGLE SOURCE OF TRUTH: Use classify_indian_vix_regime from shared_core.config_utils.thresholds
            from shared_core.config_utils.thresholds import classify_indian_vix_regime
            return classify_indian_vix_regime(vix_value)
        except ImportError:
            # Fallback if shared_core.config_utils.thresholds not available (shouldn't happen in production)
            if vix_value is None:
                return 'NORMAL'
            try:
                vix_float = float(vix_value)
            except (TypeError, ValueError):
                return 'NORMAL'
            if vix_float > 25:
                return 'PANIC'
            elif vix_float >= 15:
                return 'HIGH'
            elif vix_float >= 12:
                return 'NORMAL'
            else:
                return 'LOW'
    
    def is_vix_panic(self) -> bool:
        """Check if VIX indicates panic mode (>25)
        
        Returns True if VIX regime is 'PANIC' (VIX > 25).
        Uses classify_indian_vix_regime from shared_core.config_utils.thresholds.
        """
        vix_data = self.get_current_vix()
        if vix_data and isinstance(vix_data, dict):
            return vix_data.get('regime') == 'PANIC'
        return False
    
    def is_vix_complacent(self) -> bool:
        """Check if VIX indicates complacent/low volatility mode (<12)
        
        Returns True if VIX regime is 'LOW' (VIX < 12).
        Uses classify_indian_vix_regime from shared_core.config_utils.thresholds.
        """
        vix_data = self.get_current_vix()
        if vix_data and isinstance(vix_data, dict):
            return vix_data.get('regime') == 'LOW'
        return False
    
    def get_vix_regime(self) -> str:
        """Get current VIX regime
        
        Returns:
            VIX regime string: 'LOW', 'NORMAL', 'HIGH', 'PANIC', or 'UNKNOWN' if unavailable
        """
        vix_data = self.get_current_vix()
        if vix_data and isinstance(vix_data, dict):
            regime = vix_data.get('regime')
            return regime if regime else 'UNKNOWN'
        return 'UNKNOWN'
    
    def get_vix_value(self) -> Optional[float]:
        """Get current VIX value"""
        vix_data = self.get_current_vix()
        return vix_data.get('value') if vix_data else None


# Global VIX utilities instance
_vix_utils = None

def get_vix_utils(redis_client=None) -> VIXUtils:
    """Get global VIX utilities instance, optionally with Redis client"""
    global _vix_utils
    try:
        # ✅ FIX: Always recreate if client is None or if current instance has no client
        if _vix_utils is None or _vix_utils.redis_client is None:
            _vix_utils = VIXUtils(redis_client=redis_client)
        elif redis_client is not None:
            # If a client is provided, update the instance
            _vix_utils = VIXUtils(redis_client=redis_client)
        return _vix_utils
    except Exception as e:
        logger.error(f"❌ [VIX] Error creating VIXUtils instance: {e}")
        # Return a minimal instance that won't crash
        return VIXUtils(redis_client=None)

def get_current_vix() -> Optional[Dict[str, Any]]:
    """Convenience function to get current VIX data - SAFE: never raises exceptions"""
    try:
        return get_vix_utils().get_current_vix()
    except Exception as e:
        logger.debug(f"⚠️ [VIX] Error in get_current_vix(): {e}")
        return None

def is_vix_panic() -> bool:
    """Convenience function to check if VIX indicates panic - SAFE: never raises exceptions"""
    try:
        return get_vix_utils().is_vix_panic()
    except Exception as e:
        logger.debug(f"⚠️ [VIX] Error in is_vix_panic(): {e}")
        return False

def is_vix_complacent() -> bool:
    """Convenience function to check if VIX indicates complacency - SAFE: never raises exceptions"""
    try:
        return get_vix_utils().is_vix_complacent()
    except Exception as e:
        logger.debug(f"⚠️ [VIX] Error in is_vix_complacent(): {e}")
        return False

def get_vix_regime() -> str:
    """Convenience function to get VIX regime - SAFE: never raises exceptions, returns 'UNKNOWN' on error"""
    try:
        return get_vix_utils().get_vix_regime()
    except Exception as e:
        logger.debug(f"⚠️ [VIX] Error in get_vix_regime(): {e}")
        return 'UNKNOWN'

def get_vix_value() -> Optional[float]:
    """Convenience function to get VIX value - SAFE: never raises exceptions"""
    try:
        return get_vix_utils().get_vix_value()
    except Exception as e:
        logger.debug(f"⚠️ [VIX] Error in get_vix_value(): {e}")
        return None
