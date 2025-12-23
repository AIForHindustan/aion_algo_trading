"""
Unified Data Storage - Source of Truth for All Redis Storage Operations

This module provides a single, consistent interface for storing and retrieving
all trading data in Redis. All downstream code MUST use this class.

Features:
- Uses UniversalSymbolParser for symbol normalization (source of truth)
- Uses RedisKeyStandards for key generation (consistent key format)
- Unified structure for tick data, indicators, and Greeks
- Automatic TTL management
- Thread-safe operations
"""

import json
import time
import logging
from threading import Lock
from typing import Dict, Any, Optional, List

from shared_core.redis_clients.redis_client import (
    UnifiedRedisManager,
    SafeJSONEncoder,
    get_redis_client,
)
from shared_core.redis_clients.redis_key_standards import (
    RedisKeyStandards,
    get_symbol_parser,
    DatabaseAwareKeyBuilder,
    _infer_exchange_prefix,
)
from shared_core.redis_clients.decorators import safe_redis_operation
from shared_core.redis_clients import normalize_redis_tick_data
# ✅ REMOVED: canonical_key_builder - use DatabaseAwareKeyBuilder instead

logger = logging.getLogger(__name__)


class UnifiedDataStorage:
    """
    Unified Data Storage - Source of Truth for All Redis Storage Operations.
    
    All Redis storage operations MUST go through this class to ensure:
    - Consistent symbol normalization (via UniversalSymbolParser)
    - Consistent key format (via RedisKeyStandards)
    - Unified data structure across the system
    """
    
    GREEK_NAMES: List[str] = [
        "delta",
        "gamma",
        "theta",
        "vega",
        "rho",
        "vanna",
        "charm",
        "vomma",
        "speed",
        "zomma",
        "color",
        "gamma_exposure",
        "iv",
        "implied_volatility",
        "dte_years",
        "trading_dte",
        "expiry_series",
        "option_price",
        "expiry_date",
        "calendar_dte",
    ]
    
    def __init__(self, redis_url='redis://localhost:6379/1', redis_client=None, db: int = 1):
        """
        Initialize UnifiedDataStorage.
        
        Args:
            redis_url: Redis connection URL (default: redis://localhost:6379/1) - DEPRECATED, use redis_client
            redis_client: Optional Redis client instance (if provided, redis_url is ignored)
            db: Redis database number (default: 1 for live data)
        """
        if redis_client:
            self.redis = redis_client
        else:
            # ✅ FIXED: Use UnifiedRedisManager (preferred) instead of deprecated RedisManager82
            # UnifiedRedisManager provides consistent interface and delegates to RedisManager82 internally
            self.redis = get_redis_client(process_name="unified_data_storage", db=db)
        
        self.parser = get_symbol_parser()
        # ✅ FIXED: Directly use DatabaseAwareKeyBuilder to avoid get_key_builder() ambiguity
        # All methods are static, so no instance needed
    
    def _get_client(self):
        """
        Resolve the actual redis client regardless of whether we're wrapping
        a RobustRedisClient or a raw redis.Redis instance.
        """
        client = getattr(self.redis, "redis_client", None)
        if client is not None:
            return client
        client = getattr(self.redis, "redis", None)
        if client is not None:
            return client
        return self.redis
        
    def _normalize_symbol(self, symbol: str) -> str:
        """
        Normalize symbol using RedisKeyStandards.canonical_symbol (source of truth).
        
        Returns canonical format: {exchange}{normalized} (e.g., NFOINDIGO25NOV5900CE)
        
        ✅ FIXED: Uses canonical_symbol() for consistency with rest of pipeline.
        """
        if not symbol:
            return symbol
        
        try:
            # ✅ FIXED: Use RedisKeyStandards.canonical_symbol() for consistency
            # This ensures the same canonical format is used throughout the pipeline
            return RedisKeyStandards.canonical_symbol(symbol)
        except Exception as e:
            logger.warning(f"Symbol normalization failed for {symbol}: {e}, using original")
            # Fallback: try basic normalization
            try:
                parsed = self.parser.parse_symbol(symbol)
                exchange = _infer_exchange_prefix(symbol)
                return f"{exchange}{parsed.normalized}"
            except Exception:
                return symbol
    
    @safe_redis_operation
    def store_tick_data(self, symbol: str, data: Dict[str, Any], ttl: int = 3600):
        """
        Store tick data with unified structure.
        
        Uses UniversalSymbolParser for symbol normalization.
        For real-time tick streaming, use RedisStorage.queue_tick_for_storage() instead.
        This method stores simple tick snapshots.
        
        Key format: ticks:latest:{normalized_symbol}
        
        Args:
            symbol: Trading symbol (e.g., INDIGO25NOV5900CE)
            data: Tick data dictionary
            ttl: Time to live in seconds (default: 3600 = 1 hour)
        """
        normalized_symbol = self._normalize_symbol(symbol)
        tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(normalized_symbol)
        
        try:
            # Store current tick snapshot (for simple lookups)
            # Note: For real-time streaming, use RedisStorage.queue_tick_for_storage()
            # ✅ FIX: Serialize complex types (dict, list) to JSON before storing
            serialized_mapping = {}
            for k, v in data.items():
                if isinstance(v, (dict, list)):
                    serialized_mapping[str(k)] = json.dumps(v, cls=SafeJSONEncoder)
                elif isinstance(v, bytes):
                    import base64
                    serialized_mapping[str(k)] = base64.b64encode(v).decode("utf-8")
                elif v is None:
                    continue  # Skip None values
                else:
                    serialized_mapping[str(k)] = str(v)
            
            logger.debug(f"[REDIS_STORE_TICK] {tick_key} - Fields: {list(data.keys())}")
            logger.debug(f"[REDIS_VALUES] Sample: {{ {', '.join([f'{k}: {v}' for k, v in list(serialized_mapping.items())[:5]])} }}")
            
            self.redis.hset(tick_key, mapping=serialized_mapping)
            self.redis.expire(tick_key, ttl)
            
            # Add to recent ticks list (keep last 100)
            recent_key = DatabaseAwareKeyBuilder.live_recent_ticks_list(normalized_symbol)
            self.redis.lpush(recent_key, json.dumps(data, cls=SafeJSONEncoder))
            self.redis.ltrim(recent_key, 0, 99)
            self.redis.expire(recent_key, ttl)
            
            logger.debug(f"[REDIS_STORED] {tick_key} - {len(serialized_mapping)} fields")
        except Exception as e:
            logger.error(f"Failed to store tick data for {symbol}: {e}")
            raise
    
    @safe_redis_operation
    def store_indicators(self, symbol: str, indicators: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """DEBUG: Verify pipelining is working."""
        import time
        
        if not indicators:
            return False
        
        # Default TTL if not provided (86400s = 24h)
        actual_ttl = ttl if ttl is not None else 86400
        
        t0 = time.perf_counter_ns()
        
        try:
            # ✅ FIXED: Use self.redis (correct attribute) instead of self.redis_client
            redis_client = self.redis
            if hasattr(self.redis, 'redis'):
                redis_client = self.redis.redis
            
            # 🚨 OPTION 1: Try pipeline WITHOUT transaction
            pipeline = redis_client.pipeline(transaction=False)  # CRITICAL CHANGE
            
            normalized_symbol = self._normalize_symbol(symbol)
            
            # Separate indicators and greeks
            indicators_map = {}
            greeks_map = {}
            
            for indicator_name, value in indicators.items():
                if value is None:
                    continue
                
                # Convert to string/json safely
                if isinstance(value, (dict, list)):
                    val_str = json.dumps(value, cls=SafeJSONEncoder)
                elif isinstance(value, bytes):
                    import base64
                    val_str = base64.b64encode(value).decode("utf-8")
                else:
                    val_str = str(value)

                category = RedisKeyStandards.categorize_indicator(indicator_name)
                if category == 'greeks':
                    greeks_map[indicator_name] = val_str
                else:
                    indicators_map[indicator_name] = val_str
            
            # Store Indicators Hash (Matches Scanner HGETALL ind:{symbol})
            if indicators_map:
                ind_key = DatabaseAwareKeyBuilder.live_indicator_hash(symbol)
                pipeline.hset(ind_key, mapping=indicators_map)
                pipeline.expire(ind_key, actual_ttl)
                if len(indicators_map) <= 5:
                    print(f"   → HSET {ind_key} {list(indicators_map.keys())}")

            # Store Greeks Hash (Matches AsyncWriter pattern: ind:greeks:{symbol})
            if greeks_map:
                greeks_key = DatabaseAwareKeyBuilder.live_greeks_hash(normalized_symbol)
                pipeline.hset(greeks_key, mapping=greeks_map)
                pipeline.expire(greeks_key, actual_ttl)
                if len(greeks_map) <= 5:
                    print(f"   → HSET {greeks_key} {list(greeks_map.keys())}")
            
            # 🚨 EXECUTE AND TIME
            t1 = time.perf_counter_ns()
            
            # ✅ ADDED: Maintain active_symbols set for scanner (eliminates SCAN operations)
            pipeline.sadd("active_symbols", normalized_symbol)
            pipeline.expire("active_symbols", 300)  # 5 minutes TTL
            
            # ✅ [REDIS_OPS] Checkpoint
            logger.debug(f"[REDIS_OPS] {symbol} - Commands queued: {len(pipeline)}")
            
            results = pipeline.execute()  # Should be ONE network round-trip
            t2 = time.perf_counter_ns()
            
            pipeline_time_ms = (t2 - t1) / 1000000
            total_time_ms = (t2 - t0) / 1000000
            
            if pipeline_time_ms > 10:
                print(f"🚨 [REDIS_SLOW] {symbol}: Pipeline execute took {pipeline_time_ms:.2f}ms")
                print(f"   Total time: {total_time_ms:.2f}ms, {len(indicators)} indicators")
            
            return True
            
        except Exception as e:
            print(f"❌ [REDIS_ERROR] {symbol}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    @safe_redis_operation
    def store_greeks(self, symbol: str, greeks: Dict[str, float], ttl: int = 86400):
        """
        Store option Greeks using Redis Pipelining.
        
        ✅ WIRED: Follows redis_storage.py pattern exactly:
        - Uses self.redis (initialized in __init__) for all operations
        - Uses key_builder.live_greeks() for key generation
        - Uses key_builder.get_database() to verify DB assignment
        - All Greeks stored in DB 1 (live data)
        - Uses PIPELINE for performance (1 RTT instead of N)
        
        Args:
            symbol: Trading symbol
            greeks: Dictionary of greek_name -> value
            ttl: Time to live in seconds (default: 86400 = 24 hours)
        """
        normalized_symbol = self._normalize_symbol(symbol)
        
        # ✅ DEBUG: Log incoming Greeks
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"🔍 [GREEK_STORAGE] Batch storing {len(greeks)} Greeks for {normalized_symbol}")
        
        client = self.redis
        
        if not client:
            logger.error(f"❌ [GREEK_STORAGE] {symbol} - Redis client is None!")
            return
        
        try:
            from shared_core.redis_clients.redis_key_standards import RedisDatabase
            
            # ✅ OPTIMIZATION: Use Pipeline
            pipeline = client.pipeline() if hasattr(client, 'pipeline') else client.redis.pipeline()
            
            stored_count = 0
            # Prepare greeks for Hash storage
            greeks_str_map = {}
            for greek_name, value in greeks.items():
                if value is None:
                    continue
                greeks_str_map[greek_name] = str(value) if not isinstance(value, str) else value
            
            if greeks_str_map:
                    # Use live_greeks_hash for Hash storage (Standard)
                key = DatabaseAwareKeyBuilder.live_greeks_hash(normalized_symbol)
                
                # Verify DB assignment (using hash key)
                db = DatabaseAwareKeyBuilder.get_database(key)
                if db != RedisDatabase.DB1_LIVE_DATA:
                        logger.warning(f"⚠️ [GREEK_STORAGE] Key {key} assigned to wrong DB: {db}")

                pipeline.hset(key, mapping=greeks_str_map)
                pipeline.expire(key, ttl)
                stored_count += len(greeks_str_map)
            
            # Execute batch
            if stored_count > 0:
                # ✅ [REDIS_OPS] Checkpoint
                logger.debug(f"[REDIS_OPS] {symbol} - Commands queued in Greek pipeline: {len(pipeline)}")
                pipeline.execute()
                logger.debug(f"✅ [GREEK_STORAGE] Successfully stored {stored_count} Greeks for {normalized_symbol}")
                
        except Exception as e:
            logger.error(f"❌ [GREEK_STORAGE] Failed to store Greeks for {symbol}: {e}", exc_info=True)
            raise
    
    @safe_redis_operation
    def store_volume_state(self, instrument_token: str, data: Dict[str, Any]) -> bool:
        """Store volume state snapshot for an instrument token."""
        if not instrument_token or not data:
            return False
        
        key = DatabaseAwareKeyBuilder.live_volume_state(instrument_token)
        client = self._get_client()
        client.hset(key, mapping=data)
        return True
    
    @safe_redis_operation
    def get_volume_state(self, instrument_token: str) -> Dict[str, Any]:
        """Fetch persisted volume state for an instrument token."""
        if not instrument_token:
            return {}
        
        key = DatabaseAwareKeyBuilder.live_volume_state(instrument_token)
        client = self._get_client()
        raw_data = client.hgetall(key) or {}
        normalized = {}
        for field, value in raw_data.items():
            if isinstance(field, bytes):
                field = field.decode("utf-8")
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            normalized[field] = value
        return normalized
    
    @safe_redis_operation
    def store_volume_baseline(self, symbol: str, data: Dict[str, Any], ttl: int = 86400) -> bool:
        """Store volume baseline metrics for a symbol with optional TTL."""
        if not symbol or not data:
            return False
        
        key = DatabaseAwareKeyBuilder.live_volume_baseline(symbol)
        client = self._get_client()
        client.hset(key, mapping=data)
        if ttl:
            client.expire(key, ttl)
        return True
    
    @safe_redis_operation
    def store_volume_profile(self, symbol: str, profile_type: str, data: Dict[str, Any], ttl: int = 57600) -> bool:
        """Store computed volume profile for a symbol and profile type."""
        if not symbol or not profile_type or not data:
            return False
        
        key = DatabaseAwareKeyBuilder.live_volume_profile(symbol, profile_type)
        client = self._get_client()
        client.hset(key, mapping=data)
        if ttl:
            client.expire(key, ttl)
        return True
    
    @safe_redis_operation
    def store_straddle_volume(self, underlying: str, date: str, data: Dict[str, Any], ttl: int = 57600) -> bool:
        """Store straddle volume analytics for an underlying/date combination."""
        if not underlying or not date or not data:
            return False
        
        key = DatabaseAwareKeyBuilder.live_volume_straddle(underlying, date)
        client = self._get_client()
        client.hset(key, mapping=data)
        if ttl:
            client.expire(key, ttl)
        return True
    
    def get_all_data(self, symbol: str) -> Dict[str, Any]:
        """
        Get all data for a symbol in one call.
        
        Uses UniversalSymbolParser for symbol normalization.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Dictionary with keys: 'tick', 'indicators', 'greeks', 'recent_ticks'
        """
        normalized_symbol = self._normalize_symbol(symbol)
        
        try:
            # ✅ FIXED: Use live_ticks_latest() to match websocket parser storage key
            # Websocket parser stores in ticks:latest:{symbol}, not tick:{symbol}
            tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(normalized_symbol)
            tick_data = self.redis.hgetall(tick_key) or {}
            
            # ✅ SOURCE OF TRUTH: Get indicators using get_indicators() method (reuses complete default list)
            indicators = self.get_indicators(normalized_symbol, indicator_names=None)
            
            # ✅ SOURCE OF TRUTH: Get Greeks using get_greeks() method (consistent with get_indicators)
            greeks = self.get_greeks(normalized_symbol)
            
            recent_ticks = self.get_recent_ticks(normalized_symbol, limit=10)
            
            return {
                'tick': tick_data,
                'indicators': indicators,
                'greeks': greeks,
                'recent_ticks': recent_ticks
            }
        except Exception as e:
            logger.error(f"Failed to get all data for {symbol}: {e}")
            return {
                'tick': {},
                'indicators': {},
                'greeks': {},
                'recent_ticks': []
            }
    
    def get_tick_data(self, symbol: str) -> Dict[str, Any]:
        """Get current tick data for a symbol."""
        normalized_symbol = self._normalize_symbol(symbol)
        # ✅ FIXED: Use live_ticks_latest() to match websocket parser storage key
        # Websocket parser stores in ticks:latest:{symbol}, not ticks:{symbol}
        tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(normalized_symbol)
        tick_data_raw = self.redis.hgetall(tick_key) or {}
        
        # Type narrowing: ensure tick_data is a dict before processing
        if not isinstance(tick_data_raw, dict):
            return {}
        
        # ✅ UPDATED: Normalize field names using field mapping
        return normalize_redis_tick_data(tick_data_raw)
    
    def _parse_order_book_value(self, raw_value: Any) -> Optional[Dict[str, Any]]:
        """Normalize any stored depth/order book representation into a standard dict."""
        if raw_value is None:
            return None
        
        if isinstance(raw_value, (bytes, bytearray)):
            try:
                raw_value = raw_value.decode("utf-8")
            except Exception:
                raw_value = raw_value.decode("utf-8", errors="ignore")
        
        if isinstance(raw_value, str):
            candidate = raw_value.strip()
            if not candidate:
                return None
            try:
                raw_value = json.loads(candidate)
            except json.JSONDecodeError:
                return None
        
        if isinstance(raw_value, dict) and "depth" in raw_value and isinstance(raw_value["depth"], dict):
            raw_value = raw_value["depth"]
        
        if not isinstance(raw_value, dict):
            return None
        
        buy_levels = None
        sell_levels = None
        
        for candidate in ("buy", "bids", "bid", "depth_buy"):
            levels = raw_value.get(candidate)
            if isinstance(levels, list):
                buy_levels = levels
                break
        
        for candidate in ("sell", "asks", "ask", "depth_sell"):
            levels = raw_value.get(candidate)
            if isinstance(levels, list):
                sell_levels = levels
                break
        
        if buy_levels is None and isinstance(raw_value.get("depth_levels_buy"), list):
            buy_levels = raw_value.get("depth_levels_buy")
        if sell_levels is None and isinstance(raw_value.get("depth_levels_sell"), list):
            sell_levels = raw_value.get("depth_levels_sell")
        
        depth_levels = raw_value.get("depth_levels")
        if isinstance(depth_levels, dict):
            if buy_levels is None and isinstance(depth_levels.get("buy"), list):
                buy_levels = depth_levels.get("buy")
            if sell_levels is None and isinstance(depth_levels.get("sell"), list):
                sell_levels = depth_levels.get("sell")
        
        def _normalize_side(levels: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
            normalized: List[Dict[str, Any]] = []
            if not isinstance(levels, list):
                return normalized
            for level in levels[:5]:
                if not isinstance(level, dict):
                    continue
                last_price = level.get("last_price") or level.get("price") or level.get("ltp")
                quantity = level.get("quantity") or level.get("qty") or level.get("volume")
                if last_price is None or quantity is None:
                    continue
                try:
                    normalized.append(
                        {
                            "last_price": float(last_price),
                            "quantity": int(float(quantity)),
                            "orders": int(level.get("orders", 1)),
                        }
                    )
                except (TypeError, ValueError):
                    continue
            return normalized
        
        normalized_book = {
            "buy": _normalize_side(buy_levels),
            "sell": _normalize_side(sell_levels),
        }
        if normalized_book["buy"] or normalized_book["sell"]:
            return normalized_book
        return None
    
    def get_order_book(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch latest order book snapshot from Redis for the given symbol."""
        normalized_symbol = self._normalize_symbol(symbol)
        
        try:
            tick_snapshot = self.get_tick_data(normalized_symbol)
        except Exception:
            tick_snapshot = {}
        
        order_book = self._parse_order_book_value(
            tick_snapshot.get("order_book")
            or tick_snapshot.get("depth")
            or tick_snapshot.get("depth_data")
        )
        if order_book:
            return order_book
        
        # Fallback: read raw ticks:latest hash directly
        try:
            tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(normalized_symbol)
            raw_tick = self.redis.hgetall(tick_key) or {}
            if isinstance(raw_tick, dict):
                order_book = self._parse_order_book_value(
                    raw_tick.get("order_book")
                    or raw_tick.get("depth")
                    or raw_tick.get(b"order_book")
                    or raw_tick.get(b"depth")
                )
                if order_book:
                    return order_book
        except Exception:
            pass
        
        # Final fallback: known depth keys
        candidates: List[str] = []
        base_symbols = [normalized_symbol]
        if ":" in normalized_symbol:
            base_symbols.append(normalized_symbol.split(":", 1)[-1])
        for base in base_symbols:
            candidates.extend(
                [
                    f"depth:{base}",
                    f"depth_data:{base}",
                    f"market_depth:{base}",
                    f"order_book:{base}",
                    f"orderbook:{base}",
                ]
            )
        
        for key in candidates:
            try:
                raw_value = self.redis.get(key)
                order_book = self._parse_order_book_value(raw_value)
                if order_book:
                    return order_book
            except Exception:
                continue
        
        return None
    
    def get_recent_ticks(self, symbol: str, limit: int = 32) -> List[Dict[str, Any]]:
        """Fetch last N raw ticks (JSON payloads) for context-sensitive detectors."""
        normalized_symbol = self._normalize_symbol(symbol)
        ticks: List[Dict[str, Any]] = []
        
        stream_key = DatabaseAwareKeyBuilder.live_ticks_raw_stream(normalized_symbol)
        if stream_key:
            try:
                records = self.redis.xrevrange(stream_key, count=limit)
                if isinstance(records, list):
                    for _, fields in records:
                        payload = fields.get("data") or fields.get(b"data")
                        if payload is None:
                            continue
                        if isinstance(payload, (bytes, bytearray)):
                            payload = payload.decode("utf-8", errors="ignore")
                        try:
                            parsed = json.loads(payload)
                            if isinstance(parsed, dict):
                                ticks.append(parsed)
                        except (json.JSONDecodeError, TypeError):
                            continue
            except Exception as exc:
                logger.debug(f"Failed to fetch ticks from stream {stream_key}: {exc}")
        
        if len(ticks) < limit:
            remaining = max(limit - len(ticks), 0)
            list_key = DatabaseAwareKeyBuilder.live_recent_ticks_list(normalized_symbol)
            try:
                raw_list = self.redis.lrange(list_key, 0, max(remaining - 1, 0))
                if isinstance(raw_list, list):
                    for payload in raw_list:
                        if isinstance(payload, (bytes, bytearray)):
                            payload = payload.decode("utf-8", errors="ignore")
                        try:
                            parsed = json.loads(payload)
                            if isinstance(parsed, dict):
                                ticks.append(parsed)
                                if len(ticks) >= limit:
                                    break
                        except (json.JSONDecodeError, TypeError):
                            continue
            except Exception as exc:
                logger.debug(f"Failed to fetch ticks from list {list_key}: {exc}")
        
        return ticks
    
    def get_tick_history(self, symbol: str, limit: int = 32) -> List[Dict[str, Any]]:
        """Alias maintained for backward compatibility."""
        return self.get_recent_ticks(symbol, limit=limit)
    
    def get_indicators(self, symbol: str, indicator_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Get indicators from Redis.
        
        ✅ FIXED: Uses HGETALL on ind:{symbol} hash to match how store_indicators() stores data.
        Previously used GET on ind:{category}:{symbol}:{field} which was WRONG.
        
        Args:
            symbol: Trading symbol
            indicator_names: List of indicator names to fetch (if None, fetches all from hash)
        """
        normalized_symbol = self._normalize_symbol(symbol)
        
        # ✅ FIXED: Reuse self.redis (initialized in __init__) instead of creating new clients
        client = self.redis
        
        indicators = {}
        variants = RedisKeyStandards.get_indicator_symbol_variants(normalized_symbol)
        
        # ✅ FIXED: Use HGETALL on ind:{symbol} hash (matches store_indicators() which uses HSET)
        for variant in variants:
            if indicators:  # Already found data, skip other variants
                break
                
            ind_key = DatabaseAwareKeyBuilder.live_indicator_hash(variant)
            try:
                hash_data = client.hgetall(ind_key)
                if hash_data:
                    for field, value in hash_data.items():
                        # Decode bytes to string
                        field_name = field.decode('utf-8') if isinstance(field, bytes) else field
                        value_str = value.decode('utf-8') if isinstance(value, bytes) else value
                        
                        # Filter by indicator_names if provided
                        if indicator_names is not None and field_name not in indicator_names:
                            continue
                        
                        # Parse value
                        try:
                            if '.' in str(value_str):
                                indicators[field_name] = float(value_str)
                            else:
                                indicators[field_name] = int(value_str)
                        except (ValueError, TypeError):
                            # Try JSON parsing for complex values
                            try:
                                indicators[field_name] = json.loads(value_str)
                            except (json.JSONDecodeError, TypeError):
                                indicators[field_name] = value_str
                                
                    logger.debug(f"✅ [GET_INDICATORS] Fetched {len(indicators)} indicators for {variant} from ind:{variant}")
            except Exception as e:
                logger.debug(f"Failed to fetch indicators from {ind_key}: {e}")
        
        return indicators
    
    def get_all_indicators(self, symbol: str) -> Dict[str, Any]:
        """
        Get ALL available indicators for a symbol (full mode).
        
        Fetches indicators from all categories:
        - Technical Analysis (TA)
        - Volume
        - Microstructure/Custom
        - Greeks (via get_greeks())
        
        This is useful for pattern detection when you need complete data.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Dictionary with all available indicators
        """
        normalized_symbol = self._normalize_symbol(symbol)
        
        # Get all indicators using get_indicators() with None (uses complete default list)
        all_indicators = self.get_indicators(symbol, indicator_names=None)
        
        # Also get Greeks (they're stored separately)
        greeks = self.get_greeks(symbol)
        all_indicators.update(greeks)
        
        return all_indicators
    
    def get_greeks(self, symbol: str) -> Dict[str, float]:
        """
        Get option Greeks following redis_storage.py pattern.
        
        ✅ WIRED: Matches redis_storage.py signature:
        - Uses key_builder.live_greeks() for key generation
        - Uses get_redis_client() with explicit DB 1
        - Uses key_builder.get_database() to verify DB assignment
        """
        normalized_symbol = self._normalize_symbol(symbol)
        
        # ✅ FIXED: Reuse self.redis (initialized in __init__) instead of creating new clients
        # This prevents connection pool exhaustion and follows the correct pattern
        client = self.redis
        
        greeks = {}
        variants = RedisKeyStandards.get_indicator_symbol_variants(normalized_symbol)
        
        for variant in variants:
            # 1. Try HASH fetch first (Standard)
            try:
                greeks_key = DatabaseAwareKeyBuilder.live_greeks_hash(variant)
                hash_data = client.hgetall(greeks_key)
                if hash_data:
                    for field, value in hash_data.items():
                        field_name = field.decode('utf-8') if isinstance(field, bytes) else field
                        value_str = value.decode('utf-8') if isinstance(value, bytes) else value
                        try:
                            greeks[field_name] = float(value_str)
                        except (ValueError, TypeError):
                             pass
                    # If we found data in hash, we are good for this variant
                    if greeks:
                        continue
            except Exception:
                pass

            # 2. Fallback to individual keys (Legacy)
            for greek_name in self.GREEK_NAMES:
                if greek_name in greeks:
                    continue
                
                key = DatabaseAwareKeyBuilder.live_greeks(variant, greek_name)
                value = client.get(key)
                if value:
                    try:
                        greeks[greek_name] = float(value) if isinstance(value, str) else value
                    except (ValueError, TypeError):
                        pass
        
        return greeks


    def get_active_symbols(self) -> List[str]:
        """Get list of active symbols from Redis."""
        try:
            symbols = self.redis.smembers("active_symbols")
            if not symbols:
                return []
            return [sym.decode('utf-8') if isinstance(sym, bytes) else sym for sym in symbols]
        except Exception as e:
            logger.error(f"Failed to get active symbols: {e}")
            return []

    def get_ohlc_data(self, symbol: str) -> Dict[str, float]:
        """
        Get latest OHLC data for a symbol.
        """
        normalized_symbol = self._normalize_symbol(symbol)
        try:
            latest_key = DatabaseAwareKeyBuilder.live_ohlc_latest(normalized_symbol)
            payload = self.redis.hgetall(latest_key)
            if not payload:
                return {}
            
            result = {}
            for k, v in payload.items():
                key = k.decode('utf-8') if isinstance(k, bytes) else k
                try:
                    val = float(v) if v is not None else None
                    result[key] = val
                except (ValueError, TypeError):
                    continue
            return result
        except Exception as e:
            logger.error(f"Failed to get OHLC data for {symbol}: {e}")
            return {}

    def get_option_chain(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get option chain data for a symbol.
        """
        normalized = self._normalize_symbol(symbol)
        canonical_lower = normalized.lower()
        
        # Try primary key first
        keys = [
            f"options:chain:{canonical_lower}",
            f"option_chain:{canonical_lower}"
        ]
        
        for key in keys:
            try:
                data = self.redis.get(key)
                if data:
                    if isinstance(data, bytes):
                        data = data.decode('utf-8')
                    return json.loads(data)
            except Exception:
                continue
        return None


    def get_live_price(self, symbol: str) -> Optional[float]:
        """
        Get real-time price for a symbol from Redis.
        
        Tries multiple sources:
        1. Live Tick Data (ticks:latest:{symbol})
        2. Spot Price Cache (spot:realtime:{symbol} / price:realtime:{symbol})
        3. Index Data (index:{symbol} JSON payload)
        """
        normalized_symbol = self._normalize_symbol(symbol)
        
        # 1. Try Tick Data (ticks:latest)
        try:
            tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(normalized_symbol)
            price_raw = self.redis.hget(tick_key, "last_price")
            if price_raw:
                return float(price_raw)
        except Exception:
            pass
            
        # 2. Try Spot Price (price:realtime) - Written by gift_nifty_gap with 60s TTL
        try:
            spot_key = DatabaseAwareKeyBuilder.live_price_realtime(normalized_symbol)
            spot_val = self.redis.get(spot_key)
            if spot_val:
                return float(spot_val)
        except Exception:
            pass
            
        # 3. Try Index Data (index:{symbol}) - Full JSON payload from gift_nifty_gap
        # Handle specific index aliases if needed (e.g., NIFTY -> NIFTY 50 if exact match fails)
        try:
            # Direct check for index:{symbol}
            index_key = f"index:{normalized_symbol}"
            index_data_raw = self.redis.get(index_key)
            if index_data_raw:
                import json
                index_data = json.loads(index_data_raw)
                if index_data.get('last_price'):
                    return float(index_data['last_price'])
            
            # Special case: VIX
            if 'VIX' in normalized_symbol.upper():
                vix_key = "index:INDIA_VIX" # Canonical name often used
                vix_val = self.redis.get(vix_key)
                if vix_val:
                    import json
                    vix_data = json.loads(vix_val)
                    if vix_data.get('last_price'):
                        return float(vix_data['last_price'])
                        
            # Special case fallback for major indices if normalized name differs
            if normalized_symbol == 'NIFTY':
                 # Try typical canonical names
                 for alias in ['NIFTY_50', 'NSE:NIFTY_50']:
                    val = self.redis.get(f"index:{alias}")
                    if val:
                        import json
                        return float(json.loads(val).get('last_price', 0))
                        
            if normalized_symbol == 'BANKNIFTY':
                 for alias in ['NIFTY_BANK', 'NSE:NIFTY_BANK']:
                    val = self.redis.get(f"index:{alias}")
                    if val:
                        import json
                        return float(json.loads(val).get('last_price', 0))

        except Exception:
            pass
            
        return None


# ============================================================================
# Singleton instance for global use
# ============================================================================

_unified_storage_instance: Optional[UnifiedDataStorage] = None
_unified_storage_lock = Lock()


def get_unified_storage(redis_client=None, db: int = 1) -> UnifiedDataStorage:
    """
    Get singleton instance of UnifiedDataStorage unless an explicit redis_client is provided.
    """
    if redis_client is not None:
        return UnifiedDataStorage(redis_client=redis_client, db=db)
    
    global _unified_storage_instance
    if _unified_storage_instance is None:
        with _unified_storage_lock:
            if _unified_storage_instance is None:
                _unified_storage_instance = UnifiedDataStorage(db=db)
    return _unified_storage_instance
