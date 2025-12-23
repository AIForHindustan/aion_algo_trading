"""
Focused tick storage - stores raw ticks and OHLC data.

Redis Database Architecture:
- DB 1: Historical OHLC + current tick data + rolling windows
- DB 2: Pattern validation results only

Storage Pattern:
1. Queue ticks for batch storage (async, batched)
2. Store tick data in Redis hashes (ticks:{symbol}) using hset with mapping
3. Add to processed stream (ticks:intraday:processed) with maxlen trimming
4. Store latest tick metadata for quick lookup

All tick data is stored in DB 1 (ohlc_and_ticks).
"""

import json
import time
import logging
import threading
from typing import Dict, List, Any
from shared_core.config_utils.timestamp_normalizer import TimestampNormalizer
from shared_core.config_utils.yaml_field_loader import (
    get_field_mapping_manager,
    resolve_session_field,
    resolve_calculated_field,
)
from shared_core.redis_clients.redis_client import UnifiedRedisManager, get_redis_client

logger = logging.getLogger(__name__)


class RedisStorage:
    """
    Focused tick storage - stores raw ticks and OHLC data.
    
    Features:
    - Batches ticks for efficient Redis pipeline execution (50 ticks or 100ms threshold)
    - Automatic flushing with Redis 8.2 optimized hset with mapping parameter
    - Thread-safe operations with locking
    - Stream management with maxlen and approximate trimming
    - TTL management for tick data
    """
    
    def __init__(self, redis_client):
        """
        Initialize Redis storage for tick data.
        
        Args:
            redis_client: Redis client instance (RobustRedisClient or compatible)
        """
        self.redis = redis_client
        self.field_mapping_manager = get_field_mapping_manager()
        
        self.tick_batch = []
        self.max_batch_size = 50
        self.last_flush = time.time()
        self.flush_interval = 0.1
        self._lock = threading.Lock() if hasattr(threading, 'Lock') else None
        
        self.FIELD_LAST_PRICE = resolve_session_field("last_price")
        self.FIELD_VOLUME = resolve_session_field("bucket_incremental_volume")
        self.FIELD_ZERODHA_CUMULATIVE_VOLUME = resolve_session_field("zerodha_cumulative_volume")
        self.FIELD_ZERODHA_LAST_TRADED_QUANTITY = resolve_session_field("zerodha_last_traded_quantity")
        self.FIELD_BUCKET_INCREMENTAL_VOLUME = resolve_session_field("bucket_incremental_volume")
        self.FIELD_BUCKET_CUMULATIVE_VOLUME = resolve_session_field("bucket_cumulative_volume")
        self.FIELD_HIGH = resolve_session_field("high")
        self.FIELD_LOW = resolve_session_field("low")
        self.FIELD_SESSION_DATE = resolve_session_field("session_date")
        self.FIELD_UPDATE_COUNT = resolve_session_field("update_count")
        self.FIELD_LAST_UPDATE_TIMESTAMP = resolve_session_field("last_update_timestamp")
        self.FIELD_FIRST_UPDATE = resolve_session_field("first_update")
        self.FIELD_AVERAGE_PRICE = resolve_session_field("average_price")
        self.FIELD_OHLC = resolve_session_field("ohlc")
        self.FIELD_NET_CHANGE = resolve_session_field("net_change")
    
    def queue_tick_for_storage(self, tick_data: dict):
        """Queue tick for batch Redis storage.
        
        Args:
            tick_data: Tick data dictionary with all fields preserved
        """
        if not tick_data:
            return
        
        symbol = tick_data.get("symbol") or tick_data.get("tradingsymbol") or "UNKNOWN"
        if not symbol or symbol == "UNKNOWN":
            logger.warning(f"⚠️ Skipping tick storage - invalid symbol: {symbol}")
            return
        
        from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
        symbol = RedisKeyStandards.canonical_symbol(symbol)
        
        if self._lock:
            with self._lock:
                self.tick_batch.append(tick_data)
                batch_size = len(self.tick_batch)
                time_since_flush = time.time() - self.last_flush
                
                if batch_size >= self.max_batch_size or time_since_flush >= self.flush_interval:
                    self._flush_batch()
        else:
            self.tick_batch.append(tick_data)
            if len(self.tick_batch) >= self.max_batch_size:
                self._flush_batch()
    
    def _flush_batch(self):
        """Flush batch to Redis using pipeline."""
        if not self.tick_batch:
            return
        
        try:
            from shared_core.redis_clients.redis_client import UnifiedRedisManager
            client = get_redis_client(process_name="redis_storage", db=1)
        except Exception as e:
            logger.error(f"Failed to get Redis client for batch flush: {e}")
            self.tick_batch.clear()
            return
        
        try:
            with client.pipeline() as pipe:
                for tick_data in self.tick_batch:
                    symbol = tick_data.get("symbol") or tick_data.get("tradingsymbol") or "UNKNOWN"
                    if not symbol or symbol == "UNKNOWN":
                        continue
                    
                    symbol = self._resolve_token_to_symbol_for_storage(symbol)
                    if not symbol:
                        continue
                    
                    from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
                    symbol = RedisKeyStandards.canonical_symbol(symbol)
                    
                    from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
                    tick_hash_key = DatabaseAwareKeyBuilder.live_ticks_hash(symbol)
                    
                    tick_mapping = {}
                    for k, v in tick_data.items():
                        if v is not None:
                            canonical_field = resolve_session_field(k)
                            if canonical_field == k:
                                canonical_field = resolve_calculated_field(k)
                            
                            # ✅ FIX: Handle numpy types first (they have .item() method)
                            if hasattr(v, 'item'):  # numpy scalar
                                v = v.item()
                            
                            # ✅ FIX: Serialize complex types (dict, list) to JSON instead of str()
                            if isinstance(v, (dict, list)):
                                tick_mapping[str(canonical_field)] = json.dumps(v, default=str)
                            elif isinstance(v, bytes):
                                import base64
                                tick_mapping[str(canonical_field)] = base64.b64encode(v).decode("utf-8")
                            else:
                                tick_mapping[str(canonical_field)] = str(v)
                    if tick_mapping:
                        pipe.hset(tick_hash_key, mapping=tick_mapping)
                        pipe.expire(tick_hash_key, 300)
                    
                    from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
                    stream_key = DatabaseAwareKeyBuilder.live_processed_stream()
                    # ✅ FIX: Serialize complex types in stream fields
                    stream_fields = {}
                    for k, v in tick_data.items():
                        if v is not None:
                            # Handle numpy types first
                            if hasattr(v, 'item'):  # numpy scalar
                                v = v.item()
                            
                            if isinstance(v, (dict, list)):
                                stream_fields[str(k)] = json.dumps(v, default=str)
                            elif isinstance(v, bytes):
                                import base64
                                stream_fields[str(k)] = base64.b64encode(v).decode("utf-8")
                            else:
                                stream_fields[str(k)] = str(v)
                    pipe.xadd(
                        stream_key,
                        stream_fields,
                        maxlen=10000,
                        approximate=True
                    )
                    
                    price = float(tick_data.get('last_price', tick_data.get('price', 0)))
                    if price > 0:
                        from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
                        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
                        price_key = f"price:latest:{canonical_symbol}"
                        pipe.set(price_key, price, ex=300)
                    
                    self._archive_tick_to_historical_series(pipe, client, symbol, tick_data)
                
                pipe.execute()
            
            self.tick_batch.clear()
            self.last_flush = time.time()
            
        except Exception as e:
            logger.error(f"Error flushing tick batch: {e}")
            self.tick_batch.clear()
    
    def _archive_tick_to_historical_series(self, pipe, client, symbol: str, tick_data: Dict[str, Any]):
        """Archive tick data to historical time series."""
        try:
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
            
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            
            timestamp_ms = tick_data.get('exchange_timestamp_ms') or tick_data.get('timestamp_ms') or int(time.time() * 1000)
            timestamp = timestamp_ms / 1000.0
            
            minute_timestamp = int(timestamp // 60) * 60
            minute_key = DatabaseAwareKeyBuilder.live_ohlc_timeseries(canonical_symbol, "1min")
            
            existing_bar = client.get(minute_key)
            current_price = float(tick_data.get('last_price', tick_data.get('price', 0)))
            current_volume = float(tick_data.get('volume', tick_data.get('bucket_incremental_volume', 0)))
            
            if existing_bar:
                try:
                    bar_data = json.loads(existing_bar)
                    bar_data['high'] = max(bar_data['high'], current_price)
                    bar_data['low'] = min(bar_data['low'], current_price)
                    bar_data['close'] = current_price
                    bar_data['volume'] += current_volume
                    bar_data['tick_count'] += 1
                except:
                    bar_data = self._create_new_minute_bar(minute_timestamp, current_price, current_volume)
            else:
                bar_data = self._create_new_minute_bar(minute_timestamp, current_price, current_volume)
            
            pipe.setex(minute_key, 86400, json.dumps(bar_data))
            
            raw_ticks_key = f"ticks:historical:raw:{canonical_symbol}"
            tick_payload = {
                'p': current_price,
                'v': current_volume,
                'ts': timestamp_ms,
                's': symbol
            }
            
            pipe.zadd(raw_ticks_key, {json.dumps(tick_payload): timestamp_ms})
            pipe.zremrangebyrank(raw_ticks_key, 0, -1001)
            eight_hours_ago_ms = int(time.time() * 1000) - (8 * 60 * 60 * 1000)
            pipe.zremrangebyscore(raw_ticks_key, 0, eight_hours_ago_ms)
            pipe.expire(raw_ticks_key, 28800)
            
        except Exception as e:
            logger.error(f"Historical tick archiving failed for {symbol}: {e}")
    
    def _create_new_minute_bar(self, minute_timestamp: int, price: float, volume: float) -> Dict:
        """Create new 1-minute OHLC bar."""
        return {
            'timestamp': minute_timestamp,
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': volume,
            'tick_count': 1
        }
    
    def flush(self):
        """Manually flush any pending ticks in batch."""
        if self._lock:
            with self._lock:
                self._flush_batch()
        else:
            self._flush_batch()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        if self._lock:
            with self._lock:
                return {
                    'batch_size': len(self.tick_batch),
                    'max_batch_size': self.max_batch_size,
                    'time_since_flush': time.time() - self.last_flush,
                    'flush_interval': self.flush_interval
                }
        else:
            return {
                'batch_size': len(self.tick_batch),
                'max_batch_size': self.max_batch_size,
                'time_since_flush': time.time() - self.last_flush,
                'flush_interval': self.flush_interval
            }
    
    async def store_tick(self, symbol: str, tick_data: dict):
        """Store normalized tick data.
        
        Args:
            symbol: Trading symbol
            tick_data: Tick data dictionary
        """
        if hasattr(self.redis, 'cumulative_tracker') and self.redis.cumulative_tracker:
            symbol = self.redis.cumulative_tracker._resolve_token_to_symbol_for_storage(symbol)
        else:
            symbol = self._resolve_token_to_symbol_for_storage(symbol)
        
        from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
        symbol = RedisKeyStandards.canonical_symbol(symbol)
        
        normalized_tick = tick_data.copy()
        
        if 'exchange_timestamp' in tick_data and tick_data['exchange_timestamp']:
            normalized_tick['exchange_timestamp_ms'] = TimestampNormalizer.to_epoch_ms(tick_data['exchange_timestamp'])
        elif 'timestamp_ns' in tick_data and tick_data['timestamp_ns']:
            normalized_tick['timestamp_ns_ms'] = TimestampNormalizer.to_epoch_ms(tick_data['timestamp_ns'])
        elif 'timestamp' in tick_data and tick_data['timestamp']:
            normalized_tick['timestamp_ms'] = TimestampNormalizer.to_epoch_ms(tick_data['timestamp'])
        
        if 'last_trade_time' in tick_data and tick_data['last_trade_time']:
            normalized_tick['last_trade_time_ms'] = TimestampNormalizer.to_epoch_ms(tick_data['last_trade_time'])
        
        from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
        stream_key = DatabaseAwareKeyBuilder.live_ticks_stream(symbol)
        try:
            from shared_core.redis_clients.redis_client import UnifiedRedisManager
            realtime_client = get_redis_client(process_name="redis_storage", db=1)
        except Exception:
            realtime_client = self.redis.get_client(1) if hasattr(self.redis, 'get_client') else self.redis
        
        stream_data = {
            'data': json.dumps(normalized_tick, default=str),
            'timestamp': str(int(time.time() * 1000)),
            'symbol': symbol
        }
        try:
            # Type ignore: xadd expects Dict[FieldT, EncodableT], but all values are strings (encodable)
            realtime_client.xadd(stream_key, stream_data, maxlen=10000, approximate=True)  # type: ignore[arg-type]
        except Exception as e:
            logger.error(f"Error storing tick for {symbol}: {e}")
            return False
    
    def _resolve_token_to_symbol_for_storage(self, symbol):
        """Resolve token to symbol for Redis storage."""
        try:
            symbol_str = str(symbol) if symbol else ""
            
            if ":" in symbol_str and not symbol_str.isdigit() and not symbol_str.startswith("UNKNOWN_"):
                return symbol
            
            if not symbol_str or symbol_str == "UNKNOWN":
                return None
            
            if symbol_str.isdigit() or symbol_str.startswith("TOKEN_") or symbol_str.startswith("UNKNOWN_"):
                try:
                    if symbol_str.startswith("TOKEN_"):
                        token_str = symbol_str.replace("TOKEN_", "")
                    elif symbol_str.startswith("UNKNOWN_"):
                        token_str = symbol_str.replace("UNKNOWN_", "")
                    else:
                        token_str = symbol_str
                    
                    token = int(token_str)
                    
                    from zerodha.crawlers.hot_token_mapper import get_hot_token_mapper
                    mapper = get_hot_token_mapper()
                    resolved = mapper.token_to_symbol(token)
                    
                    if resolved and not resolved.startswith("UNKNOWN_"):
                        return resolved
                except (ValueError, ImportError):
                    pass
            
            return symbol if symbol_str and ":" in symbol_str else None
        except Exception:
            return symbol if ":" in str(symbol) else None


class HistoricalTickQueries:
    """
    Query historical tick data from Redis time series.
    
    Data Availability:
    - Raw ticks: 8-hour retention (ticks:historical:raw:{symbol})
    - 1-minute bars: 24-hour retention (ohlc:{symbol}:1min)
    - Volume profiles: 8-hour retention (volume_profile:realtime:{symbol})
    """
    
    def __init__(self, redis_client=None):
        from shared_core.redis_clients.redis_client import UnifiedRedisManager
        self.redis_client = redis_client or get_redis_client(process_name="historical_queries", db=1)
    
    def get_historical_ticks(self, symbol: str, start_time: int, end_time: int, max_ticks: int = 1000) -> List[Dict]:
        """Get historical ticks for a symbol in time range."""
        from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
        
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        raw_ticks_key = f"ticks:historical:raw:{canonical_symbol}"
        
        try:
            tick_data_raw = self.redis_client.zrangebyscore(
                raw_ticks_key, start_time, end_time, 
                start=0, num=max_ticks, withscores=True
            )
            
            # Type narrowing: ensure tick_data is iterable before processing
            if not isinstance(tick_data_raw, (list, tuple)):
                return []
            
            ticks = []
            for tick_json, timestamp in tick_data_raw:
                try:
                    tick_obj = json.loads(tick_json)
                    tick_obj['timestamp'] = timestamp
                    ticks.append(tick_obj)
                except json.JSONDecodeError:
                    continue
            
            return ticks
            
        except Exception as e:
            logger.error(f"Historical tick query failed for {symbol}: {e}")
            return []
    
    def get_ohlc_bars(self, symbol: str, timeframe: str, start_time: int, end_time: int) -> List[Dict]:
        """Get OHLC bars for a symbol and timeframe."""
        from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
        
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        
        bars = []
        current_time = start_time
        
        while current_time <= end_time:
            bar_key = DatabaseAwareKeyBuilder.live_ohlc_timeseries(canonical_symbol, timeframe)
            bar_data = self.redis_client.get(bar_key)
            
            if bar_data:
                try:
                    # Type narrowing: ensure bar_data is str or bytes before json.loads
                    if isinstance(bar_data, bytes):
                        bar_str = bar_data.decode('utf-8')
                    elif isinstance(bar_data, str):
                        bar_str = bar_data
                    else:
                        continue  # Skip non-string/bytes types
                    bars.append(json.loads(bar_str))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass
            
            if timeframe == "1min":
                current_time += 60
            elif timeframe == "5min":
                current_time += 300
        
        return bars
    
    def get_volume_profile(self, symbol: str, period: str = "1h") -> Dict:
        """Get volume profile for a symbol."""
        from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
        
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        
        if period == "realtime":
            vp_key = f"volume_profile:realtime:{canonical_symbol}"
        else:
            vp_key = f"volume_profile:{period}:{canonical_symbol}"
        
        try:
            profile_data = self.redis_client.hgetall(vp_key)
            decoded_profile = {}
            for price_bytes, volume_bytes in profile_data.items():  # type: ignore[union-attr]
                price_str = price_bytes.decode('utf-8') if isinstance(price_bytes, bytes) else str(price_bytes)
                volume_str = volume_bytes.decode('utf-8') if isinstance(volume_bytes, bytes) else str(volume_bytes)
                decoded_profile[price_str] = float(volume_str)
            
            return decoded_profile
            
        except Exception as e:
            logger.error(f"Volume profile query failed for {symbol}: {e}")
            return {}
