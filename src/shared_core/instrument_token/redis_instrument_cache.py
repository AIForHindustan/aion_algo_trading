# shared_core/instrument_token/redis_instrument_cache.py
"""
Redis-based instrument cache for algo executor.
Loads from canonical_instruments.yaml and provides fast broker-agnostic lookups.
"""
import yaml
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, TYPE_CHECKING

from shared_core.redis_clients.redis_client import get_redis_client
if TYPE_CHECKING:
    from redis import Redis
from datetime import datetime, timedelta
import asyncio

logger = logging.getLogger(__name__)

# Try to use UnifiedRedisManager if available
try:
    from shared_core.redis_clients.redis_client import UnifiedRedisManager
    UNIFIED_REDIS_AVAILABLE = True
except ImportError:
    try:
        import redis
        UNIFIED_REDIS_AVAILABLE = False
    except ImportError:
        redis = None
        UNIFIED_REDIS_AVAILABLE = False

class RedisInstrumentCache:
    """
    High-performance instrument metadata cache with Redis.
    Stores canonical instrument data and broker mappings from canonical_instruments.yaml.
    Optimized for algo executor signal-based order placement.
    """
    
    def __init__(self, redis_client=None, namespace="instruments", yaml_path=None):
        """
        Initialize Redis instrument cache.
        
        Args:
            redis_client: Optional Redis client (uses UnifiedRedisManager if not provided)
            namespace: Redis key namespace (default: "instruments")
            yaml_path: Path to canonical_instruments.yaml (auto-detects if None)
        """
        if redis_client:
            self.redis = redis_client
        elif UNIFIED_REDIS_AVAILABLE:
            self.redis = get_redis_client(
                process_name="instrument_cache",
                db=0,  # System DB
                port=6379,
                decode_responses=True
            )
        elif redis:
            self.redis = redis.Redis(
                host='localhost', port=6379, db=0, decode_responses=True
            )
        else:
            raise ImportError("Redis client not available. Install redis or shared_core.redis_clients")
        
        self.namespace = namespace
        self.canonical_data = {}
        self.yaml_path = yaml_path or self._find_canonical_yaml()
        
        # Load YAML on initialization
        if self.yaml_path and Path(self.yaml_path).exists():
            self.load_canonical_yaml(self.yaml_path)
    
    def _find_canonical_yaml(self) -> Optional[str]:
        """Auto-detect canonical_instruments.yaml location."""
        possible_paths = [
            Path(__file__).parent / "canonical_instruments.yaml",
            Path(__file__).parent.parent / "instrument_token" / "canonical_instruments.yaml",
            Path(__file__).parent.parent.parent / "shared_core" / "instrument_token" / "canonical_instruments.yaml",
        ]
        for path in possible_paths:
            if path.exists():
                return str(path)
        return None
    
    def load_canonical_yaml(self, yaml_path: str):
        """Load canonical instrument definitions from YAML."""
        try:
            with open(yaml_path, 'r') as f:
                self.canonical_data = yaml.safe_load(f) or {}
            
            instruments_count = len(self.canonical_data.get("instruments", {}))
            logger.info(f"✅ Loaded {instruments_count} instruments from {yaml_path}")
            
            # Index for fast lookups
            self._build_indices()
            
            # Pre-warm Redis cache
            self._pre_warm_cache()
            
            logger.info(f"✅ Pre-warmed Redis cache with {instruments_count} instruments")
        except Exception as e:
            logger.error(f"❌ Failed to load canonical YAML from {yaml_path}: {e}")
            raise
    
    def _build_indices(self):
        """Build lookup indices."""
        self.symbol_to_canonical = {}
        self.token_to_canonical = {}
        
        for canonical_id, instrument in self.canonical_data.get("instruments", {}).items():
            # Index by canonical ID
            self.symbol_to_canonical[canonical_id] = canonical_id
            
            # Index broker-specific symbols
            broker_maps = instrument.get("broker_mappings", {})
            for broker, mapping in broker_maps.items():
                symbol_key = f"{broker}:{mapping.get('tradingsymbol', '')}"
                self.symbol_to_canonical[symbol_key] = canonical_id
                
                # Index by token
                token = mapping.get('instrument_token') or mapping.get('symboltoken')
                if token:
                    token_key = f"{broker}:{token}"
                    self.token_to_canonical[token_key] = canonical_id
    
    def _pre_warm_cache(self):
        """Pre-load frequently accessed instruments to Redis."""
        for canonical_id, instrument in self.canonical_data.get("instruments", {}).items():
            # Store in Redis with 24h expiry
            redis_key = f"{self.namespace}:canonical:{canonical_id}"
            self.redis.setex(
                redis_key,
                timedelta(hours=24),
                json.dumps(instrument)
            )
            
            # Store reverse mappings
            broker_maps = instrument.get("broker_mappings", {})
            for broker, mapping in broker_maps.items():
                # Symbol → Canonical mapping
                symbol = mapping.get('tradingsymbol')
                if symbol:
                    symbol_key = f"{self.namespace}:lookup:symbol:{broker}:{symbol}"
                    self.redis.setex(symbol_key, timedelta(hours=24), canonical_id)
                
                # Token → Canonical mapping
                token = mapping.get('instrument_token') or mapping.get('symboltoken')
                if token:
                    token_key = f"{self.namespace}:lookup:token:{broker}:{token}"
                    self.redis.setex(token_key, timedelta(hours=24), canonical_id)
    
    def get_canonical_instrument(self, canonical_id: str) -> Optional[Dict]:
        """Get instrument by canonical ID."""
        # Try Redis first
        redis_key = f"{self.namespace}:canonical:{canonical_id}"
        cached = self.redis.get(redis_key)
        
        if cached:
            return json.loads(cached)
        
        # Fallback to in-memory
        return self.canonical_data.get("instruments", {}).get(canonical_id)
    
    def resolve_broker_symbol(self, broker: str, symbol: str, exchange: str = None) -> Optional[Dict]:
        """
        Resolve broker symbol to canonical instrument with broker-specific mapping.
        
        Args:
            broker: ZERODHA, ANGEL_ONE, UPSTOX, etc.
            symbol: Trading symbol as known to the broker
            exchange: Optional exchange code
        
        Returns:
            Canonical instrument with requested broker's mapping
        """
        # Build lookup key
        lookup_symbol = symbol
        if exchange and ":" not in symbol:
            lookup_symbol = f"{exchange}:{symbol}"
        
        # Try Redis lookup
        symbol_key = f"{self.namespace}:lookup:symbol:{broker}:{lookup_symbol}"
        canonical_id = self.redis.get(symbol_key)
        
        if not canonical_id:
            # Try fuzzy match or pattern matching
            canonical_id = self._fuzzy_match_symbol(broker, lookup_symbol)
        
        if not canonical_id:
            return None
        
        # Get canonical instrument
        instrument = self.get_canonical_instrument(canonical_id)
        if not instrument:
            return None
        
        # Return with requested broker's mapping
        broker_mapping = instrument.get("broker_mappings", {}).get(broker)
        if not broker_mapping:
            return None
        
        return {
            "canonical": instrument,
            "broker_specific": broker_mapping,
            "broker": broker,
            "canonical_id": canonical_id
        }
    
    def resolve_broker_token(self, broker: str, token: str) -> Optional[Dict]:
        """Resolve broker token to canonical instrument."""
        token_key = f"{self.namespace}:lookup:token:{broker}:{token}"
        canonical_id = self.redis.get(token_key)
        
        if canonical_id:
            instrument = self.get_canonical_instrument(canonical_id)
            if instrument:
                broker_mapping = instrument.get("broker_mappings", {}).get(broker)
                if broker_mapping:
                    return {
                        "canonical": instrument,
                        "broker_specific": broker_mapping,
                        "broker": broker,
                        "canonical_id": canonical_id
                    }
        return None
    
    def _fuzzy_match_symbol(self, broker: str, symbol: str) -> Optional[str]:
        """Advanced fuzzy matching for symbols."""
        # Remove exchange prefixes
        clean_symbol = symbol.split(":")[-1] if ":" in symbol else symbol
        
        # Try different patterns for options
        patterns = [
            clean_symbol,  # Original
            clean_symbol.replace("-EQ", "").replace("-FUT", ""),  # Remove suffixes
            # Add more fuzzy matching logic
        ]
        
        for pattern in patterns:
            # Scan Redis keys for pattern
            pattern_key = f"{self.namespace}:lookup:symbol:{broker}:*{pattern}*"
            keys = self.redis.keys(pattern_key)
            if keys:
                return self.redis.get(keys[0])
        
        return None
    
    async def stream_instrument_updates(self, stream_name="instrument:updates"):
        """
        Listen for instrument updates via Redis Stream.
        Used by robot executors for real-time lookups.
        """
        last_id = "0"
        
        while True:
            try:
                # Read from stream
                messages = self.redis.xread(
                    {stream_name: last_id},
                    count=10,
                    block=5000
                )
                
                for stream, message_list in messages:
                    for message_id, data in message_list:
                        last_id = message_id
                        
                        # Process update
                        await self._process_stream_update(data)
                        
                        # Acknowledge processing
                        self.redis.xack(stream_name, "instrument_consumers", message_id)
                
            except Exception as e:
                print(f"Stream error: {e}")
                await asyncio.sleep(1)
    
    async def _process_stream_update(self, data: Dict):
        """Process instrument update from stream."""
        update_type = data.get("type")
        
        if update_type == "NEW_INSTRUMENT":
            # Add new instrument to cache
            canonical_id = data.get("canonical_id")
            instrument_data = json.loads(data.get("instrument", "{}"))
            
            # Update Redis cache
            redis_key = f"{self.namespace}:canonical:{canonical_id}"
            self.redis.setex(redis_key, timedelta(hours=24), json.dumps(instrument_data))
            
            # Update lookup indices
            broker_maps = instrument_data.get("broker_mappings", {})
            for broker, mapping in broker_maps.items():
                symbol = mapping.get('tradingsymbol')
                if symbol:
                    symbol_key = f"{self.namespace}:lookup:symbol:{broker}:{symbol}"
                    self.redis.setex(symbol_key, timedelta(hours=24), canonical_id)
                
                # Also update token mapping
                token = mapping.get('instrument_token') or mapping.get('symboltoken')
                if token:
                    token_key = f"{self.namespace}:lookup:token:{broker}:{token}"
                    self.redis.setex(token_key, timedelta(hours=24), canonical_id)
    
    def get_all_instruments(self) -> Dict[str, Dict]:
        """Get all instruments from cache (for algo executor)."""
        instruments = {}
        
        # Try to get from Redis first (batch operation)
        pattern = f"{self.namespace}:canonical:*"
        keys = self.redis.keys(pattern)
        
        if keys:
            for key in keys:
                canonical_id = key.replace(f"{self.namespace}:canonical:", "")
                instrument = self.get_canonical_instrument(canonical_id)
                if instrument:
                    instruments[canonical_id] = instrument
        
        # Fallback to in-memory
        if not instruments:
            instruments = self.canonical_data.get("instruments", {})
        
        return instruments
    
    def get_instruments_by_broker(self, broker: str) -> Dict[str, Dict]:
        """Get all instruments available for a specific broker."""
        broker_instruments = {}
        
        for canonical_id, instrument in self.canonical_data.get("instruments", {}).items():
            broker_mapping = instrument.get("broker_mappings", {}).get(broker.upper())
            if broker_mapping:
                broker_instruments[canonical_id] = {
                    "canonical": instrument,
                    "broker_specific": broker_mapping,
                    "canonical_id": canonical_id
                }
        
        return broker_instruments
    
    def get_instrument_for_algo_executor(self, symbol: str, broker: str) -> Optional[Dict]:
        """
        Get instrument details optimized for algo executor order placement.
        
        Args:
            symbol: Trading symbol (can be in any broker format)
            broker: Target broker (ZERODHA, ANGEL_ONE)
        
        Returns:
            Dict with broker-specific token and symbol, or None if not found
        """
        # Try direct resolution
        result = self.resolve_broker_symbol(broker, symbol)
        if result:
            broker_specific = result.get("broker_specific", {})
            return {
                "symbol": broker_specific.get("tradingsymbol", symbol),
                "token": broker_specific.get("instrument_token") or broker_specific.get("symboltoken"),
                "exchange": broker_specific.get("exchange") or broker_specific.get("exchseg"),
                "canonical_id": result.get("canonical_id"),
                "canonical": result.get("canonical", {})
            }
        
        return None
    
    def get_all_instruments(self) -> Dict[str, Dict]:
        """Get all instruments from cache (for algo executor)."""
        instruments = {}
        
        # Try to get from Redis first (batch operation)
        pattern = f"{self.namespace}:canonical:*"
        keys = self.redis.keys(pattern)
        
        if keys:
            for key in keys:
                canonical_id = key.replace(f"{self.namespace}:canonical:", "")
                instrument = self.get_canonical_instrument(canonical_id)
                if instrument:
                    instruments[canonical_id] = instrument
        
        # Fallback to in-memory
        if not instruments:
            instruments = self.canonical_data.get("instruments", {})
        
        return instruments
    
    def get_instruments_by_broker(self, broker: str) -> Dict[str, Dict]:
        """Get all instruments available for a specific broker."""
        broker_instruments = {}
        
        for canonical_id, instrument in self.canonical_data.get("instruments", {}).items():
            broker_mapping = instrument.get("broker_mappings", {}).get(broker.upper())
            if broker_mapping:
                broker_instruments[canonical_id] = {
                    "canonical": instrument,
                    "broker_specific": broker_mapping,
                    "canonical_id": canonical_id
                }
        
        return broker_instruments