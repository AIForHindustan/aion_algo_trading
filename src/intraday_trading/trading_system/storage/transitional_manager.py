"""
Transitional Manager - Zero Downtime Migration
==============================================

Dual-write strategy for safe migration from old to new Redis structure.
Writes to both old and new structures during transition period.

Phase 1: Dual Write (1 week)
- Write to OLD structure (backward compatibility)
- Write to NEW unified structure (trading_system)
- Monitor both writes for consistency

Phase 2: Read from New (1 week)
- Read from NEW structure (primary)
- Fallback to OLD structure if needed
- Monitor read patterns

Phase 3: Cutover (1 day)
- Stop writing to OLD structure
- Remove OLD structure
- Full migration complete
"""

import logging
import time
import json
from datetime import datetime
from typing import Dict, Optional, Any, List
import redis

from shared_core.redis_clients.redis_client import get_redis_client
logger = logging.getLogger(__name__)

# ✅ UPDATED: Use UnifiedRedisManager from redis_files (all Redis files consolidated)
try:
    from shared_core.redis_clients.redis_client import UnifiedRedisManager, RedisClientFactory
    from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
    # ✅ REMOVED: UnifiedDataIngestor - not used in production (legacy code)
    NEW_SYSTEM_AVAILABLE = True
except ImportError:
    NEW_SYSTEM_AVAILABLE = False
    logger.warning("⚠️ New system components not available")


class TransitionalManager:
    """
    Dual-write manager for zero-downtime migration.
    
    During migration, writes to both:
    - OLD structure: Legacy Redis keys (volume_state:{symbol}, indicators:{symbol}:rsi, etc.)
    - NEW structure: Unified trading_system structure
    
    This ensures backward compatibility while migrating to new structure.
    """
    
    def __init__(self, old_redis_client=None, new_redis_client=None, enable_dual_write=False):
        """
        Initialize TransitionalManager.
        
        Args:
            old_redis_client: Redis client for OLD structure (legacy)
            new_redis_client: Redis client for NEW structure (unified)
            enable_dual_write: If True, write to both structures (default: False - migration complete)
        """
        self.enable_dual_write = enable_dual_write
        
        # OLD structure clients (legacy databases)
        if old_redis_client:
            self.old_redis = old_redis_client
        else:
            # ✅ UPDATED: Use UnifiedRedisManager
            if NEW_SYSTEM_AVAILABLE:
                # DB 2: Volume data (old structure)
                self.old_volume_db = get_redis_client(
                    process_name="transitional_manager",
                    db=2,  # DB 2: Analytics (volume_state keys)
                    max_connections=None
                )
                # DB 5: Indicators (old structure)
                self.old_indicator_db = get_redis_client(
                    process_name="transitional_manager",
                    db=5,  # DB 5: Indicators cache
                    max_connections=None
                )
                # DB 0: System data (old structure)
                self.old_system_db = get_redis_client(
                    process_name="transitional_manager",
                    db=0,  # DB 0: System
                    max_connections=None
                )
            else:
                self.old_volume_db = None
                self.old_indicator_db = None
                self.old_system_db = None
                logger.warning("⚠️ Cannot create old structure clients")
        
        # NEW structure client (unified)
        if new_redis_client:
            self.new_redis = new_redis_client
        else:
            # ✅ UPDATED: Use UnifiedRedisManager
            if NEW_SYSTEM_AVAILABLE:
                # DB 1: Realtime data (new unified structure)
                self.new_redis = get_redis_client(
                    process_name="transitional_manager",
                    db=1,  # DB 1: Realtime (unified)
                    max_connections=None
                )
            else:
                self.new_redis = None
                logger.warning("⚠️ Cannot create new structure client")
        
        # Statistics
        self.stats = {
            'old_writes': 0,
            'new_writes': 0,
            'old_write_errors': 0,
            'new_write_errors': 0,
            'dual_write_success': 0,
            'start_time': time.time()
        }
        
        if self.enable_dual_write:
            logger.info("✅ TransitionalManager initialized (dual-write mode)")
        else:
            logger.info("✅ TransitionalManager initialized (single-write mode - NEW structure only)")
    
    def store_tick(self, symbol: str, data: dict):
        """
        Store tick data in both OLD and NEW structures.
        
        OLD structure:
        - volume_state:{symbol} (DB 2)
        - indicators:{symbol}:rsi, indicators:{symbol}:macd, etc. (DB 5)
        
        NEW structure:
        - ticks:realtime:{symbol} (DB 1 - unified stream)
        - tick:latest:{symbol} (DB 1 - latest hash)
        
        Args:
            symbol: Trading symbol
            data: Tick data dictionary
        """
        if not self.enable_dual_write:
            # Only write to new structure
            self._write_new_structure(symbol, data)
            return
        
        # Dual write: Write to both structures
        old_success = self._write_old_structure(symbol, data)
        new_success = self._write_new_structure(symbol, data)
        
        if old_success and new_success:
            self.stats['dual_write_success'] += 1
        else:
            if not old_success:
                self.stats['old_write_errors'] += 1
            if not new_success:
                self.stats['new_write_errors'] += 1
    
    def _write_old_structure(self, symbol: str, data: dict) -> bool:
        """
        Write to OLD Redis structure (legacy format).
        
        OLD structure keys:
        - volume_state:{symbol} (DB 2) - Hash with volume state
        - indicators:{symbol}:rsi (DB 5) - String value
        - indicators:{symbol}:macd (DB 5) - String value
        - indicators:{symbol}:bollinger (DB 5) - String value
        - etc.
        
        Args:
            symbol: Trading symbol
            data: Tick data dictionary
        
        Returns:
            True if write successful, False otherwise
        """
        if not self.old_volume_db or not self.old_indicator_db:
            return False
        
        try:
            # Write volume state to DB 2
            volume_state_key = f"volume_state:{symbol}"
            volume_state_data = {
                'last_cumulative': str(data.get('zerodha_cumulative_volume', 0)),
                'session_date': datetime.now().strftime('%Y-%m-%d'),
                'last_price': str(data.get('last_price', 0)),
                'incremental_volume': str(data.get('bucket_incremental_volume', 0))
            }
            self.old_volume_db.hset(volume_state_key, mapping=volume_state_data)
            self.old_volume_db.expire(volume_state_key, 57600)  # 16 hours TTL
            
            # Write indicators to DB 5
            if 'rsi' in data:
                self.old_indicator_db.set(f"indicators:{symbol}:rsi", str(data['rsi']))
                self.old_indicator_db.expire(f"indicators:{symbol}:rsi", 3600)  # 1 hour TTL
            
            if 'macd' in data:
                macd_data = data['macd']
                if isinstance(macd_data, dict):
                    # MACD is a dict with macd, signal, histogram
                    self.old_indicator_db.set(f"indicators:{symbol}:macd", json.dumps(macd_data))
                else:
                    self.old_indicator_db.set(f"indicators:{symbol}:macd", str(macd_data))
                self.old_indicator_db.expire(f"indicators:{symbol}:macd", 3600)
            
            if 'bollinger_bands' in data:
                bb_data = data['bollinger_bands']
                if isinstance(bb_data, dict):
                    self.old_indicator_db.set(f"indicators:{symbol}:bollinger", json.dumps(bb_data))
                else:
                    self.old_indicator_db.set(f"indicators:{symbol}:bollinger", str(bb_data))
                self.old_indicator_db.expire(f"indicators:{symbol}:bollinger", 3600)
            
            # Write volume ratio
            if 'volume_ratio' in data:
                self.old_indicator_db.set(f"indicators:{symbol}:volume_ratio", str(data['volume_ratio']))
                self.old_indicator_db.expire(f"indicators:{symbol}:volume_ratio", 3600)
            
            # Write Greeks if available
            if 'delta' in data:
                self.old_indicator_db.set(f"indicators:{symbol}:delta", str(data['delta']))
                self.old_indicator_db.expire(f"indicators:{symbol}:delta", 3600)
            
            if 'gamma' in data:
                self.old_indicator_db.set(f"indicators:{symbol}:gamma", str(data['gamma']))
                self.old_indicator_db.expire(f"indicators:{symbol}:gamma", 3600)
            
            self.stats['old_writes'] += 1
            return True
            
        except Exception as e:
            logger.error(f"Error writing to OLD structure for {symbol}: {e}")
            return False
    
    def _write_new_structure(self, symbol: str, data: dict) -> bool:
        """
        Write to NEW unified Redis structure.
        
        NEW structure keys:
        - ticks:realtime:{symbol} (DB 1) - Stream with all tick data
        - tick:latest:{symbol} (DB 1) - Hash with latest tick fields
        
        Args:
            symbol: Trading symbol
            data: Tick data dictionary
        
        Returns:
            True if write successful, False otherwise
        """
        if not self.new_redis:
            return False
        
        try:
            # Use DatabaseAwareKeyBuilder for Redis keys
            # Use live_ticks_stream() which returns ticks:stream:{symbol}
            stream_key = DatabaseAwareKeyBuilder.live_ticks_stream(symbol)
            tick_json = json.dumps(data)
            
            self.new_redis.xadd(
                stream_key,
                {'data': tick_json, 'timestamp': str(time.time())},
                maxlen=10000,  # Keep last 10k ticks
                approximate=True
            )
            
            # ✅ CENTRALIZED: Use key builder for latest tick key (returns ticks:latest:{symbol})
            latest_key = builder.live_ticks_latest(symbol)
            key_fields = ['last_price', 'volume', 'volume_ratio', 'timestamp', 
                         'rsi', 'macd', 'delta', 'gamma', 'theta', 'vega']
            hash_data = {
                k: str(data.get(k, ''))
                for k in key_fields
                if k in data
            }
            
            # Handle nested indicators
            if 'macd' in data and isinstance(data['macd'], dict):
                hash_data['macd'] = json.dumps(data['macd'])
            if 'bollinger_bands' in data and isinstance(data['bollinger_bands'], dict):
                hash_data['bollinger_bands'] = json.dumps(data['bollinger_bands'])
            
            if hash_data:
                self.new_redis.hset(latest_key, mapping=hash_data)
                self.new_redis.expire(latest_key, 3600)  # 1 hour TTL
            
            self.stats['new_writes'] += 1
            return True
            
        except Exception as e:
            logger.error(f"Error writing to NEW structure for {symbol}: {e}")
            return False
    
    def read_tick(self, symbol: str, use_new_structure: bool = True) -> Optional[Dict[str, Any]]:
        """
        Read tick data from structure (NEW by default, fallback to OLD).
        
        ✅ Phase 2: Read from NEW structure (primary), fallback to OLD if needed.
        
        Args:
            symbol: Trading symbol
            use_new_structure: If True, read from NEW structure first (default: True)
        
        Returns:
            Tick data dictionary or None if not found
        """
        # ✅ PHASE 2: Always try NEW structure first
        if use_new_structure:
            # Try NEW structure first
            tick_data = self._read_new_structure(symbol)
            if tick_data:
                self.stats['read_new_success'] += 1
                return tick_data
            
            # Fallback to OLD structure
            logger.debug(f"Tick not found in NEW structure for {symbol}, trying OLD structure")
            tick_data = self._read_old_structure(symbol)
            if tick_data:
                self.stats['read_old_fallback'] += 1
            else:
                self.stats['read_errors'] += 1
            return tick_data
        else:
            # Read from OLD structure (backward compatibility)
            return self._read_old_structure(symbol)
    
    def _read_new_structure(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Read from NEW unified structure"""
        if not self.new_redis:
            return None
        
        try:
            # Use DatabaseAwareKeyBuilder for latest tick key
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            latest_key = DatabaseAwareKeyBuilder.live_ticks_latest(symbol)
            hash_data = self.new_redis.hgetall(latest_key)
            
            if hash_data:
                # Convert back to dict
                tick_data = {}
                for k, v in hash_data.items():
                    # Try to parse JSON for nested structures
                    if k in ['macd', 'bollinger_bands']:
                        try:
                            tick_data[k] = json.loads(v)
                        except:
                            tick_data[k] = v
                    else:
                        # Try to convert to float/int
                        try:
                            tick_data[k] = float(v)
                        except:
                            tick_data[k] = v
                
                return tick_data
            
            # Fallback: Read from stream
            # Use DatabaseAwareKeyBuilder for consistent key naming
            stream_key = DatabaseAwareKeyBuilder.live_ticks_stream(symbol)
            messages = self.new_redis.xrevrange(stream_key, count=1)
            
            if messages:
                msg_id, msg_data = messages[0]
                if 'data' in msg_data:
                    tick_json = msg_data['data']
                    if isinstance(tick_json, bytes):
                        tick_json = tick_json.decode('utf-8')
                    return json.loads(tick_json)
            
            return None
            
        except Exception as e:
            logger.debug(f"Error reading from NEW structure for {symbol}: {e}")
            return None
    
    def _read_old_structure(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Read from OLD structure (legacy format)"""
        if not self.old_volume_db or not self.old_indicator_db:
            return None
        
        try:
            tick_data = {}
            
            # Read volume state from DB 2
            volume_state_key = f"volume_state:{symbol}"
            volume_state = self.old_volume_db.hgetall(volume_state_key)
            if volume_state:
                # Decode bytes if needed
                if isinstance(list(volume_state.values())[0], bytes):
                    volume_state = {k.decode(): v.decode() for k, v in volume_state.items()}
                
                tick_data['zerodha_cumulative_volume'] = float(volume_state.get('last_cumulative', 0))
                tick_data['bucket_incremental_volume'] = float(volume_state.get('incremental_volume', 0))
                tick_data['last_price'] = float(volume_state.get('last_price', 0))
            
            # Read indicators from DB 5
            indicators = {}
            indicator_keys = ['rsi', 'macd', 'bollinger', 'volume_ratio', 'delta', 'gamma']
            
            for indicator in indicator_keys:
                key = f"indicators:{symbol}:{indicator}"
                value = self.old_indicator_db.get(key)
                if value:
                    if isinstance(value, bytes):
                        value = value.decode('utf-8')
                    
                    # Try to parse JSON for complex indicators
                    if indicator in ['macd', 'bollinger']:
                        try:
                            indicators[indicator] = json.loads(value)
                        except:
                            indicators[indicator] = value
                    else:
                        try:
                            indicators[indicator] = float(value)
                        except:
                            indicators[indicator] = value
            
            if indicators:
                tick_data.update(indicators)
            
            return tick_data if tick_data else None
            
        except Exception as e:
            logger.debug(f"Error reading from OLD structure for {symbol}: {e}")
            return None
    
    def verify_consistency(self, symbol: str) -> Dict[str, Any]:
        """
        Verify data consistency between OLD and NEW structures.
        
        Args:
            symbol: Trading symbol to verify
        
        Returns:
            Dictionary with verification results
        """
        old_data = self._read_old_structure(symbol)
        new_data = self._read_new_structure(symbol)
        
        verification = {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'old_structure_exists': old_data is not None,
            'new_structure_exists': new_data is not None,
            'consistent': False,
            'differences': []
        }
        
        if not old_data and not new_data:
            verification['consistent'] = True  # Both missing is consistent
            return verification
        
        if not old_data or not new_data:
            verification['differences'].append('One structure missing data')
            return verification
        
        # Compare key fields
        key_fields = ['last_price', 'volume_ratio', 'rsi']
        for field in key_fields:
            old_val = old_data.get(field)
            new_val = new_data.get(field)
            
            if old_val is None and new_val is None:
                continue
            
            if old_val is None or new_val is None:
                verification['differences'].append(f'{field}: One structure missing')
                continue
            
            # Compare with tolerance for floating point
            try:
                old_float = float(old_val)
                new_float = float(new_val)
                if abs(old_float - new_float) > 0.01:  # 1% tolerance
                    verification['differences'].append(f'{field}: {old_float} vs {new_float}')
            except:
                if str(old_val) != str(new_val):
                    verification['differences'].append(f'{field}: Different values')
        
        verification['consistent'] = len(verification['differences']) == 0
        return verification
    
    def get_stats(self) -> Dict[str, Any]:
        """Get migration statistics"""
        elapsed = time.time() - self.stats['start_time']
        return {
            **self.stats,
            'elapsed_seconds': elapsed,
            'old_write_rate': self.stats['old_writes'] / elapsed if elapsed > 0 else 0,
            'new_write_rate': self.stats['new_writes'] / elapsed if elapsed > 0 else 0,
            'dual_write_success_rate': (
                self.stats['dual_write_success'] / self.stats['old_writes'] 
                if self.stats['old_writes'] > 0 else 0
            )
        }
    
    def disable_dual_write(self):
        """Disable dual write (Phase 3: Cutover - stop writing to OLD structure)"""
        self.enable_dual_write = False
        logger.info("🔄 Dual write disabled - now writing only to NEW structure")
    
    def enable_dual_write_mode(self):
        """Enable dual write (Phase 1: Migration)"""
        self.enable_dual_write = True
        logger.info("🔄 Dual write enabled - writing to both OLD and NEW structures")


# Convenience function
def create_transitional_manager(old_redis_client=None, new_redis_client=None) -> TransitionalManager:
    """Create TransitionalManager with default configuration (single-write mode)"""
    return TransitionalManager(
        old_redis_client=old_redis_client,
        new_redis_client=new_redis_client,
        enable_dual_write=False  # Migration complete - write only to new structure
    )
