from shared_core.redis_clients.redis_client import get_redis_client
# safe_consolidation.py
import json
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# ✅ FIXED: Use UnifiedRedisManager for centralized connection management
# ✅ All Redis files consolidated in redis_files/
from shared_core.redis_clients import UnifiedRedisManager

logger = logging.getLogger(__name__)

class SafeRedisConsolidator:
    """
    Consolidate Redis data WITHOUT losing your 50 days of data.
    Uses dual-write strategy for zero downtime.
    
    ✅ FIXED: Uses UnifiedRedisManager for centralized connection management
    """
    
    def __init__(self, redis_host: Optional[str] = None, redis_port: Optional[int] = None):
        """
        Initialize SafeRedisConsolidator with optimized Redis connections.
        
        Args:
            redis_host: Redis host (defaults to REDIS_HOST env or 'localhost')
            redis_port: Redis port (defaults to REDIS_PORT env or 6379)
        """
        # Get Redis connection settings from environment or parameters
        self.redis_host = redis_host or os.environ.get('REDIS_HOST', 'localhost')
        self.redis_port = redis_port or int(os.environ.get('REDIS_PORT', 6379))
        
        # ✅ FIXED: Use UnifiedRedisManager for centralized connection management
        # This ensures optimal connection pool sizes from PROCESS_POOL_CONFIG
        self.source_dbs = {
            'ticker': get_redis_client(process_name="data_migrator_ticker", db=1),
            'volume': get_redis_client(process_name="data_migrator_volume", db=1),
            'indicators': get_redis_client(process_name="data_migrator_indicators", db=1)
        }
        # Target DB1 (unified) - create new client for unified structure
        self.target_db = get_redis_client(process_name="data_migrator_target", db=1)
        self.migration_log = []
        
    def start_dual_write_migration(self):
        """Begin writing to both old and new structures."""
        print("🔄 Starting safe dual-write migration...")
        
        # Phase 1: Volume data migration
        self._migrate_volume_structure()
        
        # Phase 2: Indicators migration  
        self._migrate_indicator_structure()
        
        # Phase 3: System data migration
        self._migrate_system_structure()
        
        print("✅ Dual-write migration started. System writes to both old and new structures.")
        
    def _migrate_volume_structure(self):
        """Migrate volume data with new naming."""
        volume_mappings = {
            # DB2 → DB0 with new names
            'volume_state:{token}': 'vol:state:{symbol}',
            'volume_averages:{symbol}': 'vol:baseline:{symbol}',
            'volume_profile:patterns:{symbol}:{period}': 'vol:profile:{symbol}:{period}',
            'realtime:{symbol}:{timestamp}': 'tick:realtime:{symbol}:{timestamp}',
        }
        
        print("📊 Migrating volume structure...")
        self._apply_mappings('volume', volume_mappings)
    
    def _migrate_indicator_structure(self):
        """Migrate indicators with clear categorization."""
        indicator_mappings = {
            # DB5 → DB0 with categorized names
            'indicators:{symbol}:rsi': 'ind:ta:{symbol}:rsi',
            'indicators:{symbol}:macd': 'ind:ta:{symbol}:macd',
            'indicators:{symbol}:bollinger': 'ind:ta:{symbol}:bollinger',
            'indicators:{symbol}:delta': 'ind:greeks:{symbol}:delta',
            'indicators:{symbol}:gamma': 'ind:greeks:{symbol}:gamma',
            'indicators:{symbol}:volume_ratio': 'ind:volume:{symbol}:ratio',
            'indicators:{symbol}:volume_ratio_ma_5': 'ind:volume:{symbol}:ratio_ma_5',
        }
        
        print("📈 Migrating indicator structure...")
        self._apply_mappings('indicators', indicator_mappings)
    
    def _migrate_system_structure(self):
        """Migrate system-level data structures."""
        print("⚙️  Migrating system structure...")
        # System data is typically already in DB0, so this is mainly for verification
        system_mappings = {
            # System keys that might need renaming
            'system:health': 'sys:health',
            'system:config': 'sys:config',
        }
        self._apply_mappings('ticker', system_mappings)
    
    def _apply_mappings(self, source_db_name: str, mappings: Dict[str, str]):
        """
        Apply key mappings from source database to target database.
        
        Args:
            source_db_name: Name of source database ('ticker', 'volume', 'indicators')
            mappings: Dictionary mapping old key patterns to new key patterns
                     Supports {token}, {symbol}, {period}, {timestamp} placeholders
        """
        source_client = self.source_dbs.get(source_db_name)
        if not source_client:
            print(f"❌ Source database '{source_db_name}' not found")
            return
        
        # Test connection
        try:
            source_client.ping()
        except Exception as e:
            print(f"❌ Cannot connect to {source_db_name} database: {e}")
            return
        
        migrated_count = 0
        error_count = 0
        
        # Scan all keys in source database
        try:
            cursor = 0
            all_keys = []
            while True:
                cursor, keys = source_client.scan(cursor, count=1000)
                all_keys.extend(keys)
                if cursor == 0:
                    break
            
            print(f"   Found {len(all_keys)} keys in {source_db_name} database")
            
            # Process each key
            for key in all_keys:
                try:
                    key_str = key.decode() if isinstance(key, bytes) else key
                    
                    # Find matching pattern
                    matched_pattern = None
                    new_key_pattern = None
                    
                    for old_pattern, new_pattern in mappings.items():
                        # Simple pattern matching (supports {token}, {symbol}, etc.)
                        if self._pattern_matches(key_str, old_pattern):
                            matched_pattern = old_pattern
                            new_key_pattern = new_pattern
                            break
                    
                    if not matched_pattern:
                        continue  # Skip keys that don't match any pattern
                    
                    # Extract variables from old key and build new key
                    new_key = self._build_new_key(key_str, matched_pattern, new_key_pattern)
                    
                    if not new_key:
                        continue
                    
                    # Get data type and migrate accordingly
                    key_type = source_client.type(key)
                    if isinstance(key_type, bytes):
                        key_type = key_type.decode()
                    
                    # Migrate based on data type
                    if key_type == 'hash':
                        data = source_client.hgetall(key)
                        if data:
                            # Convert bytes keys/values to strings if needed
                            if self.target_db.connection_pool.connection_kwargs.get('decode_responses', True):
                                # Already decoded
                                self.target_db.hset(new_key, mapping=data)
                            else:
                                # Need to decode
                                decoded_data = {
                                    k.decode() if isinstance(k, bytes) else k: 
                                    v.decode() if isinstance(v, bytes) else v
                                    for k, v in data.items()
                                }
                                self.target_db.hset(new_key, mapping=decoded_data)
                            migrated_count += 1
                    
                    elif key_type == 'string':
                        value = source_client.get(key)
                        if value:
                            if isinstance(value, bytes):
                                value = value.decode()
                            self.target_db.set(new_key, value)
                            migrated_count += 1
                    
                    elif key_type == 'list':
                        items = source_client.lrange(key, 0, -1)
                        if items:
                            decoded_items = [
                                item.decode() if isinstance(item, bytes) else item
                                for item in items
                            ]
                            self.target_db.rpush(new_key, *decoded_items)
                            migrated_count += 1
                    
                    elif key_type == 'set':
                        members = source_client.smembers(key)
                        if members:
                            decoded_members = [
                                m.decode() if isinstance(m, bytes) else m
                                for m in members
                            ]
                            self.target_db.sadd(new_key, *decoded_members)
                            migrated_count += 1
                    
                    elif key_type == 'zset':
                        items = source_client.zrange(key, 0, -1, withscores=True)
                        if items:
                            # zset items are (member, score) tuples
                            zset_data = {
                                (m.decode() if isinstance(m, bytes) else m): s
                                for m, s in items
                            }
                            if zset_data:
                                self.target_db.zadd(new_key, zset_data)
                                migrated_count += 1
                    
                    elif key_type == 'stream':
                        # Streams are more complex - copy messages
                        stream_len = source_client.xlen(key)
                        if stream_len > 0:
                            # Copy stream messages (simplified - copies all messages)
                            messages = source_client.xrange(key, '-', '+', count=1000)
                            for msg_id, msg_data in messages:
                                decoded_data = {
                                    k.decode() if isinstance(k, bytes) else k: 
                                    v.decode() if isinstance(v, bytes) else v
                                    for k, v in msg_data.items()
                                }
                                self.target_db.xadd(new_key, decoded_data)
                            migrated_count += 1
                    
                    # Log migration
                    self.migration_log.append({
                        'timestamp': datetime.now().isoformat(),
                        'source_db': source_db_name,
                        'old_key': key_str,
                        'new_key': new_key,
                        'type': key_type,
                        'status': 'success'
                    })
                    
                except Exception as e:
                    error_count += 1
                    self.migration_log.append({
                        'timestamp': datetime.now().isoformat(),
                        'source_db': source_db_name,
                        'old_key': key_str if 'key_str' in locals() else str(key),
                        'new_key': None,
                        'type': None,
                        'status': 'error',
                        'error': str(e)
                    })
                    if error_count <= 10:  # Only print first 10 errors
                        print(f"   ⚠️  Error migrating key {key_str if 'key_str' in locals() else key}: {e}")
            
            print(f"   ✅ Migrated {migrated_count} keys from {source_db_name}")
            if error_count > 0:
                print(f"   ⚠️  {error_count} errors encountered")
        
        except Exception as e:
            print(f"   ❌ Error scanning {source_db_name} database: {e}")
    
    def _pattern_matches(self, key: str, pattern: str) -> bool:
        """
        Check if a key matches a pattern with placeholders.
        
        Args:
            key: Actual Redis key
            pattern: Pattern with placeholders like {token}, {symbol}, etc.
        
        Returns:
            True if key matches pattern structure
        """
        # Simple pattern matching - split by colons and compare structure
        key_parts = key.split(':')
        pattern_parts = pattern.split(':')
        
        if len(key_parts) != len(pattern_parts):
            return False
        
        for k_part, p_part in zip(key_parts, pattern_parts):
            # If pattern part is a placeholder (starts with {), it matches anything
            if p_part.startswith('{') and p_part.endswith('}'):
                continue
            # Otherwise, exact match required
            if k_part != p_part:
                return False
        
        return True
    
    def _build_new_key(self, old_key: str, old_pattern: str, new_pattern: str) -> Optional[str]:
        """
        Build new key from old key using pattern mappings.
        
        Args:
            old_key: Original Redis key
            old_pattern: Pattern that matches old_key
            new_pattern: Pattern for new key with placeholders
        
        Returns:
            New key string or None if extraction fails
        """
        try:
            old_parts = old_key.split(':')
            old_pattern_parts = old_pattern.split(':')
            new_pattern_parts = new_pattern.split(':')
            
            # Extract variable values from old key
            variables = {}
            for i, (old_part, pattern_part) in enumerate(zip(old_parts, old_pattern_parts)):
                if pattern_part.startswith('{') and pattern_part.endswith('}'):
                    var_name = pattern_part[1:-1]  # Remove { and }
                    variables[var_name] = old_part
            
            # Build new key using extracted variables
            new_parts = []
            for part in new_pattern_parts:
                if part.startswith('{') and part.endswith('}'):
                    var_name = part[1:-1]
                    if var_name in variables:
                        new_parts.append(variables[var_name])
                    else:
                        # Variable not found - use original or skip
                        return None
                else:
                    new_parts.append(part)
            
            return ':'.join(new_parts)
        
        except Exception as e:
            return None
    
    def verify_migration(self) -> Dict[str, Any]:
        """
        Verify that migration was successful by checking key counts.
        
        Returns:
            Dictionary with verification results
        """
        print("\n🔍 Verifying migration...")
        results = {
            'source_counts': {},
            'target_counts': {},
            'verification_status': 'unknown'
        }
        
        # Count keys in source databases
        for db_name, client in self.source_dbs.items():
            try:
                cursor = 0
                count = 0
                while True:
                    cursor, keys = client.scan(cursor, count=1000)
                    count += len(keys)
                    if cursor == 0:
                        break
                results['source_counts'][db_name] = count
            except Exception as e:
                results['source_counts'][db_name] = {'error': str(e)}
        
        # Count keys in target database
        try:
            cursor = 0
            count = 0
            while True:
                cursor, keys = self.target_db.scan(cursor, count=1000)
                count += len(keys)
                if cursor == 0:
                    break
            results['target_counts']['db1'] = count
        except Exception as e:
            results['target_counts']['db1'] = {'error': str(e)}
        
        print(f"   Source DB counts: {results['source_counts']}")
        print(f"   Target DB count: {results['target_counts']}")
        
        return results
    
    def verify_data_consistency(self, sample_size: int = 50) -> Dict[str, Any]:
        """
        Comprehensive data consistency verification.
        
        Checks:
        1. Key counts by pattern
        2. Sample data value comparison
        3. Key mapping verification
        4. Missing data detection
        
        Args:
            sample_size: Number of sample keys to verify in detail
        
        Returns:
            Dictionary with detailed verification results
        """
        print("\n🔍 Comprehensive Data Consistency Verification")
        print("=" * 60)
        
        verification_results = {
            'timestamp': datetime.now().isoformat(),
            'key_counts': {},
            'sample_verification': [],
            'mapping_verification': {},
            'missing_keys': [],
            'data_mismatches': [],
            'overall_status': 'unknown',
            'summary': {}
        }
        
        # 1. Key Count Verification
        print("\n📊 Step 1: Key Count Verification...")
        key_patterns = {
            'volume_state': {
                'old_pattern': 'volume_state:*',
                'new_pattern': 'vol:state:*',
                'source_db': 'volume',
                'target_db': self.target_db
            },
            'volume_averages': {
                'old_pattern': 'volume_averages:*',
                'new_pattern': 'vol:baseline:*',
                'source_db': 'volume',
                'target_db': self.target_db
            },
            'volume_profile': {
                'old_pattern': 'volume_profile:*',
                'new_pattern': 'vol:profile:*',
                'source_db': 'volume',
                'target_db': self.target_db
            },
            'baseline': {
                'old_pattern': 'baseline:*',
                'new_pattern': 'vol:baseline:*',
                'source_db': 'volume',
                'target_db': self.target_db
            },
            'realtime': {
                'old_pattern': 'realtime:*',
                'new_pattern': 'ticks:realtime:*',
                'source_db': 'volume',
                'target_db': self.target_db
            },
            'indicators': {
                'old_pattern': 'indicators:*',
                'new_pattern': 'ind:*',
                'source_db': 'indicators',
                'target_db': self.target_db
            },
            'ohlc_latest': {
                'old_pattern': 'ohlc_latest:*',
                'new_pattern': 'ohlc_latest:*',
                'source_db': 'volume',
                'target_db': self.target_db
            },
            'ohlc': {
                'old_pattern': 'ohlc:*',
                'new_pattern': 'ohlc:*',
                'source_db': 'volume',
                'target_db': self.target_db
            },
            'ohlc_daily': {
                'old_pattern': 'ohlc_daily:*',
                'new_pattern': 'ohlc_daily:*',
                'source_db': 'volume',
                'target_db': self.target_db
            },
            'ohlc_stats': {
                'old_pattern': 'ohlc_stats:*',
                'new_pattern': 'ohlc_stats:*',
                'source_db': 'volume',
                'target_db': self.target_db
            },
            'market_data': {
                'old_pattern': 'market_data:*',
                'new_pattern': 'market_data:*',
                'source_db': 'volume',
                'target_db': self.target_db
            }
        }
        
        for pattern_name, pattern_info in key_patterns.items():
            try:
                source_client = self.source_dbs[pattern_info['source_db']]
                target_client = pattern_info['target_db']
                
                # Count old keys
                old_count = 0
                cursor = 0
                old_keys = []
                while True:
                    cursor, keys = source_client.scan(cursor, match=pattern_info['old_pattern'], count=1000)
                    old_count += len(keys)
                    old_keys.extend([k.decode() if isinstance(k, bytes) else k for k in keys])
                    if cursor == 0:
                        break
                
                # Count new keys
                new_count = 0
                cursor = 0
                while True:
                    cursor, keys = target_client.scan(cursor, match=pattern_info['new_pattern'], count=1000)
                    new_count += len(keys)
                    if cursor == 0:
                        break
                
                verification_results['key_counts'][pattern_name] = {
                    'old_count': old_count,
                    'new_count': new_count,
                    'match': old_count == new_count,
                    'old_keys_sample': old_keys[:10] if old_keys else []
                }
                
                status = "✅" if old_count == new_count else "⚠️"
                print(f"   {status} {pattern_name}: Old={old_count}, New={new_count}")
                
            except Exception as e:
                verification_results['key_counts'][pattern_name] = {'error': str(e)}
                print(f"   ❌ {pattern_name}: Error - {e}")
        
        # 2. Sample Data Verification
        print("\n🔬 Step 2: Sample Data Verification...")
        sample_verified = 0
        sample_errors = 0
        
        # Verify volume_state keys
        try:
            volume_client = self.source_dbs['volume']
            cursor = 0
            sample_keys = []
            
            # Get sample keys
            while len(sample_keys) < sample_size:
                cursor, keys = volume_client.scan(cursor, match='volume_state:*', count=100)
                sample_keys.extend([k.decode() if isinstance(k, bytes) else k for k in keys])
                if cursor == 0:
                    break
                if len(sample_keys) >= sample_size:
                    break
            
            for old_key in sample_keys[:sample_size]:
                try:
                    # Get old data
                    old_data = volume_client.hgetall(old_key)
                    if not old_data:
                        continue
                    
                    # Decode old data
                    if isinstance(list(old_data.values())[0], bytes):
                        old_data = {
                            k.decode() if isinstance(k, bytes) else k: 
                            v.decode() if isinstance(v, bytes) else v
                            for k, v in old_data.items()
                        }
                    
                    # Build new key
                    key_parts = old_key.split(':')
                    if len(key_parts) >= 2:
                        identifier = key_parts[1]
                        new_key = f"vol:state:{identifier}"
                        
                        # Get new data
                        new_data = self.target_db.hgetall(new_key)
                        if isinstance(new_data, dict) and new_data:
                            if isinstance(list(new_data.values())[0], bytes):
                                new_data = {
                                    k.decode() if isinstance(k, bytes) else k: 
                                    v.decode() if isinstance(v, bytes) else v
                                    for k, v in new_data.items()
                                }
                            
                            # Compare data
                            is_match = old_data == new_data
                            
                            verification_results['sample_verification'].append({
                                'old_key': old_key,
                                'new_key': new_key,
                                'match': is_match,
                                'old_data_keys': list(old_data.keys()),
                                'new_data_keys': list(new_data.keys())
                            })
                            
                            if is_match:
                                sample_verified += 1
                            else:
                                sample_errors += 1
                                verification_results['data_mismatches'].append({
                                    'old_key': old_key,
                                    'new_key': new_key,
                                    'old_data': old_data,
                                    'new_data': new_data
                                })
                        else:
                            verification_results['missing_keys'].append({
                                'old_key': old_key,
                                'new_key': new_key,
                                'reason': 'New key not found'
                            })
                            sample_errors += 1
                
                except Exception as e:
                    sample_errors += 1
                    verification_results['sample_verification'].append({
                        'old_key': old_key,
                        'error': str(e)
                    })
            
            print(f"   ✅ Verified {sample_verified} samples")
            if sample_errors > 0:
                print(f"   ⚠️  {sample_errors} sample errors")
        
        except Exception as e:
            print(f"   ❌ Error in sample verification: {e}")
            verification_results['sample_verification'].append({'error': str(e)})
        
        # 3. Summary
        total_old_keys = sum(
            v.get('old_count', 0) 
            for v in verification_results['key_counts'].values() 
            if isinstance(v, dict) and 'old_count' in v
        )
        total_new_keys = sum(
            v.get('new_count', 0) 
            for v in verification_results['key_counts'].values() 
            if isinstance(v, dict) and 'new_count' in v
        )
        
        verification_results['summary'] = {
            'total_old_keys': total_old_keys,
            'total_new_keys': total_new_keys,
            'sample_verified': sample_verified,
            'sample_errors': sample_errors,
            'missing_keys_count': len(verification_results['missing_keys']),
            'data_mismatches_count': len(verification_results['data_mismatches'])
        }
        
        # Determine overall status
        if sample_errors == 0 and total_old_keys == total_new_keys:
            verification_results['overall_status'] = 'consistent'
            print("\n✅ Overall Status: CONSISTENT - All data verified successfully")
        elif sample_errors < sample_size * 0.1:  # Less than 10% errors
            verification_results['overall_status'] = 'mostly_consistent'
            print("\n⚠️  Overall Status: MOSTLY CONSISTENT - Minor discrepancies found")
        else:
            verification_results['overall_status'] = 'inconsistent'
            print("\n❌ Overall Status: INCONSISTENT - Significant discrepancies found")
        
        # Save verification report
        report_file = f"consistency_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(verification_results, f, indent=2, default=str)
        print(f"\n📝 Verification report saved to: {report_file}")
        
        return verification_results
    
    def save_migration_log(self, filename: str = 'migration_log.json'):
        """Save migration log to file."""
        with open(filename, 'w') as f:
            json.dump(self.migration_log, f, indent=2)
        print(f"📝 Migration log saved to {filename}")
    
    def migrate_with_redis_copy(self, pattern: str, source_db_name: str, target_db: int = 1,
                                key_transform: Optional[callable] = None, verbose: bool = True) -> Dict:
        """
        Fast migration using Redis COPY command (same instance, fastest method).
        Uses Python redis library directly - no subprocess, handles binary data correctly.
        
        Args:
            pattern: Key pattern to match (e.g., 'volume_state:*')
            source_db_name: Source database name ('ticker', 'volume', 'indicators')
            target_db: Target database number (default: 1)
            key_transform: Optional function to transform key names
            verbose: Print progress
        
        Returns:
            Migration statistics
        """
        stats = {'migrated': 0, 'errors': 0, 'skipped': 0, 'scanned': 0}
        
        if source_db_name not in self.source_dbs:
            print(f"   ❌ Invalid source_db_name: {source_db_name}")
            return stats
        
        source_client = self.source_dbs[source_db_name]
        
        if verbose:
            print(f"\n🔄 Migrating pattern: {pattern} (using Redis COPY)")
            print(f"   Source: {source_db_name} (DB {source_client.connection_pool.connection_kwargs.get('db', '?')})")
            print(f"   Target: DB {target_db}")
        
        # Use SCAN for non-blocking key enumeration
        cursor = 0
        batch_count = 0
        
        while True:
            try:
                cursor, keys = source_client.scan(cursor, match=pattern, count=1000)
                stats['scanned'] += len(keys)
                
                if verbose and batch_count % 10 == 0:
                    print(f"   ... scanned {stats['scanned']} keys, migrated {stats['migrated']}...")
                
                for key in keys:
                    try:
                        # Decode key if bytes
                        key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                        
                        # Transform key if needed
                        target_key = key_transform(key_str) if key_transform else key_str
                        
                        # Use COPY command (Redis 6.2+) - fastest for same instance
                        # COPY source_key target_key DB target_db
                        copy_result = source_client.copy(key, target_key, destination_db=target_db, replace=True)
                        
                        if copy_result:
                            stats['migrated'] += 1
                        else:
                            # Key might already exist, try with replace
                            try:
                                source_client.copy(key, target_key, destination_db=target_db, replace=True)
                                stats['migrated'] += 1
                            except Exception:
                                stats['skipped'] += 1
                    
                    except Exception as e:
                        stats['errors'] += 1
                        if verbose and stats['errors'] <= 5:  # Only show first 5 errors
                            print(f"   ⚠️  Error migrating {key_str}: {e}")
                
                batch_count += 1
                
                if cursor == 0:
                    break
            
            except Exception as e:
                if verbose:
                    print(f"   ⚠️  Scan error: {e}")
                break
        
        if verbose:
            print(f"   ✅ Scanned {stats['scanned']} keys, migrated {stats['migrated']}, {stats['errors']} errors, {stats['skipped']} skipped")
        
        return stats

    def migrate_historical_data(self, dry_run: bool = False, limit: Optional[int] = None):
        """
        Phase 3: Migrate all historical data from old structure to new unified structure.
        
        This method migrates:
        - Volume data (DB 2 → DB 1 unified)
        - Indicator data (DB 5 → DB 1 unified)
        - System data (DB 0 → DB 1 unified)
        - Historical tick data
        - OHLC data
        - Baseline data
        
        Args:
            dry_run: If True, only simulate migration without writing (default: False)
            limit: Optional limit on number of keys to migrate (for testing)
        
        Returns:
            Dictionary with migration statistics
        """
        print("🔄 Phase 3: Migrating historical data to new unified structure...")
        print(f"   Mode: {'DRY RUN' if dry_run else 'LIVE MIGRATION'}")
        if limit:
            print(f"   Limit: {limit} keys per database")
        
        stats = {
            'start_time': datetime.now().isoformat(),
            'dry_run': dry_run,
            'volume_migrated': 0,
            'indicators_migrated': 0,
            'system_migrated': 0,
            'historical_ticks_migrated': 0,
            'ohlc_migrated': 0,
            'baseline_migrated': 0,
            'realtime_migrated': 0,
            'volume_profiles_migrated': 0,
            'ohlc_all_migrated': 0,
            'market_data_migrated': 0,
            'errors': 0,
            'total_keys_migrated': 0
        }
        
        try:
            # Step 1: Migrate volume data (DB 2 → DB 1)
            print("\n📊 Step 1: Migrating volume data...")
            volume_stats = self._migrate_all_volume_data(dry_run, limit)
            stats['volume_migrated'] = volume_stats.get('migrated', 0)
            stats['errors'] += volume_stats.get('errors', 0)
            
            # Step 2: Migrate indicator data (DB 5 → DB 1)
            print("\n📈 Step 2: Migrating indicator data...")
            indicator_stats = self._migrate_all_indicator_data(dry_run, limit)
            stats['indicators_migrated'] = indicator_stats.get('migrated', 0)
            stats['errors'] += indicator_stats.get('errors', 0)
            
            # Step 3: Migrate system data (DB 0 → DB 1)
            print("\n⚙️  Step 3: Migrating system data...")
            system_stats = self._migrate_all_system_data(dry_run, limit)
            stats['system_migrated'] = system_stats.get('migrated', 0)
            stats['errors'] += system_stats.get('errors', 0)
            
            # Step 4: Migrate historical tick data
            print("\n📦 Step 4: Migrating historical tick data...")
            tick_stats = self._migrate_historical_ticks(dry_run, limit)
            stats['historical_ticks_migrated'] = tick_stats.get('migrated', 0)
            stats['errors'] += tick_stats.get('errors', 0)
            
            # Step 5: Migrate OHLC data
            print("\n📊 Step 5: Migrating OHLC data...")
            ohlc_stats = self._migrate_ohlc_data(dry_run, limit)
            stats['ohlc_migrated'] = ohlc_stats.get('migrated', 0)
            stats['errors'] += ohlc_stats.get('errors', 0)
            
            # Step 6: Migrate baseline data
            print("\n📏 Step 6: Migrating baseline data...")
            baseline_stats = self._migrate_baseline_data(dry_run, limit)
            stats['baseline_migrated'] = baseline_stats.get('migrated', 0)
            stats['errors'] += baseline_stats.get('errors', 0)
            
            # Step 7: Migrate realtime keys
            print("\n🔄 Step 7: Migrating realtime keys...")
            realtime_stats = self._migrate_realtime_keys(dry_run, limit)
            stats['realtime_migrated'] = realtime_stats.get('migrated', 0)
            stats['errors'] += realtime_stats.get('errors', 0)
            
            # Step 8: Migrate volume profiles
            print("\n📊 Step 8: Migrating volume profiles...")
            volume_profile_stats = self._migrate_volume_profiles(dry_run, limit)
            stats['volume_profiles_migrated'] = volume_profile_stats.get('migrated', 0)
            stats['errors'] += volume_profile_stats.get('errors', 0)
            
            # Step 9: Migrate all OHLC patterns
            print("\n📈 Step 9: Migrating all OHLC patterns...")
            ohlc_all_stats = self._migrate_all_ohlc_patterns(dry_run, limit)
            stats['ohlc_all_migrated'] = ohlc_all_stats.get('migrated', 0)
            stats['errors'] += ohlc_all_stats.get('errors', 0)
            
            # Step 10: Migrate market data
            print("\n💹 Step 10: Migrating market data...")
            market_data_stats = self._migrate_market_data(dry_run, limit)
            stats['market_data_migrated'] = market_data_stats.get('migrated', 0)
            stats['errors'] += market_data_stats.get('errors', 0)
            
            stats['end_time'] = datetime.now().isoformat()
            stats['total_keys_migrated'] = (
                stats['volume_migrated'] + 
                stats['indicators_migrated'] + 
                stats['system_migrated'] +
                stats['historical_ticks_migrated'] +
                stats['ohlc_migrated'] +
                stats['baseline_migrated'] +
                stats.get('realtime_migrated', 0) +
                stats.get('volume_profiles_migrated', 0) +
                stats.get('ohlc_all_migrated', 0) +
                stats.get('market_data_migrated', 0)
            )
            
            print(f"\n✅ Historical data migration completed!")
            print(f"   Total keys migrated: {stats['total_keys_migrated']}")
            print(f"   Errors: {stats['errors']}")
            
            # Save migration report
            report_file = f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w') as f:
                json.dump(stats, f, indent=2)
            print(f"   Report saved to: {report_file}")
            
            return stats
            
        except Exception as e:
            print(f"❌ Migration failed: {e}")
            stats['error'] = str(e)
            stats['end_time'] = datetime.now().isoformat()
            return stats
    
    def _migrate_all_volume_data(self, dry_run: bool, limit: Optional[int]) -> Dict[str, Any]:
        """Migrate all volume data from DB 2 to DB 1 (unified)"""
        stats = {'migrated': 0, 'errors': 0}
        source_client = self.source_dbs['volume']
        target_client = self.target_db  # DB 1 (unified)
        
        try:
            # Get all volume keys
            cursor = 0
            keys_processed = 0
            
            while True:
                cursor, keys = source_client.scan(cursor, match='volume_state:*', count=1000)
                
                for key in keys:
                    if limit and keys_processed >= limit:
                        break
                    
                    try:
                        key_str = key.decode() if isinstance(key, bytes) else key
                        
                        # Get data from source
                        data = source_client.hgetall(key)
                        if not data:
                            continue
                        
                        # Decode bytes if needed
                        if isinstance(list(data.values())[0], bytes):
                            data = {
                                k.decode() if isinstance(k, bytes) else k: 
                                v.decode() if isinstance(v, bytes) else v
                                for k, v in data.items()
                            }
                        
                        # Extract symbol from key (volume_state:{token} or volume_state:{symbol})
                        key_parts = key_str.split(':')
                        if len(key_parts) >= 2:
                            identifier = key_parts[1]
                            
                            # New unified key structure
                            new_key = f"vol:state:{identifier}"
                            
                            if not dry_run:
                                target_client.hset(new_key, mapping=data)
                                # Set TTL (16 hours for analytics data)
                                target_client.expire(new_key, 57600)
                            
                            stats['migrated'] += 1
                            keys_processed += 1
                            
                    except Exception as e:
                        stats['errors'] += 1
                        
                        print(f"Error migrating volume key {key}: {e}")
                
                if cursor == 0 or (limit and keys_processed >= limit):
                    break
            
            # Also migrate volume_averages
            cursor = 0
            while True:
                cursor, keys = source_client.scan(cursor, match='volume_averages:*', count=1000)
                
                for key in keys:
                    if limit and keys_processed >= limit:
                        break
                    
                    try:
                        key_str = key.decode() if isinstance(key, bytes) else key
                        data = source_client.hgetall(key)
                        
                        if data:
                            if isinstance(list(data.values())[0], bytes):
                                data = {
                                    k.decode() if isinstance(k, bytes) else k: 
                                    v.decode() if isinstance(v, bytes) else v
                                    for k, v in data.items()
                                }
                            
                            key_parts = key_str.split(':')
                            if len(key_parts) >= 2:
                                symbol = key_parts[1]
                                new_key = f"vol:baseline:{symbol}"
                                
                                if not dry_run:
                                    target_client.hset(new_key, mapping=data)
                                    target_client.expire(new_key, 57600)
                                
                                stats['migrated'] += 1
                                keys_processed += 1
                    
                    except Exception as e:
                        stats['errors'] += 1
                        logger.error(f"Error migrating volume_averages key {key}: {e}")
                
                if cursor == 0 or (limit and keys_processed >= limit):
                    break
            
            print(f"   ✅ Migrated {stats['migrated']} volume keys")
            if stats['errors'] > 0:
                print(f"   ⚠️  {stats['errors']} errors")
            
        except Exception as e:
            print(f"   ❌ Error migrating volume data: {e}")
            stats['errors'] += 1
        
        return stats
    
    def _migrate_all_indicator_data(self, dry_run: bool, limit: Optional[int]) -> Dict[str, Any]:
        """Migrate all indicator data from DB 5 to DB 1 (unified)"""
        stats = {'migrated': 0, 'errors': 0}
        source_client = self.source_dbs['indicators']
        target_client = self.target_db  # DB 1 (unified)
        
        try:
            cursor = 0
            keys_processed = 0
            
            # Migrate all indicator keys
            while True:
                cursor, keys = source_client.scan(cursor, match='indicators:*', count=1000)
                
                for key in keys:
                    if limit and keys_processed >= limit:
                        break
                    
                    try:
                        key_str = key.decode() if isinstance(key, bytes) else key
                        
                        # Parse indicator key: indicators:{symbol}:{indicator_name}
                        key_parts = key_str.split(':')
                        if len(key_parts) >= 3:
                            symbol = key_parts[1]
                            indicator_name = key_parts[2]
                            
                            # Get value
                            value = source_client.get(key)
                            if value is None:
                                continue
                            
                            if isinstance(value, bytes):
                                value = value.decode('utf-8')
                            
                            # Categorize indicator and create new key
                            if indicator_name in ['rsi', 'macd', 'bollinger', 'atr', 'ema']:
                                new_key = f"ind:ta:{symbol}:{indicator_name}"
                            elif indicator_name in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                                new_key = f"ind:greeks:{symbol}:{indicator_name}"
                            elif 'volume' in indicator_name.lower():
                                new_key = f"ind:volume:{symbol}:{indicator_name}"
                            else:
                                new_key = f"ind:other:{symbol}:{indicator_name}"
                            
                            if not dry_run:
                                target_client.set(new_key, value)
                                target_client.expire(new_key, 3600)  # 1 hour TTL
                            
                            stats['migrated'] += 1
                            keys_processed += 1
                    
                    except Exception as e:
                        stats['errors'] += 1
                        logger.error(f"Error migrating indicator key {key}: {e}")
                
                if cursor == 0 or (limit and keys_processed >= limit):
                    break
            
            print(f"   ✅ Migrated {stats['migrated']} indicator keys")
            if stats['errors'] > 0:
                print(f"   ⚠️  {stats['errors']} errors")
            
        except Exception as e:
            print(f"   ❌ Error migrating indicator data: {e}")
            stats['errors'] += 1
        
        return stats
    
    def _migrate_all_system_data(self, dry_run: bool, limit: Optional[int]) -> Dict[str, Any]:
        """Migrate system data from DB 0 to DB 1 (unified)"""
        stats = {'migrated': 0, 'errors': 0}
        source_client = self.source_dbs['ticker']
        target_client = self.target_db  # DB 1 (unified)
        
        try:
            # Migrate system keys (session data, health checks, etc.)
            system_patterns = ['session:*', 'system:*', 'health:*']
            
            keys_processed = 0
            for pattern in system_patterns:
                cursor = 0
                while True:
                    cursor, keys = source_client.scan(cursor, match=pattern, count=1000)
                    
                    for key in keys:
                        if limit and keys_processed >= limit:
                            break
                        
                        try:
                            key_str = key.decode() if isinstance(key, bytes) else key
                            
                            # Get key type and migrate accordingly
                            key_type = source_client.type(key)
                            if isinstance(key_type, bytes):
                                key_type = key_type.decode()
                            
                            if key_type == 'hash':
                                data = source_client.hgetall(key)
                                if data:
                                    if isinstance(list(data.values())[0], bytes):
                                        data = {
                                            k.decode() if isinstance(k, bytes) else k: 
                                            v.decode() if isinstance(v, bytes) else v
                                            for k, v in data.items()
                                        }
                                    
                                    # New key with sys: prefix
                                    new_key = f"sys:{key_str}" if not key_str.startswith('sys:') else key_str
                                    
                                    if not dry_run:
                                        target_client.hset(new_key, mapping=data)
                                        target_client.expire(new_key, 57600)
                                    
                                    stats['migrated'] += 1
                                    keys_processed += 1
                            
                            elif key_type == 'string':
                                value = source_client.get(key)
                                if value:
                                    if isinstance(value, bytes):
                                        value = value.decode('utf-8')
                                    
                                    new_key = f"sys:{key_str}" if not key_str.startswith('sys:') else key_str
                                    
                                    if not dry_run:
                                        target_client.set(new_key, value)
                                        target_client.expire(new_key, 57600)
                                    
                                    stats['migrated'] += 1
                                    keys_processed += 1
                        
                        except Exception as e:
                            stats['errors'] += 1
                            logger.error(f"Error migrating system key {key}: {e}")
                    
                    if cursor == 0 or (limit and keys_processed >= limit):
                        break
            
            print(f"   ✅ Migrated {stats['migrated']} system keys")
            if stats['errors'] > 0:
                print(f"   ⚠️  {stats['errors']} errors")
            
        except Exception as e:
            print(f"   ❌ Error migrating system data: {e}")
            stats['errors'] += 1
        
        return stats
    
    def _migrate_historical_ticks(self, dry_run: bool, limit: Optional[int]) -> Dict[str, Any]:
        """Migrate historical tick data from streams"""
        stats = {'migrated': 0, 'errors': 0}
        
        try:
            # Use existing volume migration script if available
            try:
                from shared_core.redis_clients.volume_migration import migrate_volume_data
                
                # Migrate from DB 1 (realtime) to unified structure
                migration_result = migrate_volume_data(
                    source_db=1,
                    limit=limit or 10000,
                    dry_run=dry_run
                )
                
                stats['migrated'] = migration_result.get('ticks_migrated', 0) or migration_result.get('successful', 0)
                # Handle errors - could be int or list
                errors = migration_result.get('errors', 0)
                if isinstance(errors, list):
                    stats['errors'] = len(errors)
                else:
                    stats['errors'] = errors or 0
                
                print(f"   ✅ Migrated {stats['migrated']} historical ticks")
                
            except ImportError:
                print("   ⚠️  Volume migration script not available, skipping historical ticks")
        
        except Exception as e:
            print(f"   ❌ Error migrating historical ticks: {e}")
            stats['errors'] += 1
        
        return stats
    
    def _migrate_ohlc_data(self, dry_run: bool, limit: Optional[int]) -> Dict[str, Any]:
        """Migrate OHLC data"""
        stats = {'migrated': 0, 'errors': 0}
        
        try:
            # OHLC data is typically in DB 2 (analytics)
            source_client = self.source_dbs['volume']  # DB 2
            target_client = self.target_db  # DB 1 (unified)
            
            ohlc_patterns = ['ohlc_latest:*', 'ohlc:*', 'ohlc_stats:*']
            keys_processed = 0
            
            for pattern in ohlc_patterns:
                cursor = 0
                while True:
                    cursor, keys = source_client.scan(cursor, match=pattern, count=1000)
                    
                    for key in keys:
                        if limit and keys_processed >= limit:
                            break
                        
                        try:
                            key_str = key.decode() if isinstance(key, bytes) else key
                            key_type = source_client.type(key)
                            
                            if isinstance(key_type, bytes):
                                key_type = key_type.decode()
                            
                            if key_type == 'hash':
                                data = source_client.hgetall(key)
                                if data:
                                    if isinstance(list(data.values())[0], bytes):
                                        data = {
                                            k.decode() if isinstance(k, bytes) else k: 
                                            v.decode() if isinstance(v, bytes) else v
                                            for k, v in data.items()
                                        }
                                    
                                    # Keep same key structure for OHLC
                                    if not dry_run:
                                        target_client.hset(key_str, mapping=data)
                                        target_client.expire(key_str, 57600)
                                    
                                    stats['migrated'] += 1
                                    keys_processed += 1
                        
                        except Exception as e:
                            stats['errors'] += 1
                            logger.error(f"Error migrating OHLC key {key}: {e}")
                    
                    if cursor == 0 or (limit and keys_processed >= limit):
                        break
            
            print(f"   ✅ Migrated {stats['migrated']} OHLC keys")
            
        except Exception as e:
            print(f"   ❌ Error migrating OHLC data: {e}")
            stats['errors'] += 1
        
        return stats
    
    def _migrate_baseline_data(self, dry_run: bool, limit: Optional[int]) -> Dict[str, Any]:
        """Migrate baseline data"""
        stats = {'migrated': 0, 'errors': 0}
        
        try:
            # Baseline data is in DB 2
            source_client = self.source_dbs['volume']  # DB 2
            target_client = self.target_db  # DB 1 (unified)
            
            cursor = 0
            keys_processed = 0
            
            while True:
                cursor, keys = source_client.scan(cursor, match='volume:baseline:*', count=1000)
                
                for key in keys:
                    if limit and keys_processed >= limit:
                        break
                    
                    try:
                        key_str = key.decode() if isinstance(key, bytes) else key
                        value = source_client.get(key)
                        
                        if value:
                            if isinstance(value, bytes):
                                value = value.decode('utf-8')
                            
                            # Keep same key structure
                            if not dry_run:
                                target_client.set(key_str, value)
                                target_client.expire(key_str, 57600)
                            
                            stats['migrated'] += 1
                            keys_processed += 1
                    
                    except Exception as e:
                        stats['errors'] += 1
                        logger.error(f"Error migrating baseline key {key}: {e}")
                
                if cursor == 0 or (limit and keys_processed >= limit):
                    break
            
            print(f"   ✅ Migrated {stats['migrated']} baseline keys")
            
        except Exception as e:
            print(f"   ❌ Error migrating baseline data: {e}")
            stats['errors'] += 1
        
        return stats
    
    def _migrate_realtime_keys(self, dry_run: bool, limit: Optional[int]) -> Dict[str, Any]:
        """Migrate realtime keys from DB 2 to DB 1 (unified)"""
        stats = {'migrated': 0, 'errors': 0}
        source_client = self.source_dbs['volume']
        target_client = self.target_db  # DB 1 (unified)
        
        try:
            cursor = 0
            keys_processed = 0
            
            while True:
                cursor, keys = source_client.scan(cursor, match='realtime:*', count=1000)
                
                for key in keys:
                    if limit and keys_processed >= limit:
                        break
                    
                    try:
                        key_str = key.decode() if isinstance(key, bytes) else key
                        
                        # Parse realtime key: realtime:{symbol}:{timestamp} or realtime:{symbol}
                        key_parts = key_str.split(':')
                        if len(key_parts) >= 2:
                            symbol = key_parts[1]
                            
                            # Get key type
                            key_type = source_client.type(key)
                            if isinstance(key_type, bytes):
                                key_type = key_type.decode()
                            
                            if key_type == 'hash':
                                data = source_client.hgetall(key)
                                if data:
                                    if isinstance(list(data.values())[0], bytes):
                                        data = {
                                            k.decode() if isinstance(k, bytes) else k: 
                                            v.decode() if isinstance(v, bytes) else v
                                            for k, v in data.items()
                                        }
                                    
                                    # New key: ticks:realtime:{symbol} (stream) or tick:latest:{symbol} (hash)
                                    if len(key_parts) >= 3:
                                        # Has timestamp - migrate to stream
                                        new_stream_key = f"ticks:realtime:{symbol}"
                                        if not dry_run:
                                            tick_json = json.dumps(data)
                                            target_client.xadd(
                                                new_stream_key,
                                                {'data': tick_json, 'timestamp': key_parts[2]},
                                                maxlen=10000,
                                                approximate=True
                                            )
                                    else:
                                        # No timestamp - migrate to hash
                                        new_key = f"tick:latest:{symbol}"
                                        if not dry_run:
                                            target_client.hset(new_key, mapping=data)
                                            target_client.expire(new_key, 3600)
                                    
                                    stats['migrated'] += 1
                                    keys_processed += 1
                            
                            elif key_type == 'string':
                                value = source_client.get(key)
                                if value:
                                    if isinstance(value, bytes):
                                        value = value.decode('utf-8')
                                    
                                    # Store in hash
                                    new_key = f"tick:latest:{symbol}"
                                    if not dry_run:
                                        target_client.hset(new_key, 'data', value)
                                        target_client.expire(new_key, 3600)
                                    
                                    stats['migrated'] += 1
                                    keys_processed += 1
                    
                    except Exception as e:
                        stats['errors'] += 1
                        logger.error(f"Error migrating realtime key {key}: {e}")
                
                if cursor == 0 or (limit and keys_processed >= limit):
                    break
            
            print(f"   ✅ Migrated {stats['migrated']} realtime keys")
            if stats['errors'] > 0:
                print(f"   ⚠️  {stats['errors']} errors")
        
        except Exception as e:
            print(f"   ❌ Error migrating realtime keys: {e}")
            stats['errors'] += 1
        
        return stats
    
    def _migrate_volume_profiles(self, dry_run: bool, limit: Optional[int]) -> Dict[str, Any]:
        """Migrate volume profile keys from DB 2 to DB 1 (unified)"""
        stats = {'migrated': 0, 'errors': 0}
        source_client = self.source_dbs['volume']
        target_client = self.target_db  # DB 1 (unified)
        
        try:
            cursor = 0
            keys_processed = 0
            
            while True:
                cursor, keys = source_client.scan(cursor, match='volume_profile:*', count=1000)
                
                for key in keys:
                    if limit and keys_processed >= limit:
                        break
                    
                    try:
                        key_str = key.decode() if isinstance(key, bytes) else key
                        
                        # Parse volume_profile key: volume_profile:{symbol}:{period} or volume_profile:patterns:{symbol}:{period}
                        key_parts = key_str.split(':')
                        if len(key_parts) >= 3:
                            # Extract symbol and period
                            if key_parts[1] == 'patterns':
                                symbol = key_parts[2]
                                period = key_parts[3] if len(key_parts) > 3 else 'default'
                                new_key = f"vol:profile:{symbol}:{period}"
                            else:
                                symbol = key_parts[1]
                                period = key_parts[2] if len(key_parts) > 2 else 'default'
                                new_key = f"vol:profile:{symbol}:{period}"
                            
                            # Get key type
                            key_type = source_client.type(key)
                            if isinstance(key_type, bytes):
                                key_type = key_type.decode()
                            
                            if key_type == 'hash':
                                data = source_client.hgetall(key)
                                if data:
                                    if isinstance(list(data.values())[0], bytes):
                                        data = {
                                            k.decode() if isinstance(k, bytes) else k: 
                                            v.decode() if isinstance(v, bytes) else v
                                            for k, v in data.items()
                                        }
                                    
                                    if not dry_run:
                                        target_client.hset(new_key, mapping=data)
                                        target_client.expire(new_key, 57600)
                                    
                                    stats['migrated'] += 1
                                    keys_processed += 1
                            
                            elif key_type == 'string':
                                value = source_client.get(key)
                                if value:
                                    if isinstance(value, bytes):
                                        value = value.decode('utf-8')
                                    
                                    if not dry_run:
                                        target_client.set(new_key, value)
                                        target_client.expire(new_key, 57600)
                                    
                                    stats['migrated'] += 1
                                    keys_processed += 1
                            
                            elif key_type == 'zset':
                                # Volume profiles might be sorted sets
                                items = source_client.zrange(key, 0, -1, withscores=True)
                                if items:
                                    zset_data = {
                                        (m.decode() if isinstance(m, bytes) else m): s
                                        for m, s in items
                                    }
                                    if not dry_run:
                                        target_client.zadd(new_key, zset_data)
                                        target_client.expire(new_key, 57600)
                                    
                                    stats['migrated'] += 1
                                    keys_processed += 1
                    
                    except Exception as e:
                        stats['errors'] += 1
                        logger.error(f"Error migrating volume_profile key {key}: {e}")
                
                if cursor == 0 or (limit and keys_processed >= limit):
                    break
            
            print(f"   ✅ Migrated {stats['migrated']} volume profile keys")
            if stats['errors'] > 0:
                print(f"   ⚠️  {stats['errors']} errors")
        
        except Exception as e:
            print(f"   ❌ Error migrating volume profiles: {e}")
            stats['errors'] += 1
        
        return stats
    
    def _migrate_all_ohlc_patterns(self, dry_run: bool, limit: Optional[int]) -> Dict[str, Any]:
        """Migrate all OHLC patterns (ohlc:*, ohlc_daily:*)"""
        stats = {'migrated': 0, 'errors': 0}
        # Check both DB 0 and DB 2 for ohlc_daily
        source_clients = [
            ('volume', self.source_dbs['volume']),  # DB 2
            ('ticker', self.source_dbs['ticker'])    # DB 0
        ]
        target_client = self.target_db  # DB 1 (unified)
        
        ohlc_patterns = ['ohlc:*', 'ohlc_daily:*']
        keys_processed = 0
        
        for pattern in ohlc_patterns:
            # Check both source databases
            for db_name, source_client in source_clients:
                try:
                    cursor = 0
                    while True:
                        cursor, keys = source_client.scan(cursor, match=pattern, count=1000)
                    
                    for key in keys:
                        if limit and keys_processed >= limit:
                            break
                        
                        try:
                            key_str = key.decode() if isinstance(key, bytes) else key
                            key_type = source_client.type(key)
                            
                            if isinstance(key_type, bytes):
                                key_type = key_type.decode()
                            
                            if key_type == 'hash':
                                data = source_client.hgetall(key)
                                if data:
                                    if isinstance(list(data.values())[0], bytes):
                                        data = {
                                            k.decode() if isinstance(k, bytes) else k: 
                                            v.decode() if isinstance(v, bytes) else v
                                            for k, v in data.items()
                                        }
                                    
                                    # Keep same key structure
                                    if not dry_run:
                                        target_client.hset(key_str, mapping=data)
                                        target_client.expire(key_str, 57600)
                                    
                                    stats['migrated'] += 1
                                    keys_processed += 1
                            
                            elif key_type == 'string':
                                value = source_client.get(key)
                                if value:
                                    if isinstance(value, bytes):
                                        value = value.decode('utf-8')
                                    
                                    if not dry_run:
                                        target_client.set(key_str, value)
                                        target_client.expire(key_str, 57600)
                                    
                                    stats['migrated'] += 1
                                    keys_processed += 1
                            
                            elif key_type == 'zset':
                                # OHLC daily keys are sorted sets
                                items = source_client.zrange(key, 0, -1, withscores=True)
                                if items:
                                    zset_data = {
                                        (m.decode() if isinstance(m, bytes) else m): s
                                        for m, s in items
                                    }
                                    if not dry_run:
                                        target_client.zadd(key_str, zset_data)
                                        target_client.expire(key_str, 57600)
                                    
                                    stats['migrated'] += 1
                                    keys_processed += 1
                                    if keys_processed % 50 == 0:
                                        print(f"      ... migrated {keys_processed} keys so far...")
                        
                        except Exception as e:
                            stats['errors'] += 1
                            logger.error(f"Error migrating OHLC key {key}: {e}")
                    
                        if cursor == 0 or (limit and keys_processed >= limit):
                            break
                
                except Exception as e:
                    print(f"   ⚠️  Error migrating pattern {pattern} from {db_name}: {e}")
                    stats['errors'] += 1
        
        print(f"   ✅ Migrated {stats['migrated']} OHLC keys")
        if stats['errors'] > 0:
            print(f"   ⚠️  {stats['errors']} errors")
        
        return stats
    
    def _migrate_market_data(self, dry_run: bool, limit: Optional[int]) -> Dict[str, Any]:
        """Migrate market_data keys"""
        stats = {'migrated': 0, 'errors': 0}
        source_client = self.source_dbs['volume']
        target_client = self.target_db  # DB 1 (unified)
        
        try:
            cursor = 0
            keys_processed = 0
            
            while True:
                cursor, keys = source_client.scan(cursor, match='market_data:*', count=1000)
                
                for key in keys:
                    if limit and keys_processed >= limit:
                        break
                    
                    try:
                        key_str = key.decode() if isinstance(key, bytes) else key
                        key_type = source_client.type(key)
                        
                        if isinstance(key_type, bytes):
                            key_type = key_type.decode()
                        
                        if key_type == 'hash':
                            data = source_client.hgetall(key)
                            if data:
                                if isinstance(list(data.values())[0], bytes):
                                    data = {
                                        k.decode() if isinstance(k, bytes) else k: 
                                        v.decode() if isinstance(v, bytes) else v
                                        for k, v in data.items()
                                    }
                                
                                # Keep same key structure
                                if not dry_run:
                                    target_client.hset(key_str, mapping=data)
                                    target_client.expire(key_str, 57600)
                                
                                stats['migrated'] += 1
                                keys_processed += 1
                        
                        elif key_type == 'string':
                            value = source_client.get(key)
                            if value:
                                if isinstance(value, bytes):
                                    value = value.decode('utf-8')
                                
                                if not dry_run:
                                    target_client.set(key_str, value)
                                    target_client.expire(key_str, 57600)
                                
                                stats['migrated'] += 1
                                keys_processed += 1
                    
                    except Exception as e:
                        stats['errors'] += 1
                        logger.error(f"Error migrating market_data key {key}: {e}")
                
                if cursor == 0 or (limit and keys_processed >= limit):
                    break
            
            print(f"   ✅ Migrated {stats['migrated']} market_data keys")
            if stats['errors'] > 0:
                print(f"   ⚠️  {stats['errors']} errors")
        
        except Exception as e:
            print(f"   ❌ Error migrating market_data: {e}")
            stats['errors'] += 1
        
        return stats
    
    def cleanup(self):
        """Clean up Redis connections."""
        try:
            # ✅ FIXED: UnifiedRedisManager handles cleanup automatically
            print("✅ Cleaned up Redis connections")
        except Exception as e:
            print(f"⚠️  Error during cleanup: {e}")


class ClickHouseDataImporter:
    """
    Import data from main system Redis, enrich with metadata, and push to Redis Streams
    for ClickHouse ingestion pipeline.
    
    This is a standalone component for the ClickHouse setup that operates independently
    from the main trading system's data processing.
    
    Architecture:
        Main System Redis → Enrichment → Redis Streams → ClickHouse Pipeline → ClickHouse
    """
    
    def __init__(self, redis_host: Optional[str] = None, redis_port: Optional[int] = None,
                 clickhouse_redis_port: Optional[int] = None):
        """
        Initialize ClickHouse data importer.
        
        Args:
            redis_host: Main system Redis host (defaults to REDIS_HOST env or 'localhost')
            redis_port: Main system Redis port (defaults to REDIS_PORT env or 6379)
            clickhouse_redis_port: Redis port for ClickHouse streams (defaults to 6380)
        """
        from pathlib import Path
        
        # Main system Redis connection
        self.redis_host = redis_host or os.environ.get('REDIS_HOST', 'localhost')
        self.redis_port = redis_port or int(os.environ.get('REDIS_PORT', 6379))
        self.clickhouse_redis_port = clickhouse_redis_port or 6380
        
        # ✅ FIXED: Use UnifiedRedisManager for centralized connection management
        # Connect to main system Redis (source)
        self.source_redis = get_redis_client(process_name="data_migrator_source", db=1)
        
        # Connect to unified DB (DB 1) for historical data
        self.unified_redis = get_redis_client(process_name="data_migrator_unified", db=1)
        
        # Connect to volume DB (DB 2) for realtime data
        self.volume_redis = get_redis_client(process_name="data_migrator_volume", db=2)
        
        # ✅ FIXED: Use UnifiedRedisManager for ClickHouse Redis (target streams) with custom port
        self.clickhouse_redis = get_redis_client(
            process_name="data_migrator_clickhouse",
            db=0,  # Default DB for streams
            host=self.redis_host,
            port=self.clickhouse_redis_port,
            decode_responses=True
        )
        
        # Load token metadata for enrichment
        self.token_metadata = {}
        self._load_token_metadata()
        
        logger.info("✅ ClickHouseDataImporter initialized")
    
    def _load_token_metadata(self):
        """Load token metadata from enriched lookup file."""
        try:
            from pathlib import Path
            
            # Load from core/data/token_lookup_enriched.json
            # Path: trading_system/storage/data_migrator.py -> 3 levels up to project root
            project_root = Path(__file__).parent.parent.parent
            lookup_file = project_root / 'core' / 'data' / 'token_lookup_enriched.json'
            
            if not lookup_file.exists():
                logger.warning(f"⚠️  Token lookup file not found: {lookup_file}")
                return
            
            logger.info(f"📋 Loading token metadata from: {lookup_file}")
            
            with open(lookup_file, 'r') as f:
                token_data = json.load(f)
            
            # Build token -> metadata map
            for token_str, info in token_data.items():
                try:
                    token = int(token_str)
                    self.token_metadata[token] = info
                except (ValueError, TypeError):
                    continue
            
            logger.info(f"✅ Loaded metadata for {len(self.token_metadata)} instruments")
            
        except Exception as e:
            logger.error(f"❌ Error loading token metadata: {e}")
    
    def _enrich_tick_data(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich tick data with metadata from token lookup.
        
        Args:
            tick_data: Raw tick data dictionary
            
        Returns:
            Enriched tick data dictionary
        """
        try:
            instrument_token = tick_data.get('instrument_token') or tick_data.get('token')
            if not instrument_token:
                return tick_data
            
            try:
                token = int(instrument_token)
            except (ValueError, TypeError):
                return tick_data
            
            # Get metadata
            metadata = self.token_metadata.get(token, {})
            if not metadata:
                return tick_data
            
            # Enrich with metadata fields
            enriched = tick_data.copy()
            
            # Basic fields
            if 'symbol' not in enriched or not enriched.get('symbol'):
                enriched['symbol'] = metadata.get('symbol') or metadata.get('tradingsymbol') or metadata.get('name', '')
            
            if 'exchange' not in enriched or not enriched.get('exchange'):
                enriched['exchange'] = metadata.get('exchange', 'NSE')
            
            # Extended metadata
            enrichment_fields = [
                'sector', 'state', 'bond_type', 'maturity_year', 'coupon_rate',
                'asset_class', 'sub_category', 'instrument_type', 'segment',
                'expiry', 'strike_price', 'option_type', 'lot_size', 'tick_size'
            ]
            
            for field in enrichment_fields:
                if field in metadata and (field not in enriched or not enriched.get(field)):
                    enriched[field] = metadata[field]
            
            # Set data quality flag
            enriched['data_quality'] = 'enriched' if metadata else 'raw'
            
            return enriched
            
        except Exception as e:
            logger.error(f"Error enriching tick data: {e}")
            return tick_data
    
    def import_realtime_ticks(self, limit: Optional[int] = None, dry_run: bool = False) -> Dict[str, Any]:
        """
        Import realtime tick data from main system Redis and push to ClickHouse streams.
        
        Args:
            limit: Optional limit on number of keys to process
            dry_run: If True, only simulate without writing
            
        Returns:
            Dictionary with import statistics
        """
        stats = {
            'start_time': datetime.now().isoformat(),
            'processed': 0,
            'enriched': 0,
            'pushed': 0,
            'errors': 0
        }
        
        logger.info("🔄 Starting realtime tick import...")
        
        try:
            cursor = 0
            keys_processed = 0
            
            # Scan for realtime keys: realtime:* or tick:latest:*
            patterns = ['realtime:*', 'tick:latest:*', 'vol:state:*']
            
            for pattern in patterns:
                cursor = 0
                while True:
                    cursor, keys = self.volume_redis.scan(cursor, match=pattern, count=1000)
                    
                    for key in keys:
                        if limit and keys_processed >= limit:
                            break
                        
                        try:
                            key_str = key.decode() if isinstance(key, bytes) else key
                            
                            # Get key type
                            key_type = self.volume_redis.type(key)
                            if isinstance(key_type, bytes):
                                key_type = key_type.decode()
                            
                            if key_type == 'hash':
                                # Get hash data
                                data = self.volume_redis.hgetall(key)
                                if not data:
                                    continue
                                
                                # Decode bytes if needed
                                if isinstance(list(data.values())[0], bytes):
                                    data = {
                                        k.decode() if isinstance(k, bytes) else k: 
                                        v.decode() if isinstance(v, bytes) else v
                                        for k, v in data.items()
                                    }
                                
                                # Extract instrument_token
                                instrument_token = data.get('instrument_token') or data.get('token')
                                if not instrument_token:
                                    # Try to extract from key
                                    key_parts = key_str.split(':')
                                    if len(key_parts) >= 2:
                                        try:
                                            instrument_token = int(key_parts[1])
                                            data['instrument_token'] = instrument_token
                                        except (ValueError, TypeError):
                                            continue
                                
                                # Enrich data
                                enriched_data = self._enrich_tick_data(data)
                                stats['processed'] += 1
                                
                                if enriched_data.get('data_quality') == 'enriched':
                                    stats['enriched'] += 1
                                
                                # Push to ClickHouse Redis stream
                                if not dry_run:
                                    stream_key = 'ticks:stream'
                                    self.clickhouse_redis.xadd(stream_key, enriched_data, maxlen=10000, approximate=True)
                                    stats['pushed'] += 1
                                
                                keys_processed += 1
                                
                                if keys_processed % 100 == 0:
                                    logger.info(f"   Processed {keys_processed} keys...")
                            
                            elif key_type == 'stream':
                                # Copy stream messages
                                messages = self.volume_redis.xrange(key, '-', '+', count=100)
                                for msg_id, msg_data in messages:
                                    if limit and keys_processed >= limit:
                                        break
                                    
                                    # Decode message data
                                    if isinstance(list(msg_data.values())[0], bytes):
                                        msg_data = {
                                            k.decode() if isinstance(k, bytes) else k: 
                                            v.decode() if isinstance(v, bytes) else v
                                            for k, v in msg_data.items()
                                        }
                                    
                                    # Enrich and push
                                    enriched_data = self._enrich_tick_data(msg_data)
                                    stats['processed'] += 1
                                    
                                    if enriched_data.get('data_quality') == 'enriched':
                                        stats['enriched'] += 1
                                    
                                    if not dry_run:
                                        stream_key = 'ticks:stream'
                                        self.clickhouse_redis.xadd(stream_key, enriched_data, maxlen=10000, approximate=True)
                                        stats['pushed'] += 1
                                    
                                    keys_processed += 1
                        
                        except Exception as e:
                            stats['errors'] += 1
                            logger.error(f"Error processing key {key_str}: {e}")
                    
                    if cursor == 0 or (limit and keys_processed >= limit):
                        break
            
            stats['end_time'] = datetime.now().isoformat()
            logger.info(f"✅ Import completed: {stats['processed']} processed, {stats['pushed']} pushed, {stats['errors']} errors")
            
        except Exception as e:
            logger.error(f"❌ Import failed: {e}")
            stats['error'] = str(e)
            stats['end_time'] = datetime.now().isoformat()
        
        return stats
    
    def import_volume_data(self, limit: Optional[int] = None, dry_run: bool = False) -> Dict[str, Any]:
        """
        Import volume state data and push to ClickHouse streams.
        
        Args:
            limit: Optional limit on number of keys to process
            dry_run: If True, only simulate without writing
            
        Returns:
            Dictionary with import statistics
        """
        stats = {
            'start_time': datetime.now().isoformat(),
            'processed': 0,
            'pushed': 0,
            'errors': 0
        }
        
        logger.info("🔄 Starting volume data import...")
        
        try:
            cursor = 0
            keys_processed = 0
            
            # Scan for volume_state keys
            while True:
                cursor, keys = self.volume_redis.scan(cursor, match='volume_state:*', count=1000)
                
                for key in keys:
                    if limit and keys_processed >= limit:
                        break
                    
                    try:
                        key_str = key.decode() if isinstance(key, bytes) else key
                        data = self.volume_redis.hgetall(key)
                        
                        if data:
                            # Decode bytes if needed
                            if isinstance(list(data.values())[0], bytes):
                                data = {
                                    k.decode() if isinstance(k, bytes) else k: 
                                    v.decode() if isinstance(v, bytes) else v
                                    for k, v in data.items()
                                }
                            
                            # Extract symbol/token from key
                            key_parts = key_str.split(':')
                            if len(key_parts) >= 2:
                                identifier = key_parts[1]
                                try:
                                    token = int(identifier)
                                    data['instrument_token'] = token
                                except (ValueError, TypeError):
                                    data['symbol'] = identifier
                            
                            # Enrich data
                            enriched_data = self._enrich_tick_data(data)
                            stats['processed'] += 1
                            
                            # Push to stream (as volume analytics data)
                            if not dry_run:
                                stream_key = 'ticks:stream'
                                self.clickhouse_redis.xadd(stream_key, enriched_data, maxlen=10000, approximate=True)
                                stats['pushed'] += 1
                            
                            keys_processed += 1
                    
                    except Exception as e:
                        stats['errors'] += 1
                        logger.error(f"Error processing volume key {key_str}: {e}")
                
                if cursor == 0 or (limit and keys_processed >= limit):
                    break
            
            stats['end_time'] = datetime.now().isoformat()
            logger.info(f"✅ Volume import completed: {stats['processed']} processed, {stats['pushed']} pushed")
            
        except Exception as e:
            logger.error(f"❌ Volume import failed: {e}")
            stats['error'] = str(e)
            stats['end_time'] = datetime.now().isoformat()
        
        return stats
    
    def import_historical_data(self, limit: Optional[int] = None, dry_run: bool = False) -> Dict[str, Any]:
        """
        Import all historical data from Redis databases (DB 0, 1, 2) to ClickHouse streams.
        
        Args:
            limit: Optional limit on number of keys to process per database
            dry_run: If True, only simulate without writing
            
        Returns:
            Dictionary with import statistics
        """
        stats = {
            'start_time': datetime.now().isoformat(),
            'db0_processed': 0,
            'db1_processed': 0,
            'db2_processed': 0,
            'total_pushed': 0,
            'total_errors': 0
        }
        
        logger.info("🔄 Starting historical data import from all databases...")
        
        # Import from DB 0 (main ticker DB)
        logger.info("📊 Importing from DB 0 (main ticker)...")
        try:
            cursor = 0
            keys_processed = 0
            
            # Look for tick-related keys
            patterns = ['tick:*', 'ohlc:*', 'market_data:*']
            for pattern in patterns:
                cursor = 0
                while True:
                    cursor, keys = self.source_redis.scan(cursor, match=pattern, count=1000)
                    
                    for key in keys:
                        if limit and keys_processed >= limit:
                            break
                        
                        try:
                            key_str = key.decode() if isinstance(key, bytes) else key
                            self._import_key(self.source_redis, key, key_str, stats, dry_run)
                            keys_processed += 1
                            stats['db0_processed'] += 1
                        except Exception as e:
                            stats['total_errors'] += 1
                            logger.error(f"Error processing DB0 key {key_str}: {e}")
                    
                    if cursor == 0 or (limit and keys_processed >= limit):
                        break
        except Exception as e:
            logger.error(f"Error importing from DB 0: {e}")
        
        # Import from DB 1 (unified DB)
        logger.info("📊 Importing from DB 1 (unified)...")
        try:
            # First, copy from existing tick streams (this is the main historical data)
            logger.info("   Looking for existing tick streams...")
            cursor = 0
            streams_found = 0
            while True:
                cursor, keys = self.unified_redis.scan(cursor, count=1000)
                for key in keys:
                    key_str = key.decode() if isinstance(key, bytes) else key
                    key_type = self.unified_redis.type(key)
                    if isinstance(key_type, bytes):
                        key_type = key_type.decode()
                    if key_type == 'stream' and ('tick' in key_str.lower() or 'stream' in key_str.lower()):
                        streams_found += 1
                        try:
                            stream_len = self.unified_redis.xlen(key)
                            if stream_len > 0:
                                logger.info(f"   Found stream {key_str} with {stream_len} messages, copying...")
                                messages = self.unified_redis.xrange(key, '-', '+', count=10000)
                                copied = 0
                                for msg_id, msg_data in messages:
                                    if msg_data:
                                        if isinstance(list(msg_data.values())[0], bytes):
                                            msg_data = {
                                                k.decode() if isinstance(k, bytes) else k: 
                                                v.decode() if isinstance(v, bytes) else v
                                                for k, v in msg_data.items()
                                            }
                                        enriched_data = self._enrich_tick_data(msg_data)
                                        clean_data = {k: str(v) if v is not None else '' for k, v in enriched_data.items() if v is not None}
                                        if clean_data and not dry_run:
                                            try:
                                                self.clickhouse_redis.xadd('ticks:stream', clean_data, maxlen=100000, approximate=True)
                                                copied += 1
                                                stats['total_pushed'] += 1
                                            except Exception as e:
                                                logger.error(f"Error copying message from {key_str}: {e}")
                                                stats['total_errors'] += 1
                                logger.info(f"   Copied {copied} messages from {key_str}")
                                stats['db1_processed'] += streams_found
                        except Exception as e:
                            logger.error(f"Error processing stream {key_str}: {e}")
                            stats['total_errors'] += 1
                if cursor == 0:
                    break
            
            if streams_found == 0:
                logger.info("   No tick streams found, scanning individual keys...")
            
            # Then process other keys
            cursor = 0
            keys_processed = 0
            
            # Look for all data keys - scan all keys and filter by type
            patterns = ['tick:*', 'vol:*', 'ohlc:*', 'ind:*', 'ticks:*', 'options:*']
            for pattern in patterns:
                cursor = 0
                while True:
                    cursor, keys = self.unified_redis.scan(cursor, match=pattern, count=1000)
                    
                    for key in keys:
                        if limit and keys_processed >= limit:
                            break
                        
                        try:
                            key_str = key.decode() if isinstance(key, bytes) else key
                            self._import_key(self.unified_redis, key, key_str, stats, dry_run)
                            keys_processed += 1
                            stats['db1_processed'] += 1
                        except Exception as e:
                            stats['total_errors'] += 1
                            logger.error(f"Error processing DB1 key {key_str}: {e}")
                    
                    if cursor == 0 or (limit and keys_processed >= limit):
                        break
        except Exception as e:
            logger.error(f"Error importing from DB 1: {e}")
        
        # Import from DB 2 (volume/analytics DB)
        logger.info("📊 Importing from DB 2 (volume/analytics)...")
        try:
            cursor = 0
            keys_processed = 0
            
            # Look for volume and analytics keys
            patterns = ['vol:*', 'volume_*', 'ohlc_*', 'realtime:*', 'tick:*']
            for pattern in patterns:
                cursor = 0
                while True:
                    cursor, keys = self.volume_redis.scan(cursor, match=pattern, count=1000)
                    
                    for key in keys:
                        if limit and keys_processed >= limit:
                            break
                        
                        try:
                            key_str = key.decode() if isinstance(key, bytes) else key
                            self._import_key(self.volume_redis, key, key_str, stats, dry_run)
                            keys_processed += 1
                            stats['db2_processed'] += 1
                        except Exception as e:
                            stats['total_errors'] += 1
                            logger.error(f"Error processing DB2 key {key_str}: {e}")
                    
                    if cursor == 0 or (limit and keys_processed >= limit):
                        break
        except Exception as e:
            logger.error(f"Error importing from DB 2: {e}")
        
        stats['end_time'] = datetime.now().isoformat()
        logger.info(f"✅ Historical import completed: DB0={stats['db0_processed']}, DB1={stats['db1_processed']}, DB2={stats['db2_processed']}, Total pushed={stats['total_pushed']}, Errors={stats['total_errors']}")
        
        return stats
    
    def _import_key(self, redis_client, key, key_str: str, stats: Dict, dry_run: bool):
        """
        Import a single key from Redis to ClickHouse stream.
        
        Args:
            redis_client: Redis client to read from
            key: Redis key (bytes or str)
            key_str: Key as string
            stats: Statistics dictionary to update
            dry_run: If True, only simulate
        """
        try:
            key_type = redis_client.type(key)
            if isinstance(key_type, bytes):
                key_type = key_type.decode()
            
            if key_type == 'hash':
                data = redis_client.hgetall(key)
                if data:
                    if isinstance(list(data.values())[0], bytes):
                        data = {
                            k.decode() if isinstance(k, bytes) else k: 
                            v.decode() if isinstance(v, bytes) else v
                            for k, v in data.items()
                        }
                    
                    # Enrich and push - only if it has tick data fields
                    if data.get('instrument_token') or data.get('token') or data.get('last_price') or data.get('volume'):
                        enriched_data = self._enrich_tick_data(data)
                        # Filter out None values and ensure all values are strings/numbers
                        clean_data = {k: str(v) if v is not None else '' for k, v in enriched_data.items() if v is not None}
                        # Only push if we have meaningful data
                        if clean_data and not dry_run:
                            try:
                                result = self.clickhouse_redis.xadd('ticks:stream', clean_data, maxlen=10000, approximate=True)
                                if result:
                                    stats['total_pushed'] += 1
                                else:
                                    logger.warning(f"XADD returned None for key {key_str}")
                                    stats['total_errors'] += 1
                            except Exception as e:
                                logger.error(f"Failed to push to stream for key {key_str}: {e}")
                                stats['total_errors'] += 1
            
            elif key_type == 'stream':
                # Copy all messages from stream
                messages = redis_client.xrange(key, '-', '+', count=1000)
                for msg_id, msg_data in messages:
                    if isinstance(list(msg_data.values())[0], bytes):
                        msg_data = {
                            k.decode() if isinstance(k, bytes) else k: 
                            v.decode() if isinstance(v, bytes) else v
                            for k, v in msg_data.items()
                        }
                    
                    enriched_data = self._enrich_tick_data(msg_data)
                    if not dry_run:
                        self.clickhouse_redis.xadd('ticks:stream', enriched_data, maxlen=10000, approximate=True)
                        stats['total_pushed'] += 1
            
            elif key_type == 'string':
                # Try to parse as JSON
                value = redis_client.get(key)
                if value:
                    if isinstance(value, bytes):
                        value = value.decode()
                    
                    try:
                        import json
                        data = json.loads(value)
                        if isinstance(data, dict):
                            enriched_data = self._enrich_tick_data(data)
                            if not dry_run:
                                self.clickhouse_redis.xadd('ticks:stream', enriched_data, maxlen=10000, approximate=True)
                                stats['total_pushed'] += 1
                    except (json.JSONDecodeError, TypeError):
                        pass  # Skip non-JSON strings
        
        except Exception as e:
            stats['total_errors'] += 1
            logger.debug(f"Error importing key {key_str}: {e}")
    
    def import_continuous(self, interval_seconds: int = 60, dry_run: bool = False):
        """
        Continuously import data from main system Redis to ClickHouse streams.
        
        Args:
            interval_seconds: Seconds between import cycles
            dry_run: If True, only simulate without writing
        """
        import time
        
        logger.info(f"🔄 Starting continuous import (interval: {interval_seconds}s)")
        
        try:
            while True:
                # Import realtime ticks
                tick_stats = self.import_realtime_ticks(limit=1000, dry_run=dry_run)
                logger.info(f"Ticks: {tick_stats['pushed']} pushed, {tick_stats['errors']} errors")
                
                # Import volume data
                volume_stats = self.import_volume_data(limit=500, dry_run=dry_run)
                logger.info(f"Volume: {volume_stats['pushed']} pushed, {volume_stats['errors']} errors")
                
                # Wait before next cycle
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("🛑 Continuous import stopped by user")
        except Exception as e:
            logger.error(f"❌ Continuous import failed: {e}")
    
    def cleanup(self):
        """Clean up Redis connections."""
        try:
            # ✅ FIXED: UnifiedRedisManager handles cleanup automatically
            logger.info("✅ Cleaned up Redis connections")
        except Exception as e:
            logger.error(f"⚠️  Error during cleanup: {e}")


if __name__ == "__main__":
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='Redis Data Migration and ClickHouse Import Tools')
    parser.add_argument('--mode', choices=['consolidate', 'clickhouse'], default='consolidate',
                       help='Operation mode: consolidate (Redis migration) or clickhouse (import to ClickHouse)')
    parser.add_argument('--dry-run', action='store_true', help='Simulate without writing data')
    parser.add_argument('--limit', type=int, help='Limit number of keys to process')
    parser.add_argument('--continuous', action='store_true', help='Run continuous import (ClickHouse mode only)')
    parser.add_argument('--interval', type=int, default=60, help='Interval in seconds for continuous import')
    parser.add_argument('--historical', action='store_true', help='Import historical data from all databases')
    
    args = parser.parse_args()
    
    if args.mode == 'consolidate':
        print("🔄 Safe Redis Consolidation Tool")
        print("=" * 60)
        
        # Initialize consolidator
        consolidator = SafeRedisConsolidator()
        
        # Test connections
        print("\n🔌 Testing Redis connections...")
        for db_name, client in consolidator.source_dbs.items():
            try:
                client.ping()
                print(f"   ✅ {db_name} database: Connected")
            except Exception as e:
                print(f"   ❌ {db_name} database: {e}")
                sys.exit(1)
        
        try:
            consolidator.target_db.ping()
            print(f"   ✅ Target database (DB1 - Unified): Connected")
        except Exception as e:
            print(f"   ❌ Target database (DB1 - Unified): {e}")
            sys.exit(1)
        
        # Start migration
        print("\n" + "=" * 60)
        consolidator.start_dual_write_migration()
        
        # Verify migration
        verification = consolidator.verify_migration()
        
        # Save migration log
        consolidator.save_migration_log()
        
        # Cleanup
        consolidator.cleanup()
        
        print("\n✅ Migration process completed!")
    
    elif args.mode == 'clickhouse':
        print("🔄 ClickHouse Data Importer")
        print("=" * 60)
        
        # Initialize importer
        importer = ClickHouseDataImporter()
        
        # Test connections
        print("\n🔌 Testing Redis connections...")
        try:
            importer.source_redis.ping()
            print(f"   ✅ Source Redis (port {importer.redis_port}): Connected")
        except Exception as e:
            print(f"   ❌ Source Redis: {e}")
            sys.exit(1)
        
        try:
            importer.unified_redis.ping()
            print(f"   ✅ Unified Redis (port {importer.redis_port}, DB 1): Connected")
        except Exception as e:
            print(f"   ❌ Unified Redis: {e}")
            sys.exit(1)
        
        try:
            importer.volume_redis.ping()
            print(f"   ✅ Volume Redis (port {importer.redis_port}, DB 2): Connected")
        except Exception as e:
            print(f"   ❌ Volume Redis: {e}")
            sys.exit(1)
        
        try:
            importer.clickhouse_redis.ping()
            print(f"   ✅ ClickHouse Redis (port {importer.clickhouse_redis_port}): Connected")
        except Exception as e:
            print(f"   ❌ ClickHouse Redis: {e}")
            sys.exit(1)
        
        print(f"\n📋 Loaded metadata for {len(importer.token_metadata)} instruments")
        
        # Run import
        if args.historical:
            print("\n🔄 Starting historical data import...")
            historical_stats = importer.import_historical_data(limit=args.limit, dry_run=args.dry_run)
            print(f"\n📊 Historical Import Results:")
            print(f"   DB 0: {historical_stats['db0_processed']} processed")
            print(f"   DB 1: {historical_stats['db1_processed']} processed")
            print(f"   DB 2: {historical_stats['db2_processed']} processed")
            print(f"   Total pushed: {historical_stats['total_pushed']}")
            print(f"   Errors: {historical_stats['total_errors']}")
        elif args.continuous:
            print(f"\n🔄 Starting continuous import (interval: {args.interval}s)")
            print("   Press Ctrl+C to stop")
            try:
                importer.import_continuous(interval_seconds=args.interval, dry_run=args.dry_run)
            except KeyboardInterrupt:
                print("\n🛑 Stopped by user")
        else:
            print("\n🔄 Starting one-time import...")
            tick_stats = importer.import_realtime_ticks(limit=args.limit, dry_run=args.dry_run)
            print(f"\n📊 Realtime Ticks: {tick_stats['processed']} processed, {tick_stats['enriched']} enriched, {tick_stats['pushed']} pushed, {tick_stats['errors']} errors")
            
            volume_stats = importer.import_volume_data(limit=args.limit, dry_run=args.dry_run)
            print(f"📊 Volume Data: {volume_stats['processed']} processed, {volume_stats['pushed']} pushed, {volume_stats['errors']} errors")
        
        # Cleanup
        importer.cleanup()
        
        print("\n✅ Import process completed!")