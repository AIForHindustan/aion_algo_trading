#!/opt/homebrew/bin/python3.13
"""
Redis Database Migration Script
Moves analytics data to DB2 for consistency

Based on the new Redis key standards:
- DB1: Live ticks, OHLC, volume, indicators, session data
- DB2: Analytics, alert validations, performance metrics, pattern history

Uses canonical key methods from redis_key_standards.py
"""

import redis
import sys
import os
from typing import List, Tuple
from shared_core.redis_clients.redis_client import RedisClientFactory
from shared_core.redis_clients.redis_key_standards import (
    DatabaseAwareKeyBuilder,
    RedisDatabase,
    get_key_builder
)

def get_redis_client(db: int):
    """Get Redis client for specified database"""
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    redis_password = os.getenv('REDIS_PASSWORD', '')
    
    return RedisClientFactory.get_trading_client()

def get_analytics_key_patterns():
    """
    Get analytics key patterns using canonical key builder.
    Returns list of patterns that should be in DB2.
    """
    # These patterns match the analytics methods in DatabaseAwareKeyBuilder
    return [
        "pattern_history:*",           # analytics_pattern_history()
        "scanner:performance:*",       # analytics_scanner_performance()
        "alert_performance:*",         # analytics_alert_performance_*()
        "signal_quality:*",           # analytics_signal_quality()
        "pattern_performance:*",      # analytics_pattern_performance()
        "pattern_metrics:*",         # analytics_pattern_metrics()
        "forward_validation:*",       # analytics_validation()
        "analysis_cache:*"            # analytics_legacy_cache()
    ]

def is_analytics_key(key: str, builder: DatabaseAwareKeyBuilder) -> bool:
    """
    Check if a key belongs to analytics (DB2) using canonical key builder.
    
    Args:
        key: Redis key to check
        builder: DatabaseAwareKeyBuilder instance
        
    Returns:
        True if key should be in DB2, False otherwise
    """
    try:
        db = builder.get_database(key)
        return db == RedisDatabase.DB2_ANALYTICS
    except Exception:
        # Fallback: check if key matches analytics patterns
        analytics_patterns = get_analytics_key_patterns()
        return any(key.startswith(pattern.replace('*', '')) for pattern in analytics_patterns)

def find_analytics_keys_in_databases():
    """Find which databases contain analytics keys using canonical patterns"""
    print("🔍 Quick scan for analytics keys...\n")
    
    builder = get_key_builder()
    analytics_patterns = get_analytics_key_patterns()
    results = {}
    
    # Check DB0 and DB1 only (skip large DB1 full scan)
    for db_num in [0, 1]:
        try:
            client = get_redis_client(db=db_num)
            total_count = 0
            
            # Use KEYS for small databases (faster than SCAN for < 10K keys)
            for pattern in analytics_patterns:
                keys = client.keys(pattern)
                # Verify using canonical builder
                verified_keys = [k for k in keys if is_analytics_key(k.decode() if isinstance(k, bytes) else k, builder)]
                count = len(verified_keys)
                total_count += count
            
            if total_count > 0:
                results[db_num] = total_count
                print(f"  ✅ DB {db_num}: {total_count} analytics keys found")
        except Exception as e:
            print(f"  ❌ DB {db_num}: Error - {e}")
    
    return results

def migrate_analytics_to_db2(source_db: int = 0, dry_run: bool = False, batch_size: int = 100):
    """
    Migrate analytics keys from source database to DB2
    
    Args:
        source_db: Source database number (default: 0, can be 0 or 1)
        dry_run: If True, only report what would be migrated without actually migrating
        batch_size: Number of keys to process in each batch
    """
    # Connect to source and target databases
    source = get_redis_client(db=source_db)
    db2 = get_redis_client(db=2)
    
    # Get analytics patterns using canonical key builder
    builder = get_key_builder()
    analytics_patterns = get_analytics_key_patterns()
    
    print("="*80)
    print(f"REDIS DATABASE MIGRATION: DB {source_db} → DB2")
    print("="*80)
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE MIGRATION'}")
    print(f"Batch size: {batch_size}")
    print("="*80)
    print(f"\n🔄 Migrating analytics data from DB {source_db} to DB2...\n")
    
    total_migrated = 0
    total_failed = 0
    total_skipped = 0
    
    for pattern in analytics_patterns:
        print(f"\n📋 Processing pattern: {pattern}")
        pattern_migrated = 0
        pattern_failed = 0
        
        # Use KEYS for small databases (< 10K keys), SCAN for large ones
        try:
            # Try KEYS first (faster for small DBs)
            all_keys = source.keys(pattern)
            if len(all_keys) > 10000:
                # Too many keys, use SCAN
                cursor = 0
                all_keys = []
                while True:
                    cursor, keys = source.scan(cursor, match=pattern, count=batch_size)
                    all_keys.extend(keys)
                    if cursor == 0:
                        break
        except:
            # Fallback to SCAN
            cursor = 0
            all_keys = []
            while True:
                cursor, keys = source.scan(cursor, match=pattern, count=batch_size)
                all_keys.extend(keys)
                if cursor == 0:
                    break
        
        if not all_keys:
            continue
        
        # Decode bytes if needed and verify using canonical builder
        decoded_keys = [k.decode() if isinstance(k, bytes) else k for k in all_keys]
        
        # Filter to only analytics keys using canonical builder
        verified_keys = [k for k in decoded_keys if is_analytics_key(k, builder)]
        
        if not verified_keys:
            print(f"  ⏭️  No analytics keys found (all filtered by canonical builder)")
            continue
        
        print(f"  Processing {len(verified_keys)} analytics keys...")
        
        for i, key in enumerate(verified_keys):
            if i % 50 == 0 and i > 0:
                print(f"    Progress: {i}/{len(verified_keys)} keys processed...")
            
            try:
                # Verify key belongs to DB2 using canonical builder
                if not is_analytics_key(key, builder):
                    total_skipped += 1
                    continue
                
                # Check if key already exists in DB2
                if db2.exists(key):
                    total_skipped += 1
                    continue
                
                # Get key type
                key_type = source.type(key)
                
                if key_type == b'none' or key_type == 'none':
                    continue
                
                # Get value based on type
                value = None
                ttl = source.ttl(key)
                
                if key_type == b'string' or key_type == 'string':
                    value = source.get(key)
                    if isinstance(value, bytes):
                        value = value.decode()
                elif key_type == b'hash' or key_type == 'hash':
                    value = source.hgetall(key)
                elif key_type == b'list' or key_type == 'list':
                    value = source.lrange(key, 0, -1)
                elif key_type == b'set' or key_type == 'set':
                    value = source.smembers(key)
                elif key_type == b'zset' or key_type == 'zset':
                    value = source.zrange(key, 0, -1, withscores=True)
                elif key_type == b'stream' or key_type == 'stream':
                    # Streams need special handling
                    print(f"  ⚠️  Stream type not migrated: {key} (manual migration required)")
                    total_skipped += 1
                    continue
                else:
                    print(f"  ⚠️  Unknown type {key_type} for key: {key}")
                    total_skipped += 1
                    continue
                
                if value is None:
                    print(f"  ⚠️  Empty value for key: {key}")
                    total_skipped += 1
                    continue
                
                if not dry_run:
                    # Restore in DB2 based on type
                    if key_type == b'string' or key_type == 'string':
                        db2.set(key, value)
                    elif key_type == b'hash' or key_type == 'hash':
                        if value:
                            db2.hset(key, mapping=value)
                    elif key_type == b'list' or key_type == 'list':
                        if value:
                            db2.rpush(key, *value)
                    elif key_type == b'set' or key_type == 'set':
                        if value:
                            db2.sadd(key, *value)
                    elif key_type == b'zset' or key_type == 'zset':
                        if value:
                            # Convert list of tuples to dict for zadd
                            if isinstance(value, list):
                                db2.zadd(key, dict(value))
                    
                    # Set TTL if it exists
                    if ttl > 0:
                        db2.expire(key, ttl)
                    elif ttl == -1:
                        # Persistent key, no TTL needed
                        pass
                    
                    # Delete from source database
                    source.delete(key)
                
                pattern_migrated += 1
                total_migrated += 1
                
            except Exception as e:
                print(f"  ❌ Failed to migrate {key}: {e}")
                pattern_failed += 1
                total_failed += 1
            
        print(f"  ✅ Pattern complete: {pattern_migrated} migrated, {pattern_failed} failed")
    
    print("\n" + "="*80)
    print("MIGRATION SUMMARY")
    print("="*80)
    print(f"Total keys migrated: {total_migrated}")
    print(f"Total keys failed: {total_failed}")
    print(f"Total keys skipped: {total_skipped}")
    print("="*80)
    
    if dry_run:
        print("\n⚠️  This was a DRY RUN. No changes were made.")
        print("Run without --dry-run to perform actual migration.")
    else:
        print("\n✅ Migration complete!")
    
    return {
        'migrated': total_migrated,
        'failed': total_failed,
        'skipped': total_skipped
    }

def verify_migration(source_db: int = 0):
    """Verify that analytics keys are in DB2 and not in source database"""
    print("\n" + "="*80)
    print("VERIFICATION: Checking migration status")
    print("="*80)
    
    source = get_redis_client(db=source_db)
    db2 = get_redis_client(db=2)
    
    builder = get_key_builder()
    analytics_patterns = get_analytics_key_patterns()
    
    db1_count = 0
    db2_count = 0
    
    for pattern in analytics_patterns:
        # Count in source database
        cursor = 0
        while True:
            cursor, keys = source.scan(cursor, match=pattern, count=100)
            db1_count += len(keys)
            if cursor == 0:
                break
        
        # Count in DB2
        cursor = 0
        while True:
            cursor, keys = db2.scan(cursor, match=pattern, count=100)
            db2_count += len(keys)
            if cursor == 0:
                break
    
    print(f"\n📊 Analytics keys in DB {source_db}: {db1_count}")
    print(f"📊 Analytics keys in DB2: {db2_count}")
    
    if db1_count == 0 and db2_count > 0:
        print("✅ Migration verified: All analytics keys are in DB2")
    elif db1_count > 0:
        print(f"⚠️  Warning: {db1_count} analytics keys still in DB {source_db}")
    else:
        print("ℹ️  No analytics keys found in either database")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate analytics data to DB2')
    parser.add_argument('--source-db', type=int, default=0,
                       help='Source database number (default: 0, can be 0 or 1)')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Perform a dry run without making changes')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Number of keys to process per batch (default: 100)')
    parser.add_argument('--verify', action='store_true',
                       help='Verify migration status after completion')
    parser.add_argument('--find', action='store_true',
                       help='Find which databases contain analytics keys')
    
    args = parser.parse_args()
    
    try:
        # Find analytics keys if requested
        if args.find:
            find_analytics_keys_in_databases()
            return
        
        # Perform migration
        results = migrate_analytics_to_db2(
            source_db=args.source_db,
            dry_run=args.dry_run, 
            batch_size=args.batch_size
        )
        
        # Verify if requested
        if args.verify and not args.dry_run:
            verify_migration(source_db=args.source_db)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

