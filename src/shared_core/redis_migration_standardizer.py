# redis_migration_standardizer.py
"""
Redis Migration Standardizer - PORT 6379 ONLY

⚠️ CRITICAL: This script ONLY works with Redis on port 6379.
Do not use with other Redis instances or ports.

This script:
1. Analyzes Redis keys across databases
2. Creates a migration plan to standardize key naming
3. Migrates keys to appropriate databases (DB0, DB1, DB2, DB3)
4. Adds exchange prefixes where missing

Database Assignments:
- DB0: User data (user:, user_patterns:, broker_creds:, etc.)
- DB1: Market data (ohlc:, ticks:, etc.)
    - DB2: Analytics and pattern_performance (pattern_history, alert_performance, signal_quality, etc.)
- DB3: Sessions (baseline:, session:, etc.)
- DB15: Temporary data
"""
import json
import csv
import redis
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum

class Database(Enum):
    """Standard database assignments
    
    ✅ ALIGNED with DatabaseAwareKeyBuilder:
    - DB0: User accounts, credentials, preferences
    - DB1: Live market data (ticks, OHLC, indicators, volume, baselines, sessions)
    - DB2: Analytics and pattern_performance (pattern_history, alert_performance, signal_quality, pattern_metrics, etc.)
    - DB3: Reserved for future use
    - DB15: Temporary/transient data
    """
    USER_DATA = 0      # User accounts, credentials, preferences
    MARKET_DATA = 1    # Live market data, ticks, OHLC, indicators, volume, baselines
    ANALYTICS = 2      # Analytics and pattern_performance (pattern_history, alert_performance, signal_quality, pattern_metrics, etc.)
    SESSIONS = 3       # Reserved for future use
    TEMP = 15          # Temporary/transient data

@dataclass
class KeyMapping:
    old_key: str
    old_db: int
    old_type: str
    new_key: str
    new_db: int
    migration_notes: str

class RedisKeyStandardizer:
    """Standardizes Redis keys across databases
    
    ⚠️ CRITICAL: This script ONLY works with Redis on port 6379.
    Do not use with other Redis instances or ports.
    """
    
    def __init__(self, host='localhost', port=6379):
        # ✅ ENFORCE: Only allow port 6379
        if port != 6379:
            raise ValueError(
                f"❌ ERROR: This migration script ONLY works with Redis port 6379. "
                f"Attempted to use port {port}. "
                f"Please connect to the correct Redis instance on port 6379."
            )
        
        self.port = port
        self.host = host
        
        # Verify connection to port 6379
        try:
            self.client = redis.Redis(
                host=host, 
                port=port, 
                decode_responses=True,  # ✅ Enforce consistent response format
                socket_keepalive=True
            )
            # Test connection
            self.client.ping()
            print(f"✅ Connected to Redis on {host}:{port}")
        except redis.ConnectionError as e:
            raise ConnectionError(
                f"❌ ERROR: Cannot connect to Redis on {host}:{port}. "
                f"This script requires Redis on port 6379. "
                f"Error: {str(e)}"
            )
        
    def analyze_inventory(self, inventory_file: str) -> Dict:
        """Analyze current Redis state and identify standardization needs"""
        with open(inventory_file, 'r') as f:
            inventory = json.load(f)
        
        analysis = {
            'total_keys': inventory['total_keys'],
            'db_stats': {},
            'pattern_counts': inventory['patterns'],
            'issues_found': [],
            'recommendations': []
        }
        
        # Analyze each database
        for db, keys in inventory['databases'].items():
            db_int = int(db)
            analysis['db_stats'][db_int] = {
                'key_count': len(keys),
                'key_types': {},
                'patterns_found': []
            }
            
            # Categorize keys
            for key_info in keys:
                key = key_info['key']
                key_type = key_info['type']
                
                # Count key types
                analysis['db_stats'][db_int]['key_types'][key_type] = \
                    analysis['db_stats'][db_int]['key_types'].get(key_type, 0) + 1
                
                # Check for standardization issues
                self._analyze_key_issues(key, db_int, key_type, analysis)
        
        return analysis
    
    def _analyze_key_issues(self, key: str, db: int, key_type: str, analysis: Dict):
        """Identify standardization issues in keys"""
        
        # Issue: Missing exchange prefix in market data
        if db == 1 and ':' in key:
            parts = key.split(':')
            if len(parts) >= 2:
                # Check if it's a market data key without exchange prefix
                market_patterns = ['ohlc:', 'ind:', 'vol:', 'baseline:', 'volume_profile:']
                if any(key.startswith(pattern) for pattern in market_patterns):
                    # ✅ FIXED: Check if ANY part has exchange prefix, not just the first symbol part
                    # Keys like volume_profile:poc:NSENIFTY25D1626000PE already have NSE prefix
                    has_exchange_prefix = any(
                        any(part.startswith(ex) for ex in ['NSE', 'NFO', 'BSE', 'MCX', 'CDS'])
                        for part in parts
                    )
                    
                    if not has_exchange_prefix:
                        # Find the symbol part to report
                        symbol_parts = parts[1:] if parts[0] != 'ind' else parts[2:]
                        for symbol in symbol_parts:
                            if symbol and len(symbol) > 3:  # Skip short parts like 'poc', 'session'
                                analysis['issues_found'].append({
                                    'issue': 'Missing exchange prefix',
                                    'key': key,
                                    'db': db,
                                    'recommendation': f'Add exchange prefix (NSE/NFO/BSE/MCX) to symbol: {symbol}'
                                })
                                break
        
        # Issue: Mixed concerns in DB1
        if db == 1 and key.startswith(('user:', 'broker_creds:', 'account_key:')):
            analysis['issues_found'].append({
                'issue': 'User data in market database',
                'key': key,
                'db': db,
                'recommendation': f'Move to DB{Database.USER_DATA.value}'
            })
        
        # Issue: Analytics data in DB1 (should be in DB2)
        analytics_prefixes = [
            'pattern_history:', 'pattern_performance:', 'pattern_metrics:',
            'alert_performance:', 'signal_quality:', 'scanner:',
            'analysis_cache:', 'forward_validation:', 'time_window_performance:',
            'pattern_window_aggregate:', 'sharpe_inputs:', 'alert_timeline:',
            'final_validation:'
        ]
        if db == 1 and any(key.startswith(prefix) for prefix in analytics_prefixes):
            analysis['issues_found'].append({
                'issue': 'Analytics/pattern performance data in live database',
                'key': key,
                'db': db,
                'recommendation': f'Move to DB{Database.ANALYTICS.value} (pattern_performance and analytics)'
            })
    
    def create_migration_plan(self, inventory_file: str) -> List[KeyMapping]:
        """Create comprehensive migration plan"""
        with open(inventory_file, 'r') as f:
            inventory = json.load(f)
        
        migration_plan = []
        
        for db_str, keys in inventory['databases'].items():
            db = int(db_str)
            
            for key_info in keys:
                old_key = key_info['key']
                old_type = key_info['type']
                
                # Apply standardization rules
                new_key, new_db, notes = self._standardize_key(old_key, old_type, db)
                
                migration_plan.append(KeyMapping(
                    old_key=old_key,
                    old_db=db,
                    old_type=old_type,
                    new_key=new_key,
                    new_db=new_db,
                    migration_notes=notes
                ))
        
        return migration_plan
    
    def _standardize_key(self, old_key: str, key_type: str, old_db: int) -> Tuple[str, int, str]:
        """Standardize a single key according to new naming convention"""
        
        # Default: keep as-is
        new_key = old_key
        new_db = old_db
        notes = "No changes needed"
        
        # --- USER DATA (move to DB0) ---
        if old_key.startswith('user:'):
            new_db = Database.USER_DATA.value
            notes = "User data in dedicated DB"
            
        elif old_key.startswith('user_patterns:'):
            new_db = Database.USER_DATA.value
            notes = "User patterns in dedicated DB"
            
        elif old_key.startswith('broker_creds:'):
            new_db = Database.USER_DATA.value
            notes = "Broker credentials in dedicated DB"
            
        elif old_key.startswith('account_key:'):
            new_db = Database.USER_DATA.value
            notes = "Account keys in dedicated DB"
            
        elif old_key.startswith('positions:'):
            new_db = Database.USER_DATA.value
            notes = "Positions in dedicated DB"
        
        # --- MARKET DATA (standardize in DB1) ---
        # ✅ ALIGNED: All live data (indicators, OHLC, volume, baselines) go to DB1
        # This matches DatabaseAwareKeyBuilder.get_database() logic
        elif old_key.startswith('ind:'):
            new_db = Database.MARKET_DATA.value  # DB1 - Indicators are live data
            new_key = self._add_exchange_prefix(old_key, 'ind:')
            notes = "Indicator data (DB1) with exchange prefix"
            
        elif old_key.startswith('ohlc:'):
            new_db = Database.MARKET_DATA.value  # DB1
            new_key = self._add_exchange_prefix(old_key, 'ohlc:')
            notes = "OHLC data (DB1) with exchange prefix"
            
        elif old_key.startswith('vol:'):
            new_db = Database.MARKET_DATA.value  # DB1 - Volume data is live data
            new_key = self._add_exchange_prefix(old_key, 'vol:')
            notes = "Volume data (DB1) with exchange prefix"
            
        elif old_key.startswith('volume_profile:'):
            new_db = Database.MARKET_DATA.value  # DB1 - Volume profiles are live data
            new_key = self._add_exchange_prefix(old_key, 'volume_profile:')
            notes = "Volume profile (DB1) with exchange prefix"
            
        elif old_key.startswith('baseline:'):
            new_db = Database.MARKET_DATA.value  # DB1 - Baselines are live data
            new_key = self._add_exchange_prefix(old_key, 'baseline:')
            notes = "Volume baseline (DB1) with exchange prefix"
        
        # --- ANALYTICS (DB2) ---
        # ✅ ALIGNED: pattern_performance and analytics go to DB2
        # Matches DatabaseAwareKeyBuilder.get_database() logic
        analytics_prefixes = [
            'pattern_history:', 'pattern_performance:', 'pattern_metrics:',
            'alert_performance:', 'signal_quality:', 'scanner:',
            'analysis_cache:', 'forward_validation:', 'time_window_performance:',
            'pattern_window_aggregate:', 'sharpe_inputs:', 'alert_timeline:',
            'final_validation:'
        ]
        
        if any(old_key.startswith(prefix) for prefix in analytics_prefixes):
            new_db = Database.ANALYTICS.value  # DB2
            notes = "Analytics/pattern performance data (DB2)"
        
        return new_key, new_db, notes
    
    def _add_exchange_prefix(self, key: str, prefix: str) -> str:
        """Add exchange prefix to symbol if missing"""
        # Remove the prefix to get the rest
        rest = key[len(prefix):]
        parts = rest.split(':')
        
        if len(parts) == 0:
            return key
        
        # ✅ FIXED: Find the actual symbol part
        # Known non-symbol prefixes: poc, session, patterns, custom, ta, greeks, baseline, warning, etc.
        non_symbol_prefixes = {'poc', 'session', 'patterns', 'custom', 'ta', 'greeks', 'baseline', 'warning',
                              'realtime', 'daily', 'hourly', 'stats', 'updates', 'distribution', 'nodes'}
        
        symbol = None
        symbol_index = None
        
        # Strategy 1: Look for parts that already have exchange prefix
        for i, part in enumerate(parts):
            if any(part.startswith(ex) for ex in ['NSE', 'NFO', 'BSE', 'MCX', 'CDS']):
                # Already has prefix, return as-is
                return key
        
        # Strategy 2: Find the symbol (skip known non-symbol prefixes)
        for i, part in enumerate(parts):
            # Skip known non-symbol prefixes
            if part.lower() in non_symbol_prefixes:
                continue
            
            # Check if it looks like a trading symbol:
            # - Ends with CE/PE (options)
            # - Contains FUT (futures)
            # - Contains common index names
            # - Is alphanumeric and reasonably long
            if (part.endswith('CE') or part.endswith('PE') or 
                'FUT' in part.upper() or
                any(idx in part.upper() for idx in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']) or
                (len(part) > 3 and part.replace('_', '').replace('-', '').isalnum())):
                symbol = part
                symbol_index = i
                break
        
        # If we found a symbol without prefix, add it
        if symbol and symbol_index is not None:
            exchange = self._infer_exchange(symbol)
            if exchange:
                parts[symbol_index] = f"{exchange}{symbol}"  # No colon - exchange is part of symbol
                return prefix + ':'.join(parts)
        
        return key
    
    def _infer_exchange(self, symbol: str) -> Optional[str]:
        """Infer exchange from symbol pattern"""
        symbol_upper = symbol.upper()
        
        # ✅ FIXED: Check if it's actually an option (has numbers/dates, not just ends with CE/PE)
        # Options have pattern: SYMBOL + DATE + STRIKE + CE/PE (e.g., NIFTY25DEC22400CE)
        # Equities like RELIANCE are just words, not options
        has_numbers = any(c.isdigit() for c in symbol)
        ends_with_option = symbol_upper.endswith('CE') or symbol_upper.endswith('PE')
        
        # Only treat as option if it has numbers AND ends with CE/PE
        if ends_with_option and has_numbers:
            return 'NFO'
        # Future symbols
        elif 'FUT' in symbol_upper:
            return 'NFO'
        # Index symbols
        elif any(idx in symbol_upper for idx in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']):
            return 'NSE'
        # Common equity symbols (simple names without dates/numbers)
        # RELIANCE, ITC, etc. are equities, not options
        elif len(symbol) < 15 and symbol.replace('_', '').isalpha():
            return 'NSE'
        # Assume NSE for others (most equities are NSE)
        else:
            return 'NSE'
    
    def execute_migration(self, migration_plan: List[KeyMapping], dry_run: bool = True):
        """Execute the migration plan"""
        stats = {
            'total': len(migration_plan),
            'migrated': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }
        
        for mapping in migration_plan:
            try:
                # Skip if no changes needed
                if mapping.old_key == mapping.new_key and mapping.old_db == mapping.new_db:
                    stats['skipped'] += 1
                    continue
                
                # Select source database
                self.client.select(mapping.old_db)
                
                # Get data based on type
                data = self._get_key_data(mapping.old_key, mapping.old_type)
                
                if data is None:
                    stats['skipped'] += 1
                    print(f"⚠ Skipping (not found): {mapping.old_key}")
                    continue
                
                if not dry_run:
                    # Select destination database
                    self.client.select(mapping.new_db)
                    
                    # Set data in new location
                    success = self._set_key_data(mapping.new_key, mapping.old_type, data)
                    
                    if success:
                        # Optional: Delete old key (comment out for safety)
                        # self.client.select(mapping.old_db)
                        # self.client.delete(mapping.old_key)
                        stats['migrated'] += 1
                    else:
                        stats['failed'] += 1
                        stats['errors'].append(f"Failed to set: {mapping.new_key}")
                else:
                    # ✅ FIXED: Count would-be migrations in dry run mode
                    stats['migrated'] += 1
                
                print(f"{'✓' if not dry_run else '[DRY]'} {mapping.old_key} (DB{mapping.old_db}) → {mapping.new_key} (DB{mapping.new_db})")
                
            except Exception as e:
                stats['failed'] += 1
                stats['errors'].append(f"{mapping.old_key}: {str(e)}")
                print(f"✗ Error: {mapping.old_key} - {str(e)}")
        
        return stats
    
    def _get_key_data(self, key: str, key_type: str):
        """Get key data based on type"""
        try:
            if key_type == 'string':
                return self.client.get(key)
            elif key_type == 'hash':
                return self.client.hgetall(key)
            elif key_type == 'list':
                return self.client.lrange(key, 0, -1)
            elif key_type == 'zset':
                return self.client.zrange(key, 0, -1, withscores=True)
            elif key_type == 'set':
                return self.client.smembers(key)
            elif key_type == 'stream':
                return self.client.xrange(key, '-', '+', count=1000)
            else:
                print(f"Warning: Unknown type {key_type} for key {key}")
                return None
        except redis.exceptions.ResponseError as e:
            print(f"Error getting {key}: {str(e)}")
            return None
    
    def _set_key_data(self, key: str, key_type: str, data):
        """Set key data based on type"""
        try:
            if key_type == 'string':
                return self.client.set(key, data)
            elif key_type == 'hash':
                if data:
                    # ✅ FIXED: Use hset instead of deprecated hmset
                    return self.client.hset(key, mapping=data)
            elif key_type == 'list':
                if data:
                    return self.client.rpush(key, *data)
            elif key_type == 'zset':
                if data:
                    return self.client.zadd(key, dict(data))
            elif key_type == 'set':
                if data:
                    return self.client.sadd(key, *data)
            elif key_type == 'stream':
                # Streams are trickier - need to preserve IDs
                # This is a simplified implementation
                for entry_id, entry_data in data:
                    self.client.xadd(key, entry_data, id=entry_id)
                return True
            return False
        except Exception as e:
            print(f"Error setting {key}: {str(e)}")
            return False
    
    def export_migration_csv(self, migration_plan: List[KeyMapping], output_file: str):
        """Export migration plan to CSV"""
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'current_key', 'current_db', 'current_type', 
                'new_key', 'new_db', 'migration_notes'
            ])
            writer.writeheader()
            
            for mapping in migration_plan:
                writer.writerow({
                    'current_key': mapping.old_key,
                    'current_db': mapping.old_db,
                    'current_type': mapping.old_type,
                    'new_key': mapping.new_key,
                    'new_db': mapping.new_db,
                    'migration_notes': mapping.migration_notes
                })
        
        print(f"✅ Migration plan exported to {output_file}")
    
    def verify_migration(self, migration_plan: List[KeyMapping]) -> Dict:
        """Verify migration was successful"""
        verification = {
            'verified': 0,
            'mismatches': [],
            'missing': []
        }
        
        for mapping in migration_plan:
            # Skip unchanged keys
            if mapping.old_key == mapping.new_key and mapping.old_db == mapping.new_db:
                continue
            
            # Check old location (should exist before migration)
            self.client.select(mapping.old_db)
            old_exists = self.client.exists(mapping.old_key)
            
            # Check new location (should exist after migration)
            self.client.select(mapping.new_db)
            new_exists = self.client.exists(mapping.new_key)
            
            if old_exists and new_exists:
                verification['verified'] += 1
            elif not new_exists:
                verification['missing'].append(mapping.new_key)
            elif not old_exists and new_exists:
                # This is OK - migration happened
                verification['verified'] += 1
        
        return verification

# --- MAIN EXECUTION ---
def main():
    """Execute the migration
    
    ⚠️ CRITICAL: This script ONLY works with Redis on port 6379.
    """
    
    print("=" * 70)
    print("🔒 Redis Migration Standardizer - PORT 6379 ONLY")
    print("=" * 70)
    print("⚠️  WARNING: This script will modify Redis data on port 6379")
    print("⚠️  Ensure you have a backup before proceeding")
    print("=" * 70)
    print()
    
    # ✅ ENFORCE: Only allow port 6379
    try:
        migrator = RedisKeyStandardizer(host='localhost', port=6379)
    except ValueError as e:
        print(f"\n{e}")
        print("\n❌ Migration aborted. Please use Redis on port 6379.")
        return
    except ConnectionError as e:
        print(f"\n{e}")
        print("\n❌ Migration aborted. Cannot connect to Redis on port 6379.")
        return
    
    # Verify we're on the correct port
    if migrator.port != 6379:
        print(f"❌ ERROR: Script requires port 6379, but got {migrator.port}")
        return
    
    # 1. Analyze current state
    # ✅ FIXED: Use inventory from shared_core directory
    inventory_file = 'redis_inventory.json'
    print(f"🔍 Analyzing Redis inventory on port 6379...")
    print(f"📁 Using inventory file: {inventory_file}")
    
    # Check if inventory exists
    import os
    if not os.path.exists(inventory_file):
        print(f"\n❌ Inventory file not found: {inventory_file}")
        print(f"   Please run: python3 ../redis_inventory.py")
        print(f"   This will generate the complete inventory for all keys")
        return
    
    analysis = migrator.analyze_inventory(inventory_file)
    
    print(f"📊 Total keys: {analysis['total_keys']}")
    for db, stats in analysis['db_stats'].items():
        print(f"  DB{db}: {stats['key_count']} keys")
    
    print("\n🚨 Issues found:")
    for issue in analysis['issues_found'][:10]:  # Show top 10
        print(f"  - {issue['issue']}: {issue['key']}")
    
    # 2. Create migration plan
    print("\n📋 Creating migration plan...")
    if not os.path.exists(inventory_file):
        print(f"❌ Inventory file not found: {inventory_file}")
        return
    migration_plan = migrator.create_migration_plan(inventory_file)
    
    # 3. Export to CSV
    migrator.export_migration_csv(migration_plan, 'redis_migration_plan.csv')
    
    # 4. DRY RUN FIRST!
    print("\n🧪 Performing dry run...")
    dry_run_stats = migrator.execute_migration(migration_plan, dry_run=True)
    
    print(f"\n📈 Dry Run Stats:")
    print(f"  Total: {dry_run_stats['total']}")
    print(f"  Would migrate: {dry_run_stats['migrated']}")
    print(f"  Would skip: {dry_run_stats['skipped']}")
    print(f"  Would fail: {dry_run_stats['failed']}")
    
    # 5. Execute migration (auto-confirm for script execution)
    import sys
    auto_execute = '--execute' in sys.argv or '--yes' in sys.argv
    
    if auto_execute:
        response = 'yes'
        print("\n✅ Auto-executing migration (--execute flag provided)...")
    else:
        response = input("\n✅ Dry run complete. Execute real migration? (yes/no): ")
    
    if response.lower() == 'yes':
        print("\n🚀 Executing migration...")
        real_stats = migrator.execute_migration(migration_plan, dry_run=False)
        
        print(f"\n📈 Real Migration Stats:")
        print(f"  Total: {real_stats['total']}")
        print(f"  Migrated: {real_stats['migrated']}")
        print(f"  Skipped: {real_stats['skipped']}")
        print(f"  Failed: {real_stats['failed']}")
        
        if real_stats['errors']:
            print("\n❌ Errors:")
            for error in real_stats['errors'][:5]:  # Show top 5 errors
                print(f"  - {error}")
        
        # 6. Verify migration
        print("\n🔍 Verifying migration...")
        verification = migrator.verify_migration(migration_plan)
        
        print(f"  Verified: {verification['verified']}")
        print(f"  Missing: {len(verification['missing'])}")
        if verification['missing']:
            print("  Missing keys (first 5):", verification['missing'][:5])
    
    else:
        print("\n⚠ Migration cancelled.")

if __name__ == "__main__":
    main()
