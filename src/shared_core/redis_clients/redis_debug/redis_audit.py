#!/opt/homebrew/bin/python3.13
"""
Unified Redis Audit Tool
Consolidates keyspace audit, storage consistency checks, and codebase pattern matching.

Features:
- Keyspace namespace analysis with memory sampling
- Storage consistency checks (key patterns, data types, TTLs)
- Codebase pattern matching for Redis key usage
"""

import argparse
import glob
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import redis

from shared_core.redis_clients.redis_client import RedisClientFactory


DEFAULT_PROCESS_NAME = "redis_audit"


def _ensure_str(value):
    """Convert bytes to string if needed."""
    if isinstance(value, bytes):
        return value.decode('utf-8', errors='ignore')
    return str(value)


def connect_redis(
    db: int,
    host: str = 'localhost',
    port: int = 6379,
    password: str | None = None,
    quiet: bool = False,
):
    """Establish a connection to the Redis server using RedisClientFactory pools."""
    try:
        client = RedisClientFactory.get_trading_client()
        client.ping()
        if not quiet:
            info = client.info("server")
            print(
                f"✓ Connected to Redis {info.get('redis_version', 'unknown')} "
                f"(db={db}) at {host}:{port}"
            )
        return client
    except redis.exceptions.ConnectionError as exc:
        print(f"✗ Failed to connect to Redis: {exc}")
        sys.exit(1)
    except redis.exceptions.AuthenticationError as exc:
        print(f"✗ Authentication failed: {exc}")
        sys.exit(1)
    except redis.exceptions.RedisError as exc:
        print(f"✗ Connection failed: {exc}")
        sys.exit(1)


# ============================================================================
# MODE 1: Keyspace Audit (from redis_audit.py)
# ============================================================================

def analyze_namespace(key, separator=':'):
    """Extract the namespace prefix from a key."""
    key_str = _ensure_str(key)
    parts = key_str.split(separator)
    if len(parts) > 1:
        return separator.join(parts[:2]) + separator + '*'
    return key_str


def get_memory_usage(client, key):
    """Get memory usage for a key. Falls back to debug method if MEMORY USAGE is unavailable."""
    key = _ensure_str(key)
    try:
        # Prefer the MEMORY USAGE command for accuracy
        return client.memory_usage(key)
    except redis.exceptions.ResponseError:
        # Fallback: Use DEBUG OBJECT and parse the serialized length (less accurate)
        try:
            debug_info = client.debug_object(key)
            return int(debug_info.get('serializedlength', 0))
        except redis.exceptions.ResponseError:
            return 0


def audit_keyspace(client, args):
    """Main function to audit the Redis keyspace."""
    print(f"\nStarting keyspace audit with pattern: '{args.pattern}' | Sample size: {args.sample_size} keys")
    print("This may take a while for large databases...\n")

    total_keys = 0
    namespace_data = defaultdict(lambda: {'count': 0, 'memory_samples': [], 'types': Counter()})
    cursor = 0
    sampled_keys = 0

    try:
        while True:
            cursor, keys = client.scan(cursor=cursor, match=args.pattern, count=args.scan_count)
            for key in keys:
                total_keys += 1
                key_str = _ensure_str(key)
                
                # Analyze the key's namespace
                prefix = analyze_namespace(key_str, args.separator)
                namespace_data[prefix]['count'] += 1
                
                # Get key type
                key_type = client.type(key_str)
                namespace_data[prefix]['types'][key_type] += 1
                
                # Sample memory usage if needed
                if args.enable_memory_sampling and sampled_keys < args.sample_size:
                    memory_estimate = get_memory_usage(client, key)
                    namespace_data[prefix]['memory_samples'].append(memory_estimate)
                    sampled_keys += 1

            if cursor == 0:
                break
            if args.limit and total_keys >= args.limit:
                print(f"Reached scan limit of {args.limit} keys.")
                break

    except KeyboardInterrupt:
        print("\nScan interrupted by user.")
        return total_keys, namespace_data

    return total_keys, namespace_data


def generate_keyspace_report(total_keys, namespace_data, args, db_label=None):
    """Generate and display the keyspace audit report."""
    if not namespace_data:
        print("No keys found matching the criteria.")
        return

    # Calculate estimated total memory
    for prefix, data in namespace_data.items():
        samples = data['memory_samples']
        if samples:
            avg_size = sum(samples) / len(samples)
            data['estimated_total_memory'] = avg_size * data['count']
        else:
            data['estimated_total_memory'] = 0

    # Sort namespaces by estimated memory usage (descending)
    sorted_namespaces = sorted(namespace_data.items(), 
                             key=lambda x: x[1]['estimated_total_memory'], 
                             reverse=True)

    # Print Summary Report
    header = f"REDIS KEYSPACE AUDIT REPORT"
    if db_label:
        header += f" ({db_label})"
    print("=" * 80)
    print(header)
    print("=" * 80)
    print(f"Total keys scanned: {total_keys}")
    print(f"Unique namespace patterns found: {len(namespace_data)}")
    print(f"Memory sampling enabled: {args.enable_memory_sampling}")
    if args.enable_memory_sampling:
        print(f"Keys sampled for memory: {sum(len(data['memory_samples']) for data in namespace_data.values())}")
    print()

    # Print Detailed Table
    print(f"{'NAMESPACE PATTERN':<40} {'COUNT':>8} {'EST MEMORY':>12} {'TOP TYPES':<20}")
    print("-" * 80)

    for prefix, data in sorted_namespaces[:args.top]:  # Show top N namespaces
        count = data['count']
        est_mem = data['estimated_total_memory']
        mem_str = f"{est_mem / 1024:.2f} KB" if est_mem > 0 else "N/A"
        
        # Get top 2 key types
        top_types = ', '.join([f"{k}({v})" for k, v in data['types'].most_common(2)])
        
        print(f"{prefix:<40} {count:>8} {mem_str:>12} {top_types:<20}")

    print("-" * 80)


# ============================================================================
# MODE 2: Storage Consistency Audit (from redis_audit2.py)
# ============================================================================

def analyze_key_structure_inconsistencies(client):
    """Analyze key naming patterns for inconsistencies"""
    print("\n" + "="*60)
    print("KEY STRUCTURE INCONSISTENCY ANALYSIS")
    print("="*60)
    
    all_keys = []
    cursor = 0
    while True:
        cursor, keys = client.scan(cursor, count=1000)
        all_keys.extend(_ensure_str(key) for key in keys)
        if cursor == 0:
            break
    
    # Pattern analysis
    pattern_groups = defaultdict(list)
    
    for key in all_keys:
        # Extract base pattern by replacing variable parts
        base_pattern = re.sub(r':\w+:', ':*:', key)  # Replace middle segments
        base_pattern = re.sub(r':\w+$', ':*', base_pattern)  # Replace last segment
        base_pattern = re.sub(r'^\w+:', '*:', base_pattern)  # Replace first segment
        pattern_groups[base_pattern].append(key)
    
    print("\n📊 KEY PATTERN GROUPS:")
    for pattern, keys in sorted(pattern_groups.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"  {pattern}: {len(keys)} keys")
        if len(keys) <= 3:  # Show examples for small groups
            print(f"    Examples: {keys[:3]}")


def analyze_data_type_inconsistencies(client):
    """Check for inconsistent data types in similar key patterns"""
    print("\n" + "="*60)
    print("DATA TYPE INCONSISTENCY ANALYSIS")
    print("="*60)
    
    # Get all keys with their types
    key_types = {}
    cursor = 0
    batch_keys = []
    
    while True:
        cursor, keys = client.scan(cursor, count=500)
        batch_keys.extend(_ensure_str(key) for key in keys)
        if cursor == 0:
            break
    
    # Get types in batches for efficiency
    for i in range(0, len(batch_keys), 100):
        batch = batch_keys[i:i+100]
        pipeline = client.pipeline()
        for key in batch:
            pipeline.type(key)
        types = pipeline.execute()
        
        for key, key_type in zip(batch, types):
            key_types[key] = key_type
    
    # Group by pattern and check type consistency
    pattern_types = defaultdict(lambda: defaultdict(int))
    
    for key, key_type in key_types.items():
        # Create pattern groups
        patterns = [
            re.sub(r'\d+', '*', key),  # Replace numbers
            re.sub(r'[a-fA-F0-9]{8,}', '*', key),  # Replace hex/hashes
            ':'.join(key.split(':')[:2]) + ':*' if ':' in key else key,  # First 2 segments
        ]
        
        for pattern in patterns:
            pattern_types[pattern][key_type] += 1
    
    print("\n🔍 POTENTIAL DATA TYPE INCONSISTENCIES:")
    inconsistencies_found = False
    
    for pattern, type_counts in pattern_types.items():
        if len(type_counts) > 1:
            print(f"\n🚨 INCONSISTENT TYPES in pattern: {pattern}")
            for type_name, count in type_counts.items():
                print(f"   - {type_name}: {count} keys")
            inconsistencies_found = True
    
    if not inconsistencies_found:
        print("✓ No data type inconsistencies found")


def analyze_memory_usage_patterns(client):
    """Analyze memory usage patterns for inconsistencies"""
    print("\n" + "="*60)
    print("MEMORY USAGE PATTERN ANALYSIS")
    print("="*60)
    
    # Sample keys from each major pattern
    pattern_samples = defaultdict(list)
    cursor = 0
    
    while True:
        cursor, keys = client.scan(cursor, count=200)
        decoded_keys = [_ensure_str(key) for key in keys]
        for key in decoded_keys:
            if 'pattern_history:' in key:
                pattern_samples['pattern_history'].append(key)
            elif 'underlying_price:' in key:
                pattern_samples['underlying_price'].append(key)
            elif 'ticks:' in key:
                pattern_samples['ticks'].append(key)
            elif 'scanner:' in key:
                pattern_samples['scanner'].append(key)
        
        if cursor == 0 or all(len(keys) >= 5 for keys in pattern_samples.values()):
            break
    
    print("\n💾 MEMORY USAGE BY PATTERN (sampled):")
    
    for pattern_type, keys in pattern_samples.items():
        if keys:
            sample_keys = keys[:3]  # Sample first 3 keys
            total_memory = 0
            memory_usage = []
            
            for key in sample_keys:
                try:
                    mem = client.memory_usage(key)
                    if mem:
                        total_memory += mem
                        memory_usage.append((key, mem))
                except Exception as e:
                    memory_usage.append((key, f"Error: {e}"))
            
            if memory_usage:
                avg_memory = total_memory / len(memory_usage) if memory_usage else 0
                print(f"\n  {pattern_type}:")
                print(f"    Average memory: {avg_memory/1024:.2f} KB per key")
                for key, mem in memory_usage[:2]:  # Show first 2 samples
                    if isinstance(mem, int):
                        print(f"    - {key}: {mem/1024:.2f} KB")
                    else:
                        print(f"    - {key}: {mem}")


def analyze_ttl_inconsistencies(client):
    """Check for inconsistent TTL settings"""
    print("\n" + "="*60)
    print("TTL INCONSISTENCY ANALYSIS")
    print("="*60)
    
    pattern_ttls = defaultdict(list)
    cursor = 0
    
    # Sample TTLs for different patterns
    while True:
        cursor, keys = client.scan(cursor, count=200)
        decoded_keys = [_ensure_str(key) for key in keys]
        
        pipeline = client.pipeline()
        for key in decoded_keys:
            pipeline.ttl(key)
        ttls = pipeline.execute()
        
        for key, ttl in zip(decoded_keys, ttls):
            if 'pattern_history:' in key:
                pattern_ttls['pattern_history'].append(ttl)
            elif 'underlying_price:' in key:
                pattern_ttls['underlying_price'].append(ttl)
            elif 'ticks:' in key:
                pattern_ttls['ticks'].append(ttl)
            elif 'scanner:' in key:
                pattern_ttls['scanner'].append(ttl)
        
        if cursor == 0:
            break
    
    print("\n⏰ TTL ANALYSIS BY PATTERN:")
    
    for pattern, ttls in pattern_ttls.items():
        if ttls:
            persistent = ttls.count(-1)
            expiring = len(ttls) - persistent
            unique_ttls = set(ttls)
            
            print(f"\n  {pattern}:")
            print(f"    Total keys: {len(ttls)}")
            print(f"    Persistent keys: {persistent}")
            print(f"    Expiring keys: {expiring}")
            
            if len(unique_ttls) > 2:  # More than just -1 and one other value
                print(f"    ⚠️  Multiple TTL values: {sorted(list(unique_ttls))[:5]}...")


def check_list_length_consistency(client):
    """Check if lists have consistent lengths within patterns"""
    print("\n" + "="*60)
    print("LIST LENGTH CONSISTENCY ANALYSIS")
    print("="*60)
    
    list_patterns = defaultdict(list)
    cursor = 0
    
    # Find all lists and their lengths
    while True:
        cursor, keys = client.scan(cursor, count=200)
        decoded_keys = [_ensure_str(key) for key in keys]
        
        type_pipeline = client.pipeline()
        for key in decoded_keys:
            type_pipeline.type(key)
        types = type_pipeline.execute()
        
        list_keys = [key for key, key_type in zip(decoded_keys, types) if key_type == 'list']
        if list_keys:
            len_pipeline = client.pipeline()
            for key in list_keys:
                len_pipeline.llen(key)
            lengths = len_pipeline.execute()
            for key, length in zip(list_keys, lengths):
                if 'pattern_history:' in key:
                    list_patterns['pattern_history'].append((key, length))
        
        if cursor == 0:
            break
    
    print("\n📏 LIST LENGTH ANALYSIS:")
    
    for pattern, lists in list_patterns.items():
        if lists:
            lengths = [length for _, length in lists]
            avg_length = sum(lengths) / len(lengths)
            min_length = min(lengths)
            max_length = max(lengths)
            
            print(f"\n  {pattern}:")
            print(f"    Total lists: {len(lists)}")
            print(f"    Length range: {min_length} - {max_length}")
            print(f"    Average length: {avg_length:.1f}")
            
            if max_length - min_length > avg_length * 0.5:  # Significant variation
                print("    ⚠️  Significant length variation detected!")
                
                # Show examples of shortest and longest lists
                lists.sort(key=lambda x: x[1])
                print(f"    Shortest: {lists[0][0]} = {lists[0][1]}")
                print(f"    Longest: {lists[-1][0]} = {lists[-1][1]}")


def generate_code_review_recommendations():
    """Generate specific recommendations for code review"""
    print("\n" + "="*60)
    print("CODE REVIEW RECOMMENDATIONS")
    print("="*60)
    
    recommendations = [
        {
            "area": "pattern_history keys",
            "checks": [
                "Verify all pattern_history lists are created with consistent max lengths",
                "Check if list trimming logic is consistent across different pattern types",
                "Ensure all pattern_history keys use the same data structure (lists)",
                "Verify expiration policies are consistently applied"
            ]
        },
        {
            "area": "underlying_price keys", 
            "checks": [
                "Confirm all underlying_price keys use string data type consistently",
                "Check if TTL/expiration is handled uniformly across all stock symbols",
                "Verify the key format includes consistent segments after stock symbol",
                "Ensure price update logic handles all symbols the same way"
            ]
        },
        {
            "area": "ticks:stream keys",
            "checks": [
                "Verify stream data structure usage is intentional and consistent",
                "Check if stream consumption patterns are properly implemented",
                "Ensure stream trimming policies are configured"
            ]
        },
        {
            "area": "Key naming conventions",
            "checks": [
                "Audit code for consistent key pattern generation",
                "Verify no hardcoded key patterns that bypass naming conventions",
                "Check for mixed case inconsistencies in key names",
                "Ensure separator usage (':') is consistent across all modules"
            ]
        }
    ]
    
    for rec in recommendations:
        print(f"\n🔍 {rec['area'].upper()}:")
        for check in rec['checks']:
            print(f"   • {check}")


def run_consistency_audit_for_db(client, db_num):
    """Run all consistency audit analyses for a specific database"""
    print("\n" + "="*80)
    print(f"REDIS STORAGE CONSISTENCY AUDIT - DB {db_num}")
    print("="*80)
    
    # Check if database has keys
    try:
        info = client.info('keyspace')
        if info and f'db{db_num}' in info:
            keys_count = info[f'db{db_num}']['keys']
            if keys_count == 0:
                print(f"⏭️  DB {db_num}: No keys found, skipping...")
                return False
            print(f"📊 DB {db_num}: {keys_count} keys found")
        else:
            print(f"⏭️  DB {db_num}: No keys found, skipping...")
            return False
    except Exception as e:
        print(f"❌ DB {db_num}: Error checking keyspace - {e}")
        return False
    
    # Run all analyses
    try:
        analyze_key_structure_inconsistencies(client)
        analyze_data_type_inconsistencies(client) 
        analyze_memory_usage_patterns(client)
        analyze_ttl_inconsistencies(client)
        check_list_length_consistency(client)
        return True
    except Exception as e:
        print(f"❌ DB {db_num}: Error during audit - {e}")
        return False


# ============================================================================
# MODE 3: Codebase Pattern Matcher (from redis_audit3.py)
# ============================================================================

def find_redis_patterns_in_codebase(code_path):
    """Search codebase for Redis key patterns"""

    redis_patterns = {
        'pattern_history': [
            r'pattern_history.*high',
            r'pattern_history.*low',
            r'pattern_history.*last_price',
            r'pattern_history.*bucket_incremental_volume',
            r'[\"\']pattern_history:[^\"\']*[\"\']',
        ],
        'underlying_price': [
            r'underlying_price:[^\"\']*',
            r'[\"\']underlying_price:[^\"\']*[\"\']',
        ],
        'ticks_stream': [
            r'ticks:stream',
            r'[\"\']ticks:stream[^\"\']*[\"\']',
        ],
        'scanner_performance': [
            r'scanner:performance',
            r'[\"\']scanner:performance[^\"\']*[\"\']',
        ],
    }

    print(f"🔍 SEARCHING CODEBASE ({code_path}) FOR REDIS PATTERNS...\n")

    for file_path in glob.glob(os.path.join(code_path, "**", "*.py"), recursive=True):
        # Skip virtual environments and node_modules
        if '.venv' in file_path or 'node_modules' in file_path or '__pycache__' in file_path:
            continue
            
        try:
            with open(file_path, "r", encoding="utf-8") as handle:
                content = handle.read()

            found_patterns = []
            for category, patterns in redis_patterns.items():
                if any(re.search(pattern, content, re.IGNORECASE) for pattern in patterns):
                    found_patterns.append(category)

            if found_patterns:
                print(f"📄 {file_path}")
                print(f"   Patterns found: {', '.join(found_patterns)}")

                lines = content.splitlines()
                for idx, line in enumerate(lines, start=1):
                    for patterns in redis_patterns.values():
                        if any(re.search(pattern, line, re.IGNORECASE) for pattern in patterns):
                            print(f"   Line {idx}: {line.strip()[:100]}...")
                            break
                print()
        except Exception as exc:
            print(f"   Error reading {file_path}: {exc}")


# ============================================================================
# Database Discovery & Main Functions
# ============================================================================

def discover_databases(args):
    """
    Return a sorted list of databases that currently contain keys.
    Falls back to [0] if Redis doesn't report keyspace info or when discovery fails.
    """
    if args.db is not None:
        return [args.db]
    if getattr(args, "all_dbs", False):
        return list(range(0, 16))  # Reasonable default span
    try:
        client = connect_redis(
            db=0,
            host=args.host,
            port=args.port,
            password=args.password,
            quiet=True,
        )
        info = client.info("keyspace")
        dbs = sorted(int(k[2:]) for k in info.keys() if k.startswith("db"))
    except Exception as exc:
        print(f"✗ Error discovering databases: {exc}")
        dbs = [1]
    finally:
        try:
            client.close()
        except Exception:
            pass
    return dbs


def merge_namespace_data(dest, src):
    """Merge namespace data dictionaries (counts, memory samples, types)."""
    for prefix, data in src.items():
        dest[prefix]['count'] += data['count']
        dest[prefix]['types'].update(data['types'])
        dest[prefix]['memory_samples'].extend(data['memory_samples'])


# ============================================================================
# Main CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Unified Redis Audit Tool - Keyspace, Consistency, and Codebase Analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Keyspace audit (default mode)
  python redis_audit.py --mode keyspace --pattern "ticks:*"
  
  # Storage consistency audit
  python redis_audit.py --mode consistency --db 1
  
  # Codebase pattern search
  python redis_audit.py --mode codebase --path .
  
  # All modes combined
  python redis_audit.py --mode all --db 1
        """
    )
    
    # Mode selection
    parser.add_argument(
        '--mode',
        choices=['keyspace', 'consistency', 'codebase', 'all'],
        default='keyspace',
        help='Audit mode: keyspace (namespace analysis), consistency (storage checks), codebase (pattern search), all (run all modes)'
    )
    
    # Common connection options
    parser.add_argument('--host', default=os.getenv('REDIS_HOST', 'localhost'),
                       help='Redis host (default: env REDIS_HOST or localhost)')
    parser.add_argument('--port', type=int, default=int(os.getenv('REDIS_PORT', 6379)),
                       help='Redis port (default: env REDIS_PORT or 6379)')
    parser.add_argument('--password', default=os.getenv('REDIS_PASSWORD'),
                       help='Redis password (default: env REDIS_PASSWORD)')
    parser.add_argument('--db', type=int, default=None,
                       help='Specific Redis database to audit (default: auto-discover)')
    parser.add_argument('--all-dbs', action='store_true',
                       help='Force auditing of databases 0-15')
    
    # Keyspace audit options
    parser.add_argument('--pattern', default='*', help='SCAN MATCH pattern (default: *)')
    parser.add_argument('--separator', default=':', help='Namespace separator (default: :)')
    parser.add_argument('--limit', type=int, help='Limit number of keys to scan')
    parser.add_argument('--scan-count', type=int, default=1000, 
                       help='SCAN COUNT option (default: 1000)')
    parser.add_argument('--enable-memory-sampling', action='store_true',
                       help='Enable memory usage sampling (slower)')
    parser.add_argument('--sample-size', type=int, default=50,
                       help='Number of keys to sample per namespace for memory (default: 50)')
    parser.add_argument('--top', type=int, default=20,
                       help='Show top N namespaces by memory (default: 20)')
    
    # Codebase search options
    parser.add_argument('--path', type=str, default=None,
                       help='Path to codebase root for codebase mode (default: current directory)')
    
    args = parser.parse_args()
    
    # Determine which databases to audit (for Redis modes)
    if args.mode in ['keyspace', 'consistency', 'all']:
        db_list = discover_databases(args)
        if not db_list:
            print("No databases found, exiting")
            sys.exit(1)
    
    # Run selected mode(s)
    if args.mode == 'keyspace' or args.mode == 'all':
        print("="*80)
        print("MODE: KEYSPACE AUDIT")
        print("="*80)
        for db_number in db_list:
            client = connect_redis(
                db=db_number,
                host=args.host,
                port=args.port,
                password=args.password,
            )
            try:
                total_keys, namespace_data = audit_keyspace(client, args)
                generate_keyspace_report(total_keys, namespace_data, args, db_label=f"DB {db_number}")
            finally:
                try:
                    client.close()
                except Exception:
                    pass
    
    if args.mode == 'consistency' or args.mode == 'all':
        print("\n" + "="*80)
        print("MODE: STORAGE CONSISTENCY AUDIT")
        print("="*80)
        successful_audits = 0
        failed_audits = 0
        
        for db_num in db_list:
            try:
                client = connect_redis(args.host, args.port, args.password, db_num)
                if run_consistency_audit_for_db(client, db_num):
                    successful_audits += 1
                else:
                    failed_audits += 1
            except Exception as e:
                print(f"\n❌ DB {db_num}: Connection failed - {e}")
                failed_audits += 1
        
        # Generate recommendations once at the end
        if successful_audits > 0:
            generate_code_review_recommendations()
        
        print("\n" + "="*80)
        print("✅ CONSISTENCY AUDIT COMPLETE")
        print("="*80)
        print(f"Successfully audited: {successful_audits} database(s)")
        print(f"Skipped/Failed: {failed_audits} database(s)")
    
    if args.mode == 'codebase' or args.mode == 'all':
        print("\n" + "="*80)
        print("MODE: CODEBASE PATTERN SEARCH")
        print("="*80)
        code_path = args.path if args.path else str(Path(__file__).resolve().parent.parent.parent)
        find_redis_patterns_in_codebase(code_path)
    
    print("\n" + "="*80)
    print("✅ AUDIT COMPLETE")
    print("="*80)


if __name__ == '__main__':
    main()

