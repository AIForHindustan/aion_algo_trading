#!/opt/homebrew/bin/python3.13
from shared_core.redis_clients.redis_client import UnifiedRedisManager
from shared_core.redis_clients.redis_client import get_redis_client
"""
Pattern Health Check Utility
============================

Checks if critical indicators are available in Redis for pattern detection.
Helps diagnose why patterns aren't triggering.

Usage:
    python redis_files/pattern_health_check.py NFOBANKNIFTY25NOV57900CE
    python redis_files/pattern_health_check.py --all-symbols
"""

import sys
import redis
import argparse
from typing import Dict, List, Tuple, Optional

# Add project root to path
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from shared_core.redis_clients.redis_key_standards import RedisKeyStandards


def check_pattern_health(symbol: str, redis_client: Optional[redis.Redis] = None) -> bool:
    """
    Check health of critical indicators for pattern detection.
    
    Args:
        symbol: Trading symbol to check
        redis_client: Optional Redis client (creates new one if not provided)
    
    Returns:
        True if all indicators are healthy, False otherwise
    """
    # ✅ SOURCE OF TRUTH: Canonicalize symbol first
    canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
    
    if redis_client is None:
        r = get_redis_client(process_name="pattern_health_check", db=1)
    else:
        r = redis_client
    
    # Critical indicators for patterns
    critical_indicators = {
        'volume_ratio': ('volume', 1.0, 'Volume activity ratio'),
        'rsi': ('ta', 50.0, 'Relative Strength Index'),
        'macd': ('ta', 0.0, 'MACD indicator'),
        'ema_20': ('ta', 0.0, '20-period EMA'),
        'delta': ('greeks', 0.5, 'Delta (option sensitivity)'),
        'gamma': ('greeks', 0.0, 'Gamma (rate of change of delta)'),
        'theta': ('greeks', 0.0, 'Theta (time decay)'),
        'vega': ('greeks', 0.0, 'Vega (volatility sensitivity)'),
        'dte_years': ('custom', 0.1, 'Days to expiry (years)'),
        'trading_dte': ('custom', 0.0, 'Trading days to expiry'),
        'atr': ('ta', 0.0, 'Average True Range'),
        'vwap': ('ta', 0.0, 'Volume Weighted Average Price')
    }
    
    print(f"\n🔍 Pattern Health Check: {symbol}")
    if canonical_symbol != symbol:
        print(f"   (Canonical: {canonical_symbol})")
    print("=" * 70)
    
    health_issues = []
    indicator_status = {}
    
    # ✅ SOURCE OF TRUTH: Get symbol variants using canonical symbol
    variants = RedisKeyStandards.get_indicator_symbol_variants(canonical_symbol)
    print(f"📋 Checking variants: {', '.join(variants[:3])}{'...' if len(variants) > 3 else ''}")
    print()
    
    for indicator, (category, default, description) in critical_indicators.items():
        value = None
        found_variant = None
        
        # ✅ SOURCE OF TRUTH: Use DatabaseAwareKeyBuilder static method for indicator keys
        from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
        
        # Try all symbol variants
        for variant in variants:
            # ✅ SOURCE OF TRUTH: Use DatabaseAwareKeyBuilder static method instead of hardcoded format
            key = DatabaseAwareKeyBuilder.live_indicator(variant, indicator, category)
            if r.exists(key):
                try:
                    raw_value = r.get(key)
                    if raw_value:
                        value = float(raw_value)
                        found_variant = variant
                        break
                except (ValueError, TypeError):
                    pass
        
        # Determine status
        if value is None:
            status = "❌ MISSING"
            health_issues.append(f"{indicator} is missing")
            indicator_status[indicator] = {'value': None, 'status': 'MISSING', 'variant': None}
        elif value == 0 and indicator == 'volume_ratio':
            status = "❌ ZERO (CRITICAL)"
            health_issues.append(f"{indicator} is zero (critical for patterns)")
            indicator_status[indicator] = {'value': 0.0, 'status': 'ZERO', 'variant': found_variant}
        elif value == 0 and indicator in ['theta', 'gamma', 'vega']:
            status = "⚠️  ZERO (may be valid for non-options)"
            indicator_status[indicator] = {'value': 0.0, 'status': 'ZERO', 'variant': found_variant}
        elif value == default and indicator in ['rsi', 'delta']:
            status = "⚠️  DEFAULT (may indicate stale data)"
            indicator_status[indicator] = {'value': value, 'status': 'DEFAULT', 'variant': found_variant}
        else:
            status = "✅ OK"
            indicator_status[indicator] = {'value': value, 'status': 'OK', 'variant': found_variant}
        
        # Format value display
        if value is None:
            value_str = 'MISSING'
        elif isinstance(value, float):
            if abs(value) < 0.0001:
                value_str = f"{value:.6f}"
            elif abs(value) < 1:
                value_str = f"{value:.4f}"
            else:
                value_str = f"{value:.2f}"
        else:
            value_str = str(value)
        
        print(f"{indicator:15} ({description[:30]:30}): {value_str:12} {status}")
        if found_variant:
            print(f"{'':15} {'':30}  └─ Found via variant: {found_variant}")
    
    # Summary
    print()
    print("=" * 70)
    if health_issues:
        print(f"🚨 HEALTH ISSUES ({len(health_issues)}):")
        for issue in health_issues:
            print(f"   • {issue}")
        print()
        print("💡 RECOMMENDATIONS:")
        if 'volume_ratio' in ' '.join(health_issues):
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            baseline_key = DatabaseAwareKeyBuilder.live_volume_baseline(canonical_symbol)
            ticks_key_pattern = f"ticks:{canonical_symbol}*"
            print(f"   • Check volume baseline: redis-cli -n 1 get '{baseline_key}'")
            print("   • Check volume manager is calculating ratios correctly")
            print(f"   • Verify ticks are being processed: redis-cli -n 1 keys '{ticks_key_pattern}'")
        if any('missing' in issue.lower() for issue in health_issues):
            print("   • Check if data pipeline is storing indicators")
            print("   • Verify symbol variants match storage keys")
            print("   • Check scanner logs for storage errors")
        return False
    else:
        print(f"✅ All indicators healthy for pattern detection")
        return True


def check_all_symbols(redis_client: Optional[redis.Redis] = None, limit: int = 10):
    """Check health for multiple symbols"""
    if redis_client is None:
        r = get_redis_client(process_name="pattern_health_check", db=1)
    else:
        r = redis_client
    
    # Find symbols with indicators (use SCAN instead of KEYS for efficiency)
    pattern = "ind:*:*:volume_ratio"
    keys = []
    cursor = 0
    while True:
        cursor, batch = r.scan(cursor, match=pattern, count=100)
        keys.extend(batch)
        if cursor == 0:
            break
    
    # Extract unique symbols
    symbols = set()
    for key in keys:
        parts = key.split(':')
        if len(parts) >= 3:
            symbols.add(parts[2])
    
    symbols = sorted(list(symbols))[:limit]
    
    print(f"\n📊 Checking {len(symbols)} symbols...")
    print("=" * 70)
    
    healthy_count = 0
    for symbol in symbols:
        is_healthy = check_pattern_health(symbol, r)
        if is_healthy:
            healthy_count += 1
        print()
    
    print("=" * 70)
    print(f"📈 SUMMARY: {healthy_count}/{len(symbols)} symbols are healthy")
    print()


def main():
    parser = argparse.ArgumentParser(description='Check pattern health for symbols')
    parser.add_argument('symbol', nargs='?', help='Symbol to check (e.g., NFOBANKNIFTY25NOV57900CE)')
    parser.add_argument('--all-symbols', action='store_true', help='Check all symbols (limited to 10)')
    parser.add_argument('--limit', type=int, default=10, help='Limit for --all-symbols (default: 10)')
    
    args = parser.parse_args()
    
    if args.all_symbols:
        check_all_symbols(limit=args.limit)
    elif args.symbol:
        check_pattern_health(args.symbol)
    else:
        # Default: check the problematic symbol from user's example
        check_pattern_health("NFOBANKNIFTY25NOV57900CE")
        print("\n💡 Usage:")
        print("   python redis_files/pattern_health_check.py <SYMBOL>")
        print("   python redis_files/pattern_health_check.py --all-symbols")


if __name__ == "__main__":
    main()
