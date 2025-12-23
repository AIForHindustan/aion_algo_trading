#!/opt/homebrew/bin/python3.13
"""
Redis Key Diagnostic Script
Run this to see what's actually stored vs what's expected
"""

import redis
from shared_core.redis_clients.redis_client import RedisClientFactory
from shared_core.redis_clients.redis_key_standards import get_key_builder, RedisKeyStandards, get_symbol_parser

def diagnose_redis_issues():
    db1 = RedisClientFactory.get_trading_client()
    db2 = RedisClientFactory.get_trading_client()
    
    problem_symbols = ["ADANIENT25DECFUT", "NIFTY25NOV26400PE", "SBIN25NOV880CE"]
    
    print("🔍 REDIS KEY DIAGNOSIS")
    print("=" * 60)
    
    for symbol in problem_symbols:
        print(f"\n📊 SYMBOL: {symbol}")
        print("-" * 40)
        
        # What the system expects
        print("Expected keys (based on current logic):")
        variants = RedisKeyStandards.get_indicator_symbol_variants(symbol)
        for variant in variants[:5]:  # Show first 5
            key_builder = get_key_builder()
            expected_keys = [
                key_builder.live_indicator(variant, "delta"),
                key_builder.live_indicator(variant, "gamma"), 
                key_builder.live_indicator(variant, "theta"),
                key_builder.live_indicator(variant, "price_change"),
            ]
            for key in expected_keys:
                exists_db1 = db1.exists(key)
                exists_db2 = db2.exists(key)
                status = "✅" if exists_db1 or exists_db2 else "❌"
                db_info = "DB1" if exists_db1 else "DB2" if exists_db2 else "NOWHERE"
                print(f"   {status} {key} -> {db_info}")
        
        # What actually exists
        print(f"\nActual keys in Redis for '{symbol}':")
        patterns = [f"*{symbol}*", f"*{symbol.upper()}*", f"*{symbol.lower()}*"]
        for pattern in patterns:
            try:
                keys_in_db1 = db1.keys(pattern)
                keys_in_db2 = db2.keys(pattern)
                
                if keys_in_db1:
                    print(f"   DB1 '{pattern}': {len(keys_in_db1)} keys")
                    for key in keys_in_db1[:3]:  # Show first 3
                        print(f"      - {key}")
                
                if keys_in_db2:
                    print(f"   DB2 '{pattern}': {len(keys_in_db2)} keys") 
                    for key in keys_in_db2[:3]:
                        print(f"      - {key}")
                        
            except Exception as e:
                print(f"   Error checking {pattern}: {e}")

def check_symbol_parsing():
    """Check how symbols are being parsed"""
    print("\n🎯 SYMBOL PARSING ANALYSIS")
    print("=" * 60)
    
    test_symbols = [
        "ADANIENT25DECFUT",
        "NIFTY25NOV26400PE", 
        "SBIN25NOV880CE",
        "NIFTY25DEC25650CE",
        "HDFCBANK",
        "SBIN"
    ]
    
    for symbol in test_symbols:
        print(f"\n🔍 {symbol}:")
        try:
            parser = get_symbol_parser()
            parsed = parser.parse_symbol(symbol)
            print(f"   Asset Class: {parsed.asset_class}")
            print(f"   Instrument: {parsed.instrument_type}")
            print(f"   Base Symbol: {parsed.base_symbol}")
            print(f"   Normalized: {parsed.normalized}")
            
            canonical = RedisKeyStandards.canonical_symbol(symbol)
            print(f"   Canonical: {canonical}")
            
            variants = RedisKeyStandards.get_indicator_symbol_variants(symbol)
            print(f"   Variants: {variants[:3]}...")  # First 3
                
        except Exception as e:
            print(f"   ❌ Parse error: {e}")

def verify_data_ingestion(symbol: str, redis_db1):
    """Verify that data is actually being stored"""
    print(f"🔍 VERIFYING DATA INGESTION FOR: {symbol}")
    
    key_builder = get_key_builder()
    
    # Check all expected keys
    canonical = RedisKeyStandards.canonical_symbol(symbol)
    expected_keys = [
        key_builder.live_ohlc_latest(canonical),
        key_builder.live_indicator(canonical, "rsi", "ta"),
        key_builder.live_greeks(canonical, "delta"),  # ✅ Use live_greeks() method
        key_builder.live_greeks(canonical, "gamma"),
        key_builder.live_greeks(canonical, "theta"),
        key_builder.live_greeks(canonical, "vega"),
        key_builder.live_greeks(canonical, "rho"),
        key_builder.live_indicator(canonical, "price_change", "custom"),
    ]
    print(f"   Using canonical symbol: {canonical}")
    
    print("\n📊 Expected Keys Check:")
    for key in expected_keys:
        exists = redis_db1.exists(key)
        if exists:
            ttl = redis_db1.ttl(key)
            # Check data type
            key_type = redis_db1.type(key)
            try:
                if key_type == 'hash':
                    value = redis_db1.hgetall(key)
                    value_str = str(list(value.items())[:2]) if value else '{}'
                elif key_type == 'string':
                    value = redis_db1.get(key)
                    value_str = str(value)[:50] if value else 'None'
                elif key_type == 'zset':
                    value = redis_db1.zcard(key)
                    value_str = f"zset with {value} entries"
                else:
                    value_str = f"{key_type} type"
                print(f"✅ {key} - EXISTS (TTL: {ttl}s, Type: {key_type}, Value: {value_str}...)")
            except Exception as e:
                print(f"✅ {key} - EXISTS (TTL: {ttl}s, Type: {key_type}, Error reading: {e})")
        else:
            print(f"❌ {key} - MISSING")
    
    # Check what keys actually exist for this symbol
    actual_keys = redis_db1.keys(f"*{symbol}*")
    print(f"\n📋 Actual keys for {symbol}: {len(actual_keys)}")
    for key in actual_keys[:10]:  # Show first 10
        ttl = redis_db1.ttl(key)
        print(f"   - {key} (TTL: {ttl}s)")
    
    # Also check variants
    variants = RedisKeyStandards.get_indicator_symbol_variants(symbol)
    print(f"\n🔍 Symbol Variants: {variants}")
    for variant in variants[:3]:
        variant_keys = redis_db1.keys(f"*{variant}*")
        if variant_keys:
            print(f"   {variant}: {len(variant_keys)} keys found")
            for key in variant_keys[:3]:
                print(f"      - {key}")

if __name__ == "__main__":
    diagnose_redis_issues()
    check_symbol_parsing()
    
    print("\n" + "=" * 60)
    print("🔍 DATA INGESTION VERIFICATION")
    print("=" * 60)
    
    db1 = RedisClientFactory.get_trading_client()
    
    # Test problematic symbols
    test_symbols = ["ADANIENT", "ADANIENT25DECFUT", "NSEADANIENT", "NFOADANIENT25DECFUT"]
    for symbol in test_symbols:
        verify_data_ingestion(symbol, db1)
        print()

