#!/opt/homebrew/bin/python3.13
"""
Token Lookup Debug Helper
========================

This script helps debug token lookup issues by showing what's actually available
in your token lookup and Redis data.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional
from shared_core.redis_clients.redis_client import get_redis_client
# ✅ FIXED: Use UnifiedRedisManager for centralized connection management
from shared_core.redis_clients import UnifiedRedisManager

def load_token_lookup() -> Dict:
    """Load token lookup from multiple possible paths"""
    token_lookup_paths = [
        Path("core/data/token_lookup.json"),
        Path("core/data/token_lookup_enriched.json"),
        Path("data/token_lookup.json"),
    ]
    
    for token_lookup_path in token_lookup_paths:
        if token_lookup_path.exists():
            with open(token_lookup_path, 'r') as f:
                token_lookup = json.load(f)
            print(f"✅ Loaded token lookup from: {token_lookup_path}")
            return token_lookup
    
    raise FileNotFoundError(f"Token lookup file not found. Tried: {token_lookup_paths}")

def analyze_token_lookup(token_lookup: Dict):
    """Analyze the structure of token lookup data"""
    print("\n📊 Token Lookup Analysis")
    print("=" * 50)
    
    instrument_types = {}
    exchanges = {}
    symbols_count = {}
    
    for token_str, data in token_lookup.items():
        inst_type = data.get('instrument_type', 'UNKNOWN')
        exchange = data.get('exchange', 'UNKNOWN')
        key = data.get('key', '')
        name = data.get('name', '')
        
        # Count instrument types
        instrument_types[inst_type] = instrument_types.get(inst_type, 0) + 1
        
        # Count exchanges
        exchanges[exchange] = exchanges.get(exchange, 0) + 1
        
        # Extract base symbol for counting
        if ':' in key:
            base_symbol = key.split(':', 1)[1] if ':' in key else key
        else:
            base_symbol = key
        
        # Clean symbol (remove expiry, strike, option type)
        import re
        clean_symbol = re.sub(r'\d+[A-Z]{3}\d+(CE|PE|FUT)', '', base_symbol)
        clean_symbol = re.sub(r'\d+(CE|PE|FUT)', '', clean_symbol)
        
        if clean_symbol:
            symbols_count[clean_symbol] = symbols_count.get(clean_symbol, 0) + 1
    
    print("Instrument Types:")
    for inst_type, count in sorted(instrument_types.items()):
        print(f"  {inst_type}: {count}")
    
    print("\nExchanges:")
    for exchange, count in sorted(exchanges.items()):
        print(f"  {exchange}: {count}")
    
    print("\nTop Symbols (by instrument count):")
    for symbol, count in sorted(symbols_count.items(), key=lambda x: x[1], reverse=True)[:20]:
        print(f"  {symbol}: {count}")

def search_token_lookup(token_lookup: Dict, search_term: str):
    """Search for symbols in token lookup"""
    print(f"\n🔍 Searching for: '{search_term}'")
    print("=" * 50)
    
    search_term_upper = search_term.upper()
    matches = []
    
    for token_str, data in token_lookup.items():
        key = data.get('key', '').upper()
        name = data.get('name', '').upper()
        inst_type = data.get('instrument_type', '')
        exchange = data.get('exchange', '')
        
        if (search_term_upper in key or 
            search_term_upper in name or 
            search_term_upper == str(token_str)):
            
            matches.append({
                'token': token_str,
                'key': data.get('key', ''),
                'name': data.get('name', ''),
                'instrument_type': inst_type,
                'exchange': exchange
            })
    
    if matches:
        print(f"Found {len(matches)} matches:")
        for match in matches[:10]:  # Show first 10 matches
            print(f"  Token: {match['token']}")
            print(f"  Key: {match['key']}")
            print(f"  Name: {match['name']}")
            print(f"  Type: {match['instrument_type']}, Exchange: {match['exchange']}")
            print("  " + "-" * 40)
        
        if len(matches) > 10:
            print(f"  ... and {len(matches) - 10} more matches")
    else:
        print("  No matches found")

def check_redis_keys(redis_client, pattern: str):
    """Check what keys exist in Redis matching a pattern"""
    print(f"\n🔍 Redis keys matching: '{pattern}'")
    print("=" * 50)
    
    try:
        keys = redis_client.keys(pattern)
        if keys:
            print(f"Found {len(keys)} keys:")
            for key in keys[:10]:  # Show first 10 keys
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                print(f"  {key_str}")
                
                # Try to get the value
                try:
                    data = redis_client.get(key)
                    if data:
                        data_str = data.decode('utf-8') if isinstance(data, bytes) else data
                        print(f"    Data: {data_str[:100]}...")  # First 100 chars
                except:
                    print(f"    Could not read data")
            
            if len(keys) > 10:
                print(f"  ... and {len(keys) - 10} more keys")
        else:
            print("  No keys found matching pattern")
    except Exception as e:
        print(f"  Error scanning Redis: {e}")

def test_symbol_resolution(symbol: str, token_lookup: Dict, redis_client):
    """Test how a specific symbol resolves through the system. Returns price if found, None otherwise."""
    # Test token lookup
    print("1. Token Lookup Results:")
    search_token_lookup(token_lookup, symbol)
    
    # Test Redis patterns
    print("\n2. Redis Key Patterns:")
    patterns_to_try = [
        f"*{symbol}*",
        f"index:*{symbol}*", 
        f"equity:*{symbol}*",
        f"tick:*{symbol}*",
        f"price:*{symbol}*"
    ]
    
    for pattern in patterns_to_try:
        check_redis_keys(redis_client, pattern)
    
    # Test actual price fetching
    print("\n3. Price Fetching:")
    from zerodha.crawlers.intraday_crawler.update_intraday_crawler import get_redis_spot_price
    price = get_redis_spot_price(symbol, redis_client)
    if price:
        print(f"   ✅ Price found: {price}")
    else:
        print("   ❌ No price found")
    return price

def main():
    """Main debug function"""
    print("🔧 Token Lookup Debug Helper")
    print("=" * 60)
    
    # Load token lookup
    try:
        token_lookup = load_token_lookup()
        print(f"📁 Total tokens: {len(token_lookup)}")
    except Exception as e:
        print(f"❌ Failed to load token lookup: {e}")
        return
    
    # Connect to Redis
    try:
        # ✅ FIXED: Use UnifiedRedisManager for centralized connection management
        redis_client = get_redis_client(process_name="token_debugger", db=1)
        redis_client.ping()
        print("✅ Connected to Redis DB1")
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        redis_client = None
    
    # Analyze token lookup structure
    analyze_token_lookup(token_lookup)
    
    # Test specific symbols
    test_symbols = [
        "NIFTY",
        "BANKNIFTY", 
        "RELIANCE",
        "GOLD",
        "USDINR"
    ]
    
    # Track issues
    issues = []
    price_results = {}
    
    for symbol in test_symbols:
        print(f"\n🧪 Testing symbol: '{symbol}'")
        print("=" * 50)
        price = test_symbol_resolution(symbol, token_lookup, redis_client)
        price_results[symbol] = price
        if not price:
            issues.append(f"❌ {symbol}: No price found in Redis")
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 SUMMARY OF ISSUES")
    print("=" * 60)
    if issues:
        for issue in issues:
            print(issue)
    else:
        print("✅ No issues found - all symbols have prices")
    
    print(f"\nPrice Results:")
    for symbol, price in price_results.items():
        status = "✅" if price else "❌"
        print(f"  {status} {symbol}: {price if price else 'No price found'}")

if __name__ == "__main__":
    main()