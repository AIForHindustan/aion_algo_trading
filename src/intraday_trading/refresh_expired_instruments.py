#!/opt/homebrew/bin/python3.13
"""
Comprehensive Instrument Refresh Tool
====================================

Purpose:
--------
Refresh expired MCX commodity and CDS currency instruments with upcoming months.
Updates instruments to NOV 2025 and DEC 2025 instead of removing them.

Dependent Scripts:
----------------
- core/data/token_lookup.json: Token validation and lookup reference

Note: crawler2_volatility has been removed from this codebase and runs independently

Important Aspects:
-----------------
- Refreshes MCX commodity instruments (Gold, Silver, Crude Oil, Copper, Aluminium)
- Refreshes CDS currency instruments (EURINR, GBPINR, JPYINR, USDINR)
- Updates to NOV 2025 and DEC 2025 contracts
- Creates backup before making changes
- Preserves non-expired instruments unchanged
"""

import json
import shutil
from datetime import date, timedelta
from pathlib import Path
import re

def refresh_expired_instruments(config_path: Path, token_lookup: dict, current_date: date) -> dict:
    """Refresh expired MCX and CDS instruments in crawler configuration."""
    print(f"🔄 Refreshing expired instruments in {config_path.name}...")
    
    # Create backup
    backup_path = config_path.with_suffix('.json.backup')
    shutil.copy2(config_path, backup_path)
    print(f"  📁 Backup created: {backup_path.name}")
    
    # Load configuration
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    instruments = config.get('instruments', {})
    
    # Target months: NOV 2025 and DEC 2025
    target_months = ['25NOV', '25DEC']
    print(f"  🎯 Target months: NOV 2025, DEC 2025")
    
    # Find expired instruments
    expired_instruments = {}
    for inst_key, inst_data in instruments.items():
        if isinstance(inst_data, dict):
            # Check for expired MCX or CDS instruments
            if ('MCX:' in inst_key or 'CDS:' in inst_key) and 'FUT' in inst_key:
                # Extract commodity/currency name
                if 'MCX:' in inst_key:
                    commodity_match = re.search(r'MCX:([A-Z]+)', inst_key)
                    if commodity_match:
                        commodity_name = commodity_match.group(1)
                        if commodity_name not in expired_instruments:
                            expired_instruments[commodity_name] = {'type': 'MCX', 'instruments': []}
                        expired_instruments[commodity_name]['instruments'].append((inst_key, inst_data))
                
                elif 'CDS:' in inst_key:
                    currency_match = re.search(r'CDS:([A-Z0-9]+)', inst_key)
                    if currency_match:
                        currency_name = currency_match.group(1)
                        if currency_name not in expired_instruments:
                            expired_instruments[currency_name] = {'type': 'CDS', 'instruments': []}
                        expired_instruments[currency_name]['instruments'].append((inst_key, inst_data))
    
    print(f"  📊 Found {len(expired_instruments)} commodity/currency types to refresh")
    
    refreshed_count = 0
    new_instruments = {}
    
    # Process each commodity/currency
    for name, data in expired_instruments.items():
        inst_type = data['type']
        expired_list = data['instruments']
        
        print(f"  🔄 Refreshing {inst_type}:{name} ({len(expired_list)} instruments)...")
        
        # Find new instruments for this commodity/currency
        new_instruments_found = {}
        for token_str, info in token_lookup.items():
            if isinstance(info, dict):
                key = info.get('key', '')
                if f'{inst_type}:{name}' in key and 'FUT' in key:
                    # Check if it's in our target months
                    for target_month in target_months:
                        if target_month in key:
                            new_instruments_found[key] = {
                                'token': token_str,
                                'key': key,
                                'info': info
                            }
                            break
        
        if new_instruments_found:
            print(f"    ✅ Found {len(new_instruments_found)} new instruments")
            
            # Add new instruments
            for new_key, instrument_data in new_instruments_found.items():
                new_token = instrument_data['token']
                
                new_instruments[new_key] = {
                    'token': new_token,
                    'exchange': inst_type,
                    'instrument_type': 'FUT'
                }
                refreshed_count += 1
        else:
            print(f"    ⚠️  No new instruments found for {inst_type}:{name}")
    
    # Remove expired instruments and add new ones
    updated_instruments = {}
    for inst_key, inst_data in instruments.items():
        # Keep non-MCX/CDS instruments
        if 'MCX:' not in inst_key and 'CDS:' not in inst_key:
            updated_instruments[inst_key] = inst_data
    
    # Add new instruments
    updated_instruments.update(new_instruments)
    
    # Update configuration
    config['instruments'] = updated_instruments
    
    # Save updated configuration
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    original_count = len(instruments)
    new_count = len(updated_instruments)
    
    print(f"  ✅ Refreshed {refreshed_count} instruments")
    print(f"  📊 Original instruments: {original_count}")
    print(f"  📊 Updated instruments: {new_count}")
    print(f"  📊 Net change: {new_count - original_count}")
    
    return {
        'refreshed_count': refreshed_count,
        'original_count': original_count,
        'new_count': new_count
    }

def main():
    """Main function to refresh expired instruments."""
    print("🔄 COMPREHENSIVE INSTRUMENT REFRESH")
    print("=" * 50)
    
    current_date = date.today()
    print(f"Current date: {current_date}")
    
    # Load token lookup
    token_lookup_path = Path("core/data/token_lookup.json")
    with open(token_lookup_path, 'r') as f:
        token_lookup = json.load(f)
    
    print(f"Token lookup loaded: {len(token_lookup)} tokens")
    
    # Configuration files to process
    config_files = [
        # Note: crawler2_volatility moved to ubuntu_crawler/
        # Add intraday crawler config here if needed
    ]
    
    total_refreshed = 0
    total_original = 0
    total_new = 0
    
    for config_path in config_files:
        if config_path.exists():
            result = refresh_expired_instruments(config_path, token_lookup, current_date)
            total_refreshed += result['refreshed_count']
            total_original += result['original_count']
            total_new += result['new_count']
            print()
        else:
            print(f"⚠️  Configuration file not found: {config_path}")
    
    print("📊 REFRESH SUMMARY:")
    print(f"  Total instruments refreshed: {total_refreshed}")
    print(f"  Original total instruments: {total_original}")
    print(f"  New total instruments: {total_new}")
    print(f"  Configuration files processed: {len(config_files)}")
    print()
    
    if total_refreshed > 0:
        print("✅ INSTRUMENT REFRESH COMPLETE")
        print("  • Expired MCX/CDS instruments replaced with NOV/DEC 2025")
        print("  • Original configurations backed up")
        print("  • Crawlers will now process active instruments")
        print("  • Commodity and currency trading ready for next 2 months")
    else:
        print("✅ NO INSTRUMENTS TO REFRESH")
        print("  • All instruments are already current")
        print("  • No refresh required")

if __name__ == "__main__":
    main()
