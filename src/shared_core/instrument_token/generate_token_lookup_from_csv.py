#!/opt/homebrew/bin/python3.13
"""
Generate token_lookup_enriched.json from zerodha_instruments_latest.csv

This script creates a centralized, enriched JSON file from the CSV source.
All fields from CSV are included EXCEPT 'created_at' (which is the CSV file timestamp).

Source: shared_core/instrument_token/instrument_csv_files/zerodha_instruments_latest.csv
Output: shared_core/instrument_token/token_lookup_enriched.json

This is the SINGLE SOURCE OF TRUTH for all scripts.
"""

import csv
import json
import sys
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

def csv_to_json(csv_path: Path, output_json_path: Path) -> int:
    """
    Convert CSV to enriched JSON format
    
    ARGS:
        csv_path: Path to zerodha_instruments_latest.csv
        output_json_path: Path to output token_lookup_enriched.json
        
    RETURNS:
        int: Number of instruments processed
    """
    instruments = {}
    
    print(f"📥 Reading CSV: {csv_path}")
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            try:
                token = row.get('instrument_token')
                if not token:
                    continue
                
                # Create instrument entry with ALL fields except 'created_at'
                exchange = row.get('exchange', '')
                tradingsymbol = row.get('tradingsymbol', '')
                
                # Create 'key' field in format "EXCHANGE:TRADINGSYMBOL" (for backward compatibility)
                key_field = f"{exchange}:{tradingsymbol}" if exchange and tradingsymbol else tradingsymbol
                
                instrument = {
                    'instrument_token': int(token) if token.isdigit() else token,
                    'exchange_token': row.get('exchange_token', ''),
                    'tradingsymbol': tradingsymbol,
                    'name': row.get('name', ''),
                    'last_price': float(row.get('last_price', 0) or 0),
                    'expiry': row.get('expiry', '') if row.get('expiry') and row.get('expiry') != 'NULL' else None,
                    'strike': float(row.get('strike', 0) or 0) if row.get('strike') and row.get('strike') != 'NULL' else None,
                    'tick_size': float(row.get('tick_size', 0) or 0) if row.get('tick_size') and row.get('tick_size') != 'NULL' else None,
                    'lot_size': int(row.get('lot_size', 0) or 0) if row.get('lot_size') and row.get('lot_size') != 'NULL' else None,
                    'instrument_type': row.get('instrument_type', ''),
                    'segment': row.get('segment', ''),
                    'exchange': exchange,
                    'key': key_field,  # For backward compatibility with existing code
                    'source': 'zerodha_csv',  # Source identifier
                    # Note: 'created_at' is explicitly EXCLUDED
                }
                
                # Use token as key
                instruments[str(token)] = instrument
                
            except (ValueError, KeyError) as e:
                print(f"⚠️  Skipping row {row_num}: {e}")
                continue
    
    print(f"✅ Processed {len(instruments)} instruments")
    
    # Create output directory if needed
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write JSON file
    print(f"💾 Writing JSON: {output_json_path}")
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(instruments, f, indent=2, ensure_ascii=False)
    
    file_size = output_json_path.stat().st_size / (1024 * 1024)  # MB
    print(f"✅ Created JSON file: {file_size:.2f} MB")
    
    return len(instruments)

def main():
    """Main execution"""
    print("=" * 70)
    print("GENERATE TOKEN LOOKUP ENRICHED JSON FROM CSV")
    print("=" * 70)
    
    # Paths
    project_root = Path(__file__).resolve().parents[2]
    csv_path = project_root / "shared_core" / "instrument_token" / "instrument_csv_files" / "zerodha_instruments_latest.csv"
    output_json_path = project_root / "shared_core" / "instrument_token" / "token_lookup_enriched.json"
    
    if not csv_path.exists():
        print(f"❌ CSV file not found: {csv_path}")
        sys.exit(1)
    
    print(f"\n📂 Source CSV: {csv_path}")
    print(f"📂 Output JSON: {output_json_path}")
    print()
    
    # Convert CSV to JSON
    count = csv_to_json(csv_path, output_json_path)
    
    print("\n" + "=" * 70)
    print(f"✅ Successfully created token_lookup_enriched.json")
    print(f"   Instruments: {count:,}")
    print(f"   Location: {output_json_path}")
    print("=" * 70)
    print("\n📝 This is now the SINGLE SOURCE OF TRUTH for all scripts.")
    print("   Update all references to point to:")
    print(f"   {output_json_path.relative_to(project_root)}")

if __name__ == "__main__":
    main()

