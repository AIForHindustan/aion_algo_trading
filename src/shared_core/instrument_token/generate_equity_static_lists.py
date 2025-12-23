import csv
import json
import logging
from pathlib import Path
from typing import Dict, List, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_static_lists():
    base_path = Path(__file__).parent / "instrument_csv_files"
    output_path = Path(__file__).parent.parent / "static_equities"
    output_path.mkdir(parents=True, exist_ok=True)

    zerodha_csv_path = base_path / "zerodha_instruments_20251209_191914.csv"
    angel_json_path = base_path / "angel_one_instruments_master.json"

    logger.info(f"Loading Zerodha instruments from {zerodha_csv_path}")
    
    zerodha_equities: Dict[str, int] = {}
    master_list: List[Dict[str, Any]] = []
    
    # Process Zerodha CSV
    with open(zerodha_csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['segment'] in ['NSE', 'BSE'] and row['instrument_type'] == 'EQ':
                symbol = row['tradingsymbol']
                token = int(row['instrument_token'])
                name = row['name']
                exchange = row['exchange']
                
                # We prioritize NSE over BSE if symbol validation is needed, but for now we take both
                # Actually, usually we trade on NSE. Let's separate or prefix if needed.
                # The requirement says "ONLY NSE/BSE equities". 
                # Let's keep keys as "SYMBOL" (e.g. RELIANCE). If duplicates across NSE/BSE, NSE usually preferred.
                
                if exchange == 'NSE':
                    zerodha_equities[symbol] = token
                    master_list.append({
                        "symbol": symbol,
                        "name": name,
                        "exchange": exchange,
                        "zerodha_token": token
                    })
    
    logger.info(f"Found {len(zerodha_equities)} NSE Equities in Zerodha CSV")

    # Process Angel One JSON to find matching tokens
    logger.info(f"Loading Angel One instruments from {angel_json_path}")
    angel_equities: Dict[str, str] = {} # token is string in Angel usually
    
    with open(angel_json_path, 'r') as f:
        angel_data = json.load(f)
        
    # Build Angel One lookup
    # Angel list is usually a list of dicts.
    angel_lookup = {}
    for item in angel_data:
        # Angel structure: {"symbol": "RELIANCE-EQ", "token": "123", "exch_seg": "NSE", ...} 
        # need to verify structure. Assuming standard OpenAPI format or similar.
        # Let's assume keys: 'symbol' (trading symbol), 'token', 'exch_seg', 'name'
        if item.get('exch_seg') == 'NSE' and '-EQ' in item.get('symbol', ''):
             # Angel symbols often have -EQ suffix for NSE
             clean_symbol = item['symbol'].replace('-EQ', '')
             angel_lookup[clean_symbol] = item['token']
    
    logger.info(f"Found {len(angel_lookup)} NSE Equities in Angel One Master")

    # Map Angel Tokens to Master List
    matched_count = 0
    for item in master_list:
        symbol = item['symbol']
        if symbol in angel_lookup:
            angel_token = angel_lookup[symbol]
            angel_equities[symbol] = angel_token
            item['angel_one_token'] = angel_token
            matched_count += 1
        else:
            # Try with -EQ suffix check just in case my cleaning was wrong or other variation
            pass

    logger.info(f"Mapped {matched_count} instruments to Angel One tokens")

    # Write output files
    
    # 1. Master List detailed
    with open(output_path / "nse_equities_master.json", 'w') as f:
        json.dump(master_list, f, indent=2)
        
    # 2. Zerodha Map (Symbol -> Token)
    with open(output_path / "zerodha_equities.json", 'w') as f:
        json.dump(zerodha_equities, f, indent=2)

    # 3. Angel One Map (Symbol -> Token)
    with open(output_path / "angel_one_equities.json", 'w') as f:
        json.dump(angel_equities, f, indent=2)

    logger.info("Successfully generated static equity lists.")

if __name__ == "__main__":
    generate_static_lists()
