#!/usr/bin/env python3
"""
Create Focus Instruments JSON
=============================

Purpose:
--------
Generates focus_instruments.json containing ALL trading symbols from both Zerodha and Angel One
for the SAME instruments used by the intraday trading system.

This file is used by:
- instrument_registry.py: For INTRADAY_FOCUS registry type filtering
- Robot executors: For broker-agnostic symbol lookup
- Algo executor: To determine which instruments are in the focus set

Input Files:
------------
1. zerodha/crawlers/binary_crawler1/intraday_crawler_instruments.json
   - Contains Zerodha tokens and instrument_info with tradingsymbols
   - Source: Updated daily with current expiry contracts

2. shared_core/intraday/broker_instrument_enriched_tokens/angel_one_intraday_enriched_token.json
   - Contains Angel One instruments matching Zerodha instruments
   - Source: Generated from angel_one_instruments_master.json

Output File:
------------
shared_core/instrument_token/focus_instruments.json
- Format: {"symbols": [...], "source": {...}, "underlying_indices": [...]}
- Contains: All unique trading symbols from both brokers
- Used by: instrument_registry._load_focus_symbols()

Maintenance:
-----------
Run this script whenever intraday_crawler_instruments.json is updated:
- After expiry contracts are removed
- After new contracts are added (e.g., January weekly/monthly)
- After SENSEX or other indices are added

Command:
--------
python3 shared_core/instrument_token/create_focus_instruments.py
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Set, Dict, Any, List
from collections import Counter
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Path definitions
SCRIPT_DIR = Path(__file__).parent  # src/shared_core/instrument_token/
SRC_ROOT = SCRIPT_DIR.parent.parent  # src/
REPO_ROOT = SRC_ROOT.parent  # aion_algo_trading/

# Input files
ZERODHA_INSTRUMENTS = SRC_ROOT / "zerodha" / "crawlers" / "binary_crawler1" / "intraday_crawler_instruments.json"
ANGEL_ONE_INSTRUMENTS = SRC_ROOT / "shared_core" / "intraday" / "broker_instrument_enriched_tokens" / "angel_one_intraday_enriched_token.json"

# Output file
OUTPUT_FILE = SCRIPT_DIR / "focus_instruments.json"


def load_zerodha_instruments() -> Dict[str, Any]:
    """Load Zerodha instruments from intraday_crawler_instruments.json"""
    if not ZERODHA_INSTRUMENTS.exists():
        logger.error(f"❌ Zerodha instruments file not found: {ZERODHA_INSTRUMENTS}")
        sys.exit(1)
    
    try:
        with open(ZERODHA_INSTRUMENTS, 'r') as f:
            data = json.load(f)
        logger.info(f"✅ Loaded Zerodha instruments from: {ZERODHA_INSTRUMENTS.name}")
        return data
    except Exception as e:
        logger.error(f"❌ Failed to load Zerodha instruments: {e}")
        sys.exit(1)


def load_angel_one_instruments() -> Dict[str, Any]:
    """Load Angel One instruments from angel_one_intraday_enriched_token.json"""
    if not ANGEL_ONE_INSTRUMENTS.exists():
        logger.error(f"❌ Angel One instruments file not found: {ANGEL_ONE_INSTRUMENTS}")
        sys.exit(1)
    
    try:
        with open(ANGEL_ONE_INSTRUMENTS, 'r') as f:
            data = json.load(f)
        logger.info(f"✅ Loaded Angel One instruments from: {ANGEL_ONE_INSTRUMENTS.name}")
        return data
    except Exception as e:
        logger.error(f"❌ Failed to load Angel One instruments: {e}")
        sys.exit(1)


def extract_zerodha_symbols(zerodha_data: Dict[str, Any]) -> Set[str]:
    """Extract all trading symbols from Zerodha instrument_info"""
    symbols = set()
    instrument_info = zerodha_data.get("instrument_info", {})
    
    for token_str, info in instrument_info.items():
        # Try multiple field names for symbol
        symbol = (
            info.get("tradingsymbol") or
            info.get("symbol") or
            info.get("name", "")
        )
        if symbol and symbol.strip():
            symbols.add(symbol.strip())
    
    logger.info(f"📊 Extracted {len(symbols)} Zerodha symbols")
    return symbols


def extract_angel_one_symbols(angel_one_data: Dict[str, Any]) -> Set[str]:
    """Extract all trading symbols from Angel One instruments list"""
    symbols = set()
    instruments = angel_one_data.get("instruments", [])
    
    for inst in instruments:
        # Try multiple field names for symbol
        symbol = (
            inst.get("symbol") or
            inst.get("tradingsymbol") or
            inst.get("name", "")
        )
        if symbol and symbol.strip():
            symbols.add(symbol.strip())
    
    logger.info(f"📊 Extracted {len(symbols)} Angel One symbols")
    return symbols


def count_by_type(symbols: Set[str]) -> Dict[str, int]:
    """Count symbols by instrument type (FUT, CE, PE)"""
    counts = Counter()
    
    for symbol in symbols:
        symbol_upper = symbol.upper()
        if symbol_upper.endswith("FUT"):
            counts["FUT"] += 1
        elif "CE" in symbol_upper:
            counts["CE"] += 1
        elif "PE" in symbol_upper:
            counts["PE"] += 1
    
    return dict(counts)


def identify_underlying_indices(symbols: Set[str]) -> List[str]:
    """Identify underlying indices from symbols"""
    indices = set()
    
    for symbol in symbols:
        symbol_upper = symbol.upper()
        if "NIFTY" in symbol_upper and "BANKNIFTY" not in symbol_upper:
            indices.add("NIFTY")
        elif "BANKNIFTY" in symbol_upper:
            indices.add("BANKNIFTY")
        elif "SENSEX" in symbol_upper:
            indices.add("SENSEX")
        elif "FINNIFTY" in symbol_upper:
            indices.add("FINNIFTY")
        elif "MIDCPNIFTY" in symbol_upper:
            indices.add("MIDCPNIFTY")
    
    return sorted(list(indices))


def create_focus_instruments(
    zerodha_symbols: Set[str],
    angel_one_symbols: Set[str],
    zerodha_data: Dict[str, Any],
    angel_one_data: Dict[str, Any]
) -> Dict[str, Any]:
    """Create focus_instruments.json structure"""
    
    # Combine all symbols (union - removes duplicates)
    all_symbols = zerodha_symbols | angel_one_symbols
    sorted_symbols = sorted(list(all_symbols))
    
    # Count by type for each broker
    zerodha_by_type = count_by_type(zerodha_symbols)
    angel_one_by_type = count_by_type(angel_one_symbols)
    
    # Identify underlying indices
    underlying_indices = identify_underlying_indices(all_symbols)
    
    # Create output structure
    focus_data = {
        "symbols": sorted_symbols,
        "total_symbols": len(sorted_symbols),
        "source": {
            "zerodha": {
                "file": "intraday_crawler_instruments.json",
                "total": len(zerodha_symbols),
                "by_type": zerodha_by_type
            },
            "angel_one": {
                "file": "angel_one_intraday_enriched_token.json",
                "total": len(angel_one_symbols),
                "by_type": angel_one_by_type
            }
        },
        "underlying_indices": underlying_indices,
        "generated_date": datetime.now().isoformat(),
        "description": "All futures and options trading symbols for NIFTY, BANKNIFTY, and SENSEX from both Zerodha and Angel One brokers",
        "maintenance": {
            "script": "shared_core/instrument_token/create_focus_instruments.py",
            "update_frequency": "When intraday_crawler_instruments.json is updated",
            "last_updated": datetime.now().isoformat()
        }
    }
    
    logger.info(f"✅ Created focus_instruments structure:")
    logger.info(f"   Total unique symbols: {len(sorted_symbols)}")
    logger.info(f"   Zerodha symbols: {len(zerodha_symbols)}")
    logger.info(f"   Angel One symbols: {len(angel_one_symbols)}")
    logger.info(f"   Underlying indices: {underlying_indices}")
    logger.info(f"   Zerodha by type: {zerodha_by_type}")
    logger.info(f"   Angel One by type: {angel_one_by_type}")
    
    return focus_data


def save_focus_instruments(focus_data: Dict[str, Any]):
    """Save focus_instruments.json to file"""
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(focus_data, f, indent=2, ensure_ascii=False)
        
        file_size = OUTPUT_FILE.stat().st_size / 1024  # Size in KB
        logger.info(f"✅ Saved focus_instruments.json")
        logger.info(f"   File: {OUTPUT_FILE}")
        logger.info(f"   Size: {file_size:.2f} KB")
        logger.info(f"   Symbols: {len(focus_data['symbols']):,}")
    except Exception as e:
        logger.error(f"❌ Failed to save focus_instruments.json: {e}")
        raise


def main():
    """Main function"""
    logger.info("🚀 Creating Focus Instruments JSON")
    logger.info("=" * 70)
    logger.info("Purpose: All trading symbols from both Zerodha and Angel One")
    logger.info("         for the same instruments used by intraday trading system")
    logger.info("=" * 70)
    
    # Load Zerodha instruments
    logger.info("\n📋 Loading Zerodha instruments...")
    zerodha_data = load_zerodha_instruments()
    
    # Load Angel One instruments
    logger.info("\n📋 Loading Angel One instruments...")
    angel_one_data = load_angel_one_instruments()
    
    # Extract symbols
    logger.info("\n🔍 Extracting symbols...")
    zerodha_symbols = extract_zerodha_symbols(zerodha_data)
    angel_one_symbols = extract_angel_one_symbols(angel_one_data)
    
    if not zerodha_symbols:
        logger.error("❌ No Zerodha symbols found")
        sys.exit(1)
    
    if not angel_one_symbols:
        logger.error("❌ No Angel One symbols found")
        sys.exit(1)
    
    # Create focus instruments
    logger.info("\n🔧 Creating focus_instruments structure...")
    focus_data = create_focus_instruments(
        zerodha_symbols,
        angel_one_symbols,
        zerodha_data,
        angel_one_data
    )
    
    # Save to file
    logger.info("\n💾 Saving focus_instruments.json...")
    save_focus_instruments(focus_data)
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("✅ Focus Instruments Created Successfully!")
    logger.info(f"   File: {OUTPUT_FILE}")
    logger.info(f"   Total symbols: {len(focus_data['symbols']):,}")
    logger.info("=" * 70)
    logger.info("")
    logger.info("📋 Next Steps:")
    logger.info("   1. instrument_registry.py will automatically use this file")
    logger.info("   2. Robot executors can use this for broker-agnostic lookups")
    logger.info("   3. Run this script whenever intraday_crawler_instruments.json is updated")


if __name__ == "__main__":
    main()

