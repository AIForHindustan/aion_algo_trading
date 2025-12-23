#!/usr/bin/env python3
"""
Merge Enriched Metadata into Canonical Instruments YAML
========================================================

Purpose:
--------
Merges instrument metadata from:
1. enriched_metadata_binary_crawler.json (Zerodha instruments from intraday crawler)
2. angel_one_intraday_enriched_token.json (Angel One matching instruments)

Into:
- canonical_instruments.yaml (canonical format for auto executor)

This ensures the auto executor has the latest instrument tokens and metadata
from both brokers for signal-based order placement.

Usage:
------
python shared_core/instrument_token/merge_enriched_to_canonical.py
"""

import json
import yaml
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Path definitions
SCRIPT_DIR = Path(__file__).parent  # shared_core/instrument_token/
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # aion_algo_trading/

# Input files
ZERODHA_ENRICHED = PROJECT_ROOT / "zerodha" / "crawlers" / "binary_crawler1" / "enriched_metadata_binary_crawler.json"
ANGEL_ONE_ENRICHED = PROJECT_ROOT / "shared_core" / "intraday" / "broker_instrument_enriched_tokens" / "angel_one_intraday_enriched_token.json"
CANONICAL_YAML = SCRIPT_DIR / "canonical_instruments.yaml"

# Output file (same as input, updated)
OUTPUT_YAML = CANONICAL_YAML


def load_zerodha_enriched() -> Dict[str, Any]:
    """Load Zerodha enriched metadata"""
    if not ZERODHA_ENRICHED.exists():
        logger.error(f"❌ Zerodha enriched file not found: {ZERODHA_ENRICHED}")
        sys.exit(1)
    
    try:
        with open(ZERODHA_ENRICHED, 'r', encoding='utf-8') as f:
            data = json.load(f)
        instruments = data.get("instruments", [])
        logger.info(f"✅ Loaded {len(instruments)} Zerodha instruments from {ZERODHA_ENRICHED.name}")
        return {str(inst.get("instrument_token")): inst for inst in instruments}
    except Exception as e:
        logger.error(f"❌ Failed to load Zerodha enriched file: {e}")
        sys.exit(1)


def load_angel_one_enriched() -> Dict[str, Any]:
    """Load Angel One enriched metadata"""
    if not ANGEL_ONE_ENRICHED.exists():
        logger.warning(f"⚠️  Angel One enriched file not found: {ANGEL_ONE_ENRICHED}")
        return {}
    
    try:
        with open(ANGEL_ONE_ENRICHED, 'r', encoding='utf-8') as f:
            data = json.load(f)
        instruments = data.get("instruments", [])
        logger.info(f"✅ Loaded {len(instruments)} Angel One instruments from {ANGEL_ONE_ENRICHED.name}")
        # Index by token for matching, also normalize field names
        indexed = {}
        for inst in instruments:
            token = inst.get("token")
            if token:
                # Normalize field names to match expected format
                normalized = {
                    "token": str(token),
                    "symbol": inst.get("symbol", ""),
                    "name": inst.get("name", ""),
                    "exchseg": inst.get("exch_seg", inst.get("exchseg", "NFO")),
                    "lot_size": int(float(inst.get("lotsize", 0))) if inst.get("lotsize") else None,
                    "tick_size": float(inst.get("tick_size", 0)) if inst.get("tick_size") else None,
                    "expiry": inst.get("expiry", ""),
                    "strike": float(inst.get("strike", 0)) / 100 if inst.get("strike") else None,
                    "instrument_type": inst.get("instrumenttype", ""),
                }
                indexed[token] = normalized
        return indexed
    except Exception as e:
        logger.warning(f"⚠️  Failed to load Angel One enriched file: {e}")
        return {}


def load_canonical_yaml() -> Dict[str, Any]:
    """Load existing canonical instruments YAML"""
    if not CANONICAL_YAML.exists():
        logger.warning(f"⚠️  Canonical YAML not found, creating new file: {CANONICAL_YAML}")
        return {
            "version": "1.0",
            "last_updated": datetime.now().isoformat() + "Z",
            "instruments": {}
        }
    
    try:
        with open(CANONICAL_YAML, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f) or {}
        instruments = data.get("instruments", {})
        logger.info(f"✅ Loaded {len(instruments)} existing instruments from {CANONICAL_YAML.name}")
        return data
    except Exception as e:
        logger.error(f"❌ Failed to load canonical YAML: {e}")
        sys.exit(1)


def generate_canonical_id(inst: Dict[str, Any], exchange: str = "NFO") -> str:
    """Generate canonical ID from instrument metadata"""
    tradingsymbol = inst.get("tradingsymbol", "").upper()
    instrument_type = inst.get("instrument_type", "")
    
    # For options/futures, use exchange prefix
    if instrument_type in ["CE", "PE", "FUT"]:
        return f"{exchange}:{tradingsymbol}"
    
    # For equity, use NSE/BSE prefix
    if exchange in ["NSE", "BSE"]:
        return f"{exchange}:{tradingsymbol}"
    
    return tradingsymbol


def merge_instrument_into_canonical(
    canonical_id: str,
    zerodha_inst: Dict[str, Any],
    angel_one_inst: Optional[Dict[str, Any]],
    existing_entry: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """Merge instrument data into canonical format"""
    
    # Start with existing entry or create new
    entry = existing_entry.copy() if existing_entry else {}
    
    # Update canonical fields
    entry["canonical_name"] = zerodha_inst.get("name", zerodha_inst.get("tradingsymbol", ""))
    entry["instrument_type"] = zerodha_inst.get("instrument_type", "")
    
    # Update lot_size and tick_size from Zerodha (most reliable)
    if "lot_size" in zerodha_inst:
        entry["lot_size"] = zerodha_inst["lot_size"]
    if "tick_size" in zerodha_inst:
        entry["tick_size"] = zerodha_inst["tick_size"]
    
    # Update expiry and strike for options/futures
    if "expiry" in zerodha_inst:
        entry["expiry"] = zerodha_inst["expiry"]
    if "strike" in zerodha_inst and zerodha_inst.get("strike", 0) > 0:
        entry["strike"] = float(zerodha_inst["strike"])
    if zerodha_inst.get("instrument_type") in ["CE", "PE"]:
        entry["option_type"] = zerodha_inst["instrument_type"]
        entry["underlying"] = f"NFO:{zerodha_inst.get('name', 'NIFTY')}"
    
    # Initialize broker_mappings if not exists
    if "broker_mappings" not in entry:
        entry["broker_mappings"] = {}
    
    # Update Zerodha mapping
    entry["broker_mappings"]["ZERODHA"] = {
        "tradingsymbol": zerodha_inst.get("tradingsymbol", ""),
        "instrument_token": str(zerodha_inst.get("instrument_token", "")),
        "exchange": zerodha_inst.get("exchange", "NFO"),
    }
    if "exchange_token" in zerodha_inst:
        entry["broker_mappings"]["ZERODHA"]["exchange_token"] = str(zerodha_inst["exchange_token"])
    
    # Update Angel One mapping if available
    if angel_one_inst:
        entry["broker_mappings"]["ANGEL_ONE"] = {
            "tradingsymbol": angel_one_inst.get("symbol", ""),
            "symboltoken": str(angel_one_inst.get("token", "")),
            "exchseg": angel_one_inst.get("exchseg", "NFO"),
        }
    elif "ANGEL_ONE" in entry.get("broker_mappings", {}):
        # Keep existing Angel One mapping if no new data
        logger.debug(f"Keeping existing Angel One mapping for {canonical_id}")
    
    return entry


def match_angel_one_to_zerodha(
    zerodha_inst: Dict[str, Any],
    angel_one_index: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Match Angel One instrument to Zerodha instrument"""
    zerodha_symbol = zerodha_inst.get("tradingsymbol", "").upper()
    
    # Try direct symbol match first
    for token, angel_inst in angel_one_index.items():
        angel_symbol = angel_inst.get("symbol", "").upper()
        if angel_symbol == zerodha_symbol:
            return angel_inst
    
    # Try symbol conversion (Zerodha format -> Angel One format)
    try:
        from shared_core.instrument_token.instrument_registry import EnhancedInstrumentService
        angel_format_symbol = EnhancedInstrumentService.convert_zerodha_to_angel_symbol(zerodha_symbol)
        
        # Look up by converted symbol
        for token, angel_inst in angel_one_index.items():
            angel_symbol = angel_inst.get("symbol", "").upper()
            if angel_symbol == angel_format_symbol:
                return angel_inst
    except Exception as e:
        logger.debug(f"Symbol conversion failed for {zerodha_symbol}: {e}")
    
    return None


def main():
    """Main merge function"""
    logger.info("🚀 Merging Enriched Metadata into Canonical Instruments YAML")
    logger.info("=" * 70)
    
    # Load all data sources
    logger.info("\n📋 Step 1: Loading data sources...")
    zerodha_instruments = load_zerodha_enriched()
    angel_one_instruments = load_angel_one_enriched()
    canonical_data = load_canonical_yaml()
    
    # Build lookup indices
    logger.info("\n🔍 Step 2: Building lookup indices...")
    
    # Index Angel One by symbol (both original and converted formats)
    angel_one_by_symbol = {}
    for token, angel_inst in angel_one_instruments.items():
        angel_symbol = angel_inst.get("symbol", "").upper()
        if angel_symbol:
            angel_one_by_symbol[angel_symbol] = angel_inst
        
        # Also try converting to Zerodha format for reverse lookup
        try:
            from shared_core.instrument_token.instrument_registry import EnhancedInstrumentService
            zerodha_format = EnhancedInstrumentService.convert_angel_to_zerodha_symbol(angel_symbol)
            if zerodha_format != angel_symbol:
                angel_one_by_symbol[zerodha_format] = angel_inst
        except Exception:
            pass
    
    # Process Zerodha instruments
    logger.info("\n🔄 Step 3: Merging instruments...")
    updated_count = 0
    new_count = 0
    skipped_count = 0
    
    instruments = canonical_data.get("instruments", {})
    
    for token, zerodha_inst in zerodha_instruments.items():
        tradingsymbol = zerodha_inst.get("tradingsymbol", "").upper()
        exchange = zerodha_inst.get("exchange", "NFO")
        canonical_id = generate_canonical_id(zerodha_inst, exchange)
        
        # Find matching Angel One instrument
        angel_one_inst = None
        
        # Try direct symbol match
        if tradingsymbol in angel_one_by_symbol:
            angel_one_inst = angel_one_by_symbol[tradingsymbol]
        else:
            # Try symbol conversion and match
            angel_one_inst = match_angel_one_to_zerodha(zerodha_inst, angel_one_instruments)
        
        # Get existing entry
        existing_entry = instruments.get(canonical_id)
        
        # Merge
        merged_entry = merge_instrument_into_canonical(
            canonical_id, zerodha_inst, angel_one_inst, existing_entry
        )
        
        # Update instruments dict
        if canonical_id in instruments:
            updated_count += 1
        else:
            new_count += 1
        
        instruments[canonical_id] = merged_entry
    
    canonical_data["instruments"] = instruments
    canonical_data["last_updated"] = datetime.now().isoformat() + "Z"
    
    # Save updated YAML
    logger.info("\n💾 Step 4: Saving updated canonical_instruments.yaml...")
    try:
        # Create backup
        backup_path = CANONICAL_YAML.with_suffix('.yaml.backup')
        if CANONICAL_YAML.exists():
            import shutil
            shutil.copy2(CANONICAL_YAML, backup_path)
            logger.info(f"✅ Created backup: {backup_path.name}")
        
        # Write updated YAML
        with open(OUTPUT_YAML, 'w', encoding='utf-8') as f:
            yaml.dump(canonical_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        
        logger.info(f"✅ Saved updated {OUTPUT_YAML.name}")
    except Exception as e:
        logger.error(f"❌ Failed to save YAML: {e}")
        sys.exit(1)
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("✅ MERGE COMPLETE!")
    logger.info(f"   Total instruments in YAML: {len(instruments)}")
    logger.info(f"   Updated: {updated_count}")
    logger.info(f"   New: {new_count}")
    logger.info(f"   Zerodha instruments processed: {len(zerodha_instruments)}")
    logger.info(f"   Angel One instruments matched: {sum(1 for inst in instruments.values() if 'ANGEL_ONE' in inst.get('broker_mappings', {}))}")
    logger.info(f"   File: {OUTPUT_YAML}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()

