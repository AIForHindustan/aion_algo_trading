#!/usr/bin/env python3
"""
Regenerate angel_one_intraday_enriched_token.json
==================================================

Fixes the corrupted file by:
1. Loading ONLY instruments from enriched_metadata_binary_crawler.json (258 instruments)
2. Matching them with Angel One master file
3. Filtering out wrong instruments (commodities, etc.)
4. Ensuring SENSEX instruments are properly matched (BSE/BFO exchange)
"""

import json
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
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent

# Input files
ZERODHA_ENRICHED = PROJECT_ROOT / "zerodha" / "crawlers" / "binary_crawler1" / "enriched_metadata_binary_crawler.json"
ANGEL_ONE_MASTER = PROJECT_ROOT / "shared_core" / "instrument_token" / "instrument_csv_files" / "angel_one_instruments_master.json"

# Output file
OUTPUT_FILE = SCRIPT_DIR / "angel_one_intraday_enriched_token.json"


def load_zerodha_enriched() -> List[Dict[str, Any]]:
    """Load Zerodha enriched metadata - ONLY source of truth"""
    if not ZERODHA_ENRICHED.exists():
        logger.error(f"❌ Zerodha enriched file not found: {ZERODHA_ENRICHED}")
        sys.exit(1)
    
    try:
        with open(ZERODHA_ENRICHED, 'r', encoding='utf-8') as f:
            data = json.load(f)
        instruments = data.get("instruments", [])
        logger.info(f"✅ Loaded {len(instruments)} Zerodha instruments from {ZERODHA_ENRICHED.name}")
        return instruments
    except Exception as e:
        logger.error(f"❌ Failed to load Zerodha enriched file: {e}")
        sys.exit(1)


def load_angel_one_master() -> List[Dict[str, Any]]:
    """Load Angel One master instruments"""
    if not ANGEL_ONE_MASTER.exists():
        logger.error(f"❌ Angel One master file not found: {ANGEL_ONE_MASTER}")
        sys.exit(1)
    
    try:
        with open(ANGEL_ONE_MASTER, 'r', encoding='utf-8') as f:
            data = json.load(f)
        instruments = data if isinstance(data, list) else []
        logger.info(f"✅ Loaded {len(instruments)} Angel One instruments from master file")
        return instruments
    except Exception as e:
        logger.error(f"❌ Failed to load Angel One master file: {e}")
        sys.exit(1)


def convert_zerodha_to_angel_symbol(zerodha_symbol: str, expiry_date: Optional[str] = None) -> str:
    """Convert Zerodha format to Angel One format"""
    import re
    from datetime import datetime
    
    # Zerodha formats:
    # Options: NIFTY25DEC25700CE (underlying + YYMMM + strike + CE/PE)
    # Futures: NIFTY25DECFUT (underlying + YYMMM + FUT)
    
    # Angel One formats:
    # Options: NIFTY30DEC2525700CE (underlying + DDMMMYY + strike + CE/PE)
    # Futures: NIFTY30DEC25FUT (underlying + DDMMMYY + FUT)
    
    # Pattern 1: Options with strike - NIFTY25DEC25700CE
    opt_pattern = r'^([A-Z]+)(\d{2})([A-Z]{3})(\d+)(CE|PE)$'
    opt_match = re.match(opt_pattern, zerodha_symbol)
    
    if opt_match:
        underlying, year, month, strike, option_type = opt_match.groups()
        
        # Get day from expiry_date if available
        day = "30"  # Default to last day of month
        if expiry_date:
            try:
                exp_date = datetime.strptime(expiry_date, "%Y-%m-%d")
                day = f"{exp_date.day:02d}"
            except:
                pass
        
        # Angel One format: underlying + DD + MMM + YY + strike + CE/PE
        return f"{underlying}{day}{month}{year}{strike}{option_type}"
    
    # Pattern 2: Futures - NIFTY25DECFUT
    fut_pattern = r'^([A-Z]+)(\d{2})([A-Z]{3})FUT$'
    fut_match = re.match(fut_pattern, zerodha_symbol)
    
    if fut_match:
        underlying, year, month = fut_match.groups()
        
        # Get day from expiry_date if available
        day = "30"  # Default to last day of month
        if expiry_date:
            try:
                exp_date = datetime.strptime(expiry_date, "%Y-%m-%d")
                day = f"{exp_date.day:02d}"
            except:
                pass
        
        # Angel One format: underlying + DD + MMM + YY + FUT
        return f"{underlying}{day}{month}{year}FUT"
    
    return zerodha_symbol


def find_angel_one_match(
    zerodha_inst: Dict[str, Any],
    angel_one_master: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Find matching Angel One instrument"""
    zerodha_symbol = zerodha_inst.get("tradingsymbol", "").upper()
    zerodha_name = zerodha_inst.get("name", "").upper()
    zerodha_exchange = zerodha_inst.get("exchange", "")
    zerodha_expiry = zerodha_inst.get("expiry", "")
    zerodha_strike = zerodha_inst.get("strike", 0)
    zerodha_type = zerodha_inst.get("instrument_type", "")
    
    # Convert Zerodha symbol to Angel One format (with expiry date for day extraction)
    angel_format_symbol = convert_zerodha_to_angel_symbol(zerodha_symbol, zerodha_expiry)
    
    # Build index of Angel One instruments by exchange and name
    angel_index = {}
    for ao_inst in angel_one_master:
        ao_exchange = ao_inst.get("exch_seg", "").upper()
        ao_name = ao_inst.get("name", "").upper()
        
        # Only consider matching exchange
        if zerodha_exchange == "BFO" and ao_exchange not in ["BFO", "BSE"]:
            continue
        if zerodha_exchange == "NFO" and ao_exchange != "NFO":
            continue
        
        # Only consider matching underlying
        if ao_name != zerodha_name:
            continue
        
        key = f"{ao_exchange}:{ao_name}"
        if key not in angel_index:
            angel_index[key] = []
        angel_index[key].append(ao_inst)
    
    # Search in matching exchange/name bucket
    search_key = f"{zerodha_exchange}:{zerodha_name}"
    if zerodha_exchange == "BFO":
        search_key = f"BFO:{zerodha_name}"  # Try BFO first
        if search_key not in angel_index:
            search_key = f"BSE:{zerodha_name}"  # Fallback to BSE
    
    candidates = angel_index.get(search_key, [])
    
    # Try exact symbol match (converted format)
    for ao_inst in candidates:
        ao_symbol = ao_inst.get("symbol", "").upper()
        if ao_symbol == angel_format_symbol:
            return ao_inst
    
    # Try matching by expiry, strike, and type if available
    # For options: need expiry and strike
    # For futures: need expiry only (strike is 0)
    if zerodha_expiry and (zerodha_strike or zerodha_type == "FUT"):
        for ao_inst in candidates:
            ao_expiry = ao_inst.get("expiry", "").upper()
            ao_strike = ao_inst.get("strike", "")
            ao_type = ao_inst.get("instrumenttype", "").upper()
            ao_symbol = ao_inst.get("symbol", "").upper()
            
            # Convert expiry formats for comparison
            try:
                from datetime import datetime
                zerodha_exp = datetime.strptime(zerodha_expiry, "%Y-%m-%d")
                
                # Angel One expiry format can be: DDMMMYY or DDMMMYYYY
                if ao_expiry:
                    ao_exp_date = None
                    # Try DDMMMYY format (e.g., "24DEC25")
                    if len(ao_expiry) == 7:
                        ao_day = ao_expiry[:2]
                        ao_month = ao_expiry[2:5]
                        ao_year = "20" + ao_expiry[5:7]
                        month_map = {
                            'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
                            'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
                            'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
                        }
                        if ao_month in month_map:
                            try:
                                ao_exp_date = datetime.strptime(f"{ao_year}-{month_map[ao_month]}-{ao_day}", "%Y-%m-%d")
                            except:
                                pass
                    # Try DDMMMYYYY format (e.g., "24DEC2025")
                    elif len(ao_expiry) == 9:
                        ao_day = ao_expiry[:2]
                        ao_month = ao_expiry[2:5]
                        ao_year = ao_expiry[5:9]
                        month_map = {
                            'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
                            'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
                            'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
                        }
                        if ao_month in month_map:
                            try:
                                ao_exp_date = datetime.strptime(f"{ao_year}-{month_map[ao_month]}-{ao_day}", "%Y-%m-%d")
                            except:
                                pass
                    
                    if ao_exp_date and ao_exp_date.date() == zerodha_exp.date():
                        # Expiry matches, check strike and type
                        if zerodha_type in ["CE", "PE"]:
                            if ao_type in ["OPTIDX", "OPTSTK"] and ao_strike:
                                try:
                                    # Angel One strikes are often in cents (multiply by 100)
                                    ao_strike_val = float(ao_strike)
                                    # Convert to same units as Zerodha
                                    if ao_strike_val > 10000:  # Likely in cents
                                        ao_strike_val = ao_strike_val / 100
                                    
                                    zerodha_strike_val = float(zerodha_strike)
                                    # Allow small difference (within 1.0)
                                    if abs(ao_strike_val - zerodha_strike_val) < 1.0:
                                        # Also check option type in symbol
                                        if zerodha_type == "CE" and "CE" in ao_symbol:
                                            return ao_inst
                                        elif zerodha_type == "PE" and "PE" in ao_symbol:
                                            return ao_inst
                                except:
                                    pass
                        elif zerodha_type == "FUT":
                            # For futures, just check type and expiry match
                            if ao_type in ["FUTIDX", "FUTSTK"]:
                                # Prefer exact symbol match if available
                                if "FUT" in ao_symbol:
                                    return ao_inst
                                # Otherwise return first match
                                return ao_inst
            except Exception as e:
                pass
    
    return None


def is_valid_instrument(inst: Dict[str, Any]) -> bool:
    """Check if instrument is valid (NIFTY, BANKNIFTY, or SENSEX only)"""
    name = inst.get("name", "").upper()
    return name in ["NIFTY", "BANKNIFTY", "SENSEX"]


def main():
    """Main regeneration function"""
    logger.info("🚀 Regenerating angel_one_intraday_enriched_token.json")
    logger.info("=" * 70)
    
    # Load source data
    logger.info("\n📋 Step 1: Loading source data...")
    zerodha_instruments = load_zerodha_enriched()
    angel_one_master = load_angel_one_master()
    
    # Filter to only valid instruments (NIFTY, BANKNIFTY, SENSEX)
    logger.info("\n🔍 Step 2: Filtering valid instruments...")
    valid_zerodha = [inst for inst in zerodha_instruments if is_valid_instrument(inst)]
    logger.info(f"   Valid instruments: {len(valid_zerodha)} (from {len(zerodha_instruments)} total)")
    
    # Group by underlying
    by_underlying = {}
    for inst in valid_zerodha:
        name = inst.get("name", "").upper()
        if name not in by_underlying:
            by_underlying[name] = []
        by_underlying[name].append(inst)
    
    logger.info(f"   By underlying:")
    for name, insts in sorted(by_underlying.items()):
        logger.info(f"      {name}: {len(insts)} instruments")
    
    # Match with Angel One
    logger.info("\n🔍 Step 3: Matching with Angel One instruments...")
    matched_instruments = []
    unmatched_instruments = []
    
    for zerodha_inst in valid_zerodha:
        angel_one_inst = find_angel_one_match(zerodha_inst, angel_one_master)
        
        if angel_one_inst:
            # Normalize Angel One instrument
            matched_inst = {
                "token": str(angel_one_inst.get("token", "")),
                "symbol": angel_one_inst.get("symbol", ""),
                "name": angel_one_inst.get("name", ""),
                "expiry": angel_one_inst.get("expiry", ""),
                "strike": angel_one_inst.get("strike", ""),
                "lotsize": angel_one_inst.get("lotsize", ""),
                "instrumenttype": angel_one_inst.get("instrumenttype", ""),
                "exch_seg": angel_one_inst.get("exch_seg", ""),
                "tick_size": angel_one_inst.get("tick_size", ""),
            }
            matched_instruments.append(matched_inst)
        else:
            unmatched_instruments.append(zerodha_inst)
    
    logger.info(f"   Matched: {len(matched_instruments)}")
    logger.info(f"   Unmatched: {len(unmatched_instruments)}")
    
    if unmatched_instruments:
        logger.warning(f"\n⚠️  Unmatched instruments (first 10):")
        for inst in unmatched_instruments[:10]:
            logger.warning(f"      {inst.get('tradingsymbol')} ({inst.get('name')})")
    
    # Create output structure
    logger.info("\n💾 Step 4: Creating output file...")
    output_data = {
        "crawler_id": "binary_crawler1",
        "name": "Intraday Core (Trading + Execution) - Angel One",
        "description": "Angel One instruments matching Zerodha binary_crawler1 enriched metadata",
        "purpose": "Live trading book for execution, hedge, signals - Angel One format",
        "source_zerodha_file": "enriched_metadata_binary_crawler.json",
        "source_angel_one_file": "angel_one_instruments_master.json",
        "total_zerodha_instruments": len(valid_zerodha),
        "total_angel_one_instruments": len(angel_one_master),
        "matched_instruments": len(matched_instruments),
        "unmatched_instruments": len(unmatched_instruments),
        "match_rate": f"{(len(matched_instruments)/len(valid_zerodha)*100):.1f}%",
        "matching_strategies": {
            "direct": len(matched_instruments),  # Simplified for now
            "converted_with_expiry": 0,
            "converted_without_expiry": 0
        },
        "enriched_metadata": {
            "total_instruments": len(valid_zerodha),
            "enriched_count": len(matched_instruments),
            "missing_tokens": len(unmatched_instruments),
            "enriched_at": str(ZERODHA_ENRICHED),
            "source_lookup": "angel_one_instruments_master.json",
            "created_at": datetime.now().isoformat()
        },
        "instruments": matched_instruments,
        "filtered_instruments": 0,
        "filtering_rules": {
            "NIFTY": {
                "weekly": "Every Tuesday",
                "monthly": "Last Tuesday of the month"
            },
            "BANKNIFTY": {
                "weekly": "❌ Discontinued",
                "monthly": "Last Tuesday of the month"
            },
            "SENSEX": {
                "weekly": "Every Thursday",
                "monthly": "Last Thursday of the month"
            }
        },
        "filtering_stats": {
            "before": len(valid_zerodha),
            "after": len(matched_instruments),
            "removed": len(unmatched_instruments)
        }
    }
    
    # Save output
    try:
        # Create backup if exists
        if OUTPUT_FILE.exists():
            backup_file = OUTPUT_FILE.with_suffix('.json.backup')
            import shutil
            shutil.copy2(OUTPUT_FILE, backup_file)
            logger.info(f"✅ Created backup: {backup_file.name}")
        
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ Saved: {OUTPUT_FILE.name}")
    except Exception as e:
        logger.error(f"❌ Failed to save output file: {e}")
        sys.exit(1)
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("✅ REGENERATION COMPLETE!")
    logger.info(f"   Total Zerodha instruments: {len(valid_zerodha)}")
    logger.info(f"   Matched Angel One instruments: {len(matched_instruments)}")
    logger.info(f"   Unmatched: {len(unmatched_instruments)}")
    logger.info(f"   Match rate: {(len(matched_instruments)/len(valid_zerodha)*100):.1f}%")
    logger.info(f"   File: {OUTPUT_FILE}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()

