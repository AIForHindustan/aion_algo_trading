"""
Instrument Token Module - Single Source of Truth

Centralized location for instrument token lookup and registry.
"""

from pathlib import Path
from typing import Dict, Any, Optional
import json
import logging

logger = logging.getLogger(__name__)

# Centralized token lookup file location
TOKEN_LOOKUP_FILE = Path(__file__).parent / "token_lookup_enriched.json"

def get_token_lookup_path() -> Path:
    """
    Get the path to the centralized token_lookup_enriched.json file.
    
    This is the SINGLE SOURCE OF TRUTH for all scripts.
    
    RETURNS:
        Path: Path to token_lookup_enriched.json
    """
    return TOKEN_LOOKUP_FILE

def load_token_lookup() -> Dict[str, Any]:
    """
    Load the centralized token_lookup_enriched.json file.
    
    This is the SINGLE SOURCE OF TRUTH for all scripts.
    
    RETURNS:
        dict: Token lookup dictionary (token_str -> instrument_data)
    """
    lookup_file = get_token_lookup_path()
    
    if not lookup_file.exists():
        logger.error(f"❌ Token lookup file not found: {lookup_file}")
        logger.error(f"   Run: python shared_core/instrument_token/generate_token_lookup_from_csv.py")
        return {}
    
    try:
        with open(lookup_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"✅ Loaded {len(data):,} instruments from {lookup_file.name}")
        return data
    except Exception as e:
        logger.error(f"❌ Error loading token lookup: {e}")
        return {}

# Export main classes and functions
from .instrument_registry import (
    UnifiedInstrumentRegistry,
    EnhancedInstrumentService,
    get_enhanced_instrument_service,
    get_unified_registry
)
from .zerodha_lookup import ZerodhaInstrumentLookup
from .angel_one_lookup import AngelOneInstrumentLookup

__all__ = [
    'TOKEN_LOOKUP_FILE',
    'get_token_lookup_path',
    'load_token_lookup',
    'UnifiedInstrumentRegistry',
    'EnhancedInstrumentService',
    'get_enhanced_instrument_service',
    'get_unified_registry',
    'ZerodhaInstrumentLookup',
    'AngelOneInstrumentLookup',
]
