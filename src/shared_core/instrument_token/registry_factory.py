# shared_core/instrument_token/registry_factory.py
"""
Registry Factory for creating and managing different registry types.
"""
from typing import Dict
from pathlib import Path
import logging

from .instrument_registry import (
    UnifiedInstrumentRegistry,
    RegistryType,
    ZerodhaAdapter,
    AngelOneAdapter,
    IntradayZerodhaAdapter
)

logger = logging.getLogger(__name__)

_registry_instances: Dict[RegistryType, UnifiedInstrumentRegistry] = {}

def get_registry(registry_type: RegistryType = RegistryType.COMPREHENSIVE) -> UnifiedInstrumentRegistry:
    """
    Factory method to get registry instance by type.
    
    Args:
        registry_type: Type of registry to get (COMPREHENSIVE or INTRADAY_FOCUS)
    
    Returns:
        UnifiedInstrumentRegistry instance
    """
    if registry_type not in _registry_instances:
        logger.info(f"🔧 Creating {registry_type.value} registry...")
        _registry_instances[registry_type] = UnifiedInstrumentRegistry(registry_type)
        
        # Configure brokers based on registry type
        _configure_registry_brokers(_registry_instances[registry_type], registry_type)
        
        # Build the registry
        _registry_instances[registry_type].build_registry()
        logger.info(f"✅ {registry_type.value} registry created with {_registry_instances[registry_type].stats['total_unified_instruments']} instruments")
    
    return _registry_instances[registry_type]

def _configure_registry_brokers(registry: UnifiedInstrumentRegistry, registry_type: RegistryType):
    """Configure broker adapters based on registry type"""
    _base_path = Path(__file__).parent / "instrument_csv_files"
    
    # Common adapter creation
    zerodha_csv_path = _base_path / "zerodha_instruments_latest.csv"
    zerodha_json_path = Path(__file__).parent / "zerodha_token_lookup_consolidated.json"
    angel_one_path = _base_path / "angel_one_instruments_master.json"
    angel_one_unmapped_path = _base_path / "angel_one_unmapped_instruments.json"
    
    if registry_type == RegistryType.INTRADAY_FOCUS:
        # ✅ For intraday: Use IntradayZerodhaAdapter (Single Source of Truth)
        # Path: src/zerodha/crawlers/binary_crawler1/intraday_crawler_instruments.json
        intraday_json_path = _base_path.parent.parent.parent / "zerodha" / "crawlers" / "binary_crawler1" / "intraday_crawler_instruments.json"
        
        # Ensure it exists (or rely on adapter to log error)
        zerodha_adapter = IntradayZerodhaAdapter(str(intraday_json_path))
        
        # Angel One intraday instruments (matched to crawler set)
        angel_intraday_path = _base_path.parent.parent / "intraday" / "broker_instrument_enriched_tokens" / "angel_one_intraday_enriched_token.json"
        if angel_intraday_path.exists():
            angel_one_adapter = AngelOneAdapter(str(angel_intraday_path))
        else:
            logger.warning("⚠️ Angel One intraday token file missing: %s", angel_intraday_path)
            angel_one_adapter = None
    else:
        # For comprehensive: Use full adapters
        zerodha_adapter = ZerodhaAdapter(
            str(zerodha_csv_path),
            str(zerodha_json_path) if zerodha_json_path.exists() else None
        )
        
        angel_one_adapter = AngelOneAdapter(
            str(angel_one_path),
            str(angel_one_unmapped_path) if angel_one_unmapped_path.exists() else None
        )
    
    if zerodha_adapter:
        registry.register_broker_adapter("zerodha", zerodha_adapter)
    if angel_one_adapter:
        registry.register_broker_adapter("angel_one", angel_one_adapter)

def get_comprehensive_registry() -> UnifiedInstrumentRegistry:
    """Get comprehensive registry (all instruments)"""
    return get_registry(RegistryType.COMPREHENSIVE)

def get_intraday_focus_registry() -> UnifiedInstrumentRegistry:
    """Get intraday focus registry (248 instruments)"""
    return get_registry(RegistryType.INTRADAY_FOCUS)
