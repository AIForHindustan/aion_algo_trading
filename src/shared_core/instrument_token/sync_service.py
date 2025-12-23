# shared_core/instrument_token/sync_service.py
import logging
from datetime import datetime
from typing import Dict

from .instrument_registry import UnifiedInstrument
from .registry_factory import get_registry
from .registry_types import RegistryType

class RegistrySynchronizer:
    """Synchronizes changes between comprehensive and focus registries"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.comprehensive_registry = get_registry(RegistryType.COMPREHENSIVE)
        self.focus_registry = get_registry(RegistryType.INTRADAY_FOCUS)
    
    async def sync_new_instruments(self):
        """Sync new instruments from comprehensive to focus registry"""
        new_instruments = self._find_new_instruments()
        
        for unified_id, instrument in new_instruments.items():
            if self._should_add_to_focus(instrument):
                self._add_to_focus_registry(unified_id, instrument)
        
        self.logger.info(f"Synced {len(new_instruments)} new instruments")
    
    def _find_new_instruments(self) -> Dict[str, UnifiedInstrument]:
        """Find instruments in comprehensive but not in focus registry"""
        new_instruments = {}
        
        for unified_id, instrument in self.comprehensive_registry._unified_instruments.items():
            if unified_id not in self.focus_registry._unified_instruments:
                # Check if it's a focus instrument
                if self._is_focus_instrument(instrument):
                    new_instruments[unified_id] = instrument
        
        return new_instruments
    
    def _is_focus_instrument(self, instrument: UnifiedInstrument) -> bool:
        """Check if instrument should be in focus registry"""
        # Check canonical symbol
        if instrument.canonical_symbol in self.focus_registry.config.focus_symbols:
            return True
        
        # Check if any broker instrument is in focus
        for broker_insts in instrument.broker_instruments.values():
            for broker_inst in broker_insts:
                clean_symbol = self._clean_symbol(broker_inst.tradingsymbol)
                if clean_symbol in self.focus_registry.config.focus_symbols:
                    return True
        
        return False
    
    def _add_to_focus_registry(self, unified_id: str, instrument: UnifiedInstrument):
        """Add instrument to focus registry"""
        # Copy all broker instruments
        for broker_name, broker_insts in instrument.broker_instruments.items():
            for broker_inst in broker_insts:
                # Filter out expired instruments
                if not broker_inst.expiry or not self._is_expired(broker_inst.expiry):
                    self.focus_registry.add_broker_instrument(unified_id, broker_inst)
    
    def export_focus_registry(self) -> Dict:
        """Export focus registry for quick loading"""
        export_data = {
            "metadata": {
                "type": "intraday_focus",
                "version": "1.0",
                "exported_at": datetime.now().isoformat(),
                "instrument_count": len(self.focus_registry._unified_instruments)
            },
            "instruments": []
        }
        
        for unified_id, instrument in self.focus_registry._unified_instruments.items():
            instrument_data = {
                "unified_id": unified_id,
                "canonical_symbol": instrument.canonical_symbol,
                "canonical_name": instrument.canonical_name,
                "broker_instruments": []
            }
            
            for broker_name, broker_insts in instrument.broker_instruments.items():
                for broker_inst in broker_insts:
                    instrument_data["broker_instruments"].append({
                        "broker": broker_name,
                        "broker_token": broker_inst.broker_token,
                        "tradingsymbol": broker_inst.tradingsymbol,
                        "exchange": broker_inst.exchange,
                        "instrument_type": broker_inst.instrument_type,
                        "segment": broker_inst.segment,
                        "lot_size": broker_inst.lot_size,
                        "tick_size": broker_inst.tick_size,
                        "expiry": broker_inst.expiry,
                        "strike": broker_inst.strike
                    })
            
            export_data["instruments"].append(instrument_data)
        
        return export_data