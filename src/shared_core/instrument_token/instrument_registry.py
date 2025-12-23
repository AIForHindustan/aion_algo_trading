# shared_core/instrument_token/instrument_registry.py
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field
import csv
import json
from enum import Enum
from datetime import datetime, timezone
import logging
from pathlib import Path
import re
import aiohttp

MONTH_ABBRS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
               "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]

WEEKLY_SYMBOL_REGEX = re.compile(
    r'^(?P<prefix>[A-Z]+?)(?P<yy>\d{2})(?P<code>[A-Z])(?P<day>\d{2})(?P<strike>\d+)(?P<option>CE|PE)$'
)

@dataclass
class BrokerInstrument:
    """Broker-specific instrument data with source tracking"""
    broker: str
    broker_token: str
    tradingsymbol: str
    name: str
    exchange: str
    instrument_type: str
    segment: str
    lot_size: int
    tick_size: float
    expiry: Optional[str] = None
    strike: Optional[float] = None
    source_file: str = ""  # Track which file this came from
    is_active: bool = True

@dataclass
class UnifiedInstrument:
    """Unified instrument representation across all brokers"""
    unified_id: str  # Format: EXCHANGE:SYMBOL:TYPE (e.g., NSE:RELIANCE:EQ)
    canonical_symbol: str  # Standardized symbol (e.g., RELIANCE)
    canonical_name: str
    sector: str = "Unknown"
    industry: str = "Unknown"
    market_cap: Optional[float] = None
    is_index: bool = False
    underlying: Optional[str] = None
    broker_instruments: Dict[str, List[BrokerInstrument]] = field(default_factory=dict)
    
    def get_broker_instrument(self, broker: str, segment: Optional[str] = None) -> Optional[BrokerInstrument]:
        """Get broker instrument, optionally filtered by segment"""
        broker_instruments = self.broker_instruments.get(broker, [])
        if not broker_instruments:
            return None
        
        if segment:
            for inst in broker_instruments:
                if inst.segment == segment:
                    return inst
            return None
        
        # Return the first active instrument
        for inst in broker_instruments:
            if inst.is_active:
                return inst
        return broker_instruments[0] if broker_instruments else None

class BrokerAdapter(ABC):
    """Abstract base class for broker-specific instrument adapters"""
    
    @abstractmethod
    def load_instruments(self) -> List[BrokerInstrument]:
        pass
    
    @abstractmethod
    def normalize_exchange(self, exchange: str) -> str:
        """Normalize exchange codes across brokers"""
        pass
    
    @abstractmethod
    def normalize_instrument_type(self, inst_type: str) -> str:
        """Normalize instrument types across brokers"""
        pass

class ZerodhaAdapter(BrokerAdapter):
    def __init__(self, csv_path: str, consolidated_json_path: Optional[str] = None):
        self.csv_path = Path(csv_path)
        self.consolidated_json_path = Path(consolidated_json_path) if consolidated_json_path else None
        self.logger = logging.getLogger(__name__)
    
    def load_instruments(self) -> List[BrokerInstrument]:
        instruments = []
        seen_keys = set()
        
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        instrument = BrokerInstrument(
                            broker="zerodha",
                            broker_token=str(row['instrument_token']),
                            tradingsymbol=row['tradingsymbol'],
                            name=row['name'],
                            exchange=self.normalize_exchange(row['exchange']),
                            instrument_type=self.normalize_instrument_type(row['instrument_type']),
                            segment=row['segment'],
                            lot_size=int(row['lot_size']) if row['lot_size'] else 1,
                            tick_size=float(row['tick_size']) if row['tick_size'] else 0.05,
                            expiry=row['expiry'] if row['expiry'] and row['expiry'] != 'NULL' else None,
                            strike=float(row['strike']) if row['strike'] and row['strike'] != 'NULL' else None,
                            source_file=self.csv_path.name
                        )
                        instruments.append(instrument)
                        seen_keys.add((instrument.exchange, instrument.tradingsymbol, instrument.instrument_type))
                    except (ValueError, KeyError) as e:
                        self.logger.warning(f"Skipping invalid Zerodha row: {row}. Error: {e}")
                        continue
            
            self.logger.info(f"Loaded {len(instruments)} instruments from Zerodha CSV")
            
            # Load consolidated JSON for additional contracts (e.g., weekly options)
            if self.consolidated_json_path and self.consolidated_json_path.exists():
                try:
                    with open(self.consolidated_json_path, 'r', encoding='utf-8') as jf:
                        data = json.load(jf)
                    json_count = 0
                    for value in data.values():
                        try:
                            tradingsymbol = value.get('tradingsymbol')
                            if not tradingsymbol:
                                continue
                            exchange = self.normalize_exchange(value.get('exchange', ''))
                            instrument_type = self.normalize_instrument_type(value.get('instrument_type', 'EQ'))
                            key = (exchange, tradingsymbol, instrument_type)
                            if key in seen_keys:
                                continue
                            instrument = BrokerInstrument(
                                broker="zerodha",
                                broker_token=str(value.get('instrument_token')),
                                tradingsymbol=tradingsymbol,
                                name=value.get('name', tradingsymbol),
                                exchange=exchange,
                                instrument_type=instrument_type,
                                segment=value.get('segment', ''),
                                lot_size=int(value.get('lot_size') or 1),
                                tick_size=float(value.get('tick_size') or 0.05),
                                expiry=value.get('expiry') or None,
                                strike=float(value.get('strike') or 0.0) or None,
                                source_file=self.consolidated_json_path.name
                            )
                            instruments.append(instrument)
                            seen_keys.add(key)
                            json_count += 1
                        except Exception as e:
                            self.logger.debug(f"Skipping invalid consolidated entry {value}: {e}")
                            continue
                    self.logger.info(f"Loaded {json_count} additional instruments from consolidated JSON")
                except Exception as e:
                    self.logger.warning(f"Failed to load consolidated Zerodha JSON: {e}")
            
            return instruments
            
        except Exception as e:
            self.logger.error(f"Failed to load Zerodha instruments: {e}")
            return []
    
    def normalize_exchange(self, exchange: str) -> str:
        """Normalize Zerodha exchange codes"""
        exchange_map = {
            'NSE': 'NSE',
            'BSE': 'BSE', 
            'NFO': 'NFO',
            'CDS': 'CDS',
            'MCX': 'MCX',
            'BFO': 'BFO',
            'NCO': 'NCO'
        }
        return exchange_map.get(exchange.upper(), exchange.upper())
    
    def normalize_instrument_type(self, inst_type: str) -> str:
        """Normalize Zerodha instrument types"""
        type_map = {
            'EQ': 'EQ',
            'FUT': 'FUT',
            'CE': 'CE',
            'PE': 'PE',
            'XX': 'INDEX'  # Assuming XX might be index
        }
        return type_map.get(inst_type.upper(), inst_type.upper())

class IntradayZerodhaAdapter(ZerodhaAdapter):
    """
    Adapter for Intraday Crawler Instruments (Single Source of Truth).
    Parses intraday_crawler_instruments.json which contains active instruments metadata.
    """
    def __init__(self, json_path: str):
        self.json_path = Path(json_path)
        self.logger = logging.getLogger(__name__)
        # calls parent init with dummy values as we override load_instruments
        super().__init__(csv_path="dummy.csv", consolidated_json_path=None)
    
    def load_instruments(self) -> List[BrokerInstrument]:
        instruments = []
        try:
            if not self.json_path.exists():
                self.logger.error(f"Intraday source file not found: {self.json_path}")
                return []
                
            with open(self.json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            instrument_info = data.get('instrument_info')
            if not instrument_info:
                # FIX: Correctly access token_types from metadata
                metadata = data.get('metadata', {})
                token_types = metadata.get('token_types')
                
                if token_types:
                    self.logger.info(f"Using 'metadata.token_types' source from {self.json_path.name}")
                    # Convert token_types format to instrument_info format
                    instrument_info = {}
                    for token_str, info in token_types.items():
                        instrument_info[token_str] = {
                            "instrument_token": token_str,
                            "tradingsymbol": info.get("name"),  # name is tradingsymbol here
                            "name": info.get("name"),
                            "exchange": info.get("exchange"),
                            "instrument_type": info.get("type"),
                            "segment": info.get("segment"),
                            "lot_size": info.get("lot_size"),
                            "tick_size": info.get("tick_size"),
                            "expiry": info.get("expiry"),
                            "strike": info.get("strike"),
                        }
                else:
                    self.logger.error(f"No 'instrument_info' or 'metadata.token_types' available in {self.json_path.name}")
                    return []
                
            allowed_tokens = {str(tok) for tok in data.get('instruments', {}).get('all_tokens', [])}
            seen_keys = set()
            count = 0
            
            for token_str, info in instrument_info.items():
                try:
                    if allowed_tokens and token_str not in allowed_tokens:
                        continue
                    tradingsymbol = info.get('tradingsymbol')
                    if not tradingsymbol:
                        continue
                        
                    exchange = self.normalize_exchange(info.get('exchange', ''))
                    # normalize_instrument_type is inherited
                    instrument_type = self.normalize_instrument_type(info.get('instrument_type', 'EQ'))
                    
                    key = (exchange, tradingsymbol, instrument_type)
                    if key in seen_keys:
                        continue
                        
                    instrument = BrokerInstrument(
                        broker="zerodha",
                        broker_token=str(info.get('instrument_token')),
                        tradingsymbol=tradingsymbol,
                        name=info.get('name', tradingsymbol),
                        exchange=exchange,
                        instrument_type=instrument_type,
                        segment=info.get('segment', ''),
                        lot_size=int(info.get('lot_size') or 1),
                        tick_size=float(info.get('tick_size') or 0.05),
                        expiry=info.get('expiry') or None,
                        strike=float(info.get('strike') or 0.0) or None,
                        source_file=self.json_path.name
                    )
                    instruments.append(instrument)
                    seen_keys.add(key)
                    count += 1
                except Exception as e:
                    self.logger.debug(f"Skipping invalid intraday entry {token_str}: {e}")
                    continue
            
            self.logger.info(f"✅ Loaded {count} instruments from Intraday Source (Truth): {self.json_path.name}")
            return instruments
            
        except Exception as e:
            self.logger.error(f"Failed to load Intraday instruments: {e}")
            return []

class AngelOneAdapter(BrokerAdapter):
    def __init__(self, json_path: str, unmapped_path: Optional[str] = None):
        self.json_path = Path(json_path)
        self.unmapped_path = Path(unmapped_path) if unmapped_path else None
        self.logger = logging.getLogger(__name__)
    
    def normalize_exchange(self, exchange: str) -> str:
        """Normalize AngelOne exchange codes with segment awareness"""
        exchange_upper = exchange.upper()
        
        exchange_map = {
            'NSE': 'NSE',   # Equity
            'BSE': 'BSE',   # Equity
            'NFO': 'NFO',   # Equity derivatives
            'NCO': 'NCO',   # Commodities
            'CDS': 'CDS',   # Currency derivatives
            'MCX': 'MCX',   # Multi Commodity Exchange
            'BFO': 'BFO',   # Currency derivatives on BSE
        }
        return exchange_map.get(exchange_upper, exchange_upper)
    
    def load_instruments(self) -> List[BrokerInstrument]:
        instruments = []
        
        try:
            # Load main instruments
            with open(self.json_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            if isinstance(raw_data, dict):
                data = raw_data.get('instruments') or raw_data.get('instrument_info') or []
            else:
                data = raw_data
            
            for item in data:
                try:
                    exch_seg = item.get('exch_seg', '').upper()
                    segment_map = {
                        'NSE': 'NSE',
                        'BSE': 'BSE',
                        'NFO': 'NFO',
                        'MCX': 'MCX',
                        'CDS': 'CDS',
                        'BCD': 'NCO',  # Angel One sometimes uses BCD for commodities
                    }
                    segment = segment_map.get(exch_seg, exch_seg) or "UNKNOWN"

                    instrument = BrokerInstrument(
                        broker="angel_one",
                        broker_token=str(item['token']),
                        tradingsymbol=item['symbol'],
                        name=item['name'],
                        exchange=self.normalize_exchange(exch_seg),
                        instrument_type=self.normalize_instrument_type(item['instrumenttype']),
                        segment=segment,
                        lot_size=int(item['lotsize']) if item['lotsize'] else 1,
                        tick_size=float(item['tick_size']) if item.get('tick_size') else 0.05,
                        expiry=item['expiry'] if item.get('expiry') else None,
                        strike=float(item['strike']) if item.get('strike') else None,
                        source_file=self.json_path.name
                    )
                    instruments.append(instrument)
                except (ValueError, KeyError) as e:
                    self.logger.warning(f"Skipping invalid AngelOne item: {item}. Error: {e}")
                    continue
            
            self.logger.info(f"Loaded {len(instruments)} instruments from AngelOne JSON")
            return instruments
            
        except Exception as e:
            self.logger.error(f"Failed to load AngelOne instruments: {e}")
            return []
    
    def normalize_instrument_type(self, inst_type: str) -> str:
        """Normalize AngelOne instrument types to match Zerodha"""
        type_map = {
            'EQ': 'EQ',
            'FUTURES': 'FUT',
            'FUT': 'FUT',
            'FUTIDX': 'FUT',
            'FUTSTK': 'FUT',
            'OPTIDX': 'CE',  # Index options
            'OPTSTK': 'CE',  # Stock options
            'OPT': 'CE',     # Generic options
            'XX': 'INDEX'
        }
        inst_value = (inst_type or "").upper()
        if not inst_value:
            inst_value = "EQ"
        normalized = type_map.get(inst_value, inst_value)
        
        # Handle PUT options - need to check symbol or additional field
        # This might need enhancement based on actual AngelOne data
        return normalized


class RegistryType(Enum):
    COMPREHENSIVE = "comprehensive"
    INTRADAY_FOCUS = "intraday_focus"


@dataclass
class RegistryConfig:
    registry_type: RegistryType
    include_expired: bool = True
    include_all_brokers: bool = True
    focus_symbols: Set[str] = field(default_factory=set)
    max_instruments: Optional[int] = None
    update_frequency: str = "daily"
    persistence_path: Optional[Path] = None


class UnifiedInstrumentRegistry:
    """Enhanced registry with conflict resolution and metadata enrichment"""
    
    def __init__(self, registry_type: RegistryType = RegistryType.COMPREHENSIVE):
        self.logger = logging.getLogger(__name__)
        self.registry_type = registry_type
        self.config = self._get_config_for_type(registry_type)
        self._unified_instruments: Dict[str, UnifiedInstrument] = {}
        # broker -> segment -> broker_token -> [unified_ids]
        self._broker_to_unified: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
        self._symbol_to_unified: Dict[str, List[str]] = {}
        self._broker_adapters: Dict[str, BrokerAdapter] = {}
        # unified_id -> broker -> segment -> [tokens]
        self._unified_to_broker: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
        # Performance sets
        self._active_instruments: Set[str] = set()
        self._focus_instruments: Set[str] = set()
        
        # Statistics
        self.stats = {
            'total_unified_instruments': 0,
            'active_instruments': 0,
            'focus_instruments': 0,
            'broker_instruments': {},
            'mapping_confidence': {}
        }
    
    def register_broker_adapter(self, broker_name: str, adapter: BrokerAdapter):
        """Register a broker adapter"""
        self._broker_adapters[broker_name] = adapter
        self.stats['broker_instruments'][broker_name] = 0
        self.stats['mapping_confidence'][broker_name] = 'high'  # default

    def _get_config_for_type(self, registry_type: RegistryType) -> RegistryConfig:
        """Get configuration based on registry type."""
        if registry_type == RegistryType.INTRADAY_FOCUS:
            focus_symbols = self._load_focus_symbols()
            return RegistryConfig(
                registry_type=registry_type,
                include_expired=False,
                include_all_brokers=True,
                focus_symbols=focus_symbols,
                max_instruments=250,
                update_frequency="real_time",
                persistence_path=Path(__file__).parent / "intraday_focus_registry.json",
            )
        return RegistryConfig(
            registry_type=registry_type,
            include_expired=True,
            include_all_brokers=True,
            update_frequency="daily",
            persistence_path=Path(__file__).parent / "comprehensive_registry.json",
        )

    def _load_focus_symbols(self) -> Set[str]:
        """
        Load focus symbols for intraday registry.
        
        Source File:
        ------------
        shared_core/instrument_token/focus_instruments.json
        
        This file contains ALL trading symbols from both Zerodha and Angel One
        for the SAME instruments used by the intraday trading system.
        
        Generated By:
        ------------
        shared_core/instrument_token/create_focus_instruments.py
        
        Update Frequency:
        ----------------
        Run create_focus_instruments.py whenever intraday_crawler_instruments.json
        is updated (e.g., after expiry contracts removed, new contracts added).
        
        File Structure:
        --------------
        {
            "symbols": [...],  # All unique trading symbols from both brokers
            "source": {
                "zerodha": {"file": "intraday_crawler_instruments.json", ...},
                "angel_one": {"file": "angel_one_intraday_enriched_token.json", ...}
            },
            "underlying_indices": ["NIFTY", "BANKNIFTY", "SENSEX"]
        }
        
        Used By:
        --------
        - INTRADAY_FOCUS registry type filtering
        - Robot executors for broker-agnostic symbol lookup
        - Algo executor to determine focus instrument set
        """
        focus_path = Path(__file__).parent / "focus_instruments.json"
        if focus_path.exists():
            try:
                with open(focus_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return set(data.get("symbols", []))
            except Exception:
                pass
        # Fallback to default symbols if file not found
        return {
            "NIFTY",
            "BANKNIFTY",
            "SENSEX",
            "FINNIFTY",
            "MIDCPNIFTY",
            "RELIANCE",
            "TCS",
            "HDFCBANK",
            "INFY",
            "ICICIBANK",
        }
    
    def build_registry(self):
        """Build registry with type-specific filtering"""
        all_broker_instruments = {}
        uses_intraday_adapter = False  # Track if IntradayZerodhaAdapter is used
        
        # Load instruments from all brokers
        for broker_name, adapter in self._broker_adapters.items():
            broker_instruments = adapter.load_instruments()
            
            # Skip filtering if using IntradayZerodhaAdapter (Source of Truth)
            if isinstance(adapter, IntradayZerodhaAdapter):
                uses_intraday_adapter = True  # Mark that we're using the Single Source of Truth
            elif self.registry_type == RegistryType.INTRADAY_FOCUS:
                broker_instruments = self._filter_for_intraday(broker_instruments)
                
            all_broker_instruments[broker_name] = broker_instruments
            self.stats['broker_instruments'][broker_name] = len(broker_instruments)
        
        # First pass: Create unified instruments by exact symbol matching
        self._create_unified_instruments(all_broker_instruments)
        
        # Second pass: Apply registry filters then resolve conflicts
        # Skip filtering if using IntradayZerodhaAdapter (it IS the source of truth)
        if not uses_intraday_adapter:
            self._apply_registry_filters()
        self._resolve_mapping_conflicts()
        
        # Third pass: Enrich with additional metadata
        self._enrich_with_canonical_data()
        
        self._calculate_statistics()
        if self.config.persistence_path:
            self._persist_registry()
        self._log_statistics()
    
    def _create_unified_instruments(self, all_broker_instruments: Dict[str, List[BrokerInstrument]]):
        """Create unified instruments with segment-aware token mapping"""
        
        for broker_name, instruments in all_broker_instruments.items():
            for broker_inst in instruments:
                unified_id = self._generate_unified_id(broker_inst)
                
                if unified_id not in self._unified_instruments:
                    self._unified_instruments[unified_id] = UnifiedInstrument(
                        unified_id=unified_id,
                        canonical_symbol=broker_inst.tradingsymbol,
                        canonical_name=broker_inst.name,
                        broker_instruments={}
                    )
                
                # Add broker instrument to unified instrument
                if broker_name not in self._unified_instruments[unified_id].broker_instruments:
                    self._unified_instruments[unified_id].broker_instruments[broker_name] = []
                
                self._unified_instruments[unified_id].broker_instruments[broker_name].append(broker_inst)
                
                # Update segment-aware indices
                # Initialize broker in mapping if not present
                if broker_name not in self._broker_to_unified:
                    self._broker_to_unified[broker_name] = {}
                
                # Initialize segment in broker mapping if not present
                if broker_inst.segment not in self._broker_to_unified[broker_name]:
                    self._broker_to_unified[broker_name][broker_inst.segment] = {}
                
                # Map broker token (within segment) to unified ID
                if broker_inst.broker_token not in self._broker_to_unified[broker_name][broker_inst.segment]:
                    self._broker_to_unified[broker_name][broker_inst.segment][broker_inst.broker_token] = []
                
                if unified_id not in self._broker_to_unified[broker_name][broker_inst.segment][broker_inst.broker_token]:
                    self._broker_to_unified[broker_name][broker_inst.segment][broker_inst.broker_token].append(unified_id)
                
                # Update reverse mapping
                if unified_id not in self._unified_to_broker:
                    self._unified_to_broker[unified_id] = {}
                if broker_name not in self._unified_to_broker[unified_id]:
                    self._unified_to_broker[unified_id][broker_name] = {}
                if broker_inst.segment not in self._unified_to_broker[unified_id][broker_name]:
                    self._unified_to_broker[unified_id][broker_name][broker_inst.segment] = []
                
                if broker_inst.broker_token not in self._unified_to_broker[unified_id][broker_name][broker_inst.segment]:
                    self._unified_to_broker[unified_id][broker_name][broker_inst.segment].append(broker_inst.broker_token)
                
                # Symbol index
                symbol_key = broker_inst.tradingsymbol.upper()
                self._add_symbol_index(symbol_key, unified_id)
                
                normalized_key = self._normalize_symbol_key(symbol_key, broker_inst.instrument_type)
                if normalized_key != symbol_key:
                    self._add_symbol_index(normalized_key, unified_id)

    def _filter_for_intraday(self, instruments: List[BrokerInstrument]) -> List[BrokerInstrument]:
        """Filter instruments for intraday focus registry."""
        filtered: List[BrokerInstrument] = []
        for inst in instruments:
            if inst.expiry and not self.config.include_expired and self._is_expired(inst.expiry):
                continue
            clean_symbol = self._clean_symbol(inst.tradingsymbol)
            if clean_symbol in self.config.focus_symbols:
                filtered.append(inst)
                self._focus_instruments.add(inst.tradingsymbol)
        return filtered

    def _apply_registry_filters(self):
        """Apply registry-type specific filters after initial creation."""
        if self.registry_type == RegistryType.INTRADAY_FOCUS:
            to_remove: List[str] = []
            for unified_id, instrument in self._unified_instruments.items():
                if not self._is_in_focus_set(instrument):
                    to_remove.append(unified_id)
            for uid in to_remove:
                self._remove_instrument(uid)

    def _is_in_focus_set(self, instrument: UnifiedInstrument) -> bool:
        """Check if instrument is in focus set."""
        if instrument.canonical_symbol in self.config.focus_symbols:
            return True
        for broker_insts in instrument.broker_instruments.values():
            for broker_inst in broker_insts:
                clean_symbol = self._clean_symbol(broker_inst.tradingsymbol)
                if clean_symbol in self.config.focus_symbols:
                    return True
        return False

    def _remove_instrument(self, unified_id: str):
        """Remove instrument from indices and registry."""
        if unified_id not in self._unified_instruments:
            return
        instrument = self._unified_instruments[unified_id]
        # Remove from symbol index
        to_delete = []
        for symbol, ids in self._symbol_to_unified.items():
            if unified_id in ids:
                ids.remove(unified_id)
            if not ids:
                to_delete.append(symbol)
        for sym in to_delete:
            self._symbol_to_unified.pop(sym, None)
        # Remove from broker indices
        for broker_name, broker_insts in instrument.broker_instruments.items():
            for broker_inst in broker_insts:
                seg = broker_inst.segment
                token = broker_inst.broker_token
                token_map = self._broker_to_unified.get(broker_name, {}).get(seg, {})
                if token in token_map and unified_id in token_map[token]:
                    token_map[token].remove(unified_id)
                    if not token_map[token]:
                        del token_map[token]
        # Remove from reverse map
        self._unified_to_broker.pop(unified_id, None)
        # Remove from registry
        del self._unified_instruments[unified_id]

    def _is_expired(self, expiry_str: str) -> bool:
        """Heuristic expiry check; treats unknown formats as not expired."""
        try:
            if len(expiry_str) >= 10:
                expiry_date = datetime.fromisoformat(expiry_str[:10])
            else:
                return False
            return expiry_date < datetime.now(timezone.utc)
        except Exception:
            return False

    def _calculate_statistics(self):
        self.stats['total_unified_instruments'] = len(self._unified_instruments)
        self.stats['active_instruments'] = len(self._active_instruments) if self._active_instruments else 0
        self.stats['focus_instruments'] = len(self._focus_instruments) if self._focus_instruments else 0

    def _persist_registry(self):
        """Persist registry metadata if configured."""
        try:
            if not self.config.persistence_path:
                return
            snapshot = {
                "registry_type": self.registry_type.value,
                "total_unified_instruments": len(self._unified_instruments),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            with open(self.config.persistence_path, "w", encoding="utf-8") as fh:
                json.dump(snapshot, fh)
        except Exception as e:
            self.logger.debug(f"⚠️ Failed to persist registry snapshot: {e}")
    
    def _generate_unified_id(self, broker_inst: BrokerInstrument) -> str:
        """Generate unified ID with type differentiation"""
        symbol_key = broker_inst.tradingsymbol.upper()
        normalized_symbol = self._normalize_symbol_key(symbol_key, broker_inst.instrument_type)
        base_id = f"{broker_inst.exchange}:{normalized_symbol}"
        
        # Add instrument type for derivatives to avoid conflicts
        if broker_inst.instrument_type in ['FUT', 'CE', 'PE']:
            return f"{base_id}:{broker_inst.instrument_type}"
        
        return base_id
    
    def _add_symbol_index(self, symbol: str, unified_id: str) -> None:
        """Helper to index symbol → unified mapping."""
        if symbol not in self._symbol_to_unified:
            self._symbol_to_unified[symbol] = []
        if unified_id not in self._symbol_to_unified[symbol]:
            self._symbol_to_unified[symbol].append(unified_id)
    
    @staticmethod
    def _normalize_option_symbol_for_lookup(symbol: str) -> str:
        """
        Convert NFONIFTY25D0926300CE → NIFTY25D0926300CE
        or NFONIFTY30DEC26250PE → NIFTY30DEC26250PE
        """
        # Remove 'NFO' prefix if present
        if symbol.startswith('NFO'):
            # Remove NFO and keep the rest
            symbol = symbol[3:]
        
        # Also handle 'NFONIFTY' -> 'NIFTY'
        if symbol.startswith('NFONIFTY'):
            symbol = symbol[3:]  # Remove 'NFO' prefix
        
        return symbol
    
    def _normalize_symbol_key(self, symbol: str, instrument_type: str) -> str:
        """Normalize broker symbol naming conventions (e.g., strip -EQ)."""
        result = symbol
        if instrument_type == "EQ" and symbol.endswith("-EQ"):
            result = symbol[:-3]
        return result
    
    def _resolve_mapping_conflicts(self):
        """Resolve mapping conflicts between brokers (segment-aware)"""
        # Handle cases where one broker token maps to multiple unified IDs
        for broker_name, segment_map in self._broker_to_unified.items():
            for segment, token_map in segment_map.items():
                for token, unified_ids in token_map.items():
                    if len(unified_ids) > 1:
                        self.logger.warning(
                            f"Broker {broker_name} segment {segment} token {token} maps to multiple unified IDs: {unified_ids}"
                        )
                        # Strategy: Prefer the one with more broker support
                        best_unified_id = self._select_best_unified_id(unified_ids)
                        if best_unified_id:
                            self._broker_to_unified[broker_name][segment][token] = [best_unified_id]
    
    def _select_best_unified_id(self, unified_ids: List[str]) -> Optional[str]:
        """Select the best unified ID from multiple candidates"""
        # Prefer IDs that are supported by more brokers
        max_brokers = 0
        best_id = None
        
        for uid in unified_ids:
            unified_inst = self._unified_instruments.get(uid)
            if unified_inst:
                broker_count = len(unified_inst.broker_instruments)
                if broker_count > max_brokers:
                    max_brokers = broker_count
                    best_id = uid
        
        return best_id

    def _add_broker_token_mapping(self, broker_name: str, segment: str, broker_token: str, unified_id: str) -> None:
        """Helper to index mappings in both directions with segment awareness."""
        segment_key = segment or "UNKNOWN"
        self._broker_to_unified.setdefault(broker_name, {}).setdefault(segment_key, {}).setdefault(broker_token, [])
        if unified_id not in self._broker_to_unified[broker_name][segment_key][broker_token]:
            self._broker_to_unified[broker_name][segment_key][broker_token].append(unified_id)

        self._unified_to_broker.setdefault(unified_id, {}).setdefault(broker_name, {}).setdefault(segment_key, [])
        if broker_token not in self._unified_to_broker[unified_id][broker_name][segment_key]:
            self._unified_to_broker[unified_id][broker_name][segment_key].append(broker_token)
    
    def _enrich_with_canonical_data(self):
        """Enrich unified instruments with canonical names and symbols"""
        for _unified_id, instrument in self._unified_instruments.items():
            # Use the most common name across brokers
            name_counts = {}
            
            for broker_insts in instrument.broker_instruments.values():
                for broker_inst in broker_insts:
                    name_counts[broker_inst.name] = name_counts.get(broker_inst.name, 0) + 1
            
            if name_counts:
                instrument.canonical_name = max(name_counts, key=lambda k: name_counts[k])
            
            # Clean symbol (remove exchange-specific suffixes)
            instrument.canonical_symbol = self._clean_symbol(instrument.canonical_symbol)
    
    def _clean_symbol(self, symbol: str) -> str:
        """Clean and standardize symbol names"""
        # Remove common derivatives suffixes
        derivatives_suffixes = ['FUT', 'CE', 'PE', 'FUTURES', 'OPTIONS']
        cleaned = symbol
        for suffix in derivatives_suffixes:
            if cleaned.endswith(suffix):
                cleaned = cleaned[:-len(suffix)].strip()
        return cleaned
    
    def _log_statistics(self):
        """Log registry statistics"""
        self.logger.info("=== Instrument Registry Statistics ===")
        self.logger.info(f"Total Unified Instruments: {self.stats['total_unified_instruments']}")
        for broker, count in self.stats['broker_instruments'].items():
            self.logger.info(f"{broker} instruments: {count}")
        
        # Calculate mapping coverage
        total_broker_inst = sum(self.stats['broker_instruments'].values())
        coverage_ratio = self.stats['total_unified_instruments'] / total_broker_inst if total_broker_inst > 0 else 0
        self.logger.info(f"Mapping coverage: {coverage_ratio:.2%}")
    
    # Query methods
    def get_by_unified_id(self, unified_id: str) -> Optional[UnifiedInstrument]:
        """Get unified instrument by ID"""
        return self._unified_instruments.get(unified_id)
    
    def get_by_broker_token(self, broker: str, broker_token: str, segment: Optional[str] = None) -> Optional[UnifiedInstrument]:
        """Get unified instrument by broker token, optionally filtered by segment"""
        if broker not in self._broker_to_unified:
            return None

        if segment:
            if segment in self._broker_to_unified[broker] and broker_token in self._broker_to_unified[broker][segment]:
                unified_ids = self._broker_to_unified[broker][segment][broker_token]
                if unified_ids:
                    return self._unified_instruments.get(unified_ids[0])
            return None
        else:
            unified_candidates: List[Tuple[str, str]] = []
            for seg in self._broker_to_unified[broker]:
                if broker_token in self._broker_to_unified[broker][seg]:
                    unified_ids = self._broker_to_unified[broker][seg][broker_token]
                    for uid in unified_ids:
                        unified_candidates.append((uid, seg))

            if not unified_candidates:
                return None
            if len(unified_candidates) == 1:
                return self._unified_instruments.get(unified_candidates[0][0])

            self.logger.warning(
                f"Broker token {broker_token} in broker {broker} found in multiple segments: "
                f"{[seg for _, seg in unified_candidates]}. Using first match."
            )
            return self._unified_instruments.get(unified_candidates[0][0])

    def get_by_broker_token_and_segment(self, broker: str, broker_token: str, segment: str) -> Optional[UnifiedInstrument]:
        """Get unified instrument by broker token and segment (explicit)"""
        if broker in self._broker_to_unified and segment in self._broker_to_unified[broker]:
            if broker_token in self._broker_to_unified[broker][segment]:
                unified_ids = self._broker_to_unified[broker][segment][broker_token]
                if unified_ids:
                    return self._unified_instruments.get(unified_ids[0])
        return None

    def get_all_tokens_for_unified(self, unified_id: str, broker: str) -> Dict[str, List[str]]:
        """Get all tokens for a unified instrument, grouped by segment"""
        if unified_id in self._unified_to_broker and broker in self._unified_to_broker[unified_id]:
            return self._unified_to_broker[unified_id][broker]
        return {}
    
    def get_broker_instrument(self, unified_id: str, broker: str) -> Optional[BrokerInstrument]:
        """Get broker-specific instrument from unified ID"""
        unified_inst = self.get_by_unified_id(unified_id)
        if unified_inst:
            return unified_inst.get_broker_instrument(broker)
        return None
    
    def resolve_symbol(self, symbol: str, exchange: Optional[str] = None) -> List[UnifiedInstrument]:
        """Resolve symbol to unified instruments"""
        symbol_key = symbol.upper()
        
        # Normalize option symbol (NFONIFTY -> NIFTY)
        normalized_symbol = self._normalize_option_symbol_for_lookup(symbol_key)
        
        # Try normalized symbol first, then original
        unified_ids = self._symbol_to_unified.get(normalized_symbol, [])
        if not unified_ids:
            unified_ids = self._symbol_to_unified.get(symbol_key, [])
        
        # Fallback to broker suffix normalization
        if not unified_ids:
            normalized = self._normalize_symbol_key(symbol_key, "EQ")
            if normalized != symbol_key:
                unified_ids = self._symbol_to_unified.get(normalized, [])
        
        instruments = [self._unified_instruments[uid] for uid in unified_ids if uid in self._unified_instruments]
        
        if exchange:
            instruments = [inst for inst in instruments 
                          if any(bi.exchange == exchange 
                               for broker_insts in inst.broker_instruments.values()
                               for bi in broker_insts)]
        
        return instruments
    
    def resolve(self, symbol: str, exchange: Optional[str] = None) -> Optional[UnifiedInstrument]:
        """
        Backwards-compatible single instrument resolver.
        Returns the first unified instrument matching the symbol/exchange.
        """
        exchange = exchange or self.guess_exchange(symbol)
        instruments = self.resolve_symbol(symbol, exchange)
        return instruments[0] if instruments else None
    
    def get_all_brokers_for_instrument(self, unified_id: str) -> List[str]:
        """Get all brokers that support this instrument"""
        unified_inst = self.get_by_unified_id(unified_id)
        return list(unified_inst.broker_instruments.keys()) if unified_inst else []
    
    def add_broker_instrument(self, unified_id: str, broker_inst: BrokerInstrument) -> None:
        """Add or update a broker instrument in the unified registry with segment awareness"""
        broker_name = broker_inst.broker
        segment = broker_inst.segment
        
        # Create unified instrument if it doesn't exist
        if unified_id not in self._unified_instruments:
            self._unified_instruments[unified_id] = UnifiedInstrument(
                unified_id=unified_id,
                canonical_symbol=broker_inst.tradingsymbol,
                canonical_name=broker_inst.name,
                broker_instruments={}
            )
        
        # Add broker instrument to unified instrument
        if broker_name not in self._unified_instruments[unified_id].broker_instruments:
            self._unified_instruments[unified_id].broker_instruments[broker_name] = []
        
        # Check if this broker instrument already exists (by token and segment)
        existing = None
        for existing_inst in self._unified_instruments[unified_id].broker_instruments[broker_name]:
            if existing_inst.broker_token == broker_inst.broker_token and existing_inst.segment == segment:
                existing = existing_inst
                break
        
        if existing:
            # Update existing instrument
            idx = self._unified_instruments[unified_id].broker_instruments[broker_name].index(existing)
            self._unified_instruments[unified_id].broker_instruments[broker_name][idx] = broker_inst
        else:
            # Add new instrument
            self._unified_instruments[unified_id].broker_instruments[broker_name].append(broker_inst)
        
        # Update segment-aware indices
        if broker_name not in self._broker_to_unified:
            self._broker_to_unified[broker_name] = {}
        if segment not in self._broker_to_unified[broker_name]:
            self._broker_to_unified[broker_name][segment] = {}
        if broker_inst.broker_token not in self._broker_to_unified[broker_name][segment]:
            self._broker_to_unified[broker_name][segment][broker_inst.broker_token] = []
        if unified_id not in self._broker_to_unified[broker_name][segment][broker_inst.broker_token]:
            self._broker_to_unified[broker_name][segment][broker_inst.broker_token].append(unified_id)
        
        # Update reverse mapping
        if unified_id not in self._unified_to_broker:
            self._unified_to_broker[unified_id] = {}
        if broker_name not in self._unified_to_broker[unified_id]:
            self._unified_to_broker[unified_id][broker_name] = {}
        if segment not in self._unified_to_broker[unified_id][broker_name]:
            self._unified_to_broker[unified_id][broker_name][segment] = []
        if broker_inst.broker_token not in self._unified_to_broker[unified_id][broker_name][segment]:
            self._unified_to_broker[unified_id][broker_name][segment].append(broker_inst.broker_token)
        
        # Symbol index
        symbol_key = broker_inst.tradingsymbol.upper()
        self._add_symbol_index(symbol_key, unified_id)
        
        normalized_key = self._normalize_symbol_key(symbol_key, broker_inst.instrument_type)
        if normalized_key != symbol_key:
            self._add_symbol_index(normalized_key, unified_id)
    
    def guess_exchange(self, symbol: str) -> str:
        """Guess exchange from symbol name"""
        symbol_upper = symbol.upper()
        if any(x in symbol_upper for x in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']):
            return "NFO"
        elif any(x in symbol_upper for x in ['MCX', 'COMEX']):
            return "MCX"
        else:
            return "NSE"


# Singleton instance (backward compatibility)
_unified_registry = None

def get_unified_registry() -> UnifiedInstrumentRegistry:
    """
    Get singleton instance of UnifiedInstrumentRegistry (backward compatibility).
    
    This function now uses the factory pattern and returns a COMPREHENSIVE registry.
    For intraday focus, use get_intraday_focus_registry() instead.
    """
    global _unified_registry
    if _unified_registry is None:
        # Use factory to get comprehensive registry
        from .registry_factory import get_comprehensive_registry
        _unified_registry = get_comprehensive_registry()
    return _unified_registry

def get_comprehensive_registry() -> UnifiedInstrumentRegistry:
    """Get comprehensive registry (all instruments)"""
    from .registry_factory import get_comprehensive_registry as _get_comprehensive
    return _get_comprehensive()

def get_intraday_focus_registry() -> UnifiedInstrumentRegistry:
    """Get intraday focus registry (248 instruments)"""
    from .registry_factory import get_intraday_focus_registry as _get_intraday
    return _get_intraday()


class EnhancedInstrumentService:
    """Enhanced service with broker-specific resolution and analytics support"""
    
    def __init__(self, registry: UnifiedInstrumentRegistry):
        self.registry = registry
        self.logger = logging.getLogger(__name__)
        self._loader = None
    
    @property
    def loader(self):
        """Lazy-load the instrument registry loader"""
        if self._loader is None:
            self._loader = InstrumentRegistryLoader(self.registry)
        return self._loader
    
    async def refresh_angel_one_instruments(self):
        """Refresh Angel One instruments from master file or download from API"""
        self.logger.info("Refreshing Angel One instruments...")
        try:
            await self.loader.sync_broker_instruments('angel_one')
            self.logger.info("✅ Angel One instruments refreshed successfully")
        except Exception as e:
            self.logger.error(f"❌ Failed to refresh Angel One instruments: {e}", exc_info=True)
            raise
    
    @staticmethod
    def normalize_option_symbol(symbol: str) -> str:
        """
        Convert NFONIFTY25D0926300CE → NIFTY25D0926300CE
        or NFONIFTY30DEC26250PE → NIFTY30DEC26250PE
        """
        # Remove 'NFO' prefix if present
        if symbol.startswith('NFO'):
            # Remove NFO and keep the rest
            symbol = symbol[3:]
        
        # Also handle 'NFONIFTY' -> 'NIFTY'
        if symbol.startswith('NFONIFTY'):
            symbol = symbol[3:]  # Remove 'NFO' prefix
        
        return symbol
    
    @staticmethod
    def convert_angel_to_zerodha_symbol(symbol: str) -> str:
        """
        Convert Angel One format to Zerodha format:
        NIFTY30DEC26250PE → NIFTY26D3026250PE
        
        Angel: DDMMMYY (30DEC26)
        Zerodha: YY + MonthCode + DD (26D30)
        Month codes: JAN=1, FEB=2, MAR=3, APR=4, MAY=5, JUN=6, 
                    JUL=7, AUG=8, SEP=9, OCT=O, NOV=N, DEC=D
        """
        # Temporary bypass: returning symbol unchanged to avoid malformed conversions
        logger = logging.getLogger(__name__)
        logger.debug("⚠️ convert_angel_to_zerodha_symbol disabled, returning %s as-is", symbol)
        return symbol
    
    @staticmethod
    def convert_zerodha_to_angel_symbol(symbol: str) -> str:
        """
        Convert Zerodha format to Angel One format:
        NIFTY25D2326700PE → NIFTY23DEC2526700PE
        """
        import re
        
        # Pattern for Zerodha format: underlying + YY + MonthCode + DD + strike + PE/CE
        pattern = r'^([A-Z]+)(\d{2})([1-9OND])(\d{2})(\d+)(PE|CE)$'
        match = re.match(pattern, symbol)
        
        if not match:
            return symbol
        
        underlying, year, month_code, day, strike, option_type = match.groups()
        
        # Reverse month mapping
        rev_month_map = {
            '1': 'JAN', '2': 'FEB', '3': 'MAR', '4': 'APR', '5': 'MAY', '6': 'JUN',
            '7': 'JUL', '8': 'AUG', '9': 'SEP', 'O': 'OCT', 'N': 'NOV', 'D': 'DEC'
        }
        
        month = rev_month_map.get(month_code, month_code)
        
        # Angel format: DD + MMM + YY
        angel_expiry = f"{day}{month}{year}"
        
        angel_symbol = f"{underlying}{angel_expiry}{strike}{option_type}"
        
        return angel_symbol
    
    @staticmethod
    def find_token_for_symbol(symbol: str, token_lookup: dict) -> Optional[dict]:
        """
        Find token for symbol, handling multiple formats.
        """
        # Try direct match first
        if symbol in token_lookup:
            return token_lookup[symbol]
        
        # Remove NFO prefix
        clean_symbol = symbol[3:] if symbol.startswith('NFO') else symbol
        
        # Try clean symbol
        if clean_symbol in token_lookup:
            return token_lookup[clean_symbol]
        
        # Try converting Angel to Zerodha format
        if 'DEC' in symbol or 'JAN' in symbol:  # Likely Angel format
            zerodha_symbol = EnhancedInstrumentService.convert_angel_to_zerodha_symbol(clean_symbol)
            if zerodha_symbol in token_lookup:
                return token_lookup[zerodha_symbol]
        
        # Try converting Zerodha to Angel format  
        if any(x in symbol for x in ['1', '2', '3', '4', '5', '6', '7', '8', '9', 'O', 'N', 'D']):
            angel_symbol = EnhancedInstrumentService.convert_zerodha_to_angel_symbol(clean_symbol)
            if angel_symbol in token_lookup:
                return token_lookup[angel_symbol]
        
        # Last resort: search in token lookup values
        for key, value in token_lookup.items():
            if clean_symbol in key or symbol in key:
                return value
        
        return None

    def resolve_for_broker_with_segment(
        self,
        symbol_code: str,
        broker: str,
        segment: str,
        instrument_type: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Resolve symbol with explicit segment awareness.
        """
        unified_instruments = self.registry.resolve_symbol(symbol_code)
        if not unified_instruments:
            return None

        for unified_inst in unified_instruments:
            if broker in unified_inst.broker_instruments:
                for broker_inst in unified_inst.broker_instruments[broker]:
                    if broker_inst.segment == segment:
                        if not instrument_type or broker_inst.instrument_type == instrument_type:
                            return self._format_broker_response(broker_inst, unified_inst, broker)
        return None

    def detect_segment_from_symbol(self, symbol: str) -> str:
        """Detect segment from symbol patterns (heuristic)."""
        symbol_upper = symbol.upper()

        # Commodities
        if any(x in symbol_upper for x in ["GOLD", "SILVER"]):
            return "NCO"  # Angel One commodities segment

        # Equity derivatives
        if any(x in symbol_upper for x in ["NIFTY", "BANKNIFTY", "SENSEX"]):
            return "NFO"  # Equity derivatives

        # Currency derivatives
        if any(x in symbol_upper for x in ["USDINR", "EURINR"]):
            return "CDS"

        # Default: cash equities
        return "NSE"
    
    def resolve_for_broker(
    self, 
    symbol_code: str, 
    broker: str, 
    instrument_type: Optional[str] = None
) -> Optional[Dict[str, Any]]:
        """
        Enhanced multi-broker symbol resolution.
        Handles different broker symbol formats (Zerodha, Angel One, etc.).
        
        Args:
            symbol_code: Input symbol (could be broker-specific or unified)
            broker: Target broker ('zerodha', 'angel_one', etc.)
            instrument_type: Optional filter by instrument type
        
        Returns:
            Dict with broker-specific instrument details or None if not found
        """
        # Normalize option symbol before processing
        symbol_code = self.normalize_option_symbol(symbol_code)
        
        # 1. PARSE INPUT WITH MULTI-FORMAT SUPPORT
        if ':' in symbol_code:
            # Unified format: "NSE:HCLTECH" or "NSE:HCLTECH-EQ"
            exchange, symbol = symbol_code.split(':', 1)
            # Clean up any broker suffixes for proper lookup
            clean_symbol = self._strip_broker_suffixes(symbol, exchange)
        else:
            # Broker-specific format: "HCLTECH" or "HCLTECH-EQ"
            symbol = symbol_code
            clean_symbol = self._strip_broker_suffixes(symbol, None)
            exchange = self._guess_exchange(clean_symbol)
        
        broker_lower = broker.lower()
        
        # 2. BROKER-SPECIFIC SYMBOL FORMATTING
        # Convert to broker's expected format BEFORE lookup
        broker_formatted_symbol = self._format_for_broker_symbol(
            clean_symbol, exchange, broker_lower
        )
        
        # 3. MULTI-ATTEMPT LOOKUP STRATEGY
        unified_instruments = None
        
        # Strategy 1: Try with broker-formatted symbol
        if broker_formatted_symbol != clean_symbol:
            unified_instruments = self.registry.resolve_symbol(
                broker_formatted_symbol, exchange
            )
        
        # Strategy 2: Try with clean symbol (fallback)
        if not unified_instruments:
            unified_instruments = self.registry.resolve_symbol(
                clean_symbol, exchange
            )
        
        # Strategy 3: Try fuzzy matching for Angel One EQ symbols
        if not unified_instruments and broker_lower == 'angel_one':
            # For Angel One NSE equity, try appending -EQ
            if exchange == 'NSE' and not clean_symbol.endswith('-EQ'):
                angel_symbol = f"{clean_symbol}-EQ"
                unified_instruments = self.registry.resolve_symbol(
                    angel_symbol, exchange
                )
        
        # 4. DIRECT UNIFIED ID LOOKUP (if available)
        if not unified_instruments and ':' in symbol_code:
            # Try direct unified ID lookup
            unified_inst = self.registry.get_by_unified_id(symbol_code)
            if unified_inst:
                broker_inst = unified_inst.get_broker_instrument(broker_lower)
                if broker_inst:
                    return self._format_broker_response(broker_inst, unified_inst, broker_lower)
        
        # Weekly alias attempts (e.g., NFONIFTY weekly vs Angel monthly format)
        if not unified_instruments:
            weekly_aliases = self._generate_weekly_aliases(clean_symbol)
            for alias_symbol in weekly_aliases:
                alias_results = self.registry.resolve_symbol(alias_symbol, exchange)
                if alias_results:
                    unified_instruments = alias_results
                    clean_symbol = alias_symbol
                    break

        # 5. FALLBACK RESOLUTION
        if not unified_instruments:
            return self._fallback_resolve(
                clean_symbol, exchange, broker_lower
            )
        
        # 6. FILTER BY INSTRUMENT TYPE
        if instrument_type:
            unified_instruments = [
                inst for inst in unified_instruments
                if any(
                    bi.instrument_type == instrument_type
                    for broker_insts in inst.broker_instruments.values()
                    for bi in broker_insts
                )
            ]
        
        if not unified_instruments:
            return None
        
        # 7. SELECT BEST INSTRUMENT
        unified_inst = self._select_best_instrument(unified_instruments, broker_lower)
        broker_inst = unified_inst.get_broker_instrument(broker_lower)
        
        if broker_inst:
            return self._format_broker_response(broker_inst, unified_inst, broker_lower)
        
        # 8. ALTERNATIVE BROKER LOOKUP
        return self._find_alternative_broker(
            unified_inst, broker_lower, clean_symbol, exchange
        )

    def _strip_broker_suffixes(
        self, symbol: str, exchange: Optional[str] = None
    ) -> str:
        """
        Strip broker-specific suffixes from symbol.
        Handles formats from various brokers.
        """
        # Common broker suffix patterns
        suffix_patterns = [
            '-EQ',    # Angel One NSE equity
            '-BE',    # Angel One BSE equity
            '-NFO',   # F&O suffixes
            '-FUT',   # Futures
            '-CE', '-PE',  # Options
            '-ETF',   # ETFs
        ]
        
        # Exchange-specific handling
        if exchange == 'NSE':
            # Remove common NSE suffixes
            for suffix in ['-EQ', '-NSE']:
                if symbol.endswith(suffix):
                    return symbol[:-len(suffix)]
        elif exchange == 'BSE':
            # Remove common BSE suffixes
            for suffix in ['-BE', '-BSE']:
                if symbol.endswith(suffix):
                    return symbol[:-len(suffix)]
        
        # Generic suffix removal
        for suffix in suffix_patterns:
            if symbol.endswith(suffix):
                return symbol[:-len(suffix)]
        
        return symbol

    def _format_for_broker_symbol(
        self, symbol: str, exchange: str, broker: str
    ) -> str:
        """
        Format symbol according to broker's expected format.
        """
        broker_formats = {
            'angel_one': {
                'NSE': lambda s: f"{s}-EQ" if not s.endswith('-EQ') else s,
                'BSE': lambda s: f"{s}-BE" if not s.endswith('-BE') else s,
                'NFO': lambda s: s,  # F&O symbols usually don't need modification
            },
            'zerodha': {
                'NSE': lambda s: s.replace('-EQ', ''),
                'BSE': lambda s: s.replace('-BE', ''),
                'NFO': lambda s: s,
            },
            'dhan': {
                'NSE': lambda s: f"{s}_EQ" if not s.endswith('_EQ') else s,
                'BSE': lambda s: f"{s}_BE" if not s.endswith('_BE') else s,
            }
        }
        
        # Get broker-specific formatter
        if broker in broker_formats and exchange in broker_formats[broker]:
            return broker_formats[broker][exchange](symbol)
        
        # Default: return as-is
        return symbol

    def _generate_weekly_aliases(self, symbol: str) -> List[str]:
        """Generate possible aliases by converting weekly symbols to dated format."""
        match = WEEKLY_SYMBOL_REGEX.match(symbol.upper())
        if not match:
            return []
        
        prefix = match.group('prefix')
        yy = match.group('yy')
        day = match.group('day')
        strike = match.group('strike')
        option = match.group('option')
        
        base = prefix
        if base.startswith('NFO'):
            base = base[3:]
        aliases = []
        for month in MONTH_ABBRS:
            candidate = f"{base}{day}{month}{yy}{strike}{option}"
            aliases.append(candidate)
        return aliases

    def _format_broker_response(
        self, 
        broker_inst: Any, 
        unified_inst: Any, 
        broker: str
    ) -> Dict[str, Any]:
        """
        Format consistent response for all brokers.
        """
        response = {
            'tradingsymbol': broker_inst.tradingsymbol,
            'symboltoken': broker_inst.broker_token,
            'exchange': broker_inst.exchange,
            'instrument_type': broker_inst.instrument_type,
            'name': unified_inst.name if hasattr(unified_inst, 'name') 
                else broker_inst.tradingsymbol,
            'broker': broker,
        }
        
        # Add optional fields if available
        optional_fields = [
            ('lot_size', 'lot_size'),
            ('tick_size', 'tick_size'),
            ('expiry', 'expiry'),
            ('strike', 'strike'),
            ('segment', 'segment'),
        ]
        
        for field, attr in optional_fields:
            if hasattr(broker_inst, attr):
                value = getattr(broker_inst, attr)
                if value is not None:
                    response[field] = value
        
        return response
    
    def _select_best_instrument(self, instruments: List[UnifiedInstrument], 
                               broker: str) -> UnifiedInstrument:
        """Select the best instrument from multiple candidates"""
        # Prefer instruments that have the requested broker
        for inst in instruments:
            if broker in inst.broker_instruments:
                return inst
        
        # Fallback to first instrument
        return instruments[0]
    
    def _format_for_broker(self, broker_inst: BrokerInstrument, 
                          unified_inst: UnifiedInstrument, broker: str) -> Dict[str, Any]:
        """Format instrument data for specific broker requirements"""
        
        base_format = {
            "tradingsymbol": broker_inst.tradingsymbol,
            "exchange": broker_inst.exchange,
            "symboltoken": broker_inst.broker_token,
            "name": unified_inst.canonical_name,
            "instrument_type": broker_inst.instrument_type,
            "segment": broker_inst.segment,
            "lot_size": broker_inst.lot_size,
            "tick_size": broker_inst.tick_size,
            "expiry": broker_inst.expiry,
            "strike": broker_inst.strike,
            "sector": unified_inst.sector,
            "industry": unified_inst.industry,
            "unified_id": unified_inst.unified_id,
            "source_file": broker_inst.source_file
        }
        
        # Broker-specific adjustments
        if broker == "angel_one":
            # Angel One might need token as string
            base_format["symboltoken"] = str(broker_inst.broker_token)
        
        return base_format
    
    def _find_alternative_broker(self, unified_inst: UnifiedInstrument, 
                                target_broker: str, symbol: str, exchange: str) -> Optional[Dict]:
        """Find alternative when target broker doesn't have the instrument"""
        available_brokers = list(unified_inst.broker_instruments.keys())
        
        if available_brokers:
            self.logger.warning(
                f"Instrument {symbol} not available on {target_broker}. "
                f"Available on: {available_brokers}"
            )
        
        return self._fallback_resolve(symbol, exchange, target_broker, available_brokers)
    
    def _fallback_resolve(self, symbol: str, exchange: str, broker: str,
                          available_brokers: Optional[List[str]] = None) -> Dict:
        """Fallback used when a broker does not list the instrument."""
        return {
            "tradingsymbol": symbol,
            "exchange": exchange,
            "symboltoken": None,
            "available_brokers": available_brokers or [],
        }
    
    def _guess_exchange(self, symbol: str) -> str:
        """Enhanced exchange guessing with more patterns"""
        symbol_upper = symbol.upper()
        
        if any(x in symbol_upper for x in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'SENSEX']):
            return "NFO"
        elif any(x in symbol_upper for x in ['GOLD', 'SILVER', 'CRUDEOIL', 'NATURALGAS']):
            return "MCX"
        else:
            return "NSE"
    
    def debug_symbol_resolution(self, symbol_code: str, broker: str) -> Dict[str, Any]:
        """Trace symbol resolution attempts for troubleshooting."""
        steps: List[str] = []
        lookup_results: List[Dict[str, Any]] = []
        
        if ':' in symbol_code:
            exchange, symbol = symbol_code.split(':', 1)
            steps.append(f"Parsed as unified: exchange={exchange}, symbol={symbol}")
        else:
            symbol = symbol_code
            exchange = self._guess_exchange(symbol)
            steps.append(f"Parsed as simple: symbol={symbol}, guessed_exchange={exchange}")
        
        clean_symbol = self._strip_broker_suffixes(symbol, exchange)
        steps.append(f"After cleaning: {clean_symbol}")
        
        broker_symbol = self._format_for_broker_symbol(clean_symbol, exchange, broker)
        steps.append(f"Broker formatted: {broker_symbol}")
        
        result = self.registry.resolve_symbol(broker_symbol, exchange)
        lookup_results.append({
            "attempt": "broker_formatted",
            "symbol": broker_symbol,
            "found": bool(result)
        })
        
        if not result:
            result = self.registry.resolve_symbol(clean_symbol, exchange)
            lookup_results.append({
                "attempt": "clean_symbol",
                "symbol": clean_symbol,
                "found": bool(result)
            })
        
        return {
            "steps": steps,
            "lookup_attempts": lookup_results,
            "final_result": "Found" if result else "Not found",
            "recommendation": None if result else "Add to instrument registry"
        }


class InstrumentRegistryLoader:
    """Loads and syncs instrument data from multiple brokers"""
    
    def __init__(self, registry: UnifiedInstrumentRegistry):
        self.registry = registry
        self.logger = logging.getLogger(__name__)
        self.broker_loaders = {
            'angel_one': self._load_angel_one_instruments,
            'zerodha': self._load_zerodha_instruments,
        }
    
    def _parse_strike(self, strike_value: Any) -> Optional[float]:
        """Parse strike value safely, handling None and invalid values"""
        if strike_value is None:
            return None
        strike_str = str(strike_value).strip()
        if not strike_str or strike_str in ['NULL', '0.000000', '0', '-1.000000', '-1']:
            return None
        try:
            return float(strike_str)
        except (ValueError, TypeError):
            return None
    
    async def sync_broker_instruments(self, broker: str):
        """Sync instruments for specific broker"""
        if broker in self.broker_loaders:
            await self.broker_loaders[broker]()
        else:
            self.logger.warning(f"Unknown broker: {broker}")
    
    async def _load_angel_one_instruments(self):
        """Load Angel One instrument master"""
        try:
            # Try multiple possible paths
            possible_paths = [
                Path("OpenAPIScripMaster.json"),
                Path("shared_core/instrument_token/instrument_csv_files/angel_one_instruments_master.json"),
                Path(__file__).parent.parent.parent / "angel_one_instruments_master.json",
            ]
            
            master_path = None
            for path in possible_paths:
                if path.exists():
                    master_path = path
                    break
            
            if not master_path:
                # Try to download from Angel One
                self.logger.info("OpenAPIScripMaster.json not found locally, attempting download...")
                master_data = await self._download_angel_one_master()
                if not master_data:
                    self.logger.error("Failed to download Angel One master file")
                    return
            else:
                self.logger.info(f"Loading Angel One instruments from: {master_path}")
                with open(master_path, 'r', encoding='utf-8') as f:
                    master_data = json.load(f)
            
            processed_count = 0
            for instrument in master_data:
                try:
                    await self._process_angel_one_instrument(instrument)
                    processed_count += 1
                except Exception as e:
                    self.logger.warning(f"Failed to process Angel One instrument {instrument.get('symbol', 'unknown')}: {e}")
                    continue
            
            self.logger.info(f"Processed {processed_count} Angel One instruments")
                
        except Exception as e:
            self.logger.error(f"Failed to load Angel One instruments: {e}", exc_info=True)
    
    async def _process_angel_one_instrument(self, inst: Dict[str, Any]):
        """Process single Angel One instrument"""
        tradingsymbol = inst.get('symbol', inst.get('tradingsymbol', ''))
        token = str(inst.get('token', ''))
        exchange = inst.get('exch_seg', inst.get('exchseg', 'NSE'))
        name = inst.get('name', tradingsymbol)
        instrument_type = inst.get('instrumenttype', '')
        
        if not tradingsymbol or not token:
            return
        
        # Clean symbol (remove -EQ, -BE suffixes for unified format)
        clean_symbol = tradingsymbol
        if exchange == 'NSE' and tradingsymbol.endswith('-EQ'):
            clean_symbol = tradingsymbol[:-3]
        elif exchange == 'BSE' and tradingsymbol.endswith('-BE'):
            clean_symbol = tradingsymbol[:-3]
        
        # Normalize instrument type
        normalized_type = self._normalize_angel_one_instrument_type(instrument_type)
        
        # Generate unified ID
        base_id = f"{exchange}:{clean_symbol.upper()}"
        if normalized_type in ['FUT', 'CE', 'PE']:
            unified_id = f"{base_id}:{normalized_type}"
        else:
            unified_id = base_id
        
        # Create broker instrument
        broker_inst = BrokerInstrument(
            broker='angel_one',
            broker_token=token,
            tradingsymbol=tradingsymbol,
            name=name,
            exchange=exchange,
            instrument_type=normalized_type,
            segment=exchange,
            lot_size=int(inst.get('lotsize', inst.get('lot_size', 1))) if inst.get('lotsize') or inst.get('lot_size') else 1,
            tick_size=float(inst.get('tick_size', 0.05)) if inst.get('tick_size') else 0.05,
            expiry=inst.get('expiry') if inst.get('expiry') else None,
            strike=self._parse_strike(inst.get('strike')),
            source_file='OpenAPIScripMaster.json',
            is_active=True
        )
        
        # Add to registry
        self.registry.add_broker_instrument(unified_id, broker_inst)
    
    def _normalize_angel_one_instrument_type(self, inst_type: str) -> str:
        """Normalize Angel One instrument type"""
        if not inst_type:
            return 'EQ'
        
        type_map = {
            'EQ': 'EQ',
            'FUTURES': 'FUT',
            'FUT': 'FUT',
            'OPTIDX': 'CE',
            'OPTSTK': 'CE',
            'OPT': 'CE',
            'XX': 'INDEX'
        }
        return type_map.get(inst_type.upper(), inst_type.upper())
    
    async def _download_angel_one_master(self) -> Optional[List[Dict[str, Any]]]:
        """Download Angel One master file from official source"""
        try:
            url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        # Save locally for future use
                        master_path = Path("shared_core/instrument_token/instrument_csv_files/angel_one_instruments_master.json")
                        master_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(master_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, indent=2)
                        self.logger.info(f"Downloaded and saved Angel One master to: {master_path}")
                        return data
                    else:
                        self.logger.error(f"Failed to download Angel One master: HTTP {response.status}")
                        return None
        except Exception as e:
            self.logger.error(f"Error downloading Angel One master: {e}")
            return None
    
    async def _load_zerodha_instruments(self):
        """Load Zerodha instruments"""
        try:
            # Try multiple possible paths
            possible_paths = [
                Path("shared_core/instrument_token/instrument_csv_files/zerodha_instruments_latest.csv"),
                Path(__file__).parent.parent.parent / "zerodha" / "core" / "data" / "zerodha_instruments_latest.csv",
            ]
            
            csv_path = None
            for path in possible_paths:
                if path.exists():
                    csv_path = path
                    break
            
            if not csv_path:
                self.logger.error("Zerodha instruments CSV not found")
                return
            
            self.logger.info(f"Loading Zerodha instruments from: {csv_path}")
            
            processed_count = 0
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        await self._process_zerodha_instrument(row)
                        processed_count += 1
                    except Exception as e:
                        self.logger.warning(f"Failed to process Zerodha instrument {row.get('tradingsymbol', 'unknown')}: {e}")
                        continue
            
            self.logger.info(f"Processed {processed_count} Zerodha instruments")
                
        except Exception as e:
            self.logger.error(f"Failed to load Zerodha instruments: {e}", exc_info=True)
    
    async def _process_zerodha_instrument(self, row: Dict[str, str]):
        """Process single Zerodha instrument"""
        tradingsymbol = row.get('tradingsymbol', '')
        token = str(row.get('instrument_token', ''))
        exchange = row.get('exchange', 'NSE')
        name = row.get('name', tradingsymbol)
        instrument_type = row.get('instrument_type', 'EQ')
        
        if not tradingsymbol or not token:
            return
        
        # Generate unified ID
        base_id = f"{exchange}:{tradingsymbol.upper()}"
        if instrument_type in ['FUT', 'CE', 'PE']:
            unified_id = f"{base_id}:{instrument_type}"
        else:
            unified_id = base_id
        
        # Create broker instrument
        broker_inst = BrokerInstrument(
            broker='zerodha',
            broker_token=token,
            tradingsymbol=tradingsymbol,
            name=name,
            exchange=exchange,
            instrument_type=instrument_type,
            segment=row.get('segment', exchange),
            lot_size=int(row.get('lot_size', 1)) if row.get('lot_size') else 1,
            tick_size=float(row.get('tick_size', 0.05)) if row.get('tick_size') else 0.05,
            expiry=row.get('expiry') if row.get('expiry') and row.get('expiry') != 'NULL' else None,
            strike=self._parse_strike(row.get('strike')),
            source_file='zerodha_instruments_latest.csv',
            is_active=True
        )
        
        # Add to registry
        self.registry.add_broker_instrument(unified_id, broker_inst)


# Enhanced service singletons (one per registry type)
_enhanced_services: Dict[RegistryType, EnhancedInstrumentService] = {}

def get_enhanced_instrument_service(registry_type: RegistryType = RegistryType.COMPREHENSIVE) -> EnhancedInstrumentService:
    """
    Get enhanced service with specified registry type.
    
    Args:
        registry_type: Type of registry to use (COMPREHENSIVE or INTRADAY_FOCUS)
    
    Returns:
        EnhancedInstrumentService instance for the specified registry type
    
    Examples:
        # For manual order placement and analysis
        comprehensive_service = get_enhanced_instrument_service(RegistryType.COMPREHENSIVE)
        
        # For intraday trading (fast, focused)
        intraday_service = get_enhanced_instrument_service(RegistryType.INTRADAY_FOCUS)
    """
    if registry_type not in _enhanced_services:
        from .registry_factory import get_registry
        registry = get_registry(registry_type)
        _enhanced_services[registry_type] = EnhancedInstrumentService(registry)
    return _enhanced_services[registry_type]


class RegistrySynchronizer:
    """
    Synchronizes instruments between comprehensive and intraday focus registries.
    
    This class helps keep the focus registry up-to-date with new instruments
    that match the focus criteria from the comprehensive registry.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        from .registry_factory import get_comprehensive_registry, get_intraday_focus_registry
        self.comprehensive_registry = get_comprehensive_registry()
        self.focus_registry = get_intraday_focus_registry()
    
    async def sync_new_instruments(self) -> Dict[str, Any]:
        """
        Synchronize new instruments from comprehensive to focus registry.
        
        Returns:
            Dict with sync statistics
        """
        try:
            focus_symbols = self.focus_registry.config.focus_symbols
            comprehensive_instruments = self.comprehensive_registry._unified_instruments
            focus_instruments = self.focus_registry._unified_instruments
            
            new_instruments = []
            for unified_id, instrument in comprehensive_instruments.items():
                # Check if instrument is in focus set
                if unified_id not in focus_instruments:
                    # Check if it matches focus criteria
                    if self._is_in_focus_set(instrument, focus_symbols):
                        new_instruments.append(unified_id)
            
            # Add new instruments to focus registry
            for unified_id in new_instruments:
                instrument = comprehensive_instruments[unified_id]
                # Re-add to focus registry (will be filtered appropriately)
                for broker_name, broker_insts in instrument.broker_instruments.items():
                    for broker_inst in broker_insts:
                        self.focus_registry.add_broker_instrument(unified_id, broker_inst)
            
            # Rebuild focus registry indices
            self.focus_registry._apply_registry_filters()
            self.focus_registry._calculate_statistics()
            
            self.logger.info(f"✅ Synced {len(new_instruments)} new instruments to focus registry")
            
            return {
                'synced_count': len(new_instruments),
                'total_focus_instruments': len(self.focus_registry._unified_instruments),
                'new_instrument_ids': new_instruments[:10]  # First 10 for logging
            }
        except Exception as e:
            self.logger.error(f"❌ Error syncing instruments: {e}", exc_info=True)
            return {'error': str(e), 'synced_count': 0}
    
    def _is_in_focus_set(self, instrument: UnifiedInstrument, focus_symbols: Set[str]) -> bool:
        """Check if instrument is in focus set"""
        # Check canonical symbol
        if instrument.canonical_symbol in focus_symbols:
            return True
        
        # Check all broker instruments
        for broker_insts in instrument.broker_instruments.values():
            for broker_inst in broker_insts:
                # Extract base symbol from tradingsymbol (remove expiry/strike suffixes)
                clean_symbol = self._clean_symbol(broker_inst.tradingsymbol)
                if clean_symbol in focus_symbols:
                    return True
        
        return False
    
    def _clean_symbol(self, symbol: str) -> str:
        """Clean symbol to extract base name"""
        # Remove common suffixes (expiry codes, strikes, option types)
        # E.g., "NIFTY25DEC25700CE" -> "NIFTY"
        symbol_upper = symbol.upper()
        for index in ['NIFTY', 'BANKNIFTY', 'SENSEX']:
            if symbol_upper.startswith(index):
                return index
        return symbol_upper
    
    def export_focus_registry(self) -> Dict[str, Any]:
        """
        Export focus registry data for deployment.
        
        Returns:
            Dict containing exportable registry data
        """
        try:
            export_data = {
                'registry_type': self.focus_registry.registry_type.value,
                'total_instruments': len(self.focus_registry._unified_instruments),
                'focus_symbols': list(self.focus_registry.config.focus_symbols),
                'statistics': self.focus_registry.stats,
                'instruments': []
            }
            
            # Export instrument summaries (not full data for size)
            for unified_id, instrument in self.focus_registry._unified_instruments.items():
                instrument_summary = {
                    'unified_id': unified_id,
                    'canonical_symbol': instrument.canonical_symbol,
                    'canonical_name': instrument.canonical_name,
                    'brokers': list(instrument.broker_instruments.keys()),
                    'broker_count': sum(len(insts) for insts in instrument.broker_instruments.values())
                }
                export_data['instruments'].append(instrument_summary)
            
            self.logger.info(f"✅ Exported focus registry: {export_data['total_instruments']} instruments")
            return export_data
        except Exception as e:
            self.logger.error(f"❌ Error exporting focus registry: {e}", exc_info=True)
            return {'error': str(e)}
