# shared_core/instrument_token/registry_types.py
from enum import Enum
from typing import Set, Optional, List
from dataclasses import dataclass, field
import json
from pathlib import Path

class RegistryType(Enum):
    """Types of instrument registries"""
    COMPREHENSIVE = "comprehensive"  # All instruments (including expired)
    INTRADAY_FOCUS = "intraday_focus"  # Only 248 trading instruments
    BROKER_SPECIFIC = "broker_specific"  # Broker-specific instruments
    
@dataclass
class RegistryConfig:
    """Configuration for different registry types"""
    registry_type: RegistryType
    include_expired: bool = False
    include_all_brokers: bool = True
    focus_symbols: Optional[Set[str]] = None
    max_instruments: Optional[int] = None
    update_frequency: str = "daily"  # daily, weekly, realtime
    persistence_path: Path = None
    
    def __post_init__(self):
        if self.focus_symbols is None:
            self.focus_symbols = set()