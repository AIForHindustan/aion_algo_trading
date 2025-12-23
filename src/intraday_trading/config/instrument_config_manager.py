"""
Instrument Configuration Manager
================================

Centralized manager for instrument configuration loading and resolution.
Wires config/instrument_config.py into the pattern detection system.

Features:
- Symbol-to-config resolution (weekly vs monthly options)
- Lot size retrieval with date-based logic
- Volume baseline day selection
- Pattern enablement filtering
- Risk per trade configuration
"""

import logging
from typing import Dict, Optional, List
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Import instrument config
try:
    from config.instrument_config import InstrumentConfig, INSTRUMENT_CONFIGS, PATTERN_CONFIGS
    INSTRUMENT_CONFIG_AVAILABLE = True
except ImportError:
    INSTRUMENT_CONFIG_AVAILABLE = False
    InstrumentConfig = None
    INSTRUMENT_CONFIGS = {}


class InstrumentConfigManager:
    """
    Manager for instrument configuration resolution and retrieval.
    
    Handles:
    - Symbol-to-config mapping (weekly vs monthly options)
    - Lot size retrieval with date-based logic
    - Volume baseline day selection
    - Pattern enablement checking
    - Risk per trade configuration
    """
    
    def __init__(self):
        """Initialize instrument config manager."""
        self.configs = INSTRUMENT_CONFIGS if INSTRUMENT_CONFIG_AVAILABLE else {}
        self.logger = logging.getLogger(__name__)
        
        if not INSTRUMENT_CONFIG_AVAILABLE:
            self.logger.warning("⚠️ Instrument config not available - using fallbacks")
    
    def get_config_for_symbol(self, symbol: str, indicators: Optional[Dict] = None) -> Optional[InstrumentConfig]:
        """
        Get instrument config for a symbol.
        
        Resolves weekly vs monthly options based on DTE (days to expiry).
        - DTE < 7: Weekly option
        - DTE >= 7: Monthly option
        
        Args:
            symbol: Trading symbol
            indicators: Optional indicators dict (for DTE lookup)
            
        Returns:
            InstrumentConfig or None if not found
        """
        if not INSTRUMENT_CONFIG_AVAILABLE:
            return None
        
        symbol_upper = symbol.upper()
        
        # Check for commodities first (exact match)
        if 'GOLD' in symbol_upper and 'GOLD' in self.configs:
            return self.configs['GOLD']
        if 'SILVER' in symbol_upper and 'SILVER' in self.configs:
            return self.configs['SILVER']
        
        # Check for options (CE/PE suffix)
        if symbol_upper.endswith('CE') or symbol_upper.endswith('PE'):
            # Try to determine weekly vs monthly based on DTE
            is_weekly = False
            if indicators:
                dte_days = indicators.get('trading_dte') or indicators.get('dte_days')
                if dte_days is not None:
                    is_weekly = int(dte_days) < 7
            
            # Extract underlying (e.g., NIFTY, BANKNIFTY)
            underlying = self._extract_underlying(symbol)
            
            if underlying:
                # Try weekly first if DTE < 7
                if is_weekly:
                    weekly_key = f"{underlying}_WEEKLY"
                    if weekly_key in self.configs:
                        return self.configs[weekly_key]
                
                # Fallback to monthly
                monthly_key = f"{underlying}_MONTHLY"
                if monthly_key in self.configs:
                    return self.configs[monthly_key]
        
        # Check for futures (FUT suffix)
        if 'FUT' in symbol_upper:
            underlying = self._extract_underlying(symbol)
            if underlying:
                monthly_key = f"{underlying}_MONTHLY"
                if monthly_key in self.configs:
                    return self.configs[monthly_key]
        
        return None
    
    def _extract_underlying(self, symbol: str) -> Optional[str]:
        """
        Extract underlying symbol from option/future symbol.
        
        Uses UniversalSymbolParser for consistent extraction across codebase.
        
        Examples:
        - NIFTY25NOV26000CE -> NIFTY
        - BANKNIFTY25NOV57900PE -> BANKNIFTY
        - NIFTY25NOVFUT -> NIFTY
        """
        try:
            from shared_core.redis_clients.redis_key_standards import get_symbol_parser
            parser = get_symbol_parser()
            underlying = parser.get_underlying(symbol)
            if underlying:
                return underlying.upper()
        except Exception:
            pass
        
        # Fallback: manual extraction for known underlyings
        symbol_upper = symbol.upper()
        for underlying in ['BANKNIFTY', 'NIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']:
            if underlying in symbol_upper:
                return underlying
        
        return None

    
    def get_lot_size(self, symbol: str, indicators: Optional[Dict] = None, use_future: bool = False) -> Optional[int]:
        """
        Get lot size for a symbol.
        
        Args:
            symbol: Trading symbol
            indicators: Optional indicators dict (for config resolution)
            use_future: If True, use future_lot_size (for 2026 changes)
            
        Returns:
            Lot size or None if not found
        """
        config = self.get_config_for_symbol(symbol, indicators)
        if config:
            if use_future:
                return config.future_lot_size
            return config.current_lot_size
        
        return None
    
    def get_baseline_days(self, symbol: str, indicators: Optional[Dict] = None) -> Optional[int]:
        """
        Get baseline days for volume calculation.
        
        Args:
            symbol: Trading symbol
            indicators: Optional indicators dict (for config resolution)
            
        Returns:
            Baseline days (7, 15, 20, etc.) or None if not found
        """
        config = self.get_config_for_symbol(symbol, indicators)
        if config:
            return config.baseline_days
        
        return None
    
    def get_volume_multiplier(self, symbol: str, indicators: Optional[Dict] = None) -> Optional[float]:
        """
        Get volume multiplier for threshold adjustment.
        
        Args:
            symbol: Trading symbol
            indicators: Optional indicators dict (for config resolution)
            
        Returns:
            Volume multiplier or None if not found
        """
        config = self.get_config_for_symbol(symbol, indicators)
        if config:
            return config.volume_multiplier
        
        return None
    
    def is_pattern_enabled(self, symbol: str, pattern_type: str, indicators: Optional[Dict] = None) -> bool:
        """
        Check if a pattern is enabled for a symbol.
        
        Args:
            symbol: Trading symbol
            pattern_type: Pattern type (e.g., 'order_flow_breakout')
            indicators: Optional indicators dict (for config resolution)
            
        Returns:
            True if pattern is enabled, False otherwise (defaults to True if config not found)
        """
        config = self.get_config_for_symbol(symbol, indicators)
        if config and config.enabled_patterns:
            # Normalize pattern type (handle variations)
            pattern_lower = pattern_type.lower()
            enabled_lower = [p.lower() for p in config.enabled_patterns]
            
            # Check exact match or partial match
            for enabled_pattern in enabled_lower:
                if enabled_pattern in pattern_lower or pattern_lower in enabled_pattern:
                    return True
            
            return False
        
        # Default to enabled if config not found (backward compatibility)
        return True
    
    def get_risk_per_trade(self, symbol: str, indicators: Optional[Dict] = None) -> Optional[float]:
        """
        Get risk per trade percentage for a symbol.
        
        Args:
            symbol: Trading symbol
            indicators: Optional indicators dict (for config resolution)
            
        Returns:
            Risk per trade (e.g., 0.01 for 1%) or None if not found
        """
        config = self.get_config_for_symbol(symbol, indicators)
        if config:
            return config.risk_per_trade
        
        return None
    
    def get_pattern_config(self, pattern_type: str) -> Optional[Dict]:
        """
        Get pattern-specific configuration.
        
        Args:
            pattern_type: Pattern type (e.g., 'game_theory_signal')
            
        Returns:
            Pattern config dict or None
        """
        try:
            from config.instrument_config import PATTERN_CONFIGS
            return PATTERN_CONFIGS.get(pattern_type)
        except ImportError:
            return None
    
    def get_atr_period(self, symbol: str, indicators: Optional[Dict] = None) -> Optional[int]:
        """
        Get ATR period for a symbol.
        
        Args:
            symbol: Trading symbol
            indicators: Optional indicators dict (for config resolution)
            
        Returns:
            ATR period or None if not found
        """
        config = self.get_config_for_symbol(symbol, indicators)
        if config:
            return config.atr_period
        
        return None
    
    def get_all_configs(self) -> Dict[str, InstrumentConfig]:
        """
        Get all instrument configs.
        
        Returns:
            Dictionary of all configs
        """
        return self.configs.copy()


# Singleton instance
_instrument_config_manager = None

def get_instrument_config_manager() -> InstrumentConfigManager:
    """Get singleton instance of InstrumentConfigManager."""
    global _instrument_config_manager
    if _instrument_config_manager is None:
        _instrument_config_manager = InstrumentConfigManager()
    return _instrument_config_manager

