# config/instrument_config.py
"""
Instrument Configuration Data
==============================

Pure data file - defines InstrumentConfig dataclass and INSTRUMENT_CONFIGS dictionary.
All logic is in instrument_config_manager.py.

✅ Use InstrumentConfigManager.get_config_for_symbol() for symbol resolution.
"""
from dataclasses import dataclass
from typing import List

@dataclass
class InstrumentConfig:
    """
    Instrument-specific configuration dataclass.
    
    Attributes:
        symbol: Base symbol (e.g., "NIFTY", "BANKNIFTY", "GOLD", "SILVER")
        instrument_type: Type of instrument (WEEKLY_OPTION, MONTHLY_OPTION, COMMODITY)
        current_lot_size: Current lot size (2025)
        future_lot_size: Future lot size (2026+)
        baseline_days: Volume baseline days (7 for weekly, 15 for commodities, 20 for monthly)
        volume_multiplier: Volume threshold multiplier
        atr_period: ATR calculation period
        enabled_patterns: List of enabled pattern types
        risk_per_trade: Risk per trade percentage (0.01 = 1%)
    """
    symbol: str
    instrument_type: str  # WEEKLY_OPTION, MONTHLY_OPTION, COMMODITY
    current_lot_size: int
    future_lot_size: int
    baseline_days: int
    volume_multiplier: float
    atr_period: int
    enabled_patterns: List[str]
    risk_per_trade: float

# Your focused instrument universe
INSTRUMENT_CONFIGS = {
    # NIFTY Weekly Options
    "NIFTY_WEEKLY": InstrumentConfig(
        symbol="NIFTY",
        instrument_type="WEEKLY_OPTION", 
        current_lot_size=75,
        future_lot_size=65,
        baseline_days=7,
        volume_multiplier=1.0,
        atr_period=14,
        enabled_patterns=["order_flow_breakout", "gamma_exposure_reversal", "kow_straddle", "game_theory_signal"],
        risk_per_trade=0.01
    ),
    
    # NIFTY Monthly Options
    "NIFTY_MONTHLY": InstrumentConfig(
        symbol="NIFTY", 
        instrument_type="MONTHLY_OPTION",
        current_lot_size=75,
        future_lot_size=65,
        baseline_days=20,
        volume_multiplier=1.2,
        atr_period=20,
        enabled_patterns=["order_flow_breakout", "gamma_exposure_reversal", "iron_condor", "kow_straddle", "game_theory_signal"],
        risk_per_trade=0.015
    ),
    
    # BANKNIFTY Monthly 
    "BANKNIFTY_MONTHLY": InstrumentConfig(
        symbol="BANKNIFTY",
        instrument_type="MONTHLY_OPTION", 
        current_lot_size=35,
        future_lot_size=30,
        baseline_days=20,
        volume_multiplier=1.5,
        atr_period=20,
        enabled_patterns=["order_flow_breakout", "gamma_exposure_reversal", "microstructure_divergence", "kow_straddle", "game_theory_signal"],
        risk_per_trade=0.012
    ),
    
    # Commodities
    "GOLD": InstrumentConfig(
        symbol="GOLD",
        instrument_type="COMMODITY",
        current_lot_size=1,
        future_lot_size=1,
        baseline_days=15,
        volume_multiplier=1.0,
        atr_period=14,
        enabled_patterns=["usdinr_arbitrage", "order_flow_breakout", "vix_momentum", "game_theory_signal"],
        risk_per_trade=0.008
    ),
    
    "SILVER": InstrumentConfig(
        symbol="SILVER",
        instrument_type="COMMODITY",
        current_lot_size=1,
        future_lot_size=1,
        baseline_days=15,
        volume_multiplier=1.0,
        atr_period=14,
        enabled_patterns=["usdinr_arbitrage", "order_flow_breakout", "vix_momentum", "game_theory_signal"],
        risk_per_trade=0.008
    )
}

# Pattern-specific configurations
# These settings apply to specific patterns across all instruments
PATTERN_CONFIGS = {
    "game_theory_signal": {
        "min_confidence": 0.7,
        "position_multiplier": 1.0
    }
}

# ✅ NOTE: Use InstrumentConfigManager.get_config_for_symbol() instead of this function
# This file is data-only - all logic is in instrument_config_manager.py