'''
Kow Signal Straddle Strategy: A Pure VWAP-Based Options Trading System

Strategy Overview
=================

The Kow Signal Straddle is an intraday options trading strategy designed for NIFTY and BANKNIFTY indices. 
It uses VWAP-based premium positioning to identify optimal entry and exit points for At-The-Money (ATM) straddles.

Core Concept
------------

A straddle involves simultaneously selling both a Call Option (CE) and Put Option (PE) at 
the same strike price. The strategy profits from premium decay and time decay.

✅ PURE VWAP-BASED STRATEGY:

Entry Logic (SELL Straddle):
- Primary: Both CE and PE premiums < their individual VWAPs (cheap premium)
- Secondary: Combined premium < average premium (historical average)
- Tertiary: Trend Analysis (underlying VWAP - prefer NEUTRAL/RANGE_BOUND)
- Confirmation: High confidence (≥85%) required

Exit Logic (BUY Back Straddle):
- Primary: Either CE or PE premium > their individual VWAP (expensive premium)
- Secondary: Combined premium > average premium
- Tertiary: 30% premium decay target achieved
- Safety: Stop loss at -₹1000 per lot

Key Features:
- Pure VWAP-Based: Premium positioning vs VWAP (no IV logic)
- Underlying VWAP: Trend detection (NOT option premium VWAP)
- Combined Premium VWAP: Timing confirmation (option premium VWAP)
- Individual Leg VWAPs: CE and PE VWAPs tracked separately
- Trades only during active market hours (9:30 AM - 3:15 PM)
- Requires high confidence (85%+) before generating signals

✅ ALERT PAYLOAD INTEGRATION:
- Uses UnifiedAlertBuilder.build_straddle_algo_payload() for algo-ready payloads
- Creates complete algo order payload with leg-specific stop loss and targets
- Stores payload in Redis DB1 with key: alert_payload:{symbol}:{pattern}:{timestamp_ms}
- Publishes to alerts:stream for real-time processing
- All alerts use standard human-readable format from HumanReadableAlertTemplates

Data Integration
----------------

Field Name Standardization:
- Uses standardized field names from optimized_field_mapping.yaml
- Primary fields: bucket_incremental_volume, last_price
- Fallback fields: volume, zerodha_cumulative_volume, current_price
- All tick data processing follows standard field mapping conventions

Redis Integration:
- DB 0 (system): Strategy state storage (position, entry_premium, current_strike)
- DB 1 (realtime): Alert publishing (alerts:stream), algo payloads (alert_payload:*)
- DB 2 (analytics): OHLC data lookup (ohlc_latest:* keys) for underlying price fallback
- All Redis keys follow standard naming patterns from redis_key_standards

Alert System Integration:
- Publishes signals to alerts:stream (DB 1) with binary JSON format
- Creates algo-ready payloads using UnifiedAlertBuilder.build_straddle_algo_payload()
- Uses orjson for fast binary serialization (falls back to standard json)
- Signal format includes all required fields for alert manager and algo executor:
  * symbol, pattern, pattern_type, confidence, signal, action
  * last_price, price, timestamp, timestamp_ms
  * volume_ratio, expected_move, strategy_type
  * Leg-specific stop loss and targets (CE and PE)
- Signals are automatically processed by alert manager and notifiers
- Telegram notifications sent using standard HumanReadableAlertTemplates
- Signal bot routing for Kow Signal patterns

End-to-End Flow:
1. Tick data received → Standardized field mapping
2. Combined premium calculated → VWAP computed from session start
3. Signal generated → All required fields included
4. Algo payload created → UnifiedAlertBuilder.build_straddle_algo_payload()
5. Published to alerts:stream → Binary JSON format
6. Stored in Redis → alert_payload:{symbol}:{pattern}:{timestamp_ms}
7. Alert manager processes → Confidence filtering
8. Notifiers dispatch → Telegram/other channels (standard format)
9. Dashboard displays → Real-time alert visualization

Dependencies:
- orjson (optional): Faster binary JSON serialization
- redis: Redis client for data storage and streams
- Standard library: asyncio, json, logging, datetime
- Project modules: patterns, alerts, config, redis_files
- UnifiedAlertBuilder: For algo-ready payload creation

Created: September 2025
Updated: January 2025 - Integrated UnifiedAlertBuilder for algo-ready payloads
'''
import asyncio
import pandas as pd
from typing import Dict, Optional, Tuple, Any, List
import logging
import time
import json
import threading
from datetime import datetime, time as dt_time, timedelta
# ✅ REMOVED: create_pattern import - using UnifiedAlertBuilder.build_straddle_algo_payload() instead

# Try to import orjson for faster binary serialization, fallback to json
try:
    import orjson
    HAS_ORJSON = True
    ORJSON_AVAILABLE = True  # Keep for backward compatibility
except ImportError:
    import json
    HAS_ORJSON = False
    ORJSON_AVAILABLE = False
    orjson = None
# ✅ FIX: Add project root to sys.path for alerts import (alerts moved to parent root)
import os
import sys
intraday_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # intraday_trading/
project_root = os.path.dirname(intraday_root)  # aion_algo_trading/
if project_root not in sys.path:
    sys.path.insert(0, project_root)
if intraday_root not in sys.path:
    sys.path.insert(0, intraday_root)

# ✅ FIX: Make MathDispatcher import conditional to handle path setup timing
try:
    from intraday_trading.intraday_scanner.math_dispatcher import MathDispatcher
    MATH_DISPATCHER_AVAILABLE = True
except ImportError:
    # Fallback: Try alternative import path
    try:
        # Try importing from intraday_trading/intraday_scanner
        import importlib.util
        math_dispatcher_path = os.path.join(intraday_root, "intraday_scanner", "math_dispatcher.py")
        if os.path.exists(math_dispatcher_path):
            spec = importlib.util.spec_from_file_location("math_dispatcher", math_dispatcher_path)
            if spec:
                math_dispatcher_module = importlib.util.module_from_spec(spec)
                if spec.loader:
                    spec.loader.exec_module(math_dispatcher_module)
            spec.loader.exec_module(math_dispatcher_module)
            MathDispatcher = math_dispatcher_module.MathDispatcher
            MATH_DISPATCHER_AVAILABLE = True
        else:
            MathDispatcher = None
            MATH_DISPATCHER_AVAILABLE = False
    except Exception:
        # Fallback: MathDispatcher will be imported later when paths are set up
        MathDispatcher = None
        MATH_DISPATCHER_AVAILABLE = False
from intraday_trading.patterns.pattern_mathematics import PatternMathematics
# ✅ FIXED: Use shared_core.config_utils.thresholds (single source of truth)
try:
    from shared_core.config_utils.thresholds import get_kow_signal_strategy_config, get_safe_vix_regime
except ImportError:
    # Final fallback: define minimal functions (no legacy fallback - shared_core is required)
    def get_kow_signal_strategy_config():
        return {}
    def get_safe_vix_regime(redis_client=None):
        return "NORMAL"
from datetime import datetime, time as dt_time, timedelta
# ⚠️ DEPRECATED: RiskManager replaced by UnifiedAlertBuilder
# from alerts.risk_manager import RiskManager
try:
    from alerts.risk_manager import RiskManager
    RISK_MANAGER_AVAILABLE = True
except ImportError:
    RiskManager = None
    RISK_MANAGER_AVAILABLE = False
from alerts.notifiers import TelegramNotifier

# ✅ CORRECTED: VWAP-based premium selling logic functions
def correct_straddle_alert_logic(symbol: str, ce_premium: float, pe_premium: float, 
                               ce_vwap: float, pe_vwap: float, combined_vwap: float,
                               trend: str, avg_premium: float) -> Dict[str, str]:
    """
    ✅ PURE VWAP-BASED: Determine straddle strategy based on PREMIUM vs VWAP positioning.
    
    For premium collection strategy, we ONLY sell straddles when:
    - Both CE & PE premiums are BELOW their VWAP (cheap entry)
    - Market is range-bound/consolidating
    - Premium is cheap relative to historical average
    
    Returns straddle metadata used by Kow Signal alerts and Telegram templates.
    """
    # Check if both legs are below VWAP (primary condition)
    ce_below_vwap = ce_premium > 0 and ce_premium < ce_vwap
    pe_below_vwap = pe_premium > 0 and pe_premium < pe_vwap
    both_below_vwap = ce_below_vwap and pe_below_vwap
    
    # Check if premium is cheap relative to average
    combined_premium = ce_premium + pe_premium
    premium_cheap = combined_premium < avg_premium if avg_premium > 0 else True
    
    # Market condition check
    trend_ok = trend in ["NEUTRAL", "RANGE_BOUND"]
    
    # ✅ ENTRY CONDITION: All conditions must be met for premium selling
    if both_below_vwap and premium_cheap and trend_ok:
        return {
            'action': 'SELL_STRADDLE',
            'strategy': 'PREMIUM_COLLECTION_STRADDLE',
            'description': f'Sell ATM straddle - Both CE & PE below VWAP (CE: {ce_premium:.1f} < {ce_vwap:.1f}, PE: {pe_premium:.1f} < {pe_vwap:.1f})',
            'reason': 'OPTIONS_CHEAP_BELOW_VWAP',
            'risk': 'Unlimited (use strict stop-loss)',
            'management': 'Collect theta decay, exit if either leg moves above VWAP or 30-50% premium decay achieved',
            'ideal_conditions': 'Range-bound market, both premiums below VWAP',
            'entry_logic': 'VWAP_BASED_PREMIUM_SELLING',
            'confidence_boosters': [
                f'CE below VWAP: {ce_premium:.1f} < {ce_vwap:.1f}',
                f'PE below VWAP: {pe_premium:.1f} < {pe_vwap:.1f}',
                f'Combined premium {combined_premium:.1f} < average {avg_premium:.1f}',
                f'Market trend: {trend} (ideal: NEUTRAL)'
            ]
        }
    
    # ❌ NO TRADE: Conditions not met for premium selling
    return {
        'action': 'NO_TRADE',
        'strategy': 'WAIT_FOR_BETTER_ENTRY',
        'description': 'Wait - Premium selling conditions not met',
        'reason': 'PREMIUMS_NOT_CHEAP_ENOUGH',
        'risk': 'N/A',
        'management': 'Monitor for both legs to drop below VWAP',
        'ideal_conditions': 'Both CE & PE below VWAP in range-bound market',
        'waiting_for': [
            f'CE to drop below {ce_vwap:.1f} (current: {ce_premium:.1f})',
            f'PE to drop below {pe_vwap:.1f} (current: {pe_premium:.1f})',
            f'Market to become range-bound (current: {trend})'
        ]
    }


def fix_kow_signal_straddle(alert_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    ✅ PURE VWAP-BASED: Normalize Kow Signal Straddle alerts with VWAP-based premium selling logic.
    
    Uses actual premium vs VWAP comparison - no IV or expected move logic.
    """
    if not isinstance(alert_data, dict):
        return alert_data
    
    pattern_label = str(alert_data.get('pattern_type') or alert_data.get('pattern') or '').lower()
    strategy_type = str(alert_data.get('strategy_type', '') or '').lower()
    action = str(alert_data.get('action', '') or '').upper()
    
    # Only process Kow Signal straddle patterns
    if ('kow_signal' not in pattern_label and strategy_type != 'kow_signal_straddle'):
        return alert_data
    
    # Don't modify exit/re-entry signals
    if action.startswith(('EXIT', 'REENTER', 'STOP', 'CLOSE', 'NO_TRADE')):
        return alert_data
    
    # Extract premium and VWAP data from alert
    ce_premium = alert_data.get('ce_premium', 0)
    pe_premium = alert_data.get('pe_premium', 0)
    current_vwap = alert_data.get('current_vwap', 0)
    avg_premium = alert_data.get('avg_premium', 0)
    trend = alert_data.get('trend', 'UNKNOWN')
    
    # Calculate individual leg VWAP (approximate as half of combined VWAP)
    ce_vwap = current_vwap / 2 if current_vwap > 0 else 0
    pe_vwap = current_vwap / 2 if current_vwap > 0 else 0
    
    # Get corrected strategy logic
    symbol = alert_data.get('symbol', '')
    corrected = correct_straddle_alert_logic(
        symbol=symbol,
        ce_premium=ce_premium,
        pe_premium=pe_premium,
        ce_vwap=ce_vwap,
        pe_vwap=pe_vwap,
        combined_vwap=current_vwap,
        trend=trend,
        avg_premium=avg_premium
    )
    
    # Update alert data with corrected strategy
    alert_data.update(corrected)
    
    # Set signal direction based on action
    if corrected.get('action') == 'SELL_STRADDLE':
        alert_data['signal'] = 'SELL'
    else:
        alert_data['signal'] = 'WAIT'
    
    # Copy description to all relevant fields for template consistency
    strategy_desc = corrected.get('description')
    if strategy_desc:
        alert_data['description'] = strategy_desc
        alert_data['trading_instruction'] = strategy_desc
        alert_data['strategy_description'] = strategy_desc
        if not alert_data.get('reason'):
            alert_data['reason'] = corrected.get('reason', strategy_desc)
    
    # Strategy context
    strategy_context = corrected.get('strategy')
    if strategy_context:
        alert_data['strategy_context'] = strategy_context
        alert_data['strategy'] = strategy_context
    
    # Risk management text
    risk_text = corrected.get('risk')
    management_text = corrected.get('management')
    summary_parts = []
    
    if risk_text:
        summary_parts.append(f"Risk: {risk_text}")
        alert_data['risk'] = risk_text
    
    if management_text:
        summary_parts.append(f"Management: {management_text}")
        alert_data['management'] = management_text
    
    if summary_parts:
        combined_summary = " | ".join(summary_parts)
        if alert_data.get('risk_summary'):
            alert_data['risk_summary'] = f"{alert_data['risk_summary']}\n{combined_summary}"
        else:
            alert_data['risk_summary'] = combined_summary
    
    # Add confidence boosters if available
    if 'confidence_boosters' in corrected:
        alert_data['confidence_boosters'] = corrected['confidence_boosters']
    
    # Add what we're waiting for if no trade
    if 'waiting_for' in corrected:
        alert_data['waiting_for'] = corrected['waiting_for']
    
    # Add entry logic type
    if 'entry_logic' in corrected:
        alert_data['entry_logic'] = corrected['entry_logic']
    
    # Add straddle-specific risk management
    alert_data = add_premium_selling_risk_management(alert_data)
    
    return alert_data

def add_premium_selling_risk_management(alert_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    ✅ PURE VWAP-BASED: Inject risk management specifically for PREMIUM SELLING straddles.
    """
    if not isinstance(alert_data, dict):
        return alert_data
    
    action = str(alert_data.get('action', '') or '').upper()
    
    # Only apply to SELL_STRADDLE actions
    if action != 'SELL_STRADDLE':
        return alert_data
    
    # Get premium data for risk calculation
    combined_premium = alert_data.get('entry_premium')
    if combined_premium in (None, 0):
        combined_premium = alert_data.get('price') or alert_data.get('last_price') or 0
    
    try:
        premium_points = max(0.0, float(combined_premium or 0.0))
    except (TypeError, ValueError):
        premium_points = 0.0
    
    # Get lot size
    lot_ref = alert_data.get('lot_size') or alert_data.get('quantity') or 1
    try:
        lot_size = max(1.0, float(lot_ref))
    except (TypeError, ValueError):
        lot_size = 1.0
    
    premium_rupees = float(alert_data.get('entry_premium_rupees') or (premium_points * lot_size))
    
    if premium_rupees == 0.0:
        return alert_data
    
    # ✅ PREMIUM SELLING RISK PARAMETERS
    stop_rupees = float(alert_data.get('stop_loss_rupees') or (premium_rupees * 1.5))  # 50% premium increase
    target_rupees = float(alert_data.get('target_rupees') or (premium_rupees * 0.7))   # 30% premium decay
    
    alert_data['risk_metrics'] = {
        'max_loss': 'Unlimited (use stop-loss)',
        'max_profit': premium_rupees,
        'margin_required': premium_rupees * 3,  # Rough margin estimate
        'stop_loss_type': 'PREMIUM_EXPANSION',
        'stop_loss_level': stop_rupees,
        'profit_target': target_rupees,
        'premium_decay_target': '30-50%',
        'exit_conditions': [
            'Either CE or PE moves above VWAP',
            '30-50% premium decay achieved',
            'Stop loss at 50% premium increase'
        ],
        'greeks_management': {
            'theta_positive': True,  # Time decay helps
            'delta_neutral': True,   # Aim for delta neutrality
            'gamma_risk': 'Monitor gamma near expiry'
        },
        'ideal_conditions': 'Range-bound market, both premiums below VWAP',
        'risk_reward_ratio': f"1:{target_rupees/premium_rupees:.1f}"
    }
    
    alert_data['position_management'] = (
        f"Short straddle: Collect ₹{premium_rupees:,.0f} premium. "
        f"Target: ₹{target_rupees:,.0f} (30% decay). "
        f"Stop: ₹{stop_rupees:,.0f} (50% increase). "
        "Exit if either leg moves above VWAP."
    )
    
    # Add to risk summary
    extra_summary = (
        f"Premium: ₹{premium_rupees:,.0f} | "
        f"Target: ₹{target_rupees:,.0f} | "
        f"Stop: ₹{stop_rupees:,.0f} | "
        f"R:R = 1:{(target_rupees/premium_rupees):.1f}"
    )
    
    existing_summary = alert_data.get('risk_summary', '')
    if extra_summary not in existing_summary:
        if existing_summary:
            alert_data['risk_summary'] = f"{existing_summary}\n{extra_summary}"
        else:
            alert_data['risk_summary'] = extra_summary
    
    return alert_data

# ✅ FIX: Make fix_kow_signal_straddle available
FIX_KOW_SIGNAL_AVAILABLE = True

class KowSignalStraddleStrategy:
    """
    Intraday Combined VWAP Straddle Strategy for Nifty/BankNifty
    
    This strategy uses VWAP of combined premium (CE+PE) with session anchoring to generate
    entry and exit signals for ATM straddles. Fully integrated with the alert system.
    
    Key Features:
    - VWAP-based signal generation from session start (9:15 AM)
    - Standardized field name mappings (optimized_field_mapping.yaml)
    - Redis integration (DB 0: state, DB 1: alerts, DB 2: analytics)
    - Alert system integration (alerts:stream, Telegram, dashboard)
    - Binary JSON serialization for fast alert publishing
    
    Signal Format:
    All signals include required fields for alert manager compatibility:
    - Core: symbol, pattern, confidence, signal, action, last_price, timestamp
    - Strategy: strike, ce_symbol, pe_symbol, entry_premium, current_vwap
    - Metadata: volume_ratio, expected_move, strategy_type
    
    Redis Storage:
    - Strategy state: DB 0 (strategy:kow:* keys)
    - Alert publishing: DB 1 (alerts:stream)
    - Market data: DB 1 (market:nifty_spot, market:banknifty_spot)
    """
    
    # NSE Month Code Mapping for weekly options
    NSE_MONTH_CODES = {
        'A': 'JAN', 'B': 'FEB', 'C': 'MAR', 'D': 'APR',
        'E': 'MAY', 'F': 'JUN', 'G': 'JUL', 'H': 'AUG',
        'I': 'SEP', 'J': 'OCT', 'K': 'NOV', 'L': 'DEC',
        
        # Some older codes
        'M': 'JAN', 'N': 'FEB', 'O': 'MAR', 'P': 'APR',
        'Q': 'MAY', 'R': 'JUN', 'S': 'JUL', 'T': 'AUG',
        'U': 'SEP', 'V': 'OCT', 'W': 'NOV', 'X': 'DEC'
    }

    def __init__(self, redis_client=None, config: Dict = None):
        """Initialize Kow Signal Straddle Strategy with Redis client"""
        # Initialize Redis client using RedisClientFactory
        if redis_client is None:
            try:
                from shared_core.redis_clients.redis_client import RedisClientFactory
                # Use DB 1 for realtime data, DB 0 for strategy state
                self.redis_realtime = RedisClientFactory.get_trading_client()
                self.redis_system = RedisClientFactory.get_trading_client()
                self.redis = self.redis_realtime  # Default to realtime for backward compatibility
            except Exception as e:
                logging.error(f"Failed to initialize Redis client: {e}")
                self.redis_realtime = None
                self.redis_system = None
                self.redis = None
        else:
            self.redis = redis_client
            self.redis_realtime = redis_client
            # ✅ FIXED: Handle wrapper clients that delegate to underlying client
            if hasattr(redis_client, 'get_client'):
                # It's a wrapper - get the actual client for system operations
                self.redis_system = redis_client.get_client(0) if hasattr(redis_client, 'get_client') else redis_client
            elif hasattr(redis_client, '_db0'):
                # PooledRedisWrapper - use _db0 for system operations
                self.redis_system = redis_client._db0
            else:
                # Direct Redis client
                self.redis_system = redis_client
        
        self.logger = logging.getLogger(__name__)
        
        # ✅ GRACEFUL SHUTDOWN: Add running flag for graceful thread shutdown
        self.running = True
        self._shutdown_event = threading.Event()
        
        # ✅ NEW: Initialize VolumeComputationManager for dynamic baseline integration
        try:
            from shared_core.volume_files.volume_computation_manager import VolumeComputationManager
            # Use redis_realtime (DB 1) for volume baseline calculations
            self.volume_manager = VolumeComputationManager(redis_client=self.redis_realtime)
            self.logger.debug("✅ [KOW_SIGNAL] VolumeComputationManager initialized for baseline integration")
        except Exception as e:
            self.logger.warning(f"⚠️ [KOW_SIGNAL] Failed to initialize VolumeComputationManager: {e}")
            self.volume_manager = None
        
        # ✅ FIXED: Store redis client for direct baseline reads (no UnifiedTimeAwareBaseline dependency)
        # We use direct Redis reads with canonical_symbol instead of UnifiedTimeAwareBaseline
        self._baseline_redis_client = self.redis_realtime
        
        # Load configuration
        try:
            # ✅ FIXED: Use config.thresholds (relative import, same as pattern_detector.py)
            from shared_core.config_utils.thresholds import get_kow_signal_strategy_config
            self.config = config or get_kow_signal_strategy_config()
        except ImportError:
            self.config = config or {
                'entry_time': '09:30',
                'exit_time': '15:15',
                'confirmation_periods': 1,
                'max_reentries': 3,
                'underlying_symbols': ['NIFTY', 'BANKNIFTY'],
                'strike_selection': 'atm',
                'max_risk_per_trade': 0.03,
                'confidence_threshold': 0.85,
                'telegram_routing': True,
                'lot_size': 50
            }
        
        # Strategy state - per underlying (supports multiple underlyings simultaneously)
        self.position_states = {}  # {underlying: position_state_dict}
        self.combined_premium_data = {}  # {underlying: [data_list]}
        self.ce_premiums = {}  # {underlying: premium}
        self.pe_premiums = {}  # {underlying: premium}
        self.ce_volumes = {}  # {underlying: volume}
        self.pe_volumes = {}  # {underlying: volume}
        self.atm_strikes = {}  # {underlying: strike}
        # ✅ NEW: Individual leg VWAP tracking for corrected logic
        self.ce_premium_data = {}  # {underlying: [{'timestamp', 'premium', 'volume'}]}
        self.pe_premium_data = {}  # {underlying: [{'timestamp', 'premium', 'volume'}]}
        
        # Initialize per-underlying tracking
        underlying_symbols = self.config.get('underlying_symbols', ['NIFTY', 'BANKNIFTY'])
        for underlying in underlying_symbols:
            self.position_states[underlying] = {
                'current_position': None,  # 'BOTH_LEGS', 'CE_ONLY', 'PE_ONLY', 'NONE'
                'entry_time': None,
                'entry_combined_premium': 0.0,
                'reentry_count': 0,
                'current_profit': 0.0,
                'stop_loss_level': 1000,
                'current_strike': None,
                # Alert gating - prevent duplicate spam
                'last_alert_action': None,
                'last_alert_time': None,
                'alert_cooldown_seconds': 30,
            }
            self.combined_premium_data[underlying] = []
            self.ce_premiums[underlying] = None
            self.pe_premiums[underlying] = None
            self.ce_volumes[underlying] = 0
            self.pe_volumes[underlying] = 0
            self.atm_strikes[underlying] = None
            # ✅ NEW: Initialize individual leg premium data
            self.ce_premium_data[underlying] = []
            self.pe_premium_data[underlying] = []
        
        # Legacy single-strike tracking (for backward compatibility)
        self.current_atm_strike = None
        
        # Underlying price tracking (fix missing attribute)
        self.underlying_prices = []  # Stores {'timestamp', 'price', 'symbol', 'underlying'}
        
        # ✅ NEW: Underlying VWAP tracking for trend signals (not option premium VWAP)
        self.underlying_price_volume_data = {}  # {underlying: [{'timestamp', 'price', 'volume'}]}
        self._invalid_price_log = {}
        for underlying in underlying_symbols:
            self.underlying_price_volume_data[underlying] = []
        
        # IV tracking removed - strategy is pure VWAP-based
        
        # Leg tracking for P&L
        self.ce_entry_price = None
        self.pe_entry_price = None
        self.ce_exit_time = None
        self.pe_exit_time = None
        
        # Initialize risk manager and math dispatcher
        # ⚠️ DEPRECATED: RiskManager replaced by UnifiedAlertBuilder
        if RISK_MANAGER_AVAILABLE:
            self.risk_manager = RiskManager(redis_client=self.redis_realtime)
        else:
            self.risk_manager = None
        # ✅ ENHANCED: Pass redis_client for VIX fetching from Redis DB1
        # ✅ FIX: Import MathDispatcher here if not available at module level
        math_dispatcher_class = MathDispatcher
        if not MATH_DISPATCHER_AVAILABLE or math_dispatcher_class is None:
            try:
                from intraday_trading.intraday_scanner.math_dispatcher import MathDispatcher as MathDispatcherClass
                math_dispatcher_class = MathDispatcherClass
            except ImportError:
                self.logger.warning("MathDispatcher not available - Kow Signal confidence calculations may be limited")
                math_dispatcher_class = None
        
        if math_dispatcher_class is not None:
            self.math_dispatcher = math_dispatcher_class(PatternMathematics, self.risk_manager, redis_client=self.redis_realtime)
        else:
            self.math_dispatcher = None
            self.logger.warning("MathDispatcher not initialized for Kow Signal - using fallback")
        self.pattern_math = PatternMathematics()
        
        # Initialize notifier
        self.notifier = TelegramNotifier()
        
        # ✅ FIX: Mark initialization as starting (before loading state)
        self._initialization_complete = False
        
        # Load strategy state from Redis
        self._load_strategy_state_from_redis()
        
        # ✅ FIX: Mark initialization as complete to enable fallback price fetching
        self._initialization_complete = True
        
        self.logger.info("Kow Signal Straddle Strategy initialized")

    def stabilize_atm_strike(self, underlying_symbol: str, underlying_price: float) -> int:
        """
        🔥 CRITICAL FIX: Stabilize ATM strike calculation with rounding and hysteresis
        
        Problem: ATM strike keeps flipping (26050 ↔ 26200) causing options to be rejected.
        Solution: Use consistent rounding and add hysteresis to prevent rapid flipping.
        """
        # ✅ FIX 1: Use consistent rounding based on instrument
        rounding_multiples = {
            'NIFTY': 50,
            'BANKNIFTY': 100,
            'FINNIFTY': 50,
            'MIDCPNIFTY': 50,
        }
        
        multiple = rounding_multiples.get(underlying_symbol, 50)
        
        # ✅ FIX 2: Calculate nearest strike
        nearest_strike = round(underlying_price / multiple) * multiple
        
        # ✅ FIX 3: Add hysteresis - don't flip if price change is small
        # Store last calculated strike and price for each underlying
        if not hasattr(self, '_last_atm_strike'):
            self._last_atm_strike = {}
        if not hasattr(self, '_last_atm_price'):
            self._last_atm_price = {}
        
        last_strike = self._last_atm_strike.get(underlying_symbol)
        last_price = self._last_atm_price.get(underlying_symbol)
        
        if last_strike and last_price:
            # Compare current price to last price (not strike to price)
            price_diff_pct = abs(underlying_price - last_price) / last_price if last_price > 0 else 1.0
            
            # Only update strike if price moved > 0.5% (for indices) or 1% (for stocks)
            threshold = 0.005 if underlying_symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY'] else 0.01
            
            if price_diff_pct < threshold:
                # Price didn't move enough - keep last strike
                self.logger.debug(f"🔒 [ATM_STABILIZE] {underlying_symbol} - Keeping last ATM strike {last_strike} (price change: {price_diff_pct*100:.2f}% < {threshold*100:.1f}%)")
                return last_strike
        
        # ✅ FIX 4: Update and log
        self._last_atm_strike[underlying_symbol] = nearest_strike
        self._last_atm_price[underlying_symbol] = underlying_price
        
        self.logger.info(f"🎯 [ATM_STABILIZE] {underlying_symbol} - New ATM strike: {nearest_strike} (price: {underlying_price:.2f}, multiple: {multiple})")
        
        return nearest_strike

    def _get_atm_strike(self, underlying_price: float, underlying: str = None) -> int:
        """
        Calculate ATM strike based on current underlying price and underlying symbol.
        ✅ UPDATED: Now uses stabilize_atm_strike() for consistent rounding and hysteresis.
        """
        if underlying is None:
            # Try to determine from underlying_symbols
            underlying = self.config.get('underlying_symbols', ['NIFTY'])[0]
        
        # Normalize underlying symbol name
        underlying_upper = underlying.upper()
        if "BANK" in underlying_upper:
            underlying_symbol = 'BANKNIFTY'
        elif "FIN" in underlying_upper:
            underlying_symbol = 'FINNIFTY'
        elif "MIDCP" in underlying_upper:
            underlying_symbol = 'MIDCPNIFTY'
        else:
            underlying_symbol = 'NIFTY'
        
        # Use stabilized ATM strike calculation
        return self.stabilize_atm_strike(underlying_symbol, underlying_price)

    def _calculate_position_size(self, underlying: str, confidence: float) -> float:
        """
        ✅ ENHANCED: Calculate position size with time-based adjustments from RiskManager.
        
        Applies time-based position rules:
        - 2:30 PM: Reduce position size by 50% (trimming phase)
        - 2:45 PM: Stop new entries
        - 3:15 PM: Exit all positions
        - Expiry day: 50% position size
        
        Args:
            underlying: Underlying symbol (NIFTY/BANKNIFTY)
            confidence: Signal confidence (0.0 to 1.0)
            
        Returns:
            Adjusted position size (lot_size) with time-based multipliers applied
        """
        # Get base lot size
        base_lot_size = float(self._get_lot_size(underlying))
        
        # ✅ REMOVED: Time-based position rules from RiskManager (method doesn't exist)
        # Apply confidence-based adjustment
        confidence_multiplier = confidence  # Scale by confidence
        final_lot_size = base_lot_size * confidence_multiplier
        
        # Ensure minimum lot size of 1 if position is allowed
        final_lot_size = max(1.0, final_lot_size) if final_lot_size > 0 else 0.0
        
        return final_lot_size
    
    def _get_lot_size(self, underlying: str, indicators: Optional[Dict] = None) -> int:
        """
        ✅ ENHANCED: Use instrument_config.py with fallback hierarchy.
        
        Returns lot size from config hierarchy:
        1. config.instrument_config_manager (instrument_config.py) - NEW
        2. shared_core.config_utils.thresholds.get_greek_thresholds() (BANKNIFTY=35, NIFTY=75)
        3. self.config.get('lot_size')
        4. Fallback to hardcoded defaults
        """
        try:
            # ✅ FIXED: Use thresholds.py get_instrument_config() instead of config.instrument_config_manager
            from shared_core.config_utils.thresholds import get_instrument_config
            instrument_config = get_instrument_config(underlying)
            lot_size = instrument_config.get('lot_size')
            if lot_size is not None:
                return int(lot_size)
        except Exception as e:
            self.logger.debug(f"⚠️ [KOW_SIGNAL] {underlying} Instrument config not available: {e}")
        
        # Fallback to existing logic
        try:
            from shared_core.config_utils.thresholds import get_greek_thresholds
            greek_config = get_greek_thresholds(underlying)
            
            # Get from config hierarchy
            lot_size = (
                greek_config.get('lot_size') or 
                self.config.get('lot_size') or 
                self._get_fallback_lot_size(underlying)
            )
            
            return int(lot_size) if lot_size else self._get_fallback_lot_size(underlying)
        except Exception as e:
            self.logger.warning(f"⚠️ [KOW_SIGNAL] {underlying} Lot size error, using fallback: {e}")
            return self._get_fallback_lot_size(underlying)
    
    def _get_fallback_lot_size(self, underlying: str) -> int:
        """Fallback lot size calculation when shared_core is unavailable."""
        under = (underlying or '').upper()
        config_lot_sizes = self.config.get('lot_sizes', {})
        default_config = int(self.config.get('lot_size', 75))
        base_defaults = {
            'NIFTY': 75,
            'BANKNIFTY': 35,
            'DEFAULT': default_config,
        }
        merged = {**base_defaults, **{k.upper(): v for k, v in config_lot_sizes.items()}}
        if 'BANKNIFTY' in under or 'BANK' in under:
            return int(merged.get('BANKNIFTY', merged.get('DEFAULT', default_config)))
        if 'NIFTY' in under:
            return int(merged.get('NIFTY', merged.get('DEFAULT', default_config)))
        return int(merged.get('DEFAULT', default_config))

    def _is_valid_underlying_price(self, underlying: str, price: float) -> bool:
        """Validate underlying price to avoid 100x scaling errors."""
        if price is None or price <= 0:
            return False
        under = (underlying or "").upper()
        if "BANKNIFTY" in under or "BANK" in under:
            return 5000 <= price <= 200000
        if "NIFTY" in under:
            return 1000 <= price <= 100000
        return price > 1

    def _record_underlying_price(self, underlying_name: Optional[str], price: float, volume: float = 0.0, source_symbol: Optional[str] = None):
        """Update ATM strikes, in-memory caches, and Redis with latest underlying price."""
        if not underlying_name or not self._is_valid_underlying_price(underlying_name, price):
            self._log_invalid_underlying_price(underlying_name, f"⚠️ [KOW_SIGNAL] Ignoring unrealistic underlying price {price} for {underlying_name}", volume)
            return
        underlying_name = underlying_name.upper()
        old_strike = self.atm_strikes.get(underlying_name)
        new_strike = self._get_atm_strike(price, underlying_name)
        self.atm_strikes[underlying_name] = new_strike
        if old_strike != new_strike:
            # ✅ DEBUG: Log ATM strike calculation with more detail (INFO level so it shows up)
            self.logger.info(
                f"🔍 [KOW_SIGNAL] ATM strike updated for {underlying_name}: {old_strike} -> {new_strike} "
                f"(underlying_price={price:.2f}, source_symbol={source_symbol})"
            )
        if self.current_atm_strike is None:
            self.current_atm_strike = new_strike
        symbol_label = source_symbol or underlying_name
        self.underlying_prices.append({
            'timestamp': datetime.now(),
            'price': price,
            'symbol': symbol_label,
            'underlying': underlying_name
        })
        cutoff_time = datetime.now() - timedelta(hours=1)
        self.underlying_prices = [p for p in self.underlying_prices if p['timestamp'] > cutoff_time]
        if underlying_name not in self.underlying_price_volume_data:
            self.underlying_price_volume_data[underlying_name] = []
        self.underlying_price_volume_data[underlying_name].append({
            'timestamp': datetime.now(),
            'price': price,
            'volume': volume if volume and volume > 0 else 1
        })
        cutoff_time_vol = datetime.now() - timedelta(hours=4)
        self.underlying_price_volume_data[underlying_name] = [
            p for p in self.underlying_price_volume_data[underlying_name]
            if p['timestamp'] > cutoff_time_vol
        ]
        if self.redis_realtime:
            try:
                spot_key = f"market:{underlying_name.lower()}_spot"
                price_str = f"{price:.2f}"
                self.redis_realtime.set(spot_key, price_str)
                self.redis_realtime.expire(spot_key, 300)
                self.logger.debug(f"✅ [KOW_SIGNAL] Stored {underlying_name} spot price: {price} (key: {spot_key})")
            except Exception as e:
                self.logger.warning(f"Error storing spot price for {underlying_name}: {e}")
    
    def _log_invalid_underlying_price(self, underlying_name: Optional[str], message: str, volume: float = 0.0):
        """
        Throttle logging of invalid price warnings.
        
        ✅ FIX: Only log warnings during active trading (volume > 0), not during initialization.
        This prevents noisy warnings when loading stale data from Redis during startup.
        """
        if not underlying_name:
            return
        
        # ✅ FIX: Only log warnings if we have actual tick data (volume > 0)
        # During initialization, we might load stale/invalid prices from Redis, which is expected
        if volume == 0.0 and not hasattr(self, '_initialization_complete'):
            # During initialization with no volume, use debug level instead of warning
            self.logger.debug(message)
            return
        
        interval = self.config.get('invalid_price_log_interval', 15)
        now = datetime.now()
        last = self._invalid_price_log.get(underlying_name)
        if not last or (now - last).total_seconds() >= interval:
            self.logger.warning(message)
            self._invalid_price_log[underlying_name] = now

    def _calculate_combined_vwap(self, underlying: str) -> float:
        """
        ✅ ENHANCED: Calculate VWAP for combined premium with baseline integration.
        
        Uses VolumeComputationManager to get dynamic baseline for volume validation.
        Filters low-volume data points below baseline threshold for more accurate VWAP.
        """
        if underlying not in self.combined_premium_data or not self.combined_premium_data[underlying]:
            self.logger.debug(f"⚠️ [KOW_SIGNAL] {underlying} No combined premium data available for VWAP calculation")
            return 0.0
        
        total_data_points = len(self.combined_premium_data[underlying])
        
        # Filter data from session start (9:15 AM)
        session_start = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
        session_data = [d for d in self.combined_premium_data[underlying] 
                       if d['timestamp'] >= session_start]
        
        if not session_data:
            self.logger.debug(f"⚠️ [KOW_SIGNAL] {underlying} No session data for VWAP (total_data={total_data_points}, session_start={session_start})")
            return 0.0
        
        # ✅ FIXED: Get baseline directly from Redis using canonical_symbol (preserves all data points)
        baseline_volume = 0.0
        if self._baseline_redis_client:
            try:
                # Use canonical_symbol for Redis key lookup (ensures data consistency)
                from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
                
                # Get canonical symbol for underlying (e.g., "NIFTY" -> "NIFTY" or "NFO:NIFTY")
                canonical_underlying = RedisKeyStandards.canonical_symbol(underlying)
                
                # Read baseline directly from Redis (no time-aware multipliers needed for filtering)
                baseline_key = DatabaseAwareKeyBuilder.live_volume_baseline(canonical_underlying)
                baseline_data = self._baseline_redis_client.hgetall(baseline_key)
                
                if baseline_data:
                    # Decode bytes if needed
                    if isinstance(list(baseline_data.values())[0], bytes):
                        baseline_data = {
                            k.decode() if isinstance(k, bytes) else k:
                            v.decode() if isinstance(v, bytes) else v
                            for k, v in baseline_data.items()
                        }
                    
                    # Priority: baseline_1min > current > avg_volume_55d/375
                    baseline_1min = baseline_data.get('baseline_1min')
                    if baseline_1min:
                        baseline_volume = float(baseline_1min)
                    else:
                        current = baseline_data.get('current')
                        if current:
                            baseline_volume = float(current)
                        else:
                            avg_vol_55d = baseline_data.get('avg_volume_55d')
                            if avg_vol_55d:
                                baseline_volume = float(avg_vol_55d) / 375.0  # Trading minutes per day
                
                # ✅ PRESERVE ALL DATA POINTS: Only filter if baseline is very high (quality check, not aggressive filtering)
                # Use 20% threshold instead of 50% to preserve more data points for pattern confirmation
                if baseline_volume > 0:
                    min_volume_threshold = baseline_volume * 0.2  # 20% of baseline (less aggressive)
                    original_count = len(session_data)
                    filtered_data = [d for d in session_data if d.get('volume', 0) >= min_volume_threshold]
                    
                    # Only use filtered data if we still have at least 50% of original data points
                    if filtered_data and len(filtered_data) >= (original_count * 0.5):
                        session_data = filtered_data
                        self.logger.debug(
                            f"🔍 [KOW_SIGNAL] {underlying} Baseline filter applied (preserved {len(filtered_data)}/{original_count} data points): "
                            f"baseline={baseline_volume:.0f}, min_threshold={min_volume_threshold:.0f}"
                        )
                    else:
                        # Preserve all data if filtering would remove too many points
                        self.logger.debug(
                            f"✅ [KOW_SIGNAL] {underlying} Preserving all {original_count} data points "
                            f"(filtering would remove too many: {len(filtered_data) if filtered_data else 0}/{original_count})"
                        )
            except Exception as e:
                self.logger.debug(f"⚠️ [KOW_SIGNAL] {underlying} Baseline lookup failed: {e}, using all session data")
        
        session_data_count = len(session_data)
        cumulative_pv = sum(d['premium'] * d['volume'] for d in session_data)
        cumulative_volume = sum(d['volume'] for d in session_data)
        
        # ✅ ENHANCED: Validate VWAP against baseline (minimum volume requirement)
        if baseline_volume > 0 and cumulative_volume < baseline_volume * 0.1:
            self.logger.debug(
                f"⚠️ [KOW_SIGNAL] {underlying} VWAP may be unreliable: "
                f"cumulative_volume={cumulative_volume:.0f} < 10% of baseline={baseline_volume:.0f}"
            )
        
        vwap = cumulative_pv / cumulative_volume if cumulative_volume else 0.0
        
        self.logger.debug(
            f"🔍 [KOW_SIGNAL] {underlying} VWAP calculated: {vwap:.2f} "
            f"(data_points={session_data_count}, total_vol={cumulative_volume:.0f}, "
            f"baseline={baseline_volume:.0f}, cum_pv={cumulative_pv:.2f})"
        )
        
        return vwap
    
    def _calculate_ce_vwap(self, underlying: str) -> float:
        """
        ✅ ENHANCED: Calculate VWAP for CE premium with baseline integration.
        
        Uses VolumeComputationManager to validate volume data quality.
        """
        if underlying not in self.ce_premium_data or not self.ce_premium_data[underlying]:
            return 0.0
        
        session_start = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
        session_data = [d for d in self.ce_premium_data[underlying] 
                       if d['timestamp'] >= session_start]
        
        if not session_data:
            return 0.0
        
        # ✅ FIXED: Get baseline directly from Redis using canonical_symbol (preserves all data points)
        baseline_volume = 0.0
        if self._baseline_redis_client:
            try:
                from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
                
                # Get canonical symbol for underlying
                canonical_underlying = RedisKeyStandards.canonical_symbol(underlying)
                baseline_key = DatabaseAwareKeyBuilder.live_volume_baseline(canonical_underlying)
                baseline_data = self._baseline_redis_client.hgetall(baseline_key)
                
                if baseline_data:
                    # Decode bytes if needed
                    if isinstance(list(baseline_data.values())[0], bytes):
                        baseline_data = {
                            k.decode() if isinstance(k, bytes) else k:
                            v.decode() if isinstance(v, bytes) else v
                            for k, v in baseline_data.items()
                        }
                    
                    # Priority: baseline_1min > current > avg_volume_55d/375
                    baseline_1min = baseline_data.get('baseline_1min')
                    if baseline_1min:
                        baseline_volume = float(baseline_1min)
                    else:
                        current = baseline_data.get('current')
                        if current:
                            baseline_volume = float(current)
                        else:
                            avg_vol_55d = baseline_data.get('avg_volume_55d')
                            if avg_vol_55d:
                                baseline_volume = float(avg_vol_55d) / 375.0
                
                # ✅ PRESERVE ALL DATA POINTS: Only filter if baseline is very high (20% threshold, not 50%)
                if baseline_volume > 0:
                    min_volume_threshold = baseline_volume * 0.2  # 20% threshold (less aggressive)
                    original_count = len(session_data)
                    filtered_data = [d for d in session_data if d.get('volume', 0) >= min_volume_threshold]
                    
                    # Only use filtered data if we still have at least 50% of original data points
                    if filtered_data and len(filtered_data) >= (original_count * 0.5):
                        session_data = filtered_data
                        self.logger.debug(
                            f"🔍 [KOW_SIGNAL] {underlying} CE baseline filter applied (preserved {len(filtered_data)}/{original_count} data points)"
                        )
            except Exception as e:
                self.logger.debug(f"⚠️ [KOW_SIGNAL] {underlying} CE baseline lookup failed: {e}, using all session data")
        
        cumulative_pv = sum(d['premium'] * d['volume'] for d in session_data)
        cumulative_volume = sum(d['volume'] for d in session_data)
        
        return cumulative_pv / cumulative_volume if cumulative_volume else 0.0
    
    def _calculate_pe_vwap(self, underlying: str) -> float:
        """
        ✅ ENHANCED: Calculate VWAP for PE premium with baseline integration.
        
        Uses VolumeComputationManager to validate volume data quality.
        """
        if underlying not in self.pe_premium_data or not self.pe_premium_data[underlying]:
            return 0.0
        
        session_start = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
        session_data = [d for d in self.pe_premium_data[underlying] 
                       if d['timestamp'] >= session_start]
        
        if not session_data:
            return 0.0
        
        # ✅ FIXED: Get baseline directly from Redis using canonical_symbol (preserves all data points)
        baseline_volume = 0.0
        if self._baseline_redis_client:
            try:
                from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
                
                # Get canonical symbol for underlying
                canonical_underlying = RedisKeyStandards.canonical_symbol(underlying)
                baseline_key = DatabaseAwareKeyBuilder.live_volume_baseline(canonical_underlying)
                baseline_data = self._baseline_redis_client.hgetall(baseline_key)
                
                if baseline_data:
                    # Decode bytes if needed
                    if isinstance(list(baseline_data.values())[0], bytes):
                        baseline_data = {
                            k.decode() if isinstance(k, bytes) else k:
                            v.decode() if isinstance(v, bytes) else v
                            for k, v in baseline_data.items()
                        }
                    
                    # Priority: baseline_1min > current > avg_volume_55d/375
                    baseline_1min = baseline_data.get('baseline_1min')
                    if baseline_1min:
                        baseline_volume = float(baseline_1min)
                    else:
                        current = baseline_data.get('current')
                        if current:
                            baseline_volume = float(current)
                        else:
                            avg_vol_55d = baseline_data.get('avg_volume_55d')
                            if avg_vol_55d:
                                baseline_volume = float(avg_vol_55d) / 375.0
                
                # ✅ PRESERVE ALL DATA POINTS: Only filter if baseline is very high (20% threshold, not 50%)
                if baseline_volume > 0:
                    min_volume_threshold = baseline_volume * 0.2  # 20% threshold (less aggressive)
                    original_count = len(session_data)
                    filtered_data = [d for d in session_data if d.get('volume', 0) >= min_volume_threshold]
                    
                    # Only use filtered data if we still have at least 50% of original data points
                    if filtered_data and len(filtered_data) >= (original_count * 0.5):
                        session_data = filtered_data
                        self.logger.debug(
                            f"🔍 [KOW_SIGNAL] {underlying} PE baseline filter applied (preserved {len(filtered_data)}/{original_count} data points)"
                        )
            except Exception as e:
                self.logger.debug(f"⚠️ [KOW_SIGNAL] {underlying} PE baseline lookup failed: {e}, using all session data")
        
        cumulative_pv = sum(d['premium'] * d['volume'] for d in session_data)
        cumulative_volume = sum(d['volume'] for d in session_data)
        
        return cumulative_pv / cumulative_volume if cumulative_volume else 0.0

    def _get_option_symbols(self, underlying: str, strike: int, expiry: str = None) -> Tuple[str, str]:
        """Generate CE and PE symbols based on strike and expiry"""
        if expiry is None:
            expiry = self._get_current_expiry()
        
        # Format: NIFTY25NOV25900CE, NIFTY25NOV25900PE
        # Or NFO:NIFTY25NOV25900CE for exchange prefix
        ce_symbol = f"{underlying}{expiry}{strike}CE"
        pe_symbol = f"{underlying}{expiry}{strike}PE"
        return ce_symbol, pe_symbol
    
    def extract_strike_from_option_symbol_fixed(self, symbol: str) -> Optional[int]:
        """
        🔥 CRITICAL FIX: Correctly parse NSE option symbols
        
        NSE weekly option format: [UNDERLYING][DD][MonthCode][WeekNumber][STRIKE][CE/PE]
        Example: NIFTY25D0926050CE
        - 25 = Day (25th)
        - D = Month code (D = April)
        - 09 = Week number (week 09)
        - 26050 = Strike price
        - CE = Call option
        
        Monthly option format: [UNDERLYING][DD][MMM][YY][STRIKE][CE/PE] or [UNDERLYING][DD][MMM][STRIKE][CE/PE]
        Example: NIFTY25DEC2526050CE or BANKNIFTY25DEC58600CE
        - 25 = Day (25th)
        - DEC = Month (December)
        - 25 = Year (2025) - optional, may be omitted for BANKNIFTY
        - 26050/58600 = Strike price
        - CE = Call option
        
        Note: BANKNIFTY only has monthly expiry options, not weekly.
        """
        import re
        
        # ✅ FIXED: Strip any exchange prefix (NFO:, NSE:, etc.)
        clean_symbol = symbol.upper()
        for prefix in ['NFO:', 'NSE:', 'CDS:', 'MCX:']:
            if clean_symbol.startswith(prefix):
                clean_symbol = clean_symbol[len(prefix):]
                break
        
        # Extract underlying to determine if it's BANKNIFTY (monthly-only)
        underlying_match = re.match(r'^([A-Z]+)', clean_symbol)
        underlying = underlying_match.group(1) if underlying_match else ''
        is_banknifty = 'BANKNIFTY' in underlying or ('BANK' in underlying and 'NIFTY' in underlying)
        
        # ✅ CRITICAL: BANKNIFTY only has monthly expiry - skip all weekly patterns
        if is_banknifty:
            self.logger.debug(f"📅 [STRIKE_PARSE] {symbol} - BANKNIFTY detected, skipping weekly patterns (monthly-only)")
        
        # Pattern 1: Weekly options (NIFTY25D0926050CE) - SKIP for BANKNIFTY (monthly-only)
        if not is_banknifty:
            weekly_pattern = r'^([A-Z]+)(\d{2})([A-Z])(\d{2})(\d+)([CP]E)$'
            match = re.match(weekly_pattern, clean_symbol)
            
            if match:
                underlying, day, month_code, week_num, strike_str, option_type = match.groups()
                try:
                    strike = int(strike_str)
                    # Decode month code using NSE_MONTH_CODES mapping
                    month_name = self.NSE_MONTH_CODES.get(month_code, f'UNKNOWN({month_code})')
                    self.logger.debug(f"✅ [STRIKE_PARSE] Weekly option: {symbol} -> Day={day}, MonthCode={month_code}({month_name}), Week={week_num}, Strike={strike}")
                    return strike
                except (ValueError, TypeError):
                    pass
        
        # Pattern 2: Monthly options (NIFTY25DEC2526050CE or BANKNIFTY25DEC58600CE)
        # ✅ FIXED: Use MarketCalendar to parse expiry dates for accurate contract date determination
        # Try with year first (NIFTY25DEC2526050CE)
        monthly_pattern_with_year = r'^([A-Z]+)(\d{2})([A-Z]{3})(\d{2})(\d{4,6})([CP]E)$'
        match = re.match(monthly_pattern_with_year, clean_symbol)
        
        if match:
            underlying, day, month_str, year_str, strike_str, option_type = match.groups()
            try:
                strike = int(strike_str)
                # Validate: strike should be realistic (4-6 digits)
                if 1000 <= strike <= 999999:
                    # ✅ Use MarketCalendar to parse expiry date for contract validation
                    try:
                        from shared_core.market_calendar import get_cached_calendar
                        calendar = get_cached_calendar('NSE')
                        parsed_expiry = calendar.parse_expiry_from_symbol(clean_symbol)
                        expiry_info = f", Expiry={parsed_expiry.strftime('%Y-%m-%d') if parsed_expiry else 'N/A'}"
                    except Exception:
                        expiry_info = ""
                    self.logger.debug(f"✅ [STRIKE_PARSE] Monthly option (with year): {symbol} -> Day={day}, Month={month_str}, Year={year_str}, Strike={strike}{expiry_info}")
                    return strike
            except (ValueError, TypeError):
                pass
        
        # Try without year (BANKNIFTY25DEC58600CE - no year, strike starts immediately after month)
        # ✅ BANKNIFTY only has monthly expiry, so this pattern is important
        monthly_pattern_no_year = r'^([A-Z]+)(\d{2})([A-Z]{3})(\d{4,6})([CP]E)$'
        match = re.match(monthly_pattern_no_year, clean_symbol)
        
        if match:
            underlying, day, month_str, strike_str, option_type = match.groups()
            try:
                strike = int(strike_str)
                # Validate: strike should be realistic (4-6 digits)
                if 1000 <= strike <= 999999:
                    # ✅ Use MarketCalendar to parse expiry date for contract validation
                    try:
                        from shared_core.market_calendar import get_cached_calendar
                        calendar = get_cached_calendar('NSE')
                        parsed_expiry = calendar.parse_expiry_from_symbol(clean_symbol)
                        expiry_info = f", Expiry={parsed_expiry.strftime('%Y-%m-%d') if parsed_expiry else 'N/A'}"
                    except Exception:
                        expiry_info = ""
                    self.logger.debug(f"✅ [STRIKE_PARSE] Monthly option (no year): {symbol} -> Day={day}, Month={month_str}, Strike={strike}{expiry_info}")
                    return strike
            except (ValueError, TypeError):
                pass
        
        # Pattern 3: Compact format with D + 2 digits (NIFTY25D1626050CE)
        # This is likely 25th day, month code D, week 16
        # ✅ SKIP for BANKNIFTY (monthly-only)
        if not is_banknifty:
            compact_pattern = r'^([A-Z]+)(\d{2})([A-Z])(\d{2})(\d+)([CP]E)$'
            match = re.match(compact_pattern, clean_symbol)
            
            if match:
                underlying, day, month_code, week_num, strike_str, option_type = match.groups()
                try:
                    strike = int(strike_str)
                    # Decode month code using NSE_MONTH_CODES mapping
                    month_name = self.NSE_MONTH_CODES.get(month_code, f'UNKNOWN({month_code})')
                    self.logger.debug(f"✅ [STRIKE_PARSE] Compact weekly: {symbol} -> Day={day}, MonthCode={month_code}({month_name}), Week={week_num}, Strike={strike}")
                    return strike
                except (ValueError, TypeError):
                    pass
        
        # Pattern 4: Just find last digits before CE/PE (fallback)
        # ✅ SKIP fallback for BANKNIFTY if it looks like weekly format (BANKNIFTY doesn't have weekly)
        if not is_banknifty or not re.match(r'^[A-Z]+\d{2}[A-Z]\d{2}', clean_symbol):
            fallback_pattern = r'(\d+)([CP]E)$'
            match = re.search(fallback_pattern, clean_symbol)
            
            if match:
                strike_str, option_type = match.groups()
                try:
                    strike = int(strike_str)
                    # ✅ Validate strike range before returning
                    if is_banknifty:
                        # BANKNIFTY strikes: 20000-100000
                        if 20000 <= strike <= 100000:
                            self.logger.warning(f"⚠️ [STRIKE_PARSE] Using fallback for {symbol} -> Strike={strike}")
                            return strike
                    else:
                        # NIFTY strikes: 10000-40000
                        if 10000 <= strike <= 40000:
                            self.logger.warning(f"⚠️ [STRIKE_PARSE] Using fallback for {symbol} -> Strike={strike}")
                            return strike
                except (ValueError, TypeError):
                    pass
        
        if is_banknifty:
            self.logger.error(f"❌ [STRIKE_PARSE] Could not parse BANKNIFTY monthly option from {symbol} (BANKNIFTY only has monthly expiry)")
        else:
            self.logger.error(f"❌ [STRIKE_PARSE] Could not parse strike from {symbol}")
        return None
    
    def extract_strike_from_option_symbol(self, symbol: str) -> Optional[int]:
        """
        ✅ ALIAS: Wrapper for extract_strike_from_option_symbol_fixed() for backward compatibility.
        """
        return self.extract_strike_from_option_symbol_fixed(symbol)
    
    def get_current_week_expiry_strikes(self, underlying_symbol: str) -> List[int]:
        """
        Get valid strikes for current week's expiry (NIFTY) or monthly expiry (BANKNIFTY)
        
        ✅ FIXED: BANKNIFTY only has monthly expiry options, not weekly.
        Uses MarketCalendar to determine the correct expiry date.
        
        Args:
            underlying_symbol: Underlying symbol (NIFTY, BANKNIFTY, etc.)
            
        Returns:
            List of valid strike prices for current week's expiry (NIFTY) or monthly expiry (BANKNIFTY)
        """
        from datetime import datetime, timedelta
        
        # ✅ BANKNIFTY only has monthly expiry - use MarketCalendar to get monthly expiry date
        if underlying_symbol == 'BANKNIFTY':
            try:
                from shared_core.market_calendar import get_cached_calendar
                calendar = get_cached_calendar('NSE')
                # Get next monthly expiry (last Thursday of the month)
                today = datetime.now()
                # Find last Thursday of current month
                from calendar import monthcalendar
                cal = monthcalendar(today.year, today.month)
                thursdays = [week[3] for week in cal if week[3] != 0]
                last_thursday = thursdays[-1] if thursdays else 25
                expiry_date = datetime(today.year, today.month, last_thursday)
                
                # If last Thursday has passed, get next month's last Thursday
                if expiry_date < today:
                    if today.month == 12:
                        expiry_date = datetime(today.year + 1, 1, 25)
                    else:
                        next_month = today.month + 1
                        cal = monthcalendar(today.year, next_month)
                        thursdays = [week[3] for week in cal if week[3] != 0]
                        last_thursday = thursdays[-1] if thursdays else 25
                        expiry_date = datetime(today.year, next_month, last_thursday)
                
                expiry_type = "Monthly"
            except Exception as e:
                self.logger.warning(f"⚠️ [WEEKLY_STRIKES] Could not get BANKNIFTY monthly expiry: {e}, using fallback")
                expiry_date = datetime.now() + timedelta(days=30)
                expiry_type = "Monthly (fallback)"
        else:
            # NIFTY has weekly expiry (Thursday)
            today = datetime.now()
            # Find current week's expiry (Thursday for NSE)
            days_until_thursday = (3 - today.weekday()) % 7  # Monday=0, Thursday=3
            if days_until_thursday == 0 and today.weekday() != 3:
                # If today is not Thursday, add 7 days to get next Thursday
                days_until_thursday = 7
            expiry_date = today + timedelta(days=days_until_thursday)
            expiry_type = "Weekly"
        
        # Get strike range based on underlying
        if underlying_symbol == 'NIFTY':
            # Get current NIFTY price to calculate base strike
            # ✅ FIX: Only fetch price if we have valid data (skip during initialization/test)
            current_price = None
            try:
                current_price = self._get_current_underlying_price('NIFTY')
                # Validate price before using
                if not self._is_valid_underlying_price('NIFTY', current_price):
                    current_price = None
            except Exception:
                current_price = None
            
            if current_price and current_price > 0:
                # Round to nearest 50
                base_strike = round(current_price / 50) * 50
            else:
                base_strike = 26000  # Fallback
            strikes = [base_strike + i*50 for i in range(-20, 21)]  # ±20 strikes around base
        elif underlying_symbol == 'BANKNIFTY':
            # Get current BANKNIFTY price to calculate base strike
            # ✅ FIX: Only fetch price if we have valid data (skip during initialization/test)
            current_price = None
            try:
                current_price = self._get_current_underlying_price('BANKNIFTY')
                # Validate price before using
                if not self._is_valid_underlying_price('BANKNIFTY', current_price):
                    current_price = None
            except Exception:
                current_price = None
            
            if current_price and current_price > 0:
                # Round to nearest 100
                base_strike = round(current_price / 100) * 100
            else:
                base_strike = 59700  # Fallback
            strikes = [base_strike + i*100 for i in range(-20, 21)]  # ±20 strikes around base
        else:
            base_strike = 10000
            strikes = [base_strike + i*100 for i in range(-20, 21)]
        
        self.logger.debug(f"📊 [WEEKLY_STRIKES] {underlying_symbol} - {expiry_type} expiry: {expiry_date.strftime('%Y-%m-%d')}, Strikes: {len(strikes)} (range: {min(strikes)}-{max(strikes)})")
        return strikes
    
    def _extract_strike_from_symbol(self, symbol: str) -> Optional[int]:
        """
        ✅ ALIAS: Wrapper for extract_strike_from_option_symbol() for backward compatibility.
        """
        return self.extract_strike_from_option_symbol(symbol)
    
    def is_atm_option(self, symbol: str, underlying_price: float) -> bool:
        """
        🔥 FIXED: Check if option is ATM with correct parsing
        
        Uses MarketCalendar to parse expiry dates for accurate contract date determination.
        Note: BANKNIFTY only has monthly expiry options, not weekly.
        
        Args:
            symbol: Option symbol (e.g., NIFTY25D0926050CE or BANKNIFTY25DEC58600CE)
            underlying_price: Current underlying price
            
        Returns:
            True if option strike is within tolerance of ATM strike
        """
        # Extract strike
        strike = self.extract_strike_from_option_symbol_fixed(symbol)
        if not strike:
            return False
        
        # Extract underlying symbol
        import re
        underlying_match = re.match(r'^([A-Z]+)\d+', symbol.upper())
        if not underlying_match:
            return False
        
        underlying_symbol = underlying_match.group(1)
        
        # Normalize underlying symbol name
        if "BANK" in underlying_symbol:
            underlying_symbol = 'BANKNIFTY'
        elif "FIN" in underlying_symbol:
            underlying_symbol = 'FINNIFTY'
        elif "MIDCP" in underlying_symbol:
            underlying_symbol = 'MIDCPNIFTY'
        else:
            underlying_symbol = 'NIFTY'
        
        # ✅ Use MarketCalendar to parse expiry date for contract validation
        expiry_date = None
        try:
            from shared_core.market_calendar import get_cached_calendar
            calendar = get_cached_calendar('NSE')
            expiry_date = calendar.parse_expiry_from_symbol(symbol.upper())
            if expiry_date:
                self.logger.debug(f"📅 [ATM_CHECK] {symbol} - Parsed expiry date: {expiry_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            self.logger.debug(f"⚠️ [ATM_CHECK] {symbol} - Could not parse expiry date: {e}")
        
        # Calculate ATM strike
        if underlying_symbol == 'NIFTY':
            atm_strike = round(underlying_price / 50) * 50
        elif underlying_symbol == 'BANKNIFTY':
            atm_strike = round(underlying_price / 100) * 100
        else:
            atm_strike = round(underlying_price / 50) * 50
        
        # Allow some tolerance (±50 for NIFTY, ±100 for BANKNIFTY)
        if underlying_symbol == 'NIFTY':
            tolerance = 50
        elif underlying_symbol == 'BANKNIFTY':
            tolerance = 100
        else:
            tolerance = 50
        
        is_atm = abs(strike - atm_strike) <= tolerance
        
        self.logger.info(f"🔍 [ATM_CHECK] {symbol} - Strike={strike}, ATM={atm_strike}, Underlying={underlying_price:.2f}, IsATM={is_atm}")
        
        return is_atm
    
    def test_option_parsing(self):
        """Test option symbol parsing"""
        test_symbols = [
            'NIFTY25D0926050CE',  # Weekly: 25th April (week 09), strike 26050
            'NIFTY25DEC2526050CE',  # Monthly: 25th Dec 2025, strike 26050
            'NIFTY25D1625750PE',  # Weekly: 25th April (week 16), strike 25750
            'BANKNIFTY25DEC58600CE',  # Monthly: 25th Dec, strike 58600
            'NIFTY25D0926200CE',  # Weekly: 25th April (week 09), strike 26200
        ]
        
        print("\n" + "=" * 60)
        print("🧪 TESTING OPTION SYMBOL PARSING")
        print("=" * 60)
        
        # ✅ Suppress warnings during test by temporarily reducing log level
        original_level = self.logger.level
        self.logger.setLevel(logging.ERROR)  # Only show errors during test
        
        try:
            for symbol in test_symbols:
                strike = self.extract_strike_from_option_symbol_fixed(symbol)
                print(f"{symbol:30} -> Strike: {strike}")
        finally:
            # Restore original log level
            self.logger.setLevel(original_level)
        
        print("=" * 60 + "\n")
    
    def _is_atm_option(self, symbol: str, strike: int, underlying: str = None) -> bool:
        """Check if symbol matches ATM strike for the given underlying (legacy method)"""
        # ✅ FIXED: Strip any exchange prefix (NFO:, NSE:, etc.) before processing
        clean_symbol = symbol.upper()
        for prefix in ['NFO:', 'NSE:', 'CDS:', 'MCX:']:
            if clean_symbol.startswith(prefix):
                clean_symbol = clean_symbol[len(prefix):]
                break
        
        if underlying:
            # Check if symbol belongs to this underlying and matches strike
            if underlying.upper() not in clean_symbol:
                return False
        
        # ✅ FIXED: Use regex to match exact strike (not substring)
        # Example: strike=58300 should match "BANKNIFTY25NOV58300CE" but not "BANKNIFTY25NOV58200CE"
        import re
        strike_str = str(strike)
        # Match strike followed by CE or PE (end of symbol)
        pattern = strike_str + r'(CE|PE)$'
        match_result = bool(re.search(pattern, clean_symbol))
        
        # ✅ DEBUG: Log the actual comparison for troubleshooting
        extracted_strike = self._extract_strike_from_symbol(symbol)
        if extracted_strike != strike:
            self.logger.debug(
                f"   🔍 [KOW_SIGNAL] Strike mismatch: symbol={symbol}, clean={clean_symbol}, "
                f"extracted={extracted_strike}, expected={strike}, match={match_result}"
            )
        
        return match_result
    
    def _get_underlying_from_symbol(self, symbol: str) -> Optional[str]:
        """Extract underlying name from option or futures symbol."""
        if not symbol:
            return None
        
        import re
        
        symbol_upper = symbol.upper()
        
        # Pattern: UNDERLYING + DATE + STRIKE + OPTION_TYPE (e.g., NIFTY23NOV22400CE)
        opt_pattern = r'^([A-Z]+)(\d{1,2}[A-Z]{3}\d{2,4})(\d+)(CE|PE)$'
        match = re.match(opt_pattern, symbol_upper)
        if match:
            return match.group(1)
        
        # Pattern: UNDERLYING + DATE + FUT (e.g., BANKNIFTY23NOVFUT)
        fut_pattern = r'^([A-Z]+)(\d{1,2}[A-Z]{3}\d{2,4})(FUT)$'
        fut_match = re.match(fut_pattern, symbol_upper)
        if fut_match:
            return fut_match.group(1)
        
        # Fallback for well-known index futures/options
        if "BANKNIFTY" in symbol_upper or "NIFTY BANK" in symbol_upper or "BANK NIFTY" in symbol_upper:
            return "BANKNIFTY"
        if "NIFTY" in symbol_upper and "BANK" not in symbol_upper:
            return "NIFTY"
        
        # As a last resort, return the raw uppercase symbol (useful for stock symbols)
        return symbol_upper

    def _fetch_fresh_greeks_from_redis(self, symbol: str, greek_names: list = None) -> dict:
        """
        ✅ CENTRALIZED: Fetch fresh Greeks from Redis using UnifiedDataStorage.
        
        Always fetch fresh Greeks from Redis to avoid stale/zero values from cache.
        Used when signals need Greeks for straddle strategy.
        
        Args:
            symbol: Trading symbol (e.g., 'BANKNIFTY25NOV59100CE')
            greek_names: List of Greek names to fetch (default: ['delta', 'gamma', 'theta', 'rho'])
            Note: vega (IV-related) is excluded as this is a pure VWAP-based strategy
        
        Returns:
            Dict with Greek values (e.g., {'delta': 0.5, 'gamma': 0.02, ...})
        """
        greeks = {}
        
        try:
            from shared_core.redis_clients.unified_data_storage import get_unified_storage
            if not self.redis_realtime:
                return greeks
            
            # Initialize UnifiedDataStorage if not already done
            if not hasattr(self, 'unified_storage') or self.unified_storage is None:
                self.unified_storage = get_unified_storage(redis_client=self.redis_realtime)
            
            # Fetch all Greeks from UnifiedDataStorage
            fetched_greeks = self.unified_storage.get_greeks(symbol)
            
            if fetched_greeks:
                # Filter by requested greek_names if provided
                if greek_names is None:
                    # ✅ PURE VWAP-BASED: vega (IV-related) excluded from default list
                    greek_names = ['delta', 'gamma', 'theta', 'rho', 'dte_years', 'trading_dte', 'expiry_series']
                
                for greek in greek_names:
                    if greek in fetched_greeks and fetched_greeks[greek] is not None:
                        try:
                            # Try to convert to float for numeric Greeks
                            # ✅ PURE VWAP-BASED: vega (IV-related) excluded
                            if greek in ['delta', 'gamma', 'theta', 'rho', 'dte_years']:
                                greeks[greek] = float(fetched_greeks[greek])
                            elif greek == 'trading_dte':
                                greeks[greek] = int(fetched_greeks[greek])
                            else:
                                greeks[greek] = fetched_greeks[greek]
                        except (ValueError, TypeError):
                            greeks[greek] = fetched_greeks[greek]
            
        except Exception as e:
            self.logger.debug(f"⚠️ [KOW_SIGNAL] Failed to fetch Greeks from UnifiedDataStorage for {symbol}: {e}")
        
        return greeks
    
    def _get_average_premium(self, underlying: str, lookback_minutes: int = 60) -> float:
        """
        Get average combined premium over lookback period.
        
        Args:
            underlying: 'NIFTY' or 'BANKNIFTY'
            lookback_minutes: Minutes to look back (default: 60)
        
        Returns:
            Average combined premium
        """
        try:
            if underlying not in self.combined_premium_data or not self.combined_premium_data[underlying]:
                return 0.0
            
            cutoff_time = datetime.now() - timedelta(minutes=lookback_minutes)
            recent_data = [d['premium'] for d in self.combined_premium_data[underlying]
                          if d['timestamp'] > cutoff_time]
            
            if not recent_data:
                return 0.0
            
            return sum(recent_data) / len(recent_data)
        
        except Exception as e:
            self.logger.warning(f"Error calculating average premium for {underlying}: {e}")
            return 0.0
    
    
    def _calculate_underlying_vwap(self, underlying: str) -> float:
        """
        Calculate VWAP of UNDERLYING index (NOT option premiums).
        
        ✅ FIX: VWAP should be for underlying spot price, not combined option premium.
        This is used for trend detection (price above/below VWAP).
        
        Args:
            underlying: 'NIFTY' or 'BANKNIFTY'
        
        Returns:
            Underlying VWAP from session start
        """
        if underlying not in self.underlying_price_volume_data or not self.underlying_price_volume_data[underlying]:
            self.logger.debug(f"⚠️ [KOW_SIGNAL] {underlying} No underlying price/volume data for VWAP")
            return 0.0
        
        # Filter data from session start (9:15 AM)
        session_start = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
        session_data = [d for d in self.underlying_price_volume_data[underlying]
                       if d['timestamp'] >= session_start]
        
        if not session_data:
            return 0.0
        
        cumulative_pv = sum(d['price'] * d['volume'] for d in session_data)
        cumulative_volume = sum(d['volume'] for d in session_data)
        
        vwap = cumulative_pv / cumulative_volume if cumulative_volume > 0 else 0.0
        
        self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying} Underlying VWAP: {vwap:.2f} "
                         f"(data_points={len(session_data)}, volume={cumulative_volume})")
        
        return vwap
    
    def _get_trend_signal(self, underlying: str) -> str:
        """
        Use underlying VWAP for trend direction.
        
        Returns:
            'BULLISH', 'BEARISH', or 'NEUTRAL'
        """
        try:
            current_price = self._get_current_underlying_price(underlying)
            vwap = self._calculate_underlying_vwap(underlying)
            
            if not current_price or not vwap or vwap == 0:
                return "NEUTRAL"
            
            deviation_pct = (current_price - vwap) / vwap
            
            if deviation_pct > 0.002:  # 0.2% above VWAP
                trend = "BULLISH"
            elif deviation_pct < -0.002:  # 0.2% below VWAP
                trend = "BEARISH"
            else:
                trend = "NEUTRAL"
            
            self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying} Trend: {trend} "
                            f"(price={current_price:.2f}, vwap={vwap:.2f}, dev={deviation_pct*100:.2f}%)")
            
            return trend
        
        except Exception as e:
            self.logger.warning(f"Error calculating trend signal for {underlying}: {e}")
            return "NEUTRAL"
    
    def _should_enter_straddle(self, underlying: str, combined_premium: float) -> bool:
        """
        ✅ CORRECTED: Check if conditions favor straddle ENTRY for PREMIUM SELLING.
        
        For premium selling strategy, we want to SELL when:
        - Both CE & PE premiums are BELOW VWAP (cheap entry)
        - Market is range-bound/consolidating
        - Premium is relatively cheap compared to historical average
        
        Args:
            underlying: 'NIFTY' or 'BANKNIFTY'
            combined_premium: Current combined premium (CE + PE)
        
        Returns:
            True if entry conditions met for SELLING straddle
        """
        try:
            # Get current premiums and VWAP
            current_vwap = self._calculate_combined_vwap(underlying)
            ce_premium = self.ce_premiums.get(underlying, 0)
            pe_premium = self.pe_premiums.get(underlying, 0)
            
            if current_vwap <= 0:
                self.logger.debug(f"⚠️ [KOW_SIGNAL] {underlying} No combined premium VWAP data for entry check (VWAP={current_vwap:.2f})")
                return False
            
            # Check if both legs are below VWAP
            ce_below_vwap = ce_premium > 0 and ce_premium < (current_vwap / 2)  # Half of combined VWAP
            pe_below_vwap = pe_premium > 0 and pe_premium < (current_vwap / 2)
            both_below_vwap = ce_below_vwap and pe_below_vwap
            
            # Check if premium is cheap relative to average
            avg_premium = self._get_average_premium(underlying)
            premium_cheap = combined_premium < avg_premium if avg_premium > 0 else True
            
            # Get trend signal - prefer range-bound markets
            trend = self._get_trend_signal(underlying)
            trend_ok = trend in ["NEUTRAL"]  # Only neutral markets for premium selling
            
            # Entry condition: Both below VWAP + Cheap premium + Range-bound market
            should_enter = both_below_vwap and premium_cheap and trend_ok
            
            self.logger.info(f"🔍 [KOW_SIGNAL_CORRECTED] {underlying} Entry Check: "
                            f"CE={ce_premium:.2f} (below_vwap={ce_below_vwap}), "
                            f"PE={pe_premium:.2f} (below_vwap={pe_below_vwap}), "
                            f"VWAP={current_vwap:.2f}, Avg={avg_premium:.2f}, "
                            f"Trend={trend}, Should_Enter={should_enter}")
            
            return should_enter
        
        except Exception as e:
            self.logger.error(f"Corrected entry check failed for {underlying}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    def _should_exit_straddle(self, underlying: str, combined_premium: float) -> bool:
        """
        ✅ CORRECTED: Check if conditions favor straddle EXIT for PREMIUM SELLING.
        
        Exit when:
        - Either CE or PE premium moves ABOVE VWAP (becomes expensive to buy back)
        - Target profit achieved
        - Stop loss hit
        
        Args:
            underlying: 'NIFTY' or 'BANKNIFTY'
            combined_premium: Current combined premium (CE + PE)
        
        Returns:
            True if exit conditions met
        """
        try:
            # Get current premiums and VWAP
            current_vwap = self._calculate_combined_vwap(underlying)
            ce_premium = self.ce_premiums.get(underlying, 0)
            pe_premium = self.pe_premiums.get(underlying, 0)
            
            if current_vwap <= 0:
                self.logger.debug(f"⚠️ [KOW_SIGNAL] {underlying} No combined premium VWAP data for exit check (VWAP={current_vwap:.2f})")
                return False
            
            # Check if either leg moves above VWAP (exit signal)
            ce_above_vwap = ce_premium > 0 and ce_premium > (current_vwap / 2)
            pe_above_vwap = pe_premium > 0 and pe_premium > (current_vwap / 2)
            either_above_vwap = ce_above_vwap or pe_above_vwap
            
            # Check if premium becomes expensive
            avg_premium = self._get_average_premium(underlying)
            premium_expensive = combined_premium > avg_premium if avg_premium > 0 else False
            
            # Exit condition: Either above VWAP OR Premium becomes expensive
            should_exit = either_above_vwap or premium_expensive
            
            # Check profit target (premium decay)
            position_state = self.position_states.get(underlying, {})
            entry_premium = position_state.get('entry_combined_premium', 0)
            if entry_premium > 0:
                premium_decay_pct = (entry_premium - combined_premium) / entry_premium
                target_achieved = premium_decay_pct > 0.30  # 30% premium decay target
                
                if target_achieved:
                    self.logger.info(f"🎯 [KOW_SIGNAL] {underlying} Target achieved: {premium_decay_pct:.1%} decay")
                    should_exit = True
            
            self.logger.info(f"🔍 [KOW_SIGNAL_CORRECTED] {underlying} Exit Check: "
                            f"CE={ce_premium:.2f} (above_vwap={ce_above_vwap}), "
                            f"PE={pe_premium:.2f} (above_vwap={pe_above_vwap}), "
                            f"VWAP={current_vwap:.2f}, Should_Exit={should_exit}")
            
            return should_exit
        
        except Exception as e:
            self.logger.error(f"Corrected exit check failed for {underlying}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    def _get_volume_ratio(self, underlying: str) -> float:
        """
        Get volume ratio for the underlying from recent tick data or indicators.
        
        Returns volume ratio (current volume / average volume), defaulting to 1.0 if unavailable.
        """
        try:
            # Try to get volume ratio from combined premium data (most recent tick)
            if underlying in self.combined_premium_data and self.combined_premium_data[underlying]:
                # Get volume from most recent combined premium data point
                recent_data = self.combined_premium_data[underlying][-1]
                volume = recent_data.get('volume', 0)
                
                # Calculate average volume from historical data
                if len(self.combined_premium_data[underlying]) > 1:
                    avg_volume = sum(d.get('volume', 0) for d in self.combined_premium_data[underlying]) / len(self.combined_premium_data[underlying])
                    if avg_volume > 0:
                        volume_ratio = volume / avg_volume
                        self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying} Volume ratio from premium data: {volume_ratio:.2f}x (volume={volume}, avg={avg_volume:.0f})")
                        return volume_ratio
            
            # Fallback: Try to get from CE/PE volumes
            ce_vol = self.ce_volumes.get(underlying, 0)
            pe_vol = self.pe_volumes.get(underlying, 0)
            if ce_vol > 0 or pe_vol > 0:
                total_vol = (ce_vol + pe_vol) // 2
                # Use a simple heuristic: assume average is half of current if we don't have history
                volume_ratio = 2.0 if total_vol > 0 else 1.0
                self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying} Volume ratio (fallback): {volume_ratio:.2f}x")
                return volume_ratio
            
            # Default fallback
            self.logger.debug(f"⚠️ [KOW_SIGNAL] {underlying} No volume data available, using default 1.0x")
            return 1.0
        except Exception as e:
            self.logger.warning(f"Error getting volume ratio for {underlying}: {e}")
            return 1.0
    
    # Simplified _calculate_confidence (VWAP-based only):
    def _calculate_confidence(self, underlying: str) -> float:
        """
        SIMPLIFIED: Confidence based purely on VWAP positioning.
        
        High confidence when:
        - Both CE & PE are BELOW VWAP
        - Market is NEUTRAL/RANGE_BOUND
        - Premium is cheap relative to average
        """
        try:
            ce_premium = self.ce_premiums.get(underlying, 0)
            pe_premium = self.pe_premiums.get(underlying, 0)
            current_vwap = self._calculate_combined_vwap(underlying)
            avg_premium = self._get_average_premium(underlying)
            trend = self._get_trend_signal(underlying)
            
            # Check conditions
            ce_below_vwap = ce_premium > 0 and ce_premium < (current_vwap / 2)
            pe_below_vwap = pe_premium > 0 and pe_premium < (current_vwap / 2)
            both_below_vwap = ce_below_vwap and pe_below_vwap
            
            premium_cheap = (ce_premium + pe_premium) < avg_premium if avg_premium > 0 else True
            trend_ok = trend in ["NEUTRAL", "RANGE_BOUND"]
            
            # High confidence only when ALL conditions met
            if both_below_vwap and premium_cheap and trend_ok:
                return 0.9  # High confidence for entry
            
            # Medium confidence if some conditions met
            confidence = 0.5
            if both_below_vwap:
                confidence += 0.2
            if premium_cheap:
                confidence += 0.1
            if trend_ok:
                confidence += 0.1
            
            return min(confidence, 0.8)  # Cap at 0.8 if not perfect
            
        except Exception as e:
            self.logger.warning(f"Error calculating confidence for {underlying}: {e}")
            return 0.5
    
    async def on_tick(self, tick_data: Dict, send_alert: bool = True) -> Optional[Dict]:
        """
        Process incoming tick data and check if conditions are met for straddle signals.
        
        ✅ ORCHESTRATOR PATTERN: When called from pattern_detector, this method should ONLY
        validate conditions and return signals. The alert system will handle sending alerts.
        
        Args:
            tick_data: Tick data dictionary
            send_alert: If True, send alert via _send_alert() and _store_signal_in_redis().
                       If False (when called from pattern_detector), only return signal.
        
        Returns:
            Signal dict if conditions met, None otherwise
        """
        try:
            symbol = tick_data.get('symbol', tick_data.get('tradingsymbol', 'UNKNOWN'))
            last_price = tick_data.get('last_price') or tick_data.get('current_price') or 0.0
            volume = tick_data.get('bucket_incremental_volume') or tick_data.get('volume') or tick_data.get('zerodha_cumulative_volume') or 0
            
            self.logger.info(f"🔍 [KOW_SIGNAL] Received tick: symbol={symbol}, price={last_price}, volume={volume}, send_alert={send_alert}")
            
            # Filter relevant instruments
            if not self._is_relevant_tick(tick_data):
                self.logger.info(f"🔍 [KOW_SIGNAL] Tick rejected (not relevant): {symbol}")
                return None

            self.logger.info(f"✅ [KOW_SIGNAL] Tick accepted (relevant): {symbol}")

            # Update data trackers
            self._update_market_data(tick_data)

            # Check if market hours
            if not self._is_trading_hours():
                current_time = datetime.now().time()
                self.logger.info(f"🔍 [KOW_SIGNAL] Outside trading hours: {current_time} (9:30-15:15)")
                return None

            # Generate signals (VALIDATE CONDITIONS ONLY)
            signal = await self._generate_straddle_signals(tick_data)
            
            if signal:
                self.logger.info(f"🎯 [KOW_SIGNAL] Signal generated: {signal.get('symbol', 'UNKNOWN')} - {signal.get('action', 'UNKNOWN')} (confidence={signal.get('confidence', 0):.2f})")
                
                # ✅ ORCHESTRATOR: Only send alerts if explicitly requested (standalone mode)
                # When called from pattern_detector (send_alert=False), pattern_detector will
                # return the signal to alert system, which will handle alert sending via UnifiedAlertBuilder
                if send_alert:
                    await self._send_alert(signal)
                    # Store signal in Redis for tracking
                    await self._store_signal_in_redis(signal)
                    self.logger.info(f"✅ [KOW_SIGNAL] Signal stored and published to alerts:stream")
                else:
                    self.logger.info(f"✅ [KOW_SIGNAL] Signal returned to pattern_detector (alert will be sent by alert system)")
            else:
                self.logger.info(f"🔍 [KOW_SIGNAL] No signal generated for tick: {symbol}")
                
            return signal

        except Exception as e:
            self.logger.error(f"Error in on_tick: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None


    def _is_relevant_tick(self, tick_data: Dict) -> bool:
        """Check if tick is relevant for straddle strategy"""
        # Handle both 'symbol' and 'tradingsymbol' fields (standardized field mapping)
        symbol = tick_data.get('symbol', tick_data.get('tradingsymbol', ''))
        
        # Check if it's underlying index or options
        underlying_symbols = self.config.get('underlying_symbols', ['NIFTY', 'BANKNIFTY'])
        is_underlying = any(us in symbol for us in underlying_symbols)
        is_option = 'CE' in symbol or 'PE' in symbol
        
        result = is_underlying or is_option
        if result:
            self.logger.debug(f"🔍 [KOW_SIGNAL] Tick is relevant: {symbol} (underlying={is_underlying}, option={is_option})")
        else:
            self.logger.debug(f"🔍 [KOW_SIGNAL] Tick is NOT relevant: {symbol} (not in {underlying_symbols} and not CE/PE)")
        
        return result

    def _update_market_data(self, tick_data: Dict):
        """Update market data for combined premium calculation using standard field names"""
        # Use standard field names from optimized_field_mapping.yaml
        # Handle both 'symbol' and 'tradingsymbol' fields (standardized field mapping)
        symbol = tick_data.get('symbol', tick_data.get('tradingsymbol', ''))
        # Use standardized field: last_price (primary) with fallback
        # ✅ FIXED: Handle None values explicitly - ensure we never pass None to Redis
        last_price = tick_data.get('last_price') or tick_data.get('current_price') or 0.0
        if last_price is None:
            last_price = 0.0
        # Use standardized field: bucket_incremental_volume (primary) with fallback
        volume = tick_data.get('bucket_incremental_volume') or tick_data.get('volume') or tick_data.get('zerodha_cumulative_volume') or 0
        if volume is None:
            volume = 0
        # ✅ FIXED: Ensure volume is numeric (convert string to int/float)
        try:
            volume = float(volume) if volume else 0.0
        except (ValueError, TypeError):
            volume = 0.0
        
        # Update underlying price to determine ATM strike
        underlying_symbols = self.config.get('underlying_symbols', ['NIFTY', 'BANKNIFTY'])
        is_underlying = any(us in symbol.upper() for us in underlying_symbols)
        
        if is_underlying:
            underlying_name = None
            symbol_upper = symbol.upper()
            if 'BANKNIFTY' in symbol_upper:
                underlying_name = 'BANKNIFTY'
            elif 'NIFTY' in symbol_upper and 'BANK' not in symbol_upper:
                underlying_name = 'NIFTY'
            if underlying_name:
                price_value = last_price if last_price and last_price > 0 else 0.0
                if self._is_valid_underlying_price(underlying_name, price_value):
                    self._record_underlying_price(underlying_name, price_value, volume, symbol)
                else:
                    # ✅ FIX: Only try fallback if we have actual tick data (not during initialization)
                    # During initialization, skip fallback to avoid false warnings
                    if volume > 0 or hasattr(self, '_initialization_complete'):
                        fallback_price = self._get_current_underlying_price(underlying_name)
                        if self._is_valid_underlying_price(underlying_name, fallback_price):
                            self._log_invalid_underlying_price(
                                underlying_name,
                                f"⚠️ [KOW_SIGNAL] Ignoring unrealistic underlying price {last_price} for {underlying_name}, using fallback {fallback_price}",
                                volume
                            )
                            self._record_underlying_price(underlying_name, fallback_price, volume, underlying_name)
                        else:
                            self._log_invalid_underlying_price(
                                underlying_name,
                                f"⚠️ [KOW_SIGNAL] Ignoring unrealistic underlying price {last_price} for {underlying_name} (no valid fallback)",
                                volume
                            )
                    else:
                        # During initialization with no volume, silently skip
                        self.logger.debug(f"⚠️ [KOW_SIGNAL] Skipping invalid underlying price {last_price} for {underlying_name} during initialization (no volume)")

        # Update option premium data for ATM strikes of all underlyings
        # ✅ FIX: Skip futures (FUT) - only process options (CE/PE)
        symbol_upper = symbol.upper()
        is_future = symbol_upper.endswith('FUT')
        is_option = 'CE' in symbol_upper or 'PE' in symbol_upper
        
        if is_future:
            # Futures don't have strikes, skip ATM option matching
            self.logger.debug(f"🔍 [KOW_SIGNAL] Skipping future {symbol} (futures don't have ATM strikes)")
            return
        
        underlying_from_symbol = self._get_underlying_from_symbol(symbol)
        if underlying_from_symbol and is_option:
            self.logger.debug(f"🔍 [KOW_SIGNAL] Processing option tick: {symbol} (underlying={underlying_from_symbol}, price={last_price}, volume={volume})")
            
            # ✅ FIXED: If ATM strike not set yet, try to get underlying price from Redis
            if underlying_from_symbol not in self.atm_strikes or not self.atm_strikes[underlying_from_symbol]:
                self.logger.info(f"🔍 [KOW_SIGNAL] ATM strike not set for {underlying_from_symbol}, fetching from Redis...")
                # Try to get underlying price from Redis to calculate ATM strike
                underlying_price = self._get_current_underlying_price(underlying_from_symbol)
                if underlying_price and underlying_price > 0:
                    calculated_strike = self._get_atm_strike(underlying_price, underlying_from_symbol)
                    self.atm_strikes[underlying_from_symbol] = calculated_strike
                    self.logger.info(
                        f"✅ [KOW_SIGNAL] Calculated {underlying_from_symbol} ATM strike from Redis: "
                        f"{calculated_strike} (underlying_price={underlying_price:.2f})"
                    )
                else:
                    self.logger.warning(f"⚠️ [KOW_SIGNAL] Could not get underlying price for {underlying_from_symbol} from Redis (price={underlying_price})")
            
            # Now check if we have ATM strike and if option matches
            if underlying_from_symbol in self.atm_strikes:
                atm_strike = self.atm_strikes[underlying_from_symbol]
                if atm_strike:
                    is_atm = self._is_atm_option(symbol, atm_strike, underlying_from_symbol)
                    # ✅ DEBUG: Extract strike from symbol for comparison
                    extracted_strike = self._extract_strike_from_symbol(symbol)
                    # ✅ DEBUG: Log if ATM strike seems wrong (for NIFTY should be 20k-30k, for BANKNIFTY should be 50k-70k)
                    if underlying_from_symbol == "NIFTY" and (atm_strike < 10000 or atm_strike > 50000):
                        self.logger.warning(
                            f"⚠️ [KOW_SIGNAL] Suspicious ATM strike for NIFTY: {atm_strike} "
                            f"(expected ~20k-30k range). Check underlying price source!"
                        )
                    elif underlying_from_symbol == "BANKNIFTY" and (atm_strike < 40000 or atm_strike > 80000):
                        self.logger.warning(
                            f"⚠️ [KOW_SIGNAL] Suspicious ATM strike for BANKNIFTY: {atm_strike} "
                            f"(expected ~50k-70k range). Check underlying price source!"
                        )
                    self.logger.info(
                        f"🔍 [KOW_SIGNAL_DEBUG] Option {symbol} check: ATM strike={atm_strike}, "
                        f"extracted_strike={extracted_strike}, is_atm={is_atm}, underlying={underlying_from_symbol}"
                    )
                    if is_atm:
                        self.logger.info(f"✅ [KOW_SIGNAL] Option {symbol} matches ATM strike {atm_strike}, updating combined premium...")
                        self._update_combined_premium(tick_data, underlying_from_symbol)
                    else:
                        self.logger.info(f"❌ [KOW_SIGNAL] Option {symbol} does NOT match ATM strike {atm_strike}, skipping")
                else:
                    self.logger.warning(f"⚠️ [KOW_SIGNAL] ATM strike is None for {underlying_from_symbol}")
            else:
                self.logger.warning(f"⚠️ [KOW_SIGNAL] {underlying_from_symbol} not in atm_strikes dict")

    def _update_combined_premium(self, tick_data: Dict, underlying: str):
        """Track combined premium (CE + PE) for ATM straddle using standard field names per underlying"""
        # Handle both 'symbol' and 'tradingsymbol' fields (standardized field mapping)
        symbol = tick_data.get('symbol', tick_data.get('tradingsymbol', ''))
        # Use standardized field: last_price (primary) with fallback
        # ✅ FIXED: Handle None values explicitly - ensure we never pass None to Redis
        premium = tick_data.get('last_price') or tick_data.get('current_price') or 0
        if premium is None:
            premium = 0
        # Use standardized field: bucket_incremental_volume (primary) with fallback
        volume = tick_data.get('bucket_incremental_volume') or tick_data.get('volume') or tick_data.get('zerodha_cumulative_volume') or 0
        if volume is None:
            volume = 0
        
        if underlying not in self.atm_strikes or not self.atm_strikes[underlying]:
            return
        
        atm_strike = self.atm_strikes[underlying]
        
        # Update underlying price using option-provided data when available
        underlying_price_hint = (
            tick_data.get('underlying_price')
            or tick_data.get('spot_price')
            or tick_data.get('underlying_value')
        )
        try:
            underlying_price_hint = float(underlying_price_hint) if underlying_price_hint is not None else 0.0
        except (TypeError, ValueError):
            underlying_price_hint = 0.0
        if underlying_price_hint and self._is_valid_underlying_price(underlying, underlying_price_hint):
            self._record_underlying_price(underlying, underlying_price_hint, volume, symbol)
        
        # Update CE or PE premium for this underlying
        if 'CE' in symbol and premium and premium > 0:
            old_ce = self.ce_premiums.get(underlying)
            self.ce_premiums[underlying] = premium
            self.ce_volumes[underlying] = volume
            # ✅ NEW: Track individual CE premium data for VWAP calculation
            if underlying not in self.ce_premium_data:
                self.ce_premium_data[underlying] = []
            self.ce_premium_data[underlying].append({
                'timestamp': datetime.now(),
                'premium': premium,
                'volume': volume if volume and volume > 0 else 1
            })
            # Keep only last 4 hours of data
            cutoff_time = datetime.now() - timedelta(hours=4)
            self.ce_premium_data[underlying] = [
                d for d in self.ce_premium_data[underlying]
                if d['timestamp'] > cutoff_time
            ]
            self.logger.info(f"✅ [KOW_SIGNAL] {underlying} CE premium updated: {old_ce} -> {premium} (volume={volume}, strike={atm_strike}, symbol={symbol})")
                
        elif 'PE' in symbol and premium and premium > 0:
            old_pe = self.pe_premiums.get(underlying)
            self.pe_premiums[underlying] = premium
            self.pe_volumes[underlying] = volume
            # ✅ NEW: Track individual PE premium data for VWAP calculation
            if underlying not in self.pe_premium_data:
                self.pe_premium_data[underlying] = []
            self.pe_premium_data[underlying].append({
                'timestamp': datetime.now(),
                'premium': premium,
                'volume': volume if volume and volume > 0 else 1
            })
            # Keep only last 4 hours of data
            cutoff_time = datetime.now() - timedelta(hours=4)
            self.pe_premium_data[underlying] = [
                d for d in self.pe_premium_data[underlying]
                if d['timestamp'] > cutoff_time
            ]
            self.logger.info(f"✅ [KOW_SIGNAL] {underlying} PE premium updated: {old_pe} -> {premium} (volume={volume}, strike={atm_strike}, symbol={symbol})")
        
        # Calculate combined premium when both CE and PE available for this underlying
        ce_premium = self.ce_premiums.get(underlying)
        pe_premium = self.pe_premiums.get(underlying)
        ce_vol = self.ce_volumes.get(underlying, 0)
        pe_vol = self.pe_volumes.get(underlying, 0)
        
        self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying} premium status: CE={ce_premium}, PE={pe_premium}, CE_vol={ce_vol}, PE_vol={pe_vol}")
        
        if (ce_premium and pe_premium and ce_premium > 0 and pe_premium > 0):
            combined_premium = ce_premium + pe_premium
            total_volume = (ce_vol + pe_vol) // 2  # Average volume
            
            self.logger.info(f"✅ [KOW_SIGNAL] {underlying} Combined premium calculated: {combined_premium:.2f} (CE={ce_premium:.2f} + PE={pe_premium:.2f}, volume={total_volume})")
            
            self.combined_premium_data[underlying].append({
                'timestamp': datetime.now(),
                'premium': combined_premium,
                'volume': total_volume
            })
            
            data_count = len(self.combined_premium_data[underlying])
            self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying} Combined premium data points: {data_count}")
            
            # Keep only last 4 hours of data
            self._clean_old_premium_data(underlying)
        else:
            missing_legs = []
            if not ce_premium or ce_premium == 0:
                missing_legs.append("CE")
            if not pe_premium or pe_premium == 0:
                missing_legs.append("PE")
            self.logger.info(f"⚠️ [KOW_SIGNAL] {underlying} Cannot calculate combined premium - missing legs: {missing_legs} (CE={ce_premium}, PE={pe_premium})")
        
    def _clean_old_premium_data(self, underlying: str):
        """Remove data older than 4 hours to manage memory for specific underlying"""
        if underlying not in self.combined_premium_data:
            return
        cutoff_time = datetime.now() - timedelta(hours=4)
        self.combined_premium_data[underlying] = [
            d for d in self.combined_premium_data[underlying] 
            if d['timestamp'] > cutoff_time
        ]
    
    def _get_current_combined_premium(self, underlying: str) -> float:
        """Get current combined premium for specific underlying"""
        ce_premium = self.ce_premiums.get(underlying)
        pe_premium = self.pe_premiums.get(underlying)
        
        if ce_premium and pe_premium and ce_premium > 0 and pe_premium > 0:
            combined = ce_premium + pe_premium
            self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying} Current combined premium: {combined:.2f} (CE={ce_premium:.2f} + PE={pe_premium:.2f})")
            return combined
        else:
            missing = []
            if not ce_premium or ce_premium == 0:
                missing.append("CE")
            if not pe_premium or pe_premium == 0:
                missing.append("PE")
            self.logger.debug(f"⚠️ [KOW_SIGNAL] {underlying} Cannot get combined premium - missing: {missing} (CE={ce_premium}, PE={pe_premium})")
            return 0.0


    async def _generate_straddle_signals(self, tick_data: Dict) -> Optional[Dict]:
        """
        Generate straddle entry/exit signals based on pure VWAP-based premium positioning.
        
        ✅ PURE VWAP-BASED STRATEGY:
        1. Premium Position: CE/PE premiums vs their individual VWAPs
        2. Trend Analysis: Underlying VWAP to assess market direction
        3. Premium Cheap/Expensive: Combined premium vs average
        4. Confidence Scoring: VWAP-based confidence calculation
        """
        current_time = datetime.now().time()
        
        # Only trade between 9:30 AM and 3:15 PM
        if current_time < dt_time(9, 30) or current_time > dt_time(15, 15):
            self.logger.info(f"🔍 [KOW_SIGNAL] Outside trading hours in _generate_straddle_signals: {current_time} (9:30-15:15)")
            return None

        # Process each underlying separately to generate signals for all ATM straddles
        underlying_symbols = self.config.get('underlying_symbols', ['NIFTY', 'BANKNIFTY'])
        
        # ✅ DEBUG: Periodically log market state for validation (every 5 minutes)
        if not hasattr(self, '_last_debug_time'):
            self._last_debug_time = {}
        
        for underlying in underlying_symbols:
            # Get current combined premium and VWAP for this underlying
            combined_premium = self._get_current_combined_premium(underlying)
            current_vwap = self._calculate_combined_vwap(underlying)
            
            # ✅ DEBUG: Log premium data availability
            ce_premium = self.ce_premiums.get(underlying, 0.0)
            pe_premium = self.pe_premiums.get(underlying, 0.0)
            self.logger.info(f"🔍 [KOW_SIGNAL_DEBUG] {underlying}: CE={ce_premium:.2f}, PE={pe_premium:.2f}, Combined={combined_premium:.2f}, VWAP={current_vwap:.2f}")
            
            if combined_premium == 0:
                self.logger.info(f"❌ [KOW_SIGNAL] {underlying}: Skipping - no combined premium data (CE={ce_premium:.2f}, PE={pe_premium:.2f})")
                continue  # Skip if no premium data

            # Get trend signal for VWAP-based analysis
            trend = self._get_trend_signal(underlying)
            
            # Calculate confidence (for signal metadata only - not used for entry/exit decisions)
            confidence = self._calculate_confidence(underlying)
            
            # ✅ LOG: Track signal generation attempts with VWAP and trend
            premium_diff = combined_premium - current_vwap if current_vwap > 0 else 0
            premium_diff_pct = (premium_diff / current_vwap * 100) if current_vwap > 0 else 0
            self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying}: "
                            f"premium={combined_premium:.2f}, vwap={current_vwap:.2f}, "
                            f"diff={premium_diff:+.2f} ({premium_diff_pct:+.2f}%), "
                            f"trend={trend}, confidence={confidence:.2f}")

            # ✅ REMOVED: Confidence threshold override - entry/exit logic is now in _should_enter_straddle/_should_exit_straddle

            # Get position state for this underlying
            position_state = self.position_states[underlying]
            
            # ✅ REMOVED: Time-based position rules from RiskManager (method doesn't exist)
            
            # ✅ LOG: Track position state
            self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying}: Position={position_state.get('current_position')}, checking signals...")
            
            # ✅ GENERATE SIGNALS - Pure VWAP-based logic with alert gating
            signal = None
            
            if position_state['current_position'] is None:
                # NO POSITION: Check for ENTRY (premium below VWAP)
                should_enter = self._should_enter_straddle(underlying, combined_premium)
                self.logger.info(f"🔍 [KOW_SIGNAL_DEBUG] {underlying}: _should_enter_straddle returned: {should_enter}")
                if should_enter:
                    if self._should_send_alert(underlying, 'SELL_STRADDLE'):
                        signal = self._create_entry_signal(combined_premium, confidence, underlying)
                        if signal:
                            self.logger.info(f"✅ [KOW_SIGNAL] {underlying}: ENTRY signal sent (gating passed)")
                        else:
                            self.logger.warning(f"⚠️ [KOW_SIGNAL] {underlying}: Entry conditions met but signal creation failed")
                    else:
                        self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying}: Entry alert suppressed by gating")
            
            elif position_state['current_position'] == 'BOTH_LEGS':
                # FULL POSITION: Check for EXIT (premium above VWAP or stop loss)
                if self._should_exit_straddle(underlying, combined_premium):
                    if self._should_send_alert(underlying, 'EXIT_STRADDLE'):
                        signal = await self._manage_leg_exit(underlying)
                        if signal:
                            self.logger.info(f"✅ [KOW_SIGNAL] {underlying}: EXIT signal (VWAP-based) - premium={combined_premium:.2f}, vwap={current_vwap:.2f}")
                        else:
                            self.logger.warning(f"⚠️ [KOW_SIGNAL] {underlying}: Exit conditions met but signal creation failed")
                    else:
                        self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying}: Exit alert suppressed by gating")
                else:
                    # Check stop loss
                    sl_signal = self._check_stop_loss(combined_premium, underlying)
                    if sl_signal:
                        if self._should_send_alert(underlying, 'EXIT_STRADDLE'):
                            signal = sl_signal
                            self.logger.info(f"🛑 [KOW_SIGNAL] {underlying}: STOP LOSS triggered")
                        else:
                            self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying}: Stop loss alert suppressed by gating")
                    else:
                        trim_signal = self._apply_time_based_rules(underlying)
                        if trim_signal and self._should_send_alert(underlying, 'EXIT_STRADDLE'):
                            signal = trim_signal
                        elif trim_signal:
                            self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying}: Trim alert suppressed by gating")
            
            else:
                # SINGLE LEG: Check for exit only (simplified - no reentry for now)
                if self._should_exit_straddle(underlying, combined_premium):
                    if self._should_send_alert(underlying, 'EXIT_STRADDLE'):
                        signal = await self._manage_leg_exit(underlying)
                        if signal:
                            self.logger.info(f"✅ [KOW_SIGNAL] {underlying}: EXIT signal (single leg)")
                    else:
                        self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying}: Single leg exit alert suppressed by gating")
                else:
                    # Check stop loss
                    sl_signal = self._check_stop_loss(combined_premium, underlying)
                    if sl_signal:
                        if self._should_send_alert(underlying, 'EXIT_STRADDLE'):
                            signal = sl_signal
                            self.logger.info(f"🛑 [KOW_SIGNAL] {underlying}: STOP LOSS triggered (single leg)")
                        else:
                            self.logger.debug(f"🔍 [KOW_SIGNAL] {underlying}: Single leg stop loss alert suppressed by gating")
            
            # Return first signal found (can be enhanced to return multiple signals)
            if signal:
                self.logger.info(f"🎯 [KOW_SIGNAL] {underlying}: Signal generated - {signal.get('action', 'UNKNOWN')} (confidence={signal.get('confidence', 0):.2f})")
                return signal
        
        return None

    async def _manage_leg_exit(self, underlying: str) -> Dict:
        """Exit loss-making leg based on current P&L for specific underlying"""
        position_state = self.position_states[underlying]
        
        if position_state['current_position'] == 'BOTH_LEGS':
            ce_pnl = self._calculate_leg_pnl('CE', underlying)
            pe_pnl = self._calculate_leg_pnl('PE', underlying)
            
            # Exit the leg with higher loss
            if ce_pnl < pe_pnl:
                action = "EXIT_CE_LEG"
                new_position = "PE_ONLY"
            else:
                action = "EXIT_PE_LEG" 
                new_position = "CE_ONLY"
        else:
            # Exit remaining leg
            if position_state['current_position'] == 'CE_ONLY':
                action = "EXIT_CE_LEG"
                new_position = 'NONE'
            else:
                action = "EXIT_PE_LEG"
                new_position = 'NONE'
        
        position_state['current_position'] = new_position
        return self._create_leg_signal(action, f"EXIT_LOSSMAKING_LEG_{action}", underlying)

    def _check_stop_loss(self, current_premium: float, underlying: str) -> Optional[Dict]:
        """Check stop loss and trailing stop conditions for specific underlying"""
        position_state = self.position_states[underlying]
        
        if position_state['current_position'] is None:
            return None
            
        entry_premium = position_state['entry_combined_premium']
        lot_size = self._get_lot_size(underlying)
        current_profit = (entry_premium - current_premium) * lot_size
        
        # Update trailing SL based on profit levels
        if current_profit >= 1000 and position_state['stop_loss_level'] == 1000:
            position_state['stop_loss_level'] = 500  # Lock in ₹500 profit
        elif current_profit >= 1500:
            position_state['stop_loss_level'] = 1000  # Lock in ₹1000 profit
            
        # Check if stop loss hit
        if current_profit <= -position_state['stop_loss_level']:
            return self._create_exit_signal("STOP_LOSS_HIT", underlying)
            
        return None


    def _create_entry_signal(self, combined_premium: float, confidence: float, underlying: str) -> Dict:
        """Create entry signal for straddle for specific underlying"""
        if underlying not in self.atm_strikes or not self.atm_strikes[underlying]:
            self.logger.warning(f"No ATM strike available for {underlying} entry signal")
            return None
        
        strike = self.atm_strikes[underlying]
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        
        # Get current underlying price for last_price field
        underlying_price = self._get_current_underlying_price(underlying)
        current_time = datetime.now()
        timestamp_ms = int(current_time.timestamp() * 1000)
        current_vwap = self._calculate_combined_vwap(underlying)
        
        # Create symbol representation for alerts (combine both legs)
        primary_symbol = f"{underlying} {expiry} {strike} STRADDLE"
        
        # ✅ ENHANCED: Use time-aware position sizing
        lot_size = self._calculate_position_size(underlying, confidence)
        
        # If time rules block new entries, return None (no signal)
        if lot_size <= 0:
            self.logger.info(
                f"🕐 [KOW_SIGNAL] {underlying} Entry signal blocked by time-based rules "
                f"(confidence={confidence:.2f})"
            )
            return None
        
        entry_premium_points = combined_premium
        entry_premium_rupees = entry_premium_points * lot_size if lot_size else entry_premium_points
        stop_loss_rupees = entry_premium_rupees * self.config.get('straddle_stop_multiplier', 1.5) if entry_premium_rupees else 0.0
        target_rupees = entry_premium_rupees * self.config.get('straddle_target_multiplier', 0.7) if entry_premium_rupees else 0.0
        breakeven_upper = strike + entry_premium_points
        breakeven_lower = strike - entry_premium_points
        ce_price = self.ce_premiums.get(underlying)
        pe_price = self.pe_premiums.get(underlying)
        
        # Get trend and premium data for signal enrichment
        trend = self._get_trend_signal(underlying)
        avg_premium = self._get_average_premium(underlying)
        # Get individual leg VWAPs for VWAP-based logic
        ce_vwap = self._calculate_ce_vwap(underlying)
        pe_vwap = self._calculate_pe_vwap(underlying)
        # Fallback to half of combined VWAP if individual VWAPs not available
        if ce_vwap <= 0 or pe_vwap <= 0:
            ce_vwap = pe_vwap = current_vwap / 2.0 if current_vwap > 0 else 0.0
        if underlying_price and underlying_price > 0:
            expected_move_pct = (entry_premium_points / underlying_price) * 100
        elif current_vwap > 0:
            expected_move_pct = abs(entry_premium_points - current_vwap) / current_vwap * 100
        else:
            expected_move_pct = 0.0
        
        signal = {
            # Core fields required by alert manager and notifiers
            "symbol": primary_symbol,  # Primary symbol for alerts
            "pattern": "kow_signal_straddle",  # Pattern type
            "pattern_type": "kow_signal_straddle",  # Alternative pattern field
            "confidence": confidence,
            "signal": "SELL",  # Signal direction (SELL for straddle)
            "action": "SELL_STRADDLE",  # Specific action
            "last_price": underlying_price,  # Underlying price
            "price": underlying_price,  # Alternative price field
            "timestamp": current_time.isoformat(),  # ISO timestamp
            "timestamp_ms": timestamp_ms,  # Millisecond timestamp for compatibility
            
            # Strategy-specific fields
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": lot_size,
            "lot_size": lot_size,
            "entry_premium": entry_premium_points,
            "entry_premium_points": entry_premium_points,
            "entry_premium_rupees": entry_premium_rupees,
            "ce_premium": ce_price,
            "pe_premium": pe_price,
            "current_vwap": current_vwap,
            "ce_vwap": ce_vwap,
            "pe_vwap": pe_vwap,
            "underlying": underlying,
            "expiry": expiry,
            "reason": f"OPTIONS_CHEAP_BELOW_VWAP_CE={ce_price:.1f}<{ce_vwap:.1f}_PE={pe_price:.1f}<{pe_vwap:.1f}",
            "strategy_type": "kow_signal_straddle",
            "stop_loss_rupees": stop_loss_rupees,
            "target_rupees": target_rupees,
            # ✅ FIX: Add target_price and stop_loss for aion_algo_executor (convert from rupees to price)
            # For straddle: stop_loss is when premium increases, target is when premium decreases
            # Convert rupees to price points (premium points)
            "stop_loss": entry_premium_points + (stop_loss_rupees / lot_size if lot_size > 0 else stop_loss_rupees),  # Premium increases (loss)
            "target_price": entry_premium_points - (target_rupees / lot_size if lot_size > 0 else target_rupees),  # Premium decreases (profit)
            "target": entry_premium_points - (target_rupees / lot_size if lot_size > 0 else target_rupees),  # Alias for compatibility
            "breakeven_upper": breakeven_upper,
            "breakeven_lower": breakeven_lower,
            
            # Trend and premium fields for analysis
            "trend": trend,
            "avg_premium": avg_premium,
            "indicators": {
                "trend": trend,
                "avg_premium": avg_premium
            },
            
            # Additional fields for enrichment
            "volume_ratio": 1.0,  # Placeholder - can be calculated from actual volume
            "expected_move": expected_move_pct
        }

        # Track alert emission for gating
        position_state = self.position_states[underlying]
        position_state['last_alert_action'] = 'SELL_STRADDLE'
        position_state['last_alert_time'] = datetime.now()
        
        # Normalize action/description using shared notifier logic
        # ✅ FIX: Only call fix_kow_signal_straddle if available
        if FIX_KOW_SIGNAL_AVAILABLE and fix_kow_signal_straddle:
            signal = fix_kow_signal_straddle(signal)
        
        # ✅ NOTE: algo_alert payload is created in _store_signal_in_redis() via _create_algo_order_payload()
        # which uses UnifiedAlertBuilder.build_straddle_algo_payload() - single source of truth
        
        # ✅ Generate strategy_id and alert_id for entry signal
        import uuid
        strategy_id = str(uuid.uuid4())
        alert_id = str(uuid.uuid4())
        signal['alert_id'] = alert_id
        signal['strategy_id'] = strategy_id
        
        # Update position state for this underlying
        position_state = self.position_states[underlying]
        position_state.update({
            'current_position': 'BOTH_LEGS',
            'entry_time': datetime.now(),
            'entry_combined_premium': combined_premium,
            'reentry_count': 0,
            'current_strike': strike,
            # ✅ Track alert_id and strategy_id for follow-up alerts
            'entry_alert_id': alert_id,
            'strategy_id': strategy_id
        })
        
        # Track entry prices for P&L calculation (per underlying - would need to add dict tracking)
        # For now, use current premiums
        if underlying in self.ce_premiums:
            # Would need to track per underlying: self.ce_entry_prices[underlying] = self.ce_premiums[underlying]
            pass
        
        return signal
    

    def _create_reentry_signal(self, leg_type: str, confidence: float, underlying: str) -> Dict:
        """Create re-entry signal for closed leg for specific underlying"""
        if underlying not in self.atm_strikes or not self.atm_strikes[underlying]:
            return None
        
        strike = self.atm_strikes[underlying]
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        
        action = f"REENTER_{leg_type}_LEG"
        current_time = datetime.now()
        timestamp_ms = int(current_time.timestamp() * 1000)
        underlying_price = self._get_current_underlying_price(underlying)
        primary_symbol = f"{underlying} {expiry} {strike} {leg_type} REENTRY"
        
        # ✅ Get parent alert_id and strategy_id from position state
        import uuid
        position_state = self.position_states.get(underlying, {})
        parent_alert_id = position_state.get('entry_alert_id')
        strategy_id = position_state.get('strategy_id')
        alert_id = str(uuid.uuid4())
        
        # Update position back to both legs for this underlying
        position_state = self.position_states[underlying]
        position_state['current_position'] = 'BOTH_LEGS'
        
        lot_size = self._get_lot_size(underlying)
        return {
            # Core fields required by alert manager and notifiers
            "symbol": primary_symbol,
            "pattern": "kow_signal_straddle",
            "pattern_type": "kow_signal_straddle",
            "confidence": confidence,
            "signal": "REENTER",
            "action": action,
            "last_price": underlying_price,
            "price": underlying_price,
            "timestamp": current_time.isoformat(),
            "timestamp_ms": timestamp_ms,
            
            # Strategy-specific fields
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": lot_size,
            "reason": f"REENTER_{leg_type}_BELOW_VWAP",
            "underlying": underlying,
            "expiry": expiry,
            "strategy_type": "kow_signal_straddle",
            "leg_type": leg_type,
            "volume_ratio": 1.0,
            "expected_move": 0.0,
            
            # ✅ UUID-based alert_id and contextual linking
            "alert_id": alert_id,
            "parent_alert_id": parent_alert_id,  # Link to entry alert
            "strategy_id": strategy_id  # Link to strategy instance
        }
    
    def _create_leg_signal(self, action: str, reason: str, underlying: str) -> Dict:
        """
        ✅ ENHANCED: Create leg management signal with stop loss and targets for BOTH legs.
        
        Even for partial exits (single leg), we need stop loss and targets for both legs
        because the remaining leg still needs protection.
        """
        if underlying not in self.atm_strikes or not self.atm_strikes[underlying]:
            return None
        
        strike = self.atm_strikes[underlying]
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        current_time = datetime.now()
        timestamp_ms = int(current_time.timestamp() * 1000)
        underlying_price = self._get_current_underlying_price(underlying)
        primary_symbol = f"{underlying} {expiry} {strike} STRADDLE"
        
        lot_size = self._get_lot_size(underlying)
        
        # ✅ Get parent alert_id and strategy_id from position state
        import uuid
        position_state = self.position_states.get(underlying, {})
        parent_alert_id = position_state.get('entry_alert_id')
        strategy_id = position_state.get('strategy_id')
        alert_id = str(uuid.uuid4())
        
        # ✅ Get entry premiums to calculate stop loss and targets
        entry_premium_points = position_state.get('entry_combined_premium', 0.0)
        ce_entry_premium = position_state.get('ce_entry_premium', self.ce_premiums.get(underlying, 0.0))
        pe_entry_premium = position_state.get('pe_entry_premium', self.pe_premiums.get(underlying, 0.0))
        
        # ✅ Get current premiums
        ce_premium = self.ce_premiums.get(underlying, 0.0)
        pe_premium = self.pe_premiums.get(underlying, 0.0)
        
        # ✅ Calculate stop loss and targets for BOTH legs (even for partial exits)
        stop_loss_multiplier = 1.5  # 50% premium increase
        target_multiplier = 0.7     # 30% premium decay
        
        # CE leg stop loss and target
        ce_stop_loss_points = ce_entry_premium * stop_loss_multiplier if ce_entry_premium > 0 else 0.0
        ce_target_points = ce_entry_premium * target_multiplier if ce_entry_premium > 0 else 0.0
        ce_stop_loss_rupees = ce_stop_loss_points * lot_size
        ce_target_rupees = ce_target_points * lot_size
        
        # PE leg stop loss and target
        pe_stop_loss_points = pe_entry_premium * stop_loss_multiplier if pe_entry_premium > 0 else 0.0
        pe_target_points = pe_entry_premium * target_multiplier if pe_entry_premium > 0 else 0.0
        pe_stop_loss_rupees = pe_stop_loss_points * lot_size
        pe_target_rupees = pe_target_points * lot_size
        
        signal = {
            # Core fields required by alert manager and notifiers
            "symbol": primary_symbol,
            "pattern": "kow_signal_straddle",
            "pattern_type": "kow_signal_straddle",
            "confidence": 0.75,
            "signal": action.split("_")[0] if "_" in action else action,  # Extract signal from action
            "action": action,
            "last_price": underlying_price,
            "price": underlying_price,
            "timestamp": current_time.isoformat(),
            "timestamp_ms": timestamp_ms,
            
            # Strategy-specific fields
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": lot_size,
            "lot_size": lot_size,
            "reason": reason,
            "underlying": underlying,
            "expiry": expiry,
            "strategy_type": "kow_signal_straddle",
            
            # ✅ LEG-SPECIFIC STOP LOSS AND TARGETS (for both legs)
            "ce_stop_loss_points": ce_stop_loss_points,
            "ce_stop_loss_rupees": ce_stop_loss_rupees,
            "ce_target_points": ce_target_points,
            "ce_target_rupees": ce_target_rupees,
            "ce_entry_premium": ce_entry_premium,
            "ce_current_premium": ce_premium,
            
            "pe_stop_loss_points": pe_stop_loss_points,
            "pe_stop_loss_rupees": pe_stop_loss_rupees,
            "pe_target_points": pe_target_points,
            "pe_target_rupees": pe_target_rupees,
            "pe_entry_premium": pe_entry_premium,
            "pe_current_premium": pe_premium,
            
            "volume_ratio": 1.0,
            "expected_move": 0.0,
            
            # ✅ UUID-based alert_id and contextual linking
            "alert_id": alert_id,
            "parent_alert_id": parent_alert_id,  # Link to entry alert
            "strategy_id": strategy_id  # Link to strategy instance
        }
        
        # Track alert emission
        position_state['last_alert_action'] = action
        position_state['last_alert_time'] = datetime.now()
        
        return signal
    
    def _create_trim_signal(self, underlying: str, trim_percentage: float) -> Optional[Dict]:
        """
        ✅ NEW: Create trim signal to reduce position size based on time-based rules.
        
        Args:
            underlying: Underlying symbol (NIFTY/BANKNIFTY)
            trim_percentage: Percentage to trim (e.g., 0.5 for 50%)
            
        Returns:
            Trim signal dict or None if no position to trim
        """
        position_state = self.position_states.get(underlying)
        if not position_state or position_state.get('current_position') != 'BOTH_LEGS':
            return None  # No position to trim
        
        try:
            strike = position_state.get('current_strike')
            if not strike:
                self.logger.warning(f"⚠️ [KOW_SIGNAL] {underlying} Cannot create trim signal - no strike available")
                return None
            
            expiry = self._get_current_expiry()
            ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
            underlying_price = self._get_current_underlying_price(underlying)
            current_time = datetime.now()
            timestamp_ms = int(current_time.timestamp() * 1000)
            
            # Calculate trimmed lot size
            base_lot_size = self._get_lot_size(underlying)
            trimmed_lot_size = int(base_lot_size * trim_percentage)
            remaining_lot_size = base_lot_size - trimmed_lot_size
            
            if trimmed_lot_size <= 0:
                self.logger.info(f"⏰ [KOW_SIGNAL] {underlying} Trim percentage {trim_percentage:.0%} results in 0 lots, skipping trim")
                return None
            
            primary_symbol = f"{underlying} {expiry} {strike} STRADDLE"
            
            signal = {
                "symbol": primary_symbol,
                "pattern": "kow_signal_straddle",
                "pattern_type": "kow_signal_straddle",
                "confidence": 0.95,  # High confidence for time-based rules
                "signal": "TRIM",  # Trim action
                "action": "TRIM_STRADDLE",
                "last_price": underlying_price,
                "price": underlying_price,
                "timestamp": current_time.isoformat(),
                "timestamp_ms": timestamp_ms,
                
                # Strategy-specific fields
                "strike": strike,
                "ce_symbol": ce_symbol,
                "pe_symbol": pe_symbol,
                "quantity": trimmed_lot_size,  # Quantity to trim
                "lot_size": trimmed_lot_size,
                "remaining_lot_size": remaining_lot_size,
                "trim_percentage": trim_percentage,
                "underlying": underlying,
                "expiry": expiry,
                "reason": f"TIME_BASED_TRIM_{trim_percentage:.0%}_AT_2:30PM",
                "strategy_type": "kow_signal_straddle",
                
                # Metadata
                "volume_ratio": 1.0,
                "expected_move": 0.0
            }
            
            self.logger.info(
                f"⏰ [KOW_SIGNAL] {underlying} Trim signal created: "
                f"trim={trimmed_lot_size} lots ({trim_percentage:.0%}), "
                f"remaining={remaining_lot_size} lots"
            )
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error creating trim signal for {underlying}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
    
    def _create_exit_signal(self, reason: str, underlying: str) -> Dict:
        """
        ✅ ENHANCED: Create full exit signal with stop loss and targets for BOTH legs (CE and PE).
        
        Exit signals need:
        - Stop loss for CE leg (premium level)
        - Stop loss for PE leg (premium level)
        - Target for CE leg (premium level)
        - Target for PE leg (premium level)
        """
        if underlying not in self.atm_strikes or not self.atm_strikes[underlying]:
            return None
        
        strike = self.atm_strikes[underlying]
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        current_time = datetime.now()
        timestamp_ms = int(current_time.timestamp() * 1000)
        underlying_price = self._get_current_underlying_price(underlying)
        primary_symbol = f"{underlying} {expiry} {strike} STRADDLE"
        
        # Get trend data for exit signal
        trend = self._get_trend_signal(underlying)
        combined_premium = self._get_current_combined_premium(underlying)
        
        # ✅ Get parent alert_id and strategy_id from position state
        import uuid
        position_state = self.position_states.get(underlying, {})
        parent_alert_id = position_state.get('entry_alert_id')
        strategy_id = position_state.get('strategy_id')
        alert_id = str(uuid.uuid4())
        
        # ✅ Get entry premium to calculate exit stop loss and targets
        entry_premium_points = position_state.get('entry_combined_premium', combined_premium)
        entry_premium_rupees = position_state.get('entry_premium_rupees', entry_premium_points * self._get_lot_size(underlying))
        
        lot_size = self._get_lot_size(underlying)
        
        # ✅ Get current CE and PE premiums
        ce_premium = self.ce_premiums.get(underlying, 0.0)
        pe_premium = self.pe_premiums.get(underlying, 0.0)
        ce_entry_premium = position_state.get('ce_entry_premium', ce_premium)
        pe_entry_premium = position_state.get('pe_entry_premium', pe_premium)
        
        # ✅ Calculate stop loss and targets for BOTH legs (CE and PE separately)
        stop_loss_multiplier = 1.5  # 50% premium increase
        target_multiplier = 0.7     # 30% premium decay
        
        # CE leg stop loss and target
        ce_stop_loss_points = ce_entry_premium * stop_loss_multiplier if ce_entry_premium > 0 else 0.0
        ce_target_points = ce_entry_premium * target_multiplier if ce_entry_premium > 0 else 0.0
        ce_stop_loss_rupees = ce_stop_loss_points * lot_size
        ce_target_rupees = ce_target_points * lot_size
        
        # PE leg stop loss and target
        pe_stop_loss_points = pe_entry_premium * stop_loss_multiplier if pe_entry_premium > 0 else 0.0
        pe_target_points = pe_entry_premium * target_multiplier if pe_entry_premium > 0 else 0.0
        pe_stop_loss_rupees = pe_stop_loss_points * lot_size
        pe_target_rupees = pe_target_points * lot_size
        
        # Combined stop loss and target (for reference)
        combined_stop_loss_rupees = ce_stop_loss_rupees + pe_stop_loss_rupees
        combined_target_rupees = ce_target_rupees + pe_target_rupees
        
        signal = {
            # Core fields required by alert manager and notifiers
            "symbol": primary_symbol,
            "pattern": "kow_signal_straddle",
            "pattern_type": "kow_signal_straddle",
            "confidence": 0.8,
            "signal": "EXIT",
            "action": "EXIT_STRADDLE",
            "last_price": underlying_price,
            "price": underlying_price,
            "timestamp": current_time.isoformat(),
            "timestamp_ms": timestamp_ms,
            
            # Strategy-specific fields
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": lot_size,
            "lot_size": lot_size,
            "reason": f"{reason}_TREND={trend}",
            "underlying": underlying,
            "expiry": expiry,
            "strategy_type": "kow_signal_straddle",
            
            # Trend fields for analysis
            "trend": trend,
            "exit_premium": combined_premium,
            "entry_premium": entry_premium_points,
            "entry_premium_rupees": entry_premium_rupees,
            
            # ✅ EXIT STOP LOSS AND TARGETS FOR BOTH LEGS
            "stop_loss_rupees": combined_stop_loss_rupees,
            "target_rupees": combined_target_rupees,
            "stop_loss": combined_premium * stop_loss_multiplier,  # Combined premium stop level
            "target_price": combined_premium * target_multiplier,   # Combined premium target level
            "target": combined_premium * target_multiplier,  # Alias
            
            # ✅ LEG-SPECIFIC STOP LOSS AND TARGETS
            "ce_stop_loss_points": ce_stop_loss_points,
            "ce_stop_loss_rupees": ce_stop_loss_rupees,
            "ce_target_points": ce_target_points,
            "ce_target_rupees": ce_target_rupees,
            "ce_entry_premium": ce_entry_premium,
            "ce_current_premium": ce_premium,
            
            "pe_stop_loss_points": pe_stop_loss_points,
            "pe_stop_loss_rupees": pe_stop_loss_rupees,
            "pe_target_points": pe_target_points,
            "pe_target_rupees": pe_target_rupees,
            "pe_entry_premium": pe_entry_premium,
            "pe_current_premium": pe_premium,
            
            "volume_ratio": 1.0,
            "expected_move": 0.0,
            
            # ✅ UUID-based alert_id and contextual linking
            "alert_id": alert_id,
            "parent_alert_id": parent_alert_id,  # Link to entry alert
            "strategy_id": strategy_id  # Link to strategy instance
        }
        
        # Reset position state for this underlying
        position_state = self.position_states[underlying]
        position_state.update({
            'current_position': None,
            'entry_time': None,
            'entry_combined_premium': 0.0,
            'reentry_count': 0,
            'stop_loss_level': 1000
        })
        
        # Update Redis state
        self._save_strategy_state_to_redis()
        position_state['last_alert_action'] = 'EXIT_STRADDLE'
        position_state['last_alert_time'] = datetime.now()
        
        return signal

    def _create_signal(self, action: str, strike: int, reason: str) -> Dict:
        """Create standardized signal format"""
        expiry = self._get_current_expiry()
        underlying = self.config.get('underlying_symbols', ['NIFTY'])[0]
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        
        return {
            "action": action,
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": self.config.get('lot_size', 50),  # ✅ FIXED: Use config lot_size instead of position_state
            "reason": reason,
            "confidence": 0.75,
            "timestamp": datetime.now().isoformat(),
            "underlying": underlying,
            "expiry": expiry
        }
    

    def _calculate_leg_pnl(self, option_type: str, underlying: str) -> float:
        """Calculate current P&L for individual leg for specific underlying"""
        if underlying not in self.ce_premiums or underlying not in self.pe_premiums:
            return 0.0
        
        current_premium = self.ce_premiums[underlying] if option_type == 'CE' else self.pe_premiums[underlying]
        
        # For now, use current premium as entry (would need to track entry prices per underlying)
        # TODO: Track entry prices per underlying: self.ce_entry_prices[underlying]
        if current_premium:
            lot_size = 50 if underlying == 'NIFTY' else 15
            # Simplified P&L calculation (would need entry prices tracked per underlying)
            return 0.0  # Placeholder - would calculate based on entry vs current
        
        return 0.0
    
    def _get_current_underlying_price(self, underlying: str = None) -> float:
        """Get current underlying price from tracked data or Redis for specific underlying"""
        # First try in-memory tracking
        if self.underlying_prices:
            if underlying:
                upper = underlying.upper()
                # Prefer explicit matches on stored underlying key
                matching_prices = [p for p in self.underlying_prices if p.get('underlying') == upper]
                if matching_prices:
                    price = matching_prices[-1]['price']
                    source_symbol = matching_prices[-1].get('symbol', 'UNKNOWN')
                    # ✅ DEBUG: Log price source and validate
                    if not self._is_valid_underlying_price(upper, price):
                        self.logger.warning(
                            f"⚠️ [KOW_SIGNAL] Invalid underlying price from in-memory: {price} for {upper} "
                            f"(source_symbol={source_symbol}). Rejecting and trying Redis..."
                        )
                        # Don't return invalid price, fall through to Redis
                    else:
                        self.logger.debug(f"✅ [KOW_SIGNAL] Using in-memory price for {upper}: {price} (source={source_symbol})")
                        return price
                # Fallback: match by symbol only when it clearly belongs to the same underlying
                def symbol_matches(entry):
                    symbol_upper = str(entry.get('symbol', '')).upper()
                    if upper == 'NIFTY':
                        return ('NIFTY' in symbol_upper) and ('BANK' not in symbol_upper)
                    if upper == 'BANKNIFTY':
                        return ('BANKNIFTY' in symbol_upper) or ('NIFTY BANK' in symbol_upper) or ('BANK NIFTY' in symbol_upper)
                    return upper in symbol_upper
                matching_prices = [p for p in self.underlying_prices if symbol_matches(p)]
                if matching_prices:
                    price = matching_prices[-1]['price']
                    source_symbol = matching_prices[-1].get('symbol', 'UNKNOWN')
                    # ✅ DEBUG: Validate before returning
                    if not self._is_valid_underlying_price(upper, price):
                        self.logger.warning(
                            f"⚠️ [KOW_SIGNAL] Invalid underlying price from symbol match: {price} for {upper} "
                            f"(source_symbol={source_symbol}). Rejecting and trying Redis..."
                        )
                        # Don't return invalid price, fall through to Redis
                    else:
                        self.logger.debug(f"✅ [KOW_SIGNAL] Using symbol-matched price for {upper}: {price} (source={source_symbol})")
                        return price
            else:
                price = self.underlying_prices[-1]['price']
                source_symbol = self.underlying_prices[-1].get('symbol', 'UNKNOWN')
                # ✅ DEBUG: Validate before returning
                if not self._is_valid_underlying_price('UNKNOWN', price):
                    self.logger.warning(f"⚠️ [KOW_SIGNAL] Invalid underlying price from last entry: {price} (source={source_symbol})")
                    # Don't return invalid price, fall through to Redis
                else:
                    return price
        
        # Fallback to Redis (DB 1 - realtime)
        # Index data is published every 30 seconds by gift_nifty_gap.py
        # Primary keys: index:NSE:NIFTY 50, index:NSE:NIFTY BANK
        if self.redis_realtime:
            try:
                import json
                
                # Determine which underlying to fetch
                if underlying:
                    underlying_upper = underlying.upper()
                else:
                    # Default to first underlying in config
                    underlying_symbols = self.config.get('underlying_symbols', ['NIFTY'])
                    underlying_upper = underlying_symbols[0].upper() if underlying_symbols else 'NIFTY'
                
                # Map underlying to index keys (as published by gift_nifty_gap.py every 30 seconds)
                # Priority: Redis Hash (direct field access, no JSON parsing) > JSON string
                if 'NIFTY' in underlying_upper and 'BANK' not in underlying_upper:
                    # NIFTY 50 index keys
                    hash_key = "index_hash:NSE:NIFTY 50"  # Redis Hash (fast, no JSON parsing)
                    json_keys = [
                        "index:NSE:NIFTY 50",  # JSON string (backward compatibility)
                        "market_data:indices:nse_nifty_50",  # Secondary JSON key
                    ]
                elif 'BANKNIFTY' in underlying_upper or 'BANK' in underlying_upper:
                    # BANKNIFTY index keys
                    hash_key = "index_hash:NSE:NIFTY BANK"  # Redis Hash (fast, no JSON parsing)
                    json_keys = [
                        "index:NSE:NIFTY BANK",  # JSON string (backward compatibility)
                        "market_data:indices:nse_nifty_bank",  # Secondary JSON key
                    ]
                else:
                    # Unknown underlying, try NIFTY as default
                    hash_key = "index_hash:NSE:NIFTY 50"
                    json_keys = ["index:NSE:NIFTY 50"]
                
                # ✅ PRIORITY 1: Try Redis Hash first (direct field access, no JSON parsing)
                try:
                    last_price = self.redis_realtime.hget(hash_key, 'last_price')
                    if last_price:
                        if isinstance(last_price, bytes):
                            last_price = last_price.decode('utf-8')
                        price = float(last_price)
                        # ✅ FIX: Validate price before returning (reject obviously wrong values)
                        if price > 0 and self._is_valid_underlying_price(underlying_upper, price):
                            self.logger.debug(f"✅ [KOW_SIGNAL] Got valid underlying price from Redis hash {hash_key}: {price} for {underlying_upper}")
                            return price
                        else:
                            # Price is invalid, continue to next source
                            # ✅ FIX: Only log warning during active trading, not during initialization
                            # Check if we have active tick data (volume > 0) before logging
                            self.logger.debug(
                                f"⚠️ [KOW_SIGNAL] Invalid price {price} from Redis hash {hash_key} for {underlying_upper} "
                                f"(expected range: NIFTY 20k-30k, BANKNIFTY 50k-70k). Trying next source..."
                            )
                except Exception as hash_error:
                    self.logger.debug(f"⚠️ [KOW_SIGNAL] Error reading Redis hash {hash_key}: {hash_error}")
                    pass  # Fallback to JSON
                
                # ✅ PRIORITY 2: Fallback to JSON strings (backward compatibility)
                for key in json_keys:
                    try:
                        index_data = self.redis_realtime.get(key)
                        if index_data:
                            if isinstance(index_data, bytes):
                                index_data = index_data.decode('utf-8')
                            
                            # Parse JSON if it's a string
                            if isinstance(index_data, str):
                                try:
                                    index_json = json.loads(index_data)
                                except (json.JSONDecodeError, TypeError):
                                    # If not JSON, try to parse as float directly
                                    try:
                                        return float(index_data)
                                    except (ValueError, TypeError):
                                        continue
                                else:
                                    # Extract last_price from JSON (gift_nifty_gap.py format)
                                    price = index_json.get('last_price') or index_json.get('price') or index_json.get('close')
                                    if price:
                                        price_float = float(price)
                                        # ✅ FIX: Validate price before returning
                                        if self._is_valid_underlying_price(underlying_upper, price_float):
                                            return price_float
                                        else:
                                            self.logger.debug(f"⚠️ [KOW_SIGNAL] Invalid price {price_float} from JSON key {key} for {underlying_upper}")
                                            continue
                            elif isinstance(index_data, dict):
                                # Already a dict
                                price = index_data.get('last_price') or index_data.get('price') or index_data.get('close')
                                if price:
                                    price_float = float(price)
                                    # ✅ FIX: Validate price before returning
                                    if self._is_valid_underlying_price(underlying_upper, price_float):
                                        return price_float
                                    else:
                                        self.logger.debug(f"⚠️ [KOW_SIGNAL] Invalid price {price_float} from dict key {key} for {underlying_upper}")
                                        continue
                    except Exception as key_error:
                        continue  # Try next key
                
                # Fallback to OHLC data from Redis (ohlc_latest:* keys in DB 2)
                try:
                    from shared_core.redis_clients.redis_client import RedisClientFactory
                    redis_analytics = RedisClientFactory.get_trading_client()
                    
                    # Try ohlc_latest key pattern
                    # Use DatabaseAwareKeyBuilder - normalizes internally, just pass raw symbol
                    from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
                    if 'BANKNIFTY' in underlying_upper or 'BANK' in underlying_upper:
                        ohlc_symbol = "NSE:NIFTY BANK"
                    else:
                        ohlc_symbol = "NSE:NIFTY 50"
                    
                    # Use DatabaseAwareKeyBuilder for OHLC latest (DB1)
                    ohlc_key = DatabaseAwareKeyBuilder.live_ohlc_latest(ohlc_symbol)
                    ohlc_data = redis_analytics.hget(ohlc_key, 'close')
                    if ohlc_data:
                        if isinstance(ohlc_data, bytes):
                            ohlc_data = ohlc_data.decode('utf-8')
                        price_float = float(ohlc_data)
                        # ✅ FIX: Validate price before returning
                        if self._is_valid_underlying_price(underlying_upper, price_float):
                            return price_float
                        else:
                            self.logger.debug(f"⚠️ [KOW_SIGNAL] Invalid OHLC price {price_float} for {underlying_upper}")
                except Exception as ohlc_error:
                    self.logger.debug(f"Error getting OHLC data: {ohlc_error}")
                    
            except Exception as e:
                self.logger.debug(f"Error getting underlying price from Redis: {e}")
        
        return 0.0
    
    def _get_current_expiry(self) -> str:
        """Get current expiry in format like '25NOV' or '25DEC'"""
        current_date = datetime.now().date()
        current_year = str(current_date.year)[-2:]  # Last 2 digits
        
        # Determine current month's expiry
        month = current_date.month
        month_names = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 
                      'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        
        # For NIFTY, expiry is typically last Thursday of the month
        # For simplicity, use current month + year format
        return f"{current_year}{month_names[month-1]}"

    async def _store_signal_in_redis(self, signal: Dict):
        """Store signal in Redis for tracking and dashboard using standard key patterns"""
        try:
            import json
            # ✅ FIXED: Use ORJSON_AVAILABLE flag instead of direct import
            # orjson is imported at module level if available
            signal_date = datetime.now().strftime('%Y%m%d')
            signal_time = datetime.now().strftime('%H%M%S')
            
            # ✅ FIX: Clean signal dict to remove any non-serializable objects (Redis clients, etc.)
            clean_signal = {}
            for key, value in signal.items():
                try:
                    # Test if value is JSON serializable
                    json.dumps(value)
                    clean_signal[key] = value
                except (TypeError, ValueError):
                    # Skip non-serializable values or convert to string
                    if value is not None:
                        clean_signal[key] = str(value)
                    else:
                        clean_signal[key] = None
            
            # Store signal in DB 1 (realtime) for alerts stream
            signal_key = f"signals:kow:{signal_date}:{signal['action'].lower()}"
            
            if self.redis_realtime:
                # Store as JSON string in Redis
                signal_json = json.dumps(clean_signal, default=str)
                self.redis_realtime.set(signal_key, signal_json)
                self.redis_realtime.expire(signal_key, 86400)  # Keep for 24 hours
                
                # ✅ CENTRALIZED: Create and store algo order payload via UnifiedAlertBuilder
                # This is the SINGLE SOURCE OF TRUTH for all algo payloads
                try:
                    algo_payload = self._create_algo_order_payload(clean_signal)
                    if algo_payload:
                        # Also add algo_alert to signal dict for downstream consumers
                        clean_signal['algo_alert'] = algo_payload
                        
                        symbol = clean_signal.get('symbol', 'UNKNOWN')
                        pattern = clean_signal.get('pattern_type', 'kow_signal_straddle')
                        timestamp_ms = clean_signal.get('timestamp_ms', int(time.time() * 1000))
                        algo_payload_key = f"alert_payload:{symbol}:{pattern}:{timestamp_ms}"
                        algo_payload_json = json.dumps(algo_payload, default=str)
                        self.redis_realtime.set(algo_payload_key, algo_payload_json)
                        self.redis_realtime.expire(algo_payload_key, 3600)  # 1 hour TTL
                        self.logger.info(f"✅ [KOW_SIGNAL] Stored algo payload via UnifiedAlertBuilder: {algo_payload_key}")
                    else:
                        self.logger.error(f"❌ [KOW_SIGNAL] _create_algo_order_payload returned None for {clean_signal.get('symbol', 'UNKNOWN')}")
                except Exception as e:
                    self.logger.error(f"❌ [KOW_SIGNAL] Error creating algo payload via UnifiedAlertBuilder: {e}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                
                # Also publish to alerts stream for real-time processing (DB 1 - realtime)
                try:
                    alerts_stream = "alerts:stream"
                    
                    # Serialize signal to binary JSON for alerts:stream (matches alert manager expectations)
                    if HAS_ORJSON:
                        try:
                            # Use orjson for faster binary serialization if available
                            signal_binary = orjson.dumps(clean_signal, option=orjson.OPT_SERIALIZE_NUMPY)
                        except Exception as orjson_err:
                            # Fallback to standard json if orjson fails
                            self.logger.debug(f"⚠️ [KOW_SIGNAL] orjson serialization failed, using json fallback: {orjson_err}")
                            signal_binary = json.dumps(clean_signal, default=str).encode('utf-8')
                    else:
                        # Fallback to standard json (orjson not available)
                        signal_binary = json.dumps(clean_signal, default=str).encode('utf-8')
                    
                    # Publish to alerts:stream with binary JSON in 'data' field (standard format)
                    stream_data = {
                        b'data': signal_binary,  # Binary JSON for compatibility with alert manager
                    }
                    # Use DB 1 (realtime) for alerts:stream
                    self.redis_realtime.xadd(alerts_stream, stream_data, maxlen=1000, approximate=True)
                except Exception as e:
                    self.logger.error(f"Error publishing to alerts stream: {e}")
                    import traceback
                    self.logger.error(traceback.format_exc())
            
            # Store strategy state in DB 0 (system)
            if self.redis_system:
                # ✅ FIXED: Use position_states (per underlying) instead of position_state
                if self.config.get('underlying_symbols'):
                    underlying = self.config['underlying_symbols'][0]
                    position_state = self.position_states.get(underlying, {})
                    
                    state_key = "strategy:kow:position"
                    # ✅ FIXED: Handle None values - Redis doesn't accept NoneType
                    # ✅ FIXED: Avoid double string conversion (check if already string)
                    current_position = position_state.get('current_position')
                    if current_position is None:
                        current_position = 'NONE'
                    # Only convert to string if not already a string (prevents nested string corruption)
                    if not isinstance(current_position, str):
                        current_position = str(current_position)
                    self.redis_system.set(state_key, current_position)
                    
                    entry_premium = position_state.get('entry_combined_premium')
                    if entry_premium is not None:
                        self.redis_system.set("strategy:kow:entry_premium", str(entry_premium))
                    
                    if self.current_atm_strike is not None:
                        self.redis_system.set("strategy:kow:current_strike", str(self.current_atm_strike))
                    
        except Exception as e:
            self.logger.error(f"Failed to store signal in Redis: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    def _create_algo_order_payload(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        ✅ Create standardized algo order payload using UnifiedAlertBuilder.
        
        Delegates to UnifiedAlertBuilder.build_straddle_algo_payload() for consistent
        straddle order payload structure across the system.
        """
        try:
            # ✅ Use UnifiedAlertBuilder for consistent payload structure
            # Try multiple import paths for compatibility
            try:
                # First try: absolute import from intraday_trading/alerts
                from alerts.unified_alert_builder import get_unified_alert_builder
            except ImportError:
                try:
                    # Second try: relative import from alerts (if in same package)
                    from alerts.unified_alert_builder import get_unified_alert_builder
                except ImportError:
                    # Final fallback: try relative import using sys.path
                    import sys
                    import os
                    alerts_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'alerts')
                    if alerts_path not in sys.path:
                        sys.path.insert(0, alerts_path)
                    try:
                        from unified_alert_builder import get_unified_alert_builder
                    except ImportError:
                        # Last resort: try importing from parent directory
                        parent_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                        if parent_path not in sys.path:
                            sys.path.insert(0, parent_path)
                        from alerts.unified_alert_builder import get_unified_alert_builder
            
            builder = get_unified_alert_builder(redis_client=self.redis_realtime)
            if builder and hasattr(builder, 'build_straddle_algo_payload'):
                return builder.build_straddle_algo_payload(alert_data)
            else:
                raise AttributeError("UnifiedAlertBuilder.build_straddle_algo_payload not available")
            
        except (ImportError, AttributeError, Exception) as e:
            # Fallback if UnifiedAlertBuilder not available
            self.logger.warning(f"⚠️ [KOW_SIGNAL] UnifiedAlertBuilder not available ({e}), using fallback")
            try:
                # Extract essential data from alert
                symbol = alert_data.get('symbol', '')
                action = alert_data.get('action', '')
                underlying = alert_data.get('underlying', '')
                strike = alert_data.get('strike', 0)
                expiry = alert_data.get('expiry', '')
                ce_symbol = alert_data.get('ce_symbol', '')
                pe_symbol = alert_data.get('pe_symbol', '')
                
                # Premium and lot data
                entry_premium_points = float(alert_data.get('entry_premium', alert_data.get('exit_premium', 0)))
                lot_size = int(alert_data.get('lot_size', alert_data.get('quantity', 50)))
                entry_premium_rupees = float(alert_data.get('entry_premium_rupees', entry_premium_points * lot_size))
                
                # Current market prices (for reference)
                underlying_price = float(alert_data.get('last_price', alert_data.get('price', 0)))
                ce_premium = float(alert_data.get('ce_premium', alert_data.get('ce_current_premium', 0)))
                pe_premium = float(alert_data.get('pe_premium', alert_data.get('pe_current_premium', 0)))
                current_vwap = float(alert_data.get('current_vwap', 0))
                
                # ✅ Get leg-specific stop loss and targets (if available from exit signals)
                ce_stop_loss_points = float(alert_data.get('ce_stop_loss_points', 0))
                ce_target_points = float(alert_data.get('ce_target_points', 0))
                pe_stop_loss_points = float(alert_data.get('pe_stop_loss_points', 0))
                pe_target_points = float(alert_data.get('pe_target_points', 0))
                
                # Calculate risk parameters (fallback if not provided)
                stop_loss_multiplier = 1.5  # 50% premium increase
                target_multiplier = 0.7     # 30% premium decay
                
                # If leg-specific values not provided, calculate them
                if ce_stop_loss_points == 0 and entry_premium_points > 0:
                    ce_entry_premium = float(alert_data.get('ce_entry_premium', ce_premium))
                    ce_stop_loss_points = ce_entry_premium * stop_loss_multiplier if ce_entry_premium > 0 else entry_premium_points * stop_loss_multiplier / 2
                    ce_target_points = ce_entry_premium * target_multiplier if ce_entry_premium > 0 else entry_premium_points * target_multiplier / 2
                
                if pe_stop_loss_points == 0 and entry_premium_points > 0:
                    pe_entry_premium = float(alert_data.get('pe_entry_premium', pe_premium))
                    pe_stop_loss_points = pe_entry_premium * stop_loss_multiplier if pe_entry_premium > 0 else entry_premium_points * stop_loss_multiplier / 2
                    pe_target_points = pe_entry_premium * target_multiplier if pe_entry_premium > 0 else entry_premium_points * target_multiplier / 2
                
                # Convert to rupees
                ce_stop_loss_rupees = ce_stop_loss_points * lot_size
                ce_target_rupees = ce_target_points * lot_size
                pe_stop_loss_rupees = pe_stop_loss_points * lot_size
                pe_target_rupees = pe_target_points * lot_size
                
                # Combined values
                stop_loss_rupees = float(alert_data.get('stop_loss_rupees', ce_stop_loss_rupees + pe_stop_loss_rupees))
                target_rupees = float(alert_data.get('target_rupees', ce_target_rupees + pe_target_rupees))
                
                # Calculate actual stop loss and target PRICES (not premium)
                # For straddle selling: Stop when premium INCREASES by 50%
                if action == 'SELL_STRADDLE':
                    stop_premium = entry_premium_points * stop_loss_multiplier  # Premium increases
                    target_premium = entry_premium_points * target_multiplier   # Premium decreases
                else:
                    # Exit or other actions
                    stop_premium = entry_premium_points * stop_loss_multiplier
                    target_premium = entry_premium_points * target_multiplier
                
                # Create the algo order payload
                algo_payload = {
                # ========== CORE ORDER DATA ==========
                'order_type': 'STRADDLE_ORDER',
                'strategy_name': 'KOW_SIGNAL_PREMIUM_COLLECTION',
                'timestamp': alert_data.get('timestamp', datetime.now().isoformat()),
                'timestamp_ms': alert_data.get('timestamp_ms', int(time.time() * 1000)),
                
                # ========== INSTRUMENT DETAILS ==========
                'primary_symbol': symbol,
                'underlying': underlying,
                'strike': strike,
                'expiry': expiry,
                'option_type': 'STRADDLE',
                'exchange': 'NFO',
                
                # Individual leg symbols
                'ce_symbol': ce_symbol,
                'pe_symbol': pe_symbol,
                'ce_exchange': 'NFO',
                'pe_exchange': 'NFO',
                
                # ========== ORDER PARAMETERS ==========
                'action': action,  # 'SELL_STRADDLE', 'EXIT_STRADDLE', etc.
                'transaction_type': 'SELL' if action == 'SELL_STRADDLE' else 'BUY',
                'quantity': lot_size,  # Number of lots
                'lot_size': lot_size,
                'total_quantity': lot_size * 2,  # Both legs
                
                # ========== PRICE PARAMETERS ==========
                'entry_premium_points': entry_premium_points,
                'entry_premium_rupees': entry_premium_rupees,
                'ce_entry_price': ce_premium,
                'pe_entry_price': pe_premium,
                
                'current_underlying_price': underlying_price,
                'current_vwap': current_vwap,
                
                # ========== STOP LOSS CONFIGURATION ==========
                'stop_loss': {
                    'enabled': True,
                    'type': 'PREMIUM_BASED',
                    'stop_loss_points': stop_loss_rupees / lot_size if lot_size > 0 else 0,
                    'stop_loss_rupees': stop_loss_rupees,
                    'stop_premium_level': stop_premium,
                    'stop_loss_percentage': 50.0,  # 50% premium increase
                    'trigger_type': 'PREMIUM_THRESHOLD',
                    'leg_specific_stops': {
                        'ce_stop': ce_stop_loss_points,
                        'ce_stop_rupees': ce_stop_loss_rupees,
                        'pe_stop': pe_stop_loss_points,
                        'pe_stop_rupees': pe_stop_loss_rupees
                    }
                },
                
                # ========== TARGET CONFIGURATION ==========
                'target': {
                    'enabled': True,
                    'type': 'PREMIUM_DECAY',
                    'target_points': target_rupees / lot_size if lot_size > 0 else 0,
                    'target_rupees': target_rupees,
                    'target_premium_level': target_premium,
                    'target_percentage': 30.0,  # 30% premium decay
                    'leg_specific_targets': {
                        'ce_target': ce_target_points,
                        'ce_target_rupees': ce_target_rupees,
                        'pe_target': pe_target_points,
                        'pe_target_rupees': pe_target_rupees
                    },
                    'partial_exits': [
                        {'percentage': 50, 'premium_decay': 20},  # Exit 50% at 20% decay
                        {'percentage': 50, 'premium_decay': 30}   # Exit remaining at 30% decay
                    ]
                },
                
                # ========== POSITION MANAGEMENT ==========
                'position_management': {
                    'max_position_duration_hours': 6,
                    'auto_rollover': False,
                    'delta_hedge_threshold': 0.3,
                    'gamma_risk_threshold': 0.15,
                    'exit_on_vwap_break': True,
                    'exit_if_leg_above_vwap': True,
                    'trailing_stop_enabled': False
                },
                
                # ========== RISK PARAMETERS ==========
                'risk_parameters': {
                    'max_capital_per_trade': 50000,  # ₹50,000
                    'max_loss_per_trade': 25000,     # ₹25,000
                    'risk_reward_ratio': f"1:{target_multiplier}",
                    'margin_required': entry_premium_rupees * 3,
                    'breakeven_upper': strike + entry_premium_points,
                    'breakeven_lower': strike - entry_premium_points,
                    'expected_move_percent': (entry_premium_points / underlying_price * 100) if underlying_price > 0 else 0
                },
                
                # ========== ALGO EXECUTION SETTINGS ==========
                'execution_settings': {
                    'order_type': 'LIMIT',
                    'price_type': 'PREMIUM',
                    'validity': 'DAY',
                    'disclosed_quantity': 0,
                    'product_type': 'NRML',
                    'amo_order': False,
                    'iceberg_order': False,
                    'strategy_id': 'KOW_VWAP_STRADDLE',
                    'client_id': 'KOW_SIGNAL_BOT'
                },
                
                # ========== MONITORING & ALERTS ==========
                'monitoring': {
                    'check_interval_seconds': 30,
                    'alert_on_sl_trigger': True,
                    'alert_on_target_achieved': True,
                    'alert_on_leg_above_vwap': True,
                    'notify_channels': ['TELEGRAM', 'WEBHOOK', 'EMAIL'],
                    'webhook_url': 'https://your-webhook.com/algo-orders'
                },
                
                # ========== META DATA ==========
                'metadata': {
                    'strategy_version': '2.0',
                    'logic_type': 'VWAP_PREMIUM_COLLECTION',
                    'confidence': float(alert_data.get('confidence', 0.8)),
                    'trend': alert_data.get('trend', ''),
                    'avg_premium': float(alert_data.get('avg_premium', 0)),
                    'entry_conditions_met': alert_data.get('confidence_boosters', []),
                    'risk_summary': alert_data.get('risk_summary', '')
                }
                }
                
                return algo_payload
                
            except Exception as e:
                self.logger.error(f"Error creating algo order payload (fallback): {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                # Return minimal payload on error
                return {
                    'symbol': alert_data.get('symbol', 'UNKNOWN'),
                    'action': alert_data.get('action', 'UNKNOWN'),
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
        except Exception as e:
            self.logger.error(f"Error creating algo order payload: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            # Return minimal payload on error
            return {
                'symbol': alert_data.get('symbol', 'UNKNOWN'),
                'action': alert_data.get('action', 'UNKNOWN'),
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
        
    async def _send_alert(self, signal: Dict):
        """
        ✅ ENHANCED: Send alert using standard human-readable format from notifiers.
        
        Uses HumanReadableAlertTemplates.format_actionable_alert() for consistent formatting.
        This ensures all Kow Signal alerts use the same format as other patterns.
        """
        try:
            # ✅ Use standard human-readable format from notifiers
            from alerts.notifiers import HumanReadableAlertTemplates
            
            # Format alert using standard template
            formatted_alert = HumanReadableAlertTemplates.format_actionable_alert(signal)
            
            # ✅ FIX: Check if event loop is running before using asyncio.to_thread
            # If loop is shutdown, call synchronously instead
            try:
                loop = asyncio.get_running_loop()
                if loop.is_closed():
                    # Loop is closed - call synchronously
                    self.notifier.send_alert(signal)
                else:
                    # Loop is running - use asyncio.to_thread
                    await asyncio.to_thread(self.notifier.send_alert, signal)
            except RuntimeError:
                # No running loop or loop is shutdown - call synchronously
                self.notifier.send_alert(signal)
            
            self.logger.debug(f"✅ [KOW_SIGNAL] Alert sent using standard format: {signal.get('action', 'UNKNOWN')}")
        except Exception as e:
            self.logger.error(f"Failed to send alert: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    def _is_trading_hours(self) -> bool:
        """Check if current time is within trading hours (9:30 AM - 3:15 PM)"""
        current_time = datetime.now().time()
        return current_time >= dt_time(9, 30) and current_time <= dt_time(15, 15)

    def _should_send_alert(self, underlying: str, action: str) -> bool:
        """Gating helper to suppress repeated alerts of the same action."""
        position_state = self.position_states.get(underlying)
        if not position_state:
            return True

        last_action = position_state.get('last_alert_action')
        last_time = position_state.get('last_alert_time')
        cooldown = position_state.get('alert_cooldown_seconds', 30)

        if last_action is None or last_time is None:
            return True
        if action != last_action:
            return True

        time_since_last = (datetime.now() - last_time).total_seconds()
        if time_since_last >= cooldown:
            return True

        self.logger.debug(
            f"🔍 [KOW_SIGNAL] {underlying}: Suppressing repeated {action} alert "
            f"(last sent {time_since_last:.0f}s ago, cooldown={cooldown}s)"
        )
        return False

    def _load_strategy_state_from_redis(self):
        """Load strategy state from Redis on startup"""
        if not self.redis_system:
            return
        
        try:
            position = self.redis_system.get("strategy:kow:position")
            entry_premium = self.redis_system.get("strategy:kow:entry_premium")
            current_strike = self.redis_system.get("strategy:kow:current_strike")
            
            # ✅ FIXED: Use position_states (per underlying) instead of position_state
            # For backward compatibility, apply to first underlying if available
            if self.config.get('underlying_symbols'):
                underlying = self.config['underlying_symbols'][0]
                if underlying not in self.position_states:
                    self.position_states[underlying] = {
                        'current_position': None,
                        'entry_time': None,
                        'entry_combined_premium': 0.0,
                        'current_strike': None,
                        'stop_loss_level': 1000,
                        'reentry_count': 0,
                        'current_profit': 0.0,
                        'last_alert_action': None,
                        'last_alert_time': None,
                        'alert_cooldown_seconds': 30,
                    }
                
                if position:
                    # ✅ FIXED: Clean position value (remove nested string corruption)
                    if isinstance(position, bytes):
                        position = position.decode('utf-8')
                    position = str(position).strip("'\"b")  # Remove quotes and 'b' prefix
                    if position and position != 'NONE' and not position.startswith('b'):
                        self.position_states[underlying]['current_position'] = position
                if entry_premium:
                    try:
                        if isinstance(entry_premium, bytes):
                            entry_premium = entry_premium.decode('utf-8')
                        self.position_states[underlying]['entry_combined_premium'] = float(entry_premium)
                    except (ValueError, TypeError):
                        pass
                if current_strike:
                    try:
                        if isinstance(current_strike, bytes):
                            current_strike = current_strike.decode('utf-8')
                        self.current_atm_strike = int(current_strike)
                        self.position_states[underlying]['current_strike'] = int(current_strike)
                    except (ValueError, TypeError):
                        pass
            
            # ✅ NEW: Attempt to load historical combined premium data from stream
            # This allows strategy to recover VWAP if started mid-session
            # ✅ FIX: Skip during initialization if no ticks are flowing (prevents false warnings)
            # Only load if we're in trading hours or if there's recent data
            from datetime import datetime, time as dt_time
            current_time = datetime.now().time()
            is_trading_hours = current_time >= dt_time(9, 15) and current_time <= dt_time(15, 30)
            if is_trading_hours:
                self._load_historical_premium_data_from_stream()
            else:
                self.logger.debug("Skipping historical data load - outside trading hours")
            
        except Exception as e:
            self.logger.warning(f"Error loading strategy state from Redis: {e}")
    
    def _load_historical_premium_data_from_stream(self):
        """Load historical combined premium data from Redis stream for VWAP calculation"""
        if not self.redis_realtime:
            return
        
        try:
            # Use DatabaseAwareKeyBuilder instead of hardcoded key
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            stream_key = DatabaseAwareKeyBuilder.live_processed_stream()
            
            # Get today's session start time
            today = datetime.now().date()
            session_start = datetime.combine(today, dt_time(9, 15))
            
            # Get stream messages from session start
            # Use XRANGE with timestamp filter (approximate - Redis streams use millisecond timestamps)
            session_start_ms = int(session_start.timestamp() * 1000)
            stream_id_start = f"{session_start_ms}-0"
            
            # Get messages from session start (limit to last 1000 to avoid memory issues)
            messages = self.redis_realtime.xrevrange(stream_key, count=1000)
            
            if not messages:
                self.logger.debug("No historical messages found in stream for VWAP recovery")
                return
            
            # Process messages to reconstruct combined premium data
            processed_count = 0
            # ✅ FIX: Suppress warnings during historical data load (initialization)
            original_level = self.logger.level
            self.logger.setLevel(logging.ERROR)  # Only show errors during historical load
            
            try:
                for msg_id, fields in messages:
                    try:
                        # ✅ FIXED: Stream format is DIRECT fields (not nested in 'data')
                        # Format from produce_ticks_batch_sync: {'symbol': '<str>', 'price': '<str>', 'volume': '<str>', ...}
                        # All values are stored as strings in Redis stream
                        tick_data = {}
                        for key, value in fields.items():
                            if isinstance(key, bytes):
                                key = key.decode('utf-8')
                            if isinstance(value, bytes):
                                value_str = value.decode('utf-8')
                            else:
                                value_str = str(value)
                            
                            # Convert string values back to appropriate types
                            if key in ['price', 'last_price', 'high', 'low', 'open', 'close']:
                                try:
                                    tick_data[key] = float(value_str) if value_str else 0.0
                                except (ValueError, TypeError):
                                    tick_data[key] = 0.0
                            elif key in ['volume', 'timestamp', 'exchange_timestamp_ms', 'instrument_token', 
                                         'bucket_incremental_volume', 'bucket_cumulative_volume', 
                                         'zerodha_cumulative_volume', 'zerodha_last_traded_quantity']:
                                try:
                                    tick_data[key] = int(value_str) if value_str else 0
                                except (ValueError, TypeError):
                                    tick_data[key] = 0
                            else:
                                tick_data[key] = value_str
                        
                        # ✅ FIXED: Handle both formats (direct fields OR 'data' field with JSON)
                        # Some streams may have 'data' field (from crawler), others have direct fields (from produce_ticks_batch_sync)
                        if 'data' in tick_data and isinstance(tick_data['data'], str):
                            try:
                                # Parse JSON from 'data' field if it exists
                                tick_data = json.loads(tick_data['data'])
                            except:
                                pass  # Keep direct fields if JSON parse fails
                        
                        # Process tick to update combined premium data
                        if self._is_relevant_tick(tick_data):
                            self._update_market_data(tick_data)
                            processed_count += 1
                            
                    except Exception as e:
                        self.logger.debug(f"Error processing historical message: {e}")
                        continue
            
            finally:
                # ✅ FIX: Restore original log level after historical data load
                self.logger.setLevel(original_level)
            
            if processed_count > 0:
                self.logger.info(f"✅ Loaded {processed_count} historical ticks from stream for VWAP recovery")
            else:
                self.logger.debug("No relevant historical ticks found for VWAP recovery")
                
        except Exception as e:
            # ✅ FIX: Restore log level even on error
            try:
                self.logger.setLevel(original_level)
            except:
                pass
            self.logger.warning(f"Error loading historical premium data from stream: {e}")
    
    def _save_strategy_state_to_redis(self):
        """Save current strategy state to Redis"""
        if not self.redis_system:
            return
        
        try:
            # ✅ FIXED: Use position_states (per underlying) instead of position_state
            # For backward compatibility, save first underlying's state
            if self.config.get('underlying_symbols'):
                underlying = self.config['underlying_symbols'][0]
                position_state = self.position_states.get(underlying, {})
                
                # ✅ FIXED: Handle None values - Redis doesn't accept NoneType
                # ✅ FIXED: Avoid double string conversion (check if already string)
                current_position = position_state.get('current_position')
                if current_position is None:
                    current_position = 'NONE'
                # Only convert to string if not already a string (prevents nested string corruption)
                if not isinstance(current_position, str):
                    current_position = str(current_position)
                self.redis_system.set("strategy:kow:position", current_position)
                
                entry_premium = position_state.get('entry_combined_premium')
                if entry_premium is not None:
                    self.redis_system.set("strategy:kow:entry_premium", str(entry_premium))
                
                if self.current_atm_strike is not None:
                    self.redis_system.set("strategy:kow:current_strike", str(self.current_atm_strike))
        except Exception as e:
            self.logger.warning(f"Error saving strategy state to Redis: {e}")
    
    async def start_tick_listener(self):
        """Start listening to Redis tick streams for real-time data using RobustStreamConsumer"""
        self.logger.info("🔍 [KOW_SIGNAL] start_tick_listener() called")
        
        if not self.redis_realtime:
            self.logger.error("❌ [KOW_SIGNAL] Redis client not available, cannot start tick listener")
            return
        
        self.logger.info("✅ [KOW_SIGNAL] Starting Kow Signal Straddle tick listener...")
        
        # ✅ FIXED: Use RobustStreamConsumer for proper consumer group integration
        import os
        import time as time_module
        from intraday_trading.intraday_scanner.data_pipeline import RobustStreamConsumer
        from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
        
        # Use DatabaseAwareKeyBuilder for stream key
        stream_key = DatabaseAwareKeyBuilder.live_processed_stream()
        group_name = "kow_signal_strategy_group"
        consumer_name = f"kow_signal_{os.getpid()}_{int(time_module.time())}"
        
        # Create robust consumer
        try:
            consumer = RobustStreamConsumer(
                stream_key=stream_key,
                group_name=group_name,
                consumer_name=consumer_name,
                db=1  # DB 1 for realtime streams
            )
            self.logger.info(f"✅ [KOW_SIGNAL] RobustStreamConsumer created successfully (group: {group_name}, consumer: {consumer_name})")
        except Exception as e:
            self.logger.error(f"❌ [KOW_SIGNAL] Failed to create RobustStreamConsumer: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return  # Exit if consumer creation fails
        
        # Track last state save time
        last_state_save = time_module.time()
        state_save_interval = 60  # Save state every 60 seconds
        
        def process_tick_message(message_data):
            """Callback to process tick messages from stream (sync wrapper for async on_tick)"""
            try:
                # Log first message to confirm callback is being called
                if not hasattr(process_tick_message, '_logged_first'):
                    self.logger.info("🔍 [KOW_SIGNAL] process_tick_message callback called for first time")
                    process_tick_message._logged_first = True
                # ✅ FIXED: Parse stream message format (same as data_pipeline.parse_stream_message)
                # Stream messages have bytes keys, decode them
                if not message_data or not isinstance(message_data, dict):
                    return True  # ACK and skip invalid messages
                
                # Decode bytes keys to strings
                parsed = {}
                for key, value in message_data.items():
                    try:
                        if isinstance(key, bytes):
                            key_str = key.decode('utf-8')
                        else:
                            key_str = str(key)
                        
                        if isinstance(value, bytes):
                            # For 'data' field, keep as string for JSON parsing
                            if key_str == 'data':
                                parsed[key_str] = value.decode('utf-8')
                            else:
                                # Try to decode other fields
                                try:
                                    parsed[key_str] = value.decode('utf-8')
                                except:
                                    parsed[key_str] = value
                        else:
                            parsed[key_str] = value
                    except Exception as key_err:
                        self.logger.warning(f"Error processing key in stream message: {key_err}")
                        continue
                
                # ✅ FIXED: Stream format is DIRECT fields (not nested in 'data')
                # Format from produce_ticks_batch_sync: {'symbol': '<str>', 'price': '<str>', 'volume': '<str>', ...}
                # All values are stored as strings in Redis stream
                tick_data = None
                
                # ✅ PRIORITY 1: Check for 'data' field (from crawler format - backward compatibility)
                if 'data' in parsed:
                    data_value = parsed['data']
                    if isinstance(data_value, str):
                        try:
                            tick_data = json.loads(data_value)
                        except json.JSONDecodeError as e:
                            self.logger.debug(f"Failed to parse JSON from data field: {e}")
                            # Fall through to direct fields
                        except Exception as e:
                            self.logger.debug(f"Error parsing data field: {e}")
                            # Fall through to direct fields
                    elif isinstance(data_value, bytes):
                        try:
                            tick_data = json.loads(data_value.decode('utf-8'))
                        except Exception as e:
                            self.logger.debug(f"Failed to parse bytes data field: {e}")
                            # Fall through to direct fields
                
                # ✅ PRIORITY 2: Use direct fields format (from produce_ticks_batch_sync)
                if tick_data is None:
                    # Convert string values back to appropriate types (same as _load_historical_premium_data_from_stream)
                    tick_data = {}
                    for key, value in parsed.items():
                        if key == 'data':
                            continue  # Skip 'data' field if we're using direct fields
                        
                        value_str = str(value) if not isinstance(value, str) else value
                        
                        # Convert string values back to appropriate types
                        if key in ['price', 'last_price', 'high', 'low', 'open', 'close']:
                            try:
                                tick_data[key] = float(value_str) if value_str else 0.0
                            except (ValueError, TypeError):
                                tick_data[key] = 0.0
                        elif key in ['volume', 'timestamp', 'exchange_timestamp_ms', 'instrument_token', 
                                     'bucket_incremental_volume', 'bucket_cumulative_volume', 
                                     'zerodha_cumulative_volume', 'zerodha_last_traded_quantity']:
                            try:
                                tick_data[key] = int(value_str) if value_str else 0
                            except (ValueError, TypeError):
                                tick_data[key] = 0
                        else:
                            tick_data[key] = value_str
                
                if not tick_data:
                    return True  # ACK and skip empty messages
                
                # Run async on_tick in sync context
                try:
                    # Create new event loop for this thread if needed
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_closed():
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                    
                    # Run async function with timeout to prevent blocking
                    try:
                        loop.run_until_complete(asyncio.wait_for(self.on_tick(tick_data), timeout=5.0))
                    except asyncio.TimeoutError:
                        self.logger.warning(f"⚠️ [KOW_SIGNAL] on_tick timed out after 5s, ACKing message to prevent backlog")
                    except Exception as async_err:
                        self.logger.error(f"❌ [KOW_SIGNAL] Error in async on_tick: {async_err}")
                        import traceback
                        self.logger.error(traceback.format_exc())
                except Exception as async_err:
                    self.logger.error(f"❌ [KOW_SIGNAL] Error setting up event loop: {async_err}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                
                return True  # ACK successful (always return True to prevent backlog)
                
            except Exception as e:
                self.logger.error(f"Error processing tick message in Kow Signal strategy: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                return True  # ACK even on error to prevent infinite reprocessing
        
        # ✅ FIXED: Run RobustStreamConsumer in a thread (it's blocking)
        import threading
        
        def run_consumer():
            """Run consumer in separate thread"""
            self.logger.info("🔍 [KOW_SIGNAL] Consumer thread function called")
            try:
                self.logger.info("🔍 [KOW_SIGNAL] Calling consumer.process_messages()...")
                consumer.process_messages(process_tick_message)
            except Exception as e:
                self.logger.error(f"❌ [KOW_SIGNAL] Error in consumer thread: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
        
        self.logger.info("🔍 [KOW_SIGNAL] Creating consumer thread...")
        consumer_thread = threading.Thread(target=run_consumer, daemon=True, name="KowSignalConsumer")
        consumer_thread.start()
        self.logger.info("✅ [KOW_SIGNAL] Kow Signal Straddle consumer thread started")
        
        # Main loop: periodically save state
        while self.running:
            try:
                # ✅ GRACEFUL SHUTDOWN: Check shutdown event periodically instead of blocking sleep
                # Sleep in small increments to allow immediate shutdown response
                sleep_increment = 1.0  # Check every 1 second
                total_slept = 0.0
                while total_slept < state_save_interval and self.running:
                    await asyncio.sleep(sleep_increment)
                    total_slept += sleep_increment
                    # Check if shutdown was requested
                    if self._shutdown_event.is_set() or not self.running:
                        self.logger.info("🛑 [KOW_SIGNAL] Shutdown signal received, stopping tick listener...")
                        break
                
                # Exit loop if shutdown requested
                if not self.running or self._shutdown_event.is_set():
                    break
                
                # Save state periodically (only if still running)
                if self.running:
                    self._save_strategy_state_to_redis()
                    
                    # Check if consumer thread is still alive
                    if not consumer_thread.is_alive():
                        if self.running:  # Only restart if still running
                            self.logger.warning("Consumer thread died, restarting...")
                            consumer_thread = threading.Thread(target=run_consumer, daemon=True, name="KowSignalConsumer")
                            consumer_thread.start()
                        else:
                            self.logger.info("🛑 [KOW_SIGNAL] Shutdown in progress, not restarting consumer thread")
                            break
                    
            except Exception as e:
                if self.running:  # Only log errors if still running
                    self.logger.error(f"Error in tick listener main loop: {e}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                    await asyncio.sleep(1)
                else:
                    # Shutdown in progress, exit loop
                    break
        
        # ✅ GRACEFUL SHUTDOWN: Stop consumer thread
        self.logger.info("🛑 [KOW_SIGNAL] Stopping consumer thread...")
        if hasattr(consumer, 'stop'):
            try:
                consumer.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping consumer: {e}")
        
        # Wait for consumer thread to finish (with timeout)
        if consumer_thread.is_alive():
            consumer_thread.join(timeout=2)
            if consumer_thread.is_alive():
                self.logger.warning("⚠️ [KOW_SIGNAL] Consumer thread did not stop within timeout")
            else:
                self.logger.info("✅ [KOW_SIGNAL] Consumer thread stopped gracefully")
        
        self.logger.info("✅ [KOW_SIGNAL] Tick listener stopped gracefully")
    
    def stop(self):
        """
        ✅ GRACEFUL SHUTDOWN: Stop the tick listener gracefully.
        
        Sets the running flag to False and signals the shutdown event,
        allowing the main loop to exit cleanly.
        """
        self.logger.info("🛑 [KOW_SIGNAL] Stop signal received, initiating graceful shutdown...")
        self.running = False
        self._shutdown_event.set()
        self.logger.info("✅ [KOW_SIGNAL] Shutdown signal sent")
    
    @staticmethod
    async def run_strategy():
        """Main entry point to run the strategy"""
        strategy = KowSignalStraddleStrategy()
        await strategy.start_tick_listener()


if __name__ == "__main__":
    asyncio.run(KowSignalStraddleStrategy.run_strategy())
        
