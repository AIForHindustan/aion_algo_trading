"""
Redis Key Lookup Standards - ENFORCED THROUGHOUT CODEBASE
NOW WITH DUAL CONNECTION SUPPORT: STRING vs BINARY REDIS

CRITICAL RULE: ALL Redis operations MUST use direct key lookups.
Pattern matching (KEYS, SCAN) is FORBIDDEN except in admin/debug scripts.

DATABASE ASSIGNMENT:
- DB0: User configuration, order management, broker credentials, preferences
- DB1: Live ticks, OHLC data, volume data, indicators, session data
- DB2: Analytics, alert validations, performance metrics, pattern history
- Pub/Sub: Alert publishing (channel-based, no keys)

CONNECTION TYPE ASSIGNMENT:
- STRING Redis (decode_responses=True): All processed data, JSON, indicators, prices
- BINARY Redis (decode_responses=False): Raw ticks, pickled objects, classifier state, binary streams

This ensures:
- O(1) performance instead of O(N) blocking scans
- Non-blocking Redis operations
- Predictable performance characteristics
- Clear separation between processed data (string) and raw data (binary)
- No encoding errors when loading pickled objects

✅ UPDATED: All keys now have explicit connection type annotations
✅ CONSOLIDATED: SINGLE source of truth for ALL Redis keys
✅ DUAL CONNECTION: Clear guidance for string vs binary Redis usage
updated: 12/19/2025 by Lokesh
"""

from __future__ import annotations

import logging
from typing import Optional, List, Dict, Any, Union, Final, Tuple
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)

class RedisDatabase(Enum):
    """Redis database assignments for clear separation"""
    DB0_USER_CONFIG = 0      # User data, settings, preferences, etc.
    DB1_LIVE_DATA = 1      # Live ticks, OHLC, volume, indicators, session data
    DB2_ANALYTICS = 2      # Analytics, alert validations, performance metrics

class ConnectionType(Enum):
    """Redis connection type for dual connection setup"""
    STRING = "string"      # Processed data, JSON, indicators (decode_responses=True)
    BINARY = "binary"      # Raw data, pickled objects, state (decode_responses=False)

# ============================================================================
# Database & Connection-Aware Key Builder
# ============================================================================

class DatabaseAwareKeyBuilder:
    """
    Builds Redis keys with explicit database AND connection type assignment.
    Ensures consistent key structure across all databases and connections.
    """
    
    @staticmethod
    def get_database(key: str) -> RedisDatabase:
        """
        Determine which database a key belongs to based on its prefix.
        This ensures operations use the correct Redis connection.
        """
        if not key:
            return RedisDatabase.DB1_LIVE_DATA  # Default to live data
        
        # DB0: User configuration prefixes
        user_config_prefixes = {
            'user:', 'order:', 'trade:', 'position:', 'holding:', 
            'broker:', 'config:', 'preference:', 'credential:',
            'session:', 'token:', 'benchmark:', 'alert_pref:',
            'watchlist:', 'portfolio:', 'account:', 'strategy_config:',
            'instrument_cache:', 'user_instrument:', 'user_alert:',
            'notification:', 'subscription:', 'license:', 'api_key:'
        }
        
        # DB1: Live data prefixes
        live_data_prefixes = {
            'ohlc:', 'session:', 'vol:', 'ind:', 'ticks:', 
            'underlying_price:', 'options:', 'bucket_incremental_volume:',
            'ind:intent:', 'ind:constraint:', 'ind:transition:', 'ind:regime:',
            'edge_state:', 'price:', 'fut:', 'alerts:', 'patterns:'
        }
        
        # DB2: Analytics prefixes  
        analytics_prefixes = {
            'pattern_history:', 'scanner:', 'alert_performance:', 
            'signal_quality:', 'pattern_performance:', 'pattern_metrics:',
            'forward_validation:', 'analysis_cache:', 'validation:',
            'time_window_performance:', 'pattern_window_aggregate:',
            'sharpe_inputs:', 'alert_timeline:', 'final_validation:'
        }
        
         # Check prefixes in order of specificity
        for prefix in user_config_prefixes:
            if key.startswith(prefix):
                return RedisDatabase.DB0_USER_CONFIG
                
        for prefix in live_data_prefixes:
            if key.startswith(prefix):
                return RedisDatabase.DB1_LIVE_DATA
                
        for prefix in analytics_prefixes:
            if key.startswith(prefix):
                return RedisDatabase.DB2_ANALYTICS
                
        # Default to live data for unknown keys
        return RedisDatabase.DB1_LIVE_DATA

    @staticmethod
    def get_connection_type(key: str) -> ConnectionType:
        """
        Determine which connection type (string vs binary) to use for a key.
        
        STRING Redis (processed data):
        - User configuration, order management, broker credentials, preferences
        - All indicators, OHLC, prices, volume, alerts
        - Anything that will be JSON encoded or used as strings
        
        BINARY Redis (raw data):
        - Raw tick streams, binary data
        - Pickled objects (classifiers, state)
        - Anything that needs decode_responses=False
        """
        if not key:
            return ConnectionType.STRING  # Default to string
        
        # BINARY connection keys (raw data, pickled objects)
        binary_prefixes = {
            'ticks:raw:',           # Raw binary tick streams
            'ticks:historical:raw:', # Historical raw ticks
            'ticks:unified:raw:',    # Unified raw stream
            'ticks:raw:binary:',     # Raw binary stream
            'edge_state:',           # Pickled classifier state
            'classifier:',           # Pickled classifier objects
            'model_state:',          # Pickled model state
            'binary:cache:',         # Binary cache
            'pickled:',              # Pickled objects
            'broker:session:',       # Broker session (might contain binary tokens)
            'user:token:',           # User tokens (binary encoded)
            'api_key:',              # API keys (binary encoded for security)
        }
        
        for prefix in binary_prefixes:
            if key.startswith(prefix):
                return ConnectionType.BINARY
        
        # Check for edge_state: which contains pickled objects
        if key.startswith('edge_state:'):
            return ConnectionType.BINARY
        
        # All user config, orders, analytics are STRING connection
        return ConnectionType.STRING


    @staticmethod
    def get_client_info(key: str) -> Tuple[RedisDatabase, ConnectionType]:
        """
        Get both database and connection type for a key.
        Returns: (database, connection_type)
        """
        return (
            DatabaseAwareKeyBuilder.get_database(key),
            DatabaseAwareKeyBuilder.get_connection_type(key)
        )

    # ============================================================================
    # DB0: USER CONFIGURATION KEYS (User data, orders, broker, preferences)
    # ALL THESE USE STRING CONNECTION (JSON data)
    # ============================================================================
    
    @staticmethod
    def user_config(user_id: str, config_type: str) -> str:
        """User configuration - DB0, STRING connection"""
        return f"user:{user_id}:config:{config_type}"
    
    @staticmethod
    def user_preferences(user_id: str) -> str:
        """User preferences - DB0, STRING connection"""
        return f"user:{user_id}:preferences"
    
    @staticmethod
    def user_session(user_id: str, session_id: Optional[str] = None) -> str:
        """User session data - DB0, STRING connection"""
        if session_id:
            return f"user:{user_id}:session:{session_id}"
        return f"user:{user_id}:sessions"
    
    @staticmethod
    def user_credentials(user_id: str) -> str:
        """User credentials - DB0, STRING connection (hashed/encrypted)"""
        return f"user:{user_id}:credentials"
    
    @staticmethod
    def user_tokens(user_id: str) -> str:
        """User authentication tokens - DB0, STRING connection"""
        return f"user:{user_id}:tokens"
    
    @staticmethod
    def order_history(user_id: str, order_id: Optional[str] = None) -> str:
        """Order history - DB0, STRING connection"""
        if order_id:
            return f"order:{user_id}:{order_id}"
        return f"order:{user_id}:history"
    
    @staticmethod
    def trade_history(user_id: str, trade_id: Optional[str] = None) -> str:
        """Trade history - DB0, STRING connection"""
        if trade_id:
            return f"trade:{user_id}:{trade_id}"
        return f"trade:{user_id}:history"
    
    @staticmethod
    def positions(user_id: str, symbol: Optional[str] = None) -> str:
        """User positions - DB0, STRING connection"""
        if symbol:
            return f"position:{user_id}:{symbol}"
        return f"position:{user_id}:all"
    
    @staticmethod
    def holdings(user_id: str) -> str:
        """User holdings - DB0, STRING connection"""
        return f"holding:{user_id}"
    
    @staticmethod
    def broker_connection(broker_name: str, user_id: Optional[str] = None) -> str:
        """Broker connection configuration - DB0, STRING connection"""
        if user_id:
            return f"broker:{broker_name}:{user_id}"
        return f"broker:{broker_name}:config"
    
    @staticmethod
    def broker_session(broker_name: str, user_id: str) -> str:
        """Broker session (might need BINARY for tokens) - DB0, BINARY connection"""
        return f"broker:session:{broker_name}:{user_id}"
    
    @staticmethod
    def broker_tokens(broker_name: str, user_id: str) -> str:
        """Broker tokens - DB0, BINARY connection (secure storage)"""
        return f"broker:token:{broker_name}:{user_id}"
    
    @staticmethod
    def alert_preferences(user_id: str) -> str:
        """User alert preferences - DB0, STRING connection"""
        return f"alert_pref:{user_id}"
    
    @staticmethod
    def watchlist(user_id: str, list_name: Optional[str] = None) -> str:
        """User watchlists - DB0, STRING connection"""
        if list_name:
            return f"watchlist:{user_id}:{list_name}"
        return f"watchlist:{user_id}"
    
    @staticmethod
    def portfolio(user_id: str) -> str:
        """User portfolio - DB0, STRING connection"""
        return f"portfolio:{user_id}"
    
    @staticmethod
    def account_summary(user_id: str) -> str:
        """Account summary - DB0, STRING connection"""
        return f"account:{user_id}:summary"
    
    @staticmethod
    def strategy_config(user_id: str, strategy_name: str) -> str:
        """Strategy configuration - DB0, STRING connection"""
        return f"strategy_config:{user_id}:{strategy_name}"
    
    @staticmethod
    def user_instrument_cache(user_id: str) -> str:
        """User-specific instrument cache - DB0, STRING connection"""
        return f"user_instrument:{user_id}:cache"
    
    @staticmethod
    def user_alerts(user_id: str) -> str:
        """User alerts - DB0, STRING connection"""
        return f"user_alert:{user_id}"
    
    @staticmethod
    def notifications(user_id: str) -> str:
        """User notifications - DB0, STRING connection"""
        return f"notification:{user_id}"
    
    @staticmethod
    def subscription_status(user_id: str) -> str:
        """Subscription status - DB0, STRING connection"""
        return f"subscription:{user_id}:status"
    
    @staticmethod
    def license_info(user_id: str) -> str:
        """License information - DB0, STRING connection"""
        return f"license:{user_id}"
    
    @staticmethod
    def api_keys(user_id: str) -> str:
        """API keys - DB0, BINARY connection (secure storage)"""
        return f"api_key:{user_id}"
    
    @staticmethod
    def benchmark_data(benchmark_type: str) -> str:
        """Benchmark data - DB0, STRING connection"""
        return f"benchmark:{benchmark_type}"
    
    @staticmethod
    def global_config(config_key: str) -> str:
        """Global configuration - DB0, STRING connection"""
        return f"config:global:{config_key}"
    
    @staticmethod
    def system_preferences() -> str:
        """System-wide preferences - DB0, STRING connection"""
        return "preference:system"


    # ============================================================================
    # DB1: LIVE DATA KEYS (Ticks, OHLC, Volume, Indicators)
    # ============================================================================
    
    @staticmethod
    def live_ohlc_latest(symbol: str) -> str:
        """Latest OHLC data - DB1, STRING connection"""
        normalized = RedisKeyStandards.canonical_symbol(symbol)
        return f"ohlc_latest:{normalized}"
    
    @staticmethod
    def live_ohlc_timeseries(symbol: str, interval: str = "1d") -> str:
        """OHLC time series - DB1, STRING connection"""
        normalized = RedisKeyStandards.canonical_symbol(symbol)
        return f"ohlc:{normalized}:{interval}"
    
    @staticmethod
    def live_ohlc_daily(symbol: str) -> str:
        """OHLC daily data - DB1, STRING connection"""
        normalized = RedisKeyStandards.canonical_symbol(symbol)
        return f"ohlc_daily:{normalized}"
    
    @staticmethod
    def live_session(symbol: str, date: str) -> str:
        """Trading session data - DB1, STRING connection"""
        return f"session:{symbol}:{date}"
    
    @staticmethod
    def live_underlying_price(symbol: str) -> str:
        """Underlying price data - DB1, STRING connection"""
        return f"underlying_price:{symbol}"
    
    @staticmethod
    def live_price_realtime(symbol: str) -> str:
        """Real-time price - DB1, STRING connection (price:realtime:{symbol})"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"price:realtime:{canonical_sym}"

    # ============================================================================
    # DB1: TICK STORAGE KEYS (with connection type specification)
    # ============================================================================

    @staticmethod
    def live_ticks_hash(symbol: str) -> str:
        """Ticks hash storage - DB1, STRING connection (ticks:{symbol})"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ticks:{canonical_sym}"
    
    @staticmethod
    def live_ticks_latest(symbol: str) -> str:
        """Latest tick hash storage - DB1, STRING connection (ticks:latest:{symbol})"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ticks:latest:{canonical_sym}"
    
    @staticmethod
    def live_tick_snapshot(symbol: str) -> str:
        """Tick snapshot - DB1, STRING connection (tick:{symbol})"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"tick:{canonical_sym}"

    @staticmethod
    def live_recent_ticks_list(symbol: str) -> str:
        """Recent ticks list - DB1, STRING connection (recent_ticks:{symbol})"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"recent_ticks:{canonical_sym}"
    
    @staticmethod
    def live_ticks_stream(symbol: str) -> str:
        """Ticks stream (processed) - DB1, STRING connection (ticks:stream:{symbol})"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ticks:stream:{canonical_sym}"
    
    @staticmethod
    def live_ticks_raw_stream(symbol: str) -> str:
        """Raw ticks stream (binary) - DB1, BINARY connection (ticks:raw:{symbol})"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ticks:raw:{canonical_sym}"
    
    @staticmethod
    def live_processed_stream() -> str:
        """Global processed tick stream - DB1, STRING connection (ticks:intraday:processed)"""
        return "ticks:intraday:processed"
    
    @staticmethod
    def live_enriched_stream() -> str:
        """Stream for enriched tick data with indicators (scanner input) - DB1, STRING"""
        return "ticks:intraday:enriched"
    
    @staticmethod
    def live_unified_raw_stream() -> str:
        """Unified raw tick stream - DB1, BINARY connection (ticks:unified:raw)"""
        return "ticks:unified:raw"
    
    @staticmethod
    def live_raw_binary_stream() -> str:
        """Raw binary tick stream - DB1, BINARY connection (ticks:raw:binary)"""
        return "ticks:raw:binary"
    
    @staticmethod
    def live_historical_ticks_raw(symbol: str) -> str:
        """Historical raw ticks sorted set - DB1, BINARY connection (ticks:historical:raw:{symbol})"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ticks:historical:raw:{canonical_sym}"

    # ============================================================================
    # DB1: VOLUME KEYS
    # ============================================================================

    @staticmethod 
    def live_volume_state(instrument_token: str) -> str:
        """Volume state - DB1, STRING connection"""
        return f"vol:state:{instrument_token}"

    @staticmethod
    def active_baselines() -> str:
        """
        Active baselines set - DB1, STRING connection.
        Legacy key used by Pattern Detector, ML Backend, and Dashboard.
        Returns "active_baselines" to maintain compatibility.
        """
        return "active_baselines"
    
    @staticmethod
    def live_volume_baseline(symbol: str) -> str:
        """Volume baseline - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"vol:baseline:{canonical_sym}"
    
    @staticmethod
    def live_volume_profile(symbol: str, profile_type: str, date: Optional[str] = None) -> str:
        """Volume profile data - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        if date:
            return f"volume_profile:{profile_type}:{canonical_sym}:{date}"
        return f"volume_profile:{profile_type}:{canonical_sym}"
    
    @staticmethod
    def live_volume_profile_realtime(symbol: str) -> str:
        """Real-time volume profile - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"volume_profile:realtime:{canonical_sym}"
    
    @staticmethod
    def live_volume_profile_poc(symbol: str) -> str:
        """Volume profile POC - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"volume_profile:poc:{canonical_sym}"
    
    @staticmethod
    def live_volume_straddle(underlying: str, date: str) -> str:
        """Straddle volume data - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(underlying)
        return f"vol:straddle:{canonical_sym}:{date}"

    @staticmethod
    def live_volume_session(symbol: str, date: str) -> str:
        """Unified volume session data - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"vol:session:{canonical_sym}:{date}"

    # ============================================================================
    # INDICATOR KEYS (STRING connection - JSON data)
    # ============================================================================

    @staticmethod
    def live_indicator(symbol: str, indicator_name: str, category: Optional[str] = None) -> str:
        """Indicator data - DB1, STRING connection (JSON)"""
        if not category:
            category = RedisKeyStandards.categorize_indicator(indicator_name)
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:{category}:{canonical_sym}:{indicator_name}"
    
    @staticmethod
    def live_greeks(symbol: str, greek_name: Optional[str] = None) -> str:
        """Greeks data - DB1, STRING connection (JSON)"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        if greek_name:
            return f"ind:greeks:{canonical_sym}:{greek_name}"
        return f"ind:greeks:{canonical_sym}:greeks"
    
    @staticmethod
    def live_indicator_hash(symbol: str) -> str:
        """Indicator hash key for batch storage - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:{canonical_sym}"
    
    @staticmethod
    def live_greeks_hash(symbol: str) -> str:
        """Greeks hash key for batch storage - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:greeks:{canonical_sym}"

    @staticmethod
    def legacy_index_data(symbol: str, raw_key: Optional[str] = None) -> str:
        """
        Legacy index data key - DB1, STRING connection.
        Used by Dashboard for raw index values (e.g. index:NSE:NIFTY_50).
        """
        if raw_key:
            return raw_key  # Allow passthrough if already formatted
        return f"index:{symbol}"

    @staticmethod
    def legacy_microstructure(symbol: str) -> str:
        """Legacy microstructure key - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:microstructure:{canonical_sym}"


    @staticmethod
    def legacy_microstructure(symbol: str) -> str:
        """Legacy microstructure key - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:microstructure:{canonical_sym}"

    # ============================================================================
    # USER AND SYSTEM KEYS (DB0)
    # ============================================================================
    
    @staticmethod
    def user_data(username_or_account: str) -> str:
        """User profile data - DB0, STRING"""
        return f"user:{username_or_account}"
    
    @staticmethod
    def account_mapping(account_number_raw: str) -> str:
        """Account number mapping key - DB0, STRING"""
        return f"account_key:{account_number_raw}"
        
    @staticmethod
    def user_positions(account_number: str, strategy_tag: Optional[str] = None) -> str:
        """User positions key - DB0, STRING"""
        base = f"positions:{account_number}"
        if strategy_tag:
            return f"{base}:{strategy_tag}"
        return base

    @staticmethod
    def auto_trade_channel(user_id: str) -> str:
        """Auto-trade pubsub channel - DB0, STRING"""
        return f"auto_trade:{user_id}"

    @staticmethod
    def user_orders(user_id: str) -> str:
        """User orders list (LPUSH/LRANGE) - DB0, STRING"""
        return f"user_orders:{user_id}"

    @staticmethod
    def user_order_detail(user_id: str, order_id: str) -> str:
        """User order details (JSON) - DB0, STRING"""
        return f"user_orders:{user_id}:{order_id}"

    @staticmethod
    def user_order_pattern(user_id: str) -> str:
        """User order pattern (SCAN) - DB0, STRING"""
        return f"user_orders:{user_id}:*"

    @staticmethod
    def order_tracker(user_id: str, order_id: str) -> str:
        """Order execution tracker (JSON) - DB0, STRING"""
        return f"order_tracker:{user_id}:{order_id}"

    @staticmethod
    def user_performance(user_id: str) -> str:
        """User performance metrics (JSON) - DB0, STRING"""
        return f"user_performance:{user_id}"

    @staticmethod
    def user_patterns(user_id: str) -> str:
        """User pattern preferences (JSON) - DB0, STRING"""
        return f"user_patterns:{user_id}"

    @staticmethod
    def user_notifications(user_id: str, notification_type: str = "rejections") -> str:
        """User notifications (List/JSON) - DB0, STRING"""
        return f"user_notifications:{user_id}:{notification_type}"

    # ============================================================================
    # PRICE AND FUTURES KEYS
    # ============================================================================

    @staticmethod
    def live_futures_price(symbol: str) -> str:
        """Futures price cache key - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"fut:price:{canonical_sym}:last"
    
    @staticmethod
    def live_futures_prices_hash() -> str:
        """Futures prices hash key - DB1, STRING connection"""
        return "futures:prices:latest"   

    # ============================================================================
    # BUCKET AND VOLUME BUCKET KEYS
    # ============================================================================

    @staticmethod
    def live_bucket_history(symbol: str, resolution: str = "5min") -> str:
        """Bucket history - DB1, STRING connection"""
        return f"bucket_incremental_volume:history:{resolution}:{symbol}"
    
    @staticmethod
    def live_bucket_daily(symbol: str, date: str) -> str:
        """Daily bucket aggregate - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"vol:bucket:daily:{canonical_sym}:{date}"
    
    @staticmethod
    def live_bucket_timeslot(symbol: str, hour: int, minute: int) -> str:
        """Time slot bucket - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"vol:bucket:{canonical_sym}:{hour:02d}:{minute:02d}"

    # ============================================================================
    # OPTION CHAIN KEYS
    # ============================================================================

    @staticmethod
    def live_option_chain(underlying: str, strike: int, option_type: str, field: str) -> str:
        """Option chain data - DB1, STRING connection"""
        underlying_lower = underlying.lower()
        return f"options:{underlying_lower}:{strike}{option_type}:{field}"
    
    @staticmethod
    def get_option_chain_key(underlying: str) -> str:
        """Snapshot key for full option-chain payloads - DB1, STRING connection"""
        base = RedisKeyStandards.canonical_symbol(underlying) if underlying else ""
        sanitized = base.lower()
        sanitized = sanitized.replace("index", "")
        sanitized = sanitized.replace(":", "_")
        sanitized = sanitized.replace(" ", "_")
        sanitized = sanitized.strip("_")
        return f"options:chain:{sanitized}"
    
    # ============================================================================
    #EDGE INDICATORS: Intent, Constraint, Transition, Regime Layers
    # ALL THESE USE STRING CONNECTION (JSON data)
    # ============================================================================
    
    # ========== INTENT LAYER ==========
    @staticmethod
    def intent_dhdi(symbol: str) -> str:
        """Delta-Hedge Demand Index - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:intent:{canonical}:dhdi"
    
    @staticmethod
    def intent_motive(symbol: str) -> str:
        """Motive classification - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:intent:{canonical}:motive"
    
    @staticmethod
    def intent_hedge_pressure(symbol: str) -> str:
        """Moving average of hedge pressure - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:intent:{canonical}:hedge_pressure_20"
    
    # ========== CONSTRAINT LAYER ==========
    @staticmethod
    def constraint_gamma_stress(symbol: str) -> str:
        """Gamma stress (with charm+vanna) - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:constraint:{canonical}:gamma_stress"
    
    @staticmethod
    def constraint_charm_ratio(symbol: str) -> str:
        """Charm to Gamma ratio - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:constraint:{canonical}:charm_ratio"
    
    @staticmethod
    def constraint_lci(symbol: str) -> str:
        """Liquidity Commitment Index - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:constraint:{canonical}:lci"
    
    @staticmethod
    def constraint_session_mult(symbol: str) -> str:
        """Session multiplier - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:constraint:{canonical}:session_mult"
    
    # ========== TRANSITION LAYER ==========
    @staticmethod
    def transition_probability(symbol: str) -> str:
        """Regime transition probability - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:transition:{canonical}:transition_prob"
    
    @staticmethod
    def transition_time_compression(symbol: str) -> str:
        """Time compression signal - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:transition:{canonical}:tcs_10"
    
    @staticmethod
    def transition_latency_collapse(symbol: str) -> str:
        """Latency collapse detection - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:transition:{canonical}:latency_collapse"
    
    # ========== REGIME LAYER ==========
    @staticmethod
    def regime_current(symbol: str) -> str:
        """Current regime ID - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:regime:{canonical}:current"
    
    @staticmethod
    def regime_confidence(symbol: str) -> str:
        """Regime confidence (0-1) - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:regime:{canonical}:confidence"
    
    @staticmethod
    def regime_should_trade(symbol: str) -> str:
        """Trading gate (1/0) - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:regime:{canonical}:should_trade"
    
    @staticmethod
    def regime_recommended_strategies(symbol: str) -> str:
        """JSON list of recommended strategies - DB1, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:regime:{canonical}:recommended_strategies"

    # ============================================================================
    # EDGE STATE STORAGE (DB1) - BINARY CONNECTION (pickled objects)
    # ============================================================================

    @staticmethod
    def edge_state(symbol: str, state_type: str) -> str:
        """General edge state storage - DB1, BINARY connection (pickled)"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"edge_state:{canonical}:{state_type}"
    
    @staticmethod
    def intent_windows(symbol: str) -> str:
        """Intent layer rolling windows - DB1, BINARY connection (pickled)"""
        return DatabaseAwareKeyBuilder.edge_state(symbol, "intent_windows")
    
    @staticmethod
    def constraint_windows(symbol: str) -> str:
        """Constraint layer rolling windows - DB1, BINARY connection (pickled)"""
        return DatabaseAwareKeyBuilder.edge_state(symbol, "constraint_windows")
    
    @staticmethod
    def transition_windows(symbol: str) -> str:
        """Transition layer rolling windows - DB1, BINARY connection (pickled)"""
        return DatabaseAwareKeyBuilder.edge_state(symbol, "transition_windows")
    
    @staticmethod
    def regime_classifier_state(symbol: str) -> str:
        """River classifier state (pickled) - DB1, BINARY connection"""
        return DatabaseAwareKeyBuilder.edge_state(symbol, "classifier_state")
    
    @staticmethod
    def latency_measurements(symbol: str) -> str:
        """Latency timestamp measurements - DB1, BINARY connection"""
        return DatabaseAwareKeyBuilder.edge_state(symbol, "latency_measurements")
    
    @staticmethod
    def gamma_surface(symbol: str) -> str:
        """Gamma/charm/vanna surface - DB1, BINARY connection"""
        return DatabaseAwareKeyBuilder.edge_state(symbol, "gamma_surface")
    
    # ============================================================================
    # BINARY STATE HELPER METHODS
    # ============================================================================
    
    @staticmethod
    def classifier_state_key(symbol: str) -> str:
        """Classifier state key - DB1, BINARY connection"""
        return f"classifier:{symbol}:state"
    
    @staticmethod
    def pickled_object_key(namespace: str, identifier: str) -> str:
        """Generic pickled object key - DB1, BINARY connection"""
        return f"pickled:{namespace}:{identifier}"
    
    @staticmethod
    def binary_cache_key(cache_type: str, symbol: str) -> str:
        """Binary cache key - DB1, BINARY connection"""
        return f"binary:cache:{cache_type}:{symbol}"

    # ============================================================================
    # DB2: ANALYTICS & VALIDATION KEYS (Patterns, Performance, Alerts)
    # ALL THESE USE STRING CONNECTION
    # ============================================================================
    
    @staticmethod
    def analytics_pattern_history(pattern_type: str, symbol: str, field: str) -> str:
        """Pattern history data - DB2, STRING connection"""
        return f"pattern_history:{pattern_type}:{symbol}:{field}"
    
    @staticmethod
    def analytics_scanner_performance(metric: str, timeframe: str) -> str:
        """Scanner performance - DB2, STRING connection"""
        return f"scanner:performance:{metric}:{timeframe}"
    
    @staticmethod
    def analytics_alert_performance_stats() -> str:
        """Alert performance stats - DB2, STRING connection"""
        return "alert_performance:stats"
    
    @staticmethod
    def analytics_alert_performance_pattern(pattern: str) -> str:
        """Pattern-specific alert performance - DB2, STRING connection"""
        return f"alert_performance:stats:{pattern}"
    
    @staticmethod
    def analytics_signal_quality(symbol: str, pattern_type: str) -> str:
        """Signal quality metrics - DB2, STRING connection"""
        return f"signal_quality:{symbol}:{pattern_type}"
    
    @staticmethod
    def analytics_pattern_performance(symbol: str, pattern_type: str) -> str:
        """Pattern performance - DB2, STRING connection"""
        return f"pattern_performance:{symbol}:{pattern_type}"
    
    @staticmethod
    def analytics_pattern_metrics(symbol: str, pattern_type: str) -> str:
        """Pattern metrics - DB2, STRING connection"""
        return f"pattern_metrics:{symbol}:{pattern_type}"
    
    @staticmethod
    def analytics_validation(alert_id: str) -> str:
        """Alert validation data - DB2, STRING connection"""
        return f"forward_validation:alert:{alert_id}"
    
    @staticmethod
    def analytics_legacy_cache(symbol: str, indicator_name: str) -> str:
        """Legacy analysis cache - DB2, STRING connection (for migration)"""
        return f"analysis_cache:indicators:{symbol}:{indicator_name}"
    
    @staticmethod
    def analytics_final_validation(pattern_type: str, symbol: str) -> str:
        """Final validation results with time-window data - DB2, STRING connection"""
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        return f"final_validation:{pattern_type}:{canonical_symbol}"
    
    @staticmethod
    def analytics_pattern_window_aggregate(pattern_type: str, window_key: str) -> str:
        """Pattern-window aggregate statistics - DB2, STRING connection"""
        return f"pattern_window_aggregate:{pattern_type}:{window_key}"
    
    @staticmethod
    def analytics_time_window_performance(pattern_type: str, symbol: str, window_key: str) -> str:
        """Time-window performance data - DB2, STRING connection"""
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        return f"time_window_performance:{pattern_type}:{canonical_symbol}:{window_key}"
    
    @staticmethod
    def analytics_alert_timeline(alert_id: str) -> str:
        """Alert validation timeline - DB2, STRING connection"""
        return f"alert_timeline:{alert_id}"
    
    @staticmethod
    def analytics_sharpe_inputs(pattern_type: str, window_key: str) -> str:
        """Sharpe ratio calculation inputs - DB2, STRING connection"""
        return f"sharpe_inputs:{pattern_type}:{window_key}"
    
    @staticmethod
    def analytics_alert_storage(alert_id: str) -> str:
        """Alert storage key - DB2, STRING connection"""
        return f"alert:{alert_id}"
    
    @staticmethod
    def analytics_validation_storage(alert_id: str) -> str:
        """Validation storage key - DB2, STRING connection"""
        return f"validation:{alert_id}"
    
    @staticmethod
    def analytics_validation_stream() -> str:
        """Validation results stream - DB2, STRING connection"""
        return "alerts:validation:results"
    
    @staticmethod
    def analytics_validation_recent() -> str:
        """Recent validation results list - DB2, STRING connection"""
        return "validation_results:recent"
    # ============================================================================
    # EDGE PERFORMANCE ANALYTICS (DB2, STRING connection)
    # ============================================================================

    @staticmethod
    def edge_performance(symbol: str, window: str, metric: str) -> str:
        """General edge performance tracking - DB2, STRING connection"""
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return f"time_window_performance:edge:{canonical}:{window}:{metric}"
    
    @staticmethod
    def regime_accuracy(symbol: str, window: str = "1hour") -> str:
        """Regime classification accuracy - DB2, STRING connection"""
        return DatabaseAwareKeyBuilder.edge_performance(symbol, window, "regime_accuracy")
    
    @staticmethod
    def transition_prediction_accuracy(symbol: str, window: str = "5min") -> str:
        """Transition prediction accuracy - DB2, STRING connection"""
        return DatabaseAwareKeyBuilder.edge_performance(symbol, window, "transition_accuracy")
    
    @staticmethod
    def constraint_break_predictions(symbol: str, window: str = "15min") -> str:
        """Constraint break prediction success rate - DB2, STRING connection"""
        return DatabaseAwareKeyBuilder.edge_performance(symbol, window, "constraint_break_success")
    
    @staticmethod
    def liquidity_stress_predictions(symbol: str, window: str = "30min") -> str:
        """Liquidity stress prediction success - DB2, STRING connection"""
        return DatabaseAwareKeyBuilder.edge_performance(symbol, window, "liquidity_stress_success")


    # ============================================================================
    # ALERT STREAMS (DB1, STRING connection)
    # ============================================================================
    
    @staticmethod
    def live_alerts_stream() -> str:
        """Main alert stream - DB1, STRING connection"""
        return "alerts:stream"
    
    @staticmethod
    def live_alerts_telegram() -> str:
        """Telegram alert stream - DB1, STRING connection"""
        return "alerts:telegram"
    
    @staticmethod
    def live_pattern_stream(pattern_type: str, symbol: str) -> str:
        """Pattern-specific stream - DB1, STRING connection"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"patterns:{pattern_type}:{canonical_sym}"
    
    # ============================================================================
    # Pub/Sub Channels (No database - channel-based)
    # ============================================================================
    
    @staticmethod
    def pubsub_alerts_system() -> str:
        """System alerts channel - Pub/Sub"""
        return "alerts:system"
    
    @staticmethod
    def pubsub_validation_results() -> str:
        """Validation results channel - Pub/Sub"""
        return "alerts:validation:results"
    
    @staticmethod
    def pubsub_ohlc_updates(symbol: str) -> str:
        """OHLC updates channel - Pub/Sub"""
        return f"ohlc_updates:{symbol}"

# ============================================================================
# Symbol Normalization & OHLC Key Utilities 
# ============================================================================
COMPRESSED_MONTHS = {
    'D23': 'DEC', 'J26': 'JAN', 'F25': 'FEB', 'M24': 'MAR',
    'A23': 'APR', 'M25': 'MAY', 'J24': 'JUN',
    'J23': 'JUL', 'A26': 'AUG', 'S25': 'SEP',
    'O24': 'OCT', 'N23': 'NOV',
}

def expand_compressed_expiry(expiry: str) -> str:
    """Convert 25D23 → 25DEC, 26J26 → 26JAN, etc."""
    # Handle cases like 25D23 (Year 25, Compressed Month Code D23)
    if len(expiry) >= 4:
        year_prefix = expiry[:2]
        compressed_part = expiry[2:]
        if compressed_part in COMPRESSED_MONTHS:
            return f"{year_prefix}{COMPRESSED_MONTHS[compressed_part]}"
    return expiry

@dataclass
class ParsedSymbol:
    symbol_raw: str
    exchange: str  # NFO, NSE, BSE, MCX, CDS
    instrument_type: str  # OPT, FUT, EQ, INDEX, COMM, CURRENCY
    underlying: str  # NIFTY, BANKNIFTY, RELIANCE, GOLD, USDINR
    expiry: Optional[str] = None  # YYYYMMDD or DDMMMYY
    expiry_date: Optional[datetime] = None
    strike: Optional[float] = None
    option_type: Optional[str] = None  # CE, PE
    lot_size: Optional[int] = None
    tick_size: Optional[float] = None

import json
import logging

logger = logging.getLogger(__name__)

class UniversalSymbolParser:
    """
    Universal parser for all Indian market symbols.
    Loads metadata from instruments.json for accurate parsing.
    """
    _instance: Optional["UniversalSymbolParser"] = None
    _instrument_cache: Optional[Dict] = None
    
    @classmethod
    def get_instance(cls, metadata_path: Optional[Union[str, Path]] = None) -> "UniversalSymbolParser":
        """
        Return singleton instance optionally reloading metadata if a new path is supplied.
        """
        if cls._instance is None:
            cls._instance = cls(metadata_path=metadata_path)
        elif metadata_path:
            cls._instance.reload_metadata(metadata_path)
        return cls._instance
    
    def __init__(self, metadata_path: Optional[Union[str, Path]] = None):
        if getattr(self, "_initialized", False):
            return
        
        self._metadata_override = Path(metadata_path).expanduser() if metadata_path else None
        self._instruments_path = self._metadata_override or self._discover_instruments_path()
        if not self._instruments_path:
            raise FileNotFoundError("Could not find instrument metadata file")
        
        self._metadata_cache: Dict[str, Dict] = {}
        self._token_cache: Dict[int, Dict] = {}
        self._symbol_cache: Dict[str, Dict] = {}
        self._price_cache: Dict[str, float] = {}
        
        self._load_instruments()
        self._initialized = True
    
    def reload_metadata(self, metadata_path: Optional[Union[str, Path]] = None) -> None:
        """Reload metadata from disk when the source file changes."""
        override_path = Path(metadata_path).expanduser() if metadata_path else self._metadata_override
        if override_path:
            self._metadata_override = override_path
        self._instruments_path = self._metadata_override or self._discover_instruments_path()
        if not self._instruments_path:
            raise FileNotFoundError("Could not find instrument metadata file")
        self._metadata_cache.clear()
        self._token_cache.clear()
        self._symbol_cache.clear()
        self._price_cache.clear()
        self._load_instruments()
    
    def _discover_instruments_path(self) -> Optional[Path]:
        """Discover metadata file in common repo-relative locations."""
        resolved = Path(__file__).resolve()
        search_templates = [
            ("zerodha", "crawlers", "binary_crawler1", "intraday_crawler_instruments.json"),
            ("src", "zerodha", "crawlers", "binary_crawler1", "intraday_crawler_instruments.json"),
        ]
        candidates: List[Path] = []
        seen: set = set()
        for ancestor in [resolved.parent] + list(resolved.parents):
            for rel_parts in search_templates:
                candidate = ancestor.joinpath(*rel_parts)
                if candidate in seen:
                    continue
                seen.add(candidate)
                candidates.append(candidate)
        for candidate in candidates:
            if candidate.exists():
                return candidate
        logger.error("❌ Could not locate intraday_crawler_instruments.json using default search paths")
        return None
    
    def _register_token_metadata(self, token_value: Any, metadata: Optional[Dict]) -> bool:
        """Cache metadata for a token after normalizing it."""
        if metadata is None and token_value is None:
            return False
        token_candidate = token_value
        if token_candidate is None and isinstance(metadata, dict):
            token_candidate = metadata.get('token') or metadata.get('instrument_token')
        token = self._coerce_token(token_candidate)
        if token is None:
            return False
        
        metadata = dict(metadata or {})
        metadata['token'] = token
        existing = self._token_cache.get(token, {})
        merged = {**existing, **metadata}
        
        raw_symbol = (
            merged.get('tradingsymbol')
            or merged.get('symbol')
            or merged.get('name')
        )
        if not raw_symbol:
            key = merged.get('key')
            if isinstance(key, str) and ':' in key:
                raw_symbol = key.split(':', 1)[1]
        
        if raw_symbol:
            cleaned_symbol = raw_symbol.strip().upper()
            canonical_symbol = RedisKeyStandards.canonical_symbol(cleaned_symbol)
            merged.setdefault('tradingsymbol', cleaned_symbol)
            merged['canonical_symbol'] = canonical_symbol
            self._symbol_cache[cleaned_symbol] = merged
            self._symbol_cache[canonical_symbol] = merged
        
        self._token_cache[token] = merged
        return True
    
    def _load_instruments(self) -> bool:
        """Load instrument metadata from JSON file"""
        try:
            if not self._instruments_path or not self._instruments_path.exists():
                logger.error(f"Instruments file not found: {self._instruments_path}")
                return False
            
            with self._instruments_path.open('r', encoding='utf-8') as f:
                data = json.load(f)
            
            token_entries = 0
            metadata_section = data.get('metadata', {})
            token_types = metadata_section.get('token_types')
            if isinstance(token_types, dict):
                for token_value, token_meta in token_types.items():
                    if self._register_token_metadata(token_value, token_meta):
                        token_entries += 1
            
            instrument_info = data.get('instrument_info')
            if isinstance(instrument_info, dict):
                for token_value, token_meta in instrument_info.items():
                    if self._register_token_metadata(token_value, token_meta):
                        token_entries += 1
            
            for token_info in data.get('tokens', []):
                if isinstance(token_info, dict):
                    if self._register_token_metadata(
                        token_info.get('token') or token_info.get('instrument_token'),
                        token_info
                    ):
                        token_entries += 1
                elif isinstance(token_info, int):
                    if self._register_token_metadata(token_info, {'token': token_info}):
                        token_entries += 1
            
            hot_metadata_path = self._instruments_path.parent / "hot_token_metadata.json"
            if hot_metadata_path.exists():
                try:
                    with hot_metadata_path.open('r', encoding='utf-8') as hot_file:
                        hot_data = json.load(hot_file)
                    for token_value, token_meta in hot_data.items():
                        if self._register_token_metadata(token_value, token_meta):
                            token_entries += 1
                except Exception as exc:
                    logger.warning("⚠️ Failed to load hot_token_metadata.json: %s", exc)
            
            indices = data.get('indices', {})
            self._metadata_cache['indices'] = indices
            
            for index_name, index_data in indices.items():
                spot_price = index_data.get('spot_price')
                if not spot_price:
                    continue
                try:
                    numeric_price = float(spot_price)
                except (TypeError, ValueError):
                    continue
                underlying = index_name.upper()
                self._price_cache[underlying] = numeric_price
                
                if underlying == 'NIFTY':
                    self._price_cache['NIFTY 50'] = numeric_price
                    self._price_cache['NIFTY50'] = numeric_price
                elif underlying == 'BANKNIFTY':
                    self._price_cache['NIFTY BANK'] = numeric_price
                    self._price_cache['BANK NIFTY'] = numeric_price
                elif underlying == 'SENSEX':
                    self._price_cache['SENSEX 50'] = numeric_price
            
            logger.info(
                "✅ Loaded %d instrument tokens, %d cached underlying prices",
                token_entries,
                len(self._price_cache),
            )
            return True
            
        except FileNotFoundError:
            logger.error(f"Instruments file not found: {self._instruments_path}")
            return False
        except json.JSONDecodeError as exc:
            logger.error(f"Failed to parse instrument metadata: {exc}")
            return False
        except Exception as e:
            logger.error(f"Failed to load instruments: {e}")
            return False
    
    def _coerce_token(self, token: Any) -> Optional[int]:
        """Convert arbitrary token input into an int."""
        if token is None:
            return None
        try:
            return int(token)
        except (TypeError, ValueError):
            logger.debug("Skipping invalid instrument token: %s", token)
            return None
    
    def get_token_metadata(self, token: Union[int, str]) -> Optional[Dict]:
        """Return metadata for a token if available."""
        token_int = self._coerce_token(token)
        if token_int is None:
            return None
        return self._token_cache.get(token_int)
    
    def resolve_token_to_symbol(self, token: Union[int, str], canonical: bool = True) -> Optional[str]:
        """Resolve a token into a canonical symbol."""
        metadata = self.get_token_metadata(token)
        if not metadata:
            return None
        raw_symbol = (
            metadata.get('tradingsymbol')
            or metadata.get('symbol')
            or metadata.get('name')
        )
        if not raw_symbol:
            return None
        if not canonical:
            return raw_symbol.strip().upper()
        return RedisKeyStandards.canonical_symbol(raw_symbol)
    
    def is_known_token(self, token: Union[int, str]) -> bool:
        """Check whether a token exists in the metadata cache."""
        token_int = self._coerce_token(token)
        if token_int is None:
            return False
        return token_int in self._token_cache
    
    def get_all_tokens(self) -> List[int]:
        """Return all known instrument tokens."""
        return list(self._token_cache.keys())
    
    def get_all_symbols(self, canonical: bool = True) -> List[str]:
        """Return unique symbols contained in the metadata."""
        symbols: set = set()
        for metadata in self._token_cache.values():
            raw_symbol = (
                metadata.get('tradingsymbol')
                or metadata.get('symbol')
                or metadata.get('name')
            )
            if not raw_symbol:
                continue
            if canonical:
                symbols.add(RedisKeyStandards.canonical_symbol(raw_symbol))
            else:
                symbols.add(raw_symbol.strip().upper())
        return list(symbols)
    
    def parse(self, symbol: str) -> Optional[ParsedSymbol]:
        """
        Parse any market symbol into structured format.
        Priority: Metadata cache → Regex parsing → Fallback
        """
        if not symbol or not isinstance(symbol, str):
            return None
        
        symbol_upper = symbol.upper().strip()
        symbol_upper = symbol_upper.replace("'", "").replace('"', "")
        
        # Try cache first (fastest)
        candidates = [symbol_upper]
        if ':' in symbol_upper:
            candidates.append(symbol_upper.split(':', 1)[1])

        prefix_candidates = []
        for prefix in ("NFO", "BFO", "NSE", "NSEB", "BSE", "MCX", "CDS"):
            if symbol_upper.startswith(prefix):
                remainder = symbol_upper[len(prefix):]
                remainder = remainder.lstrip(":")
                remainder = remainder.lstrip()
                prefix_candidates.append(remainder)
        candidates.extend(prefix_candidates)

        for candidate in candidates:
            if not candidate:
                continue
            cached = self._symbol_cache.get(candidate)
            if cached:
                return self._parse_from_metadata(cached, symbol)
        
        # Regex parsing (fallback)
        return self._parse_option_symbol(symbol_upper, 'NFO') if symbol_upper.endswith('CE') or symbol_upper.endswith('PE') else self._parse_future_symbol(symbol_upper, 'NFO')
    
    def get_underlying(self, symbol: str) -> Optional[str]:
        """
        Extract the underlying symbol from an option/future symbol.
        
        Examples:
            - NIFTY25DEC26000CE -> NIFTY
            - BANKNIFTY25DECFUT -> BANKNIFTY
            - NIFTY 50 -> NIFTY
            - RELIANCE -> RELIANCE
        
        Args:
            symbol: Trading symbol (option, future, or equity)
            
        Returns:
            Underlying symbol or the original symbol if not parseable
        """
        if not symbol:
            return None
        
        # Handle common index aliases first
        symbol_clean = symbol.upper().strip()
        symbol_clean = symbol_clean.replace("'", "").replace('"', '')
        
        # Direct index mappings
        index_map = {
            'NIFTY 50': 'NIFTY',
            'NIFTY50': 'NIFTY',
            'NIFTY BANK': 'BANKNIFTY',
            'BANK NIFTY': 'BANKNIFTY', 
            'SENSEX 50': 'SENSEX',
            'INDIA VIX': 'INDIAVIX',
        }
        
        if symbol_clean in index_map:
            return index_map[symbol_clean]
        
        # Try to parse the symbol
        try:
            parsed = self.parse(symbol)
            if parsed and parsed.underlying:
                return parsed.underlying
        except Exception:
            pass
        
        # Fallback: return the cleaned symbol
        return symbol_clean

    def _normalize_symbol_key(self, symbol: Optional[str]) -> Optional[str]:
        if not symbol:
            return None
        return symbol.upper().replace("'", "").replace('"', "").strip()

    def update_price(self, symbol: str, price: float) -> None:
        """Store latest price in cache for fast lookup."""
        if symbol is None:
            return
        try:
            numeric_price = float(price)
        except (TypeError, ValueError):
            return
        if numeric_price <= 0:
            return

        underlying = self.get_underlying(symbol) or symbol
        candidates = {
            self._normalize_symbol_key(symbol),
            self._normalize_symbol_key(underlying),
            self._normalize_symbol_key(RedisKeyStandards.canonical_symbol(symbol) if symbol else None),
            self._normalize_symbol_key(RedisKeyStandards.canonical_symbol(underlying) if underlying else None),
        }

        for key in filter(None, candidates):
            self._price_cache[key] = numeric_price

    def get_underlying_price(self, symbol: str, redis_client=None) -> Optional[float]:
        """
        Resolve underlying price via cache and optional Redis fallback.
        """
        if not symbol:
            return None

        underlying = self.get_underlying(symbol) or symbol
        candidates: List[str] = []
        for candidate in filter(None, [symbol, underlying]):
            normalized = self._normalize_symbol_key(candidate)
            canonical = RedisKeyStandards.canonical_symbol(candidate) if candidate else None
            candidates.extend(
                filter(
                    None,
                    [
                        candidate,
                        normalized,
                        self._normalize_symbol_key(canonical),
                    ],
                )
            )

        for key in candidates:
            price = self._price_cache.get(key)
            if price is not None:
                return price

        redis_conn = None
        if redis_client:
            try:
                if hasattr(redis_client, "get_client"):
                    redis_conn = redis_client.get_client(1)
                elif hasattr(redis_client, "redis"):
                    redis_conn = redis_client.redis
                else:
                    redis_conn = redis_client
            except Exception:
                redis_conn = getattr(redis_client, "redis", None)

        if redis_conn:
            redis_keys: List[str] = []
            for key in candidates:
                if not key:
                    continue
                canonical = RedisKeyStandards.canonical_symbol(key) if key else None
                for candidate_key in filter(None, [key, canonical]):
                    redis_keys.append(DatabaseAwareKeyBuilder.live_underlying_price(candidate_key))
                    redis_keys.append(DatabaseAwareKeyBuilder.live_price_realtime(candidate_key))

            for redis_key in redis_keys:
                try:
                    value = redis_conn.get(redis_key)
                except Exception:
                    continue
                if value is None:
                    continue
                try:
                    numeric_price = float(value)
                except (TypeError, ValueError):
                    continue
                self.update_price(underlying, numeric_price)
                return numeric_price

        return None
    
    def _parse_from_metadata(self, metadata: Dict, original_symbol: str) -> ParsedSymbol:
        """Parse from instrument metadata"""
        instrument_type = metadata.get('instrument_type', '').upper()
        
        return ParsedSymbol(
            symbol_raw=original_symbol,
            exchange=metadata.get('exchange', ''),
            instrument_type=instrument_type,
            underlying=metadata.get('underlying', ''),
            expiry=metadata.get('expiry', ''),
            strike=metadata.get('strike', 0.0),
            option_type=metadata.get('option_type', ''),
            lot_size=metadata.get('lot_size'),
            tick_size=metadata.get('tick_size')
        )

    def _parse_option_symbol(self, symbol: str, exchange: str) -> Optional[ParsedSymbol]:
        """Parse option symbol with regex"""
        # NFO format: NFONIFTY25DEC25500CE
        # Remove NFO prefix if present
        if symbol.startswith('NFO'):
            symbol = symbol[3:]
        
        # Regex patterns
        patterns = [
            # 1. Compressed format: NFONIFTY25D2325750PE → NIFTY, 25D23, 25750, PE (Higher Priority)
            r'^(?P<underlying>NIFTY|BANKNIFTY|FINNIFTY|MIDCPNIFTY)(?P<expiry>\d{2}[A-Z]\d{0,2})(?P<strike>\d{4,6})(?P<option_type>CE|PE)$',
            # 2. NIFTY/BANKNIFTY: NIFTY25DEC25500CE
            r'^(?P<underlying>NIFTY|BANKNIFTY|FINNIFTY|MIDCPNIFTY)(?P<expiry>\d{2}[A-Z]{3})(?P<strike>\d{5})(?P<option_type>CE|PE)$',
            # 3. SENSEX: SENSEX25DEC85800CE
            r'^(?P<underlying>SENSEX|BANKEX)(?P<expiry>\d{2}[A-Z]{3})(?P<strike>\d{5})(?P<option_type>CE|PE)$',
            # 4. MCX: GOLDM24JAN62000CE
            r'^(?P<underlying>GOLD|SILVER|COPPER|CRUDEOIL|NATURALGAS)(?P<expiry>[A-Z]+\d{2}[A-Z]{3})(?P<strike>\d+)(?P<option_type>CE|PE)$',
            # 5. CDS: USDINR25DEC8350CE
            r'^(?P<underlying>USDINR|EURINR|GBPINR|JPYINR)(?P<expiry>\d{2}[A-Z]{3})(?P<strike>\d{4})(?P<option_type>CE|PE)$',
        ]
        
        for pattern in patterns:
            match = re.match(pattern, symbol, re.IGNORECASE)
            if match:
                groups = match.groupdict()
                
                # Expand compressed expiry if needed
                expiry = groups['expiry'].upper()
                expiry = expand_compressed_expiry(expiry)
                
                strike = float(groups['strike'])
                if groups['underlying'] in ['USDINR', 'EURINR', 'GBPINR', 'JPYINR']:
                    strike = strike / 100.0  # Currency strikes are in paise
                
                return ParsedSymbol(
                    symbol_raw=symbol,
                    exchange=exchange,
                    instrument_type='OPT',
                    underlying=groups['underlying'].upper(),
                    expiry=expiry,
                    strike=strike,
                    option_type=groups['option_type'].upper()
                )
        
        return None
    
    def _parse_future_symbol(self, symbol: str, exchange: str) -> Optional[ParsedSymbol]:
        """Parse future symbol"""
        # Remove FUT suffix
        if symbol.endswith('FUT'):
            symbol = symbol[:-3]
        
        # Regex patterns
        patterns = [
            # NIFTY25DECFUT
            r'^(?P<underlying>NIFTY|BANKNIFTY|FINNIFTY|MIDCPNIFTY)(?P<expiry>\d{2}[A-Z]{3})$',
            # SENSEX25DECFUT
            r'^(?P<underlying>SENSEX|BANKEX)(?P<expiry>\d{2}[A-Z]{3})$',
            # GOLDM24JANFUT
            r'^(?P<underlying>GOLD|SILVER|COPPER|CRUDEOIL|NATURALGAS)(?P<expiry>[A-Z]+\d{2}[A-Z]{3})$',
            # USDINR25DECFUT
            r'^(?P<underlying>USDINR|EURINR|GBPINR|JPYINR)(?P<expiry>\d{2}[A-Z]{3})$',
        ]
        
        for pattern in patterns:
            match = re.match(pattern, symbol, re.IGNORECASE)
            if match:
                groups = match.groupdict()
                expiry = groups['expiry'].upper()
                expiry = expand_compressed_expiry(expiry)
                return ParsedSymbol(
                    symbol_raw=symbol + 'FUT' if exchange == 'NFO' else symbol,
                    exchange=exchange,
                    instrument_type='FUT',
                    underlying=groups['underlying'].upper(),
                    expiry=expiry,
                )
        return None    

    def _parse_expiry(self, day: str, month: str) -> datetime:
        """
        Parse expiry date from day and month codes.
        
        ✅ FIXED: Now uses centralized MarketCalendar.parse_expiry_from_symbol() for consistency.
        Handles both Zerodha naming formats:
        - Old format: DDMMM (e.g., "25NOV" = November 25, 2025)
        - New format: YYMMM (e.g., "26JAN" = January 2026, expiry last Tuesday of month)
        
        For YYMMM format, calculates the last Tuesday of the month (India F&O expiry as of Sep 2025).
        """
        # ✅ FIXED: Use centralized MarketCalendar instead of duplicating logic
        # Reconstruct a minimal symbol to pass to MarketCalendar
        # Use a dummy base symbol since we only need the expiry part parsed
        dummy_symbol = f"DUMMY{day}{month}FUT"
        
        import logging
        logger.info(f"UniversalSymbolParser: Parsing expiry for {day}{month}")

        try:
            from shared_core.market_calendar import MarketCalendar
            market_calendar = MarketCalendar()
            parsed_expiry = market_calendar.parse_expiry_from_symbol(dummy_symbol)
            if parsed_expiry:
                return parsed_expiry
        except (ValueError, Exception):
            pass
            
        # Fallback to local logic if MarketCalendar fails
        from calendar import monthrange
        from datetime import timedelta
        month_map = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }
        day_num = int(day)
        month_num = month_map[month.upper()] if month else 1
        current_year = datetime.now().year
        # If day is 20-31 and in current/future year range, likely a year (e.g., 24, 25, 26, 27)
        if day_num > 31:
            # Definitely a year (e.g., "26JAN" = 2026)
            year = 2000 + day_num
            # Calculate last Tuesday of the month (India F&O expiry)
            last_day = monthrange(year, month_num)[1]
            last_date = datetime(year, month_num, last_day)
            # Find last Tuesday (weekday 1 = Tuesday)
            weekday = last_date.weekday()
            if weekday == 1:  # Tuesday
                days_back = 0
            elif weekday == 0:  # Monday
                days_back = 6
            else:  # Wednesday-Sunday
                days_back = weekday - 1
            expiry_date = last_date - timedelta(days=days_back)
            return expiry_date
        elif day_num >= 20 and day_num <= 31:
            # Could be either format - check if it's a valid day for the month
            try:
                # Nifty Weekkly expiry every Tuesday
                # Banknifty Monthly expiry every last Friday of the month
                # Sensex Weekly expiry every Thursday
                test_date = datetime(current_year, month_num, day_num)
                if day_num in [24, 25, 26, 27, 28, 29, 30, 31] and current_year >= 2025:
                    # Likely a year indicator (e.g., "26JAN" = 2026)
                    year = 2000 + day_num
                    # Calculate last Tuesday of the month (India F&O expiry)
                    last_day = monthrange(year, month_num)[1]
                    last_date = datetime(year, month_num, last_day)
                    weekday = last_date.weekday()
                    if weekday == 1:  # Tuesday
                        days_back = 0
                    elif weekday == 0:  # Monday
                        days_back = 6
                    else:  # Wednesday-Sunday
                        days_back = weekday - 1
                    expiry_date = last_date - timedelta(days=days_back)
                    return expiry_date
                else:
                    # Valid day, use as-is
                    return test_date
            except ValueError:
                # Invalid day for this month, must be a year
                year = 2000 + day_num
                last_day = monthrange(year, month_num)[1]
                last_date = datetime(year, month_num, last_day)
                weekday = last_date.weekday()
                if weekday == 1:  # Tuesday
                    days_back = 0
                elif weekday == 0:  # Monday
                    days_back = 6
                else:  # Wednesday-Sunday
                    days_back = weekday - 1
                expiry_date = last_date - timedelta(days=days_back)
                return expiry_date
        else:
            # Day is 1-19, definitely a day (old format: DDMMM)
            return datetime(current_year, month_num, day_num)

    def _parse_weekly_expiry(self, year_code: str, month_letter: str, day_code: str) -> datetime:
        """
        Parse weekly option expiry with format YY + <month letter> + DD.
        Month letters follow NSE convention (A=Jan ... L=Dec).
        """
        month_letter = month_letter.upper()
        month_map = {
            'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6,
            'G': 7, 'H': 8, 'I': 9, 'J': 10, 'K': 11, 'L': 12
        }
        if month_letter not in month_map:
            raise ValueError(f"Invalid weekly month code: {month_letter}")
        year = 2000 + int(year_code)
        month = month_map[month_letter]
        day = int(day_code)
        return datetime(year, month, day)

    def _clean_symbol(self, symbol: str) -> str:
        """Clean symbol string"""
        return symbol.strip().upper().replace(' ', '')

# Global instances
_parser_instance: Optional[UniversalSymbolParser] = None
_key_builder_instance: Optional[DatabaseAwareKeyBuilder] = None

def get_symbol_parser(metadata_path: Optional[Union[str, Path]] = None) -> UniversalSymbolParser:
    """Get singleton instance of UniversalSymbolParser."""
    global _parser_instance
    parser = UniversalSymbolParser.get_instance(metadata_path=metadata_path)
    _parser_instance = parser
    return parser

class SymbolResolver:
    """Shared resolver that exposes token → canonical symbol lookups."""
    
    def __init__(self, metadata_path: Optional[Union[str, Path]] = None):
        self._metadata_path = Path(metadata_path).expanduser() if metadata_path else None
        self._parser = get_symbol_parser(metadata_path=metadata_path)
    
    def reload(self, metadata_path: Optional[Union[str, Path]] = None) -> None:
        """Reload metadata via the shared parser."""
        if metadata_path:
            self._metadata_path = Path(metadata_path).expanduser()
        self._parser = get_symbol_parser(metadata_path=self._metadata_path)
    
    def resolve(self, token: Union[int, str]) -> Optional[str]:
        """Resolve token to canonical symbol."""
        return self._parser.resolve_token_to_symbol(token)
    
    def get_metadata(self, token: Union[int, str]) -> Optional[Dict]:
        """Return raw metadata for a token."""
        return self._parser.get_token_metadata(token)
    
    def is_known_token(self, token: Union[int, str]) -> bool:
        """Check if token exists in metadata."""
        return self._parser.is_known_token(token)
    
    def get_all_tokens(self) -> List[int]:
        """Return all known instrument tokens."""
        return self._parser.get_all_tokens()
    
    def get_all_symbols(self, canonical: bool = True) -> List[str]:
        """Return all known symbols."""
        return self._parser.get_all_symbols(canonical=canonical)

_symbol_resolver_instance: Optional[SymbolResolver] = None

def get_symbol_resolver(metadata_path: Optional[Union[str, Path]] = None) -> SymbolResolver:
    """Return global SymbolResolver instance."""
    global _symbol_resolver_instance
    if _symbol_resolver_instance is None:
        _symbol_resolver_instance = SymbolResolver(metadata_path=metadata_path)
    elif metadata_path:
        _symbol_resolver_instance.reload(metadata_path=metadata_path)
    return _symbol_resolver_instance

def get_key_builder() -> DatabaseAwareKeyBuilder:
    """Get singleton instance of DatabaseAwareKeyBuilder."""
    global _key_builder_instance
    if _key_builder_instance is None:
        _key_builder_instance = DatabaseAwareKeyBuilder()
    return _key_builder_instance

def is_fno_instrument(symbol: str) -> bool:
    """Check if symbol is a Futures/Options instrument."""
    if not symbol:
        return False
    
    try:
        parser = UniversalSymbolParser.get_instance()
        parsed = parser.parse(symbol)
        return parsed and parsed.instrument_type in ('FUT', 'OPT') if parsed else False
    except (ValueError, Exception):
        sym_upper = symbol.upper()
        if sym_upper.endswith('FUT'):
            return True
        if sym_upper.endswith(('CE', 'PE')):
            if sym_upper.startswith('NFO'):
                return True
            if re.search(r'\d+(NOV|DEC|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT)(CE|PE)$', sym_upper):
                return True
            if re.search(r'\d{4,}(CE|PE)$', sym_upper):
                return True
            return False
        if re.search(r'\d{2}(NOV|DEC|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT)', sym_upper):
            return True
        return False

def _infer_exchange_prefix(symbol: str) -> str:
    """Infer exchange prefix for symbols without explicit exchange metadata."""
    if not symbol:
        return ""
    
    sym_upper = symbol.upper()
    if ':' in sym_upper:
        return sym_upper.split(':', 1)[0]
    
    try:
        parser = get_symbol_parser().get_instance()
        parsed = parser.parse(symbol)
        # ✅ FIXED: Prioritize instrument type (FUT/OPT) over asset_class
        # If it's a future or option, it's NFO regardless of asset_class
        # UNLESS it is explicitly a BSE/SENSEX instrument
        if 'SENSEX' in sym_upper or 'BANKEX' in sym_upper or sym_upper.startswith('BSE'):
            return 'BFO'
            
        if parsed and parsed.instrument_type in ('FUT', 'OPT'):
            return 'NFO'
        elif parsed and parsed.underlying == 'USDINR':
            return 'CDS'
        elif parsed and parsed.underlying == 'GOLD':
            return 'MCX'
        else:
            return 'NSE'
    except (ValueError, Exception):
        return 'NSE'



class RedisKeyStandards:
    """
    Standardized Redis key lookup utilities with database AND connection awareness.
    
    ✅ UPDATED: All keys now explicitly assigned to DB1 or DB2 and STRING or BINARY
    ✅ ENFORCED: Clear separation between live data and analytics
    ✅ DUAL CONNECTION: Proper connection type for each key type
    """
    
    @staticmethod
    def get_redis_client(key: str, redis_clients: Dict[RedisDatabase, Any]) -> Any:
        """
        Get the correct Redis client based on key prefix.
        
        Args:
            key: Redis key to determine database for
            redis_clients: Dict mapping RedisDatabase to client instances
            
        Returns:
            Appropriate Redis client for the key's database
        """
        # ✅ FIXED: Directly use DatabaseAwareKeyBuilder to avoid get_key_builder() ambiguity
        db = DatabaseAwareKeyBuilder.get_database(key)
        return redis_clients.get(db)
    
    @staticmethod
    def get_dual_redis_client(key: str, string_clients: Dict[RedisDatabase, Any], 
                             binary_clients: Dict[RedisDatabase, Any]) -> Any:
        """
        Get the correct Redis client (string or binary) based on key.
        
        Args:
            key: Redis key
            string_clients: Dict mapping RedisDatabase to string clients
            binary_clients: Dict mapping RedisDatabase to binary clients
            
        Returns:
            Appropriate Redis client (string or binary) for the key
        """
        db = DatabaseAwareKeyBuilder.get_database(key)
        conn_type = DatabaseAwareKeyBuilder.get_connection_type(key)
        
        if conn_type == ConnectionType.STRING:
            return string_clients.get(db)
        else:  # ConnectionType.BINARY
            return binary_clients.get(db)
    
    @staticmethod
    def resolve_instrument_token(token: Union[int, str]) -> Optional[str]:
        """Resolve token to canonical symbol using the shared resolver."""
        resolver = get_symbol_resolver()
        return resolver.resolve(token)
    
    @staticmethod
    def canonical_symbol(symbol: str) -> str:
        """
        Canonical Redis symbol format (matches OHLC/indicator storage).
        
        ✅ FIXED: Removed incorrect expiry date transformations (25DEC→30DEC, 26JAN→27JAN).
        These were causing wrong expiry dates for different underlyings:
        - BANKNIFTY 25DEC = Dec 25 (correct, should NOT be converted)
        - NIFTY 25DEC monthly = Dec 30 (different expiry day)
        
        The metadata JSON file (intraday_crawler_instruments.json) is the source of truth for expiry dates.
        Symbol parsing should NOT modify expiry codes - use metadata instead.
        """
        if not symbol:
            return ""
            
        # ✅ GUARD: Consistent robust cleaning (User Request)
        # 1. Handle bytes if passed accidentally (though type hint says str)
        if isinstance(symbol, bytes):
            symbol = symbol.decode('utf-8', errors='ignore')
            
        # 2. String conversion and basic strip
        symbol = str(symbol).upper().strip()
        
        # 3. Remove known corruption artifacts FIRST
        # These come from double-stringification of bytes: "b'SYMBOL'" -> "SYMBOL"
        # And "NSEB'..." -> "NSE..."
        # ✅ FIXED: Don't break NSEBANKNIFTY (which contains NSEB)
        if "NSEb'" in symbol: symbol = symbol.replace("NSEb'", "NSE")
        if "BFOb'" in symbol: symbol = symbol.replace("BFOb'", "BFO")
        if "NSEB'" in symbol: symbol = symbol.replace("NSEB'", "NSE") # Only replace if followed by quote
        
        # Guard against NSEB at start ONLY if not followed by ANK (BANKNIFTY case)
        if symbol.startswith("NSEB") and not symbol.startswith("NSEBANK"): 
             symbol = symbol.replace("NSEB", "NFO", 1)
            
        # 4. Enforce strict alphanumeric (plus standard separator removal)
        # This removes colons (:), quotes (',"), spaces, and other accumulation
        clean = re.sub(r"[:'\"]", "", symbol)
        clean = re.sub(r"[^A-Z0-9]", "", clean)
        
        if not clean:
            return ""
            
        base_symbol = clean
        
        known_prefixes = ('NSE', 'BSE', 'NFO', 'BFO', 'MCX', 'CDS')
        for prefix in known_prefixes:
            if base_symbol.startswith(prefix):
                remaining = base_symbol[len(prefix):]
                if len(remaining) >= 6 and remaining[:6] in ['USDINR', 'EURINR', 'GBPINR', 'JPYINR']:
                    base_symbol = remaining
                    break
                elif len(remaining) > 0 and remaining[0].isalpha():
                    base_symbol = remaining
                    break
        
        # Parse and add correct prefix
        try:
            parser = get_symbol_parser()
            parsed = parser.parse(base_symbol)
            normalized = parsed.symbol_raw if parsed else base_symbol
            # ❌ REMOVED: Expiry transformations on normalized symbol
            # ✅ FIXED: Check if normalized already has a known prefix BEFORE adding one
            if any(normalized.startswith(pref) for pref in known_prefixes):
                return normalized
            # ✅ FIXED: Also check if base_symbol already has a known prefix
            # This prevents double prefix like BFOBFOSENSEX or NFONFONIFTY
            for pref in known_prefixes:
                if base_symbol.startswith(pref):
                    # base_symbol already had prefix, normalized might have stripped it
                    # Return with original prefix
                    return f"{pref}{normalized}" if not normalized.startswith(pref) else normalized
            exchange = _infer_exchange_prefix(base_symbol)
            return f"{exchange}{normalized}"
        except (ValueError, Exception):
            for pref in known_prefixes:
                if base_symbol.startswith(pref):
                    return base_symbol
            prefix = _infer_exchange_prefix(base_symbol)
            if prefix:
                # ✅ FIXED: Don't add prefix if already present
                if any(base_symbol.startswith(p) for p in known_prefixes):
                    return base_symbol
                return f"{prefix}{base_symbol}"
            return base_symbol

    @staticmethod
    def get_indicator_symbol_variants(symbol: str) -> List[str]:
        """Return prioritized symbol variants for indicator storage/retrieval."""
        if not symbol:
            return []
        
        variants: List[str] = []
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        variants.append(canonical_sym)
        
        try:
            parser = get_symbol_parser()
            parsed = parser.parse(symbol)
            candidates = [
                parsed.symbol_raw if parsed else None,
            ]
            
            for candidate in candidates:
                if candidate and candidate not in variants:
                    variants.append(candidate)
            
        except (ValueError, Exception):
            base_symbol = (symbol or "").split(':', 1)[-1].upper()
            original_base = base_symbol
            for pref in ('NSE', 'BSE', 'NFO', 'BFO'):
                if base_symbol.startswith(pref):
                    stripped = base_symbol[len(pref):]
                    base_symbol = stripped.lstrip(':')
                    break
            if not base_symbol:
                base_symbol = original_base
            
            inferred_prefix = _infer_exchange_prefix(base_symbol)
            candidates = [
                RedisKeyStandards.canonical_symbol(symbol),
                RedisKeyStandards.canonical_symbol(base_symbol),
                base_symbol,
            ]
            if '-' in base_symbol:
                candidates.append(base_symbol.replace('-', '_'))
            if '_' in base_symbol:
                candidates.append(base_symbol.replace('_', '-'))
            if inferred_prefix:
                candidates.extend([
                    f"{inferred_prefix}:{base_symbol}",
                    f"{inferred_prefix}:{RedisKeyStandards.canonical_symbol(base_symbol)}",
                    f"{inferred_prefix}{base_symbol}",
                    f"{inferred_prefix}{RedisKeyStandards.canonical_symbol(base_symbol)}",
                ])
            for candidate in candidates:
                if candidate and candidate not in variants:
                    variants.append(candidate)
        
        return variants

    @staticmethod
    def categorize_indicator(indicator: str) -> str:
        """Categorize indicator for unified key structure."""
        indicator_lower = indicator.lower()
        if indicator_lower in ['rsi', 'macd', 'bollinger', 'ema', 'sma', 'vwap', 'atr'] or \
           indicator_lower.startswith('ema_') or indicator_lower.startswith('sma_') or \
           indicator_lower.startswith('bollinger_'):
            return 'ta'
        elif indicator_lower in ['delta', 'gamma', 'theta', 'vega', 'rho', 
                                  # Advanced Greeks (higher-order)
                                  'vanna', 'charm', 'vomma', 'speed', 'zomma', 'color',
                                  # Option metadata
                                  'dte_years', 'trading_dte', 'expiry_series', 'option_price',
                                  # Implied Volatility
                                  'iv', 'implied_volatility', 'gamma_exposure']:
            # ✅ FIX: All Greek-related fields (including advanced Greeks, DTE, IV, and gamma_exposure) should be categorized as 'greeks'
            return 'greeks'
        elif indicator_lower in [
            # ✅ CRITICAL: Check microstructure indicators BEFORE volume check
            # This ensures cumulative_volume_delta is categorized as 'custom', not 'volume'
            'order_flow_imbalance', 'order_book_imbalance', 'microprice', 
            'cumulative_volume_delta', 'spread_absolute', 'spread_bps',
            'depth_imbalance', 'best_bid_size', 'best_ask_size',
            'total_bid_depth', 'total_ask_depth', 'volatility_regime'
        ]:
            # ✅ NEW: Microstructure indicators categorized as 'custom'
            # All stored as ind:custom:{symbol}:{indicator_name}
            return 'custom'
        elif indicator_lower in ['usdinr_sensitivity', 'usdinr_correlation', 'theoretical_price', 
                                  'usdinr_theoretical_price', 'usdinr_basis']:
            # ✅ NEW: Cross-asset indicators categorized as 'custom'
            return 'custom'
        elif indicator_lower in ['buy_pressure', 'vix_level']:
            # ✅ NEW: Market regime/order flow indicators categorized as 'custom'
            return 'custom'
        # ✅ Edge Indicators (Intent, Constraint, Transition, Regime)
        elif indicator_lower.startswith('dhdi') or indicator_lower.startswith('motive') or \
             indicator_lower.startswith('hedge_pressure'):
            return 'intent'
        elif indicator_lower in ['gamma_stress', 'charm_ratio', 'lci', 'session_mult']:
            return 'constraint'
        elif indicator_lower in ['transition_prob', 'latency_collapse'] or \
             indicator_lower.startswith('tcs_'):
            return 'transition'
        elif indicator_lower.startswith('regime_') or indicator_lower == 'should_trade':
            return 'regime'
        elif 'volume' in indicator_lower:
            # ✅ FIXED: Volume check moved AFTER microstructure check
            # This prevents cumulative_volume_delta from being mis-categorized
            return 'volume'
        else:
            return 'custom'
    
    @staticmethod
    def get_indicator_key(symbol: str, indicator: str, category: Optional[str] = None, use_cache: bool = True) -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        This method is maintained for backward compatibility with existing code.
        **New code should NOT use this method.**
        
        **For new code, use:**
        ```python
        from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
        category = RedisKeyStandards._categorize_indicator(indicator)
        key = DatabaseAwareKeyBuilder.live_indicator(symbol, indicator, category)
        ```
        
        Args:
            symbol: Trading symbol
            indicator: Indicator name (e.g., 'rsi', 'atr', 'gamma')
            category: Optional category override (auto-detected if not provided)
            use_cache: Ignored (kept for compatibility)
            
        Returns:
            Canonical Redis key for indicator (DB1)
        """
        # ✅ FIXED: Directly use DatabaseAwareKeyBuilder to avoid get_key_builder() ambiguity
        if not category:
            category = RedisKeyStandards.categorize_indicator(indicator)
        return DatabaseAwareKeyBuilder.live_indicator(symbol, indicator, category)
    
    @staticmethod
    def get_session_key(symbol: str, date: Optional[str] = None) -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        For new code, use: builder.live_session(symbol, date)
        """
        # ✅ FIXED: Directly use DatabaseAwareKeyBuilder to avoid get_key_builder() ambiguity
        if not date:
            from datetime import datetime
            date = datetime.now().strftime('%Y-%m-%d')
        return DatabaseAwareKeyBuilder.live_session(symbol, date)
    
    @staticmethod
    def get_ohlc_key(symbol: str, interval: str = "latest") -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        For new code, use:
        - builder.live_ohlc_latest(symbol) for latest OHLC
        - builder.live_ohlc_daily(symbol) for daily OHLC
        - builder.live_ohlc_timeseries(symbol, interval) for timeseries
        """
        # ✅ FIXED: Directly use DatabaseAwareKeyBuilder to avoid get_key_builder() ambiguity
        if interval == "latest":
            return DatabaseAwareKeyBuilder.live_ohlc_latest(symbol)
        elif interval == "daily":
            return DatabaseAwareKeyBuilder.live_ohlc_daily(symbol)
        else:
            return DatabaseAwareKeyBuilder.live_ohlc_timeseries(symbol, interval)
    
    @staticmethod
    def get_alert_performance_stats_key() -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        For new code, use: builder.analytics_alert_performance_stats()
        """
        # ✅ FIXED: Directly use DatabaseAwareKeyBuilder to avoid get_key_builder() ambiguity
        return DatabaseAwareKeyBuilder.analytics_alert_performance_stats()
    
    @staticmethod
    def get_alert_performance_pattern_key(pattern: str) -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        For new code, use: builder.analytics_alert_performance_pattern(pattern)
        """
        # ✅ FIXED: Directly use DatabaseAwareKeyBuilder to avoid get_key_builder() ambiguity
        return DatabaseAwareKeyBuilder.analytics_alert_performance_pattern(pattern)
    
    @staticmethod
    def get_alert_performance_patterns_set_key() -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        Returns the key for the set of all patterns with performance data.
        For new code, construct directly: "alert_performance:patterns"
        """
        return "alert_performance:patterns"
    
    @staticmethod
    def get_signal_quality_key(symbol: str, pattern_type: str) -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        For new code, use: builder.analytics_signal_quality(symbol, pattern_type)
        """
        # ✅ FIXED: Directly use DatabaseAwareKeyBuilder to avoid get_key_builder() ambiguity
        return DatabaseAwareKeyBuilder.analytics_signal_quality(symbol, pattern_type)
    
    @staticmethod
    def get_pattern_performance_key(symbol: str, pattern_type: str) -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        For new code, use: builder.analytics_pattern_performance(symbol, pattern_type)
        """
        # ✅ FIXED: Directly use DatabaseAwareKeyBuilder to avoid get_key_builder() ambiguity
        return DatabaseAwareKeyBuilder.analytics_pattern_performance(symbol, pattern_type)

# ============================================================================
# CONNECTION TYPE UTILITIES
# ============================================================================

def get_connection_type_for_key(key: str) -> ConnectionType:
    """
    Helper function to get connection type for a key.
    """
    return DatabaseAwareKeyBuilder.get_connection_type(key)

def should_use_binary_redis(key: str) -> bool:
    """
    Quick check if key should use binary Redis.
    """
    return DatabaseAwareKeyBuilder.get_connection_type(key) == ConnectionType.BINARY

def should_use_string_redis(key: str) -> bool:
    """
    Quick check if key should use string Redis.
    """
    return DatabaseAwareKeyBuilder.get_connection_type(key) == ConnectionType.STRING

# ============================================================================
# Debug Utilities
# ============================================================================

def debug_symbol_parsing(symbol: str):
    """Debug function to see what's happening with symbol parsing"""
    print(f"🔍 [SYMBOL_DEBUG] Parsing: {symbol}")
    
    parser = get_symbol_parser()
    try:
        parsed = parser.parse(symbol) if parser else None
        if parsed:
            print(f"   Parsed: {parsed}")
            print(f"   Symbol Raw: {parsed.symbol_raw}")
            print(f"   Exchange: {parsed.exchange}")
            print(f"   Instrument Type: {parsed.instrument_type}")
            print(f"   Underlying: {parsed.underlying}")
            print(f"   Expiry: {parsed.expiry}")
            print(f"   Expiry Date: {parsed.expiry_date}")
            print(f"   Strike: {parsed.strike}")
            print(f"   Option Type: {parsed.option_type}")
            print(f"   Lot Size: {parsed.lot_size}")
            print(f"   Tick Size: {parsed.tick_size}")
    except Exception as e:
        print(f"   Parse error: {e}")
    
    # Test canonical symbol
    canonical = RedisKeyStandards.canonical_symbol(symbol)
    print(f"   Canonical: {canonical}")
    
    # Test variants
    variants = RedisKeyStandards.get_indicator_symbol_variants(symbol)
    print(f"   Variants: {variants}")

# ============================================================================
# Usage Examples with Database AND Connection Awareness
# ============================================================================

EXAMPLES = {
    "DUAL CONNECTION USAGE": """
    # ✅ CORRECT - Using dual connections with key standards
    
    from shared_core.redis_clients.redis_key_standards import (
        DatabaseAwareKeyBuilder, 
        RedisKeyStandards,
        ConnectionType,
        RedisDatabase,
        get_connection_type_for_key
    )
    
    # Example 1: Store processed tick (STRING Redis)
    from redis_config import get_string_redis, get_binary_redis
    
    symbol = "NIFTY25DEC26300CE"
    builder = DatabaseAwareKeyBuilder()
    
    # Store indicator (STRING Redis)
    indicator_key = builder.live_indicator(symbol, "rsi")
    string_redis = get_string_redis("market_data")
    string_redis.hset(indicator_key, "value", json.dumps(65.5))
    
    # Store raw tick (BINARY Redis)
    raw_tick_key = builder.live_ticks_raw_stream(symbol)
    binary_redis = get_binary_redis("market_data")
    binary_tick_data = b'\\x01\\x02\\x03...'  # Raw binary
    binary_redis.set(raw_tick_key, binary_tick_data)
    
    # Example 2: Store classifier state (BINARY Redis)
    classifier_state = {"model": "trained", "accuracy": 0.95}
    import pickle
    pickled_state = pickle.dumps(classifier_state)
    
    state_key = builder.regime_classifier_state(symbol)
    binary_redis.set(state_key, pickled_state)  # No encoding errors!
    
    # Example 3: Load with correct connection type
    def load_with_correct_connection(key: str):
        if get_connection_type_for_key(key) == ConnectionType.BINARY:
            client = get_binary_redis("market_data")
            data = client.get(key)
            return pickle.loads(data) if data else None
        else:
            client = get_string_redis("market_data")  
            data = client.get(key)
            return json.loads(data) if data else None
    
    # Example 4: Automatic client selection
    def get_correct_client(key: str):
        # Using RedisKeyStandards helper
        string_clients = {
            RedisDatabase.DB1_LIVE_DATA: get_string_redis("market_data"),
            RedisDatabase.DB2_ANALYTICS: get_string_redis("analytics_validation")
        }
        binary_clients = {
            RedisDatabase.DB1_LIVE_DATA: get_binary_redis("market_data"),
            RedisDatabase.DB2_ANALYTICS: get_binary_redis("analytics_validation")
        }
        
        return RedisKeyStandards.get_dual_redis_client(
            key, string_clients, binary_clients
        )
    
    # Get client for any key
    client = get_correct_client(indicator_key)  # Returns string client
    client = get_correct_client(state_key)      # Returns binary client
    """,
    
    "KEY TYPE SUMMARY": """
    # 📋 KEY TYPE SUMMARY
    
    # STRING Redis Keys (decode_responses=True):
    # - All indicators: ind:*
    # - All OHLC data: ohlc:*
    # - Volume data: vol:*
    # - Prices: price:*, fut:*, futures:*
    # - Options data: options:*
    # - Analytics: pattern_*, scanner:*, alert_performance:*
    # - Edge indicators: ind:intent:*, ind:constraint:*, etc.
    
    # BINARY Redis Keys (decode_responses=False):
    # - Raw tick streams: ticks:raw:*
    # - Historical raw ticks: ticks:historical:raw:*
    # - Edge state (pickled): edge_state:*
    # - Classifier state: classifier:*
    # - Pickled objects: pickled:*
    # - Binary cache: binary:cache:*
    
    # Pub/Sub Channels (no database):
    # - Alert channels: alerts:*
    # - Validation channels: alerts:validation:*
    # - OHLC updates: ohlc_updates:*
    """
}
