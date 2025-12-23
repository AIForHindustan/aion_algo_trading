"""
Data Pipeline Module
Handles Redis subscription, tick ingestion, and preprocessing
Receives data from crawlers via Redis and forwards to pattern engine
"""

import json
import time
import threading
import logging
from logging.handlers import RotatingFileHandler
import redis
import queue
from collections import deque
from datetime import datetime
from pathlib import Path
import os
import sys
import atexit
import signal
from typing import Any, Dict, List, Optional, TYPE_CHECKING
import asyncio
from shared_core.redis_clients.redis_client import get_redis_client
from shared_core.redis_clients.consumer_group_manager import ConsumerGroupManager
# ✅ NOTE: import redis is kept for redis.ConnectionError exception handling only

STREAM_CONSUMER_IDLE_THRESHOLD_MS = int(os.environ.get("DATA_PIPELINE_CONSUMER_IDLE_MS", "300000"))
STREAM_CONSUMER_CLEANUP_INTERVAL = int(os.environ.get("DATA_PIPELINE_CONSUMER_CLEANUP_INTERVAL", "300"))
STREAM_NAME_TICKS = "ticks:intraday:processed"
STREAM_CONSUMER_GROUP = "data_pipeline_group"
STREAM_HEALTH_CHECK_INTERVAL = int(os.environ.get("STREAM_HEALTH_CHECK_INTERVAL", "60"))

# Add parent directories to path for redis_files imports
# CRITICAL: Prioritize intraday_trading so it finds intraday_trading/redis_files first
script_dir = Path(__file__).parent
project_root = script_dir.parent  # intraday_trading
parent_root = project_root.parent  # aion_algo_trading
# Remove if already present to avoid duplicates
if str(project_root) in sys.path:
    sys.path.remove(str(project_root))
if str(parent_root) in sys.path:
    sys.path.remove(str(parent_root))
# Insert parent_root FIRST so alerts module is found from repo root
# Then intraday_trading so it finds intraday_trading/redis_files
sys.path.insert(0, str(parent_root))
sys.path.insert(1, str(project_root))

# Registry helper (feature-flag aware) for scanner
try:
    from shared_core.instrument_token.registry_helper import (
        get_scanner_registry,
        resolve_symbol_with_monitoring,
    )
except Exception:
    get_scanner_registry = None  # type: ignore
    resolve_symbol_with_monitoring = None  # type: ignore

def _coerce_bool(value: Any) -> bool:
    """Convert config/env values to bool safely."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    value_str = str(value).strip().lower()
    if not value_str:
        return False
    return value_str in ("1", "true", "yes", "on")

try:
    from intraday_trading.safe_debug import monitor as safe_monitor
except ImportError:
    safe_monitor = None

try:
    from intraday_trading.intraday_scanner.math_dispatcher import MathDispatcher
    MATH_DISPATCHER_AVAILABLE = True
except ImportError:
    MathDispatcher = None
    MATH_DISPATCHER_AVAILABLE = False

try:
    from intraday_trading.patterns.pattern_mathematics import PatternMathematics
    PATTERN_MATHEMATICS_AVAILABLE = True
except ImportError:
    PatternMathematics = None
    PATTERN_MATHEMATICS_AVAILABLE = False

# Use UnifiedRedisManager for centralized connection management
from shared_core.redis_clients.redis_client import UnifiedRedisManager
from shared_core.redis_clients.redis_key_standards import (
    RedisKeyStandards,
    DatabaseAwareKeyBuilder,
    get_symbol_parser,
)
from shared_core.utils.serialization import fix_numpy_serialization
from shared_core.utils import resolve_underlying_price, INDEX_PRICE_KEY_OVERRIDES


DEBUG_TRACE_ENABLED = os.getenv("DEBUG_TRACE", "1").lower() in ("1", "true", "yes")


def _normalize_index_alias(alias: str) -> List[str]:
    """Generate normalized variants for an index alias (handles prefixes, spaces, underscores)."""
    if not alias:
        return []
    alias_upper = alias.upper()
    variants = {alias_upper.strip()}
    if ':' in alias_upper:
        variants.add(alias_upper.split(':', 1)[1].strip())
    variants.add(alias_upper.replace('_', ' ').strip())
    variants.add(alias_upper.replace('_', '').strip())
    variants.add(alias_upper.replace(' ', '').strip())
    if alias_upper.startswith('NSE'):
        variants.add(alias_upper[3:].strip())
    if alias_upper.startswith('BSE'):
        variants.add(alias_upper[3:].strip())
    return [variant for variant in variants if variant]


def _build_index_alias_lookup() -> Dict[str, str]:
    parser = get_symbol_parser()
    metadata = getattr(parser, "_metadata_cache", {})
    indices = metadata.get("indices", {})
    lookup: Dict[str, str] = {}
    for base_symbol in indices.keys():
        base_upper = base_symbol.upper()
        for variant in _normalize_index_alias(base_symbol):
            lookup[variant] = base_upper
    for base_symbol, config in INDEX_PRICE_KEY_OVERRIDES.items():
        base_upper = base_symbol.upper()
        for variant in _normalize_index_alias(base_symbol):
            lookup[variant] = base_upper
        for alias in config.get('symbols', []):
            for variant in _normalize_index_alias(alias):
                lookup[variant] = base_upper
    return lookup


INDEX_ALIAS_LOOKUP = _build_index_alias_lookup()

# ✅ SOLUTION 2: Robust Stream Consumer for handling pending messages and connection resilience
class RobustStreamConsumer:
    """
    Robust Redis stream consumer with automatic reconnection and backlog handling.
    Processes both pending and new messages to prevent backlog accumulation.
    """
    
    def __init__(self, stream_key: str, group_name: str, consumer_name: str, db: int = 1):
        """
        Initialize robust stream consumer.
        
        Args:
            stream_key: Redis stream key name
            group_name: Consumer group name
            consumer_name: Consumer name (unique per process)
            db: Redis database number (default: 1 for realtime)
        """
        self.stream_key = stream_key
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.db = db
        self.redis_client = None
        self.logger = logging.getLogger(__name__)
        self.redis_client = UnifiedRedisManager.get_client(process_name="data_pipeline", database=self.db)
        self.consumer_idle_threshold = STREAM_CONSUMER_IDLE_THRESHOLD_MS
        self.health_check_interval = STREAM_HEALTH_CHECK_INTERVAL
        self._last_health_check = 0.0
    
        # Ensure consumer group exists
        try:
            groups_raw = self.redis_client.xinfo_groups(self.stream_key)
            # ✅ FIX: Ensure groups is a list/iterable (handle case where it might be None or awaitable)
            # Type narrowing: explicitly convert to list to satisfy type checker
            groups: list = []
            if groups_raw is None:
                groups = []
            elif isinstance(groups_raw, list):
                groups = groups_raw
            elif isinstance(groups_raw, tuple):
                groups = list(groups_raw)
            else:
                # Try to convert if iterable (but not awaitable)
                if hasattr(groups_raw, '__iter__') and not hasattr(groups_raw, '__await__'):
                    try:
                        groups = list(groups_raw)  # type: ignore[arg-type]
                    except (TypeError, ValueError):
                        groups = []
                else:
                    groups = []
            
            group_exists = any(
                g.get('name') == self.group_name if isinstance(g.get('name'), str) 
                else g.get('name', b'').decode('utf-8') == self.group_name
                for g in groups if isinstance(g, dict) and g.get('name') is not None
            )
            if not group_exists:
                # ✅ CRITICAL FIX: Start from '$' (latest) instead of '0' (beginning)
                # Starting from '$' ensures we only process new messages going forward
                self.redis_client.xgroup_create(
                    name=self.stream_key,
                    groupname=self.group_name,
                    id='$',  # Start from latest message (not beginning) to avoid old backlog
                    mkstream=True
                )
                self.logger.info(f"✅ Created consumer group '{self.group_name}' for '{self.stream_key}' starting from latest ('$')")
        except Exception as e:
            error_str = str(e).lower()
            if "busygroup" in error_str or "already exists" in error_str:
                pass  # Group already exists
            elif "no such key" in error_str:
                # Stream doesn't exist yet, will be created on first XADD
                pass
            else:
                self.logger.warning(f"⚠️ Error checking/creating consumer group: {e}")
    
    def process_messages(self, process_callback, poll_callback=None):
        """
        Process messages with automatic reconnection and backlog handling.
        
        Args:
            process_callback: Callback function(message_data) to process each message
            poll_callback: Optional callback invoked after each poll attempt (source, has_data)
        """
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        while True:
            try:
                now = time.time()
                if now - self._last_health_check >= self.health_check_interval:
                    self._check_consumer_health()
                    self._last_health_check = now
                # ✅ CRITICAL FIX: Process pending messages FIRST
                pending_messages = self._get_pending_messages()
                if poll_callback:
                    try:
                        poll_callback('pending', bool(pending_messages))
                    except Exception:
                        pass
                if pending_messages:
                    self._process_message_batch(pending_messages, process_callback)
                
                # ✅ THEN process new messages
                new_messages = self._get_new_messages()
                if poll_callback:
                    try:
                        poll_callback('new', bool(new_messages))
                    except Exception:
                        pass
                if new_messages:
                    self._process_message_batch(new_messages, process_callback)
                
                consecutive_errors = 0  # reset on success
                
            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors > max_consecutive_errors:
                    self.logger.critical(f"🚨 Too many errors in stream consumer: {e}")
                    break
                time.sleep(1)  # wait before retrying

    def _get_pending_messages(self):
        """Get pending messages with short timeout"""
        try:
            if self.redis_client:
                result = self.redis_client.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_key: '0'},  # '0' = pending messages
                    count=50,  # Process up to 50 pending per batch
                    block=500  # 0.5s timeout
                )
                return result if result else None
        except Exception as e:
            error_str = str(e).lower()
            if "timeout" in error_str or "no such key" in error_str or "no such stream" in error_str:
                return None
            # For other errors, log and return None (don't break the loop)
            self.logger.debug(f"Error reading pending messages: {e}")
            return None
    
    def _get_new_messages(self):
        """Get new messages with normal timeout"""
        try:
            if self.redis_client:
                result = self.redis_client.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_key: '>'},  # '>' = new messages
                    count=50,
                    block=2000  # 2s timeout
                )
            return result if result else None
        except Exception as e:
            error_str = str(e).lower()
            if "timeout" in error_str or "no such key" in error_str or "no such stream" in error_str:
                return None
            # For other errors, log and return None (don't break the loop)
            self.logger.debug(f"Error reading new messages: {e}")
            return None
    
    def _process_message_batch(self, messages, process_callback):
        """Process a batch of messages and acknowledge them"""
        if not messages:
            return
        
        for stream, message_list in messages:
            if not message_list:
                continue
            for message_id, message_data in message_list:
                try:
                    process_callback(message_data)
                except Exception as e:
                    self.logger.error(f"🚨 POISON PILL in batch processing {message_id}: {e}")
                finally:
                    # ✅ GUARANTEED ACK: Always acknowledge to prevent batch blockage
                    if self.redis_client:
                        try:
                            self.redis_client.xack(self.stream_key, self.group_name, message_id)
                        except Exception as ack_err:
                            self.logger.debug(f"ACK error in batch: {ack_err}")
    
    def _check_consumer_health(self):
        """Inspect consumer group and remove idle consumers with pending lag."""
        if not self.redis_client:
            return
        try:
            consumers = self.redis_client.xinfo_consumers(self.stream_key, self.group_name)
        except redis.ResponseError as exc:
            if "NOGROUP" in str(exc).upper():
                self.logger.warning(
                    "⚠️ Consumer group %s does not exist on %s",
                    self.group_name,
                    self.stream_key,
                )
            else:
                self.logger.error(
                    "❌ Failed to fetch consumer info for %s/%s: %s",
                    self.stream_key,
                    self.group_name,
                    exc,
                )
            return
        except Exception as exc:
            self.logger.error(
                "❌ Unexpected consumer health error for %s/%s: %s",
                self.stream_key,
                self.group_name,
                exc,
            )
            return

        dead_consumers = []
        for consumer in consumers or []:
            idle = consumer.get("idle", 0)
            pending = consumer.get("pending", 0)
            consumer_name = consumer.get("name")
            if idle >= self.consumer_idle_threshold and pending > 0:
                dead_consumers.append(consumer_name)
                self.logger.warning(
                    "🚨 Dead consumer detected: %s (idle=%sms pending=%s)",
                    consumer_name,
                    idle,
                    pending,
                )
        
        for consumer_name in dead_consumers:
            try:
                self.redis_client.xgroup_delconsumer(self.stream_key, self.group_name, consumer_name)
                self.logger.info(
                    "✅ Removed dead consumer %s from %s/%s",
                    consumer_name,
                    self.stream_key,
                    self.group_name,
                )
            except Exception as exc:
                self.logger.error(
                    "❌ Failed to remove consumer %s: %s",
                    consumer_name,
                    exc,
                )
        
        if len(dead_consumers) > 10:
            self.logger.warning(
                "🔄 Removed %d dead consumers from %s/%s. Consider rebalancing workers.",
                len(dead_consumers),
                self.stream_key,
                self.group_name,
            )



def cleanup_redis_connections():
    """
    Cleanup function to close all cached Redis connections properly.
    Called on process exit to prevent connection leaks.
    """
    try:
        UnifiedRedisManager.cleanup_all_clients()
    except Exception:
        pass

import numpy as np

# ✅ Primary import path: shared_core
try:
    from shared_core.utils.time_utils import IndianMarketTimeParser
    TIME_UTILS_AVAILABLE = True
except ImportError:
    TIME_UTILS_AVAILABLE = False

    class INDIAN_TIME_PARSER:  # type: ignore
        @staticmethod
        def parse(timestamp):
            return timestamp

# ✅ FIX: Import TimestampNormalizer from shared_core
if TYPE_CHECKING:
    # For type checking, always use the shared_core version
    from shared_core.config_utils.timestamp_normalizer import TimestampNormalizer
else:
    # Runtime import with fallbacks
    try:
        from shared_core.config_utils.timestamp_normalizer import TimestampNormalizer
        TIMESTAMP_NORMALIZER_AVAILABLE = True
    except ImportError:
            # Use a different name to avoid type conflicts
            class _FallbackTimestampNormalizer:
                @staticmethod
                def to_epoch_ms(timestamp):
                    if isinstance(timestamp, (int, float)):
                        return int(timestamp * 1000) if timestamp < 1000000000000 else int(timestamp)
                    return int(datetime.now().timestamp() * 1000)
            # Alias to TimestampNormalizer for runtime compatibility
            TimestampNormalizer = _FallbackTimestampNormalizer  # type: ignore[assignment, misc]
            TIMESTAMP_NORMALIZER_AVAILABLE = False

# Canonical field-mapping helpers
from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
from shared_core.redis_clients import (
    normalize_redis_tick_data,
    map_tick_for_pattern,  # ✅ ADD: For consumer-specific field mapping during migration
    get_redis_key_fields,
)
from shared_core.config_utils.yaml_field_loader import (
    get_field_mapping_manager,
    normalize_session_record,
    resolve_calculated_field,
    resolve_session_field,
)
YAML_FIELD_LOADER_AVAILABLE = True

import pytz

IST = pytz.timezone("Asia/Kolkata")

FIELD_MAPPING_MANAGER = get_field_mapping_manager()
SESSION_FIELD_ZERODHA_CUM = resolve_session_field("zerodha_cumulative_volume")
SESSION_FIELD_BUCKET_CUM = resolve_session_field("bucket_cumulative_volume")
SESSION_FIELD_BUCKET_INC = resolve_session_field("bucket_incremental_volume")
SESSION_FIELD_LTQ = resolve_session_field("zerodha_last_traded_quantity")

def get_current_ist_time():
    """Get current time in IST timezone"""
    return datetime.now(IST)


def get_current_ist_timestamp():
    """Get current timestamp in IST as ISO string"""
    return get_current_ist_time().isoformat()

class DataPipelineEnforcer:
    """
    ✅ NEW: Enforce data quality and break infinite recovery loops
    Implements circuit breaker pattern with exponential backoff
    """
    
    def __init__(self):
        self.recovery_attempts = 0
        self.max_recovery_attempts = 3
        self.circuit_breaker_tripped = False
        self.last_recovery_time = None
        self.logger = logging.getLogger(__name__)
    
    def should_attempt_recovery(self) -> bool:
        """
        Check if recovery should be attempted (circuit breaker logic)
        Returns True if recovery can proceed, False if circuit breaker is active
        """
        current_time = time.time()
        
        # Circuit breaker check
        if self.circuit_breaker_tripped:
            if self.last_recovery_time and current_time - self.last_recovery_time < 300:  # 5 minute cooldown
                self.logger.debug("🔌 Circuit breaker active - skipping recovery")
                return False
            else:
                self.logger.info("🔌 Circuit breaker reset after cooldown")
                self.circuit_breaker_tripped = False
                self.recovery_attempts = 0
                return True
        
        # Exponential backoff
        if self.last_recovery_time:
            backoff_seconds = 2 ** self.recovery_attempts
            time_since_last = current_time - self.last_recovery_time
            if time_since_last < backoff_seconds:
                self.logger.debug(
                    f"⏳ Backoff in progress ({time_since_last:.0f}s / {backoff_seconds}s)"
                )
                return False
        
        return True
    
    def record_recovery_attempt(self) -> bool:
        """
        Record a recovery attempt and check if circuit breaker should trip
        Returns True if recovery can proceed, False if max attempts reached
        """
        current_time = time.time()
        self.recovery_attempts += 1
        self.last_recovery_time = current_time
        
        if self.recovery_attempts > self.max_recovery_attempts:
            self.logger.critical(
                f"🚨 MAX RECOVERY ATTEMPTS ({self.max_recovery_attempts}) - "
                f"TRIPPING CIRCUIT BREAKER (5 min cooldown)"
            )
            self.circuit_breaker_tripped = True
            return False
        
        self.logger.info(
            f"🔄 Recovery attempt {self.recovery_attempts}/{self.max_recovery_attempts}"
        )
        return True
    
    def reset_recovery_state(self):
        """Reset recovery state after successful recovery"""
        if self.recovery_attempts > 0:
            self.logger.info(
                f"✅ Recovery successful - reset counter (was {self.recovery_attempts})"
            )
        self.recovery_attempts = 0
        self.last_recovery_time = None
    
    def get_status(self) -> dict:
        """Get current enforcer status for monitoring"""
        return {
            'circuit_breaker_tripped': self.circuit_breaker_tripped,
            'recovery_attempts': self.recovery_attempts,
            'max_recovery_attempts': self.max_recovery_attempts,
            'last_recovery_time': self.last_recovery_time,
            'cooldown_remaining': (
                300 - (time.time() - self.last_recovery_time)
                if self.circuit_breaker_tripped and self.last_recovery_time
                else 0
            )
        }


class DataPipeline:
    """
    Main data ingestion pipeline
    Subscribes to Redis channels and provides tick data to pattern engine
    """
    
    # =========================================================================
    # CRITICAL FIELD DEFINITIONS - Single Source of Truth
    # These field tuples define which fields must survive data cleaning/enrichment
    # =========================================================================

    # Greek Fields (Options - computed by crawler or enrichment layer)
    CRITICAL_GREEK_FIELDS = (
        "delta",
        "gamma",
        "theta",
        "vega",
        "rho",
        "iv",
        "implied_volatility",
        "color",                    # Second-order: rate of change of gamma
        "vanna",                    # Cross-greek: delta sensitivity to IV
        "volga",                    # Cross-greek: vega sensitivity to IV (vomma)
        "charm",                    # Cross-greek: delta sensitivity to time
        "veta",                     # Cross-greek: vega sensitivity to time
        "speed",                    # Third-order: rate of change of gamma
        "zomma",                    # Cross-greek: gamma sensitivity to IV
        "ultima",                   # Third-order: vomma sensitivity to IV
    )

    # Option Fields (basic + advanced)
    CRITICAL_OPTION_FIELDS = (
        "option_price",
        "strike_price",
        "strike",
        "expiry_date",
        "expiry",
        "option_type",
        "instrument_type",          # CE, PE, FUT
        "open_interest",
        "oi",
        "oi_change",
        "oi_change_pct",
        "underlying_price",
        "spot_price",
        "dte_years",
        "trading_dte",
        "calendar_dte",
        "expiry_series",            # WEEKLY, MONTHLY
        "is_weekly",
        "moneyness",                # ITM, ATM, OTM
        "intrinsic_value",
        "extrinsic_value",
        "time_value",
        "lot_size",
        "tick_size",
    )

    # Advanced Option Fields (computed metrics)
    CRITICAL_ADVANCED_OPTION_FIELDS = (
        "gamma_exposure",
        "gamma_exposure_dollar",
        "delta_exposure",
        "vega_exposure",
        "net_gamma",
        "net_delta",
        "put_call_ratio",
        "pcr",
        "pcr_oi",
        "pcr_volume",
        "max_pain",
        "max_pain_strike",
        "iv_percentile",
        "iv_rank",
        "iv_skew",
        "term_structure_slope",
        "atm_iv",
        "otm_skew",
        "risk_reversal",
        "butterfly_spread",
        "iv_surface",               # Dict of strike->IV mappings
    )

    # Futures Fields
    CRITICAL_FUTURES_FIELDS = (
        "futures_price",
        "future_price",
        "open_interest",
        "oi",
        "oi_change",
        "underlying_price",
        "spot_price",
        "basis",
        "basis_pct",
        "roll_yield",
        "contango",
        "backwardation",
        "cost_of_carry",
        "fair_value",
        "premium",
        "discount",
    )

    # Index Fields
    CRITICAL_INDEX_FIELDS = (
        "spot_price",
        "index_value",
        "open_interest",
        "oi_change",
        "underlying_price",
        "vix",
        "india_vix",
        "vix_level",
        "vix_regime",
        "advance_decline_ratio",
        "breadth",
        "constituents_up",
        "constituents_down",
    )

    # Tick Data Fields (raw market data)
    CRITICAL_TICK_FIELDS = (
        "last_price",
        "price",
        "ltp",
        "volume",
        "volume_traded_today",
        "cumulative_volume",
        "last_traded_quantity",
        "ltq",
        "average_price",
        "average_traded_price",
        "timestamp",
        "timestamp_ms",
        "exchange_timestamp",
        "exchange_timestamp_ms",
        "last_trade_time",
        "ohlc",                     # Dict with open/high/low/close
        "open",
        "high",
        "low",
        "close",
        "change",
        "change_pct",
        "net_change",
        "net_change_pct",
        "bid",
        "ask",
        "bid_quantity",
        "ask_quantity",
        "total_buy_quantity",
        "total_sell_quantity",
        "depth",                    # Order book depth
        "tradingsymbol",
        "symbol",
        "instrument_token",
        "exchange",
        "segment",
        "mode",
    )

    # Technical Indicators (standard TA)
    CRITICAL_INDICATOR_FIELDS = (
        "rsi",
        "rsi_14",
        "macd",
        "macd_signal",
        "macd_histogram",
        "ema_9",
        "ema_20",
        "ema_50",
        "ema_200",
        "sma_9",
        "sma_20",
        "sma_50",
        "sma_200",
        "vwap",
        "vwap_deviation",
        "atr",
        "atr_14",
        "bollinger_upper",
        "bollinger_lower",
        "bollinger_middle",
        "bollinger_width",
        "stochastic_k",
        "stochastic_d",
        "adx",
        "plus_di",
        "minus_di",
        "cci",
        "mfi",
        "obv",
        "williams_r",
        "pivot_point",
        "support_1",
        "support_2",
        "resistance_1",
        "resistance_2",
        "supertrend",
        "supertrend_direction",
    )

    # Custom/Proprietary Indicators
    CRITICAL_CUSTOM_INDICATOR_FIELDS = (
        "volume_ratio",
        "normalized_volume",
        "volume_context",
        "volume_zscore",
        "volume_percentile",
        "order_flow_imbalance",
        "order_flow_delta",
        "buy_pressure",
        "sell_pressure",
        "institutional_activity",
        "retail_activity",
        "smart_money_flow",
        "dark_pool_activity",
        "block_trade_ratio",
        "momentum_score",
        "trend_strength",
        "volatility_regime",
        "microstructure_score",
        "liquidity_score",
        "spread_normalized",
        "tick_intensity",
        "time_weighted_price",
    )

    # Edge Computation Fields (from calculations_edge.py)
    CRITICAL_EDGE_FIELDS = (
        # Intent Layer
        "dhdi",                     # Delta-Hedge Demand Index
        "motive",                   # Hedge/Spec/Mixed classification
        "hedge_pressure",
        "hedge_pressure_20",
        "speculative_ratio",
        "intent_score",
        
        # Constraint Layer
        "gamma_stress",
        "charm_ratio",
        "lci",                      # Liquidity Commitment Index
        "session_mult",
        "constraint_score",
        "gamma_flip_distance",
        "dealer_gamma_net",
        
        # Transition Layer
        "transition_prob",
        "transition_probability",
        "tcs",                      # Time Compression Signal
        "tcs_10",
        "latency_collapse",
        "regime_shift_probability",
        
        # Regime Layer
        "regime_current",
        "regime_id",
        "regime_confidence",
        "should_trade",
        "recommended_strategies",
        "regime_duration",
        "regime_stability",
        
        # Edge State
        "edge_state",
        "classifier_state",
        "intent_windows",
        "constraint_windows",
        "transition_windows",
    )

    # Volume Analysis Fields
    CRITICAL_VOLUME_FIELDS = (
        "volume_ratio",
        "normalized_volume",
        "volume_context",
        "relative_volume",
        "rvol",
        "volume_ma_ratio",
        "volume_spike",
        "unusual_volume",
        "volume_profile_poc",       # Point of Control
        "volume_profile_vah",       # Value Area High
        "volume_profile_val",       # Value Area Low
        "cumulative_delta",
        "delta_divergence",
        "bucket_volume",
        "bucket_cumulative",
        "bucket_incremental",
    )

    # Price Action Fields
    CRITICAL_PRICE_FIELDS = (
        "price_change",
        "price_change_pct",
        "price_move",
        "price_movement",
        "net_price_change",
        "net_price_change_pct",
        "last_price_change",
        "last_price_change_pct",
        "gap",
        "gap_pct",
        "range",
        "range_pct",
        "true_range",
        "body_size",                # Candle body
        "wick_ratio",               # Candle wick analysis
        "trend_direction",
        "swing_high",
        "swing_low",
    )

    # All numeric fields that need type conversion from Redis
    _NUMERIC_SNAPSHOT_FIELDS = {
        # Greeks
        "delta", "gamma", "theta", "vega", "rho", "iv", "implied_volatility",
        "color", "vanna", "volga", "charm", "veta", "speed", "zomma", "ultima",
        # Options
        "dte_years", "trading_dte", "calendar_dte", "option_price", "strike_price",
        "strike", "underlying_price", "spot_price", "intrinsic_value", "extrinsic_value",
        "gamma_exposure", "delta_exposure", "vega_exposure", "max_pain",
        # Prices
        "last_price", "price", "ltp", "open", "high", "low", "close",
        "bid", "ask", "average_price", "vwap",
        # Volume
        "volume", "volume_traded_today", "cumulative_volume", "open_interest", "oi",
        "volume_ratio", "normalized_volume", "relative_volume",
        # Indicators
        "rsi", "macd", "atr", "adx", "cci", "mfi", "obv",
        # Edge
        "dhdi", "hedge_pressure", "gamma_stress", "lci", "tcs", "transition_prob",
        "regime_confidence", "intent_score", "constraint_score",
        # Changes
        "price_change", "price_change_pct", "oi_change", "oi_change_pct",
    }

    def _get_unified_symbol(self, tick_data: dict) -> str:
        """
        ✅ UNIFIED: Single source of truth for symbol extraction and normalization.
        Uses robust UniversalSymbolParser from shared_core for consistent parsing.
        Returns canonical symbol format that matches Redis key standards.
        
        This method consolidates all symbol extraction logic into one place, ensuring
        consistent symbol format across all code paths using the centralized parsing logic.
        
        Args:
            tick_data: Dictionary containing tick data with symbol fields
            
        Returns:
            Canonical symbol string (e.g., NSEHDFCBANK, NFOBANKNIFTY30DEC60200CE)
            Returns empty string if symbol cannot be determined
        """
        try:
            # Step 1: Extract raw symbol from all possible fields
            raw_symbol = (
                tick_data.get("tradingsymbol") or 
                tick_data.get("symbol") or 
                tick_data.get("instrument_token")
            )
            
            if not raw_symbol:
                return ""
                
            # Step 2: Handle numeric tokens and TOKEN_ format (token resolution)
            symbol_str = str(raw_symbol)
            
            # ✅ FIX: Strip embedded quotes from malformed symbols (e.g., NSEB'NIFTY50')
            symbol_str = symbol_str.strip().strip("'\"` ")
            
            if (symbol_str.isdigit() or 
                symbol_str.startswith("TOKEN_") or 
                symbol_str == "UNKNOWN"):
                
                token = tick_data.get("instrument_token")
                # Try cache first
                cached = self._get_symbol_from_cache(token)
                if cached and cached != "UNKNOWN":
                    symbol_str = cached
                elif token and token != "":
                    # Try resolver
                    resolver = getattr(self, "resolve_token_to_symbol", None)
                    if callable(resolver):
                        try:
                            resolved = resolver(token)
                            if resolved and isinstance(resolved, str) and not resolved.startswith("UNKNOWN"):
                                symbol_str = resolved
                        except Exception as e:
                            self.logger.debug(f"Token resolution failed: {e}")
            
            if not symbol_str or symbol_str == "UNKNOWN":
                return ""
            
            # Step 3: ✅ CRITICAL FIX: Use UniversalSymbolParser.parse_symbol() for robust parsing
            # This ensures we use the same robust parsing logic as the rest of the codebase
            try:
                from shared_core.redis_clients.redis_key_standards import get_symbol_parser, RedisKeyStandards
                
                # Parse symbol using UniversalSymbolParser (robust parsing logic)
                parser = get_symbol_parser()
                parsed = parser.parse_symbol(symbol_str)
                
                # Get canonical symbol from parsed result (uses parser's normalize logic)
                canonical_symbol = RedisKeyStandards.canonical_symbol(symbol_str)
                
                # Use canonical symbol if available, otherwise use parsed normalized symbol
                if canonical_symbol and canonical_symbol != symbol_str:
                    self.logger.debug(f"🔍 [SYMBOL_PARSE] {symbol_str} -> {canonical_symbol} (type={parsed.instrument_type})")
                    symbol_str = canonical_symbol
                elif parsed.normalized and parsed.normalized != symbol_str:
                    self.logger.debug(f"🔍 [SYMBOL_PARSE] {symbol_str} -> {parsed.normalized} (type={parsed.instrument_type})")
                    symbol_str = parsed.normalized
                else:
                    # Symbol is already in correct format, just log the parsed type
                    self.logger.debug(f"🔍 [SYMBOL_PARSE] {symbol_str} (type={parsed.instrument_type}, already canonical)")
                    
            except Exception as e:
                self.logger.warning(f"⚠️ [SYMBOL_PARSE] Failed to parse {symbol_str} using UniversalSymbolParser: {e}")
                # Fallback: Try canonical_symbol directly
                try:
                    from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
                    canonical_symbol = RedisKeyStandards.canonical_symbol(symbol_str)
                    if canonical_symbol and canonical_symbol != symbol_str:
                        symbol_str = canonical_symbol
                except Exception as e2:
                    self.logger.debug(f"Symbol normalization fallback also failed: {e2}")
            
            # Step 4: Update tick_data for downstream consistency
            if symbol_str and symbol_str != "UNKNOWN":
                tick_data['symbol'] = symbol_str
                tick_data['tradingsymbol'] = symbol_str
            
            return symbol_str if symbol_str != "UNKNOWN" else ""
            
        except Exception as e:
            self.logger.error(f"❌ [SYMBOL_EXTRACT] Unified symbol extraction failed: {e}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            return ""

    def _get_complete_redis_data(self, symbol: str) -> dict:
        """Get Redis data with proper type conversion"""
        try:
            base_key = f"ind:{symbol}"
            greeks_key = f"ind:greeks:{symbol}"
            indicator_data = self.redis_client.hgetall(base_key)
            greek_data = self.redis_client.hgetall(greeks_key)
            if not indicator_data and not greek_data:
                return {}

            merged = dict(indicator_data)
            merged.update(greek_data)

            cleaned: dict[str, Any] = {}
            instrument_type = merged.get('instrument_type', '')
            for key, value in merged.items():
                normalized = self._normalize_redis_value(value)
                cleaned[key] = normalized

            self._enforce_delta_sign(cleaned, instrument_type)
            if 'gamma_exposure' not in cleaned or cleaned.get('gamma_exposure') in (None, ''):
                self._maybe_recalculate_gamma_exposure(symbol, cleaned)

            summary_keys = ['delta', 'gamma', 'gamma_exposure', 'volume_ratio', 'order_flow_imbalance', 'vix_level']
            summary = {field: cleaned.get(field) for field in summary_keys if cleaned.get(field) not in (None, '')}
            self.logger.info(
                "🗃️ [REDIS_ENRICH] %s - fetched %s fields %s",
                symbol,
                len(cleaned),
                summary,
            )
            return cleaned
        except Exception as e:
            self.logger.debug(f"⚠️ [REDIS_FETCH_FAIL] {symbol}: {e}")
            return {}

    def _normalize_redis_value(self, value: Any) -> Any:
        """Convert stored Redis values (including np.float64 strings) to native Python types."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            # Handle numpy string repr: np.float64(0.00123)
            if stripped.startswith('np.float64(') and stripped.endswith(')'):
                inner = stripped[len('np.float64('):-1]
                try:
                    return float(inner)
                except (ValueError, TypeError):
                    return None
            # Plain numeric string
            try:
                return float(stripped)
            except (ValueError, TypeError):
                return stripped
        return value

    def _enforce_delta_sign(self, cleaned: Dict[str, Any], instrument_type: str) -> None:
        delta = cleaned.get('delta')
        if delta in (None, ''):
            return
        try:
            delta_val = float(delta)
        except (ValueError, TypeError):
            return
        instrument_type = str(instrument_type).upper()
        if instrument_type == 'PE' and delta_val > 0:
            cleaned['delta'] = -abs(delta_val)
        elif instrument_type == 'CE' and delta_val < 0:
            cleaned['delta'] = abs(delta_val)
        else:
            cleaned['delta'] = delta_val

    def _maybe_recalculate_gamma_exposure(self, symbol: str, cleaned: Dict[str, Any]) -> None:
        gamma = cleaned.get('gamma')
        oi = cleaned.get('open_interest')
        spot = cleaned.get('underlying_price') or cleaned.get('last_price') or cleaned.get('spot_price')
        iv = cleaned.get('iv') or cleaned.get('implied_volatility')
        try:
            if gamma in (None, ''):
                return
            gamma = float(gamma)
            if oi in (None, ''):
                return
            oi = float(oi)
            if spot in (None, ''):
                return
            spot = float(spot)
            if iv in (None, ''):
                return
            iv = float(iv)
        except (ValueError, TypeError, TypeError):
            return
        if any(val is None for val in (gamma, oi, spot, iv)):
            return
        gamma_exposure = gamma * oi * spot * spot * iv
        cleaned['gamma_exposure'] = gamma_exposure
        if DEBUG_TRACE_ENABLED:
            self.logger.debug(
                "🔬 [%s] gamma_exposure recomputed -> %.4e",
                symbol,
                gamma_exposure,
            )

    def __init__(self, config=None, redis_client=None, pattern_detector=None, 
                 alert_manager=None, tick_processor=None, scanner=None):
        """Initialize the data pipeline with user-approved single source of truth"""
        self.config = config or {}
        self.running = False
        self.worker_id = self.config.get("worker_id", os.getpid())
        
        # ✅ Initialize symbol parser (single source of truth)
        from shared_core.redis_clients.redis_key_standards import get_symbol_parser
        self.symbol_parser = get_symbol_parser()
        
        # ✅ Initialize Redis - use provided client or create new one
        self.redis_client = UnifiedRedisManager.get_client(process_name="data_pipeline", database=1)
        
        # ✅ Store passed component references
        self.pattern_detector = pattern_detector
        self.alert_manager = alert_manager
        self.tick_processor = tick_processor
        self.scanner = scanner  # For game theory access
        
        # Track data sources
        self.crawler_fields = set()
        
        # ✅ Initialize logger early
        import logging
        self.logger = logging.getLogger("DataPipeline")

        if not self.logger.handlers:
            # Add basic console handler if no handlers exist
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        # ✅ Feature-flag aware registry (optional, for token resolution with monitoring)
        self.registry = None
        try:
            from shared_core.instrument_token.registry_helper import get_scanner_registry
            self.registry = get_scanner_registry()
            self.logger.info("✅ Registry initialized for data pipeline (with feature flags)")
        except Exception as e:
            self.logger.debug(f"Registry not available for data pipeline: {e}")
            self.registry = None
        # ✅ FIX: Check if MathDispatcher and PatternMathematics are available before instantiating
        if MATH_DISPATCHER_AVAILABLE and MathDispatcher is not None and PATTERN_MATHEMATICS_AVAILABLE and PatternMathematics is not None:
            self.math_dispatcher = MathDispatcher(PatternMathematics, None)
        else:
            self.math_dispatcher = None
            import logging
            logger = logging.getLogger(__name__)
            if not MATH_DISPATCHER_AVAILABLE or MathDispatcher is None:
                logger.warning("⚠️ MathDispatcher not available - pattern confidence calculations may be limited")
            elif not PATTERN_MATHEMATICS_AVAILABLE or PatternMathematics is None:
                logger.warning("⚠️ PatternMathematics not available - MathDispatcher cannot be initialized")
        self.stats = {
            "ticks_received": 0,
            "ticks_processed": 0,
            "stale_ticks_rejected": 0,
            "ticks_deduplicated": 0,
            "tick_queue_enqueued": 0,
            "tick_queue_drops": 0,
            "batches_processed": 0,
            "errors": 0,
            "json_errors": 0,
            "validation_errors": 0,
            "protocol_errors": 0,
        }
        # Channel-specific error tracking
        self.channel_errors: Dict[str, int] = {
            "market_data.ticks": 0,
            "premarket.orders": 0,
            "alerts.manager": 0,
        }
        # Track symbols where we already logged a freshness fallback to avoid log spam
        self._freshness_fallback_logged = set()
        self.latency_stats = {
            "symbol_resolution_time": 0.0,
            "cleaning_time": 0.0,
            "validation_time": 0.0,
            "pattern_detection_time": 0.0,
            "total_processing_time": 0.0,
        }
        
        # Initialize cumulative bucket_incremental_volume tracking for incremental bucket_incremental_volume calculation
        self._cumulative_volume_tracker = {}
        
        # Deduplication tracking to reduce processing overhead
        self.last_processed = {}  # symbol -> last processed timestamp
        self.dedupe_window = 0.1  # 100ms between processing same symbol
        
        # Idle state detection - grace period before considering pipeline stalled
        self.idle_grace_period = 30.0  # 30 seconds grace period for idle detection
        self.last_heartbeat = time.time()  # Track last heartbeat for health checks
        
        # News processing tracking
        self.last_news_check = 0
        self.news_check_interval = 30  # Check for news every 30 seconds

        # Tick flow debugging and queue controls
        self.tick_queue_maxsize = int(self.config.get("tick_queue_maxsize", 10000))
        env_tick_debug = os.environ.get("TICK_FLOW_DEBUG")
        tick_debug_value = env_tick_debug if env_tick_debug is not None else self.config.get("tick_flow_debug", False)
        self.tick_flow_debug = _coerce_bool(tick_debug_value)
        self.tick_flow_debug_symbol = self.config.get("tick_flow_debug_symbol")
        self._last_tick_flow_log = 0.0
        
        # Pipeline blockage tracking (monitors per-symbol processing cadence)
        self.processing_times = deque(maxlen=1000)
        self.pipeline_blockage_threshold = float(
            self.config.get(
                "pipeline_blockage_threshold",
                os.environ.get("PIPELINE_BLOCKAGE_THRESHOLD", 5.0),
            )
        )
        self.pipeline_monitor_interval = float(
            self.config.get(
                "pipeline_monitor_interval",
                os.environ.get("PIPELINE_MONITOR_INTERVAL", 1.0),
            )
        )
        self._pipeline_monitor_thread: Optional[threading.Thread] = None
        self._pipeline_monitor_running = False
        self._pipeline_recovery_count = 0

        # SINGLE project root calculation
        self.intraday_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # intraday_trading/
        self.project_root = os.path.dirname(self.intraday_root)  # aion_algo_trading/ (parent for redis_files)
        if self.project_root not in sys.path:
            sys.path.insert(0, self.project_root)
        if self.intraday_root not in sys.path:
            sys.path.insert(0, self.intraday_root)
        
        # ✅ PRELOAD SYMBOL MAPPING: warm cache before ticks arrive
        self._symbol_cache = {}
        self._symbol_mapping_loaded = False
        # ✅ FIXED: Always use intraday crawler metadata (single source of truth)
        env_override_path = os.environ.get("TOKEN_LOOKUP_PATH")
        possible_paths: List[Path] = []
        seen_paths = set()
        repo_root_path = Path(self.project_root).parent

        def _add_possible_path(candidate: Optional[Path]) -> None:
            if not candidate:
                return
            candidate_path = Path(candidate).expanduser()
            candidate_str = str(candidate_path)
            if candidate_str in seen_paths:
                return
            seen_paths.add(candidate_str)
            possible_paths.append(candidate_path)

        if env_override_path:
            _add_possible_path(Path(env_override_path))

        # ✅ FIXED: zerodha is valid module in src/, so use project_root (src) not repo_root
        crawler_instruments_path = Path(self.project_root) / "zerodha" / "crawlers" / "binary_crawler1" / "intraday_crawler_instruments.json"
        _add_possible_path(crawler_instruments_path)

        self._symbol_mapping_path = None
        for path in possible_paths:
            if path.exists():
                self._symbol_mapping_path = path
                self.logger.info(f"✅ Found intraday token metadata at: {path}")
                break
        if not self._symbol_mapping_path:
            # Always fall back to crawler instruments path even if missing; downstream load will error loudly
            self._symbol_mapping_path = crawler_instruments_path
            self.logger.error(
                "❌ intraday_crawler_instruments.json not found at %s; TOKEN_LOOKUP_PATH overrides were also missing. "
                "Symbol resolution may fail until the file is generated.",
                crawler_instruments_path,
            )

        # Import schema helpers from consolidated config (optional)
        try:
            from config.schemas import (
                calculate_derived_fields,
                map_kite_to_unified,
                normalize_zerodha_tick_data,
                get_asset_class,
            )
            from zerodha.crawlers.utils.instrument_mapper import InstrumentMapper
            instrument_mapper = InstrumentMapper()
            hot_token_resolver = instrument_mapper.token_to_symbol
            self.calculate_derived_fields = calculate_derived_fields
            self.map_kite_to_unified = map_kite_to_unified
            self.normalize_zerodha_tick_data = normalize_zerodha_tick_data
            self.get_asset_class = get_asset_class
            self.schema_validation_available = True
            self._hot_token_mapper = instrument_mapper
        except ImportError:
            # Provide safe fallbacks so downstream calls do not fail
            self.schema_validation_available = False
            self.calculate_derived_fields = lambda cleaned: {}
            self.map_kite_to_unified = lambda data: data
            self.normalize_zerodha_tick_data = lambda data: data
            hot_token_resolver = None
            self._hot_token_mapper = None
        
        # ✅ Create unified token resolver with registry + hot_token_mapper fallback
        def resolve_token_to_symbol_with_fallback(token):
            """
            Resolve token to symbol using registry first, then hot_token_mapper as fallback.
            Includes performance monitoring.
            
            Strategy:
            1. Try registry.get_by_broker_token() for direct token lookup (fastest, monitored)
            2. Fallback to hot_token_mapper (primary method for token->symbol mapping)
            3. Validate resolved symbol with registry if available
            """
            if not token:
                return None
            
            # Try registry first (direct token lookup with monitoring)
            if self.registry and hasattr(self.registry, 'registry'):
                try:
                    # Try to get instrument by broker token from registry
                    unified_inst = self.registry.registry.get_by_broker_token("zerodha", str(token))
                    if unified_inst:
                        # Get the broker instrument to extract symbol
                        broker_inst = unified_inst.get_broker_instrument("zerodha")
                        if broker_inst:
                            symbol = broker_inst.tradingsymbol
                            if symbol and symbol != "UNKNOWN":
                                # Record successful registry lookup
                                from shared_core.instrument_token.registry_config import record_lookup_metrics
                                record_lookup_metrics(0.001, cache_hit=True)  # Fast lookup
                                return symbol
                except Exception as e:
                    self.logger.debug(f"Registry token lookup failed for token {token}: {e}")
            
            # Fallback to hot_token_mapper (primary method for token->symbol mapping)
            if hot_token_resolver:
                try:
                    symbol = hot_token_resolver(token)
                    if symbol and symbol != "UNKNOWN":
                        # If registry is available, validate the symbol (with monitoring)
                        if self.registry:
                            try:
                                from shared_core.instrument_token.registry_helper import resolve_symbol_with_monitoring
                                result = resolve_symbol_with_monitoring(
                                    self.registry, symbol, "zerodha", module_name="data_pipeline"
                                )
                                if result:
                                    # Symbol validated in registry
                                    return symbol
                                # If not found in registry, still return symbol (hot_token_mapper is authoritative)
                                self.logger.debug(f"Symbol {symbol} from token {token} not found in registry (using hot_token_mapper result)")
                            except Exception as e:
                                self.logger.debug(f"Registry validation failed for symbol {symbol}: {e}")
                        return symbol
                except Exception as e:
                    self.logger.debug(f"Hot token mapper lookup failed for token {token}: {e}")
            
            return None
        
        self.resolve_token_to_symbol = resolve_token_to_symbol_with_fallback
        
        # Set default asset class if not available
        if not hasattr(self, 'get_asset_class'):
            def _default_asset_class(symbol: str) -> str:
                symbol_str = str(symbol or "").upper()
                if "CE" in symbol_str or "PE" in symbol_str:
                    return "options"
                if "FUT" in symbol_str:
                    return "futures"
                return "equity"
            self.get_asset_class = _default_asset_class

        # Configurable parameters
        self.buffer_capacity = self.config.get("buffer_size", 50000)
        self.batch_size = self.config.get(
            "batch_size", 10
        )  # Smaller batch size for faster processing
        self.dedup_window = self.config.get("dedup_window", 5.0)  # seconds
        self.max_log_size = self.config.get("max_log_size", 10 * 1024 * 1024)  # 10MB
        self.log_backup_count = self.config.get("log_backup_count", 5)

        # Initialize last_price cache for underlying last_price tracking
        self.price_cache = {}
        
        # Setup logging FIRST (before using self.logger)
        self._setup_logging()
        self._load_symbol_mapping()
        
        # ✅ NEW: Initialize data quality enforcer with circuit breaker (after logger setup)
        self.enforcer = DataPipelineEnforcer()
        self.logger.info("✅ DataPipelineEnforcer initialized with circuit breaker")
        self._price_range_cache = {}

        self.historical_archive_enabled = self.config.get('enable_historical_archive', False)  # Disabled by default
        self.historical_archive = None
        if self.historical_archive_enabled:
            try:
                from utils.historical_archive import get_historical_archive
                postgres_config = self.config.get('postgres_config', {})
                self.historical_archive = get_historical_archive(
                    postgres_config=postgres_config,
                    batch_size=self.config.get('archive_batch_size', 1000),
                    batch_interval=self.config.get('archive_batch_interval', 5.0),
                    enable_postgres=self.config.get('enable_postgres_archive', False),
                    fallback_to_parquet=True
                )
                self.logger.info("✅ Historical archive initialized (Tier 2)")
            except Exception as e:
                self.logger.warning(f"⚠️ Historical archive unavailable: {e}. Continuing without archival.")
                self.historical_archive = None
        else:
            self.logger.debug("ℹ️ Historical archive disabled (module not available)")

        # ✅ Spoofing detection simplified - uses Redis-based detection only (no detector object)
        self.spoofing_detector = None
        self.spoofing_enabled = False

        # News cache - symbol -> news data
        self.news_cache = {}
        self.news_cache_expiry = 1800  # 30 minutes
        self.last_news_cleanup = time.time()

        # Spoofing cache - symbol -> spoofing alert data
        self.spoofing_cache = {}
        self.spoofing_cache_expiry = 300  # 5 minutes (shorter than news)
        self.last_spoofing_cleanup = time.time()

        # Spoofing blocks - symbol -> block expiry time
        self.spoofing_blocks = {}

        # ✅ SOLUTION 4: Use UnifiedRedisManager for single connection pool
        # This prevents connection pool exhaustion by using one shared pool for the process
        if redis_client:
            self.redis_client = redis_client
            # If wrapped, unwrap to check/wrap again if needed or use as is
            if hasattr(self.redis_client, 'redis'):
                 # It's already wrapped or is a wrapper
                 pass
        else:
            # Create shared client for this process
            self.redis_client = get_redis_client(process_name="data_pipeline", db=1)
            
        # Ensure it has store_by_data_type
        if not hasattr(self.redis_client, 'store_by_data_type'):
             self.redis_client = self._wrap_redis_client(self.redis_client)
            
        # Reuse same client for realtime and news to minimize connections
        self.realtime_client = self.redis_client
        self.news_client = self.redis_client
        self.consumer_manager: Optional[ConsumerGroupManager] = None
        self.consumer_name: Optional[str] = None
        try:
            self.consumer_manager = ConsumerGroupManager(
                redis_client=self.redis_client,
                group_name=STREAM_CONSUMER_GROUP,
                stream_key=STREAM_NAME_TICKS
            )
            generated_name = f"pipeline_worker_{self.worker_id}_{int(time.time())}"
            self.consumer_name = self.consumer_manager.create_consumer(generated_name)
            self.logger.info(f"✅ Registered pipeline worker as consumer: {self.consumer_name}")
        except Exception as consumer_exc:
            self.consumer_name = f"pipeline_worker_{self.worker_id}"
            self.logger.warning(f"⚠️ ConsumerGroupManager unavailable: {consumer_exc}")
        self._setup_auto_restart()
        
        # Initialize VIX utils to fetch regime/value published to Redis
        self.vix_utils = None
        self._cached_vix_snapshot = None
        self._last_vix_fetch = 0.0
        self.vix_refresh_interval = int(self.config.get("vix_refresh_seconds", 30))
        try:
            # ✅ FIXED: Use get_vix_utils() factory function instead of direct instantiation
            from shared_core.utils.vix_utils import get_vix_utils
            self.vix_utils = get_vix_utils(redis_client=self.realtime_client)
            self.logger.info("✅ VIXUtils initialized for Redis-based VIX retrieval")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to initialize VIXUtils: {e}")
            self.vix_utils = None
        
        # DataPipeline fetches all data from Redis (indicators, Greeks, volume_ratio, price_change, microstructure, etc.)
        # Full data is preserved after clean_tick and sent downstream with all fields
        self.logger.info("✅ DataPipeline: Fetching all data from Redis and preserving full tick data downstream")

        # ✅ UNIFIED STORAGE: Initialize UnifiedDataStorage once and reuse (prevents connection pool exhaustion)
        try:
            from shared_core.redis_clients.unified_data_storage import get_unified_storage
            self._unified_storage = get_unified_storage(redis_client=self.realtime_client)
            self.logger.info("✅ UnifiedDataStorage initialized for data_pipeline")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to initialize UnifiedDataStorage: {e}")
            self._unified_storage = None

        # ✅ Spoofing detection removed - simplified architecture

        # Partial message buffers to recover from split/corrupt pubsub payloads
        self.partial_message_buffers: Dict[str, str] = {}
        self.max_partial_buffer = int(self.config.get("max_partial_buffer", 8192))

    def _wrap_redis_client(self, raw_client):
        """
        ✅ FIXED: Wrap raw Redis client to add store_by_data_type method.
        This allows using store_by_data_type with RedisClientFactory clients.
        """
        from shared_core.redis_clients.redis_client import RedisClientFactory
        from shared_core.redis_clients.redis_client import get_database_for_data_type, get_ttl_for_data_type
        
        class RedisClientWrapper:
            def __init__(self, client):
                self.client = client
                # Cache DB clients for different databases
                self._db_clients = {}
            
            def __getattr__(self, name):
                # Delegate all other methods to the underlying client
                return getattr(self.client, name)
            
            def _get_client_for_db(self, db_num):
                """Get client for specific database"""
                if db_num not in self._db_clients:
                    # ✅ FIXED: Use UnifiedRedisManager for centralized connection management
                    db_client = get_redis_client(process_name="data_pipeline", db=db_num)
                    self._db_clients[db_num] = db_client
                return self._db_clients[db_num]
            
            def store_by_data_type(self, data_type: str, key: str, value: str, ttl: Optional[int] = None):
                """Store data in appropriate database based on type"""
                try:
                    db_num = get_database_for_data_type(data_type)
                    if ttl is None:
                        ttl = get_ttl_for_data_type(data_type)
                    
                    db_client = self._get_client_for_db(db_num)
                    
                    # Store with TTL
                    if ttl and ttl > 0:
                        db_client.setex(key, ttl, value)
                    else:
                        db_client.set(key, value)
                    
                    return True
                except Exception as e:
                    self.logger.error(f"❌ Error storing {key} via store_by_data_type: {e}")
                    return False
            
            def retrieve_by_data_type(self, key: str, data_type: str):
                """Retrieve data from appropriate database based on type"""
                try:
                    db_num = get_database_for_data_type(data_type)
                    db_client = self._get_client_for_db(db_num)
                    return db_client.get(key)
                except Exception as e:
                    self.logger.debug(f"Error retrieving {key} via retrieve_by_data_type: {e}")
                    return None
        
        wrapper = RedisClientWrapper(raw_client)
        if hasattr(self, "logger"):
            wrapper.logger = self.logger
        return wrapper

    def _load_symbol_mapping(self):
        """
        Preload instrument token ➜ symbol mapping from the generated JSON file.
        This avoids repeated InstrumentMapper instantiations on every tick.
        """
        mapping_path = getattr(self, "_symbol_mapping_path", None)
        self._symbol_cache = {}
        if not mapping_path:
            self._symbol_mapping_loaded = False
            return
        
        try:
            with open(mapping_path, "r") as fh:
                payload = json.load(fh)
        except FileNotFoundError:
            if hasattr(self, "logger"):
                self.logger.warning(f"Token symbol mapping file not found: {mapping_path}")
            self._symbol_mapping_loaded = False
            return
        except Exception as exc:
            if hasattr(self, "logger"):
                self.logger.error(f"Failed to load token symbol mapping {mapping_path}: {exc}")
            self._symbol_mapping_loaded = False
            return
        
        # Handle formats: new intraday crawler file, scanner {"tokens": {...}}, or flat dict
        token_entries: Optional[Dict[str, Any]] = None
        if isinstance(payload, dict):
            instrument_info = payload.get("instrument_info")
            if isinstance(instrument_info, dict):
                token_entries = instrument_info
            else:
                tokens_section = payload.get("tokens")
                if isinstance(tokens_section, dict):
                    token_entries = tokens_section
                else:
                    token_entries = payload
        else:
            token_entries = payload

        if not isinstance(token_entries, dict):
            self.logger.warning(f"Invalid token symbol mapping format at {mapping_path}")
            self._symbol_mapping_loaded = False
            return
        
        loaded = 0
        for token_key, entry in token_entries.items():
            try:
                token_int = int(token_key)
            except (TypeError, ValueError):
                continue
            
            if isinstance(entry, dict):
                symbol_value = entry.get("symbol")
                if not symbol_value:
                    key_field = entry.get("key")
                    if isinstance(key_field, str) and ":" in key_field:
                        symbol_value = key_field.split(":", 1)[1]
                    else:
                        symbol_value = key_field or entry.get("name")
            else:
                symbol_value = entry
            
            if not symbol_value:
                continue
            
            self._symbol_cache[token_int] = symbol_value
            loaded += 1
        
        self._symbol_mapping_loaded = loaded > 0
        if self._symbol_mapping_loaded:
            self.logger.info(
                "✅ Preloaded %d token→symbol entries from %s",
                loaded,
                mapping_path,
            )
        else:
            self.logger.warning(f"Token symbol mapping file had no usable entries: {mapping_path}")

    def _get_symbol_from_cache(self, instrument_token):
        """Return cached symbol for the given instrument token if available."""
        if instrument_token is None:
            return None
        if not hasattr(self, "_symbol_cache"):
            return None
        try:
            token_int = int(instrument_token)
        except (TypeError, ValueError):
            return None
        return self._symbol_cache.get(token_int)

    def _setup_logging(self):
        """Setup logging with rotation for data pipeline"""
        log_timestamp = get_current_ist_time()
        log_dir = Path(f"logs/data_pipeline/{log_timestamp.strftime('%Y/%m/%d')}")
        log_dir.mkdir(parents=True, exist_ok=True)
        log_filename = log_dir / f"{log_timestamp.strftime('%H%M%S')}_pipeline.log"

        self.logger = logging.getLogger("DataPipeline")
        self.logger.setLevel(logging.WARNING)

        # Remove existing handlers to avoid duplicates
        self.logger.handlers = []

        # Rotating file handler
        handler = RotatingFileHandler(
            str(log_filename),
            maxBytes=self.max_log_size,
            backupCount=self.log_backup_count,
        )
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # Also add console handler for critical errors
        console = logging.StreamHandler()
        console.setLevel(logging.ERROR)
        console.setFormatter(formatter)
        self.logger.addHandler(console)

    def _setup_auto_restart(self):
        """Setup health checks and auto-restart for crash safety."""
        try:
            import signal

            def handle_sigterm(signum, frame):
                self.logger.info(f"🛑 Worker {self.worker_id} received SIGTERM, performing cleanup...")
                self._cleanup_on_exit()
                sys.exit(0)

            signal.signal(signal.SIGTERM, handle_sigterm)
        except Exception as exc:
            # Signals may not be available in some contexts (e.g., threads)
            self.logger.debug(f"Signal handler setup skipped: {exc}")
        atexit.register(self._cleanup_on_exit)

    def _cleanup_on_exit(self):
        """Cleanup when worker exits (even on crash)."""
        if getattr(self, "_consumer_cleanup_done", False):
            return
        self._consumer_cleanup_done = True
        try:
            if getattr(self, "consumer_name", None) and getattr(self, "redis_client", None):
                self.redis_client.xgroup_delconsumer(
                    STREAM_NAME_TICKS,
                    STREAM_CONSUMER_GROUP,
                    self.consumer_name
                )
                self.logger.info(f"🧹 Worker {self.worker_id} removed consumer {self.consumer_name}")
        except Exception as exc:
            self.logger.debug(f"Consumer cleanup skipped: {exc}")
        if getattr(self, "consumer_manager", None):
            try:
                self.consumer_manager.stop()
            except Exception as exc:
                self.logger.debug(f"Consumer manager stop skipped: {exc}")

    def start(self):
        """Start the data pipeline"""
        if self.running:
            self.logger.warning("Data pipeline is already running")
            return

        self.logger.info("🚀 Starting data pipeline...")
        # perf_probe removed - no longer using performance monitoring monkey patches
        self.perf = None
        
        # Start consuming in a separate thread
        import threading
        # Use streams instead of Pub/Sub for better performance
        use_streams = self.config.get("use_redis_streams", True)  # Default to streams
        if use_streams:
            self.consumer_thread = threading.Thread(target=self.start_consuming_streams, daemon=True)
            self.logger.info("📡 Using Redis Streams (XREADGROUP) for data ingestion")
        else:
            self.consumer_thread = threading.Thread(target=self.start_consuming, daemon=True)
            self.logger.info("📡 Using Redis Pub/Sub for data ingestion")
        self.consumer_thread.start()
        self._start_pipeline_monitor()
    
    def _update_stream_poll(self, source: str, has_data: bool):
        """Track last time we successfully polled the Redis stream."""
        current_time = time.time()
        self.last_stream_poll = current_time
        if has_data:
            self.last_poll_with_data = current_time
            self._idle_state = False

    def _start_pipeline_monitor(self):
        """Start background thread for blockage detection."""
        if self._pipeline_monitor_thread and self._pipeline_monitor_thread.is_alive():
            return
        self._pipeline_monitor_running = True
        self._pipeline_monitor_thread = threading.Thread(
            target=self._pipeline_monitor_loop,
            name="PipelineMonitor",
            daemon=True,
        )
        self._pipeline_monitor_thread.start()
        self.logger.info(
            "🩺 Pipeline monitor running (threshold=%ss interval=%ss)",
            self.pipeline_blockage_threshold,
            self.pipeline_monitor_interval,
        )

    def _pipeline_monitor_loop(self):
        """Detect symbols that are stuck longer than the threshold."""
        while self._pipeline_monitor_running:
            try:
                now = time.time()
                for symbol, last_time in list(self.last_processed.items()):
                    if not symbol:
                        continue
                    blocked_for = now - last_time
                    if blocked_for >= self.pipeline_blockage_threshold:
                        self.logger.error(
                            "🚨 Pipeline blocked for %s for %.1fs",
                            symbol,
                            blocked_for,
                        )
                        self._recover_pipeline(symbol)
                        # Avoid repetitive alerts until new data arrives
                        self.last_processed[symbol] = now
                time.sleep(self.pipeline_monitor_interval)
            except Exception as exc:
                self.logger.error(f"Pipeline monitor error: {exc}")
                time.sleep(self.pipeline_monitor_interval)

    def _record_pipeline_progress(self, symbol: Optional[str], start_time: float):
        """Update per-symbol timestamps for blockage detection."""
        duration = time.time() - start_time
        self.processing_times.append(duration)
        if symbol:
            self.last_processed[symbol] = time.time()

    def _recover_pipeline(self, symbol: str):
        """Best-effort recovery for a blocked symbol."""
        try:
            acked = self._ack_and_skip_symbol(symbol)
            reset = self._reset_worker_state(symbol)
            models = self._reinitialize_models()
            self._pipeline_recovery_count += 1
            self._last_pipeline_recovery = time.time()
            self.logger.info(
                "✅ Pipeline recovered for %s (acked=%s reset_state=%s reinit_models=%s total_recoveries=%s)",
                symbol,
                acked,
                reset,
                models,
                self._pipeline_recovery_count,
            )
        except Exception as exc:
            self.logger.error(f"❌ Failed to recover pipeline for {symbol}: {exc}")

    def _ack_and_skip_symbol(self, symbol: str) -> int:
        """Ack a small batch of pending entries to unstick the consumer group."""
        if not self.redis_client:
            return 0
        try:
            pending_entries = self.redis_client.xpending_range(
                STREAM_NAME_TICKS,
                STREAM_CONSUMER_GROUP,
                min="-",
                max="+",
                count=20,
            )
        except Exception as exc:
            self.logger.debug(f"Pending inspection failed for {symbol}: {exc}")
            return 0

        acked = 0
        for entry in pending_entries or []:
            entry_id = entry.get("message_id") or entry.get("id")
            if not entry_id:
                continue
            try:
                self.redis_client.xack(STREAM_NAME_TICKS, STREAM_CONSUMER_GROUP, entry_id)
                acked += 1
            except Exception as exc:
                self.logger.debug(f"Failed to ack {entry_id}: {exc}")
        if acked:
            self.logger.warning(
                "⚠️ Acked %d pending entries while recovering %s",
                acked,
                symbol,
            )
        return acked

    def _reset_worker_state(self, symbol: str) -> bool:
        """Clear cached state for a symbol so new ticks aren't blocked."""
        removed = False
        if symbol in self._cumulative_volume_tracker:
            self._cumulative_volume_tracker.pop(symbol, None)
            removed = True
        if symbol in self.last_processed:
            self.last_processed.pop(symbol, None)
            removed = True
        return removed

    def _reinitialize_models(self) -> bool:
        """Ask downstream components to refresh if they support it."""
        reloaded = False
        try:
            if self.pattern_detector and hasattr(self.pattern_detector, "reload_models"):
                self.pattern_detector.reload_models()
                reloaded = True
        except Exception as exc:
            self.logger.debug(f"Model reload failed: {exc}")
        return reloaded

    def _cleanup_stream_consumers(self):
        """Remove idle consumers with pending work to prevent backlog."""
        stream_name = STREAM_NAME_TICKS
        group_name = STREAM_CONSUMER_GROUP
        if not self.redis_client:
            return
        try:
            consumers = self.redis_client.xinfo_consumers(stream_name, group_name)
        except redis.ResponseError as exc:
            if "NOGROUP" in str(exc).upper():
                self.logger.warning(
                    "⚠️ Consumer group %s does not exist on %s", group_name, stream_name
                )
            else:
                self.logger.error(
                    "❌ Failed to inspect consumers for %s/%s: %s", stream_name, group_name, exc
                )
            return
        except Exception as exc:
            self.logger.error(
                "❌ Unexpected error inspecting consumers for %s/%s: %s",
                stream_name,
                group_name,
                exc,
            )
            return

        dead_consumers = []
        for consumer in consumers or []:
            idle_ms = consumer.get("idle", 0)
            pending = consumer.get("pending", 0)
            consumer_name = consumer.get("name")
            if idle_ms >= STREAM_CONSUMER_IDLE_THRESHOLD_MS and pending > 0:
                dead_consumers.append(consumer_name)
                self.logger.warning(
                    "🚨 Dead consumer detected: %s (idle=%sms pending=%s)",
                    consumer_name,
                    idle_ms,
                    pending,
                )

        for consumer_name in dead_consumers:
            try:
                self.redis_client.xgroup_delconsumer(stream_name, group_name, consumer_name)
                self.logger.info(
                    "✅ Removed dead consumer %s from %s/%s",
                    consumer_name,
                    stream_name,
                    group_name,
                )
            except Exception as exc:
                self.logger.error(
                    "❌ Failed to remove consumer %s from %s/%s: %s",
                    consumer_name,
                    stream_name,
                    group_name,
                    exc,
                )

        if dead_consumers:
            self.logger.warning(
                "🔄 Removed %d idle consumers from %s/%s. Pending consumers should rebalance.",
                len(dead_consumers),
                stream_name,
                group_name,
            )

    def start_consuming_streams(self):
        """Consume ticks directly without intermediate Redis stream buffering."""
        self.running = True
        self.last_stream_poll = time.time()
        next_cleanup = time.time() + STREAM_CONSUMER_CLEANUP_INTERVAL

        while self.running:
            try:
                if time.time() >= next_cleanup:
                    self._cleanup_stream_consumers()
                    next_cleanup = time.time() + STREAM_CONSUMER_CLEANUP_INTERVAL

                messages = self._read_ticks_optimized()
                if not messages:
                    # ✅ FIXED: Reset retry count on successful read (no error)
                    if hasattr(self, '_stream_connection_retry_count'):
                        self._stream_connection_retry_count = 0
                    time.sleep(0.01)
                    continue

                # ✅ FIXED: Reset retry count on successful read (got messages)
                if hasattr(self, '_stream_connection_retry_count'):
                    self._stream_connection_retry_count = 0
                
                self._update_stream_poll("message", True)

                for stream_name_bytes, stream_messages in messages:
                    stream_name = stream_name_bytes.decode("utf-8") if isinstance(stream_name_bytes, bytes) else stream_name_bytes
                    for message_id_bytes, message_data in stream_messages:
                        message_id = message_id_bytes.decode("utf-8")
                        try:
                            parsed = self.parse_stream_message(stream_name, message_data)
                            if parsed:
                                symbol = self._get_unified_symbol(parsed) or "UNKNOWN"
                                self._tick_flow_log(
                                    "STREAM_MESSAGE",
                                    symbol,
                                    stream=stream_name,
                                    message_id=message_id,
                                )
                                # ✅ CRITICAL: Filter out stale ticks (> 5 minutes old) before processing
                                if self._should_skip_stream_tick(stream_name, parsed):
                                    self.redis_client.xdel(stream_name, message_id)
                                    self.logger.debug(f"🗑️ Deleted stale tick message {message_id} for {symbol} from {stream_name}")
                                    continue
                                self._tick_flow_log(
                                    "STREAM_ACCEPT",
                                    symbol,
                                    stream=stream_name,
                                    message_id=message_id,
                                )
                                start_time = time.time()
                                self._process_market_tick(parsed)
                                self._record_pipeline_progress(symbol, start_time)
                        finally:
                            self.redis_client.xack(stream_name, "data_pipeline_group", message_id)
            except redis.ConnectionError as conn_err:
                error_msg = str(conn_err).lower()
                if "too many connections" in error_msg:
                    # ✅ FIXED: Handle "Too many connections" in stream consumption
                    if not hasattr(self, '_stream_connection_retry_count'):
                        self._stream_connection_retry_count = 0
                    
                    self._stream_connection_retry_count += 1
                    backoff_time = min(2 ** self._stream_connection_retry_count, 30)  # Max 30 seconds
                    
                    self.logger.error(f"🚨 TOO MANY CONNECTIONS in stream consumption (attempt {self._stream_connection_retry_count})")
                    self.logger.error(f"   Stream: ticks:intraday:processed exists with data")
                    self.logger.error(f"   Consumer group: data_pipeline_group has lag")
                    self.logger.error(f"   Waiting {backoff_time}s before retry...")
                    
                    time.sleep(backoff_time)
                    # Reset retry count on successful read (will happen in next iteration)
                    continue
                else:
                    # Other connection errors
                    self.logger.error(f"Redis connection error in stream consumption: {conn_err}")
                    time.sleep(1)
                    continue
            except Exception as exc:
                error_msg = str(exc).lower()
                if "too many connections" in error_msg:
                    # Handle "Too many connections" in any exception
                    if not hasattr(self, '_stream_connection_retry_count'):
                        self._stream_connection_retry_count = 0
                    
                    self._stream_connection_retry_count += 1
                    backoff_time = min(2 ** self._stream_connection_retry_count, 30)
                    
                    self.logger.error(f"🚨 TOO MANY CONNECTIONS: {exc}")
                    self.logger.error(f"   Waiting {backoff_time}s before retry...")
                    time.sleep(backoff_time)
                    continue
                else:
                    self.logger.error(f"Stream consumption error: {exc}")
                    time.sleep(0.1)
    
    def parse_stream_message(self, stream_name: str, message_data) -> Optional[Dict]:
        """
        Simple stream message parser that handles Redis bytes decoding.
        """
        try:
            # Normalize stream name
            if isinstance(stream_name, bytes):
                stream_name = stream_name.decode('utf-8')
            
            # Handle empty messages
            if not message_data:
                return None
            
            # Convert message to dict format
            parsed_dict = {}
            
            # Handle Redis stream format: dict with bytes keys/values
            if isinstance(message_data, dict):
                for key, value in message_data.items():
                    # Decode key
                    if isinstance(key, bytes):
                        key = key.decode('utf-8')
                    
                    # Decode value if it's bytes
                    if isinstance(value, bytes):
                        try:
                            # Try to decode as JSON first
                            decoded = value.decode('utf-8')
                            try:
                                value = json.loads(decoded)
                            except json.JSONDecodeError:
                                value = decoded  # Use as plain string
                        except UnicodeDecodeError:
                            value = None  # Invalid bytes
                    
                    parsed_dict[key] = value
            else:
                self.logger.warning(f"Unexpected message type: {type(message_data)}")
                return None
            
            # Extract data if it's in the crawler format
            if 'data' in parsed_dict and isinstance(parsed_dict['data'], str):
                try:
                    data = json.loads(parsed_dict['data'])
                    if isinstance(data, dict):
                        # Merge with parsed_dict (data takes precedence)
                        return {**parsed_dict, **data}
                except json.JSONDecodeError:
                    pass
            
            return parsed_dict
            
        except Exception as e:
            self.logger.error(f"Error parsing stream message: {e}")
            return None

    def _should_skip_stream_tick(self, stream_name: str, tick: dict) -> bool:
        """Return True if the stream tick is older than configured threshold."""
        # ✅ FIXED: Only monitor ticks:intraday:processed (the stream intraday crawler publishes to)
        monitored_streams = {"ticks:intraday:processed"}
        if stream_name not in monitored_streams:
            return False

        # ✅ UPDATED: Default to 5 minutes (300 seconds) instead of 15 minutes
        max_age_seconds = int(self.config.get("stream_stale_skip_seconds", 300))
        if max_age_seconds <= 0:
            return False

        try:
            ts_seconds = None
            if tick.get("exchange_timestamp_ms"):
                ts_seconds = float(tick["exchange_timestamp_ms"]) / 1000.0
            elif tick.get("exchange_timestamp"):
                raw_ts = tick["exchange_timestamp"]
                if isinstance(raw_ts, (int, float)):
                    ts_seconds = float(raw_ts)
                else:
                    ts_seconds = TimestampNormalizer.to_epoch_ms(raw_ts) / 1000.0
            elif tick.get("timestamp_ms"):
                ts_seconds = float(tick["timestamp_ms"]) / 1000.0
            elif tick.get("timestamp"):
                raw_ts = tick["timestamp"]
                if isinstance(raw_ts, (int, float)):
                    ts_seconds = float(raw_ts)
                    # ✅ FIXED: Correct epoch length detection (same as intraday_crawler)
                    if ts_seconds >= 1e15:
                        # Nanoseconds - convert to seconds
                        ts_seconds = ts_seconds / 1_000_000_000.0
                    elif ts_seconds >= 1e10:
                        # Milliseconds - convert to seconds
                        ts_seconds = ts_seconds / 1000.0
                    # else: Already in seconds (no conversion needed)
                else:
                    ts_seconds = TimestampNormalizer.to_epoch_ms(raw_ts) / 1000.0

            if not ts_seconds:
                return False

            import time

            age = time.time() - ts_seconds
            if age > max_age_seconds:
                # ✅ FIXED: Use unified symbol extraction method
                symbol = self._get_unified_symbol(tick) or "UNKNOWN"
                self.logger.warning(
                    f"⚠️ [STREAM_SKIP] {symbol} stream tick skipped (age={age:.1f}s, threshold={max_age_seconds}s, source={stream_name})"
                )
                if hasattr(self, "stats"):
                    self.stats["stream_ticks_skipped"] = self.stats.get("stream_ticks_skipped", 0) + 1
                self._tick_flow_log(
                    "STREAM_SKIP",
                    symbol,
                    stream=stream_name,
                    age=f"{age:.1f}",
                    threshold=max_age_seconds,
                )
                return True
            return False
        except Exception:
            return False

    def _parse_intraday_processed_message(self, parsed):
        """
        ✅ SPECIFIC FIX: Parse ticks:intraday:processed messages
        Handles multiple possible field names and formats
        """
        try:
            # ✅ FIX: Check if the message ITSELF is the tick data (flat fields)
            if 'symbol' in parsed and 'last_price' in parsed:
                 self.logger.debug(f"🔍 Found flat message format with symbol: {parsed.get('symbol')}")
                 return parsed

            # ✅ FIX 4: Try multiple possible data field names
            data_candidates = ['data', 'tick_data', 'payload', 'tick']

            for field in data_candidates:
                if field in parsed:
                    data_value = parsed[field]
                    self.logger.debug(f"🔍 Trying field '{field}': {type(data_value)}")

                    # Handle the data value (could be string, bytes, or already dict)
                    tick_data = None

                    if isinstance(data_value, dict):
                        tick_data = data_value
                    elif isinstance(data_value, (str, bytes)):
                        # Convert to string if bytes
                        if isinstance(data_value, bytes):
                            try:
                                data_str = data_value.decode('utf-8')
                            except UnicodeDecodeError:
                                self.logger.error(f"Failed to decode bytes for field {field}")
                                continue
                        else:
                            data_str = data_value

                        # Parse JSON string
                        try:
                            tick_data = json.loads(data_str)
                        except json.JSONDecodeError as e:
                            self.logger.error(f"JSON parse error for field {field}: {e}")
                            continue
                    else:
                        self.logger.warning(f"Unexpected data type for field {field}: {type(data_value)}")
                        continue

                    if tick_data and isinstance(tick_data, dict):
                        # ✅ FIX 5: Ensure symbol is present using unified method
                        if 'symbol' not in tick_data or not tick_data['symbol']:
                            symbol = self._get_unified_symbol(tick_data)
                            if symbol:
                                tick_data['symbol'] = symbol

                        self.logger.debug(f"✅ Successfully parsed tick for: {tick_data.get('symbol', 'UNKNOWN')}")
                        return tick_data

            # If we get here, no valid data field was found
            self.logger.warning(
                f"❌ No valid data field found in message. Available fields: {list(parsed.keys())}\n"
                f"Sample values: {{ { {k: str(v)[:100] for k, v in list(parsed.items())[:3]} } }}"
            )
            return None

        except Exception as e:
            self.logger.error(f"Error parsing intraday processed message: {e}")
            return None

    def _extract_symbol_from_parsed(self, tick_data):
        """
        Extract symbol from parsed data with multiple fallbacks.
        
        ✅ DEPRECATED: Use _get_unified_symbol() instead.
        This method now delegates to _get_unified_symbol() for consistency.
        """
        symbol = self._get_unified_symbol(tick_data)
        return symbol if symbol else 'UNKNOWN'
    
    def _read_ticks_optimized(self):
        """
        Optimized tick reading using XREADGROUP (replaces Pub/Sub polling).
        Reads from Redis streams with consumer groups for reliability.
        """
        # Note: os is already imported at the top of the file
        
        # Get Redis client for DB 1 (streams)
        if not hasattr(self, 'realtime_client') or not self.realtime_client:
            return None
        
        try:
            # Use dedicated consumer group for data pipeline
            group_name = STREAM_CONSUMER_GROUP
            consumer_name = self.consumer_name or f"pipeline_worker_{self.worker_id}"
            
            # The intraday crawler publishes to ticks:intraday:processed, NOT ticks:raw:binary
            stream_key = getattr(self.consumer_manager, "stream_key", STREAM_NAME_TICKS)
            if not stream_key:
                stream_key = RedisKeyStandards.live_processed_stream()
            
            # Ensure consumer group exists for the processed tick stream
            try:
                groups = self.redis_client.xinfo_groups(stream_key)
                group_exists = any(g.get('name') == group_name for g in groups)
                if not group_exists:
                    self.redis_client.xgroup_create(
                        name=stream_key,
                        groupname=group_name,
                        id='$',
                        mkstream=True
                    )
                    self.logger.info(f"✅ Created consumer group '{group_name}' for stream '{stream_key}'")
            except Exception as e:
                error_str = str(e).lower()
                if "busygroup" in error_str or "already exists" in error_str:
                    pass
                else:
                    self.logger.debug(f"Consumer group check for '{stream_key}': {e}")
            
            # Read from the processed stream using XREADGROUP (blocking, efficient)
            messages = self.realtime_client.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams={stream_key: '>'},  # ✅ FIXED: Only read from ticks:intraday:processed
                count=25,      # Process 25 messages per batch (adjust based on load)
                block=1000,    # Block for 1 second (efficient blocking, no polling)
                noack=False    # Require explicit acknowledgment
            )
            
            # ✅ DEBUG: Log stream read details
            if messages:
                current_time_ms = time.time() * 1000
                for stream_name, stream_messages in messages:
                    for message_id_bytes, _ in stream_messages:
                        message_id = message_id_bytes.decode('utf-8')
                        # Extract timestamp from message ID (format: timestamp_ms-sequence)
                        try:
                            msg_timestamp_ms = int(message_id.split('-')[0])
                            msg_age_seconds = (current_time_ms - msg_timestamp_ms) / 1000.0
                            self.logger.debug(f"📥 [STREAM_READ] {stream_name} - message_id={message_id}, msg_age={msg_age_seconds:.1f}s")
                        except:
                            self.logger.debug(f"📥 [STREAM_READ] {stream_name} - message_id={message_id}")
            
            return messages if messages else None
            
        except redis.ConnectionError:
            raise  # Re-raise connection errors
        except Exception as e:
            error_str = str(e).lower()
            if "no such key" in error_str or "no such stream" in error_str:
                # Stream not ready yet, return None (will retry)
                return None
            elif "unknown consumer group" in error_str:
                # Only create group for ticks:intraday:processed (the stream we actually use)
                # Use DatabaseAwareKeyBuilder for stream key
                from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
                stream_key = getattr(self.consumer_manager, "stream_key", DatabaseAwareKeyBuilder.live_processed_stream())
                try:
                    self.realtime_client.xgroup_create(
                        name=stream_key,
                        groupname=group_name,
                        id='$',  # Start from latest
                        mkstream=True
                    )
                    # Retry read
                    return self._read_ticks_optimized()
                except:
                    return None
            else:
                # Log non-critical errors
                self.logger.debug(f"Stream read error (non-critical): {e}")
                return None
    
    def _process_binary_stream_tick(self, stream_data: dict):
        """Process binary stream tick (from ticks:raw:binary)"""
        try:
            # Binary stream has: binary_data (base64), instrument_token, timestamp, mode, source
            # For now, we'll skip binary processing and wait for processed stream
            # Or you can decode binary_data here if needed
            self.logger.debug(f"Received binary stream tick: {stream_data.get('instrument_token')}")
            # TODO: Add binary decoding logic if needed
        except Exception as e:
            self.logger.error(f"Error processing binary stream tick: {e}")

    def start_consuming(self):
        """Start consuming from Redis channels"""
        self.running = True

        # Subscribe to channels (store as instance variable for reconnection)
        # ✅ STANDARD: Only subscribe to index/news/alert channels (Pub/Sub)
        self.channels = [
            "index:NSE:NIFTY 50",  # NIFTY 50 index data
            "index:NSE:NIFTY BANK",  # BANK NIFTY index data
            "index:NSEIX:GIFT NIFTY",  # GIFT NIFTY index data
            "index:NSE:INDIA VIX",  # INDIA VIX data
            "market_data.news",  # News data from gift_nifty_gap.py
            "premarket.orders",  # Pre-market data
            "alerts.manager",  # Spoofing alerts from crawler
        ]
        
        # Store subscribed channels for reconnection
        self.subscribed_channels = self.channels.copy()

        try:
            # Ensure Redis client is connected
            if not hasattr(self.redis_client, 'pubsub'):
                raise AttributeError("Redis client does not support pubsub")
            
            # Create pubsub instance bound to realtime DB client to avoid DB switching side-effects
            # Use redis_config to get the correct database for stream_data
            try:
                from shared_core.redis_clients.redis_client import get_database_for_data_type
                realtime_db = get_database_for_data_type("stream_data")
            except Exception:
                realtime_db = 1
            
            # Store realtime_db as instance variable for reconnection
            self.realtime_db = realtime_db
            
            # Use cached optimized process-specific client (always use cached, no get_client() calls)
            if realtime_db == 1:
                dedicated_client = self.realtime_client
            else:
                # For other databases, get cached optimized client for that DB
                dedicated_client = UnifiedRedisManager.get_client(process_name="data_pipeline", database=realtime_db)
            if dedicated_client is None:
                raise RuntimeError("No Redis client available for Pub/Sub")
            self.pubsub = dedicated_client.pubsub()
            self.pubsub.subscribe(*self.channels)
            self.logger.info(f"📡 Subscribed to channels: {self.channels}")


            # Start listening with timeout handling
            message_count = 0
            consecutive_errors = 0
            max_consecutive_errors = 5
            
            while self.running:
                try:
                    # Update heartbeat to show thread is alive
                    self.last_heartbeat = time.time()

                    # Check if pubsub is still valid before reading
                    if not self.pubsub or not hasattr(self.pubsub, 'get_message'):
                        self.logger.warning("⚠️ Pubsub connection lost, attempting to reconnect...")
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            self.logger.error("❌ Too many pubsub connection failures, restarting...")
                            break
                        
                        # Try to recreate pubsub connection
                        try:
                            # Use cached optimized process-specific client (no get_client() calls)
                            if self.realtime_db == 1:
                                dedicated_client = self.realtime_client
                            else:
                                # Get cached optimized client for this database
                                dedicated_client = get_cached_redis_client(db=self.realtime_db)
                            if dedicated_client:
                                # Test connection
                                dedicated_client.ping()
                                # Recreate pubsub and resubscribe
                                if self.pubsub:
                                    try:
                                        self.pubsub.close()
                                    except:
                                        pass
                                self.pubsub = dedicated_client.pubsub()
                                self.pubsub.subscribe(*self.channels)
                                self.logger.info(f"✅ Re-subscribed to {len(self.channels)} channels after reconnection")
                                consecutive_errors = 0
                            else:
                                raise RuntimeError("No Redis client available")
                        except Exception as recon_err:
                            self.logger.warning(f"⚠️ Reconnection attempt failed: {recon_err}, retrying...")
                            time.sleep(2)
                            continue

                    # ✅ OPTIMIZED: Use XREADGROUP instead of Pub/Sub polling
                    # This replaces inefficient get_message(timeout=1.0) polling
                    try:
                        messages = self._read_ticks_optimized()
                        if messages:
                            for stream_name, stream_messages in messages:
                                for message_id_bytes, message_data in stream_messages:
                                    message_id = message_id_bytes.decode('utf-8') if isinstance(message_id_bytes, bytes) else str(message_id_bytes)
                                    
                                    # ✅ DEBUG: Calculate stream message age
                                    try:
                                        msg_timestamp_ms = int(message_id.split('-')[0])
                                        current_time_ms = time.time() * 1000
                                        stream_msg_age = (current_time_ms - msg_timestamp_ms) / 1000.0
                                        self.logger.debug(f"📨 [STREAM_PROCESS] stream={stream_name}, msg_id={message_id}, stream_age={stream_msg_age:.1f}s")
                                    except:
                                        pass
                                    
                                    try:
                                        # Parse stream message
                                        parsed_data = self.parse_stream_message(stream_name, message_data)
                                        if parsed_data:
                                            if self._should_skip_stream_tick(stream_name, parsed_data):
                                                # ACK moved to finally block
                                                continue
                                            # Convert to Pub/Sub message format for compatibility
                                            fake_message = {
                                                "type": "message",
                                                "channel": stream_name.encode() if isinstance(stream_name, str) else stream_name,
                                                "data": json.dumps(parsed_data) if isinstance(parsed_data, dict) else str(parsed_data)
                                            }
                                            message_count += 1
                                            self._process_message(fake_message)
                                    except Exception as msg_err:
                                        # 🚨 POISON PILL HANDLING: Log error but still ACK
                                        self.logger.error(f"🚨 POISON PILL: Error processing stream message {message_id}: {msg_err}")
                                        self.logger.debug(f"   Failed data: {message_data}")
                                        # ACK moved to finally block - allows processing to continue
                                    finally:
                                        # ✅ GUARANTEED ACK: Always acknowledge to prevent message blocking
                                        if hasattr(self, 'realtime_client') and self.realtime_client:
                                            try:
                                                self.realtime_client.xack(stream_name, 'data_pipeline_group', message_id)
                                            except Exception as ack_err:
                                                self.logger.debug(f"ACK error (non-critical): {ack_err}")
                            
                            consecutive_errors = 0  # Reset on successful batch
                            if message_count <= 5 or message_count % 100 == 0:
                                # Count total messages in batch
                                total_msgs = sum(len(stream_data[1]) if len(stream_data) >= 2 else 0 for stream_data in messages)
                                self.logger.debug(f"✅ Processed {total_msgs} messages from streams")
                        else:
                            # No messages (normal with blocking read)
                            consecutive_errors = 0
                    except redis.ConnectionError as conn_err:
                        self.logger.warning(f"⚠️ Redis connection error during stream read: {conn_err}")
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            break
                        time.sleep(1)
                        continue
                    except Exception as stream_err:
                        error_str = str(stream_err).lower()
                        if "timeout" in error_str or "no such key" in error_str:
                            # Normal timeout or stream not ready, continue
                            consecutive_errors = 0
                            continue
                        else:
                            self.logger.warning(f"⚠️ Stream reading error: {stream_err}")
                            consecutive_errors += 1
                            if consecutive_errors >= max_consecutive_errors:
                                break
                            time.sleep(1)
                            continue

                    # Periodic cleanup of old dedup hashes
                    self._cleanup_old_hashes()
                    
                    # Periodic cleanup of old rolling windows
                    self._cleanup_old_rolling_windows()
                    
                    # Periodic news processing from symbol keys (every 30 seconds) - TEMPORARILY DISABLED
                    # current_time = time.time()
                    # if current_time - self.last_news_check >= self.news_check_interval:
                    #     self._process_news_from_symbol_keys()
                    #     self.last_news_check = current_time

                except redis.ConnectionError as conn_err:
                    self.logger.warning(f"⚠️ Redis connection error in pipeline loop: {conn_err}")
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        self.logger.error("❌ Too many connection errors, breaking loop")
                        break
                    # Force pubsub recreation
                    self.pubsub = None
                    time.sleep(2)
                    continue
                except Exception as e:
                    if "timeout" in str(e).lower():
                        # Normal timeout, continue
                        continue
                    else:
                        # Handle protocol errors specifically
                        error_msg = str(e)
                        if "Protocol" in error_msg or "ck_sequence" in error_msg:
                            if self.logger and self.logger.handlers:
                                self.logger.warning(
                                    f"⚠️ Protocol error in data pipeline (likely corrupt data): {error_msg[:100]}..."
                                )
                            self.stats["protocol_errors"] += 1
                            consecutive_errors += 1
                            if consecutive_errors >= max_consecutive_errors:
                                break
                        else:
                            if self.logger and self.logger.handlers:
                                self.logger.error(
                                    f"❌ Unexpected error in data pipeline: {e}"
                                )
                            consecutive_errors += 1
                            if consecutive_errors >= max_consecutive_errors:
                                break
                        time.sleep(0.5)  # Reduced sleep for faster recovery

        except redis.ConnectionError as e:
            error_msg = str(e).lower()
            if self.logger and self.logger.handlers:
                self.logger.error(f"❌ Redis connection error: {e}")
            self.stats["errors"] += 1
            
            # CRITICAL: Handle "Too many connections" - this means connection pool is exhausted
            # The issue is likely that we're creating too many connections or not reusing them
            if "too many connections" in error_msg:
                if self.logger and self.logger.handlers:
                    self.logger.error(f"🚨 TOO MANY CONNECTIONS ERROR - Connection pool exhausted")
                    self.logger.error(f"   This usually means connections aren't being reused properly")
                    self.logger.error(f"   Stream: ticks:intraday:processed exists with 5000 messages")
                    self.logger.error(f"   Consumer group: data_pipeline_group has lag of 5000")
                
                # ✅ FIXED: Instead of stopping, try to reuse existing connection
                # The realtime_client should be a pooled connection that can be reused
                # Don't create new connections - just wait and retry with existing client
                if not hasattr(self, '_connection_retry_count'):
                    self._connection_retry_count = 0
                
                self._connection_retry_count += 1
                
                # Wait with exponential backoff, but don't stop the pipeline
                backoff_time = min(2 ** self._connection_retry_count, 30)  # Max 30 seconds
                
                if self.logger and self.logger.handlers:
                    self.logger.warning(f"⚠️ Waiting {backoff_time}s before retry (attempt {self._connection_retry_count})")
                
                time.sleep(backoff_time)
                
                # ✅ FIXED: Restart the consuming method to retry with backoff
                # Don't use continue here - we're outside the while loop
                if self.running:
                    self.start_consuming()  # Recursively restart
                return
            else:
                # Normal connection error (not "too many connections")
                # Reset backoff for normal errors
                self._connection_limit_backoff = 1
                # Attempt reconnection with normal delay
                time.sleep(2)
                if self.running:
                    if self.logger and self.logger.handlers:
                        self.logger.info("🔄 Attempting to reconnect and resubscribe...")
                    # Recursively restart consuming (will recreate pubsub)
                    self.start_consuming()
        except Exception as e:
            # Handle protocol errors specifically
            error_msg = str(e)
            if "Protocol" in error_msg or "ck_sequence" in error_msg:
                if self.logger and self.logger.handlers:
                    self.logger.warning(
                        f"⚠️ Protocol error in data pipeline (likely corrupt data): {error_msg[:100]}..."
                    )
                self.stats["protocol_errors"] += 1
            else:
                if self.logger and self.logger.handlers:
                    self.logger.error(f"❌ Unexpected error in data pipeline: {e}")
                self.stats["errors"] += 1
        finally:
            self.stop()


    def _process_message(self, message):
        """Process incoming Redis message with specific error handling"""
        # Handle channel (may be bytes or str depending on decode_responses)
        channel = message["channel"]
        if isinstance(channel, bytes):
            channel = channel.decode("utf-8", errors="ignore")
        elif not isinstance(channel, str):
            channel = str(channel)
        
        # Handle payload (may be bytes or str depending on decode_responses)
        raw_payload = message.get("data", "")
        if isinstance(raw_payload, bytes):
            raw_payload = raw_payload.decode("utf-8", errors="ignore")
        elif not isinstance(raw_payload, str):
            raw_payload = str(raw_payload)

        fragments = [frag for frag in raw_payload.splitlines() if frag.strip()]
        if not fragments:
            fragments = [raw_payload]

        for fragment in fragments:
            combined = self.partial_message_buffers.get(channel, "") + fragment
            combined_stripped = combined.strip()
            if not combined_stripped:
                continue

            try:
                data = json.loads(combined_stripped)
                # Clear buffer on successful parse
                self.partial_message_buffers.pop(channel, None)
            except json.JSONDecodeError as e:
                # Cache partial payload for next fragment
                self.partial_message_buffers[channel] = combined[-self.max_partial_buffer :]
                preview = combined[-120:].replace("\n", " ")
                self.logger.warning(
                    f"Invalid JSON from {channel}: {e}. Cached fragment tail: {preview}"
                )
                self.stats["json_errors"] += 1
                self.channel_errors[channel] = self.channel_errors.get(channel, 0) + 1
                continue
            except ValueError as e:
                self.stats["validation_errors"] += 1
                self.channel_errors[channel] = self.channel_errors.get(channel, 0) + 1
                self.logger.warning(f"Validation error for {channel}: {e}")
                continue
            except Exception as e:
                error_msg = str(e)
                if "Protocol" in error_msg or "ck_sequence" in error_msg:
                    self.logger.warning(
                        f"⚠️ Protocol error processing {channel} (corrupt data): {error_msg[:100]}..."
                    )
                    self.stats["protocol_errors"] += 1
                else:
                    self.stats["errors"] += 1
                    self.logger.error(f"❌ Unexpected error in data pipeline: {e}")
                self.channel_errors[channel] = self.channel_errors.get(channel, 0) + 1
                continue

            try:
                # ✅ FIX: Decode channel from bytes to string if needed
                if isinstance(channel, bytes):
                    channel = channel.decode('utf-8')
                
                # Tick data is consumed via start_consuming_streams() which reads from DatabaseAwareKeyBuilder.live_processed_stream()
                if channel == "market_data.news":
                    self.logger.info(f"📰 Received news message: {data.get('title', 'Unknown')[:50]}...")
                    self._process_news_from_channel(data)
                elif channel == "premarket.orders":
                    self._process_premarket_order(data)
                elif channel == "alerts.manager":
                    self.logger.debug(
                        f"Received alert manager data for {data.get('symbol', 'UNKNOWN')} - sophisticated detection active"
                    )
                elif channel.startswith("index:"):
                    self._process_index_data(channel, data)
                else:
                    self.logger.warning(f"Unknown channel: {channel}")
            except ValueError as e:
                self.stats["validation_errors"] += 1
                self.channel_errors[channel] = self.channel_errors.get(channel, 0) + 1
                self.logger.warning(f"Validation error for {channel}: {e}")
            except Exception as e:
                self.stats["errors"] += 1
                self.channel_errors[channel] = self.channel_errors.get(channel, 0) + 1
                self.logger.error(f"❌ Error processing {channel}: {e}", exc_info=True)



    def _enqueue_tick_for_feeder(self, tick: dict, symbol: str) -> None:
        """Make the cleaned tick available to scanner_main via tick_queue."""
        if not hasattr(self, "tick_queue") or self.tick_queue is None:
            self.tick_queue = queue.Queue(maxsize=self.tick_queue_maxsize)
        try:
            self.tick_queue.put_nowait(tick)
            self.stats["tick_queue_enqueued"] = self.stats.get("tick_queue_enqueued", 0) + 1
            self.last_heartbeat = time.time()
            self._tick_flow_log(
                "QUEUE_ENQUEUE",
                symbol,
                queue_size=getattr(self.tick_queue, "qsize", lambda: 0)(),
                ts=tick.get("exchange_timestamp_ms") or tick.get("timestamp_ms"),
            )
        except queue.Full:
            self.stats["tick_queue_drops"] = self.stats.get("tick_queue_drops", 0) + 1
            self._tick_flow_log("QUEUE_FULL", symbol, queue_size=self.tick_queue_maxsize)
            try:
                self.tick_queue.get_nowait()
                self.tick_queue.put_nowait(tick)
                self._tick_flow_log("QUEUE_DROP_OLDEST", symbol)
            except queue.Empty:
                pass

    def _process_market_tick(self, data):
        """Stripped down processing path with minimal validation overhead."""
        # ✅ FIX: Ensure data is a dict before calling _get_unified_symbol
        # Advanced indicators or other data types might not be dicts
        if not isinstance(data, dict):
            # Try to convert to dict if possible (e.g., if it's a dict-like object)
            if hasattr(data, '__dict__'):
                data = data.__dict__
            elif hasattr(data, 'get'):
                # Already dict-like, use as-is
                pass
            else:
                # Try to extract symbol from object attributes
                symbol = getattr(data, 'symbol', None) or getattr(data, 'tradingsymbol', None)
                if symbol:
                    data = {'symbol': symbol, 'tradingsymbol': symbol}
                else:
                    self.logger.warning(f"⚠️ [DATA_TYPE_ERROR] _process_market_tick received non-dict data: {type(data)}, cannot extract symbol")
                    self._tick_flow_log("INVALID_DATA_TYPE", "UNKNOWN")
                    return
        
        symbol = self._get_unified_symbol(data)
        if not symbol:
            self._tick_flow_log("MISSING_SYMBOL", "UNKNOWN")
            return
        
        # ✅ DEBUG: Log symbol being processed to trace unexpected symbols
        if "ADANIPORTS" in symbol.upper():
            self.logger.warning(f"🔍 [SYMBOL_TRACE] Processing ADANIPORTS symbol: {symbol}, source: {data.get('source', 'unknown')}, stream: {data.get('stream_name', 'unknown')}")
        self._tick_flow_log(
            "RAW_TICK",
            symbol,
            stream_timestamp=data.get("exchange_timestamp_ms") or data.get("timestamp_ms"),
        )
        try:
            cleaned = self._clean_tick_data(data)
        except Exception as exc:
            self.logger.error(f"❌ Failed to clean tick for {symbol}: {exc}")
            self._tick_flow_log("CLEAN_FAILED", symbol, error=str(exc))
            return

        if not cleaned:
            self.logger.debug(f"⚠️ Clean tick returned None for {symbol} (likely stale tick)")
            self._tick_flow_log("CLEAN_RETURNED_NONE", symbol)
            return
        
        # ✅ COMPREHENSIVE LOGGING: Log ALL data preserved after clean_tick
        symbol_after_clean = cleaned.get("symbol") or cleaned.get("tradingsymbol") or symbol
        self.logger.info(f"📊 [DATA_FLOW_TRACE] ===== AFTER _clean_tick_data for {symbol_after_clean} =====")
        
        # Log all critical field categories
        greeks_present = [f for f in ['delta', 'gamma', 'theta', 'vega', 'rho'] if cleaned.get(f) not in (None, 0.0)]
        greeks_values = {f: cleaned.get(f) for f in ['delta', 'gamma', 'theta', 'vega', 'rho'] if cleaned.get(f) not in (None, 0.0)}
        volume_fields = {k: v for k, v in cleaned.items() if 'volume' in k.lower()}
        price_fields = {k: v for k, v in cleaned.items() if any(x in k.lower() for x in ['price', 'open', 'high', 'low', 'close'])}
        indicator_fields = {k: v for k, v in cleaned.items() if k in ['rsi', 'macd', 'ema_5', 'ema_10', 'ema_20', 'atr', 'vwap', 'bollinger_bands']}
        microstructure_fields = {k: v for k, v in cleaned.items() if k in ['microprice', 'order_flow_imbalance', 'order_book_imbalance', 'cumulative_volume_delta']}
        
        self.logger.info(f"   🔍 Greeks ({len(greeks_present)}/5): {greeks_values if greeks_values else 'ALL MISSING'}")
        self.logger.info(f"   📊 Volume fields ({len(volume_fields)}): {list(volume_fields.keys())}")
        self.logger.info(f"   💰 Price fields ({len(price_fields)}): {list(price_fields.keys())[:5]}...")
        self.logger.info(f"   📈 Indicators ({len(indicator_fields)}): {list(indicator_fields.keys())}")
        self.logger.info(f"   🔬 Microstructure ({len(microstructure_fields)}): {list(microstructure_fields.keys())}")
        self.logger.info(f"   📦 Total fields in cleaned data: {len(cleaned)}")
        self.logger.info(f"📊 [DATA_FLOW_TRACE] ===== END clean_tick data for {symbol_after_clean} =====")
        
        self._enqueue_tick_for_feeder(cleaned, symbol)

        # Pass cleaned tick data with ALL fields preserved (indicators, Greeks, volume_ratio, price_change, microstructure, etc.)
        self._process_tick_for_patterns_fast(cleaned)


    
    def _normalize_timestamps(self, data: dict) -> dict:
        """
        ✅ TIMESTAMP NORMALIZATION: Fix invalid timestamps in data.
        
        Handles:
        - Invalid future timestamps (e.g., 1763524100000 which is year 2025+)
        - Missing timestamps
        - Ensures both timestamp and timestamp_ms are present
        """
        timestamp_ms = data.get('timestamp_ms')
        
        # Fix invalid future timestamps (beyond year 2033 = 2000000000000 ms)
        # Note: Year 2025 timestamps should be valid (around 1735689600000)
        if timestamp_ms and timestamp_ms > 2000000000000:
            from datetime import datetime
            self.logger.warning(f"🕒 Fixing invalid timestamp: {timestamp_ms} - Using current time")
            current_ms = int(datetime.now().timestamp() * 1000)
            data['timestamp_ms'] = current_ms
            data['timestamp'] = datetime.now().isoformat()
        
        # Ensure timestamp exists (create from timestamp_ms if available)
        if not data.get('timestamp') and timestamp_ms:
            try:
                from datetime import datetime
                data['timestamp'] = datetime.fromtimestamp(timestamp_ms / 1000).isoformat()
            except Exception as e:
                from datetime import datetime
                self.logger.debug(f"Could not convert timestamp_ms to timestamp: {e}")
                data['timestamp'] = datetime.now().isoformat()
        elif not data.get('timestamp'):
            from datetime import datetime
            data['timestamp'] = datetime.now().isoformat()
        
        # Ensure timestamp_ms exists (create from timestamp if available)
        if not data.get('timestamp_ms'):
            timestamp_str = data.get('timestamp')
            if timestamp_str:
                try:
                    from datetime import datetime
                    if isinstance(timestamp_str, str):
                        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        data['timestamp_ms'] = int(dt.timestamp() * 1000)
                    else:
                        data['timestamp_ms'] = int(datetime.now().timestamp() * 1000)
                except Exception as e:
                    from datetime import datetime
                    self.logger.debug(f"Could not convert timestamp to timestamp_ms: {e}")
                    data['timestamp_ms'] = int(datetime.now().timestamp() * 1000)
            else:
                from datetime import datetime
                data['timestamp_ms'] = int(datetime.now().timestamp() * 1000)
        
        return data
    
    def _validate_pattern_data_quality_comprehensive(self, alert_data: dict) -> bool:
        """✅ SINGLE SOURCE OF TRUTH: Comprehensive upstream validation."""
        try:
            symbol = alert_data.get('symbol', 'UNKNOWN')

            if not self._validate_tick_freshness_comprehensive(alert_data):
                return False

            if not self._validate_price_comprehensive(symbol, alert_data):
                return False

            if not self._validate_volume_comprehensive(symbol, alert_data):
                return False

            if not self._validate_indicators_comprehensive(alert_data):
                return False

            if is_option_symbol(symbol):
                if not self._validate_option_data_comprehensive(symbol, alert_data):
                    return False

            return True
        except Exception as exc:
            self.logger.error(f"Comprehensive data validation error: {exc}")
            return False

    def _validate_tick_freshness_comprehensive(self, tick_data: dict) -> bool:
        """✅ SINGLE SOURCE: Timestamp validation shared by all downstream consumers.
        
        Uses the same timestamp priority logic as _validate_tick_freshness for consistency.
        """
        try:
            import time
            symbol = tick_data.get('symbol', 'UNKNOWN')
            
            # Get tick timestamp using same priority as _validate_tick_freshness
            tick_timestamp = None
            timestamp_ms = None
            
            # Priority 1: exchange_timestamp_ms (epoch milliseconds)
            if tick_data.get("exchange_timestamp_ms"):
                ts_ms = self._safe_float(tick_data.get("exchange_timestamp_ms"), 0.0)
                if ts_ms > 0:
                    timestamp_ms = ts_ms
                    tick_timestamp = ts_ms / 1000.0
            
            # Priority 2: exchange_timestamp_epoch (epoch seconds or milliseconds)
            if timestamp_ms is None and tick_data.get("exchange_timestamp_epoch"):
                ts_epoch = self._safe_float(tick_data.get("exchange_timestamp_epoch"), 0.0)
                if ts_epoch > 0:
                    if ts_epoch > 1e10:
                        timestamp_ms = ts_epoch
                        tick_timestamp = ts_epoch / 1000.0
                    else:
                        timestamp_ms = ts_epoch * 1000.0
                        tick_timestamp = ts_epoch
            
            # Priority 3: exchange_timestamp
            if timestamp_ms is None and tick_data.get("exchange_timestamp"):
                ts_exchange = tick_data.get("exchange_timestamp")
                try:
                    ts_val = self._safe_float(ts_exchange, 0.0)
                    if ts_val > 0:
                        if ts_val > 1e10:
                            timestamp_ms = ts_val
                            tick_timestamp = ts_val / 1000.0
                        else:
                            timestamp_ms = ts_val * 1000.0
                            tick_timestamp = ts_val
                except (TypeError, ValueError):
                    try:
                        # TimestampNormalizer is already imported at top from shared_core
                        from shared_core.timestamp_normalizer import TimestampNormalizer
                        timestamp_ms = TimestampNormalizer.to_epoch_ms(ts_exchange)
                        tick_timestamp = timestamp_ms / 1000.0
                    except Exception:
                        pass
            
            # Priority 4: timestamp_ms
            if timestamp_ms is None and tick_data.get("timestamp_ms"):
                ts_ms = self._safe_float(tick_data.get("timestamp_ms"), 0.0)
                if ts_ms > 0:
                    timestamp_ms = ts_ms
                    tick_timestamp = ts_ms / 1000.0
            
            # Priority 5: timestamp
            if timestamp_ms is None and tick_data.get("timestamp"):
                ts_raw = tick_data["timestamp"]
                try:
                    ts_val = self._safe_float(ts_raw, 0.0)
                    if ts_val > 1e10:
                        timestamp_ms = ts_val
                        tick_timestamp = ts_val / 1000.0
                    elif ts_val > 0:
                        timestamp_ms = ts_val * 1000.0
                        tick_timestamp = ts_val
                except (TypeError, ValueError):
                    try:
                        # TimestampNormalizer is already imported at top from shared_core
                        timestamp_ms = TimestampNormalizer.to_epoch_ms(ts_raw)
                        tick_timestamp = timestamp_ms / 1000.0
                    except Exception:
                        pass
            
            # Priority 6: last_trade_time_ms
            if timestamp_ms is None and tick_data.get("last_trade_time_ms"):
                timestamp_ms = self._safe_float(tick_data.get("last_trade_time_ms"), 0.0)
                if timestamp_ms > 0:
                    tick_timestamp = timestamp_ms / 1000.0
            
            # Priority 7: last_trade_time
            if timestamp_ms is None and tick_data.get("last_trade_time"):
                tick_timestamp = self._safe_float(tick_data.get("last_trade_time"), 0.0)
                if tick_timestamp > 0:
                    timestamp_ms = tick_timestamp * 1000.0

            if not timestamp_ms or timestamp_ms <= 0:
                self.logger.warning(f"❌ [TICK_FRESHNESS_COMPREHENSIVE] Missing timestamp: {symbol}")
                return False

            current_time_ms = time.time() * 1000
            tick_age_seconds = (current_time_ms - timestamp_ms) / 1000.0

            if tick_age_seconds > 300:
                self.logger.warning(
                    f"🕒 [TICK_FRESHNESS_COMPREHENSIVE] STALE TICK REJECTED: {symbol} age={tick_age_seconds:.1f}s > 300s"
                )
                return False

            if tick_age_seconds < -30:
                self.logger.warning(
                    f"🕒 [TICK_FRESHNESS_COMPREHENSIVE] FUTURE TICK REJECTED: {symbol} age={tick_age_seconds:.1f}s (future)"
                )
                return False

            return True
        except Exception as exc:
            symbol = tick_data.get('symbol', 'UNKNOWN')
            self.logger.error(f"❌ [TICK_FRESHNESS_COMPREHENSIVE] Timestamp validation error for {symbol}: {exc}")
            return False

    def _validate_price_comprehensive(self, symbol: str, data: dict) -> bool:
        """✅ SINGLE SOURCE: Price and OHLC validation."""
        try:
            price = float(data.get('last_price', 0) or 0)
            if price <= 0:
                self.logger.warning(f"❌ Invalid price: {symbol} price={price}")
                return False

            base_symbol = self._extract_base_symbol(symbol)
            expected_range = None
            validator = getattr(self.enforcer, "price_validator", None)
            price_ranges = getattr(validator, "expected_ranges", {}) if validator else {}

            if base_symbol in self._price_range_cache:
                expected_range = self._price_range_cache.get(base_symbol)
            else:
                expected_range = price_ranges.get(base_symbol)
                self._price_range_cache[base_symbol] = expected_range

            if expected_range:
                min_price, max_price = expected_range
                if not (min_price <= price <= max_price):
                    self.logger.warning(
                        f"❌ Price out of range: {symbol} {price:.2f} expected [{min_price:.2f}, {max_price:.2f}]"
                    )
                    return False

            ohlc = data.get('ohlc', {})
            if isinstance(ohlc, dict):
                high_price = ohlc.get('high')
                low_price = ohlc.get('low')
                if (
                    high_price is not None
                    and low_price is not None
                    and high_price < low_price
                ):
                    self.logger.warning(f"❌ OHLC invalid: {symbol} high={high_price} < low={low_price}")
                    return False

                if (
                    low_price is not None
                    and high_price is not None
                    and not (low_price <= price <= high_price)
                ):
                    self.logger.debug(f"⚠️ Price outside OHLC range: {symbol}")

            return True
        except Exception as exc:
            self.logger.error(f"Price validation error: {exc}")
            return False

    def _validate_volume_comprehensive(self, symbol: str, data: dict) -> bool:
        """✅ SINGLE SOURCE: Volume validation."""
        try:
            volume = (
                data.get('volume')
                or data.get('bucket_incremental_volume')
                or data.get('zerodha_last_traded_quantity')
                or 0
            )
            volume = float(volume or 0)

            if volume < 0:
                self.logger.warning(f"❌ Invalid volume: {symbol} volume={volume}")
                return False

            if volume > 1_000_000_000:
                self.logger.warning(f"❌ Unreasonable volume: {symbol} volume={volume}")
                return False

            volume_ratio = float(data.get('volume_ratio', 0) or 0)
            if volume_ratio < 0:
                self.logger.warning(f"❌ Invalid volume ratio: {symbol} ratio={volume_ratio}")
                return False

            return True
        except Exception as exc:
            self.logger.error(f"Volume validation error: {exc}")
            return False

    def _validate_indicators_comprehensive(self, data: dict) -> bool:
        """✅ SINGLE SOURCE: Indicator sanity checks."""
        try:
            symbol = data.get('symbol', 'UNKNOWN')
            ema_5 = data.get('ema_5')
            ema_20 = data.get('ema_20')
            ema_50 = data.get('ema_50')

            if ema_5 is not None and ema_20 is not None and ema_50 is not None:
                if ema_5 == ema_20 == ema_50:
                    self.logger.warning(f"❌ Stale EMAs: {symbol} all EMAs identical")
                    return False

            rsi = data.get('rsi')
            if rsi is not None and not (0 <= float(rsi) <= 100):
                self.logger.warning(f"❌ Invalid RSI: {symbol} RSI={rsi}")
                return False

            return True
        except Exception as exc:
            self.logger.error(f"Indicator validation error: {exc}")
            return False

    def _validate_option_data_comprehensive(self, symbol: str, data: dict) -> bool:
        """✅ SINGLE SOURCE: Option-specific validation."""
        try:
            dte = data.get('trading_dte')
            if dte is None:
                dte_years = data.get('dte_years')
                dte = float(dte_years or 0) * 365 if dte_years else 0
            dte = float(dte or 0)
            if dte <= 0:
                self.logger.warning(f"❌ Expired option: {symbol} DTE={dte}")
                return False

            iv = data.get('iv') or data.get('implied_volatility') or 0
            iv = float(iv or 0)
            if iv <= 0 or iv > 5:
                self.logger.warning(f"❌ Invalid IV: {symbol} IV={iv}")
                return False

            option_price = data.get('option_price')
            if option_price is None:
                option_price = data.get('last_price', 0)
            option_price = float(option_price or 0)
            if option_price <= 0:
                self.logger.warning(f"❌ Invalid option price: {symbol} price={option_price}")
                return False

            delta = data.get('delta')
            if delta is not None and not (-1 <= float(delta) <= 1):
                self.logger.warning(f"❌ Invalid delta: {symbol} delta={delta}")
                return False

            gamma = data.get('gamma')
            if gamma is not None and float(gamma) < 0:
                self.logger.warning(f"❌ Invalid gamma: {symbol} gamma={gamma}")
                return False

            underlying_price = (
                data.get('underlying_price')
                or data.get('spot_price')
                or data.get('underlying_last_price')
                or 0
            )
            underlying_price = float(underlying_price or 0)
            if underlying_price <= 0:
                self.logger.warning(f"❌ Missing underlying price: {symbol}")
                return False

            return True
        except Exception as exc:
            self.logger.error(f"Option validation error: {exc}")
            return False
    
    def _process_tick_for_patterns(self, tick_data):
        """Process tick data for pattern detection and alerts"""
        start_time = time.time()
        symbol = tick_data.get("symbol", "UNKNOWN") if isinstance(tick_data, dict) else "UNKNOWN"
        try:
            # ✅ FIXED: Use unified symbol extraction method
            symbol = self._get_unified_symbol(tick_data) or symbol
            # Ensure symbol is present in indicators downstream (use normalized symbol)
            if "symbol" not in tick_data and symbol:
                tick_data["symbol"] = symbol
            elif symbol and tick_data.get("symbol") != symbol:
                # Update tick_data with normalized symbol if it changed
                tick_data["symbol"] = symbol
                tick_data["tradingsymbol"] = symbol
            
            # ✅ CRITICAL: Log data BEFORE normalization to see what arrives
            self.logger.info(f"📊 [DATA_FLOW_TRACE] _process_tick_for_patterns received data for {symbol} (BEFORE normalize_redis_tick_data):")
            greeks_before = [f for f in ['delta', 'gamma', 'theta', 'vega', 'rho'] if tick_data.get(f) not in (None, 0.0)]
            self.logger.info(f"   🔍 Greeks before normalize: {len(greeks_before)}/{5} present {greeks_before if greeks_before else '(all missing)'}")
            self.logger.info(f"   🔍 volume_ratio before normalize: {tick_data.get('volume_ratio')}")
            self.logger.info(f"   🔍 price_change before normalize: {tick_data.get('price_change_pct') or tick_data.get('price_change')}")
            
            # ✅ CRITICAL FIX: Normalize field names using redis_key_mapping before processing
            # This ensures all consumers receive data with canonical field names
            tick_data = normalize_redis_tick_data(tick_data)
            
            # ✅ ADD: Map for pattern detector consumption (handles field name variations during migration)
            # This adds consumer-specific aliases (e.g., volume → bucket_incremental_volume, price → last_price)
            # so pattern detectors can access fields even if field names vary during migration
            tick_data = map_tick_for_pattern(tick_data)
            
            # ✅ CRITICAL: Log data AFTER normalization and mapping to see final field names
            self.logger.info(f"📊 [DATA_FLOW_TRACE] _process_tick_for_patterns AFTER normalize_redis_tick_data + map_tick_for_pattern for {symbol}:")
            greeks_after = [f for f in ['delta', 'gamma', 'theta', 'vega', 'rho'] if tick_data.get(f) not in (None, 0.0)]
            self.logger.info(f"   🔍 Greeks after normalize: {len(greeks_after)}/{5} present {greeks_after if greeks_after else '(all missing)'}")
            self.logger.info(f"   🔍 volume_ratio after normalize: {tick_data.get('volume_ratio')}")
            self.logger.info(f"   🔍 price_change after normalize: {tick_data.get('price_change_pct') or tick_data.get('price_change')}")
            
            # Verify critical fields are accessible (for migration compatibility)
            critical_fields = ['volume_ratio', 'delta', 'gamma', 'theta', 'vega', 'price_change_pct']
            missing_fields = [f for f in critical_fields if f not in tick_data or tick_data[f] is None]
            if missing_fields:
                self.logger.warning(f"⚠️ [FIELD_MAPPING] {symbol} - Missing fields after mapping: {missing_fields}")
            
            self.logger.info(f"Processing tick for patterns: {symbol}")
            
            # ✅ SINGLE SOURCE OF TRUTH: Volume already calculated by WebSocket parser
            # and set bucket_incremental_volume, incremental_volume, and volume fields
            self.logger.debug(f"Using pre-calculated volume from WebSocket parser")
            
            # ✅ CALCULATIONS SEPARATION: Indicators are computed by calculations.py (Polars-based)
            # and stored to Redis. Pattern detector will read indicators from Redis.
            # Do NOT compute indicators here - pass cleaned tick_data to pattern detector.
            try:
                # Use cleaned tick_data - pattern_detector will fetch indicators from Redis
                indicators = tick_data.copy() if isinstance(tick_data, dict) else tick_data
                if indicators:
                    if "symbol" not in indicators:
                        indicators["symbol"] = symbol
                    
                    # ✅ TIMESTAMP NORMALIZATION: Fix invalid timestamps before pattern detection
                    indicators = self._normalize_timestamps(indicators)
                    
                    # ✅ CRITICAL FIX: Fetch Greeks from Redis (computed by calculations.py)
                    # Pattern detector will also fetch other indicators from Redis as needed
                    if self.redis_client:
                        try:
                            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards, DatabaseAwareKeyBuilder
                                
                            # Get DB1 client (where Greeks are stored)
                            db1_client = None
                            if hasattr(self.redis_client, 'get_client'):
                                db1_client = self.redis_client.get_client(1)  # DB 1
                            elif hasattr(self.redis_client, 'redis'):
                                db1_client = self.redis_client.redis
                            elif hasattr(self.redis_client, 'get'):
                                db1_client = self.redis_client
                            
                            if db1_client:
                                canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
                                
                            # Fetch all Greeks from Redis
                            greek_names = ['delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'implied_volatility', 
                                            'dte_years', 'trading_dte', 'expiry_series', 'option_price', 'vanna', 'charm', 'vomma', 'speed', 'zomma', 'color', 'gamma_exposure']
                            greeks_fetched = 0
                            
                            for greek_name in greek_names:
                                # ✅ Check tick_data FIRST - it might already have Greeks from upstream
                                if greek_name in tick_data and tick_data[greek_name] not in (None, 0, 0.0):
                                    indicators[greek_name] = tick_data[greek_name]
                                    continue

                                # ✅ FIXED: Use unified Redis data fetcher instead of direct Redis access
                                try:
                                    redis_data = self._get_complete_redis_data(canonical_symbol)
                                    if greek_name in redis_data:
                                        greek_value = redis_data[greek_name]
                                        # Only update if value is non-zero OR if we don't have this Greek yet
                                        # This prevents overwriting with 0.0 when Redis has valid data
                                        if greek_value != 0.0 or greek_name not in indicators:
                                            indicators[greek_name] = greek_value
                                            greeks_fetched += 1
                                except Exception as e:
                                    self.logger.debug(f"Failed to fetch {greek_name} from unified Redis fetcher: {e}")
                            
                            if greeks_fetched > 0:
                                self.logger.info(f"✅ [GREEKS_FETCH] {symbol} - Fetched {greeks_fetched} Greeks from Redis (canonical: {canonical_symbol})")
                        except Exception as e:
                            self.logger.debug(f"Error fetching Greeks from Redis for {symbol}: {e}")
                
                # ✅ DATA QUALITY GATE: Comprehensive validation happens once here
                if not self._validate_pattern_data_quality_comprehensive(indicators):
                    self.logger.debug(f"🛑 Data quality rejected for {symbol} - skipping patterns")
                elif hasattr(self, 'pattern_detector'):
                    patterns = self.pattern_detector.detect_patterns(indicators)
                    self.logger.info(f"Pattern detection for {symbol}: {len(patterns)} patterns")

                    self.logger.info(f"🔍 DEBUG: patterns={len(patterns) if patterns else 0}, has_alert_manager={hasattr(self, 'alert_manager')}")
                    if patterns and hasattr(self, 'alert_manager'):
                        self.logger.info(f"🔍 DEBUG: Processing {len(patterns)} patterns for {symbol}")
                        for pattern in patterns:
                            pattern['symbol'] = symbol
                            # CRITICAL: Include all calculated indicators in the pattern payload
                            # This ensures indicators are available in alert payload for dashboard
                            if indicators and isinstance(indicators, dict):
                                # Ensure indicators dict exists in pattern
                                if 'indicators' not in pattern or not isinstance(pattern.get('indicators'), dict):
                                    pattern['indicators'] = {}
                                # ✅ CRITICAL: Merge ALL calculated indicators into pattern
                                # This includes microstructure metrics, advanced indicators, cross-asset metrics, etc.
                                pattern['indicators'].update(indicators)
                                
                                # ✅ COMPREHENSIVE: Add ALL indicators to top-level for easy access
                                # This ensures all computed indicators are available in alert payload
                                # Include all indicator types: technical, microstructure, Greeks, cross-asset
                                all_indicator_keys = [
                                    # Technical indicators
                                    'rsi', 'macd', 'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200',
                                    'atr', 'vwap', 'bollinger_bands', 'volume_profile', 'volume_ratio', 'price_change',
                                    'z_score',
                                    # ✅ Microstructure indicators (all 11)
                                    'microprice', 'order_flow_imbalance', 'order_book_imbalance', 'spread_absolute',
                                    'spread_bps', 'depth_imbalance', 'cumulative_volume_delta',
                                    'best_bid_size', 'best_ask_size', 'total_bid_depth', 'total_ask_depth',
                                    # Advanced Greeks
                                    'gamma_exposure', 'vanna', 'charm', 'vomma', 'speed', 'zomma', 'color',
                                    # Cross-asset indicators
                                    'usdinr_sensitivity', 'usdinr_correlation', 'usdinr_theoretical_price', 'usdinr_basis',
                                    # Greeks for options (so they're at top level in alerts)
                                    'delta', 'gamma', 'theta', 'vega', 'rho', 'dte_years', 'trading_dte', 'iv', 'implied_volatility',
                                    # Additional indicators
                                    'buy_pressure', 'vix_level',
                                    # ✅ Edge Indicators (Intent, Constraint, Transition, Regime)
                                    'edge_intent', 'edge_constraint', 'edge_transition', 'edge_regime',
                                    'edge_should_trade', 'edge_timestamp',
                                ]
                                
                                for indicator_key in all_indicator_keys:
                                    if indicator_key in indicators and indicator_key not in pattern:
                                        pattern[indicator_key] = indicators[indicator_key]
                                
                                # ✅ PRESERVE: Also add any other indicators not in the hardcoded list
                                # This ensures no computed indicators are dropped
                                for indicator_key, indicator_value in indicators.items():
                                    if indicator_key not in pattern and indicator_key not in ['symbol', 'timestamp', 'timestamp_ms']:
                                        pattern[indicator_key] = indicator_value
                            self.logger.info(f"🔍 DEBUG: Calling alert_manager.send_alert for {symbol}: {pattern.get('pattern', 'UNKNOWN')}")
                            alert_sent = self.alert_manager.send_alert(pattern)
            except Exception as inner_exc:
                self.logger.error(f"Pattern indicator preparation failed for {symbol}: {inner_exc}", exc_info=True)
                raise
        except Exception as exc:
            self.logger.error(f"Pattern processing error for {symbol}: {exc}", exc_info=True)
            self.stats["errors"] = self.stats.get("errors", 0) + 1
        finally:
            duration = time.time() - start_time
            self.latency_stats["pattern_detection_time"] = (
                self.latency_stats.get("pattern_detection_time", 0.0) + duration
            )


    def _log_cleaned_tick_summary(self, tick: dict):
        """Emit a compact, informative debug line after cleaning to trace logic issues.

        Includes: symbol, prices, bucket_incremental_volume fields, mode, timestamps (ISO + ms), depth validity,
        and flags suspicious combinations (e.g., full mode with zero bucket_incremental_volume fields).
        """
        pass

    def _get_stream_maxlen(self, stream_key: str) -> int:
        """
        Determine appropriate maxlen for Redis stream based on stream type.
        ALWAYS use maxlen in XADD operations to prevent unbounded stream growth.
        
        Args:
            stream_key: Redis stream key name
            
        Returns:
            int: Maximum number of messages to keep in stream
        """
        # Main streams - match optimizer targets
        if stream_key == 'ticks:intraday:processed':
            return 5000  # Match optimizer target
        # ✅ FIXED: ticks:raw:binary is not used by intraday crawler (removed)
        elif stream_key == 'alerts:stream':
            return 1000  # Match optimizer target
        elif stream_key.startswith('ticks:'):
            # Per-symbol streams - keep smaller to prevent memory bloat
            return 1000
        else:
            # Default for unknown streams
            return 10000
    
    def _publish_to_stream_with_maxlen(self, redis_client, stream_key: str, stream_data: dict) -> bool:
        """
        Publish to Redis stream with ALWAYS applying maxlen parameter.
        This prevents unbounded stream growth at the source.
        
        Args:
            redis_client: Redis client instance
            stream_key: Stream key name
            stream_data: Data to publish
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            maxlen = self._get_stream_maxlen(stream_key)
            safe_data = fix_numpy_serialization(stream_data)
            redis_client.xadd(
                stream_key,
                safe_data,
                maxlen=maxlen,
                approximate=True  # Faster trimming, slight approximation acceptable
            )
            return True
        except Exception as e:
            self.logger.error(f"Stream publish error for {stream_key}: {e}")
            return False

    def _flush_batch(self):
        """Flush batch buffer to main buffer and queue"""
        if not self.batch_buffer:
            return
        
        # ✅ FIXED: Ensure all required attributes are initialized
        if not hasattr(self, 'buffer_lock'):
            import threading
            self.buffer_lock = threading.Lock()
        if not hasattr(self, 'tick_buffer'):
            from collections import deque
            self.tick_buffer = deque()
        if not hasattr(self, 'batch_buffer'):
            self.batch_buffer = []
        if not hasattr(self, 'tick_queue'):
            import queue
            self.tick_queue = queue.Queue()

        with self.buffer_lock:
            # Extend tick buffer with batch
            self.tick_buffer.extend(self.batch_buffer)

            # Also add to queue for timeout-based retrieval
            for tick in self.batch_buffer:
                try:
                    self.tick_queue.put_nowait(tick)
                except queue.Full:
                    # If queue is full, remove oldest and add new
                    try:
                        self.tick_queue.get_nowait()
                        self.tick_queue.put_nowait(tick)
                    except queue.Empty:
                        pass

        batch_size = len(self.batch_buffer)
        
        # ✅ FIXED: Publish tick data to Redis Streams (indicators from WebSocket parser/scanner_main)
        if hasattr(self, 'redis_client') and self.redis_client:
            # Group ticks by symbol for batch processing
            symbol_ticks = {}
            for tick in self.batch_buffer:
                try:
                    symbol = tick.get('symbol', 'UNKNOWN')
                    if symbol not in symbol_ticks:
                        symbol_ticks[symbol] = []
                    symbol_ticks[symbol].append(tick)
                    
                    # Publish to tick stream for real-time processing in DB 1 (realtime)
                    # Ensure tick data has the symbol field populated
                    tick['symbol'] = symbol
                    # ✅ FIXED: Use DatabaseAwareKeyBuilder for tick streams
                    stream_key = DatabaseAwareKeyBuilder.live_ticks_raw_stream(symbol)
                    realtime_client = self.realtime_client
                    
                    # Check if key exists and is not a stream, delete it to avoid WRONGTYPE error
                    key_type = realtime_client.type(stream_key)
                    if key_type and key_type != 'stream':
                        realtime_client.delete(stream_key)
                    
                    # Convert dict to proper stream format
                    stream_data = {
                        'data': json.dumps(tick, default=str),
                        'timestamp': str(int(time.time() * 1000)),
                        'symbol': symbol
                    }
                    try:
                        # ✅ FIXED: ALWAYS use maxlen in XADD to prevent unbounded stream growth
                        if self._publish_to_stream_with_maxlen(realtime_client, stream_key, stream_data):
                            if hasattr(self, 'stats'):
                                self.stats["ticks_published_to_stream"] = self.stats.get("ticks_published_to_stream", 0) + 1
                    except redis.ConnectionError as conn_err:
                        if "Too many connections" in str(conn_err):
                            # Don't log every single tick failure - just increment error counter
                            if hasattr(self, 'stats'):
                                self.stats["connection_errors"] = self.stats.get("connection_errors", 0) + 1
                                # Log only once per 100 errors to avoid log spam
                                if self.stats.get("connection_errors", 0) % 100 == 1:
                                    self.logger.warning(f"⚠️ Connection pool exhausted (suppressing further logs until resolved)")
                        else:
                            raise  # Re-raise non-connection errors
                except Exception as e:
                    # Handle other errors (but not connection exhaustion)
                    if "Too many connections" not in str(e):
                        self.logger.error(f"Failed to publish tick to stream: {e}")
                    # Increment error counter silently for connection exhaustion
                    if hasattr(self, 'stats'):
                        self.stats["errors"] = self.stats.get("errors", 0) + 1
            
            # DataPipeline only publishes ticks to Redis streams - no local calculations
            if symbol_ticks:
                self.logger.debug(f"📊 [BATCH_FLUSH] Published {len(symbol_ticks)} symbols to Redis streams (indicators from WebSocket parser/scanner_main)")
        
        self.batch_buffer.clear()
        self.last_batch_time = time.time()
        if hasattr(self, 'stats'):
            self.stats["batches_processed"] += 1


    def _extract_underlying_from_symbol(self, symbol: str) -> str:
        """Extract underlying symbol from option/future symbols using shared parser."""
        if not symbol:
            return ""
        underlying = extract_underlying_from_symbol(symbol)
        return underlying or ""

    def _get_sector_from_symbol_name(self, symbol: str) -> str:
        """Map symbol to sector based on naming patterns"""
        if not symbol:
            return ""
        
        symbol_upper = symbol.upper()
        bank_keywords = ['BANK', 'HDFC', 'ICICI', 'SBI', 'AXIS', 'KOTAK', 'INDUSIND', 
                         'YESBANK', 'FEDERAL', 'IDFC', 'BANDHAN', 'RBL', 'UNION',
                         'PNB', 'CANARA', 'BANKOFBARODA', 'BANKOFINDIA']
        it_keywords = ['INFOSYS', 'TCS', 'WIPRO', 'HCL', 'TECHMAHINDRA', 'LTIM', 
                       'LTTS', 'PERSISTENT', 'MINDTREE', 'COFORGE', 'MPHASIS']
        auto_keywords = ['MARUTI', 'M&M', 'TATA', 'MOTORS', 'BAJAJ', 'HERO', 
                         'EICHER', 'ASHOK', 'LEYLAND', 'TVS', 'MOTHERSUM']
        pharma_keywords = ['SUNPHARMA', 'DRREDDY', 'CIPLA', 'LUPIN', 'TORRENT', 
                           'AUROBINDO', 'DIVIS', 'GLENMARK', 'CADILA']
        fmcg_keywords = ['HUL', 'ITC', 'NESTLE', 'DABUR', 'MARICO', 'BRITANNIA', 
                         'GODREJ', 'TATA', 'CONSUMER']
        energy_keywords = ['RELIANCE', 'ONGC', 'GAIL', 'IOC', 'BPCL', 'HPCL', 
                           'OIL', 'PETRONET']
        metal_keywords = ['TATASTEEL', 'JSWSTEEL', 'SAIL', 'JINDAL', 'VEDANTA', 
                          'HINDALCO', 'NMDC']
        
        for keyword in bank_keywords:
            if keyword in symbol_upper:
                return "BANK"
        for keyword in it_keywords:
            if keyword in symbol_upper:
                return "IT"
        for keyword in auto_keywords:
            if keyword in symbol_upper:
                return "AUTO"
        for keyword in pharma_keywords:
            if keyword in symbol_upper:
                return "PHARMA"
        for keyword in fmcg_keywords:
            if keyword in symbol_upper:
                return "FMCG"
        for keyword in energy_keywords:
            if keyword in symbol_upper:
                return "ENERGY"
        for keyword in metal_keywords:
            if keyword in symbol_upper:
                return "METAL"
        return ""


    def _publish_to_scanner_stream(self, symbol, indicators):
        """Publish enriched data to scanner stream (Phase 3)"""
        try:
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            scanner_stream = DatabaseAwareKeyBuilder.live_enriched_stream()
            
            # Enrich with required pattern detection fields
            # Ensure we serialize complex types for Redis
            enriched_data = {
                'symbol': symbol,
                'canonical_symbol': indicators.get('symbol', symbol),
                'last_price': indicators.get('last_price', 0),
                'volume': indicators.get('volume', 0),
                'volume_ratio': indicators.get('volume_ratio', 0),
                'delta': indicators.get('delta'),
                'gamma': indicators.get('gamma'),
                'theta': indicators.get('theta'),
                'vega': indicators.get('vega'),
                'iv': indicators.get('iv'),
                'oi': indicators.get('oi', 0),
                'underlying_price': indicators.get('underlying_price', 0),
                'timestamp': time.time(),
                # Dump full indicators as JSON backup
                'indicators': json.dumps({k: v for k, v in indicators.items() if isinstance(v, (str, int, float, bool, type(None)))}),
            }
            
            # Remove None values
            enriched_data = {k: v for k, v in enriched_data.items() if v is not None}
            
            self.realtime_client.xadd(
                scanner_stream,
                enriched_data,
                maxlen=5000, 
            )
        except Exception as e:
            self.logger.error(f"❌ Failed to publish to scanner stream: {e}")

    def _process_tick_for_patterns_fast(self, tick_data):
        """Non-blocking pattern detection via background threads."""
        # ✅ FIXED: Use unified symbol extraction method
        symbol = self._get_unified_symbol(tick_data)
        if not symbol:
            self.logger.warning(f"⚠️ [PATTERN_DETECTION_FLOW] _process_tick_for_patterns_fast: No symbol extracted from tick_data")
            return

        # ✅ FIX: ALWAYS publish enriched data to scanner stream (Phase 3)
        # This is required for downstream consumers (scanner_main) regardless of pattern detection
        try:
            self._publish_to_scanner_stream(symbol, tick_data)
        except Exception as e:
            self.logger.debug(f"⚠️ Failed to publish to scanner stream for {symbol}: {e}")

        # Pattern detection is optional (requires pattern_detector to be initialized)
        if hasattr(self, "pattern_detector") and self.pattern_detector:
            import threading
            worker = threading.Thread(
                target=self._async_pattern_detection,
                args=(symbol, tick_data),
                daemon=True,
            )
            worker.start()
        else:
            self.logger.debug(f"   ⏭️ pattern_detector not available, skipping pattern detection for {symbol}")

        if hasattr(self, "stats"):
            self.stats["ticks_processed"] = self.stats.get("ticks_processed", 0) + 1

    def _is_index_symbol(self, symbol: str) -> bool:
        """
        Check if symbol is an index (not a tradeable instrument).
        Indices should be skipped for pattern detection and volume ratio checks.
        """
        if not symbol:
            return False
        # ✅ FIX: Use replace() to remove ALL embedded quotes (strip only removes edge quotes)
        symbol_upper = str(symbol).upper().replace("'", "").replace('"', "").replace("`", "").strip()
        
        # Known index symbols (used for underlying price, VIX, etc.)
        index_patterns = [
            "NSENIFTY50", "NSENIFTYBANK", "NSESENSEX",  # NSE format
            "BSENIFTY50", "BSENIFTYBANK", "BSESENSEX",  # BSE format  
            "BFOBSENSEX", "BFOBSENSEX50", "BFOSENSEX",  # BFO index format (both with and without B)
            "NIFTY 50", "NIFTY50", "NIFTYBANK", "SENSEX",  # Common aliases
            "NSE:NIFTY 50", "NSE:NIFTYBANK", "NSE:SENSEX",  # Prefixed
            "BSE:SENSEX", "BFO:SENSEX",  # BSE prefixed
            "INDEX:", "INDIAVIX",  # Index prefix and VIX
        ]
        for pattern in index_patterns:
            if pattern in symbol_upper:
                return True
        # Also check if symbol starts with "index:" (Redis key format)
        if symbol_upper.startswith("INDEX:"):
            return True
        # ✅ FIX: Check for known index-only underlyings (no options suffix)
        # These are pure index symbols without CE/PE/FUT suffix
        pure_index_underlyings = ["NIFTY50", "NIFTYBANK", "SENSEX", "INDIAVIX", "FINNIFTY", "MIDCPNIFTY"]
        if symbol_upper in pure_index_underlyings:
            return True
        # Check with NSE/BSE/BFO prefix  
        for prefix in ["NSE", "BSE", "NFO", "BFO"]:
            for idx in pure_index_underlyings:
                if symbol_upper == f"{prefix}{idx}":
                    return True
        return False

    def _async_pattern_detection(self, symbol, tick_data):
        """Background pattern detection with full tick data including all indicators, Greeks, volume_ratio, price_change."""
        try:
            # ✅ FIX: Skip pattern detection for index symbols
            # Indices (NSENIFTY50, NSENIFTYBANK, NSESENSEX) are used for underlying price, VIX regime, etc.
            # but should NOT have trading patterns detected on them
            if self._is_index_symbol(symbol):
                self.logger.debug(f"⏭️ [INDEX_SKIP] Skipping pattern detection for index symbol: {symbol}")
                return
            
            # Use full tick_data with ALL fields preserved (indicators, Greeks, volume_ratio, price_change, microstructure, etc.)
            indicators = tick_data.copy() if isinstance(tick_data, dict) else tick_data

            if not indicators or not hasattr(self, "pattern_detector") or not self.pattern_detector:
                self.logger.warning(f"   ❌ Skipping pattern detection: indicators={bool(indicators)}, pattern_detector={hasattr(self, 'pattern_detector')}")
                return
            
            # ✅ CRITICAL FIX: Ensure symbol is in indicators dict (required by detect_patterns)
            # Even though symbol is passed as parameter, detect_patterns() expects it in indicators dict
            if not isinstance(indicators, dict):
                self.logger.error(f"   ❌ indicators is not a dict: {type(indicators)}")
                return
            
            # Ensure symbol field exists in indicators (detect_patterns reads from indicators.get("symbol"))
            if "symbol" not in indicators or not indicators.get("symbol") or indicators.get("symbol") == "UNKNOWN":
                if symbol and symbol != "UNKNOWN":
                    indicators["symbol"] = symbol
                    # Also set tradingsymbol for compatibility
                    if "tradingsymbol" not in indicators:
                        indicators["tradingsymbol"] = symbol
                    self.logger.debug(f"   ✅ Added symbol={symbol} to indicators dict")
                else:
                    self.logger.error(f"   ❌ Cannot add symbol to indicators: symbol={symbol}, indicators keys: {list(indicators.keys())[:20]}")
                    return
            
            # ✅ LOG: What data is being passed to pattern_detector
            self.logger.info(f"   🎯 Calling pattern_detector.detect_patterns() with {len(indicators)} indicators")

            # ✅ NEW: Publish enriched data to scanner stream (Phase 3)
            self._publish_to_scanner_stream(symbol, indicators)
            
            greeks_in_indicators = [f for f in ['delta', 'gamma', 'theta', 'vega', 'rho'] if indicators.get(f) not in (None, 0.0)]
            volume_ratio = indicators.get('volume_ratio')
            symbol_in_indicators = indicators.get('symbol', 'MISSING')
            self._ensure_gamma_exposure(symbol_in_indicators, indicators, greeks)  # compute if missing
            self.logger.info(
                "   📊 Indicators summary: symbol=%s, Greeks=%s/5, volume_ratio=%s, gamma_exposure=%s, "
                "order_flow_imbalance=%s, vix_level=%s, total_fields=%s",
                symbol_in_indicators,
                len(greeks_in_indicators),
                volume_ratio,
                indicators.get('gamma_exposure'),
                indicators.get('order_flow_imbalance'),
                indicators.get('vix_level') or indicators.get('vix_regime'),
                len(indicators),
            )
            critical_snapshot = {
                'delta': indicators.get('delta'),
                'gamma': indicators.get('gamma'),
                'gamma_exposure': indicators.get('gamma_exposure'),
                'order_flow_imbalance': indicators.get('order_flow_imbalance'),
                'vix_level': indicators.get('vix_level'),
                'volume_ratio': indicators.get('volume_ratio'),
            }
            self.logger.info(f"   🧩 Pattern input snapshot: {critical_snapshot}")
            indicators_keys = list(indicators.keys())
            greek_key_candidates = ['delta', 'gamma', 'theta', 'vega', 'rho', 'gamma_exposure']
            greeks_keys = [key for key in greek_key_candidates if indicators.get(key) is not None]
            if DEBUG_TRACE_ENABLED:
                self.logger.debug(
                    "📡 [DP→PD] %s → detect_patterns() | ind: %s fields | greeks: %s fields | sample: %s",
                    symbol_in_indicators,
                    len(indicators_keys),
                    len(greeks_keys),
                    indicators_keys[:8],
                )
            if 'gamma_exposure' not in indicators:
                self.logger.warning(
                    f"⚠️ [MISSING] {symbol_in_indicators} - gamma_exposure NOT in indicators → "
                    "gamma_exposure_reversal may skip"
                )

            # ✅ FIX: Wire game theory evaluation into pattern detection
            game_theory_signal = None
            if hasattr(self, "scanner") and self.scanner:
                if hasattr(self.scanner, "_should_run_game_theory") and self.scanner._should_run_game_theory(symbol):
                    try:
                        game_theory_signal = self.scanner._evaluate_game_theory_layer(symbol, tick_data, indicators)
                        if game_theory_signal:
                            indicators['game_theory_signal'] = game_theory_signal
                            # Generate game theory alerts if enabled
                            if hasattr(self.scanner, "game_theory_alert_generator") and self.scanner.game_theory_alert_generator:
                                try:
                                    gt_alerts = self.scanner.game_theory_alert_generator.generate_intraday_alerts([game_theory_signal])
                                    for gt_alert in gt_alerts:
                                        if hasattr(self, "alert_manager") and self.alert_manager:
                                            self.alert_manager.send_alert(gt_alert)
                                except Exception as gt_alert_err:
                                    self.logger.debug(f"Game theory alert generation error for {symbol}: {gt_alert_err}")
                    except Exception as gt_err:
                        self.logger.debug(f"Game theory evaluation error for {symbol}: {gt_err}")

            patterns = self.pattern_detector.detect_patterns(indicators) or []
            self.logger.info(f"   ✅ pattern_detector.detect_patterns() returned {len(patterns)} patterns")
            if patterns:
                pattern_names = [p.get('pattern', p.get('pattern_type', 'unknown')) for p in patterns]
                self.logger.info(f"   🎯 Detected patterns: {pattern_names}")
            else:
                self.logger.warning(f"   ❌ No patterns detected for {symbol}")
            
            if not patterns or not hasattr(self, "alert_manager") or not self.alert_manager:
                if not patterns:
                    self.logger.info(f"   ⏭️ No patterns to send alerts for {symbol}")
                elif not hasattr(self, "alert_manager"):
                    self.logger.warning(f"   ⚠️ alert_manager not available")
                return

            for pattern in patterns:
                pattern["symbol"] = symbol
                if isinstance(indicators, dict):
                    # ✅ CRITICAL: Ensure indicators dict exists and contains ALL computed indicators
                    if "indicators" not in pattern or not isinstance(pattern.get("indicators"), dict):
                        pattern["indicators"] = {}
                    # ✅ PRESERVE: Merge ALL indicators (including microstructure, advanced, etc.)
                    pattern["indicators"].update(indicators)
                    
                    # ✅ COMPREHENSIVE: Add all indicators to top-level for easy access
                    # This ensures all computed indicators are available in alert payload
                    for indicator_key, indicator_value in indicators.items():
                        if indicator_key not in pattern and indicator_key not in ['symbol', 'timestamp', 'timestamp_ms']:
                            pattern[indicator_key] = indicator_value
                
                # ✅ FIX: Apply game theory confirmation to patterns
                if game_theory_signal and hasattr(self, "scanner") and self.scanner:
                    if hasattr(self.scanner, "_game_theory_confirms"):
                        if not self.scanner._game_theory_confirms(symbol, pattern, game_theory_signal):
                            # Skip pattern if game theory doesn't confirm
                            continue
                        # Fuse game theory context into pattern
                        if hasattr(self.scanner, "_fuse_all_signals"):
                            fused = self.scanner._fuse_all_signals(symbol, pattern, game_theory_signal)
                            if fused:
                                pattern.update(fused)
                
                self.alert_manager.send_alert(pattern)
        except Exception as exc:
            self.logger.debug(f"Async pattern detection error for {symbol}: {exc}")



    def _get_underlying_price(self, option_symbol: str) -> float:
        """Get underlying last_price for options using the shared resolver."""
        try:
            cache_writer = None
            if hasattr(self, 'price_cache') and isinstance(self.price_cache, dict):
                def cache_writer(symbol: str, price: float) -> None:
                    self.price_cache[symbol] = price
            return resolve_underlying_price_helper(
                option_symbol,
                redis_client=getattr(self, 'redis_client', None),
                logger_obj=self.logger,
                historical_queries=getattr(self, 'historical_queries', None),
                cache_store_fn=cache_writer,
            )
        except Exception as e:
            self.logger.debug(f"Failed to get underlying last_price for {option_symbol}: {e}")
            return 0.0

    def _extract_underlying_symbol(self, option_symbol: str) -> str:
        """Extract underlying symbol from option symbol."""
        try:
            clean_symbol = option_symbol.replace('NFO:', '') if option_symbol else ''
            base_underlying = extract_underlying_from_symbol(option_symbol)
            if base_underlying:
                return f"NFO:{base_underlying}"
            return clean_symbol
        except Exception as e:
            self.logger.debug(f"Failed to extract underlying from {option_symbol}: {e}")
            return ""

    def _update_price_cache(self, symbol: str, last_price: float):
        """Update last_price cache with latest last_price for underlying symbols."""
        try:
            if last_price > 0 and symbol:
                self.price_cache[symbol] = last_price
                # Also update with NFO: prefix if it's an underlying
                if not symbol.startswith('NFO:'):
                    self.price_cache[f"NFO:{symbol}"] = last_price
        except Exception as e:
            self.logger.debug(f"Failed to update last_price cache for {symbol}: {e}")

    def _safe_float(self, value, default=0.0):
        """Safely convert value to float, handling strings and None"""
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _ensure_gamma_exposure(self, symbol: str, indicators: Dict[str, Any], greeks: Optional[Dict[str, Any]]) -> bool:
        """Ensure gamma_exposure is populated when gamma + OI + spot are available."""
        if not symbol or not indicators:
            return False
        existing = indicators.get('gamma_exposure')
        if existing not in (None, '') and self._safe_float(existing, 0.0) != 0.0:
            return False

        greeks_source: Dict[str, Any] = {}
        potential_greeks = indicators.get('greeks')
        if isinstance(potential_greeks, dict):
            greeks_source.update(potential_greeks)
        if isinstance(greeks, dict):
            greeks_source.update(greeks)

        gamma = self._safe_float(indicators.get('gamma') or greeks_source.get('gamma'), 0.0)
        if gamma == 0.0:
            return False

        oi = self._safe_float(indicators.get('open_interest'), 0.0)
        if oi == 0.0:
            return False

        spot = 0.0
        for candidate in (
            indicators.get('underlying_price'),
            indicators.get('spot_price'),
            indicators.get('last_price'),
        ):
            spot = self._safe_float(candidate, 0.0)
            if spot:
                break
        if spot == 0.0:
            return False

        iv = self._safe_float(
            greeks_source.get('iv')
            or greeks_source.get('implied_volatility')
            or indicators.get('iv'),
            0.2,
        )
        iv = max(iv, 0.01)

        gamma_exposure = gamma * oi * spot * spot * iv
        indicators['gamma_exposure'] = gamma_exposure

        if DEBUG_TRACE_ENABLED:
            self.logger.debug(
                "🔬 %s gamma_exposure = %s × %s × %s² × %s = %.2e",
                symbol,
                gamma,
                oi,
                spot,
                iv,
                gamma_exposure,
            )

        try:
            redis_client = getattr(self, 'redis_client', None)
            if redis_client:
                redis_conn = getattr(redis_client, 'redis', redis_client)
                redis_conn.hset(
                    f"ind:{symbol}",
                    "gamma_exposure",
                    str(fix_numpy_serialization(gamma_exposure)),
                )
        except Exception as exc:
            if DEBUG_TRACE_ENABLED:
                self.logger.debug(f"⚠️ [GEX_STORE] {symbol} store failed: {exc}")

        return True

    def _get_volume_from_tick(self, tick_data):
        """Extract bucket_incremental_volume from tick with all possible field names"""
        try:
            volume_fields = [
                "zerodha_cumulative_volume",
                "zerodha_cumulative_volume",
                "bucket_incremental_volume",
                "vol",
                "zerodha_last_traded_quantity",
                "quantity",
            ]
            for field in volume_fields:
                if field in tick_data and tick_data[field] is not None:
                    return int(float(tick_data[field]))
        except Exception:
            pass
        return 0

    def _set_price_change_fields(self, cleaned: dict, value) -> None:
        """Write canonical price change fields using numeric coercion."""
        try:
            numeric = float(value)
            cleaned["price_change_pct"] = numeric
            cleaned["price_change"] = numeric
        except (TypeError, ValueError):
            pass

    def _clean_ohlc_data(self, tick_data, cleaned):
        """Clean OHLC (Open, High, Low, Close) data"""
        symbol = cleaned.get("symbol", "UNKNOWN")
        ohlc = tick_data.get("ohlc", {})
        if isinstance(ohlc, dict):
            cleaned["ohlc"] = {}
            for key in ["open", "high", "low", "close"]:
                val = ohlc.get(key)
                if isinstance(val, dict):
                    cleaned["ohlc"][key] = self._safe_float(val.get("value", 0))
                elif val is not None:
                    cleaned["ohlc"][key] = self._safe_float(val)
                else:
                    cleaned["ohlc"][key] = 0.0
            
            # Debug OHLC preservation
            if cleaned["ohlc"].get("open", 0) == 0.0:
                self.logger.info(f"🔍 [OHLC_DEBUG] {symbol} - OHLC open is 0.0, tick_data ohlc: {ohlc}")
            else:
                self.logger.info(f"🔍 [OHLC_DEBUG] {symbol} - OHLC preserved: open={cleaned['ohlc'].get('open')}, last_price={cleaned.get('last_price')}")
            
            # Flatten OHLC to top-level for indicator consumers
            try:
                cleaned["open"] = self._safe_float(cleaned["ohlc"].get("open", 0))
                cleaned["high"] = self._safe_float(cleaned["ohlc"].get("high", 0))
                cleaned["low"] = self._safe_float(cleaned["ohlc"].get("low", 0))
                cleaned["close"] = self._safe_float(cleaned["ohlc"].get("close", 0))
            except Exception:
                pass
        else:
            # Create default OHLC if missing
            last_price = cleaned.get("last_price", 0)
            cleaned["ohlc"] = {
                "open": self._safe_float(last_price),
                "high": self._safe_float(last_price),
                "low": self._safe_float(last_price),
                "close": self._safe_float(last_price),
            }
            # Also set flattened defaults
            cleaned["open"] = cleaned["ohlc"]["open"]
            cleaned["high"] = cleaned["ohlc"]["high"]
            cleaned["low"] = cleaned["ohlc"]["low"]
            cleaned["close"] = cleaned["ohlc"]["close"]

    def _clean_depth_data(self, tick_data, cleaned):
        """Canonicalizes multiple possible upstream shapes into:
            cleaned['depth'] = {'buy': [...], 'sell': [...]} and sets 'depth_valid'.
        Supports:
            - tick_data['depth'] as a dict with buy/sell (Kite standard)
            - 'depth_levels_buy' / 'depth_levels_sell' (crawler alternates)
            - 'depth_levels' as a dict with buy/sell (crawler consolidated)
        """

        buy_lvls = None
        sell_lvls = None

        # Alternates (crawler variants)
        if isinstance(tick_data.get("depth_levels_buy"), list):
            buy_lvls = tick_data.get("depth_levels_buy")
        if isinstance(tick_data.get("depth_levels_sell"), list):
            sell_lvls = tick_data.get("depth_levels_sell")

        # Kite-standard
        depth_obj = tick_data.get("depth")
        if isinstance(depth_obj, dict):
            if buy_lvls is None and isinstance(depth_obj.get("buy"), list):
                buy_lvls = depth_obj.get("buy")
            if sell_lvls is None and isinstance(depth_obj.get("sell"), list):
                sell_lvls = depth_obj.get("sell")

        # Consolidated 'depth_levels' (crawler)
        levels = tick_data.get("depth_levels")
        if isinstance(levels, dict):
            if buy_lvls is None and isinstance(levels.get("buy"), list):
                buy_lvls = levels.get("buy")
            if sell_lvls is None and isinstance(levels.get("sell"), list):
                sell_lvls = levels.get("sell")

        if buy_lvls or sell_lvls:
            depth_payload = {"buy": buy_lvls or [], "sell": sell_lvls or []}
            cleaned["depth"] = depth_payload
            cleaned["order_book"] = depth_payload
            cleaned["depth_data"] = depth_payload
            cleaned["depth_valid"] = bool(
                depth_payload.get("buy") or depth_payload.get("sell")
            )
            # Flatten best bid/ask to top-level for indicator consumers
            try:
                best_bid = (
                    cleaned_depth.get("buy", [{}])[0] if cleaned_depth.get("buy") else {}
                )
                best_ask = (
                    cleaned_depth.get("sell", [{}])[0] if cleaned_depth.get("sell") else {}
                )
                if best_bid:
                    if "last_price" in best_bid:
                        cleaned["best_bid_price"] = self._safe_float(best_bid.get("last_price"))
                    if "quantity" in best_bid:
                        cleaned["best_bid_quantity"] = self._safe_float(best_bid.get("quantity"))
                if best_ask:
                    if "last_price" in best_ask:
                        cleaned["best_ask_price"] = self._safe_float(best_ask.get("last_price"))
                    if "quantity" in best_ask:
                        cleaned["best_ask_quantity"] = self._safe_float(best_ask.get("quantity"))
            except Exception:
                pass
        else:
            cleaned["depth"] = {"buy": [], "sell": []}
            cleaned["order_book"] = {"buy": [], "sell": []}
            cleaned["depth_data"] = {"buy": [], "sell": []}
            cleaned["depth_valid"] = False

    def _attach_order_book_snapshot(self, symbol: str, cleaned: dict, redis_data: Optional[dict] = None) -> None:
        """Ensure cleaned tick carries an order_book snapshot for downstream consumers."""
        try:
            existing = cleaned.get("order_book")
            if isinstance(existing, dict):
                has_levels = bool(existing.get("buy") or existing.get("sell"))
                if has_levels:
                    return
            
            # Try Redis data merged earlier
            candidate = None
            if isinstance(redis_data, dict):
                candidate = (
                    redis_data.get("order_book")
                    or redis_data.get("depth")
                    or redis_data.get("depth_data")
                )
            if candidate and isinstance(candidate, str):
                try:
                    candidate = json.loads(candidate)
                except (json.JSONDecodeError, TypeError):
                    candidate = None
            
            if isinstance(candidate, dict) and candidate:
                cleaned["order_book"] = candidate
                cleaned["depth_data"] = candidate
                return
            
            # Fallback: use UnifiedDataStorage snapshot
            if getattr(self, "_unified_storage", None):
                order_book = self._unified_storage.get_order_book(symbol)
                if order_book:
                    cleaned["order_book"] = order_book
                    cleaned["depth_data"] = order_book
        except Exception as exc:
                self.logger.debug(f"⚠️ [ORDER_BOOK_ATTACH] Failed to attach order book for {symbol}: {exc}")

    def _attach_tick_history(self, symbol: str, cleaned: dict, limit: int = 32) -> None:
        """Attach recent tick history to cleaned data for game theory/microstructure detectors."""
        try:
            existing_ticks = cleaned.get("ticks") or cleaned.get("tick_history")
            if isinstance(existing_ticks, list) and len(existing_ticks) > 0:
                cleaned.setdefault("tick_history", existing_ticks)
                return
            
            if getattr(self, "_unified_storage", None):
                tick_history = self._unified_storage.get_recent_ticks(symbol, limit=limit)
                if tick_history:
                    cleaned["ticks"] = tick_history
                    cleaned["tick_history"] = tick_history
        except Exception as exc:
            self.logger.debug(f"⚠️ [TICK_HISTORY_ATTACH] Failed to attach tick history for {symbol}: {exc}")

    def _clean_timestamps(self, tick_data, cleaned):
        """Handle timestamp fields with one-time normalization to epoch milliseconds using Zerodha field names."""
        
        # ✅ ONE-TIME NORMALIZATION: Convert any timestamp format to epoch milliseconds
        # Use Zerodha field names exactly as per optimized_field_mapping.yaml
        
        # Primary timestamp: exchange_timestamp (preferred by Zerodha)
        exchange_timestamp_raw = tick_data.get("exchange_timestamp")
        if not exchange_timestamp_raw:
            # Some crawlers publish exchange_timestamp_epoch (seconds)
            exchange_timestamp_raw = tick_data.get("exchange_timestamp_epoch")
        exchange_timestamp_ms = tick_data.get("exchange_timestamp_ms") or tick_data.get("timestamp_ms")
        if exchange_timestamp_raw:
            exchange_timestamp_epoch = TimestampNormalizer.to_epoch_ms(exchange_timestamp_raw)
            cleaned["exchange_timestamp"] = exchange_timestamp_raw  # Keep original
            cleaned["exchange_timestamp_ms"] = exchange_timestamp_epoch  # Always epoch milliseconds
            # Set timestamp fields for downstream consumers
            cleaned["timestamp"] = exchange_timestamp_epoch  # Primary timestamp
            cleaned["timestamp_ms"] = exchange_timestamp_epoch
        elif exchange_timestamp_ms:
            # ✅ FIXED: exchange_timestamp_ms from websocket_parser is already in epoch milliseconds
            # Don't call TimestampNormalizer.to_epoch_ms() on it - it's already normalized
            try:
                # ✅ FIXED: Correct epoch length detection (same as intraday_crawler)
                ts_ms_numeric = self._safe_float(exchange_timestamp_ms, 0.0)
                if ts_ms_numeric >= 1e15:
                    # Nanoseconds - convert to milliseconds
                    exchange_timestamp_epoch = int(ts_ms_numeric / 1_000_000)
                elif ts_ms_numeric >= 1e10:
                    # Already in milliseconds (epoch milliseconds)
                    exchange_timestamp_epoch = int(ts_ms_numeric)
                else:
                    # Seconds or string - try to parse it
                    exchange_timestamp_epoch = TimestampNormalizer.to_epoch_ms(exchange_timestamp_ms)
            except (TypeError, ValueError):
                # Fallback: try to parse as string/other format
                exchange_timestamp_epoch = TimestampNormalizer.to_epoch_ms(exchange_timestamp_ms)
            cleaned["exchange_timestamp_ms"] = exchange_timestamp_epoch
            cleaned["timestamp"] = exchange_timestamp_epoch
            cleaned["timestamp_ms"] = exchange_timestamp_epoch
        else:
            # Fallback to timestamp_ns if exchange_timestamp not available
            timestamp_ns_raw = tick_data.get("timestamp_ns")
            if timestamp_ns_raw:
                # ✅ FIXED: Correct epoch length detection (same as intraday_crawler)
                if isinstance(timestamp_ns_raw, (int, float)):
                    ts_ns = float(timestamp_ns_raw)
                    if ts_ns >= 1e15:
                        # Nanoseconds - convert to milliseconds
                        timestamp_ns_epoch = int(ts_ns / 1_000_000)
                    elif ts_ns >= 1e10:
                        # Already in milliseconds
                        timestamp_ns_epoch = int(ts_ns)
                    else:
                        # Seconds - convert to milliseconds
                        timestamp_ns_epoch = int(ts_ns * 1000)
                else:
                    # String or other format - use TimestampNormalizer
                    timestamp_ns_epoch = TimestampNormalizer.to_epoch_ms(timestamp_ns_raw)
                cleaned["timestamp_ns"] = timestamp_ns_raw  # Keep original
                cleaned["timestamp_ns_ms"] = timestamp_ns_epoch  # Always epoch milliseconds
                cleaned["timestamp"] = timestamp_ns_epoch
                cleaned["timestamp_ms"] = timestamp_ns_epoch
            else:
                # Final fallback to legacy timestamp
                timestamp_raw = tick_data.get("timestamp")
                if not timestamp_raw:
                    timestamp_raw = get_current_ist_timestamp()
                timestamp_epoch = TimestampNormalizer.to_epoch_ms(timestamp_raw)
                cleaned["timestamp"] = timestamp_epoch  # Primary timestamp for calculations
                cleaned["timestamp_ms"] = timestamp_epoch  # Always epoch milliseconds
        
        # Last trade time - Zerodha specific field
        last_trade_time_raw = tick_data.get("last_trade_time")
        if last_trade_time_raw:
            last_trade_time_epoch = TimestampNormalizer.to_epoch_ms(last_trade_time_raw)
            cleaned["last_trade_time"] = last_trade_time_raw  # Keep original
            cleaned["last_trade_time_ms"] = last_trade_time_epoch  # Always epoch milliseconds

    def _calculate_derived_fields(self, cleaned):
        """
        ✅ SINGLE SOURCE OF TRUTH: Calculate derived fields - price_change comes from websocket parser.
        Priority:
        1. Use price_change from tick_data (computed by websocket parser) - PRIMARY SOURCE
        2. Map Zerodha's 'change'/'net_change' fields if price_change is missing
        3. Only calculate as last resort if websocket parser didn't provide it
        
        This ensures websocket parser is the single source of truth for price_change calculations.
        """
        symbol = cleaned.get("symbol", "UNKNOWN")
        
        try:
            # ✅ SINGLE SOURCE OF TRUTH: price_change is computed by websocket_parser
            # Check if websocket parser already calculated it
            existing_price_change_pct = cleaned.get("price_change_pct")
            existing_price_change = cleaned.get("price_change")
            
            # ✅ PRIORITY 1: Use websocket parser's calculation if available
            if existing_price_change_pct is not None or existing_price_change is not None:
                # Websocket parser already calculated it - use it and set aliases
                if existing_price_change_pct is not None:
                    cleaned["price_change"] = existing_price_change_pct
                    cleaned["net_change"] = existing_price_change_pct
                    cleaned["change"] = existing_price_change_pct
                elif existing_price_change is not None:
                    cleaned["price_change_pct"] = existing_price_change
                    cleaned["net_change"] = existing_price_change
                    cleaned["change"] = existing_price_change
                self.logger.debug(f"✅ [PRICE_CHANGE] {symbol} - Using websocket parser's price_change (single source of truth)")
                return  # Early return - websocket parser is the source of truth
            
            # ✅ PRIORITY 2: Map Zerodha's 'change'/'net_change' fields if price_change is missing
            zerodha_change = cleaned.get("change") or cleaned.get("net_change")
            if zerodha_change is not None:
                try:
                    zerodha_change_float = float(zerodha_change)
                    cleaned["price_change_pct"] = zerodha_change_float
                    cleaned["price_change"] = zerodha_change_float
                    cleaned["net_change"] = zerodha_change_float
                    cleaned["change"] = zerodha_change_float
                    self.logger.debug(f"✅ [PRICE_CHANGE] {symbol} - Mapped Zerodha change field: {zerodha_change_float:.4f}%")
                    return  # Early return - Zerodha field is the source
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"⚠️ [PRICE_CHANGE] {symbol} - Could not convert Zerodha change: {e}")
            
            # ✅ PRIORITY 3: Last resort - calculate only if websocket parser didn't provide it
            # This should rarely happen if websocket parser is working correctly
            ohlc_data = cleaned.get("ohlc", {})
            open_price = self._safe_float(ohlc_data.get("open", 0))
            last_price = self._safe_float(cleaned.get("last_price", 0))
            
            if open_price > 0 and last_price > 0:
                computed_change = ((last_price - open_price) / open_price) * 100
                cleaned["price_change_pct"] = computed_change
                cleaned["price_change"] = computed_change
                cleaned["net_change"] = computed_change
                cleaned["change"] = computed_change
                self.logger.debug(f"⚠️ [PRICE_CHANGE] {symbol} - Calculated as fallback (websocket parser didn't provide): {computed_change:.4f}%")
            else:
                # Try Redis as last resort
                try:
                    canonical_symbol = cleaned.get("symbol", symbol)
                    redis_data = self._get_complete_redis_data(canonical_symbol)
                    base_price = redis_data.get('open') or redis_data.get('ohlc_open')
                    if base_price and base_price > 0 and last_price > 0:
                        computed_change = ((last_price - base_price) / base_price) * 100
                        cleaned["price_change_pct"] = computed_change
                        cleaned["price_change"] = computed_change
                        cleaned["net_change"] = computed_change
                        cleaned["change"] = computed_change
                        self.logger.debug(f"⚠️ [PRICE_CHANGE] {symbol} - Calculated from Redis OHLC (websocket parser didn't provide): {computed_change:.4f}%")
                except Exception as e:
                    self.logger.debug(f"⚠️ [PRICE_CHANGE] {symbol} - Could not calculate price_change: {e}")
                    # Set to 0.0 only if truly can't calculate
                    if open_price == 0 and last_price == 0:
                        cleaned["price_change_pct"] = 0.0
                        cleaned["price_change"] = 0.0
                        cleaned["net_change"] = 0.0  # Set alias
                        cleaned["change"] = 0.0  # Set alias
                        self.logger.debug(f"🔍 [PRICE_CHANGE_DEBUG] {symbol} - price_change set to 0.0 (both open_price and last_price are 0, cannot calculate)")
                    else:
                        # At least one price is available - don't set to 0.0 yet
                        # Let it remain None so downstream can try to calculate or fetch from Redis
                        self.logger.debug(f"🔍 [PRICE_CHANGE_DEBUG] {symbol} - price_change not set (open_price={open_price}, last_price={last_price}, will try Redis OHLC or leave as None)")
            
            # ✅ CRITICAL: Final sync - ensure price_change, price_change_pct, and net_change are always aliases
            # This ensures canonical field mapping is enforced regardless of which field was set first
            final_price_change = cleaned.get("price_change")
            final_price_change_pct = cleaned.get("price_change_pct")
            final_net_change = cleaned.get("net_change")
            
            # Find the first non-zero, non-None value to use as the source of truth
            source_value = None
            source_field = None
            for field, value in [("price_change", final_price_change), 
                                  ("price_change_pct", final_price_change_pct),
                                  ("net_change", final_net_change)]:
                if value is not None and abs(value) > 0.001:
                    source_value = value
                    source_field = field
                    break
            
            # If we found a source value, sync all fields to it
            if source_value is not None:
                if abs(final_price_change or 0) < 0.001:
                    cleaned["price_change"] = source_value
                if abs(final_price_change_pct or 0) < 0.001:
                    cleaned["price_change_pct"] = source_value
                if abs(final_net_change or 0) < 0.001:
                    cleaned["net_change"] = source_value
                cleaned["change"] = source_value
                self.logger.debug(f"✅ [PRICE_CHANGE_DEBUG] {symbol} - Synced all price fields to {source_field}={source_value:.4f}% (alias mapping)")
            elif final_price_change_pct is not None and (final_net_change is None or abs(final_net_change) < 0.001):
                cleaned["net_change"] = final_price_change_pct
                cleaned["change"] = final_price_change_pct
                if abs(final_price_change or 0) < 0.001:
                    cleaned["price_change"] = final_price_change_pct
                self.logger.debug(f"✅ [PRICE_CHANGE_DEBUG] {symbol} - Synced net_change={final_price_change_pct:.4f}% from price_change_pct (alias mapping)")
            elif final_net_change is not None and (final_price_change_pct is None or abs(final_price_change_pct) < 0.001):
                cleaned["price_change_pct"] = final_net_change
                cleaned["price_change"] = final_net_change
                cleaned["change"] = final_net_change
                self.logger.debug(f"✅ [PRICE_CHANGE_DEBUG] {symbol} - Synced price_change_pct={final_net_change:.4f}% from net_change (alias mapping)")
            elif final_price_change is not None and (final_price_change_pct is None or abs(final_price_change_pct) < 0.001):
                cleaned["price_change_pct"] = final_price_change
                cleaned["net_change"] = final_price_change
                cleaned["change"] = final_price_change
                self.logger.debug(f"✅ [PRICE_CHANGE_DEBUG] {symbol} - Synced price_change_pct={final_price_change:.4f}% from price_change (alias mapping)")
        except (KeyError, TypeError, ZeroDivisionError) as e:
            # ✅ FIXED: Don't set to 0.0 on exception - only set if truly missing
            # This prevents overwriting valid None values that downstream can calculate
            if "price_change_pct" not in cleaned:
                # Only set if field doesn't exist at all, not if it's None
                # Leave None so downstream can try to calculate
                self.logger.debug(f"🔍 [PRICE_CHANGE_DEBUG] {symbol} - Exception calculating price_change: {e} (leaving as None for downstream calculation)")
            elif cleaned.get("price_change_pct") is None:
                # Field exists but is None - try to preserve any existing value from other fields
                if "price_change" in cleaned and cleaned.get("price_change") is not None:
                    cleaned["price_change_pct"] = cleaned["price_change"]
                elif "net_change" in cleaned and cleaned.get("net_change") is not None:
                    cleaned["price_change_pct"] = cleaned["net_change"]
                elif "change" in cleaned and cleaned.get("change") is not None:
                    cleaned["price_change_pct"] = cleaned["change"]
                else:
                    # Only set to 0.0 if we've exhausted all options
                    cleaned["price_change_pct"] = 0.0
                    self.logger.debug(f"🔍 [PRICE_CHANGE_DEBUG] {symbol} - Exception: set price_change_pct to 0.0 (no alternative sources)")
            
            # Sync other fields from price_change_pct if available
            if cleaned.get("price_change_pct") is not None:
                if "price_change" not in cleaned or cleaned.get("price_change") is None:
                    cleaned["price_change"] = cleaned["price_change_pct"]
                if "net_change" not in cleaned or cleaned.get("net_change") is None:
                    cleaned["net_change"] = cleaned["price_change_pct"]
                if "change" not in cleaned or cleaned.get("change") is None:
                    cleaned["change"] = cleaned["price_change_pct"]

        # Use unified schema for field mapping and derived calculations
        # ✅ CRITICAL: Only update fields that don't already exist to preserve calculated values
        derived_fields = self.calculate_derived_fields(cleaned)
        # Only add fields that don't exist or are None (preserve existing calculated values)
        for key, value in derived_fields.items():
            if key not in cleaned or cleaned.get(key) is None:
                cleaned[key] = value
            # ✅ FIXED: Don't overwrite price_change_pct if it's already been calculated correctly
            # This prevents config/schemas.py from overwriting our calculated price_change_pct with 0.0

        # Handle data pipeline specific field mappings not in unified schema
        # 2) weighted bid/ask synonyms
        if "weighted_bid_price" not in cleaned and "weighted_bid" in cleaned:
            cleaned["weighted_bid_price"] = self._safe_float(
                cleaned.get("weighted_bid")
            )
        if "weighted_ask_price" not in cleaned and "weighted_ask" in cleaned:
            cleaned["weighted_ask_price"] = self._safe_float(
                cleaned.get("weighted_ask")
            )

        # 3) bid/ask spread and mid last_price if depth available and not supplied
        depth = cleaned.get("depth") if isinstance(cleaned.get("depth"), dict) else None
        if depth:
            try:
                best_bid = (
                    self._safe_float(depth.get("buy", [{}])[0].get("last_price", 0))
                    if depth.get("buy")
                    else 0.0
                )
                best_ask = (
                    self._safe_float(depth.get("sell", [{}])[0].get("last_price", 0))
                    if depth.get("sell")
                    else 0.0
                )
                if "bid_ask_spread" not in cleaned and best_bid and best_ask:
                    cleaned["bid_ask_spread"] = best_ask - best_bid
                if "mid_price" not in cleaned and best_bid and best_ask:
                    cleaned["mid_price"] = (best_bid + best_ask) / 2.0
            except Exception:
                pass
    
    def _is_missing_indicator_value(self, value) -> bool:
        """Return True when a crawler-computed field is missing or blank."""
        if value is None:
            return True
        if isinstance(value, (int, float)):
            return value == 0 or value == 0.0
        if isinstance(value, str):
            trimmed = value.strip()
            if trimmed == "":
                return True
            try:
                numeric = float(trimmed.replace('%', ''))
                return numeric == 0.0
            except (ValueError, TypeError):
                return False
        return False

    def _build_underlying_variants(self, underlying_symbol: str) -> list[str]:
        """Generate candidate Redis keys for an underlying instrument."""
        variants: list[str] = []
        if not underlying_symbol:
            return variants
        try:
            from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
        except Exception:
            RedisKeyStandards = None  # type: ignore
        base_symbol = underlying_symbol
        if ':' in underlying_symbol:
            _, base_symbol = underlying_symbol.split(':', 1)
        base_symbol = base_symbol.strip().upper()
        base_no_space = base_symbol.replace(' ', '')
        base_with_space = base_symbol.replace('_', ' ')

        def _add(candidate: str):
            if candidate and candidate not in variants:
                variants.append(candidate)

        _add(underlying_symbol)
        _add(base_symbol)
        _add(base_no_space)
        if base_with_space != base_symbol:
            _add(base_with_space)

        for prefix in ('NSE', 'BSE'):
            _add(f"{prefix}:{base_with_space}")
            _add(f"{prefix}:{base_no_space}")
            _add(f"{prefix}{base_no_space}")

        if RedisKeyStandards:
            try:
                canonical = RedisKeyStandards.canonical_symbol(base_symbol)
                _add(canonical)
            except Exception:
                pass

        if base_symbol == 'NIFTY':
            _add('NSE:NIFTY 50')
        if base_symbol == 'BANKNIFTY':
            _add('NSE:NIFTY BANK')

        return variants
    
    def _should_replace_indicator(self, field: str, current_value) -> bool:
        """
        Decide whether an indicator value should be replaced by Redis/snapshot fallback.
        
        ✅ FIXED: Price movement fields - treat 0.0 as missing (allows recalculation from OHLC).
        Price change of exactly 0.0% is rare in active trading, so if it's 0.0, we should
        recalculate from OHLC data to ensure accuracy.
        
        Volume indicators always prefer Redis (authoritative source). Other indicators reuse
        the stricter _is_missing_indicator_value logic.
        """
        if field in self.CRITICAL_VOLUME_FIELDS:
            return True
        if field in self.CRITICAL_PRICE_FIELDS:
            # ✅ FIXED: Treat 0.0 as missing for price_change fields to allow recalculation
            # This ensures we recalculate from OHLC if Redis has stale 0.0 value
            return current_value is None or current_value == 0.0
        return self._is_missing_indicator_value(current_value)
    
    def _coerce_snapshot_value(self, field: str, value):
        """Convert raw Redis hash values into native Python types."""
        if value is None:
            return None
        if isinstance(value, bytes):
            try:
                value = value.decode('utf-8')
            except Exception:
                return None
        if isinstance(value, str):
            trimmed = value.strip()
            if trimmed.lower() in ("", "none", "null"):
                return None
            if trimmed.startswith("{") or trimmed.startswith("["):
                try:
                    return json.loads(trimmed)
                except (TypeError, json.JSONDecodeError):
                    pass
            if field in self._NUMERIC_SNAPSHOT_FIELDS:
                try:
                    return float(trimmed)
                except (TypeError, ValueError):
                    return None
            return trimmed
        return value
    
        # Try index hash family for indices like NIFTY/BANKNIFTY
        index_snapshot = self._get_index_hash_snapshot(symbol, redis_client)
        if index_snapshot:
            return index_snapshot
        return {}

    def _resolve_index_base_symbol(self, symbol: str) -> Optional[str]:
        """Resolve symbol to canonical index base using INDEX_ALIAS_LOOKUP."""
        if not symbol or not INDEX_ALIAS_LOOKUP:
            return None
        try:
            symbol_upper = str(symbol).upper().strip()
        except Exception:
            return None
        variants = _normalize_index_alias(symbol_upper)
        for variant in variants:
            resolved = INDEX_ALIAS_LOOKUP.get(variant)
            if resolved:
                return resolved
        return None

    def _get_index_hash_snapshot(self, symbol: str, redis_client) -> dict:
        """Read index quotes from index_hash:* keys for indices (NIFTY/BANKNIFTY/etc.)."""
        # Never treat derivatives (options/futures) as indices when hydrating ticks.
        if is_derivative_symbol(symbol or ""):
            return {}
        base_symbol = self._resolve_index_base_symbol(symbol)
        if not base_symbol:
            return {}
        config = INDEX_PRICE_KEY_OVERRIDES.get(base_symbol)
        if not config:
            return {}
        index_keys = config.get('index_keys', [])
        if not index_keys:
            return {}

        for key in index_keys:
            try:
                snapshot = redis_client.hgetall(key)
            except Exception as exc:
                self.logger.debug(f"⚠️ [INDEX_HYDRATE] Failed to fetch {key}: {exc}")
                continue
            if not snapshot:
                continue
            normalized = {}
            for raw_key, raw_value in snapshot.items():
                key_str = raw_key.decode('utf-8', errors='ignore') if isinstance(raw_key, bytes) else str(raw_key)
                value = raw_value.decode('utf-8', errors='ignore') if isinstance(raw_value, bytes) else raw_value
                normalized[key_str] = value

            # Ensure basic fields exist so downstream consumers can treat it like a tick hash
            normalized.setdefault('symbol', symbol)
            normalized.setdefault('underlying', base_symbol)
            return normalized
        return {}
    
    def _hydrate_missing_indicator_fields(self, symbol: str, source_tick: dict, cleaned: dict):
        """
        Recover crawler-computed indicator fields (greeks, volume ratio) if they were dropped.
        """
        critical_fields = set(self.CRITICAL_GREEK_FIELDS + self.CRITICAL_VOLUME_FIELDS + self.CRITICAL_PRICE_FIELDS)
        missing_fields = [
            field for field in critical_fields
            if self._should_replace_indicator(field, cleaned.get(field))
        ]
        if not missing_fields:
            return
        
        restored_fields = []
        # First prefer values from the source tick we just parsed
        for field in list(missing_fields):
            candidate = source_tick.get(field)
            if not self._should_replace_indicator(field, candidate):
                cleaned[field] = candidate
                restored_fields.append(field)
                missing_fields.remove(field)
        
        if missing_fields:
            # ✅ FIXED: Use unified _get_complete_redis_data instead of deprecated _get_latest_tick_snapshot
            redis_data = self._get_complete_redis_data(symbol)
            latest_snapshot = redis_data  # _get_complete_redis_data includes tick snapshot data
            if latest_snapshot:
                for field in list(missing_fields):
                    if field not in latest_snapshot:
                        continue
                    candidate = self._coerce_snapshot_value(field, latest_snapshot.get(field))
                    if candidate is None:
                        continue
                    if self._should_replace_indicator(field, candidate):
                        continue
                    cleaned[field] = candidate
                    restored_fields.append(field)
                    missing_fields.remove(field)
        
        if restored_fields:
            self.logger.debug(
                f"✅ [INDICATOR_HYDRATE] {symbol} - Restored fields: {sorted(restored_fields)}"
            )
    
    def _get_current_vix_snapshot(self):
        """Fetch VIX snapshot via VIXUtils with caching to avoid Redis spam."""
        if not self.vix_utils:
            return None
        now = time.time()
        if (
            self._cached_vix_snapshot
            and now - self._last_vix_fetch < max(self.vix_refresh_interval, 5)
        ):
            return self._cached_vix_snapshot
        try:
            snapshot = self.vix_utils.get_current_vix()
            if snapshot:
                self._cached_vix_snapshot = snapshot
                self._last_vix_fetch = now
            return snapshot
        except Exception as e:
            self.logger.debug(f"⚠️ [VIX] Failed to fetch VIX snapshot: {e}")
            return self._cached_vix_snapshot
    
    def _attach_external_data(self, symbol: str, cleaned: dict):
        """
        Attach external data (index values) to the tick.
        
        Note: External data (NIFTY, BANKNIFTY, VIX) is available in Redis DB1:
        - index:{prefix}{symbol} keys
        - Also written to JSON files by crawlers
        
        Currently a no-op stub as index data is fetched via:
        - _attach_vix_data() for VIX
        - _smart_merge_redis_data() for indicator data
        """
        # External index data is attached via Redis lookups in other methods
        pass
    
    def _attach_vix_data(self, cleaned: dict):
        """Attach latest VIX value/regime fetched from Redis to the tick."""
        try:
            snapshot = self._get_current_vix_snapshot()
            if not snapshot:
                return
            vix_value = snapshot.get('value')
            vix_regime = snapshot.get('regime')
            if 'vix_value' not in cleaned:
                cleaned['vix_value'] = vix_value
            if 'vix' not in cleaned:
                cleaned['vix'] = vix_value
            if 'vix_level' not in cleaned:
                cleaned['vix_level'] = vix_value
            if 'vix_regime' not in cleaned:
                cleaned['vix_regime'] = vix_regime
            cleaned.setdefault('vix_timestamp', snapshot.get('timestamp'))
        except Exception as exc:
            self.logger.debug(f"⚠️ [VIX] Unable to attach VIX data: {exc}")
    
    def _build_indicator_keys(self, symbol_variant: str, indicator_name: str, category: str) -> list[str]:
        """
        ⚠️ DEPRECATED: This method generates STRING-based key candidates (ind:{category}:{symbol}:{field}).
        
        Indicators are now stored as HASH at ind:{symbol} with fields.
        Use HGET/HGETALL on ind:{symbol} instead of GET on individual keys.
        
        This method is kept for backward compatibility during migration.
        """
        candidates: list[str] = []
        # Note: These STRING-based keys are DEPRECATED. Use ind:{symbol} HASH instead.
        try:
            base_key = DatabaseAwareKeyBuilder.live_indicator(symbol_variant, indicator_name, category)
            if base_key:
                candidates.append(base_key)
                if category == 'volume' and 'ind:volume:' in base_key:
                    candidates.append(base_key.replace('ind:volume:', 'ind:vol:'))
        except Exception:
            pass
        
        candidates.append(f"ind:{category}:{symbol_variant}:{indicator_name}")
        if category == 'volume':
            candidates.append(f"ind:vol:{symbol_variant}:{indicator_name}")
        
        deduped: list[str] = []
        for key in candidates:
            if key and key not in deduped:
                deduped.append(key)
        return deduped

    def _preserve_crawler_fields(self, tick_data: dict, symbol: str) -> dict:
        """Preserve crawler fields but DON'T wreck non-numeric fields"""
        cleaned = {
            "tradingsymbol": symbol,
            "symbol": symbol,
            "processed_at": get_current_ist_timestamp(),
            "source": "data_pipeline",
        }
        
        # Define ONLY numeric fields that should be float-coerced
        # Added missing critical numeric fields from expanded scanner telemetry
        NUMERIC_ONLY_FIELDS = {
            'delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'implied_volatility',
            'last_price', 'volume', 'open', 'high', 'low', 'close', 'average_price',
            'price_change', 'price_change_pct', 'change', 'net_change',
            'volume_ratio', 'normalized_volume', 'zerodha_cumulative_volume', 'bucket_incremental_volume',
            'dte', 'dte_years', 'trading_dte', 'strike_price', 'underlying_price',
            'spot_price', 'option_price', 'vanna', 'charm', 'vomma', 'speed', 
            'zomma', 'color', 'gamma_exposure', 'volatility',
            'microprice', 'order_flow_imbalance', 'order_book_imbalance', 
            'cumulative_volume_delta', 'spread_absolute', 'spread_bps',
            'best_bid_size', 'best_ask_size', 'total_bid_depth', 'total_ask_depth',
            'oi', 'open_interest', 'oi_day_high', 'oi_day_low',
            'upper_circuit', 'lower_circuit', 'upper_circuit_limit', 'lower_circuit_limit'
        }
        
        for key, value in tick_data.items():
            if value is None:
                cleaned[key] = None
            elif key in NUMERIC_ONLY_FIELDS:
                try:
                    # Handle string-encoded numeric values safely
                    if isinstance(value, str):
                        clean_val = value.replace('%', '').replace(',', '').strip()
                        if not clean_val:
                             cleaned[key] = value # Keep original empty string
                             continue
                        cleaned[key] = float(clean_val)
                    else:
                        cleaned[key] = float(value)
                except (ValueError, TypeError):
                    # Keep original for Redis fallback - DON'T set 0.0!
                    cleaned[key] = value
            else:
                # Non-numeric fields: keep as-is (NO float coercion!)
                if key not in cleaned:
                    cleaned[key] = value
        
        return cleaned


    def _is_duplicate(self, tick):
        """Enhanced deduplication with cleanup"""
        # ✅ FIXED: Use unified symbol extraction method
        symbol = self._get_unified_symbol(tick) or None
        timestamp = tick.get("timestamp", "")
        last_price = tick.get("last_price")
        bucket_incremental_volume = tick.get("bucket_incremental_volume", 0)

        # Create more comprehensive hash including bucket_incremental_volume
        tick_hash = f"{symbol}:{last_price}:{bucket_incremental_volume}:{timestamp}"

        # Check if duplicate
        if symbol in self.last_tick_hash:
            last_hash, last_time = self.last_tick_hash[symbol]

            # Check if within dedup window
            current_time = time.time()
            if (
                tick_hash == last_hash
                and (current_time - last_time) < self.dedup_window
            ):
                self.stats["ticks_deduplicated"] += 1
                return True

        # Update last tick hash
        self.last_tick_hash[symbol] = (tick_hash, time.time())
        return False

    def _cleanup_old_hashes(self):
        """Clean up old deduplication hashes periodically"""
        current_time = time.time()

        # Only cleanup every interval
        if current_time - self.last_dedup_cleanup < self.dedup_cleanup_interval:
            return

        self.last_dedup_cleanup = current_time

        # Remove hashes older than 2x dedup window
        cutoff_time = current_time - (self.dedup_window * 2)
        symbols_to_remove = []

        for symbol, (hash_val, timestamp) in self.last_tick_hash.items():
            if timestamp < cutoff_time:
                symbols_to_remove.append(symbol)

        for symbol in symbols_to_remove:
            del self.last_tick_hash[symbol]

        if symbols_to_remove:
            self.logger.debug(f"Cleaned {len(symbols_to_remove)} old dedup hashes")
    
    def _tick_flow_log(self, stage: str, symbol: str = "UNKNOWN", **details: Any) -> None:
        """Optional verbose logging for tracing tick ingestion."""
        if not getattr(self, "tick_flow_debug", False):
            return
        if self.tick_flow_debug_symbol and symbol and self.tick_flow_debug_symbol not in str(symbol):
            return
        detail_chunks = []
        for key, value in details.items():
            if value is None:
                continue
            detail_chunks.append(f"{key}={value}")
        detail_str = ", ".join(detail_chunks)
        self.logger.warning(f"🐛 [TICK_FLOW] {stage} {symbol} {detail_str}".rstrip())

    def get_next_tick(self, timeout=1.0):
        """Get next tick with timeout"""
        try:
            if hasattr(self, "tick_queue"):
                tick = self.tick_queue.get(timeout=timeout)
                self._update_stream_poll('tick_queue', True)
                return tick
            return None
        except queue.Empty:
            try:
                self._update_stream_poll('tick_queue', False)
            except Exception:
                pass
            return None
        except Exception as e:
            self.logger.error(f"Error in get_next_tick: {e}")
            return None

    def get_batch(self, max_size=None):
        """Get batch of ticks for efficient processing"""
        batch_size = max_size or self.batch_size
        batch = []

        # First flush any pending batch
        with self.batch_lock:
            if self.batch_buffer:
                self._flush_batch()

        with self.buffer_lock:
            while self.tick_buffer and len(batch) < batch_size:
                batch.append(self.tick_buffer.popleft())

            self.stats["ticks_processed"] += len(batch)

        return batch if batch else None

    def _compute_idle_state(self, age: float, current_time: float, max_age: float):
        """Determine if we're simply waiting for fresh data despite active polling."""
        consumer_thread = getattr(self, "consumer_thread", None)
        last_poll = getattr(self, "last_stream_poll", self.last_heartbeat)
        poll_age = current_time - last_poll
        idle = (
            age >= max_age
            and poll_age < self.idle_grace_period
            and consumer_thread is not None
            and consumer_thread.is_alive()
        )
        return idle, poll_age, consumer_thread

    def is_idle(self, max_age=10):
        """Return True when pipeline is actively polling but no ticks arrived recently."""
        current_time = time.time()
        age = current_time - self.last_heartbeat
        idle, _, _ = self._compute_idle_state(age, current_time, max_age)
        return idle

    def is_healthy(self, max_age=10):
        """Check if pipeline thread is still alive and processing or waiting for data."""
        if not self.running:
            return False

        current_time = time.time()
        age = current_time - self.last_heartbeat
        if age < max_age:
            self._idle_state = False
            return True

        idle, poll_age, consumer_thread = self._compute_idle_state(age, current_time, max_age)
        if idle:
            self._idle_state = True
            if current_time - self._last_idle_log > 30:
                self.logger.info(
                    f"⏸️ Pipeline idle ({age:.1f}s since tick, polls {poll_age:.1f}s ago) - awaiting fresh data"
                )
                self._last_idle_log = current_time
            return True

        self._idle_state = False
        if consumer_thread and not consumer_thread.is_alive():
            self.logger.error("❌ Pipeline consumer thread is not alive")
        else:
            self.logger.warning(
                f"❌ No ticks for {age:.1f}s and no successful polls for {poll_age:.1f}s - pipeline unhealthy"
            )
        return False

    def get_stats(self):
        """Get detailed pipeline statistics"""
        stats = {
            **self.stats,
            "buffer_usage": len(self.tick_buffer),
            "buffer_capacity": self.buffer_capacity,
            "batch_buffer_size": len(self.batch_buffer),
            "dedup_cache_size": len(self.last_tick_hash),
            "channel_errors": self.channel_errors,
            "error_rate": self.stats["errors"] / max(self.stats["ticks_received"], 1),
            "dedup_rate": self.stats["ticks_deduplicated"]
            / max(self.stats["ticks_received"], 1),
        }
        
        # ✅ REMOVED: HybridCalculations statistics - no longer used in DataPipeline
        
        if hasattr(self, "tick_queue") and self.tick_queue:
            try:
                stats["tick_queue_size"] = self.tick_queue.qsize()
            except Exception:
                stats["tick_queue_size"] = -1
            stats["tick_queue_maxsize"] = self.tick_queue_maxsize

        return stats

    def stop(self):
        """Stop the data pipeline gracefully"""
        self.running = False
        self._pipeline_monitor_running = False
        if self._pipeline_monitor_thread and self._pipeline_monitor_thread.is_alive():
            self._pipeline_monitor_thread.join(timeout=2.0)
            self._pipeline_monitor_thread = None

        # ✅ TIER 2: Stop historical archive and flush remaining data
        if self.historical_archive:
            try:
                self.historical_archive.stop()
                self.logger.info("✅ Historical archive stopped and flushed")
            except Exception as e:
                self.logger.warning(f"Error stopping historical archive: {e}")

        # Flush any remaining batch
        with self.batch_lock:
            if self.batch_buffer:
                self._flush_batch()

        if self.pubsub:
            try:
                self.pubsub.unsubscribe()
                self.pubsub.close()
            except:
                pass

        # Log final statistics with proper error handling
        try:
            stats = self.get_stats()
            self.logger.info(f"📊 Pipeline stopped. Final stats:")
            for key, value in stats.items():
                if isinstance(value, float):
                    self.logger.info(f"  {key}: {value:.2%}")
                else:
                    self.logger.info(f"  {key}: {value}")
        except Exception as e:
            # If logging fails, print to console instead
            try:
                sys.stderr.write("📊 Pipeline stopped. Final stats:\n")
                stats = self.get_stats()
                for key, value in stats.items():
                    if isinstance(value, float):
                        sys.stderr.write(f"  {key}: {value:.2%}\n")
                    else:
                        sys.stderr.write(f"  {key}: {value}\n")
                sys.stderr.write(f"Note: Logging failed during shutdown: {e}\n")
            except Exception:
                pass

        self._cleanup_on_exit()

        # Close all log handlers to prevent I/O errors
        try:
            for handler in self.logger.handlers:
                handler.close()
        except:
            pass

    def _clean_tick_data(self, tick_data):
        """Wrapper that guarantees we always return minimal tick data."""
        try:
            symbol = self._get_unified_symbol(tick_data)
            cleaned = self._clean_tick_data_core_enhanced(tick_data, symbol)
            if cleaned is None:
                symbol = tick_data.get("symbol", "UNKNOWN") if isinstance(tick_data, dict) else "UNKNOWN"
                self.logger.warning(f"⚠️ [CLEAN_TICK_FALLBACK] {symbol} produced None, returning minimal tick payload")
                return self._build_fallback_tick_payload(symbol, tick_data)
            if cleaned and safe_monitor:
                symbol_name = cleaned.get("symbol") or cleaned.get("tradingsymbol")
                if symbol_name:
                    safe_monitor.log_processing(symbol_name, "clean_tick_data")
            
            # ✅ CRITICAL: Log data after cleaning to trace data flow
            symbol = cleaned.get("symbol") or cleaned.get("tradingsymbol") or "UNKNOWN"
            self.logger.info(f"📊 [DATA_FLOW_TRACE] After _clean_tick_data for {symbol}:")
            
            # Log critical fields
            critical_fields = {
                'Greeks': ['delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'implied_volatility', 'dte_years', 'trading_dte', 'expiry_series', 'option_price', 'underlying_price'],
                'Volume': ['volume_ratio', 'normalized_volume', 'volume_context', 'bucket_incremental_volume'],
                'Price': ['price_change', 'price_change_pct', 'price_move', 'price_movement', 'last_price'],
            }
            
            for category, fields in critical_fields.items():
                present_fields = []
                missing_fields = []
                for field in fields:
                    value = cleaned.get(field)
                    if value is not None and value != 0.0:
                        present_fields.append(f"{field}={value}")
                    else:
                        missing_fields.append(field)
                
                if present_fields:
                    self.logger.info(f"   ✅ {category}: {', '.join(present_fields[:5])}{'...' if len(present_fields) > 5 else ''}")
                if missing_fields:
                    self.logger.info(f"   ❌ {category} MISSING: {', '.join(missing_fields[:5])}{'...' if len(missing_fields) > 5 else ''}")
            
            # Log total keys (convert any byte keys to strings before sorting)
            total_keys = len(cleaned.keys())
            self.logger.info(f"   📦 Total fields in cleaned data: {total_keys}")
            try:
                normalized_keys = [
                    key.decode('utf-8', errors='ignore') if isinstance(key, bytes) else str(key)
                    for key in cleaned.keys()
                ]
                sorted_keys = sorted(normalized_keys)
            except Exception:
                sorted_keys = list(cleaned.keys())
            self.logger.info(f"   🔑 All keys: {sorted_keys[:30]}{'...' if total_keys > 30 else ''}")
            
            return cleaned
        except Exception as e:
            try:
                symbol = tick_data.get("symbol", "UNKNOWN") if isinstance(tick_data, dict) else "UNKNOWN"
            except Exception:
                symbol = "UNKNOWN"
            self.logger.error(f"Error in _clean_tick_data for {symbol}: {e}", exc_info=True)
            return self._build_fallback_tick_payload(symbol, tick_data)

    def _build_fallback_tick_payload(self, symbol: str, original_tick):
        """
        Preserve as much original metadata as possible when _clean_tick_data_core_enhanced()
        signals that a tick should be skipped. This keeps volume_ratio/volume fields
        intact for downstream diagnostics instead of zeroing them out.
        """
        now_ms = int(time.time() * 1000)
        fallback_tick = {
            "symbol": symbol,
            "tradingsymbol": symbol,
            "last_price": 0.0,
            "timestamp_ms": now_ms,
            "source": "clean_tick_fallback",
        }

        if not isinstance(original_tick, dict):
            return fallback_tick

        # Carry forward any timestamp the crawler already attached
        for ts_field in ("timestamp_ms", "exchange_timestamp_ms", "processed_timestamp_ms"):
            ts_value = original_tick.get(ts_field)
            if ts_value:
                fallback_tick["timestamp_ms"] = int(self._safe_float(ts_value, now_ms))
                fallback_tick[ts_field] = ts_value
                break
        if original_tick.get("timestamp"):
            fallback_tick["timestamp"] = original_tick["timestamp"]
        if original_tick.get("exchange_timestamp"):
            fallback_tick["exchange_timestamp"] = original_tick["exchange_timestamp"]

        # Preserve the original prices/volumes instead of overwriting them with zeros
        last_price = (
            original_tick.get("last_price")
            or original_tick.get("last_traded_price")
            or original_tick.get("ltp")
        )
        if last_price is not None:
            fallback_tick["last_price"] = self._safe_float(last_price, 0.0)

        bucket_incremental = (
            original_tick.get("bucket_incremental_volume")
            or original_tick.get("incremental_volume")
            or original_tick.get("last_traded_quantity")
        )
        if bucket_incremental is not None:
            fallback_tick["bucket_incremental_volume"] = bucket_incremental

        bucket_cumulative = (
            original_tick.get("bucket_cumulative_volume")
            or original_tick.get("volume_traded_for_the_day")
            or original_tick.get("zerodha_cumulative_volume")
        )
        if bucket_cumulative is not None:
            fallback_tick["bucket_cumulative_volume"] = bucket_cumulative

        if original_tick.get("volume") is not None:
            fallback_tick["volume"] = original_tick["volume"]

        # Preserve pre-computed volume ratios (single source of truth)
        for field in ("volume_ratio", "volume_ratio_enhanced", "normalized_volume"):
            if original_tick.get(field) is not None:
                fallback_tick[field] = original_tick[field]

        # ✅ FIX: Preserve price_change fields to prevent data loss
        for field in ("price_change", "price_change_pct", "change", "net_change"):
            if original_tick.get(field) is not None:
                fallback_tick[field] = original_tick[field]

        # ✅ FIX: Preserve OHLC data for price_change calculation
        if original_tick.get("open") is not None:
            fallback_tick["open"] = original_tick["open"]
        if original_tick.get("open_price") is not None:
            fallback_tick["open_price"] = original_tick["open_price"]
        if original_tick.get("ohlc") is not None:
            fallback_tick["ohlc"] = original_tick["ohlc"]

        if original_tick.get("instrument_token") is not None:
            fallback_tick["instrument_token"] = original_tick["instrument_token"]
        elif original_tick.get("token") is not None:
            fallback_tick["instrument_token"] = original_tick["token"]

        return fallback_tick

    def _debug_redis_fetch_types(self, symbol: str, redis_data: dict):
        """Debug Redis fetch types for troubleshooting"""
        if not redis_data:
            return
        
        type_counts = {}
        problematic = []
        
        for key, value in redis_data.items():
            type_name = type(value).__name__
            type_counts[type_name] = type_counts.get(type_name, 0) + 1
            
            # Check for string values that should be numeric
            if isinstance(value, str):
                # Check if key looks numeric
                numeric_keys = ['delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 
                               'price', 'volume_ratio', 'change', 'dte', 'expiry']
                if any(nk in key.lower() for nk in numeric_keys):
                    try:
                        float(value.replace('%', '').replace(',', ''))
                        # Convertible to float, but still string
                        problematic.append(f"{key}='{value}' (string→float needed)")
                    except (ValueError, TypeError):
                        # Not convertible
                        pass
        
        if type_counts:
            self.logger.info(f"🔍 [REDIS_TYPES_DEBUG] {symbol} - Redis types: {type_counts}")
        
        if problematic:
            self.logger.warning(f"⚠️ [REDIS_TYPES_DEBUG] {symbol} - Problematic string→float fields: {problematic[:5]}")

    def _clean_tick_data_core_enhanced(self, tick_data: dict, symbol: str) -> dict:
        """Simplified cleaning with correct order of operations"""
        if not symbol:
             symbol = self._get_unified_symbol(tick_data) or "UNKNOWN"

        # Step 1: First get Redis data (contains correct Greeks)
        try:
            redis_data = self._get_complete_redis_data(symbol)
        except Exception as e:
            self.logger.debug(f"⚠️ [REDIS_FETCH] Failed for {symbol}: {e}")
            redis_data = {}
        
        # Step 2: Merge Redis data first (Greeks should come from Redis and be strings)
        # With decode_responses=True, redis_data already contains strings
        if redis_data:
            # Greek fields come from Redis - ensure they are in tick_data for numeric cleaning
            # Use specific list of Greeks that we know are calculated correctly in Redis
            greek_fields = ['delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'implied_volatility']
            for field in greek_fields:
                if field in redis_data and redis_data[field] is not None:
                    # Overwrite tick_data with Redis values (Source of Truth for Greeks)
                    tick_data[field] = redis_data[field]
            
            # Also merge other indicators if available
            for key, value in redis_data.items():
                if key not in tick_data and value is not None:
                    tick_data[key] = value
            
            redis_summary = {
                'delta': tick_data.get('delta'),
                'gamma': tick_data.get('gamma'),
                'gamma_exposure': tick_data.get('gamma_exposure'),
                'volume_ratio': tick_data.get('volume_ratio'),
            }
            self.logger.info(
                "🧱 [PIPELINE_ENRICH] %s - merged Redis data keys=%s %s",
                symbol,
                len(redis_data),
                redis_summary,
            )
        else:
            self.logger.info(f"🧱 [PIPELINE_ENRICH] {symbol} - no Redis indicators found to merge")

        # Step 3 & 4: Process and preserve crawler fields with careful numeric coercion
        # This replaces _clean_numeric_fields_fast and manual field merging
        cleaned = self._preserve_crawler_fields(tick_data, symbol)
        crawler_summary = {
            'volume_ratio': cleaned.get('volume_ratio'),
            'order_flow_imbalance': cleaned.get('order_flow_imbalance'),
            'bucket_incremental_volume': cleaned.get('bucket_incremental_volume'),
            'last_price': cleaned.get('last_price'),
        }
        self.logger.info(
            "🧱 [PIPELINE_ENRICH] %s - preserved crawler fields %s",
            symbol,
            crawler_summary,
        )
        
        # Step 5: Asset Class Tagging using centralized symbol_parser
        symbol_for_class = cleaned.get("tradingsymbol") or cleaned.get("symbol") or symbol
        parsed = self.symbol_parser.parse(str(symbol_for_class or ""))
        if parsed:
            cleaned["instrument_type"] = parsed.instrument_type
            if parsed.instrument_type == 'OPT':
                cleaned["asset_class"] = "options"
            elif parsed.instrument_type == 'FUT':
                cleaned["asset_class"] = "futures"
            elif parsed.instrument_type == 'EQ':
                cleaned["asset_class"] = "equity"
            else:
                cleaned["asset_class"] = "unknown"
        else:
            cleaned["asset_class"] = "unknown"
            cleaned["instrument_type"] = "EQ"


        # Step 6: Freshness check
        freshness_threshold = int(self.config.get("tick_freshness_threshold_seconds", 300))
        if not self._validate_tick_freshness(cleaned, freshness_threshold):
            self.stats["stale_ticks_rejected"] = self.stats.get("stale_ticks_rejected", 0) + 1
            return None

        # Step 7: Preserve raw data for debugging
        if "raw_data" not in cleaned:
            cleaned["raw_data"] = tick_data

        return cleaned


    def _preserve_all_original_data(self, tick_data: dict, cleaned: dict):
        """Preserve EVERY field from original tick data including all indicators, Greeks, volume_ratio, price_change."""
        for key, value in tick_data.items():
            if key in ['_id', '__class__']:
                continue
            cleaned[key] = value
    
    def _clean_microstructure_indicators(self, tick_data: dict, cleaned: dict):
        """
        ✅ CRITICAL: Extract and parse microstructure indicators with explicit float conversion.
        
        Ensures all microstructure indicators are properly converted to float with error handling.
        This is critical because indicators may come as strings from Redis or other sources.
        """
        microstructure_fields = [
            'microprice', 'order_flow_imbalance', 'order_book_imbalance', 
            'cumulative_volume_delta', 'spread_absolute', 'spread_bps',
            'depth_imbalance', 'best_bid_size', 'best_ask_size',
            'total_bid_depth', 'total_ask_depth',
            # ✅ ADDED: Cross-asset fields that need float conversion
            'usdinr_sensitivity', 'usdinr_correlation', 'usdinr_theoretical_price', 'usdinr_basis'
        ]
        
        for field in microstructure_fields:
            if field in tick_data:
                value = tick_data[field]
                if value is not None:
                    try:
                        cleaned[field] = float(value)
                    except (TypeError, ValueError):
                        # Keep original value if conversion fails (might be valid as-is)
                        cleaned[field] = value
    
    def _clean_depth_orderbook_parsing(self, tick_data: dict, cleaned: dict):
        """
        ✅ CRITICAL: Parse depth/order_book data from JSON strings if needed.
        
        Handles cases where depth/order_book may be stored as JSON strings in Redis
        or other sources. Also normalizes bids/asks to buy/sell structure.
        """
        # Check for depth/order_book fields that might be JSON strings
        for field in ['depth', 'order_book', 'market_depth']:
            if field in tick_data:
                value = tick_data[field]
                if isinstance(value, str):
                    try:
                        parsed = json.loads(value)
                        cleaned[field] = parsed
                        # Also store as 'depth' for consistency
                        if field != 'depth':
                            cleaned['depth'] = parsed
                    except (json.JSONDecodeError, TypeError):
                        # If JSON parsing fails, keep original value
                        cleaned[field] = value
                elif isinstance(value, dict):
                    cleaned[field] = value
                    if field != 'depth':
                        cleaned['depth'] = value
        
        # ✅ Ensure order book has proper structure (normalize bids/asks to buy/sell)
        if cleaned.get('depth') and isinstance(cleaned['depth'], dict):
            depth = cleaned['depth']
            # Normalize to have 'buy' and 'sell' keys
            if 'buy' not in depth or 'sell' not in depth:
                if 'bids' in depth and 'asks' in depth:
                    cleaned['depth'] = {
                        'buy': depth.get('bids', []),
                        'sell': depth.get('asks', [])
                    }
                    # Also update order_book if it exists
                    if 'order_book' in cleaned:
                        cleaned['order_book'] = cleaned['depth']

    def _smart_merge_redis_data(self, cleaned: dict, redis_data: dict, original_tick: dict):
        """Merge Redis data to fill gaps without overwriting fresh tick data. Preserves ALL indicators, Greeks, volume_ratio, price_change."""
        
        def _should_use_redis_value(current_val, redis_val):
            """Decide whether to use Redis value based on types"""
            if redis_val is None:
                return False
            
            # If current value is None or 0, use Redis value
            if current_val is None or current_val == 0 or current_val == 0.0:
                return True
            
            # If Redis value is string but looks numeric, convert and compare
            if isinstance(redis_val, str):
                try:
                    # Convert string to float for comparison
                    redis_float = float(redis_val.replace('%', '').replace(',', ''))
                    # Only use if current value is also string or zero
                    if isinstance(current_val, str):
                        return True
                    elif current_val == 0.0:
                        return True
                    # Don't replace float with string-converted float if current is non-zero
                    return False
                except (ValueError, TypeError):
                    # Redis value is non-numeric string, don't use for numeric fields
                    return False
        
            # Both are proper types, use Redis if it's non-zero and current is zero
            if isinstance(redis_val, (int, float)) and redis_val != 0:
                if current_val == 0 or current_val == 0.0:
                    return True
            
            return False
        
        # Merge with type awareness
        for key, redis_value in redis_data.items():
            current_value = cleaned.get(key)
            
            if _should_use_redis_value(current_value, redis_value):
                # Convert Redis value to proper type if needed
                if isinstance(redis_value, str):
                    try:
                        # Try to convert to float for numeric fields
                        cleaned[key] = float(redis_value.replace('%', '').replace(',', ''))
                        self.logger.debug(f"✅ [SMART_MERGE_TYPE] {key}: string '{redis_value}' → float {cleaned[key]}")
                    except (ValueError, TypeError):
                        # Keep as string
                        cleaned[key] = redis_value
                else:
                    cleaned[key] = redis_value
        # Critical fields that should always be merged from Redis if missing
        # Includes ALL indicators, advanced Greeks, option metadata, and microstructure metrics
        critical_fields = [
            # Volume and Price
            'volume_ratio', 'price_change', 'price_change_pct', 'normalized_volume', 'volume_spike', 'volume_trend',
            
            # Classical Greeks
            'delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'implied_volatility',
            
            # Advanced Greeks (higher-order) - ALL 6 higher-order Greeks
            'vanna', 'charm', 'vomma', 'speed', 'zomma', 'color',
            
            # Option Metadata
            'dte_years', 'trading_dte', 'expiry_series', 'option_price', 'underlying_price', 'strike_price',
            'spot_price', 'moneyness', 'intrinsic_value', 'time_value',
            
            # Gamma Exposure and Advanced Option Metrics
            'gamma_exposure', 'gamma_risk', 'delta_risk', 'theta_decay', 'vega_sensitivity', 'rho_sensitivity',
            
            # Technical Indicators
            'rsi', 'macd', 'sma_20', 'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200',
            'atr', 'vwap', 'bollinger_bands', 'z_score', 'iv_baseline', 'price_move', 'price_movement',
            
            # Microstructure Indicators (ALL 11 indicators)
            'microprice', 'order_flow_imbalance', 'order_book_imbalance',
            'cumulative_volume_delta', 'spread_absolute', 'spread_bps',
            'depth_imbalance', 'best_bid_size', 'best_ask_size',
            'total_bid_depth', 'total_ask_depth',
            
            # Cross-Asset Metrics (MCX) - ALL 4 metrics
            'usdinr_sensitivity', 'usdinr_correlation', 'usdinr_theoretical_price', 'usdinr_basis',
            
            # Market Regime Indicators
            'buy_pressure', 'vix_level', 'volatility_regime',
        ]
        
        for key, redis_value in redis_data.items():
            current_value = cleaned.get(key)
            
            # Always merge critical fields if missing or zero
            if key in critical_fields:
                if current_value is None or current_value == 0 or current_value == 0.0:
                    if redis_value is not None:
                        cleaned[key] = redis_value
                        self.logger.debug(f"✅ [SMART_MERGE] Merged {key}={redis_value} from Redis")
                continue
            
            # For volume_ratio: prefer Redis if tick value is very low OR unrealistically high (>50)
            if key == 'volume_ratio':
                # ✅ FIX: Reject unrealistic values (>50) - likely calculation errors or percentage vs ratio confusion
                if current_value is not None and current_value > 50.0:
                    self.logger.warning(f"⚠️ [VOLUME_FIX] Rejecting unrealistic volume_ratio={current_value:.2f} from tick_data (likely error), using Redis value: {redis_value}")
                    if redis_value is not None and 0 < redis_value <= 50.0:
                        cleaned[key] = redis_value
                        continue
                    else:
                        # If Redis value is also bad, set to 0 and let fallback handle it
                        cleaned[key] = 0.0
                        continue
                
                if redis_value and redis_value > 0.1 and (current_value is None or current_value < 0.1):
                    cleaned[key] = redis_value
                    self.logger.info(f"✅ [VOLUME_FIX] Using Redis volume_ratio: {redis_value:.4f} (was: {current_value})")
                    continue
                elif original_tick.get('volume_ratio') in [None, 0, 0.0] and redis_value is not None:
                    # ✅ FIX: Validate Redis value is reasonable before using
                    if 0 < redis_value <= 50.0:
                        cleaned[key] = redis_value
                        self.logger.debug(f"✅ [SMART_MERGE] Using Redis volume_ratio: {redis_value:.4f}")
                    else:
                        self.logger.warning(f"⚠️ [VOLUME_FIX] Rejecting unrealistic Redis volume_ratio={redis_value:.2f}, keeping current: {current_value}")
                    continue
            
            # Skip if we already have a non-null value from tick data
            if current_value not in [None, 0, 0.0, ""]:
                continue
            
            # Merge Greeks for options
            if key in self.CRITICAL_GREEK_FIELDS:
                symbol = cleaned.get('symbol', '')
                if is_option_symbol(symbol) and redis_value is not None:
                    cleaned[key] = redis_value
            else:
                # For other fields, use Redis value to fill gaps
                if redis_value is not None:
                    cleaned[key] = redis_value



    def _get_tick_age(self, tick_data: dict) -> float | None:
        """Quick helper to get tick age without full validation.
        
        Uses the same timestamp priority logic as _validate_tick_freshness for consistency.
        """
        try:
            import time
            tick_timestamp = None
            
            # Quick timestamp extraction (same priority as _validate_tick_freshness)
            # Priority 1: exchange_timestamp_ms (epoch milliseconds)
            if tick_data.get("exchange_timestamp_ms"):
                ts_ms = self._safe_float(tick_data.get("exchange_timestamp_ms"), 0.0)
                if ts_ms > 0:
                    tick_timestamp = ts_ms / 1000.0
            
            # Priority 2: exchange_timestamp_epoch
            if tick_timestamp is None and tick_data.get("exchange_timestamp_epoch"):
                ts_epoch = self._safe_float(tick_data.get("exchange_timestamp_epoch"), 0.0)
                if ts_epoch > 0:
                    if ts_epoch > 1e10:
                        tick_timestamp = ts_epoch / 1000.0
                    else:
                        tick_timestamp = ts_epoch
            
            # Priority 3: exchange_timestamp
            if tick_timestamp is None and tick_data.get("exchange_timestamp"):
                try:
                    ts_val = self._safe_float(tick_data.get("exchange_timestamp"), 0.0)
                    if ts_val > 0:
                        if ts_val > 1e10:
                            tick_timestamp = ts_val / 1000.0
                        else:
                            tick_timestamp = ts_val
                except (TypeError, ValueError):
                    try:
                        # TimestampNormalizer is already imported at top from shared_core
                        ts_ms = TimestampNormalizer.to_epoch_ms(tick_data.get("exchange_timestamp")) if tick_data.get("exchange_timestamp") is not None else 0.0
                        if ts_ms > 0:
                            tick_timestamp = ts_ms / 1000.0
                    except Exception:
                        pass
            
            # Priority 4: timestamp_ms
            if tick_timestamp is None and tick_data.get("timestamp_ms"):
                ts_ms = self._safe_float(tick_data.get("timestamp_ms"), 0.0)
                if ts_ms > 0:
                    tick_timestamp = ts_ms / 1000.0
            
            # Priority 5: timestamp
            if tick_timestamp is None and tick_data.get("timestamp"):
                ts_raw = tick_data["timestamp"]
                try:
                    ts_val = self._safe_float(ts_raw, 0.0)
                    if ts_val > 1e10:
                        tick_timestamp = ts_val / 1000.0
                    elif ts_val > 0:
                        tick_timestamp = ts_val
                except (TypeError, ValueError):
                    try:
                        # TimestampNormalizer is already imported at top from shared_core
                        ts_ms = TimestampNormalizer.to_epoch_ms(ts_raw) if ts_raw is not None else 0.0
                        if ts_ms > 0:
                            tick_timestamp = ts_ms / 1000.0
                    except Exception:
                        pass
            
            # Priority 6: last_trade_time
            if tick_timestamp is None and tick_data.get("last_trade_time"):
                tick_timestamp = self._safe_float(tick_data.get("last_trade_time"), 0.0)
            
            if tick_timestamp and tick_timestamp > 0:
                return time.time() - tick_timestamp
            return None
        except Exception:
            return None
    
    def _validate_tick_freshness(self, tick_data: dict, max_age_seconds: int = 180) -> bool:
        """
        ✅ CRITICAL: Validate tick freshness to ensure we're processing current data.
        
        Rejects ticks that are older than max_age_seconds to prevent:
        - Using stale data for calculations
        - Storing outdated IV/Greeks values
        - Triggering patterns on old data
        
        Args:
            tick_data: Cleaned tick data dictionary
            max_age_seconds: Maximum age in seconds (default: 180 seconds = 3 minutes)
            
        Returns:
            True if tick is fresh (within max_age_seconds), False if stale
        """
        try:
            import time
            symbol = tick_data.get('symbol', 'UNKNOWN')
            
            # Get tick timestamp from various possible fields (same priority as _clean_timestamps)
            tick_timestamp = None
            timestamp_source = None
            
            # Priority 1: exchange_timestamp_ms (epoch milliseconds) - already normalized by _clean_timestamps
            if tick_data.get("exchange_timestamp_ms"):
                ts_ms = self._safe_float(tick_data.get("exchange_timestamp_ms"), 0.0)
                if ts_ms > 0:
                    tick_timestamp = ts_ms / 1000.0  # Convert to seconds
                    timestamp_source = "exchange_timestamp_ms"
            
            # Priority 2: exchange_timestamp_epoch (epoch seconds) - added support
            if tick_timestamp is None and tick_data.get("exchange_timestamp_epoch"):
                ts_epoch = self._safe_float(tick_data.get("exchange_timestamp_epoch"), 0.0)
                if ts_epoch > 0:
                    # If it's > 1e10, it's already in milliseconds, convert to seconds
                    if ts_epoch > 1e10:
                        tick_timestamp = ts_epoch / 1000.0
                    else:
                        tick_timestamp = ts_epoch
                    timestamp_source = "exchange_timestamp_epoch"
            
            # Priority 3: exchange_timestamp (epoch seconds or string that needs parsing)
            if tick_timestamp is None and tick_data.get("exchange_timestamp"):
                ts_exchange = tick_data.get("exchange_timestamp")
                try:
                    # Try to parse as float first
                    ts_val = self._safe_float(ts_exchange, 0.0)
                    if ts_val > 0:
                        # If > 1e10, it's milliseconds
                        if ts_val > 1e10:
                            tick_timestamp = ts_val / 1000.0
                        else:
                            tick_timestamp = ts_val
                        timestamp_source = "exchange_timestamp"
                except (TypeError, ValueError):
                    # If it's a string, try to parse it using TimestampNormalizer
                    try:
                        # TimestampNormalizer is already imported at top from shared_core
                        ts_ms = TimestampNormalizer.to_epoch_ms(ts_exchange)
                        if ts_ms > 0:
                            tick_timestamp = ts_ms / 1000.0
                            timestamp_source = "exchange_timestamp (parsed)"
                    except Exception:
                        pass
            
            # Priority 4: timestamp_ms (epoch milliseconds)
            if tick_timestamp is None and tick_data.get("timestamp_ms"):
                ts_ms = self._safe_float(tick_data.get("timestamp_ms"), 0.0)
                if ts_ms > 0:
                    tick_timestamp = ts_ms / 1000.0
                    timestamp_source = "timestamp_ms"
            
            # Priority 5: timestamp (epoch milliseconds or seconds)
            if tick_timestamp is None and tick_data.get("timestamp"):
                ts_raw = tick_data["timestamp"]
                try:
                    ts_val = self._safe_float(ts_raw, 0.0)
                    # If timestamp is > 1e10, it's in milliseconds
                    if ts_val > 1e10:
                        tick_timestamp = ts_val / 1000.0
                    elif ts_val > 0:
                        tick_timestamp = ts_val
                    timestamp_source = "timestamp"
                except (TypeError, ValueError):
                    # Try parsing as string
                    try:
                        # TimestampNormalizer is already imported at top from shared_core
                        ts_ms = TimestampNormalizer.to_epoch_ms(ts_raw)
                        if ts_ms > 0:
                            tick_timestamp = ts_ms / 1000.0
                            timestamp_source = "timestamp (parsed)"
                    except Exception:
                        pass
            
            # Priority 6: last_trade_time
            if tick_timestamp is None and tick_data.get("last_trade_time"):
                tick_timestamp = self._safe_float(tick_data.get("last_trade_time"), 0.0)
                if tick_timestamp > 0:
                    timestamp_source = "last_trade_time"
            
            # If no timestamp found, log and assume fresh (fail-safe)
            if tick_timestamp is None or tick_timestamp <= 0:
                self.logger.warning(
                    f"⚠️ [TICK_FRESHNESS] {symbol} - No valid timestamp found, assuming fresh. "
                    f"Fields checked: exchange_timestamp_ms={tick_data.get('exchange_timestamp_ms')}, "
                    f"exchange_timestamp_epoch={tick_data.get('exchange_timestamp_epoch')}, "
                    f"exchange_timestamp={tick_data.get('exchange_timestamp')}, "
                    f"timestamp_ms={tick_data.get('timestamp_ms')}, timestamp={tick_data.get('timestamp')}, "
                    f"last_trade_time={tick_data.get('last_trade_time')}"
                )
                return True
            
            # Calculate age
            current_time = time.time()
            tick_age = current_time - tick_timestamp
            fallback_age = None
            fallback_source = None

            # Fallback 1: processed_at (ISO string set when we clean the tick)
            processed_at_iso = tick_data.get("processed_at")
            if processed_at_iso:
                try:
                    processed_at_ms = TimestampNormalizer.to_epoch_ms(processed_at_iso)
                    fallback_age = current_time - (processed_at_ms / 1000.0)
                    fallback_source = "processed_at"
                except Exception:
                    fallback_age = None
                    fallback_source = None

            # Fallback 2: allow callers to explicitly provide processed timestamp in ms
            if fallback_age is None and tick_data.get("processed_timestamp_ms"):
                try:
                    processed_ts_ms = self._safe_float(tick_data.get("processed_timestamp_ms"), 0.0)
                    if processed_ts_ms > 0:
                        fallback_age = current_time - (processed_ts_ms / 1000.0)
                        fallback_source = "processed_timestamp_ms"
                except Exception:
                    fallback_age = None
                    fallback_source = None

            fallback_fresh = (
                fallback_age is not None
                and fallback_age >= 0
                and fallback_age <= max_age_seconds
            )
            
            # ✅ OPTIMIZED: Fast rejection for very stale ticks (> 600s) - minimal logging
            if tick_age > 600:
                if fallback_fresh:
                    self._log_freshness_fallback(
                        symbol,
                        timestamp_source,
                        fallback_source,
                        tick_age,
                        fallback_age,
                    )
                    return True
                self.logger.warning(
                    f"⚠️ [TICK_FRESHNESS] {symbol} - Very stale tick rejected (age: {tick_age:.1f}s, "
                    f"source: {timestamp_source}, timestamp: {tick_timestamp:.3f}, current: {current_time:.3f})"
                )
                if safe_monitor and tick_age > 300:
                    safe_monitor.log_processing(symbol, "stale_rejected")
                return False
            
            # Validate freshness for moderately stale ticks
            if tick_age > max_age_seconds:
                if fallback_fresh:
                    self._log_freshness_fallback(
                        symbol,
                        timestamp_source,
                        fallback_source,
                        tick_age,
                        fallback_age,
                    )
                    return True
                # Log moderately stale ticks (180-600s) for debugging
                self.logger.warning(
                    f"⚠️ [TICK_FRESHNESS] {symbol} - Tick age {tick_age:.1f}s exceeds max {max_age_seconds}s "
                    f"(timestamp: {tick_timestamp:.3f}, current: {current_time:.3f}, source: {timestamp_source}, "
                    f"fields: exchange_timestamp_ms={tick_data.get('exchange_timestamp_ms')}, "
                    f"exchange_timestamp_epoch={tick_data.get('exchange_timestamp_epoch')}, "
                    f"exchange_timestamp={tick_data.get('exchange_timestamp')}, "
                    f"timestamp_ms={tick_data.get('timestamp_ms')}, timestamp={tick_data.get('timestamp')}, "
                    f"last_trade_time={tick_data.get('last_trade_time')})"
                )
                if safe_monitor and tick_age > 300:
                    safe_monitor.log_processing(symbol, "stale_rejected")
                return False
            
            # Log if tick is getting close to stale threshold (for monitoring)
            if tick_age > max_age_seconds * 0.8:  # 80% of max age
                self.logger.debug(
                    f"🔍 [TICK_FRESHNESS] {symbol} - Tick age {tick_age:.1f}s is close to threshold "
                    f"({max_age_seconds}s, source: {timestamp_source})"
                )
            
            return True
            
        except Exception as e:
            # If validation fails, log error but don't reject tick (fail-safe)
            symbol = tick_data.get('symbol', 'UNKNOWN')
            self.logger.warning(
                f"⚠️ [TICK_FRESHNESS] {symbol} - Validation error: {e}, allowing tick through"
            )
            import traceback
            self.logger.debug(f"❌ [TICK_FRESHNESS] {symbol} - Traceback: {traceback.format_exc()}")
            return True

    def _log_freshness_fallback(
        self,
        symbol: str,
        primary_source: Optional[str],
        fallback_source: Optional[str],
        observed_age: float,
        fallback_age: float,
    ) -> None:
        """
        Log whenever we intentionally trust a local timestamp (processed_at/etc.)
        because the exchange-provided timestamp is chronically stale (e.g., session-open time).
        """
        if not hasattr(self, "_freshness_fallback_logged"):
            self._freshness_fallback_logged = set()
        fallback_label = fallback_source or "processed_at"
        primary_label = primary_source or "unknown"
        message = (
            f"⚠️ [TICK_FRESHNESS] {symbol} - Exchange timestamp via {primary_label} "
            f"reported age {observed_age:.1f}s, using {fallback_label} age {fallback_age:.1f}s instead"
        )
        if symbol not in self._freshness_fallback_logged:
            self._freshness_fallback_logged.add(symbol)
            self.logger.warning(message)
        else:
            self.logger.debug(message)

    def _normalize_numeric_types(self, cleaned):
        """Normalize numeric-like fields to proper Python numbers for downstream code.

        This converts a small whitelist of last_price/quantity fields and nested OHLC/depth
        entries into floats/ints using the existing _safe_float helper. It mutates
        the cleaned dict in-place.
        """
        # Whitelist of top-level numeric fields to coerce to float
        numeric_fields = [
            "last_price",
            "average_price",
            "bucket_incremental_volume",
            "zerodha_cumulative_volume",
            "bucket_cumulative_volume",  # Raw cumulative bucket_incremental_volume from Zerodha
            "bucket_incremental_volume",  # Set to 0 (no calculations)
            "volume_ratio",  # 🎯 CRITICAL: Add volume_ratio to numeric fields
            "buy_quantity",
            "sell_quantity",
            "total_buy_quantity",
            "total_sell_quantity",
            "zerodha_last_traded_quantity",
            "oi",
            "oi_day_high",
            "oi_day_low",
            "change",
            "net_change",  # Calculated from last_price - ohlc.close
            "price_change_pct",
            "spoofing_score",
            "weighted_bid",
            "weighted_ask",
            # Canonical/synonyms added downstream
            "weighted_bid_price",
            "weighted_ask_price",
            "bid_ask_spread",
            "mid_price",
            "order_book_imbalance",
            "order_flow_imbalance",
            "microprice",
            "spread_absolute",
            "spread_bps",
            "depth_imbalance",
            "cumulative_volume_delta",
            "best_bid_size",
            "best_ask_size",
            "total_bid_depth",
            "total_ask_depth",
            "gamma_exposure",
            "vanna",
            "charm",
            "vomma",
            "speed",
            "zomma",
            "color",
            "usdinr_sensitivity",
            "usdinr_correlation",
            "usdinr_theoretical_price",
            "usdinr_basis",
            
            # ✅ CRITICAL: Normalize IV and Greeks as numeric types
            "iv",  # Implied volatility
            "implied_volatility",  # Implied volatility (alias)
            "volatility",  # Volatility (alias for IV)
            "delta",  # Option delta
            "gamma",  # Option gamma
            "theta",  # Option theta
            "vega",  # Option vega
            "rho",  # Option rho
            "dte_years",  # Days to expiry (in years)
            "trading_dte",  # Trading days to expiry
            "option_price",  # Option price
            "underlying_price",  # Underlying/spot price
            "spot_price",  # Spot price (alias)
            "strike_price",  # Strike price
            "price_change",
            "price_change_pct",
            "price_move",
            "price_movement",
        ]

        for f in numeric_fields:
            if f in cleaned:
                cleaned[f] = self._safe_float(cleaned.get(f))

        # Normalize OHLC
        ohlc = cleaned.get("ohlc")
        if isinstance(ohlc, dict):
            for key in ["open", "high", "low", "close"]:
                if key in ohlc:
                    ohlc[key] = self._safe_float(ohlc.get(key))
            cleaned["ohlc"] = ohlc

        # Normalize depth levels (last_price, quantity)
        depth = cleaned.get("depth")
        if isinstance(depth, dict):
            for side in ("buy", "sell"):
                levels = depth.get(side, [])
                if isinstance(levels, list):
                    for lvl in levels:
                        if isinstance(lvl, dict):
                            if "last_price" in lvl:
                                lvl["last_price"] = self._safe_float(lvl.get("last_price"))
                            if "quantity" in lvl:
                                # quantities are often ints but safe_float is fine
                                lvl["quantity"] = self._safe_float(lvl.get("quantity"))
            cleaned["depth"] = depth


        # Normalize any numeric-looking preserved crawler fields
        crawler_numeric = [
            "total_bid_orders",
            "total_ask_orders",
            "avg_bid_order_size",
            "avg_ask_order_size",
            "order_imbalance",
            "order_count_imbalance",
            "bid_ask_ratio",
            "total_bid_qty",
            "total_ask_qty",
            "depth_levels",
        ]
        for f in crawler_numeric:
            if f in cleaned:
                cleaned[f] = self._safe_float(cleaned.get(f))

    def _process_index_data(self, channel, data):
        """Process index data (NIFTY, VIX, etc.) from crawlers/gift_nifty_gap.py"""
        try:
            # Extract index name from channel (e.g., 'index:NSE:NIFTY 50' -> 'nifty50')
            parts = channel.split(":")
            if len(parts) >= 2:
                # Channel format: index:<EXCHANGE>:<INDEX NAME>
                raw_name = parts[-1]
                index_symbol = raw_name.replace(" ", "").lower()  # e.g., 'nifty50', 'niftybank', 'indiavix', 'giftnifty'
                index_data = {
                    "index": index_symbol,
                    "last_price": self._safe_float(data.get("last_price", 0)),
                    "change": self._safe_float(data.get("change", 0)),
                    "change_pct": self._safe_float(data.get("change_pct", 0)),
                    "timestamp": data.get("timestamp", get_current_ist_timestamp()),
                    "raw_data": data,
                }

                # Store in Redis for VIX/indicator use (60s TTL), normalized key
                redis_key = f"index_data:{index_symbol}"
                safe_index_data = fix_numpy_serialization(index_data)
                self.redis_client.setex(redis_key, 60, json.dumps(safe_index_data))

                # Increment stats if available
                if hasattr(self, "stats"):
                    self.stats["index_updates"] = self.stats.get("index_updates", 0) + 1

        except Exception as e:
            self.logger.warning(f"Error processing {channel}: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# Entry Point for Supervisor-Spawned Workers
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    import signal
    import sys
    
    parser = argparse.ArgumentParser(description="Data Pipeline Worker")
    parser.add_argument("--worker-id", type=int, default=0, help="Worker ID for this process")
    parser.add_argument("--auto-recover", type=str, default="true", help="Enable auto-recovery")
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s - Worker-{args.worker_id} - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    logger.info(f"🚀 Starting Data Pipeline Worker {args.worker_id}")
    
    # Create pipeline with worker-specific config
    config = {
        "worker_id": args.worker_id,
        "auto_recover": args.auto_recover.lower() == "true",
    }
    
    pipeline = DataPipeline(config=config)
    
    # Handle graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"⏹️ Worker {args.worker_id} received shutdown signal")
        pipeline.running = False
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Start pipeline (this blocks in consumer thread)
        pipeline.start()
        
        # Keep main thread alive while pipeline runs
        while pipeline.running:
            import time
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info(f"⏹️ Worker {args.worker_id} interrupted")
    except Exception as e:
        logger.error(f"❌ Worker {args.worker_id} error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        pipeline.running = False
        logger.info(f"🏁 Worker {args.worker_id} exited")
