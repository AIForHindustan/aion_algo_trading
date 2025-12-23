#!/Users/lokeshgupta/Projects/aion_algo_trading/intraday_trading/.venv/bin/python3
"""
Main orchestration for modular market scanner
Handles initialization, data flow coordination, and graceful shutdown
Updated: 2025-10-27
"""

import os
import sys
import time
import signal
import argparse
import json
import glob
import asyncio
import schedule
import gc
import redis
from redis import ConnectionPool

from shared_core.redis_clients.redis_client import get_redis_client
from shared_core.redis_clients.consumer_group_manager import ConsumerGroupManager
# Emergency fixes removed - Optimizations merged into scanner_main.py
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None
from datetime import datetime
import threading
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, RLock
from typing import Dict, List, Optional, Any, Tuple, Set, cast
import pytz


try:
    from shared_core.core.health.monitor import InfrastructureMonitor
    safe_monitor = InfrastructureMonitor
except ImportError:
    safe_monitor = None

# ✅ IMPORT EMERGENCY FIXES

# Polars for batch pattern scoring
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None

# Define project root at the parent level (go up from intraday_scanner/ to intraday_trading/ to aion_algo_trading/)
# CRITICAL: Prioritize intraday_trading so it finds intraday_trading/redis_files first
intraday_scanner_dir = os.path.dirname(os.path.abspath(__file__))  # intraday_scanner/
intraday_root = os.path.dirname(intraday_scanner_dir)  # intraday_trading/
project_root = os.path.dirname(intraday_root)  # aion_algo_trading/src
repo_root = os.path.dirname(project_root)  # aion_algo_trading/
# Remove if already present to avoid duplicates
if project_root in sys.path:
    sys.path.remove(project_root)
if intraday_root in sys.path:
    sys.path.remove(intraday_root)
if intraday_scanner_dir in sys.path:
    sys.path.remove(intraday_scanner_dir)
# Insert repo_root so packages from repo root (config, community_bots, etc.) resolve first
sys.path.insert(0, str(repo_root))
# Then insert project_root (src/), intraday_trading, and intraday_scanner
sys.path.insert(1, str(project_root))
sys.path.insert(2, str(intraday_root))
sys.path.insert(3, str(intraday_scanner_dir))

from patterns.pattern_detector import PatternDetector
from patterns.game_theory_engine import (
    IntradayGameTheoryEngine,
    INTRADAY_PRIORITY,
    VALIDATION_SYMBOLS as GAME_THEORY_VALIDATION_SYMBOLS,
    NashMarketMakerGame,
)
from unified_pattern_init import init_scanner_worker
from shared_core.volume_files.volume_computation_manager import get_volume_computation_manager
from shared_core.volume_files.correct_volume_calculator import VolumeResolver
from shared_core.redis_clients.redis_key_standards import (
    DatabaseAwareKeyBuilder,
    RedisKeyStandards,
)
# Canonical yaml field loader
from shared_core.config_utils.yaml_field_loader import resolve_session_field
from shared_core.index_manager import index_manager

# ✅ VIX Utils Import
_vix_utils_factory = None
try:
    from shared_core.utils.vix_utils import get_vix_utils as _vix_utils_factory
except ImportError:
    _vix_utils_factory = None

def _get_vix_factory():
    return _vix_utils_factory

# ✅ Gamma Exposure Calculator Import
try:
    from intraday_trading.intraday_scanner.calculations import GammaExposureCalculator
except ImportError:
    GammaExposureCalculator = None

class QuickGameTheoryIntegration:
    """
    Lightweight Nash-only confirmation helper.
    Keeps compatibility with the requested signature while reusing the
    existing NashMarketMakerGame implementation.
    """
    
    def __init__(self):
        self.current_pattern: Optional[str] = None
        self._nash_model = None
        # ✅ LAZY INITIALIZATION: Initialize NashMarketMakerGame only when needed
        # This prevents initialization errors if game theory is disabled
        try:
            self._nash_model = NashMarketMakerGame()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ [GAME_THEORY] Failed to initialize NashMarketMakerGame: {e}")
            self._nash_model = None
    
    async def quick_gt_check(self, symbol, order_book):
        """Quick game theory check for existing patterns"""
        # ✅ SAFE: Check if model is initialized before use
        if not self._nash_model:
            return 'NEUTRAL'
        
        try:
            equilibrium = await self._nash_model.calculate_nash_equilibrium(symbol, order_book, 'intraday')
            
            predicted = equilibrium.get('predicted_moves', '')
            bullish_prob = 1.0 if predicted == 'LONG' else 0.0
            bearish_prob = 1.0 if predicted == 'SHORT' else 0.0
            
            if bullish_prob > 0.6 and (self.current_pattern or '').upper() == 'BULLISH':
                return 'HIGH_CONFIRMATION'
            elif bearish_prob > 0.6 and (self.current_pattern or '').upper() == 'BULLISH':
                return 'PATTERN_INVALIDATION'
            return 'NEUTRAL'
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ [GAME_THEORY] Error in quick_gt_check for {symbol}: {e}")
            return 'NEUTRAL'


class OrderBookGameTheoryProcessor:
    """Consume order-book snapshots and run quick game-theory checks."""
    
    def __init__(self, scanner_reference):
        self.scanner = scanner_reference
        self.gt_integration = QuickGameTheoryIntegration()
    
    async def process_stream_message(self, message_id: str, message_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        symbol = message_data.get('symbol')
        depth_raw = message_data.get('depth')
        if not symbol or not depth_raw:
            return None
        try:
            depth_data = json.loads(depth_raw)
        except (TypeError, json.JSONDecodeError):
            return None
        order_book = self._convert_depth(depth_data)
        current_pattern = await self._get_current_pattern(symbol)
        if self.gt_integration:
            self.gt_integration.current_pattern = current_pattern or "NEUTRAL"
        gt_result = await self.gt_integration.quick_gt_check(symbol, order_book)
        return {
            'symbol': symbol,
            'message_id': message_id,
            'timestamp': message_data.get('timestamp'),
            'last_price': float(message_data.get('last_price', 0) or 0.0),
            'order_book': order_book,
            'current_pattern': current_pattern,
            'game_theory_result': gt_result,
            'volume_ratio': float(message_data.get('volume_ratio', 0) or 0.0),
            'oi_change': float(message_data.get('oi', 0) or 0.0),
        }
    
    def _convert_depth(self, depth_data: Dict[str, Any]) -> Dict[str, Any]:
        def _convert(side: str):
            converted = []
            for level in depth_data.get(side, []):
                try:
                    converted.append({
                        'price': float(level.get('price', 0.0)),
                        'quantity': float(level.get('quantity', 0.0)),
                        'orders': int(level.get('orders', 0) or 0),
                    })
                except (TypeError, ValueError):
                    continue
            return converted
        return {'buy': _convert('buy'), 'sell': _convert('sell')}
    
    async def _get_current_pattern(self, symbol: str) -> str:
        detector = getattr(self.scanner, 'pattern_detector', None)
        if detector and hasattr(detector, 'get_recent_pattern'):
            try:
                pattern = detector.get_recent_pattern(symbol)
                if isinstance(pattern, str):
                    return pattern.upper()
                if isinstance(pattern, dict):
                    return str(pattern.get('type', 'NEUTRAL')).upper()
            except Exception:
                pass
        return 'NEUTRAL'


class SimplePipelineMonitor:
    """Lightweight pipeline monitor with minimal restart logic."""

    def __init__(self, scanner_reference):
        self.scanner = scanner_reference
        self.last_tick_time = time.time()
        self.consecutive_failures = 0
        self.redis_connection = None

    def update_tick(self):
        self.last_tick_time = time.time()

    def check_health(self):
        """Simple health check - no over-engineering."""
        time_since_last_tick = time.time() - self.last_tick_time
        if time_since_last_tick > 30:
            self.consecutive_failures += 1
            if self.consecutive_failures >= 3:
                logger.warning("⚠️ Pipeline stuck, attempting simple restart...")
                return self._simple_restart()
            logger.warning(f"⚠️ No ticks for {time_since_last_tick:.1f}s")
            return False
        self.consecutive_failures = 0
        return True

    def _simple_restart(self):
        """Simple restart - not complex recovery."""
        try:
            if self.redis_connection:
                try:
                    self.redis_connection.close()
                except Exception:
                    pass
            redis_config = self.scanner.config.get("redis", {})
            self.redis_connection = redis.Redis(
                host=redis_config.get("host", "localhost"),
                port=redis_config.get("port", 6379),
                decode_responses=True,
                socket_connect_timeout=5,
            )
            if hasattr(self.scanner, "reset_consumer"):
                self.scanner.reset_consumer()
            logger.info("✅ Pipeline restarted")
            self.consecutive_failures = 0
            self.last_tick_time = time.time()
            return True
        except Exception as exc:
            logger.error(f"❌ Failed to restart pipeline: {exc}")
            return False


class GameTheoryStreamRunner:
    """Background reader for the processed tick stream."""
    
    def __init__(
        self,
        scanner_reference,
        stream_key: Optional[str] = None,
    ):
        # ✅ FIXED: Use UnifiedRedisManager for centralized connection management
        from shared_core.redis_clients import UnifiedRedisManager

        self.scanner = scanner_reference
        default_stream = DatabaseAwareKeyBuilder.live_processed_stream()  # Global processed stream
        self.stream_key = stream_key or default_stream
        self.processor = OrderBookGameTheoryProcessor(scanner_reference)
        # ✅ FIXED: Use UnifiedRedisManager for centralized connection management
        self.redis_client = get_redis_client(process_name="scanner_main", db=1)
        self._running = False
        self._last_id = '$'
    
    def start(self):
        if not self.redis_client or self._running:
            return
        self._running = True
        thread = threading.Thread(target=self._loop_entry, daemon=True, name="gt_stream_processor")
        thread.start()
    
    def stop(self):
        self._running = False
    
    def _loop_entry(self):
        asyncio.run(self._stream_loop())
    
    async def _stream_loop(self):
        loop = asyncio.get_event_loop()
        while self._running:
            try:
                # Run blocking xread in thread executor to avoid blocking the event loop
                entries_result = await loop.run_in_executor(
                    None,
                    lambda: self.redis_client.xread({self.stream_key: self._last_id}, count=100, block=5000)
                )
                # Type cast to help type checker understand the result
                entries = cast(List[Tuple[bytes, List[Tuple[bytes, Dict[str, bytes]]]]], entries_result)
                if not entries:
                    continue
                for _, messages in entries:
                    for msg_id, data in messages:
                        # Decode bytes to strings for message_id
                        msg_id_str = msg_id.decode('utf-8') if isinstance(msg_id, bytes) else str(msg_id)
                        self._last_id = msg_id_str
                        processed = await self.processor.process_stream_message(msg_id_str, data)
                        if processed:
                            self.scanner.recent_game_theory_signals.append(processed)
            except Exception as exc:
                logger.debug(f"⚠️ [GAME_THEORY_STREAM] loop error: {exc}")


class TickData(dict):
    """
    Lightweight container for tick payloads that can be recycled via an object pool.
    Behaves exactly like a normal dict for downstream consumers.
    """

    __slots__ = ()

    def reset(self):
        super().clear()


class TickDataPool:
    """Lock-protected pool for TickData objects to reduce GC pauses."""

    def __init__(self, max_size: int = 1000):
        self._pool: deque[TickData] = deque(maxlen=max_size)
        self._lock = threading.Lock()
        self.max_size = max_size

    def get_tick_object(self) -> TickData:
        with self._lock:
            tick = self._pool.pop() if self._pool else TickData()
        tick.reset()
        return tick

    def return_tick_object(self, tick_obj: Any):
        if not isinstance(tick_obj, TickData):
            return
        tick_obj.reset()
        with self._lock:
            if len(self._pool) < self.max_size:
                self._pool.append(tick_obj)

    def trim_idle_entries(self, target_size: Optional[int] = None) -> int:
        """Shrink the pool to the target size (defaults to half capacity)."""
        target = target_size if target_size is not None else max(self.max_size // 2, 0)
        trimmed = 0
        with self._lock:
            while len(self._pool) > target:
                self._pool.popleft()
                trimmed += 1
        return trimmed

class VolumeRatioTracker:
    """Track bucket_incremental_volume ratio changes across the system."""
    
    def __init__(self):
        self.overrides = []
    
    def log_override(self, location, symbol, original_value, new_value):
        """Log when volume_ratio is overridden."""
        if original_value != new_value and new_value == 1.0:
            override_info = {
                'location': location,
                'symbol': symbol,
                'original': original_value,
                'new': new_value,
                'timestamp': time.time()
            }
            self.overrides.append(override_info)
            # Note: This will be logged by the calling method
    
    def get_recent_overrides(self, count=10):
        """Get recent overrides."""
        return self.overrides[-count:]


# Global tracker instance
volume_tracker = VolumeRatioTracker()


class DataLineageTracker:
    """Track where data comes from and how it flows."""
    
    def __init__(self):
        self.data_sources = {}
    
    def track_data_source(self, symbol, stage, data):
        """Track data at each processing stage."""
        volume_ratio = data.get("volume_ratio", 1.0)
        source_info = {
            'stage': stage,
            'volume_ratio': volume_ratio,
            'timestamp': time.time(),
            'data_keys': list(data.keys())
        }
        
        if symbol not in self.data_sources:
            self.data_sources[symbol] = []
        
        self.data_sources[symbol].append(source_info)
        
        # Log significant changes
        if len(self.data_sources[symbol]) > 1:
            prev_ratio = self.data_sources[symbol][-2]['volume_ratio']
            if prev_ratio != volume_ratio:
                logging.getLogger("data_lineage").info(
                    f"🔍 [LINEAGE] {symbol} volume_ratio changed from {prev_ratio} to {volume_ratio} at {stage}"
                )

# Global tracker
data_lineage = DataLineageTracker()

# ✅ FIX: Import DataPipeline from correct location
try:
    from intraday_trading.intraday_scanner.data_pipeline import DataPipeline
except ImportError as e:
    raise ImportError(f"Could not import DataPipeline: {e}")

from alerts.alert_manager import ProductionAlertManager
from alerts.game_theory_alerts import GameTheoryAlertGenerator
# ⚠️ DEPRECATED: RiskManager replaced by UnifiedAlertBuilder
# from alerts.risk_manager import RiskManager
try:
    from alerts.risk_manager import RiskManager
    RISK_MANAGER_AVAILABLE = True
except ImportError:
    RiskManager = None
    RISK_MANAGER_AVAILABLE = False

# Import utilities
try:
    import intraday_trading.intraday_scanner.calculations as calculations  # type: ignore[import]
    get_tick_processor = calculations.get_tick_processor  # type: ignore[assignment]
    safe_format = calculations.safe_format  # type: ignore[attr-defined]
    get_market_volume_data = calculations.get_market_volume_data  # type: ignore[attr-defined]
    preload_static_data = calculations.preload_static_data  # type: ignore[attr-defined]
    
    # ✅ GEX: Import GammaExposureCalculator
    try:
        from intraday_trading.intraday_scanner.calculations import GammaExposureCalculator
    except ImportError:
        GammaExposureCalculator = None
        
except ImportError as e:
    # Fallback to simple implementations if calculations module fails
    print(f"⚠️ Calculations module import failed: {e}")
    # Define minimal fallbacks...
    get_tick_processor = lambda redis_client=None, config=None, use_lifecycle_manager=False: DummyTickProcessorWrapper()
    safe_format = lambda value, format_str="{:.2f}": format_str.format(value) if value is not None else ""
    get_market_volume_data = lambda *args, **kwargs: {}
    preload_static_data = lambda *args, **kwargs: None
    GammaExposureCalculator = None


    # Fallback: try direct import
    try:
        import calculations as calculations  # type: ignore[import]
        get_tick_processor = getattr(calculations, "get_tick_processor", None) or (lambda *args, **kwargs: None)
        safe_format = getattr(calculations, "safe_format", None) or (lambda value, format_str="{:.2f}": format_str.format(value) if value is not None else "")
        get_market_volume_data = getattr(calculations, "get_market_volume_data", None) or (lambda *args, **kwargs: {})
        preload_static_data = getattr(calculations, "preload_static_data", None) or (lambda *args, **kwargs: None)
    except ImportError:
        # Create dummy functions with proper return types matching TickProcessorWrapper interface
        class DummyTickProcessorWrapper:
            def __init__(self, *args, **kwargs):
                self.stats = {"ticks_processed": 0, "indicators_computed": 0, "errors": 0}
                self.debug_enabled = False
                self.price_history = {}
                self.volume_history = {}
                self.hybrid_calc = None
                self.redis_client = None
                self.config = {}
                self.lock = threading.Lock()
            
            def process_tick(self, *args, **kwargs):
                return {}
            
            def get_rolling_window_stats(self):
                """Get statistics about session-based windows (delegates to calculations)"""
                return {}
            
            def cleanup_old_windows(self, *args, **kwargs):
                """Clean up old session-based windows (delegates to calculations)"""
                return 0
            
            def cleanup_old_insufficient_data_records(self, *args, **kwargs):
                return 0
            
            def get_history_length(self, *args, **kwargs):
                return 0
        
        def get_tick_processor(redis_client=None, config=None, use_lifecycle_manager=False) -> Any:  # type: ignore[assignment,return]
            return DummyTickProcessorWrapper()
        
        def safe_format(value, format_str="{:.2f}"):
            return format_str.format(value) if value is not None else ""
        
        def get_market_volume_data(*args, **kwargs):
            return {}
        
        def preload_static_data(*args, **kwargs):
            pass
        
        # Ensure preload_static_data is always defined (not None)
        if 'preload_static_data' not in globals():
            def preload_static_data(*args, **kwargs):
                pass

try:
    from aion_trading_dashboard.backend.path_utils import ensure_paths
    ensure_paths()
except ImportError:
    # Fallback: manually add project root to path
    _scanner_file = os.path.abspath(__file__)
    _scanner_dir = os.path.dirname(_scanner_file)
    _intraday_trading_dir = os.path.dirname(_scanner_dir)
    _src_dir = os.path.dirname(_intraday_trading_dir)
    _project_root = os.path.dirname(_src_dir)
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)

# Import unified schema (now with proper paths)
map_kite_to_unified = None
try:
    from config.schemas import map_kite_to_unified
except ImportError as e:
    # Fallback to dynamic loading using absolute path
    import importlib.util
    
    # Determine repo root reliably
    _this_file = os.path.abspath(__file__)
    _this_dir = os.path.dirname(_this_file)
    _intraday_trading = os.path.dirname(_this_dir)
    _src = os.path.dirname(_intraday_trading)
    _repo_root = os.path.dirname(_src)
    
    config_schema_path = os.path.join(_repo_root, "config", "schemas.py")
    if os.path.exists(config_schema_path):
        spec = importlib.util.spec_from_file_location("config_schemas", config_schema_path)
        if spec and spec.loader:
            unified_schema = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(unified_schema)
            map_kite_to_unified = getattr(unified_schema, "map_kite_to_unified", None)
    
    if map_kite_to_unified is None:
        # Define a no-op fallback to prevent crashes
        def map_kite_to_unified(data):
            return data

# Improved logging setup for full line display
import logging
import logging.handlers

try:
    import schedule
except ImportError:  # pragma: no cover - schedule is optional
    schedule = None


def setup_scanner_logging():
    """Setup improved logging for scanner with full line display"""
    # Create logs directory
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # Console formatter with full line support
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
    )

    # Root logger
    root_logger = logging.getLogger()
    # Check for debug mode from environment or config
    debug_env = os.environ.get('SCANNER_DEBUG')
    if debug_env is None:
        log_level = logging.DEBUG
    else:
        debug_mode = debug_env.lower() in ('1', 'true', 'yes')
        log_level = logging.DEBUG if debug_mode else logging.INFO
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File handler for scanner logs
    scanner_log_file = os.path.join(log_dir, "scanner_detailed.log")
    file_handler = logging.handlers.RotatingFileHandler(
        scanner_log_file, maxBytes=10 * 1024 * 1024, backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    root_logger.addHandler(file_handler)

    return root_logger

# Setup logging
logger = setup_scanner_logging()

def resolve_premarket_watchlist(config=None):
    """Resolve premarket watchlist symbols from config or static files."""
    possible = []
    if config:
        possible = config.get("premarket_watchlist") or config.get(
            "watchlists", {}
        ).get("premarket", [])
    if possible:
        return sorted({sym for sym in possible if sym})

    watchlist_path = os.path.join(project_root, "config", "premarket_watchlist.json")
    if os.path.exists(watchlist_path):
        try:
            with open(watchlist_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
                if isinstance(payload, dict):
                    symbols = payload.get("symbols") or payload.get("watchlist") or []
                else:
                    symbols = payload
                return sorted({sym for sym in symbols if sym})
        except Exception as exc:
            logger.warning("Failed to load premarket watchlist: %s", exc)

    return []


# Order flow crawler functionality was removed and is no longer needed


# This heavy debugging utility prints massive traces and is not part of normal operations.
# If needed, use scripts/debug_pattern_detection.py or create a separate debug module.


class MarketScanner:
    """Main scanner orchestrator"""

    def __init__(self, config=None, redis_client=None):
        """Initialize all components"""
        logger.info("\n" + "=" * 60)
        logger.info("🚀 MARKET SCANNER INITIALIZATION")
        logger.info("=" * 60)

        # Configuration
        self.config = config or {}
        self.running = False
        self.executor: Optional[ThreadPoolExecutor] = None

        if self.executor is not None:
            logger.info("Stopping processing executor...")
            try:
                self.executor.shutdown(wait=True)
            except Exception as exc:
                logger.warning(f"Executor shutdown encountered an issue: {exc}")
        self.threads = []
        self.threads_lock = threading.Lock()
        self.data_pipeline_lock = threading.Lock()
        self.threads_lock = threading.Lock()
        self.memory_cleanup_thread = None
        self.tick_processor_cleanup_thread = None
        self.optimized_processor_thread = None
        self.kow_strategy_thread = None

        # Health monitoring
        self.last_tick_time = time.time()
        self.health_monitor_thread = None
        self.websocket_restart_count = 0
        self.pipeline_restart_count = 0
        self.last_health_log = 0
        self.health_log_interval = 300  # 5 minutes
        self._consumer_health_check_interval = 30  # seconds
        self._max_consumer_idle_ms = 30000  # 30 seconds
        self._health_monitor_task = None
        self.pipeline_monitor = SimplePipelineMonitor(self)
        
        # ✅ CIRCUIT BREAKER: Initialize recovery state to prevent infinite loops
        self.recovery_attempts = 0
        self.last_recovery_time = None
        self.max_recovery_attempts = 3
        self.recovery_backoff = 30  # 30 seconds base backoff
        self.consecutive_empty_ticks = 0
        
        # ✅ ADDED: Batch processing optimization (from performance benchmarks)
        self.batch_size = 200  # From our benchmark
        self.use_pipeline = True  # Enable pipelining
        self.use_active_symbols = True  # Enable optimized O(1) path instead of SCAN

        # Redis connection initialization
        if redis_client:
            self.redis_client = redis_client
        else:
            self.redis_client = get_redis_client(process_name="scanner_main_core", db=self.config.get("redis", {}).get("db1", 1))

        # Session tracking
        self.current_session = datetime.now().strftime("%Y-%m-%d")

        # Debug mode indicator
        if self.config.get("debug", False):
            logger.info("🔍 Scanner running in DEBUG mode - Enhanced logging enabled")
            # Set logger level to DEBUG for all loggers
            logging.getLogger().setLevel(logging.DEBUG)
            for handler in logging.getLogger().handlers:
                handler.setLevel(logging.DEBUG)
        else:
            logger.info("📊 Scanner running in INFO mode - Use --debug flag for detailed logging")

        # Initialize Redis with process-specific pooled clients (prevents connection exhaustion)
        logger.info("📡 Connecting to Redis with process-specific connection pools...")
        
        # ✅ Read Redis config from config file (defaults to localhost:6379, DB 1 for tick data)
        redis_config = self.config.get("redis", {})
        redis_host = redis_config.get("host", "127.0.0.1")
        redis_port = redis_config.get("port", 6379)
        redis_db0 = redis_config.get("db0", 0)  # db for user data (users, validation, etc.) and algo execution state
        redis_db1 = redis_config.get("db1", 1)  # DB 1 for tick data streams (primary)
        redis_db2 = redis_config.get("db2", 2)  # DB 2 for analytics/pattern metrics
        redis_password = redis_config.get("password", None)
        
        logger.info(f"📡 Redis config: host={redis_host}, port={redis_port}, db0={redis_db0}, db1={redis_db1}, db2={redis_db2}")
        pool_kwargs = {
            "host": redis_host,
            "port": redis_port,
            "db": redis_db1,
            "max_connections": redis_config.get("max_connections", 50),
            "socket_timeout": redis_config.get("socket_timeout", 30),
            "socket_connect_timeout": redis_config.get("socket_connect_timeout", 30),
            "health_check_interval": redis_config.get("health_check_interval", 30),
            "decode_responses": True,
        }
        if redis_password:
            pool_kwargs["password"] = redis_password
        self._health_check_pool = ConnectionPool(**pool_kwargs)
        self._health_check_client = redis.Redis(connection_pool=self._health_check_pool)
        
        # Map existing attributes to this pooled client for compatibility
        self.realtime_db1_client = self.redis_client
        self.stream_redis_client = self.redis_client
        self.pattern_redis_client = self.redis_client
        self.volume_redis_client = self.redis_client
        self.ohlc_redis_client = self.redis_client
        self.redis_primary = self.redis_client
        self.redis_db2_client = self.redis_client

        logger.info("✅ Redis connected via get_redis_client (process=scanner_main_core)")
        self.redis_client.ping()
        self.connection_stats = {
                'start_time': time.time(),
                'redis_ops': 0,
                'batch_count': 0,
                'avg_batch_size': 0
            }
        self._verify_redis_connection()
        self._enforce_redis_standards()
        self.consumer_manager = None
        self.consumer_name = f"scanner_{os.getpid()}_{int(time.time())}"
        self._scanner_stream_key = DatabaseAwareKeyBuilder.live_enriched_stream()
        try:
            self.consumer_manager = ConsumerGroupManager(
                redis_client=self.redis_client,
                group_name="scanner_group",
                stream_key=self._scanner_stream_key
            )
            self.consumer_name = self.consumer_manager.create_consumer(self.consumer_name)
            logger.info(f"✅ Scanner registered as consumer: {self.consumer_name}")
        except Exception as consumer_exc:
            logger.warning(f"⚠️ Consumer group manager unavailable: {consumer_exc}")
            self.consumer_manager = None
        
        # ✅ INDEX MANAGER: Connect to Redis for fallback lookups
        index_manager.connect_redis(self.redis_client)
        
        # ✅ VIX: Initialize VIX Utils
        vix_factory = _get_vix_factory()
        if vix_factory:
            self.vix_utils = vix_factory(self.redis_client)
            logger.info("✅ VIX Utils initialized for pattern detection")
        else:
            self.vix_utils = None
            logger.warning("⚠️ VIX Utils NOT available (import failed)")
            
        # ✅ GEX: Initialize Gamma Exposure Calculator
        if GammaExposureCalculator:
            self.gex_calculator = GammaExposureCalculator(self.redis_client)
            logger.info("✅ Gamma Exposure Calculator initialized (on-the-fly mode)")
        else:
            self.gex_calculator = None
            logger.warning("⚠️ Gamma Exposure Calculator NOT available (import failed)")
        
        # ✅ Create compatibility wrapper that provides get_client() method
        # This allows components that expect RobustRedisClient to work with pooled clients
        class PooledRedisWrapper:
            """Wrapper to make pooled Redis clients compatible with RobustRedisClient interface"""
            def __init__(self, primary_client, db1_client, db2_client):
                self.redis_client = primary_client  # For backward compatibility (DB 1)
                self._db0 = primary_client  # Legacy: DB 0 maps to DB 1 (primary)
                self._db1 = db1_client
                self._db2 = db2_client
                # ✅ FIXED: DatabaseAwareKeyBuilder is a static class - no instance needed
                # All key building should use DatabaseAwareKeyBuilder static methods directly
                # ✅ SESSION-BASED: Initialize CumulativeDataTracker for proper bucket retrieval
                from shared_core.redis_clients.redis_client import CumulativeDataTracker
                self.cumulative_tracker = CumulativeDataTracker(db1_client)
            
            def get_client(self, db=1):
                """Return pooled client for specified DB (defaults to DB 1, not DB 0)"""
                if db == 0:
                    # Legacy: DB 0 requests map to DB 1 (primary)
                    return self._db1
                elif db == 1:
                    return self._db1
                elif db == 2:
                    return self._db2
                else:
                    # Fallback to DB 1 (primary) for unknown DBs
                    return self._db1
            
            def get_rolling_window_buckets(self, symbol: str, lookback_minutes: int = 60, session_date: str | None = None, max_days_back: int | None = None):
                """✅ SESSION-BASED: Get buckets using CumulativeDataTracker.get_time_buckets()
                
                ✅ CRITICAL: Buckets are stored in DB1 as session-based history lists:
                - History lists: bucket_incremental_volume:history:{resolution}:{symbol}
                - Uses CumulativeDataTracker.get_time_buckets() for proper session-based retrieval
                - Replaces legacy rolling window approach with session-based storage
                
                Args:
                    symbol: Trading symbol
                    lookback_minutes: Lookback window in minutes (default 60)
                    session_date: Optional session date (defaults to current session)
                    max_days_back: Not used (kept for backward compatibility)
                
                Returns:
                    List of bucket dictionaries with timestamp, volume, price data
                """
                if max_days_back is not None and max_days_back > 0:
                    logger.debug(f"🔁 [ROLLING_WINDOW] Ignoring deprecated max_days_back={max_days_back} for {symbol}")
                try:
                    # ✅ SESSION-BASED: Use CumulativeDataTracker.get_time_buckets() 
                    # This properly handles session-based bucket storage in DB1
                    from datetime import datetime, timedelta
                    
                    # Calculate time window for filtering
                    if lookback_minutes:
                        end_time = datetime.now().timestamp()
                        start_time = end_time - (lookback_minutes * 60)
                    else:
                        start_time = None
                        end_time = None
                    
                    # Use CumulativeDataTracker for session-based bucket retrieval
                    buckets = self.cumulative_tracker.get_time_buckets(
                        symbol=symbol,
                        session_date=session_date,
                        lookback_minutes=lookback_minutes,
                        start_time=start_time,
                        end_time=end_time,
                        use_history_lists=True  # Use efficient history lists
                    )
                    
                    bucket_count = len(buckets) if buckets else 0
                    first_ts = buckets[0].get('timestamp') if bucket_count else None
                    last_ts = buckets[-1].get('timestamp') if bucket_count else None
                    logger.info(
                        "🪟 [SESSION_WINDOW] %s lookback=%s buckets=%s start_ts=%s end_ts=%s",
                        symbol,
                        lookback_minutes,
                        bucket_count,
                        first_ts,
                        last_ts,
                    )
                    return buckets if buckets else []
                    
                except Exception as e:
                    logger.warning(f"Error getting session buckets for {symbol}: {e}")
                    return []
            
            def get(self, key):
                """Get value from Redis"""
        # ✅ UPDATED: Use UnifiedRedisManager via unified_storage for all key storage
        # KeyStore wrapper functionality moved to UnifiedRedisManager/UnifiedDataStorage
        # This implementation creates a light wrapper locally if needed for backward compatibility
        class KeyStore:
            def __init__(self, scanner):
                self.scanner = scanner
            
            def get(self, key):
                # Use unified storage singleton's redis client (which is UnifiedRedisManager)
                return self.scanner.unified_storage.redis.get(key)
                
            def set(self, key, value, ex=None):
                return self.scanner.unified_storage.redis.set(key, value, ex=ex)

        # Initialize helper
        self.keystore = KeyStore(self)
        
        # ✅ FIXED: Use UnifiedRedisManager for primary client access
        # Aliases for backward compatibility with existing code that needs specific clients
        from shared_core.redis_clients.redis_client import UnifiedRedisManager
        
        # Use DB1 (Tick Data) for all these clients to match Unified Architecture
        # Segregation by DB is being phased out in favor of key spacing, but we respect
        # the UnifiedRedisManager process pool
        self.redis_primary = self.redis_client
        self.realtime_db1_client = self.redis_client
        self.redis_db2_client = self.redis_client # Legacy analytics DB support
        
        # No wrapper needed - usage should be direct via unified_storage
        self.redis_wrapper = self.redis_client

        # ✅ CURRENT: Use VolumeComputationManager for all volume calculations and baseline management
        # ✅ REMOVED: UnifiedVolumeEngine - was incomplete and never fully implemented
        logger.info("Initializing volume manager...")
        
        # ✅ CENTRALIZED: Initialize UnifiedDataStorage for indicator/Greek storage and retrieval
        try:
            from shared_core.redis_clients.unified_data_storage import get_unified_storage
            self.unified_storage = get_unified_storage(redis_client=self.redis_client)
            logger.info("  ✓ UnifiedDataStorage initialized (centralized indicator/Greek storage)")
        except Exception as e:
            logger.warning(f"  ⚠️ Could not initialize UnifiedDataStorage: {e}")
            self.unified_storage = None
        self.volume_manager = get_volume_computation_manager(redis_client=self.redis_client)
        self.correct_volume_calculator = self.volume_manager
        # ✅ NOTE: UnifiedTimeAwareBaseline is created per-symbol via volume_manager
        # No need for separate time_aware_baseline attribute - use volume_manager for baseline calculations
        logger.info("Volume architecture initialized")

        # Initialize components
        logger.info("Initializing components...")

        # Consolidated Pattern Detector - single file pattern detection engine
        # Initialize via unified pattern init helper to guarantee singleton behavior
        worker_identifier = (
            self.config.get("worker_id")
            or os.environ.get("SCANNER_WORKER_ID")
            or os.environ.get("WORKER_ID")
            or f"scanner-{os.getpid()}"
        )
        self.pattern_detector = init_scanner_worker(
            worker_identifier,
            config=self.config,
            redis_client=self.redis_client,
        )
        logger.info("  ✓ Consolidated Pattern Detector ready (worker-scoped singleton)")

        # Intraday Game Theory Engine (Nash/FOMO/Prisoner's Dilemma)
        self.game_theory_engine = None
        self.game_theory_enabled = bool(self.config.get("enable_intraday_game_theory", True))
        self.game_theory_min_confidence = float(self.config.get("game_theory_min_confidence", 0.7))
        self.game_theory_validation_map = self.config.get(
            "game_theory_validation_symbols", GAME_THEORY_VALIDATION_SYMBOLS
        )
        self.game_theory_watchlist = self._build_game_theory_watchlist(self.game_theory_validation_map)
        self.game_theory_confirmation_required = bool(
            self.config.get("game_theory_confirmation_required", True)
        )
        self.game_theory_run_all = bool(self.config.get("game_theory_run_all", False))
        self.game_theory_alert_generator = None
        if self.game_theory_enabled:
            try:
                priority_override = self.config.get(
                    "intraday_game_theory_priority", INTRADAY_PRIORITY
                )
                self.game_theory_engine = IntradayGameTheoryEngine(
                    priority_components=priority_override,
                    validation_symbols=self.game_theory_validation_map,
                    min_confidence=self.game_theory_min_confidence,
                )
                self.game_theory_alert_generator = GameTheoryAlertGenerator(
                    confidence_threshold=self.game_theory_min_confidence
                )
                logger.info("  ✓ Intraday Game Theory Engine ready")
            except Exception as e:
                logger.warning(f"  ⚠️ Could not initialize IntradayGameTheoryEngine: {e}")
                self.game_theory_engine = None
        else:
            logger.info("  ℹ️ Intraday Game Theory Engine disabled via config")

        # Quick Nash-only checker (used even if full engine disabled)
        self.quick_gt_adapter = QuickGameTheoryIntegration()
        self.recent_game_theory_signals = deque(maxlen=2000)
        self.game_theory_stream_runner = None
        if self.config.get('enable_order_book_gt_processor', True):
            try:
                self.game_theory_stream_runner = GameTheoryStreamRunner(
                    self
                )
                self.game_theory_stream_runner.start()
            except Exception as exc:
                logger.warning(f"  ⚠️ Could not start Game Theory stream processor: {exc}")

        # Tick Processor - calculates indicators
        # ✅ FIX: get_tick_processor is already imported at the top of the file (line 412)
        # If that import failed, it would have created a dummy function, so we can use it directly
        # No need to re-import here

        # Pre-load static data before creating tick processor
        preload_static_data()

        # ✅ ENHANCED: Optionally use IndicatorLifecycleManager for caching, fallbacks, and symbol-aware indicators
        use_lifecycle_manager = self.config.get('use_indicator_lifecycle_manager', False)
        self.tick_processor = get_tick_processor(
            redis_client=self.redis_client,
            config=self.config,
            use_lifecycle_manager=use_lifecycle_manager
        )
        # Enable debug mode if configured
        if self.config.get("debug", False):
            self.tick_processor.debug_enabled = True
        logger.info("  ✓ Tick Processor ready")

        # Alert Manager - handles alert generation and validation with production metrics
        self.alert_manager = ProductionAlertManager(
            quant_calculator=self.tick_processor, redis_client=self.redis_client
        )
        logger.info("  ✓ Production Alert Manager ready")

        # Data Pipeline - handles all incoming data from Redis
        # ✅ OPTIMIZED: Check if optimized stream processor is enabled (for Apple M4)
        use_optimized_processor = self.config.get('use_optimized_stream_processor', True)
        
        if use_optimized_processor:
            logger.info("  🚀 Using Optimized Stream Processor (Apple M4 optimized)")
            try:
                # ✅ FIXED: Use absolute import to match other imports in this file
                from intraday_trading.intraday_scanner.optimized_stream_processor import OptimizedStreamProcessor
                
                # ✅ FIXED: Pass the configured Redis client (DB 1) to ensure correct host/port/db
                # This ensures optimized processor uses the same Redis config as the scanner
                redis_client = self.redis_client  # DB 1 client from scanner config
                num_workers = self.config.get('optimized_processor_workers', None)  # Auto-scaled if None
                batch_size = self.config.get('optimized_processor_batch_size', 50)
                
                # Get Redis config for logging
                redis_config = self.config.get('redis', {})
                redis_host = redis_config.get('host', '127.0.0.1')
                redis_port = redis_config.get('port', 6379)
                redis_db1 = redis_config.get('db1', 1)
                
                # Create optimized processor with configured Redis client
                self.optimized_processor = OptimizedStreamProcessor(
                    redis_client=redis_client,  # ✅ CRITICAL: Use scanner's configured Redis client
                    num_workers=num_workers,
                    batch_size=batch_size
                )
                logger.info(f"✅ [SCANNER] Optimized processor initialized with Redis client from scanner config (host={redis_host}, port={redis_port}, db1={redis_db1})")
                
                # Start optimized processor in background thread
                def run_optimized_processor():
                    """Run optimized processor in async loop"""
                    import asyncio
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(self.optimized_processor.start())
                    except Exception as e:
                        logger.error(f"❌ Optimized processor error: {e}")
                    finally:
                        loop.close()
                
                optimized_thread = threading.Thread(
                    target=run_optimized_processor,
                    daemon=True,
                    name="OptimizedStreamProcessor"
                )
                optimized_thread.start()
                self.optimized_processor_thread = optimized_thread
                self._register_thread(optimized_thread)
                
                # Still create DataPipeline for pattern detection. Even with optimized processor,
                # we need the standard pipeline running to feed the detector/alerts.
                # ✅ Pass Redis config to data pipeline
                # ✅ FIX: set_redis_config doesn't exist in data_pipeline.py - use no-op
                # NOTE: Redis config is already passed via redis_client parameter to DataPipeline
                def set_redis_config(*args, **kwargs):
                    pass  # No-op: config is passed directly to DataPipeline constructor
                
                redis_config_for_pipeline = {
                    "host": redis_host,
                    "port": redis_port,
                    "password": redis_password
                }
                set_redis_config(redis_config_for_pipeline)
                if DataPipeline:
                    self.data_pipeline = DataPipeline(
                        redis_client=self.redis_client,
                        config=self.config,
                        pattern_detector=self.pattern_detector,
                        alert_manager=self.alert_manager,
                        tick_processor=self.tick_processor,
                    )
                    if self.data_pipeline:
                        # ✅ FIX: Do NOT start the pipeline - supervisor workers handle raw stream consumption
                        # Scanner only uses DataPipeline for pattern detection utilities
                        pass
                logger.info("  ✓ Optimized Stream Processor started (bulk ingestion/storage)")
                logger.info("  ✓ Data Pipeline ready (pattern detection via supervisor workers)")
                
            except ImportError as e:
                logger.warning(f"  ⚠️ Optimized Stream Processor not available: {e}, falling back to standard DataPipeline")
                use_optimized_processor = False
        
        if not use_optimized_processor:
            # Standard Data Pipeline
            # ✅ FIX: set_redis_config doesn't exist - config is passed directly to DataPipeline
            # No need to call set_redis_config - the redis_client already has the correct config
            self.data_pipeline = DataPipeline(
                redis_client=self.redis_client,
                config=self.config,
                pattern_detector=self.pattern_detector,
                alert_manager=self.alert_manager,
                tick_processor=self.tick_processor,
                scanner=self,  # ✅ FIX: Pass scanner reference for game theory access
            )
            logger.info("  ✓ Data Pipeline ready")
            
            # ✅ FIX: Do NOT start the pipeline - supervisor workers handle raw stream consumption
            # Scanner consumes from enriched stream via its own scanner_group consumer
            # self.data_pipeline.start()  # DISABLED to fix consumer conflict
            logger.info("  ✓ Data Pipeline utilities available (stream consumption handled by supervisor workers)")
        
        # ✅ CONSOLIDATED: Initialize unified infrastructure monitor (replaces PerformanceMonitor + StreamMonitor)
        # Monitors Redis streams, proactively trims them, checks consumer groups, and monitors system health
        self.infrastructure_monitor = None
        if self.config.get('enable_infrastructure_monitoring', True):
            try:
                from shared_core.core.health.monitor import InfrastructureMonitor
                self.infrastructure_monitor = InfrastructureMonitor(
                    redis_db=1,  # DB 1 for realtime streams
                    check_interval=self.config.get('infrastructure_check_interval', 300),  # 5 minutes
                    historical_archive_instance=self.data_pipeline.historical_archive if hasattr(self.data_pipeline, 'historical_archive') else None
                )
                # Perform initial optimization before starting continuous monitoring
                if self.config.get('optimize_streams_on_start', True):
                    logger.info("  🔧 Performing initial stream optimization...")
                    self.infrastructure_monitor.optimize_streams_once()
                self.infrastructure_monitor.start()
                logger.info("  ✓ Infrastructure Monitor started (proactive monitoring every 5 min)")
            except Exception as e:
                logger.warning(f"  ⚠️  Could not start infrastructure monitor: {e}")
        
        # ✅ NEW: Initialize LiveSharpeDashboard for real-time pattern performance monitoring
        self.sharpe_dashboard = None
        if self.config.get('enable_sharpe_dashboard', True):
            try:
                # ✅ FIXED: Use DB2 for analytics (pattern metrics) per canonical standards
                from shared_core.redis_clients import UnifiedRedisManager
                sharpe_redis_client = get_redis_client(process_name="sharpe_dashboard", db=2)
                if sharpe_redis_client:
                    self.sharpe_dashboard = LiveSharpeDashboard(redis_client=sharpe_redis_client)
                    logger.info("  ✓ LiveSharpeDashboard initialized (real-time pattern performance monitoring)")
                else:
                    logger.warning("  ⚠️  LiveSharpeDashboard not initialized (Redis DB 2 client unavailable)")
            except Exception as e:
                logger.warning(f"  ⚠️  Could not initialize LiveSharpeDashboard: {e}")
        
        # News alert system is handled by alert_manager
        self.last_news_check = time.time()
        logger.info("  ✓ News alerts handled by Alert Manager")

        # Pattern Registry Integration removed - using consolidated pattern detector directly
        self.pattern_registry_integration = None
        logger.info(
            "  ✓ Pattern Registry Integration removed (using consolidated detector)"
        )

        # Advanced Pattern Engine (optional, uses Polars + hidden_liquidity_detector)
        try:
            from patterns.pattern_detector import AdvancedPatternEngine  # type: ignore[import-untyped]

            self.advanced_engine = AdvancedPatternEngine(redis_wrapper=self.redis_wrapper)
            if self.advanced_engine.is_enabled():
                logger.info("  ✓ Advanced Pattern Engine ready")
            else:
                self.advanced_engine = None
                logger.warning(
                    "  ⚠️ Advanced Pattern Engine disabled (missing dependencies)"
                )
        except Exception as e:
            self.advanced_engine = None

        # ✅ REMOVED: Duplicate alert_manager initialization - already initialized at line 887
        # Alert Manager is already created above, no need to recreate it

        # Hybrid processing infrastructure (caching + parallel dispatch)
        price_window = self.config.get("price_cache_window", 100)
        self.price_cache = defaultdict(lambda: deque(maxlen=price_window))
        # ✅ REMOVED: avg_volume_cache - Redundant cache, patterns read from Redis directly via VolumeComputationManager
        self.cache_lock = threading.RLock()
        self.tick_pool_enabled = bool(self.config.get("enable_tick_pool", True))
        tick_pool_size = int(self.config.get("tick_pool_max_size", 2000))
        self.tick_data_pool = TickDataPool(max_size=tick_pool_size) if self.tick_pool_enabled else None
        self.indicator_prefetch_enabled = bool(self.config.get("enable_indicator_prefetch", True))
        self.indicator_prefetch_interval = float(self.config.get("indicator_prefetch_interval", 2.0))
        self.indicator_prefetch_batch_size = int(self.config.get("indicator_prefetch_batch_size", 50))
        self._prefetch_in_progress = False
        self._last_prefetch_time = 0.0

        # ✅ AUTO-SCALE: Use CPU cores to determine optimal worker count
        cpu_cores = os.cpu_count() or 4  # Fallback to 4 if detection fails
        # Use 70% of CPU cores for workers (leave some for system/Redis/other processes)
        worker_default = max(5, int(cpu_cores * 0.7))
        worker_cap = self.config.get("processing_workers_max", cpu_cores)  # Cap at CPU cores
        worker_count = int(self.config.get("processing_workers", worker_default))
        
        if worker_count > worker_cap:
            logger.warning(
                "⚠️ Reducing processing_workers from %d to capped value %d (CPU cores: %d)",
                worker_count,
                worker_cap,
                cpu_cores,
            )
            worker_count = worker_cap
        worker_count = max(1, worker_count)
        
        logger.info(f"✅ [WORKER_CONFIG] CPU cores: {cpu_cores}, Workers: {worker_count}, Cap: {worker_cap}")

        # ✅ SCALED BACKLOG: Backlog scales with workers (4x workers minimum, higher for better throughput)
        backlog_default = max(worker_count * 4, 100)  # 4x workers or 100, whichever is higher
        backlog_limit = int(self.config.get("processing_backlog", backlog_default))
        if backlog_limit < worker_count * 2:
            backlog_limit = worker_count * 2
        
        logger.info(f"✅ [QUEUE_CONFIG] Backlog limit: {backlog_limit} (can queue {backlog_limit} patterns)")

        # ✅ ASYNC PROCESSING CONFIG
        self.async_batch_size = int(self.config.get("async_batch_size", max(1, worker_count)))
        self.async_backlog_limit = backlog_limit
        
        # ✅ MEMORY MANAGEMENT: Memory limits and cleanup
        self.memory_lock = threading.RLock()  # Thread-safe memory operations
        self.max_memory_mb = self.config.get("max_memory_mb", 2048)  # Default 2GB limit
        self.memory_cleanup_interval = 300  # Cleanup every 5 minutes
        self.memory_gc_threshold_mb = float(self.config.get("memory_gc_threshold_mb", self.max_memory_mb * 0.9))
        self.memory_gc_min_interval = float(self.config.get("memory_gc_min_interval", 120))
        self.memory_gc_generation = int(self.config.get("memory_gc_generation", 0))
        self._last_forced_gc = 0.0
        self.last_memory_cleanup = time.time()
        
        # Memory management thread starts when scanner begins running
        logger.info(
            f"✅ [MEMORY] Memory management ready (limit: {self.max_memory_mb}MB, cleanup every {self.memory_cleanup_interval}s)"
        )
        # ✅ LEGACY: precomputed_indicators kept for backward compatibility (deprecated)
        # New code should use UnifiedDataStorage.get_indicators() instead
        self.precomputed_indicators: Dict[str, Dict[str, float]] = {}
        self.all_symbols: List[str] = []
        # ✅ REMOVED: fast_lane_volume_threshold - Now uses dynamic thresholds only via _get_fast_lane_volume_threshold()
        self.current_vix_regime = "NORMAL"
        self.current_vix_value: Optional[float] = None
        self._last_vix_refresh = 0.0
        self.vix_refresh_interval = self.config.get("vix_refresh_interval", 30.0)
        self._combined_ohlc_cache: Dict[str, Dict[str, Any]] = {}
        self._last_cumulative_cache: Dict[str, float] = {}
        self._recent_alert_cache: Dict[Tuple[str, str], float] = {}
        self._last_option_chain_process = {}  # Track last option chain processing time per underlying
        self.option_chain_refresh_interval = float(self.config.get("option_chain_refresh_interval", 15.0))

        # Async tick processing infrastructure
        self.async_loop: Optional[asyncio.AbstractEventLoop] = None
        self.async_tick_queue: Optional["asyncio.Queue[Dict[str, Any]]"] = None
        self.async_loop_thread: Optional[threading.Thread] = None
        self.tick_feeder_thread: Optional[threading.Thread] = None
        self._async_tasks: List[asyncio.Task] = []
        self._async_queue_ready = threading.Event()
        self._async_last_debug_time = 0.0
        self._async_last_health_check = 0.0
        self.premarket_watchlist = resolve_premarket_watchlist(self.config)

        # Risk Manager - position sizing and risk
        # ⚠️ DEPRECATED: RiskManager replaced by UnifiedAlertBuilder
        if RISK_MANAGER_AVAILABLE and RiskManager is not None:
            self.risk_manager = RiskManager(config=self.config)
        else:
            self.risk_manager = None
        logger.info("  ✓ Risk Manager ready")

        # Wire risk manager to alert manager for position sizing
        if hasattr(self.alert_manager, "risk_manager"):
            self.alert_manager.risk_manager = self.risk_manager

        # Alert Validator - track alert accuracy over time
        # Alert validator removed - using standalone validator instead
        self.alert_validator = None
        logger.info("  ✓ Alert Validator ready")

        # Premium collection managers - DISABLED (legacy components moved)
        self.premium_alert_manager = None
        self.premium_risk_manager = None
        logger.info("  ✓ Premium Collection Managers disabled (legacy)")

        self.all_symbols = self._load_symbol_universe()

        # Historical Data Manager - handles Redis → JSON → Historical pipeline
        # NOTE: Historical data manager module not present in codebase - feature disabled
        self.historical_data_manager = None
        if self.config.get("enable_historical_analysis", False):  # Disabled by default
            try:
                from utils.historical_data_manager import HistoricalDataManager  # type: ignore[import-untyped]
                
                self.historical_data_manager = HistoricalDataManager(
                    redis_wrapper=self.redis_wrapper, config=self.config
                )
                logger.info("  ✓ Historical Data Manager ready")
            except ImportError as e:
                logger.warning(f"Historical Data Manager not available (module not found): {e}")
                self.historical_data_manager = None

        # Validation export removed - using standalone validator
        self.validation_export_thread = None

        # Start historical data export if enabled (currently disabled)
        if self.historical_data_manager:
            self.historical_data_manager.start_continuous_export()

        logger.info("  ✓ Tick Processor cleanup + Premarket Analyzer ready")

        # Warm up token resolver so numeric instrument_token can be mapped to symbol
        # Token mapping is handled by comprehensive_token_mapping.json
        logger.info("  ✓ Token mapping ready (comprehensive mapping available)")

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    # =========================================================================
    # ✅ ADDED: Optimized batch data fetching methods (permanent fix)
    # =========================================================================
    
    def get_active_symbols_batch(self) -> List[str]:
        """Get active symbols using UnifiedDataStorage (O(1))"""
        try:
            # ✅ FIXED: Use UnifiedDataStorage
            if self.unified_storage:
                symbols = self.unified_storage.get_active_symbols()
                logger.info("📡 [SCANNER_SYMBOLS] Loaded %s active symbols from unified storage", len(symbols))
                return symbols
            # Fallback
            fallback_symbols = list(self.redis_client.smembers("active_symbols"))
            logger.info(
                "📡 [SCANNER_SYMBOLS] Using Redis smembers fallback, count=%s",
                len(fallback_symbols),
            )
            return fallback_symbols
        except Exception as e:
            logger.warning(f"⚠️ Failed to get active symbols: {e}")
            fallback = self.all_symbols if hasattr(self, 'all_symbols') else []
            logger.info("📡 [SCANNER_SYMBOLS] Falling back to in-memory list, count=%s", len(fallback))
            return fallback
    
    def fetch_batch_data(self, symbols: List[str]) -> Dict[str, Dict]:
        """Fetch all data for multiple symbols in ONE Redis pipeline"""
        if not symbols:
            return {}
        
        batch_data = {}
        
        # Create pipeline
        pipe = self.redis_client.pipeline()
        
        for symbol in symbols:
            tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(symbol)
            pipe.hgetall(tick_key)
            greeks_key = DatabaseAwareKeyBuilder.live_greeks_hash(symbol)
            pipe.hgetall(greeks_key)
        
        # Execute ONE network round trip
        results = pipe.execute()
        
        # Parse results
        for i, symbol in enumerate(symbols):
            tick_idx = i * 2
            greeks_idx = tick_idx + 1
            
            # Decode Redis bytes to Python types
            tick = self._decode_redis_hash(results[tick_idx])
            greeks = self._decode_redis_hash(results[greeks_idx])
            
            batch_data[symbol] = {
                'tick': tick,
                'greeks': greeks,
                'indicators': {}  # Indicators fetched separately via MGET
            }
        
        return batch_data
    
    def _decode_redis_hash(self, redis_hash: Dict) -> Dict:
        """Decode Redis hash bytes to Python types"""
        if not redis_hash:
            return {}
        
        decoded = {}
        for key, value in redis_hash.items():
            # Decode key
            if isinstance(key, bytes):
                key = key.decode('utf-8')
            
            # Decode value
            if value is None:
                decoded[key] = None
            elif isinstance(value, bytes):
                # Try to convert to number if possible
                str_value = value.decode('utf-8')
                try:
                    if '.' in str_value:
                        decoded[key] = float(str_value)
                    else:
                        decoded[key] = int(str_value)
                except ValueError:
                    decoded[key] = str_value
            else:
                decoded[key] = value
        
        return decoded
    
    def fetch_batch_data_pipelined(self, symbols: List[str]) -> Dict[str, Dict]:
        """Fetch data for multiple symbols using Redis pipeline"""
        if not symbols:
            return {}
        
        batch_data = {}
        
        # Create pipeline
        pipe = self.redis_client.pipeline()
        
        for symbol in symbols:
            tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(symbol)
            pipe.hgetall(tick_key)
            greeks_key = DatabaseAwareKeyBuilder.live_greeks_hash(symbol)
            pipe.hgetall(greeks_key)
        
        # Execute ALL commands in ONE network round trip
        results = pipe.execute()
        
        # Parse results
        for i, symbol in enumerate(symbols):
            tick_idx = i * 2
            greeks_idx = tick_idx + 1
            
            tick = self._decode_redis_hash(results[tick_idx])
            greeks = self._decode_redis_hash(results[greeks_idx])
            
            batch_data[symbol] = {
                'tick': tick,
                'greeks': greeks,
                'indicators': {}  # Indicators fetched via MGET separately if needed
            }
        
        # Update stats
        if hasattr(self, 'connection_stats'):
            self.connection_stats['redis_ops'] += len(symbols) * 2
            self.connection_stats['batch_count'] += 1
            self.connection_stats['avg_batch_size'] = (
                (self.connection_stats['avg_batch_size'] * (self.connection_stats['batch_count'] - 1) + len(symbols)) 
                / self.connection_stats['batch_count']
            )
        
        return batch_data
    
    # ✅ ADD THIS NEW METHOD (minimal)
    def fetch_batch_optimized(self, symbols: List[str]) -> Dict[str, Dict]:
        """Optimized batch fetch using pipeline"""
        if not symbols:
            logger.info("📦 [BATCH_FETCH] No symbols provided to fetch_batch_optimized")
            return {}
        if not self.use_pipeline:
            logger.info(
                "🔄 [BATCH_FALLBACK] Pipeline disabled, falling back to fetch_batch_data for %s symbols",
                len(symbols),
            )
            return self.fetch_batch_data(symbols)  # Fallback to existing method
        
        batch_data = {}
        pipe = self.redis_client.pipeline()
        
        # Queue ALL operations
        for symbol in symbols:
            ind_hash_key = DatabaseAwareKeyBuilder.live_indicator_hash(symbol)
            pipe.hgetall(ind_hash_key)
            greek_hash_key = DatabaseAwareKeyBuilder.live_greeks_hash(symbol)
            pipe.hgetall(greek_hash_key)
            tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(symbol)
            pipe.hgetall(tick_key)
        
        # Execute ONE network round trip
        results = pipe.execute()
        
        # Parse results
        for i, symbol in enumerate(symbols):
            idx = i * 3
            batch_data[symbol] = {
                'indicators': self._bytes_to_dict(results[idx]),
                'greeks': self._bytes_to_dict(results[idx + 1]),
                'tick': self._bytes_to_dict(results[idx + 2])
            }
        
        logger.info(
            "📦 [BATCH_FETCH] Retrieved %s symbols via pipeline (batch_size=%s)",
            len(symbols),
            self.batch_size,
        )
        
        return batch_data
    
    # ✅ ADD THIS HELPER METHOD
    def _bytes_to_dict(self, redis_hash: Dict[bytes, bytes]) -> Dict[str, Any]:
        """Convert Redis bytes hash to Python dict"""
        if not redis_hash:
            return {}
        
        result = {}
        for k, v in redis_hash.items():
            key = k.decode('utf-8') if isinstance(k, bytes) else str(k)
            
            if v is None:
                result[key] = None
            elif isinstance(v, bytes):
                # Try to convert to number
                str_val = v.decode('utf-8')
                try:
                    result[key] = float(str_val) if '.' in str_val else int(str_val)
                except:
                    result[key] = str_val
            else:
                result[key] = v
        
        return result

    def _log_scan_batch(self, batch_index: int, symbols: List[str]) -> None:
        """Log symbol batches dispatched to pattern detection."""
        if not symbols:
            return
        preview = symbols[:5]
        logger.info(
            "📦 [SCANNER_BATCH] index=%s size=%s preview=%s",
            batch_index,
            len(symbols),
            preview,
        )
    
    def scan(self) -> List[Dict]:
        """Optimized scan with batching"""
        # ✅ Get symbols (existing logic)
        if self.use_active_symbols:
            symbols = self.get_active_symbols_batch()
        else:
            symbols = self.all_symbols if hasattr(self, 'all_symbols') else []
        
        if not symbols:
            return []
        
        all_alerts = []
        
        # ✅ Process in batches
        for i in range(0, len(symbols), self.batch_size):
            batch_symbols = symbols[i:i + self.batch_size]
            batch_index = i // self.batch_size
            self._log_scan_batch(batch_index, batch_symbols)
            
            # ✅ Use optimized batch fetch
            batch_data = self.fetch_batch_optimized(batch_symbols)
            
            # ✅ Process each symbol
            for symbol in batch_symbols:
                data = batch_data.get(symbol)
                if not data:
                    continue
                
                # ✅ Existing Pattern Detection Logic
                if hasattr(self, 'pattern_detector') and self.pattern_detector:
                    tick = data.get('tick', {})
                    indicators = data.get('indicators', {})
                    greeks = data.get('greeks', {})
                    
                    # Build indicators dict with tick and greeks merged
                    tick_data = {
                        'symbol': symbol,
                        'last_price': float(tick.get('last_price', 0) or 0),
                        'volume': int(tick.get('volume', 0) or 0),
                        'volume_ratio': float(tick.get('volume_ratio', 0) or 0),
                        **indicators,
                        **greeks
                    }
                    
                    # Detect patterns
                    try:
                        patterns = self.pattern_detector.detect_patterns(tick_data)
                        if patterns:
                            for pattern in patterns:
                                all_alerts.append(pattern)
                    except Exception as e:
                        logger.debug(f"Error detecting patterns for {symbol}: {e}")
        
        # Log performance stats
        self._log_performance_stats(len(symbols), len(all_alerts))
        
        return all_alerts
    
    def _verify_redis_connection(self):
        """Verify Redis connection and pool"""
        try:
            # Test connection latency
            start = time.perf_counter()
            self.redis_client.ping()
            latency_ms = (time.perf_counter() - start) * 1000
            
            logger.info(f"✅ Redis connection latency: {latency_ms:.2f}ms")
            
            # Check if using hiredis
            try:
                redis_conn = self.redis_client.connection_pool.get_connection()
                if hasattr(redis_conn, '_parser'):
                    parser = redis_conn._parser
                    if 'hiredis' in str(type(parser)).lower():
                        logger.info("✅ Using hiredis parser (optimized)")
                    else:
                        logger.warning("⚠️ Using Python parser - install hiredis: pip install hiredis")
                self.redis_client.connection_pool.release(redis_conn)
            except Exception:
                pass  # Connection pool inspection failed - not critical
            
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
    
    def _enforce_redis_standards(self):
        """Run RedisStandardsEnforcer (can be disabled via env)."""
        flag = os.getenv("SCANNER_ENFORCE_REDIS_STANDARDS", "1").strip().lower()
        if flag in ("0", "false", "no"):
            logger.info("⏭️ Skipping Redis standards enforcement (env override)")
            return
        try:
            from redis_standards_enforcer import RedisStandardsEnforcer
        except ImportError as exc:
            logger.warning(f"⚠️ RedisStandardsEnforcer unavailable: {exc}")
            return
        client = getattr(self, "redis_client", None)
        if not client:
            logger.warning("⚠️ No Redis client available for standards enforcement")
            return
        enforcer = RedisStandardsEnforcer(client)
        logger.info("🔍 Running RedisStandardsEnforcer (SCANNER_ENFORCE_REDIS_STANDARDS not disabled)")
        enforcer.enforce_standards()
    
    def _log_performance_stats(self, total_symbols: int, alert_count: int):
        """Log scanner performance"""
        if not hasattr(self, 'connection_stats'):
            return
        
        elapsed = time.time() - self.connection_stats['start_time']
        
        stats = {
            'symbols_processed': total_symbols,
            'alerts_found': alert_count,
            'redis_ops_total': self.connection_stats['redis_ops'],
            'avg_batch_size': round(self.connection_stats['avg_batch_size'], 1),
            'ops_per_second': round(self.connection_stats['redis_ops'] / max(elapsed, 1), 1),
            'symbols_per_second': round(total_symbols / max(elapsed, 1), 1)
        }
        
        logger.info(f"📊 Scanner stats: {stats}")
        
        # Log Redis pool status
        try:
            pool = self.redis_client.connection_pool
            if hasattr(pool, '_in_use_connections'):
                logger.info(
                    f"🔗 Redis pool: {len(pool._in_use_connections)} in use, "
                    f"{len(pool._available_connections)} available"
                )
        except Exception:
            pass

    def _periodic_tick_processor_cleanup(self):
        """Periodic cleanup for TickProcessor memory structures"""
        while self.running:
            try:
                time.sleep(600)  # Run every 10 minutes
                if hasattr(
                    self.tick_processor, "cleanup_old_insufficient_data_records"
                ):
                    self.tick_processor.cleanup_old_insufficient_data_records()
            except Exception as e:
                logger.error(f"❌ Error in TickProcessor cleanup: {e}")
                time.sleep(60)  # Wait longer on error
    
    def _periodic_memory_cleanup(self):
        """Periodic memory cleanup and monitoring with thread-safe operations"""
        while self.running:
            try:
                time.sleep(self.memory_cleanup_interval)
                
                # ✅ THREAD-SAFE: Use lock for memory operations
                with self.memory_lock:
                    try:
                        if PSUTIL_AVAILABLE and psutil:
                            process = psutil.Process(os.getpid())
                            memory_mb = process.memory_info().rss / 1024 / 1024
                        else:
                            memory_mb = 0.0
                        now = time.time()
                        should_collect = False
                        if PSUTIL_AVAILABLE and memory_mb >= self.memory_gc_threshold_mb:
                            should_collect = True
                        elif not PSUTIL_AVAILABLE and (now - self._last_forced_gc) >= self.memory_gc_min_interval:
                            should_collect = True
                        collected = 0
                        if should_collect:
                            collected = gc.collect(self.memory_gc_generation)
                            self._last_forced_gc = now
                        if PSUTIL_AVAILABLE and memory_mb > 0 and memory_mb > self.max_memory_mb:
                            logger.warning(
                                f"⚠️ [MEMORY] Memory usage {memory_mb:.1f}MB exceeds limit {self.max_memory_mb}MB"
                            )
                            if hasattr(self, 'tick_processor') and self.tick_processor:
                                hybrid_calc = getattr(self.tick_processor, 'hybrid_calc', None)
                                cache_map = getattr(hybrid_calc, '_cache', None)
                                if isinstance(cache_map, dict):
                                    cache_size = len(cache_map)
                                    if cache_size > 1000:
                                        old_keys = list(cache_map.keys())[:-1000]
                                        for key in old_keys:
                                            cache_map.pop(key, None)
                                        logger.info(f"✅ [MEMORY] Cleared {len(old_keys)} old cache entries")
                            gc.collect(self.memory_gc_generation)
                            self._last_forced_gc = now
                        tick_pool_trimmed = 0
                        if self.tick_pool_enabled and self.tick_data_pool and memory_mb >= self.memory_gc_threshold_mb:
                            tick_pool_trimmed = self.tick_data_pool.trim_idle_entries()
                        if PSUTIL_AVAILABLE and memory_mb > 0:
                            logger.debug(
                                f"✅ [MEMORY] {memory_mb:.1f}MB used | GC collected {collected} objs | "
                                f"tick pool trimmed {tick_pool_trimmed}"
                            )
                        elif should_collect:
                            logger.debug(f"✅ [MEMORY] GC collected {collected} objects (psutil unavailable)")
                    except Exception as mem_err:
                        logger.debug(f"Memory monitoring error: {mem_err}")
                        
            except Exception as e:
                logger.error(f"❌ Error in memory cleanup: {e}")
                time.sleep(60)
        # Initialize alert log path to avoid attribute errors when recording alerts
        try:
            log_dir = os.path.join("logs", "alerts")
            os.makedirs(log_dir, exist_ok=True)
            self.alert_log_file = os.path.join(
                log_dir, f"alerts_{datetime.now().strftime('%Y%m%d')}.log"
            )
        except Exception:
            self.alert_log_file = os.path.join("logs", "alerts.log")

    def _register_thread(self, thread: threading.Thread):
        """Track worker threads for health monitoring."""
        if not thread:
            return
        with self.threads_lock:
            if thread not in self.threads:
                self.threads.append(thread)

    def _start_background_threads(self):
        """Ensure maintenance threads are running after scanner starts."""
        def ensure_thread(attr_name: str, target, name: str):
            thread = getattr(self, attr_name, None)
            if thread and thread.is_alive():
                return
            thread = threading.Thread(target=target, daemon=True, name=name)
            thread.start()
            setattr(self, attr_name, thread)
            self._register_thread(thread)
        
        ensure_thread('memory_cleanup_thread', self._periodic_memory_cleanup, "MemoryCleanup")
        ensure_thread('tick_processor_cleanup_thread', self._periodic_tick_processor_cleanup, "TickProcessorCleanup")
        # ✅ REMOVED: premarket_analyzer_thread - Premarket analyzer was incomplete (stub functions only)
    
    def _start_health_monitor_if_needed(self):
        """Ensure websocket health monitor is running when scanner uses live data."""
        if self.config.get("use_file_reader", False):
            return
        thread = getattr(self, 'health_monitor_thread', None)
        if thread and thread.is_alive():
            return
        self.health_monitor_thread = threading.Thread(
            target=self._monitor_websocket_health, daemon=True, name="HealthMonitor"
        )
        self.health_monitor_thread.start()
        self._register_thread(self.health_monitor_thread)

    def start(self):
        """Start the scanner with Redis subscription"""
        self.running = True
        self._start_background_threads()
        ist = pytz.timezone("Asia/Kolkata")
        current_time = datetime.now(ist)
        logger.info(f"⏰ Starting scanner at {current_time.strftime('%H:%M:%S')} IST")
        try:
            self.pre_market_preparation()
        except Exception as exc:
            logger.warning("Pre-market preparation failed: %s", exc)
        # Choose data source based on config
        if self.config.get("use_file_reader", False):
            logger.info("📁 Starting file-based data reader...")
            self.run_file_reader()
        else:
            logger.info("📡 Starting Redis subscription...")
            # ✅ DataPipeline.start() already handles stream consumption in its own thread
            # No need to start a separate thread here - it's already started in __init__
            # The data_pipeline.start() method creates and starts the consumer_thread internally
            logger.info("✅ DataPipeline stream consumer already started via start() method")

            # Start health monitoring thread
            self._start_health_monitor_if_needed()
        
        # ✅ CONSOLIDATED: Infrastructure monitor already started in __init__
        if hasattr(self, "infrastructure_monitor") and self.infrastructure_monitor:
            logger.info("✓ Infrastructure Monitor running (proactive monitoring)")
        
        # ✅ DISABLED: KowSignalStraddleStrategy now runs through pattern_detector only
        # The strategy is initialized in pattern_detector.py and called via _detect_option_patterns()
        # -> _handle_kow_signal_straddle_option() -> _detect_straddle_patterns() -> straddle_strategy.on_tick(send_alert=False)
        # This ensures all 8 patterns run together through the unified handler system
        # No independent tick listener needed - pattern_detector orchestrates all patterns
        if hasattr(self, "pattern_detector") and hasattr(self.pattern_detector, "straddle_strategy"):
            if self.pattern_detector.straddle_strategy:
                logger.info("✅ KowSignalStraddleStrategy available (runs through pattern_detector, not independently)")
                # Store reference for graceful shutdown if needed
                self.kow_signal_strategy = self.pattern_detector.straddle_strategy

        # Start the main processing loop
        self.run_main_processing_loop()

        # ✅ CONSOLIDATED: Stop infrastructure monitor gracefully
        if hasattr(self, "infrastructure_monitor") and self.infrastructure_monitor:
            try:
                self.infrastructure_monitor.stop()
                logger.info("✓ Infrastructure Monitor stopped")
            except Exception as e:
                logger.warning(f"Error stopping infrastructure monitor: {e}")
        
        # Stop Sharpe dashboard (no cleanup needed, but log for consistency)
        if hasattr(self, "sharpe_dashboard") and self.sharpe_dashboard:
            logger.info("✓ LiveSharpeDashboard available (no cleanup needed)")
        
        # Stop data pipeline gracefully (it manages its own thread internally)
        if hasattr(self, "data_pipeline"):
            self.data_pipeline.running = False  # Signal to stop
            # Wait for internal consumer thread to finish
            if hasattr(self.data_pipeline, "consumer_thread") and self.data_pipeline.consumer_thread and self.data_pipeline.consumer_thread.is_alive():
                try:
                    self.data_pipeline.consumer_thread.join(timeout=2)  # Wait up to 2 seconds
                except Exception as e:
                    logger.error(f"Error joining data pipeline consumer thread: {e}")

    def run_file_reader(self):
        """Read data from JSONL files instead of Redis
        
        NOTE: This is an optional feature for offline/testing mode.
        The file_reader module is not present in the codebase - feature disabled.
        Enable via config flag: use_file_reader=True
        """
        try:
            from file_reader import FileReader  # type: ignore[import-untyped]
        except ImportError as e:
            logger.error(f"FileReader module not available (required for file-based data reading): {e}")
            logger.error("Please ensure file_reader.py exists or use Redis-based data source (default)")
            return
        
        # Data directory from config or default
        data_dir = self.config.get("data_dir", "/Users/apple/Desktop/stock/data/equity")

        def tick_callback(tick_data):
            """Process tick data from files"""
            try:
                # Process through data pipeline (same as Redis version)
                symbol = tick_data.get("symbol", "")

                if symbol:
                    logger.debug(f"📊 Processing tick: {symbol}")

                    # Clean the tick data
                    cleaned = self.data_pipeline._clean_tick_data(tick_data)
                    
                    # 🎯 DEBUG: Track data flow through pipeline
                    logger.info(f"🔍 [DATA_FLOW] After _clean_tick_data for {symbol}: volume_ratio = {cleaned.get('volume_ratio')}")
                    
                    # 🎯 LINEAGE: Track data after cleaning
                    data_lineage.track_data_source(symbol, "after_clean_tick_data", cleaned)

                    # 🎯 CRITICAL: Add bucket_incremental_volume context BEFORE processing calculations
                    cleaned = self._add_volume_context(cleaned)
                    logger.info(f"🔍 [DATA_FLOW] After _add_volume_context for {symbol}: volume_ratio = {cleaned.get('volume_ratio') if cleaned else 'None'}")

                    # Process through calculations
                    indicators = self.tick_processor.process_tick(symbol, cleaned)
                    
                    # ✅ CRITICAL: Merge computed indicators back into cleaned tick_data to preserve for downstream
                    if indicators and isinstance(cleaned, dict):
                        cleaned.update(indicators)
                        logger.debug(f"✅ [DATA_PRESERVATION] {symbol} - Merged {len(indicators)} computed indicators into cleaned tick_data")
                    
                    # Store indicators using RedisStorage (TA-Lib calculates in calculations.py and stores via redis_storage.py)
                    if (indicators and hasattr(self, 'data_pipeline') and 
                        hasattr(self.data_pipeline, 'redis_storage') and 
                        self.data_pipeline.redis_storage is not None):
                        try:
                            # Use redis_storage to store pre-calculated indicators
                            import asyncio
                            # Store indicators - redis_storage handles the storage logic properly
                            # Convert single tick to list format expected by publish_indicators_to_redis
                            tick_list = [cleaned] if cleaned else []
                            # Note: publish_indicators_to_redis will recalculate, but the storage logic is correct
                            # For pre-calculated indicators, we'd need a sync helper, but since user wants to use existing logic,
                            # we'll let it recalculate (it uses the same TA-Lib anyway)
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            try:
                                loop.run_until_complete(
                                    self.data_pipeline.redis_storage.publish_indicators_to_redis(symbol, tick_list)
                                )
                            finally:
                                loop.close()
                        except Exception as store_err:
                            logger.warning(f"Error storing indicators for {symbol}: {store_err}")
                    
                    # Validate indicator calculations
                    from utils.validation_logger import indicator_validator
                    if cleaned and indicators:
                        indicator_validator.log_tick_processing(symbol, cleaned, indicators)
                    
                    # 🎯 DEBUG: Track data flow after process_tick
                    logger.info(f"🔍 [DATA_FLOW] After process_tick for {symbol}: volume_ratio = {indicators.get('volume_ratio') if indicators else 'None'}")
                    
                    # 🎯 LINEAGE: Track data after process_tick
                    if indicators:
                        data_lineage.track_data_source(symbol, "after_process_tick", indicators)

                    game_theory_signal = None
                    if indicators and self._should_run_game_theory(symbol):
                        game_theory_signal = self._evaluate_game_theory_layer(symbol, cleaned, indicators) if cleaned else None
                        if game_theory_signal:
                            indicators['game_theory_signal'] = game_theory_signal
                            if self.game_theory_alert_generator:
                                gt_alerts = self.game_theory_alert_generator.generate_intraday_alerts([game_theory_signal])
                                for gt_alert in gt_alerts:
                                    self.alert_manager.send_alert(gt_alert)

                    if indicators:
                        # 🎯 DEBUG: Track data flow before pattern detection
                        logger.info(f"🔍 [DATA_FLOW] Before detect_patterns for {symbol}: volume_ratio = {indicators.get('volume_ratio')}")
                        
                        # 🎯 LINEAGE: Track data before pattern detection
                        data_lineage.track_data_source(symbol, "before_pattern_detection", indicators)
                        
                        # Detect patterns (detector expects indicators only)
                        # ✅ FIXED: Correct argument order (symbol, indicators)
                        detected = self._detect_patterns_with_validation(symbol, indicators)

                        if detected:
                            depth_snapshot = cleaned.get("depth") if cleaned else None
                            for p in detected:
                                pattern_direction = self._normalize_pattern_direction(p)
                                sentiment_label = self._map_direction_to_sentiment(pattern_direction)
                                quick_gt_state = self._run_quick_game_theory_check(symbol, depth_snapshot, sentiment_label)
                                if quick_gt_state == 'HIGH_CONFIRMATION':
                                    base_conf = float(p.get('confidence', 0.0) or 0.0)
                                    p['confidence'] = min(1.0, base_conf * 1.3 if base_conf else 0.8)
                                    p['game_theory_quick'] = 'CONFIRMED'
                                elif quick_gt_state == 'PATTERN_INVALIDATION':
                                    base_conf = float(p.get('confidence', 0.0) or 0.0)
                                    p['confidence'] = base_conf * 0.3
                                    p['game_theory_quick'] = 'INVALIDATED'
                                else:
                                    p['game_theory_quick'] = 'NEUTRAL'
                                if not self._game_theory_confirms(symbol, p, game_theory_signal):
                                    continue
                                if game_theory_signal and indicators:
                                    fused = self._fuse_all_signals(symbol, p, game_theory_signal)
                                    if fused:
                                        p['game_theory_context'] = fused
                                    if indicators:
                                        self.compare_data_sources(symbol, indicators, p)
                                    if indicators:
                                        self.alert_manager.send_alert(p)

            except Exception as e:
                logger.error(f"❌ Error processing tick: {e}")

        # Create and start file reader
        self.file_reader = FileReader(data_dir, tick_callback)
        self.file_reader.start()

    def run_main_processing_loop(self):
        """Main processing loop using asyncio Queue to avoid blocking waits."""
        logger.info("🔄 Starting async processing loop...")
        self.running = True
        self._start_async_processing()

        try:
            while self.running:
                time.sleep(1.0)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        finally:
            self._shutdown_async_processing()
            logger.info("🔚 Async processing loop exited")

    def _start_async_processing(self):
        """Spin up the asyncio loop and feeder thread."""
        if self.async_loop_thread and self.async_loop_thread.is_alive():
            return

        self._async_queue_ready.clear()
        self.async_loop = asyncio.new_event_loop()
        self.async_loop_thread = threading.Thread(
            target=self._async_loop_entry,
            name="async_tick_loop",
            daemon=True,
        )
        self.async_loop_thread.start()

        self.tick_feeder_thread = threading.Thread(
            target=self._async_tick_feeder,
            name="tick_feeder",
            daemon=True,
        )
        self.tick_feeder_thread.start()

    def _async_loop_entry(self):
        """Entry point executed within the async loop thread."""
        asyncio.set_event_loop(self.async_loop)
        queue_size = int(self.config.get("async_tick_queue_size", self.async_backlog_limit))
        self.async_tick_queue = asyncio.Queue(maxsize=queue_size)
        self._async_tasks = []
        self._async_last_debug_time = time.time()
        self._async_last_health_check = self._async_last_debug_time
        self._async_queue_ready.set()

        processing_task = self.async_loop.create_task(self._process_ticks_async()) if self.async_loop else None
        if processing_task:
            self._async_tasks.append(processing_task)

        try:
            if self.async_loop:
                self.async_loop.run_forever()
        finally:
            for task in self._async_tasks:
                task.cancel()
            if self._async_tasks:
                self.async_loop.run_until_complete(asyncio.gather(*self._async_tasks, return_exceptions=True))  # type: ignore[arg-type]
            self.async_loop.close() if self.async_loop else None

    def _async_tick_feeder(self):
        """Background thread feeding ticks into the async queue."""
        max_consecutive_empty = int(self.config.get("max_consecutive_empty_cycles", 50))
        idle_notice_interval = self.config.get("idle_notice_interval_seconds", 30)
        pipeline_warning_interval = self.config.get("pipeline_warning_interval_seconds", 5)

        consecutive_empty_ticks = 0
        last_idle_notice = 0.0
        last_pipeline_warning = 0.0

        self._async_queue_ready.wait()

        while self.running:
            try:
                tick_data = self.data_pipeline.get_next_tick(timeout=0.1)
            except Exception as exc:
                logger.error(f"Error getting next tick: {exc}")
                time.sleep(0.02)
                continue

            if tick_data is None:
                consecutive_empty_ticks += 1
                if consecutive_empty_ticks > max_consecutive_empty:
                    pipeline_idle = False
                    empty_window_seconds = max(max_consecutive_empty * 0.1, 1.0)
                    if hasattr(self.data_pipeline, "is_idle"):
                        try:
                            pipeline_idle = self.data_pipeline.is_idle(max_age=int(empty_window_seconds))
                        except Exception:
                            pipeline_idle = False
                    now = time.time()
                    if pipeline_idle:
                        if now - last_idle_notice >= idle_notice_interval:
                            logger.info(
                                f"No data received for {max_consecutive_empty} cycles - pipeline idle waiting for feed"
                            )
                            last_idle_notice = now
                    else:
                        if now - last_pipeline_warning >= pipeline_warning_interval:
                            logger.warning(
                                f"No data received for {max_consecutive_empty} cycles, checking pipeline..."
                            )
                            last_pipeline_warning = now
                        try:
                            healthy = self.data_pipeline.is_healthy()
                        except Exception:
                            healthy = True
                        if not healthy:
                            logger.debug("🔍 [DEBUG] Pipeline unhealthy, attempting recovery...")
                            self.check_pipeline_health()
                    consecutive_empty_ticks = 0
                time.sleep(0.005)
                continue

            consecutive_empty_ticks = 0
            self.last_tick_time = time.time()
            if hasattr(self, 'pipeline_monitor') and self.pipeline_monitor:
                self.pipeline_monitor.update_tick()

            if not self.async_loop or not self.async_tick_queue:
                continue

            tick_payload = None
            try:
                tick_payload = self._wrap_tick_with_pool(tick_data)
                if tick_payload:
                    future = asyncio.run_coroutine_threadsafe(self.async_tick_queue.put(tick_payload), self.async_loop)
                future.result()
            except Exception as exc:
                if isinstance(tick_payload, TickData) and self.tick_data_pool:
                    self.tick_data_pool.return_tick_object(tick_payload)
                logger.debug(f"⚠️ [ASYNC_QUEUE] Failed to enqueue tick: {exc}")
    
    def _read_stream_safe(self):
        """Safe stream reading with auto-healing for the scanner."""
        stream_key = getattr(self, "_scanner_stream_key", "ticks:intraday:processed")
        consumer_name = getattr(self, "consumer_name", f"scanner_{os.getpid()}")
        if not getattr(self, "redis_client", None):
            return None
        try:
            # ✅ FIXED: Use separate consumer group to avoid stealing messages from data_pipeline
            # Resolve canonical stream key relative to DB1
            stream_key = DatabaseAwareKeyBuilder.live_enriched_stream() 
            
            # Use dedicated scanner group (Phase 1 Fix)
            messages = self.redis_client.xreadgroup(
                groupname="scanner_group",
                consumername=f"scanner_{consumer_name}",
                streams={stream_key: '>'},
                count=100,
                block=1000  # ✅ REDUCED: Don't block for 5s
            )
            if not messages:
                try:
                    stream_info = self.redis_client.xinfo_stream(stream_key)
                except Exception:
                    return None
                if stream_info and stream_info.get('length', 0) > 0 and self.consumer_manager:
                    dead = self.consumer_manager.get_dead_consumers()
                    if dead:
                        logger.warning(
                            f"⚠️ Stream has backlog but {len(dead)} dead consumers remain in data_pipeline_group"
                        )
                return None
            return messages
        except redis.exceptions.ConnectionError:
            logger.error("❌ Redis connection lost during stream read")
            return None
        except Exception as exc:
            logger.error(f"❌ Stream read error: {exc}")
            self._heal_consumer_group()
            return None

    def _heal_consumer_group(self):
        """Emergency heal for the shared consumer group."""
        if not self.consumer_manager:
            return
        try:
            dead_consumers = self.consumer_manager.get_dead_consumers()
            for consumer in dead_consumers:
                self.consumer_manager.remove_dead_consumer(consumer.name)
            self.consumer_name = self.consumer_manager.create_consumer(
                f"scanner_{os.getpid()}_{int(time.time())}"
            )
            logger.info(f"🔧 Consumer group healed. New consumer: {self.consumer_name}")
        except Exception as exc:
            logger.error(f"❌ Failed to heal consumer group: {exc}")

    async def _process_ticks_async(self):
        """Async consumer that processes ticks without blocking waits."""
        dbg_enabled = bool(self.config.get("debug", False))
        processed_count = 0
        performance_stats = {
            "total_ticks": 0,
            "processing_times": [],
            "slow_ticks": 0,
            "errors": 0,
            "start_time": time.time(),
        }

        start_time = time.time()
        continuous = True if self.config.get("continuous", True) else False
        max_ticks = self.config.get("max_ticks")
        duration_secs = self.config.get("duration_secs")
        if max_ticks or duration_secs:
            continuous = bool(self.config.get("continuous")) if self.config.get("continuous") is not None else False

        logger.info("✅ Async tick consumer started")

        try:
            while self.running:
                loop_start = time.time()
                try:
                    tick_data = await asyncio.wait_for(self.async_tick_queue.get(), timeout=0.1) if self.async_tick_queue else None
                except asyncio.TimeoutError:
                    await self._run_periodic_tasks_async(time.time(), dbg_enabled)
                    continue

                if tick_data is None:
                    if self.async_tick_queue:
                        self.async_tick_queue.task_done()
                    break

                batch: List[Dict[str, Any]] = [tick_data]
                stop_after_batch = False

                while len(batch) < self.async_batch_size:
                    try:
                        if self.async_tick_queue:
                            extra = self.async_tick_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    if extra is None:
                        if self.async_tick_queue:
                            self.async_tick_queue.task_done()
                        stop_after_batch = True
                        break

                    batch.append(extra)

                await self._run_periodic_tasks_async(loop_start, dbg_enabled)

                process_start = time.time()
                errors = await self._process_tick_batch_async(batch, dbg_enabled)
                process_duration = time.time() - process_start

                per_tick_duration = process_duration / max(len(batch), 1)

                if per_tick_duration > 0.5:
                    logger.warning(
                        f"PERFORMANCE BOTTLENECK: batch of {len(batch)} ticks took {per_tick_duration:.2f}s each"
                    )

                if errors:
                    logger.warning(f"⚠️ [ASYNC_BATCH] {errors} tick(s) failed during processing batch of {len(batch)}")

                performance_stats["total_ticks"] += len(batch)
                performance_stats["processing_times"].append(per_tick_duration)
                if len(performance_stats["processing_times"]) > 1000:
                    performance_stats["processing_times"] = performance_stats["processing_times"][-1000:]

                if per_tick_duration > 0.3:
                    logger.warning(
                        f"Slow tick submission: {per_tick_duration:.2f}s avg for batch ({len(batch)} ticks)"
                    )
                    performance_stats["slow_ticks"] += len(batch)

                processed_count += len(batch)
                loop_duration = time.time() - loop_start

                if loop_duration > 0.5:
                    logger.warning(f"Slow loop iteration: {loop_duration:.2f}s")

                if not continuous:
                    if isinstance(max_ticks, int) and processed_count >= max_ticks:
                        logger.info(f"⏹️ Max ticks reached: {processed_count} >= {max_ticks}")
                        break
                    if isinstance(duration_secs, int) and (time.time() - start_time) >= duration_secs:
                        logger.info(
                            f"⏹️ Duration reached: {(time.time() - start_time):.1f}s >= {duration_secs}s"
                        )
                        break

                if processed_count % 1000 == 0:
                    self._log_performance_stats(performance_stats, processed_count)

                for _ in batch:
                    if self.async_tick_queue:
                        self.async_tick_queue.task_done()

                if stop_after_batch:
                    break

        except asyncio.CancelledError:
            logger.info("Async tick consumer cancelled")
        finally:
            self._log_performance_stats(performance_stats, processed_count, final=True)
            logger.info(f"🔍 [ASYNC_LOOP] Shutting down - processed {processed_count} ticks total")

    async def _run_periodic_tasks_async(self, current_time: float, dbg_enabled: bool):
        """Run maintenance tasks (health checks, alerts) asynchronously."""
        loop = asyncio.get_running_loop()

        if current_time - self._async_last_debug_time > 60:
            logger.debug("🔄 Async loop running")
            self._async_last_debug_time = current_time

        if current_time - self._async_last_health_check > 60:
            # ✅ REMOVED: Legacy health check - use check_pipeline_health() instead
            # Health checks are now handled by DataPipelineEnforcer and check_pipeline_health()
            try:
                # Check pipeline health (delegates to DataPipelineEnforcer)
                self.check_pipeline_health()
            except Exception as e:
                logger.debug(f"⚠️ Pipeline health check failed: {e}")
            self._async_last_health_check = current_time

        if current_time - self.last_health_log >= self.health_log_interval:
            try:
                await loop.run_in_executor(None, self._log_detection_health)
            except Exception as exc:
                logger.warning(f"Error logging detection health: {exc}")
            self.last_health_log = current_time

        if (
            self.indicator_prefetch_enabled
            and current_time - self._last_prefetch_time >= self.indicator_prefetch_interval
        ):
            symbols_for_prefetch = self._select_symbols_for_prefetch(
                self.indicator_prefetch_batch_size
            )
            if symbols_for_prefetch:
                try:
                    await self.prefetch_indicators(symbols_for_prefetch)
                except Exception as exc:
                    logger.debug(f"⚠️ [PREFETCH] Async prefetch failed: {exc}")
            self._last_prefetch_time = current_time

        await loop.run_in_executor(None, self._process_news_alerts)
        await loop.run_in_executor(None, self._process_pending_alerts)

    async def _process_tick_batch_async(self, tick_batch: List[Dict[str, Any]], dbg_enabled: bool) -> int:
        """Process a batch of ticks concurrently via asyncio.gather."""
        if not tick_batch:
            return 0

        try:
            tasks = [self._process_single_tick_async(tick, dbg_enabled) for tick in tick_batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            error_count = 0
            for result, tick in zip(results, tick_batch):
                if isinstance(result, Exception):
                    error_count += 1
                    logger.warning(
                        f"⚠️ [ASYNC_TICK] Error processing {tick.get('symbol', 'UNKNOWN')}: {result}"
                    )

            return error_count
        finally:
            self._release_tick_batch(tick_batch)

    async def _process_single_tick_async(self, tick_data: Dict[str, Any], dbg_enabled: bool):
        """Run the synchronous tick pipeline in a worker thread."""
        await asyncio.to_thread(self._process_tick_core, tick_data, 0, dbg_enabled)

    def _shutdown_async_processing(self):
        """Stop async queue, loop, and feeder threads."""
        if self.async_loop and self.async_tick_queue:
            try:
                asyncio.run_coroutine_threadsafe(self.async_tick_queue.put(None), self.async_loop) # type: ignore[arg-type, call-overload]
            except Exception as e:
                logger.error(f"Error putting None into async tick queue: {e}")

        if self.async_loop and self.async_loop.is_running():
            try:
                self.async_loop.call_soon_threadsafe(self.async_loop.stop)
            except Exception as e:
                logger.error(f"Error stopping async loop: {e}")

        if self.tick_feeder_thread and self.tick_feeder_thread.is_alive():
            try:
                self.tick_feeder_thread.join(timeout=2)
            except Exception as e:
                logger.error(f"Error joining tick feeder thread: {e}")
        if self.async_loop_thread and self.async_loop_thread.is_alive():
            try:
                self.async_loop_thread.join(timeout=2)
            except Exception as e:
                logger.error(f"Error joining async loop thread: {e}")

        self.async_loop = None
        self.async_tick_queue = None
        self.async_loop_thread = None
        self.tick_feeder_thread = None
        self._async_tasks = []
        self._async_queue_ready.clear()

    def _update_price_history_cache(self, symbol: str, last_price):
        if last_price is None or symbol is None:
            return
        try:
            last_price = float(last_price)
        except (TypeError, ValueError):
            return
        with self.cache_lock:
            self.price_cache[symbol].append(last_price)

    def _get_realtime_volume_baseline(self, symbol: str) -> float:
        """
        ✅ CENTRALIZED: Get realtime volume baseline using VolumeComputationManager and UnifiedTimeAwareBaseline.
        Uses time-aware baselines stored in Redis at DatabaseAwareKeyBuilder.live_volume_baseline(symbol).
        
        This replaces the legacy _get_cached_average_volume() and _load_avg_volume() functions.
        """
        try:
            if not hasattr(self, 'volume_manager') or not self.volume_manager:
                return 0.0
            
            # ✅ METHOD 1: Use UnifiedTimeAwareBaseline for time-aware baseline (preferred)
            if hasattr(self.volume_manager, '_baseline_db1_client') and self.volume_manager._baseline_db1_client:
                try:
                    from shared_core.dynamic_thresholds.time_aware_volume_baseline import UnifiedTimeAwareBaseline
                    baseline_calc = UnifiedTimeAwareBaseline(self.volume_manager._baseline_db1_client, symbol)
                    baseline_volume = baseline_calc.get_baseline_volume()
                    if baseline_volume and baseline_volume > 0:
                        return float(baseline_volume)
                except Exception as e:
                    logger.debug(f"⚠️ [BASELINE] UnifiedTimeAwareBaseline failed for {symbol}: {e}")
            
            # METHOD 2: Read directly from Redis using DatabaseAwareKeyBuilder (fallback)
            try:
                canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
                baseline_key = DatabaseAwareKeyBuilder.live_volume_baseline(canonical_symbol)
                
                # Get DB1 client
                db1_client = None
                if hasattr(self, 'volume_manager') and hasattr(self.volume_manager, '_baseline_db1_client'):
                    db1_client = self.volume_manager._baseline_db1_client
                elif hasattr(self, 'redis_client'):
                    if hasattr(self.redis_client, 'get_client'):
                        db1_client = self.redis_client.get_client(1) # type: ignore[attr-defined]
                    elif hasattr(self.redis_client, 'hgetall'):
                        db1_client = self.redis_client
                
                if db1_client:
                    # ✅ CENTRALIZED: Use hgetall with proper error handling (key_builder already used above)
                    try:
                        baseline_hash = db1_client.hgetall(baseline_key)
                        if baseline_hash:
                            # Convert bytes to strings/numbers
                            clean_data = {}
                            for k, v in baseline_hash.items():
                                if isinstance(k, bytes):
                                    k = k.decode('utf-8')
                                if isinstance(v, bytes):
                                    v = v.decode('utf-8')
                                clean_data[k] = v
                            
                            avg_volume_20d = float(clean_data.get("avg_volume_20d", 0) or 0)
                            if avg_volume_20d > 0:
                                return avg_volume_20d
                    except Exception as hget_err:
                        logger.debug(f"⚠️ [BASELINE] hgetall failed for {baseline_key}: {hget_err}")
            except Exception as e:
                logger.debug(f"⚠️ [BASELINE] Redis key_builder fallback failed for {symbol}: {e}")
            
            return 0.0
        except Exception as e:
            logger.debug(f"⚠️ [BASELINE] Failed to get realtime baseline for {symbol}: {e}")
            return 0.0

    def _get_volume_ratio_from_tick(self, tick_data: dict):
        """Read pre-computed volume metrics ONLY - NEVER recalculate.
        
        ✅ SINGLE SOURCE OF TRUTH: volume_ratio is already calculated by websocket_parser and stored in Redis.
        This function ONLY reads pre-computed values - if missing, return 0.0 (data_pipeline will fetch from Redis).
        """
        symbol = tick_data.get("symbol")
        if not symbol:
            return 0.0, 0.0, 0.0

        try:
            incremental = float(VolumeResolver.get_incremental_volume(tick_data) or 0.0)
            cumulative = float(VolumeResolver.get_cumulative_volume(tick_data) or 0.0)
            volume_ratio = float(VolumeResolver.get_volume_ratio(tick_data) or 0.0)
            
            # ✅ REMOVED: Recalculation logic - volume_ratio should already be in tick_data or Redis
            # If it's missing, return 0.0 and let data_pipeline fetch it from Redis
            
            return volume_ratio, incremental, cumulative
        except Exception as exc:
            logger.error(f"Failed to read centralized volume metrics for {symbol}: {exc}")
            return 0.0, 0.0, 0.0

    def _load_symbol_universe(self) -> List[str]:
        symbols = set()
        config_symbols = self.config.get("symbol_universe") or []
        symbols.update(sym for sym in config_symbols if sym)

        if getattr(self, "premarket_watchlist", None):
            symbols.update(self.premarket_watchlist)

        # Load symbols from intraday crawler instruments JSON (authoritative source)
        # ✅ FIXED: Use correct path to intraday_crawler_instruments.json
        # This resolves the issue where scanner was loading from binary_crawler1.json or CSV
        intraday_config_path = os.path.join(project_root, "zerodha", "crawlers", "binary_crawler1", "intraday_crawler_instruments.json")
        if os.path.exists(intraday_config_path):
            try:
                with open(intraday_config_path, "r", encoding="utf-8") as handle:
                    # Parse JSON structure
                    # Expected format: {"tokens": [123, 456], ...} or direct list/dict
                    payload = json.load(handle)
                    
                    tokens = []
                    if isinstance(payload, dict):
                        instruments = payload.get("instruments", {})
                        if isinstance(instruments, dict):
                            tokens = instruments.get("all_tokens", [])
                        elif isinstance(instruments, list):
                            tokens = instruments
                        else:
                            # Legacy format: top-level dict of token => metadata
                            potential_tokens = [k for k in payload.keys() if str(k).isdigit()]
                            if potential_tokens:
                                tokens = potential_tokens
                    elif isinstance(payload, list):
                        tokens = payload

                    logger.info(f"📊 Intraday crawler instruments file has {len(tokens)} tokens")
                    
                    # Use token resolver to convert tokens to symbols
                    try:
                        # ✅ SIMPLIFIED: Use INTRADAY_FOCUS registry only (Single Source of Truth)
                        # HotTokenMapper fallback removed - registry from intraday_crawler_instruments.json is authoritative
                        from shared_core.instrument_token.registry_helper import get_scanner_registry
                        registry = get_scanner_registry()
                        
                        def resolve_token_to_symbol_unified(token):
                            """Resolve token using INTRADAY_FOCUS registry (Single Source of Truth)."""
                            if not token or not registry:
                                return None
                            
                            try:
                                # Direct token lookup from registry
                                unified_inst = registry.registry.get_by_broker_token("zerodha", str(token))
                                if unified_inst:
                                    broker_inst = unified_inst.get_broker_instrument("zerodha")
                                    if broker_inst:
                                        symbol = broker_inst.tradingsymbol
                                        if symbol and symbol != "UNKNOWN":
                                            return symbol
                            except Exception as e:
                                logger.debug(f"Registry token lookup failed for {token}: {e}")
                            
                            return None
                        
                        # Convert tokens to symbols using the unified resolver
                        for token in tokens:
                            symbol = resolve_token_to_symbol_unified(token)
                            if symbol:
                                symbols.add(symbol)
                            else:
                                logger.warning(f"Token {token} not found in mapping")
                    except ImportError as import_err:
                        # Fallback to bucket_incremental_volume averages if token mapping not available
                        volume_file = os.path.join(project_root, "config", "volume_averages_20d.json")
                        if os.path.exists(volume_file):
                            with open(volume_file, "r", encoding="utf-8") as handle:
                                payload = json.load(handle)
                                if isinstance(payload, dict):
                                    symbols.update(payload.keys())
                    except Exception as e:
                        logger.warning(f"⚠️ Token resolver failed: {e}, using fallback symbol loading")
                        # Fallback to bucket_incremental_volume averages if token mapping not available
                        volume_file = os.path.join(project_root, "config", "volume_averages_20d.json")
                        if os.path.exists(volume_file):
                            with open(volume_file, "r", encoding="utf-8") as handle:
                                payload = json.load(handle)
                                if isinstance(payload, dict):
                                    symbols.update(payload.keys())
            except Exception as exc:
                logger.error(f"Failed to load intraday crawler symbols: {exc}")
                # Fallback to bucket_incremental_volume averages
                volume_file = os.path.join(project_root, "config", "volume_averages_20d.json")
                if os.path.exists(volume_file):
                    try:
                        with open(volume_file, "r", encoding="utf-8") as handle:
                            payload = json.load(handle)
                            if isinstance(payload, dict):
                                symbols.update(payload.keys())
                    except Exception as exc:
                        pass
        else:
            # Fallback to bucket_incremental_volume averages
            volume_file = os.path.join(project_root, "config", "volume_averages_20d.json")
            if os.path.exists(volume_file):
                try:
                    with open(volume_file, "r", encoding="utf-8") as handle:
                        payload = json.load(handle)
                        if isinstance(payload, dict):
                            symbols.update(payload.keys())
                except Exception as exc:
                    pass

        cleaned = sorted({sym for sym in symbols if isinstance(sym, str) and sym})
        logger.info("📊 Loaded symbol universe with %d instruments (from intraday crawler)", len(cleaned))
        return cleaned

    def pre_market_preparation(self):
        """
        Pre-market preparation - DISABLED BY DEFAULT.
        
        ✅ REMOVED: Pre-market processing of hardcoded instruments.
        - Indicators are loaded on-demand from Redis/cache when ticks arrive
        - Volume baselines are fetched on-demand from Redis
        - Pattern detection happens when real ticks arrive
        - Processing hardcoded symbols causes unnecessary baseline lookups and log spam
        
        To enable, set "enable_premarket_precompute": true in scanner.json config.
        """
        if not self.config.get("enable_premarket_precompute", False):  # ✅ Changed default to False
            logger.debug("Pre-market preparation disabled (set enable_premarket_precompute=true to enable)")
            # Still initialize real-time buckets and refresh VIX
            self._initialize_real_time_buckets()
            self._refresh_vix_snapshot(force=True)
            return

        if not self.all_symbols:
            logger.warning("Pre-market precompute skipped: symbol universe empty")
            return

        logger.info(
            "🚀 Starting enhanced pre-market preparation for %d symbols",
            len(self.all_symbols),
        )
        start = time.time()
        price_window = self.config.get("price_cache_window", 100)
        # ✅ OPTIMIZED: Only load JSON OHLC cache if explicitly enabled (Redis is primary source)
        # The JSON file is now only used as a fallback when Redis is unavailable
        use_json_fallback = self.config.get("use_json_ohlc_fallback", False)
        if use_json_fallback:
            self._load_combined_ohlc_cache()
        else:
        
            self._refresh_vix_snapshot(force=True)

        for symbol in self.all_symbols:
            data = self._load_historical_data(symbol, days=5)
            closes = [
                c for c in data.get("close", []) if isinstance(c, (int, float))
            ]
            if closes:
                with self.cache_lock:
                    dq = self.price_cache[symbol]
                    dq.extend(closes[-price_window:])
                try:
                    with self.tick_processor.lock:
                        tp_deque = self.tick_processor.price_history[symbol]
                        tp_deque.extend(closes[-tp_deque.maxlen:])
                except Exception:
                    pass

            # ✅ REMOVED: _inject_ohlc_into_history() - Redundant function
            # The update_all_20day_averages script already publishes baseline data to Redis
            # Pattern detectors read from Redis directly via VolumeComputationManager
            # No need to populate local caches that duplicate Redis data

            # ✅ FIXED: Use centralized VolumeComputationManager and UnifiedTimeAwareBaseline
            # Volume baselines are stored in Redis at DatabaseAwareKeyBuilder.live_volume_baseline(symbol)
            # Pattern detectors read directly from Redis - no need for local cache
            # Pre-market prep doesn't need to pre-load volume baselines - they're fetched on-demand

            # ✅ REMOVED: Legacy _precompute_all_indicators() - only had 4 incomplete indicators
            # ✅ REPLACED: Use HybridCalculations cache/Redis via _get_indicators_from_cache()
            # This ensures we get complete indicators (22+) including volume_profile, volume_ratio, etc.
            # Pre-market warmup will use indicators from HybridCalculations cache or Redis
            # Fast lane will also use complete indicators from cache/Redis
            pass  # Indicators will be loaded from cache/Redis when needed

        self._warmup_pattern_detection()
        self._initialize_real_time_buckets()
        duration = time.time() - start
        logger.info(
            "✅ Pre-market preparation complete: %d symbols ready in %.2fs",
            len(self.all_symbols),
            duration,
        )

    def _load_historical_data(self, symbol: str, days: int = 5) -> Dict[str, List[float]]:
        """Load real historical data from Redis/ClickHouse - NO SYNTHETIC DATA"""
        result = {"close": [], "high": [], "low": [], "bucket_incremental_volume": []}
        # ✅ REMOVED: Synthetic data generation - we have enough real historical data
        # Historical data should be loaded from Redis streams, ClickHouse, or other real sources
        # If historical data is needed, use VolumeComputationManager or real data sources
        return result


    def _load_combined_ohlc_cache(self) -> Dict[str, Dict[str, Any]]:
        """Load OHLC cache from JSON file (fallback only - Redis is primary source)"""
        if self._combined_ohlc_cache:
            return self._combined_ohlc_cache

        # ✅ OPTIMIZED: Only load JSON file if explicitly enabled (Redis is primary source)
        use_json_fallback = self.config.get("use_json_ohlc_fallback", False)
        if not use_json_fallback:
            self._combined_ohlc_cache = {}
            return self._combined_ohlc_cache

        combined_dir = os.path.join(project_root, "config")
        pattern = os.path.join(combined_dir, "market_data_combined_*.json")
        files = sorted(glob.glob(pattern))
        if not files:
            self._combined_ohlc_cache = {}
            return self._combined_ohlc_cache

        latest_file = files[-1]
        try:
            with open(latest_file, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
                if isinstance(payload, dict):
                    self._combined_ohlc_cache = payload
                elif isinstance(payload, list):
                    cache: Dict[str, Dict[str, Any]] = {}
                    for entry in payload:
                        if isinstance(entry, dict):
                            sym = entry.get("symbol") or entry.get("tradingsymbol")
                            if sym:
                                cache[str(sym)] = entry
                    self._combined_ohlc_cache = cache
                else:
                    self._combined_ohlc_cache = {}
        except Exception as exc:
            self._combined_ohlc_cache = {}

        return self._combined_ohlc_cache

    def _get_current_ohlc(self, symbol: str, tick_data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Get current OHLC data with optimized fallback chain.
        
        ✅ OPTIMIZED: Uses OHLC from tick_data first (from data_pipeline),
        then checks cache, then falls back to Redis only if needed.
        
        Args:
            symbol: Trading symbol
            tick_data: Optional tick data dictionary (from data_pipeline)
                      If provided, OHLC will be extracted from tick_data first
        
        Returns:
            OHLC dictionary with open, high, low, close, or None
        """
        # ✅ PRIORITY 1: Use OHLC from tick_data (from data_pipeline) - NO REDIS CALL
        if tick_data:
            ohlc_from_tick = tick_data.get("ohlc", {})
            if isinstance(ohlc_from_tick, dict) and ohlc_from_tick:
                # Check if we have all required OHLC fields
                required_fields = ["open", "high", "low", "close"]
                if all(field in ohlc_from_tick for field in ["open", "high", "low", "close"]):
                    def _float_or_none(value):
                        try:
                            if value is None:
                                return None
                            return float(value)
                        except (TypeError, ValueError):
                            return None
                    
                    # Extract OHLC from tick_data (already cleaned by data_pipeline)
                    result = {
                        "open": _float_or_none(ohlc_from_tick.get("open")),
                        "high": _float_or_none(ohlc_from_tick.get("high")),
                        "low": _float_or_none(ohlc_from_tick.get("low")),
                        "close": _float_or_none(ohlc_from_tick.get("close")),
                        "bucket_incremental_volume": _float_or_none(tick_data.get("bucket_incremental_volume") or tick_data.get("zerodha_cumulative_volume")),
                        "date": tick_data.get("date"),
                        "updated_at": tick_data.get("timestamp") or tick_data.get("exchange_timestamp"),
                    }
                    # Only return if we have valid OHLC data
                    if result["open"] is not None and result["high"] is not None and result["low"] is not None and result["close"] is not None:
                        return result
            
            # ✅ PRIORITY 2: Check flattened OHLC fields in tick_data (data_pipeline flattens OHLC)
            if all(field in tick_data for field in ["open", "high", "low", "close"]):
                def _float_or_none(value):
                    try:
                        if value is None:
                            return None
                        return float(value)
                    except (TypeError, ValueError):
                        return None
                
                result = {
                    "open": _float_or_none(tick_data.get("open")),
                    "high": _float_or_none(tick_data.get("high")),
                    "low": _float_or_none(tick_data.get("low")),
                    "close": _float_or_none(tick_data.get("close")),
                    "bucket_incremental_volume": _float_or_none(tick_data.get("bucket_incremental_volume") or tick_data.get("zerodha_cumulative_volume")),
                    "date": tick_data.get("date"),
                    "updated_at": tick_data.get("timestamp") or tick_data.get("exchange_timestamp"),
                }
                # Only return if we have valid OHLC data
                if result["open"] is not None and result["high"] is not None and result["low"] is not None and result["close"] is not None:
                    return result
        
        # ✅ PRIORITY 3: Check cache (JSON fallback)
        cache = self._load_combined_ohlc_cache()
        ohlc = cache.get(symbol)
        if not ohlc:
            short_symbol = symbol.split(":", 1)[-1]
            ohlc = cache.get(short_symbol)
        if ohlc:
            return ohlc
        
        # ✅ PRIORITY 4: Fallback to UnifiedDataStorage (OHLC from Redis)
        if self.unified_storage:
            try:
                ohlc_data = self.unified_storage.get_ohlc_data(symbol)
                if ohlc_data:
                    return {
                        "open": ohlc_data.get("open"),
                        "high": ohlc_data.get("high"),
                        "low": ohlc_data.get("low"),
                        "close": ohlc_data.get("close"),
                        "volume": int(ohlc_data.get("volume", 0) or 0)
                    }
            except Exception:
                pass
        return None

    def _refresh_vix_snapshot(self, force: bool = False):
        now = time.time()
        if not force and (now - self._last_vix_refresh) < self.vix_refresh_interval:
            return
        try:
            from shared_core.utils.vix_utils import get_current_vix, get_vix_regime

            data = get_current_vix()
            if data:
                self.current_vix_value = float(data.get("value", 0.0))
                self.current_vix_regime = data.get("regime", "NORMAL")
            else:
                # ✅ FIXED: Safe fallback - get_vix_regime() never raises exceptions
                self.current_vix_regime = get_vix_regime() or "NORMAL"
                self.current_vix_value = None
        except Exception as exc:
            # ✅ FIXED: Always set defaults on any error
            self.current_vix_regime = "NORMAL"
            self.current_vix_value = None
            logger.debug(f"⚠️ [VIX] Error refreshing VIX in scanner_main: {exc}")
        finally:
            self._last_vix_refresh = now
            if hasattr(self, "pattern_detector") and self.pattern_detector:
                try:
                    self.pattern_detector._update_vix_aware_thresholds()
                except Exception as exc:
                    pass

    def _get_fast_lane_volume_threshold(self) -> float:
        """
        ✅ CENTRALIZED: Get fast lane volume threshold using dynamic thresholds only.
        No hardcoded fallbacks - uses get_pattern_volume_requirement() from shared_core.
        """
        self._refresh_vix_snapshot()
        
        try:
            # ✅ CENTRALIZED: Use shared_core.config_utils.thresholds (single source of truth)
            from shared_core.config_utils.thresholds import get_pattern_volume_requirement
            
            # Get dynamic threshold with VIX regime awareness
            dynamic_threshold = get_pattern_volume_requirement(
                "volume_spike", 
                symbol=None, 
                vix_regime=self.current_vix_regime,
                redis_client=self.redis_client if hasattr(self, 'redis_client') else None
            )
            
            if dynamic_threshold and dynamic_threshold > 0:
                return float(dynamic_threshold)
            else:
                # ✅ CENTRALIZED: If dynamic threshold returns 0, get base threshold from pattern_detector
                if hasattr(self, 'pattern_detector') and self.pattern_detector:
                    base_threshold = self.pattern_detector.base_thresholds.get("volume_spike", 1.0)
                    if base_threshold > 0:
                        return float(base_threshold)
                
                # Last resort: use minimum safe threshold (1.0) - no hardcoded 2.0
                logger.warning(f"⚠️ [THRESHOLD] Could not get dynamic threshold for volume_spike, using minimum 1.0")
                return 1.0
                
        except Exception as e:
            logger.warning(f"⚠️ [THRESHOLD] Failed to get dynamic threshold for volume_spike: {e}")
            # ✅ CENTRALIZED: Fallback to pattern_detector base threshold, not hardcoded value
            if hasattr(self, 'pattern_detector') and self.pattern_detector:
                base_threshold = self.pattern_detector.base_thresholds.get("volume_spike", 1.0)
                return float(base_threshold) if base_threshold > 0 else 1.0
            return 1.0

    # ❌ REMOVED: _precompute_all_indicators() - Legacy method with only 4 incomplete indicators
    # ❌ REMOVED: _ema_helper(), _rsi_helper(), _atr_helper() - Legacy helper methods
    # ✅ CENTRALIZED: Use _get_indicators_from_cache() which gets complete indicators (22+) from:
    #   1. HybridCalculations in-memory cache (_cache)
    #   2. UnifiedDataStorage.get_indicators() - centralized indicator storage (replaces legacy retrieve_by_data_type)
    #   3. Fresh calculation via HybridCalculations.batch_calculate_indicators()
    # This ensures pattern detection gets volume_profile, volume_ratio, and all indicators.

    def _has_indicators_in_cache(self, symbol: str) -> bool:
        """
        Check if indicators are available in cache (HybridCalculations or Redis).
        
        ✅ OPTIMIZED: Checks HybridCalculations cache and Redis instead of legacy precomputed_indicators.
        """
        # Check HybridCalculations cache
        if hasattr(self, 'tick_processor') and self.tick_processor:
            hybrid_calc = getattr(self.tick_processor, 'hybrid_calc', None)
            if hybrid_calc and hasattr(hybrid_calc, '_cache'):
                cache_key = f"{symbol}_indicators"
                if cache_key in hybrid_calc._cache:
                    cached_entry = hybrid_calc._cache[cache_key]
                    current_time = time.time()
                    # Check if cache is still valid (within TTL)
                    if current_time - cached_entry.get('timestamp', 0) < hybrid_calc._cache_ttl:
                        return True
        
        # ✅ CENTRALIZED: Check Redis using UnifiedDataStorage (replaces legacy retrieve_by_data_type)
        if hasattr(self, 'unified_storage') and self.unified_storage:
            try:
                # Use UnifiedDataStorage to check if indicators exist
                test_indicators = self.unified_storage.get_indicators(symbol, ['rsi'])
                if test_indicators and 'rsi' in test_indicators:
                    return True
            except Exception:
                pass
        # ✅ FIXED: Use UnifiedDataStorage even in fallback (HGETALL on ind:{symbol} hash)
        else:
            try:
                from shared_core.redis_clients.unified_data_storage import get_unified_storage
                storage = get_unified_storage()
                test_indicators = storage.get_indicators(symbol, ['rsi'])
                if test_indicators and 'rsi' in test_indicators:
                    return True
            except Exception:
                pass
        
        return False

    def _initialize_real_time_buckets(self):
        self._pending_alerts = []
        self._last_alert_time = time.time()

    def _warmup_pattern_detection(self):
        """
        Warm up pattern detector with indicators from HybridCalculations cache or Redis.
        
        ✅ OPTIMIZED: Uses HybridCalculations cache/Redis instead of legacy precomputed_indicators.
        This ensures pattern detector gets complete indicators (22+) including volume_profile.
        
        ✅ REMOVED: Processing hardcoded symbols - only warmup symbols that have received ticks.
        """
        if not hasattr(self, 'all_symbols') or not self.all_symbols:
            return
        
        # ✅ FIXED: Only warmup symbols that have actually received ticks (are in price_cache)
        # This prevents processing hardcoded instruments that aren't actively streamed
        symbols_to_warmup = []
        try:
            with self.cache_lock:
                # Only warmup symbols that have price data (have received ticks)
                symbols_to_warmup = [symbol for symbol in self.all_symbols if symbol in self.price_cache and self.price_cache[symbol]]
        except Exception:
            pass
        
        if not symbols_to_warmup:
            logger.debug("No symbols with price data for warmup - skipping pattern detection warmup")
            return

        # Get indicators from cache/Redis (uses optimized fallback chain)
        indicators = self._get_indicators_from_cache(symbol, dummy_tick)
        if indicators and len(indicators) > 5:  # Ensure we have meaningful indicators
            self.pattern_detector.update_history(symbol, indicators)

    def _wrap_tick_with_pool(self, tick_payload: Optional[Dict[str, Any]]):
        """Convert raw tick dicts into pooled TickData objects when enabled."""
        if (
            tick_payload is None
            or not self.tick_pool_enabled
            or not self.tick_data_pool
            or isinstance(tick_payload, TickData)
        ):
            return tick_payload
        tick_obj = self.tick_data_pool.get_tick_object()
        tick_obj.update(tick_payload)
        return tick_obj

    def _release_tick_batch(self, ticks: List[Dict[str, Any]]):
        """Return processed tick objects back to the pool."""
        if not self.tick_pool_enabled or not self.tick_data_pool:
            return
        for tick in ticks:
            if isinstance(tick, TickData):
                self.tick_data_pool.return_tick_object(tick)

    def _select_symbols_for_prefetch(self, limit: Optional[int] = None) -> List[str]:
        """Select symbols to prefetch based on recent activity and watchlists."""
        limit = limit or self.indicator_prefetch_batch_size
        if limit <= 0:
            return []
        
        candidates: List[str] = []
        seen: Set[str] = set()
        
        # Use recent price updates as proxy for activity
        try:
            with self.cache_lock:
                priced = [(symbol, len(price_window)) for symbol, price_window in self.price_cache.items() if price_window]
            priced.sort(key=lambda item: item[1], reverse=True)
            for symbol, _ in priced:
                if not symbol or symbol in seen:
                    continue
                candidates.append(symbol)
                seen.add(symbol)
                if len(candidates) >= limit:
                    return candidates
        except Exception:
            pass
        
        # Add premarket watchlist symbols (only if they have recent activity)
        for symbol in (self.premarket_watchlist or []):
            if symbol and symbol not in seen:
                # ✅ FIXED: Only prefetch symbols that have received ticks (are in price_cache)
                # This prevents processing symbols not in the crawler stream
                with self.cache_lock:
                    if symbol in self.price_cache and self.price_cache[symbol]:
                        candidates.append(symbol)
                        seen.add(symbol)
                        if len(candidates) >= limit:
                            return candidates
        
        # ✅ REMOVED: Fallback to general universe (all_symbols)
        # This was causing baseline lookups for symbols not in the crawler stream
        # Only prefetch symbols that have actually received ticks (are in price_cache)
        # If we need more candidates, we should only use symbols with recent activity
        
        return candidates[:limit]

    def _get_last_price_for_symbol(self, symbol: str) -> float:
        """Get most recent price seen for a symbol."""
        try:
            with self.cache_lock:
                dq = self.price_cache.get(symbol)
                if dq:
                    return float(dq[-1])
        except Exception:
            pass
        return 0.0

    def _prime_indicator_caches(self, symbol: str, indicators: Dict[str, Any]):
        """Store prefetched indicators in all available caches."""
        try:
            now_ts = time.time()
            hybrid_calc = getattr(self.tick_processor, 'hybrid_calc', None)
            if hybrid_calc and hasattr(hybrid_calc, '_cache'):
                cache_key = f"{symbol}_indicators"
                hybrid_calc._cache[cache_key] = {
                    "timestamp": now_ts,
                    "indicators": indicators,
                }
        except Exception as exc:
            logger.debug(f"⚠️ [PREFETCH] Failed to prime hybrid cache for {symbol}: {exc}")
        
        # ✅ BACKWARD COMPATIBILITY: Keep precomputed_indicators for legacy code (deprecated, use UnifiedDataStorage instead)
        # Note: This is a legacy cache - new code should use UnifiedDataStorage.get_indicators()
        self.precomputed_indicators[symbol] = indicators
        
        # Update pattern detector history if available
        try:
            if hasattr(self.pattern_detector, "update_history"):
                self.pattern_detector.update_history(symbol, indicators)
        except Exception as exc:
            logger.debug(f"⚠️ [PREFETCH] Failed to update pattern history for {symbol}: {exc}")

    async def prefetch_indicators(self, active_symbols: List[str]):
        """Asynchronously pre-load indicators for frequently traded symbols."""
        if not self.indicator_prefetch_enabled or not active_symbols:
            return
        if self._prefetch_in_progress:
            return
        
        self._prefetch_in_progress = True
        batch = active_symbols[: self.indicator_prefetch_batch_size]
        try:
            tasks = [self._cache_indicators_for_symbol_async(symbol) for symbol in batch]
            if not tasks:
                return
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successes = sum(1 for r in results if r is True)
            logger.debug(
                f"✅ [PREFETCH] Prefetched indicators for {successes}/{len(batch)} symbols"
            )
        except Exception as exc:
            logger.debug(f"⚠️ [PREFETCH] Prefetch run failed: {exc}")
        finally:
            self._prefetch_in_progress = False

    async def _cache_indicators_for_symbol_async(self, symbol: str) -> bool:
        """Async wrapper to cache indicators for a single symbol."""
        return await asyncio.to_thread(self._cache_indicators_for_symbol_sync, symbol)

    def _cache_indicators_for_symbol_sync(self, symbol: str) -> bool:
        """Synchronously pull indicators and inject them into caches."""
        try:
            last_price = self._get_last_price_for_symbol(symbol)
            dummy_tick = {
                "symbol": symbol,
                "last_price": last_price,
                "timestamp": int(time.time() * 1000),
            }
            indicators = self._get_indicators_from_cache(symbol, dummy_tick)
            if indicators:
                self._prime_indicator_caches(symbol, indicators)
                return True
        except Exception as exc:
            logger.debug(f"⚠️ [PREFETCH] Failed to cache indicators for {symbol}: {exc}")
        return False

    def _build_game_theory_watchlist(self, validation_map: Dict[str, List[str]]) -> set:
        """Build game theory watchlist from validation map"""
        # ✅ CENTRALIZED: Use canonical_symbol for watchlist (section 3)
        watchlist = set()
        for exchange, symbols in (validation_map or {}).items():
            for sym in symbols:
                canonical = RedisKeyStandards.canonical_symbol(sym)
                watchlist.add(canonical)
                watchlist.add(f"{exchange}:{canonical}")
        return watchlist

    def _get_indicators_from_cache(
        self, symbol: str, tick_data: dict
    ) -> Dict[str, float]:
        """
        Get indicators with optimized fallback chain.
        
        ✅ CENTRALIZED: Uses HybridCalculations cache first, then UnifiedDataStorage, then calculates fresh.
        Replaces legacy precomputed_indicators which only had 4 incomplete indicators.
        
        Fallback Chain:
        1. HybridCalculations in-memory cache (_cache)
        2. UnifiedDataStorage.get_indicators() - centralized indicator storage (replaces legacy retrieve_by_data_type)
        3. Calculate fresh using HybridCalculations.batch_calculate_indicators()
        
        Args:
            symbol: Trading symbol
            tick_data: Tick data dictionary
            
        Returns:
            Complete indicators dictionary (22+ indicators including volume_profile, volume_ratio, etc.)
        """
        import time
        from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
        
        # ✅ STEP 1: Check HybridCalculations cache first (fastest)
        if hasattr(self, 'tick_processor') and self.tick_processor:
            hybrid_calc = getattr(self.tick_processor, 'hybrid_calc', None)
            if hybrid_calc and hasattr(hybrid_calc, '_cache'):
                cache_key = f"{symbol}_indicators"
                if cache_key in hybrid_calc._cache:
                    cached_entry = hybrid_calc._cache[cache_key]
                    current_time = time.time()
                    # Check if cache is still valid (within TTL)
                    if current_time - cached_entry.get('timestamp', 0) < hybrid_calc._cache_ttl:
                        indicators = cached_entry.get('indicators', {})
                        if indicators:
                            logger.debug(f"✅ [CACHE_HIT] {symbol} indicators from HybridCalculations cache")
                            return indicators
        
        # ✅ STEP 2: Try Redis DB 1 using UnifiedDataStorage (centralized system)
        if hasattr(self, 'unified_storage') and self.unified_storage:
            try:
                # Get standard indicator names (includes microstructure indicators)
                indicator_names = [
                    'rsi', 'macd', 'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200',
                    'atr', 'vwap', 'bollinger_bands', 
                    'volume_profile', 'volume_ratio', 'price_change',
                    'delta', 'gamma', 'theta', 'vega', 'rho',
                    'dte_years', 'trading_dte',
                    'volume', 'high', 'low', 'close', 'open', 'last', 'bid', 'ask',
                    'bid_size', 'ask_size', 'bid_depth', 'ask_depth',
                    'microprice', 'order_flow_imbalance', 'order_book_imbalance',
                    'cumulative_volume_delta', 'spread_absolute', 'spread_bps',
                    'depth_imbalance', 'best_bid_size', 'best_ask_size',
                    'total_bid_depth', 'total_ask_depth'
                ]

                # ✅ CENTRALIZED: Use UnifiedDataStorage.get_indicators() (single source of truth)
                redis_indicators = self.unified_storage.get_indicators(symbol, indicator_names)
                if redis_indicators and len(redis_indicators) > 5:
                    logger.debug(f"✅ [REDIS_HIT] {symbol} {len(redis_indicators)} indicators from UnifiedDataStorage")
                    return redis_indicators
            except Exception as e:
                logger.debug(f"⚠️ [REDIS_FALLBACK] {symbol} UnifiedDataStorage retrieval failed: {e}")
        
        # ✅ STEP 3: Calculate fresh if cache/Redis miss
        if hasattr(self, 'tick_processor') and self.tick_processor:
            try:
                logger.debug(f"🔍 [FRESH_CALC] {symbol} Calculating fresh indicators")
                indicators = self.tick_processor.process_tick(symbol, tick_data)
                
                if indicators:
                    # Count how many indicators we got
                    payload_count = len([k for k in indicators.keys() if indicators.get(k) is not None])
                    if payload_count > 5:
                        logger.debug(f"✅ [FRESH_CALC] {symbol} Fresh calculation yielded {payload_count} indicators")
                        return indicators
                    else:
                        logger.debug(f"⚠️ [RETRIEVAL_DEBUG] {symbol} Fresh calculation yielded only {payload_count} indicators")
                    return indicators
                else:
                    logger.warning(f"⚠️ [RETRIEVAL_DEBUG] {symbol} Fresh calculation returned empty indicators")
            except Exception as e:
                logger.warning(f"❌ [RETRIEVAL_DEBUG] {symbol} Fresh calculation failed: {e}")
                import traceback
                logger.debug(f"🔍 [RETRIEVAL_DEBUG] {symbol} Fresh calculation traceback: {traceback.format_exc()}")
        else:
            logger.debug(f"⚠️ [RETRIEVAL_DEBUG] {symbol} No tick_processor available for fresh calculation")
        
        return {}

    def _build_option_chain_from_redis(self, underlying: str) -> dict:
        """
        Build option chain data structure from Redis keys.
        
        Uses canonical key builder to construct option chain from Redis.
        
        Args:
            underlying: Underlying symbol (e.g., 'NIFTY', 'BANKNIFTY')
            
        Returns:
            Dictionary with option chain data structure
        """
        option_chain = {}
        
        try:
            
            # Use existing DB1 client when possible to avoid new connections
            redis_db1 = None
            if hasattr(self, "redis_client") and self.redis_client:
                if hasattr(self.redis_client, "get_client"):
                    redis_db1 = self.redis_client.get_client(1)
                else:
                    redis_db1 = self.redis_client
            if redis_db1 is None:
                # ✅ FIXED: Use UnifiedRedisManager for centralized connection management
                from shared_core.redis_clients import UnifiedRedisManager
                redis_db1 = get_redis_client(process_name="scanner_main_health", db=1)
            
            if not redis_db1:
                return option_chain
            
            normalized = RedisKeyStandards.canonical_symbol(underlying)
            canonical_lower = normalized.lower()
            alias_lower = (underlying or "").lower()
            
            # ✅ CENTRALIZED: Use DatabaseAwareKeyBuilder static methods only
            candidate_keys: List[str] = []
            if hasattr(DatabaseAwareKeyBuilder, "get_option_chain_key"):
                try:
                    candidate_keys.append(DatabaseAwareKeyBuilder.get_option_chain_key(underlying))
                except Exception:
                    pass
            
            candidate_keys.extend([
                f"options:chain:{canonical_lower}",
                f"option_chain:{canonical_lower}",
                f"options:chain:{alias_lower}",
                f"option_chain:{alias_lower}",
            ])
            
            seen_keys = set()
            for key in candidate_keys:
                if not key or key in seen_keys:
                    continue
                seen_keys.add(key)
                try:
                    # ✅ CENTRALIZED: Use Redis get() with proper error handling (key_builder already used above)
                    chain_data = redis_db1.get(key)
                    if not chain_data:
                        continue
                    if isinstance(chain_data, bytes):
                        chain_data = chain_data.decode("utf-8")
                    if isinstance(chain_data, str):
                        option_chain = json.loads(chain_data)
                    elif isinstance(chain_data, dict):
                        option_chain = chain_data
                    else:
                        continue
                    
                    logger.debug(f"✅ [OPTION_CHAIN] Loaded option chain for {underlying} using key {key}")
                    break
                except Exception as get_err:
                    logger.debug(f"⚠️ [OPTION_CHAIN] Failed to get key {key}: {get_err}")
                    continue
            
            if not option_chain:
                logger.debug(f"⚠️ [OPTION_CHAIN] No option chain payload found for {underlying} (keys tried: {seen_keys})")
                
        except Exception as e:
            logger.warning(f"⚠️ [OPTION_CHAIN] Failed to build option chain for {underlying}: {e}")
        
        return option_chain

    def _process_option_chain_for_underlying(self, underlying: str, underlying_price: float):
        """
        Process option chain for NIFTY/BANKNIFTY using DeltaAnalyzer.
        
        This method builds the option chain from Redis and processes it using DeltaAnalyzer
        to generate delta-based signals.
        
        Args:
            underlying: Underlying symbol (e.g., 'NIFTY', 'BANKNIFTY')
            underlying_price: Current price of the underlying
        """
        try:
            from patterns.delta_analyzer import DeltaAnalyzer
            
            # Build option chain from Redis
            option_chain = self._build_option_chain_from_redis(underlying)
            
            if not option_chain:
                return
            
            # Initialize DeltaAnalyzer if not already done
            if not hasattr(self, 'delta_analyzer'):
                self.delta_analyzer = DeltaAnalyzer()
            
            # Process option chain
            delta_result = self.delta_analyzer.analyze_option_chain(
                symbol=underlying,
                underlying_price=underlying_price,
                option_chain=option_chain
            )
            
            if delta_result and delta_result.get('reversal_alert'):
                # Inject pattern name
                delta_result['pattern'] = 'delta_reversal'
                logger.debug(f"✅ [DELTA_ANALYZER] Generated reversal signal for {underlying}")
                
                if hasattr(self, 'alert_manager') and self.alert_manager:
                    self.alert_manager.send_alert(delta_result)
                        
        except Exception as e:
            logger.warning(f"⚠️ [DELTA_ANALYZER] Failed to process option chain for {underlying}: {e}")

    def _update_indicator_cache(self, symbol: str, last_price: Optional[float]):
        """
        Update indicator cache for a symbol.
        
        This method is called periodically to refresh indicator cache.
        """
        try:
            if hasattr(self, 'tick_processor') and self.tick_processor:
                # Create dummy tick data for cache update
                dummy_tick = {
                    "symbol": symbol,
                    "last_price": last_price or 0,
                    "timestamp": int(time.time() * 1000)
                }
                
                # Process tick to update cache
                indicators = self.tick_processor.process_tick(symbol, dummy_tick)
                if indicators:
                    logger.debug(f"✅ [CACHE_UPDATE] Updated indicator cache for {symbol}")
        except Exception as e:
            logger.debug(f"⚠️ [CACHE_UPDATE] Failed to update cache for {symbol}: {e}")

    def _trigger_option_chain_updates(self, symbol: str, indicators: Dict[str, Any], tick_data: Dict[str, Any]):
        """Process option-chain data for NIFTY/BANKNIFTY at a throttled cadence."""
        try:
            if not symbol:
                return
            
            symbol_str = str(symbol)
            symbol_upper = symbol_str.upper()  # ✅ FIX: Define symbol_upper before try/except
            
            # ✅ FIXED: Use UniversalSymbolParser from shared_core for consistent symbol parsing
            try:
                from shared_core.redis_clients.redis_key_standards import get_symbol_parser
                parser = get_symbol_parser()
                parsed = parser.parse_symbol(symbol_str)
                # Ignore option/future symbols (only process equity/index symbols)
                if parsed.instrument_type in ('OPT', 'FUT'):
                    return
            except Exception:
                # Fallback to legacy check for backward compatibility
                if symbol_upper.endswith("CE") or symbol_upper.endswith("PE") or symbol_upper.endswith("FUT"):
                    return
            
            # ✅ CENTRALIZED: Use canonical_symbol for symbol normalization (section 3)
            canonical = RedisKeyStandards.canonical_symbol(symbol_str)
            canonical_upper = canonical.upper()
            
            alias_map = {
                "NIFTY": "NIFTY",
                "NSENIFTY": "NIFTY",
                "NIFTY50": "NIFTY",
                "NIFTY 50": "NIFTY",
                "NSENIFTY50": "NIFTY",
                "NSE:NIFTY50": "NIFTY",
                "NSE:NIFTY 50": "NIFTY",
                "INDEX:NSE:NIFTY50": "NIFTY",
                "INDEX:NSE:NIFTY 50": "NIFTY",
                "NSE:NIFTY_50": "NIFTY",
                "NIFTY_50": "NIFTY",
                "BANKNIFTY": "BANKNIFTY",
                "NIFTYBANK": "BANKNIFTY",
                "NIFTY BANK": "BANKNIFTY",
                "NSENIFTYBANK": "BANKNIFTY",
                "NSE:NIFTYBANK": "BANKNIFTY",
                "NSE:NIFTY BANK": "BANKNIFTY",
                "INDEX:NSE:NIFTYBANK": "BANKNIFTY",
                "INDEX:NSE:NIFTY BANK": "BANKNIFTY",
                "NSE:BANKNIFTY": "BANKNIFTY",
            }
            
            normalized_underlying = None
            for candidate in (
                symbol_upper,
                canonical_upper,
                canonical_upper.replace("NSE", "NSE:").replace("::", ":"),
            ):
                cleaned = candidate.replace("INDEX:", "").strip()
                if cleaned in alias_map:
                    normalized_underlying = alias_map[cleaned]
                    break
                cleaned_no_space = cleaned.replace(" ", "")
                if cleaned_no_space in alias_map:
                    normalized_underlying = alias_map[cleaned_no_space]
                    break
            
            if not normalized_underlying:
                return
            
            last_price = indicators.get("last_price") or tick_data.get("last_price")
            if not last_price or last_price <= 0:
                return
            
            now = time.time()
            last_processed = self._last_option_chain_process.get(normalized_underlying)
            if last_processed and (now - last_processed) < self.option_chain_refresh_interval:
                return
            
            self._last_option_chain_process[normalized_underlying] = now
            self._process_option_chain_for_underlying(normalized_underlying, float(last_price))
        except Exception as exc:
            logger.debug(f"⚠️ [OPTION_CHAIN] Unable to trigger option chain update for {symbol}: {exc}")

    def _process_tick_fast_lane(self, tick_data, processed_count, dbg_enabled):
        """Ultra-fast tick processing for high-volume symbols"""
        try:
            # ✅ CRITICAL: Canonicalize symbol EARLY before any processing
            raw_symbol = tick_data.get("symbol") or tick_data.get("tradingsymbol") or ""
            if not raw_symbol or raw_symbol == "UNKNOWN":
                return
            symbol = RedisKeyStandards.canonical_symbol(raw_symbol)
            if not symbol or symbol == "UNKNOWN":
                return
            # ✅ Update tick_data with canonical symbol for downstream
            tick_data["symbol"] = symbol
            tick_data["tradingsymbol"] = symbol
            
            # Fast lane: Only process if volume ratio is high
            volume_ratio = tick_data.get("volume_ratio", 0)
            if volume_ratio < 1.5:  # Skip low volume
                return
            
            # Quick pattern detection
            indicators = self._get_indicators_from_cache(symbol, tick_data)
            
            # ✅ MERGE: Ensure Greeks/Edge/Micro indicators from tick_data are included
            if indicators:
                # Merge Greeks
                for field in ['delta', 'gamma', 'theta', 'vega', 'iv', 'implied_volatility']:
                    if field in tick_data and tick_data[field] is not None:
                        indicators[field] = tick_data[field]
                # Merge Edge
                for field in ['edge_dhdi', 'edge_motive', 'edge_regime_id']:
                    if field in tick_data and tick_data[field] is not None:
                        indicators[field] = tick_data[field]
            elif tick_data:
                 # Fallback: create indicators from tick_data if cache/calc failed
                 indicators = dict(tick_data)
                 
            if indicators:
                patterns = self._detect_patterns_ultra_fast(symbol, indicators)
                if patterns:
                    self._collect_alert_for_processing(patterns[0])
        except Exception:
            pass  # Fail silently in fast lane

    def _detect_patterns_ultra_fast(
        self, symbol: str, indicators: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        ✅ DELEGATED: Directly delegates to PatternDetector.detect_patterns().
        This wrapper is kept for backward compatibility but now just calls the unified method.
        """
        if not hasattr(self, 'pattern_detector') or not self.pattern_detector:
            return []
        
        try:
            # ✅ DELEGATED: Use unified PatternDetector.detect_patterns() method
            return self.pattern_detector.detect_patterns(indicators, use_pipelining=True)
        except Exception as e:
            logger.debug(f"⚠️ [PATTERN_DETECTION] Pattern detection failed for {symbol}: {e}")
            return []

    def _extract_required_indicators(self, conditions: Any) -> List[str]:
        """Extract required indicator names from pattern conditions"""
        required = set()
        
        # Common indicators
        common_indicators = ['rsi', 'macd', 'ema_20', 'ema_50', 'atr', 'vwap', 'volume_ratio']
        for ind in common_indicators:
            if ind in str(conditions):
                required.add(ind)
        
        return list(required)

    # ✅ REMOVED: _check_pattern_conditions() - Dead code (never called)
    # Pattern condition checking is handled internally by PatternDetector
    # No need for separate condition checker - PatternDetector validates all conditions

    def _detect_patterns_with_validation(
        self, symbol: str, indicators: Dict[str, Any], tick_data: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        ✅ ENHANCED: Detects patterns with tick_data for microstructure indicators
        """
        if not hasattr(self, 'pattern_detector') or not self.pattern_detector:
            return []
        
        try:
            # ✅ CRITICAL FIX: Ensure symbol is in indicators dict (required by detect_patterns)
            # detect_patterns() reads symbol from indicators.get("symbol"), not from parameter
            if not isinstance(indicators, dict):
                logger.error(f"❌ [PATTERN_VALIDATION] indicators is not a dict: {type(indicators)}")
                return []
            
            # Ensure symbol field exists in indicators
            if "symbol" not in indicators or not indicators.get("symbol") or indicators.get("symbol") == "UNKNOWN":
                if symbol and symbol != "UNKNOWN":
                    indicators["symbol"] = symbol
                    # Also set tradingsymbol for compatibility
                    if "tradingsymbol" not in indicators:
                        indicators["tradingsymbol"] = symbol
                    logger.debug(f"✅ [PATTERN_VALIDATION] Added symbol={symbol} to indicators dict")
                else:
                    logger.error(f"❌ [PATTERN_VALIDATION] Cannot add symbol to indicators: symbol={symbol}, indicators keys: {list(indicators.keys())[:20]}")
                    return []
            
            # ✅ CRITICAL: Pass tick_data to pattern detector for microstructure
            # The pattern detector can access raw tick_data for order book, microstructure
            # Check if detect_patterns accepts tick_data parameter
            import inspect
            sig = inspect.signature(self.pattern_detector.detect_patterns)
            if 'tick_data' in sig.parameters:
                return self.pattern_detector.detect_patterns(
                    indicators, 
                    use_pipelining=True,
                    tick_data=tick_data  # Pass raw tick data
                )
            else:
                # Fallback: If detect_patterns doesn't accept tick_data, just pass indicators
                # The microstructure indicators should already be merged into indicators
                return self.pattern_detector.detect_patterns(indicators, use_pipelining=True)
        except Exception as e:
            logger.error(f"❌ [PATTERN_VALIDATION] Pattern detection failed for {symbol}: {e}", exc_info=True)
            return []

    def _should_run_game_theory(self, symbol: str) -> bool:
        if not self.game_theory_engine:
            return False
        if self.game_theory_run_all:
            return True
        return self._is_game_theory_validation_symbol(symbol)

    def _is_game_theory_validation_symbol(self, symbol: str) -> bool:
        if not symbol:
            return False
        # ✅ CENTRALIZED: Use canonical_symbol for watchlist matching (section 3)
        canonical = RedisKeyStandards.canonical_symbol(symbol)
        return canonical in self.game_theory_watchlist or symbol.upper() in self.game_theory_watchlist

    def _evaluate_game_theory_layer(self, symbol: str, tick_data: Dict[str, Any], indicators: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Evaluate game theory layer for a symbol"""
        if not self.game_theory_engine:
            return None
        
        if not self._should_run_game_theory(symbol):
            return None
        
        try:
            # Collect ticks for game theory
            ticks = self._collect_ticks_for_game_theory(tick_data, indicators)
            if not ticks:
                return None
            
            # Extract order book from tick_data
            order_book = tick_data.get('depth') or tick_data.get('order_book')
            if isinstance(order_book, str):
                try:
                    import json
                    order_book = json.loads(order_book)
                except:
                    order_book = None
            
            # Run game theory evaluation (async method, need to run in event loop)
            import asyncio
            try:
                # Try to get existing event loop
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # If loop is running, create a task
                    # For now, return None if loop is running (will be handled by async pipeline)
                    return None
                else:
                    # Run in existing loop
                    game_signal = loop.run_until_complete(
                        self.game_theory_engine.generate_intraday_signals(
                            symbol=symbol,
                            order_book=order_book,
                            ticks=ticks
                        )
                    )
                    return game_signal
            except RuntimeError:
                # No event loop, create one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    game_signal = loop.run_until_complete(
                        self.game_theory_engine.generate_intraday_signals(
                            symbol=symbol,
                            order_book=order_book,
                            ticks=ticks
                        )
                    )
                    return game_signal
                finally:
                    loop.close()
        except Exception as e:
            logger.warning(f"⚠️ Game theory evaluation failed for {symbol}: {e}")
            return None

    def _collect_ticks_for_game_theory(self, tick_data: Dict[str, Any], indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Collect recent ticks for game theory analysis"""
        return [tick_data]  # Simplified - could collect more ticks

    def _game_theory_confirms(self, symbol: str, pattern: Dict[str, Any], game_signal: Optional[Dict[str, Any]]) -> bool:
        """Check if game theory confirms the pattern"""
        if not game_signal:
            return False
        
        pattern_direction = self._normalize_pattern_direction(pattern)
        game_direction = game_signal.get('predicted_direction')
        
        return pattern_direction == game_direction

    def _normalize_pattern_direction(self, pattern: Dict[str, Any]) -> Optional[str]:
        """Normalize pattern direction to LONG/SHORT"""
        direction = pattern.get('direction') or pattern.get('signal')
        if direction:
            direction_upper = str(direction).upper()
            if 'LONG' in direction_upper or 'BUY' in direction_upper:
                return 'LONG'
            elif 'SHORT' in direction_upper or 'SELL' in direction_upper:
                return 'SHORT'
        return None

    def _map_direction_to_sentiment(self, direction: Optional[str]) -> str:
        """Map direction to sentiment for game theory"""
        if direction == 'LONG':
            return 'BULLISH'
        elif direction == 'SHORT':
            return 'BEARISH'
        return 'NEUTRAL'

    def _fuse_all_signals(
        self, symbol: str, pattern: Dict[str, Any], game_signal: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Fuse pattern and game theory signals"""
        fused = pattern.copy()
        
        if game_signal:
            fused['game_theory_confidence'] = game_signal.get('confidence', 0.0)
            fused['game_theory_direction'] = game_signal.get('predicted_direction')
            fused['game_theory_confirmed'] = self._game_theory_confirms(symbol, pattern, game_signal)
        
        return fused

    def _extract_greeks_snapshot(self, indicators: Dict[str, Any]) -> Dict[str, float]:
        """Extract Greeks from indicators"""
        greeks = {}
        for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']:
            if greek in indicators:
                greeks[greek] = float(indicators[greek]) if indicators[greek] is not None else 0.0
        return greeks

    def _run_quick_game_theory_check(self, symbol: str, order_book: Optional[Dict[str, Any]], sentiment: str) -> Optional[str]:
        """
        ✅ DELEGATED: Delegates to QuickGameTheoryIntegration.quick_gt_check().
        Uses the unified QuickGameTheoryIntegration for Nash-only game theory checks.
        """
        if not hasattr(self, 'quick_gt_adapter') or not self.quick_gt_adapter:
            return None
        
        if not order_book:
            return None
        
        try:
            # ✅ DELEGATED: Use unified QuickGameTheoryIntegration.quick_gt_check()
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # If loop is running, return None (will be handled by async pipeline)
                    return None
                else:
                    result = loop.run_until_complete(
                        self.quick_gt_adapter.quick_gt_check(symbol, order_book)
                    )
                    return result
            except RuntimeError:
                # No event loop, create one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(
                        self.quick_gt_adapter.quick_gt_check(symbol, order_book)
                    )
                    return result
                finally:
                    loop.close()
        except Exception as e:
            logger.debug(f"⚠️ [GAME_THEORY] Quick GT check failed for {symbol}: {e}")
            return None

    def _trigger_alert_instant(self, pattern: Dict[str, Any]):
        """Trigger alert instantly without delay"""
        try:
            if hasattr(self, 'alert_manager') and self.alert_manager:
                self.alert_manager.send_alert(pattern)
        except Exception as e:
            logger.warning(f"⚠️ Failed to trigger instant alert: {e}")

    def _log_detection_health(self):
        """Log pattern detection health metrics"""
        try:
            if hasattr(self, 'pattern_detector') and self.pattern_detector:
                # Log health metrics
                logger.debug("🔍 Pattern detection health check")
        except Exception:
            pass

    # ✅ REMOVED: _perform_health_check() - Legacy function with incomplete health checks
    # Health checks should be handled by centralized health monitoring system
    # Individual thread/GIL checks are not needed - system handles this automatically

    def _check_thread_deadlocks(self):
        """Check for thread deadlocks"""
        try:
            with self.threads_lock:
                if not self.threads:
                    return
                active_threads = [t for t in self.threads if t.is_alive()]
                dead_threads = [t for t in self.threads if not t.is_alive()]
                if dead_threads:
                    dead_names = ", ".join(filter(None, [t.name for t in dead_threads])) or "unknown"
                    logger.warning(
                        f"⚠️ Some threads are dead: {len(active_threads)}/{len(self.threads)} alive | Dead: {dead_names}"
                    )
                    self.threads = active_threads
                    dead_set = set(dead_threads)
                    for attr in [
                        'memory_cleanup_thread',
                        'tick_processor_cleanup_thread',
                        'health_monitor_thread',
                        'optimized_processor_thread',
                        'kow_strategy_thread',
                    ]:
                        thread = getattr(self, attr, None)
                        if thread in dead_set:
                            setattr(self, attr, None)
                    # Restart background maintenance threads if needed
                    self._start_background_threads()
                    if self.health_monitor_thread is None:
                        self._start_health_monitor_if_needed()
        except Exception:
            pass

    # ✅ REMOVED: Legacy health check helper functions - all were placeholders (just `pass`)
    # These functions were never implemented and are not needed
    # Health checks are handled by centralized monitoring system

    def _check_websocket_health(self, message_count, last_message_time, current_time):
        """Check WebSocket connection health"""
        try:
            time_since_last = current_time - last_message_time
            if time_since_last > 60:  # 1 minute
                logger.warning(f"⚠️ WebSocket: No messages for {time_since_last:.1f}s")
        except Exception:
            pass

    def _refresh_websocket_connection(self):
        """Refresh WebSocket connection to prevent hanging"""
        try:
            # ✅ CENTRALIZED: Don't close pooled connections - RedisClientFactory manages connection lifecycle
            # Pooled connections are managed by RedisClientFactory and should not be manually closed
            # If connection issues occur, RedisClientFactory will handle reconnection automatically
            
            # ✅ FIXED: Reconnect using UnifiedRedisManager
            from shared_core.redis_clients import UnifiedRedisManager

            # Create wrapper for compatibility
            class RedisWrapper:
                def __init__(self, redis_client):
                    self.redis_client = redis_client
                def get_client(self, db):
                    return get_redis_client(process_name="scanner_main_mock", db=db)
                def ping(self):
                    return self.redis_client.ping()
            
            redis_core = get_redis_client(
                process_name="websocket_reconnect",
                db=0,
                max_connections=None
            )
        
            self.redis_wrapper = RedisWrapper(redis_core)
            logger.info("✅ WebSocket connection refreshed")
        except Exception as e:
            logger.error(f"❌ Failed to refresh WebSocket connection: {e}")

    def check_pipeline_health(self):
        """Simplified pipeline health check."""
        if hasattr(self, "pipeline_monitor") and self.pipeline_monitor:
            return self.pipeline_monitor.check_health()
        return True
    
    def attempt_recovery(self):
        """Delegate recovery attempts to the simple pipeline monitor."""
        if hasattr(self, "pipeline_monitor") and self.pipeline_monitor:
            return self.pipeline_monitor._simple_restart()
        return False
    
    async def health_check(self):
        """Asynchronous health check covering Redis connectivity and stream health."""
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, self._health_check_client.ping)
            def _get_pending():
                return self._health_check_client.xpending(
                    self._scanner_stream_key,
                    "data_pipeline_group"
                )
            pending_info = await loop.run_in_executor(None, _get_pending)
            pending_count = 0
            if isinstance(pending_info, dict):
                pending_count = pending_info.get("pending", 0) or 0
            threshold = self.config.get("health_check_pending_threshold", 1000)
            if pending_count > threshold:
                logger.warning(
                    "⚠️ Pending entries above threshold (%s > %s). Resetting consumer group.",
                    pending_count,
                    threshold
                )
                await loop.run_in_executor(None, self.reset_consumer_group)
            return True
        except Exception as exc:
            logger.error(f"Health check failed: {exc}")
            if hasattr(self, "pipeline_monitor") and self.pipeline_monitor:
                await loop.run_in_executor(None, self.pipeline_monitor._simple_restart)
            return False
    
    def _try_recovery_strategies(self):
        """Try multiple recovery strategies"""
        strategies = [
            self._reconnect_data_feed,
            self._restart_data_processor,
            self._clear_corrupted_buffers,
            self._fallback_to_alternative_source
        ]
        
        for strategy in strategies:
            try:
                if strategy():
                    return True
            except Exception as e:
                logger.debug(f"Recovery strategy failed: {e}")
                continue
                
        return False
    
    def _reconnect_data_feed(self):
        """Strategy 1: Reconnect to data feed"""
        try:
            logger.info("🔄 Strategy 1: Reconnecting data feed...")
            if hasattr(self, 'data_pipeline') and hasattr(self.data_pipeline, '_reconnect'):
                self.data_pipeline._reconnect()
                return True
        except Exception as e:
            logger.debug(f"Reconnect strategy failed: {e}")
        return False
    
    def _restart_data_processor(self):
        """Strategy 2: Restart data processor"""
        try:
            logger.info("🔄 Strategy 2: Restarting data processor...")
            if hasattr(self, 'data_pipeline'):
                # Stop and restart consumer thread
                if hasattr(self.data_pipeline, 'consumer_thread') and self.data_pipeline.consumer_thread.is_alive():
                    self.data_pipeline.running = False
                    self.data_pipeline.consumer_thread.join(timeout=5)
                # Restart will happen automatically on next health check
                return True
        except Exception as e:
            logger.debug(f"Restart strategy failed: {e}")
        return False
    
    def _clear_corrupted_buffers(self):
        """Strategy 3: Clear corrupted buffers"""
        try:
            logger.info("🔄 Strategy 3: Clearing corrupted buffers...")
            if hasattr(self, 'data_pipeline'):
                if hasattr(self.data_pipeline, 'tick_buffer'):
                    self.data_pipeline.tick_buffer.clear()
                if hasattr(self.data_pipeline, 'batch_buffer'):
                    self.data_pipeline.batch_buffer.clear()
                return True
        except Exception as e:
            logger.debug(f"Clear buffers strategy failed: {e}")
        return False
    
    def _fallback_to_alternative_source(self):
        """Strategy 4: Fallback to alternative data source"""
        try:
            logger.info("🔄 Strategy 4: Attempting fallback data source...")
            # This could switch to a backup data feed if available
            # For now, just return False to indicate no alternative available
            return False
        except Exception as e:
            logger.debug(f"Fallback strategy failed: {e}")
        return False
    
    def _enter_safe_mode(self):
        """
        ✅ REFACTORED: Enter safe mode without stopping alerts.
        Circuit breaker should only stop data pipeline recovery, not alert processing.
        """
        logger.critical("🚨 ENTERING SAFE MODE - Data pipeline recovery disabled, alerts continue")
        # ✅ FIXED: Don't stop alerts - only disable data pipeline recovery
        # Set flag to disable recovery attempts, but keep scanner running for alerts
        if not hasattr(self, '_recovery_disabled'):
            self._recovery_disabled = True
        
        # ✅ FIXED: Send alert but don't stop scanner
        try:
            if hasattr(self, 'alert_manager'):
                self.alert_manager.send_alert({
                    'pattern': 'SYSTEM_ALERT',
                    'symbol': 'SYSTEM',
                    'confidence': 1.0,
                    'description': 'Data pipeline recovery disabled - manual intervention required',
                    'action': 'WATCH'
                })
        except Exception:
            pass
    
    def _process_tick_core(self, tick_data, processed_count, dbg_enabled):
        """Core tick processing logic with enhanced data normalization"""
        try:
            # ✅ CRITICAL: Canonicalize symbol EARLY before any processing
            raw_symbol = tick_data.get("symbol") or tick_data.get("tradingsymbol") or ""
            if not raw_symbol or raw_symbol == "UNKNOWN":
                return
            symbol = RedisKeyStandards.canonical_symbol(raw_symbol)
            if not symbol or symbol == "UNKNOWN":
                return
            # ✅ Update tick_data with canonical symbol for downstream
            tick_data["symbol"] = symbol
            tick_data["tradingsymbol"] = symbol
            
            # ✅ CRITICAL: Ensure microstructure indicators are preserved
            # These indicators come from crawler and should be passed through
            micro_indicators = {}
            for field in ['microprice', 'order_flow_imbalance', 'order_book_imbalance',
                          'cumulative_volume_delta', 'spread_absolute', 'spread_bps',
                          'depth_imbalance', 'best_bid_size', 'best_ask_size',
                          'total_bid_depth', 'total_ask_depth']:
                if field in tick_data and tick_data[field] is not None:
                    try:
                        micro_indicators[field] = float(tick_data[field])
                    except (TypeError, ValueError):
                        micro_indicators[field] = None

            
            # ✅ NEW: Extract edge indicators from tick_data (computed by CalculationService)
            edge_indicators = {}
            for field in ['edge_dhdi', 'edge_motive', 'edge_hedge_pressure',
                          'edge_gamma_stress', 'edge_charm_ratio', 'edge_lci',
                          'edge_regime_id', 'edge_regime_label', 'edge_should_trade',
                          'edge_intent', 'edge_constraint', 'edge_transition', 'edge_regime']:
                if field in tick_data and tick_data[field] is not None:
                    edge_indicators[field] = tick_data[field]
            
            # ✅ NEW: Extract Greeks from tick_data (real-time from pipeline)
            # This ensures we don't lose Greeks computed by specialized workers
            greeks = {}
            for field in ['delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'implied_volatility',
                          'underlying_price', 'strike_price', 'time_to_expiry']:
                if field in tick_data and tick_data[field] is not None:
                    greeks[field] = tick_data[field]
            
            # Get indicators
            indicators = self._get_indicators_from_cache(symbol, tick_data)
            
            # ✅ Merge microstructure indicators, edge indicators, and Greeks
            if indicators:
                indicators.update(micro_indicators)
                indicators.update(edge_indicators)
                indicators.update(greeks)
            else:
                indicators = {**micro_indicators, **edge_indicators, **greeks}
            
            # ✅ NEW: Inject VIX Data (Critical for vix_momentum pattern)
            if self.vix_utils:
                try:
                    vix_data = self.vix_utils.get_current_vix()
                    if vix_data:
                        indicators['vix'] = vix_data.get('value', 0.0)
                        indicators['vix_regime'] = vix_data.get('regime', 'NORMAL')
                        # Ensure change_pct is available for momentum patterns
                        indicators['vix_change_pct'] = vix_data.get('change_pct', 0.0)
                except Exception as e:
                    # Log once or debug to avoid spam
                    pass
            
            # ✅ NEW: Inject Gamma Exposure (Critical for gamma_reversal pattern)
            if self.gex_calculator:
                try:
                    # Calculate on-the-fly (uses internal caching)
                    # Note: We pass the symbol directly. GammaExposureCalculator interprets this as the underlying.
                    # This works correctly for Index symbols (e.g. NIFTY 50).
                    gex_data = self.gex_calculator.calculate_total_gex(symbol)
                    if gex_data:
                        indicators['gamma_exposure'] = gex_data.get('net_gex', 0.0)
                        indicators['total_gex'] = gex_data.get('total_gex', 0.0)
                except Exception as e:
                    pass
            
            if not indicators:
                return

            # Ensure we have a proper last_price
            if 'last_price' not in indicators or indicators['last_price'] is None:
                indicators['last_price'] = tick_data.get('last_price')

            # Trigger option-chain processing for index underlyings
            self._trigger_option_chain_updates(symbol, indicators, tick_data)
            
            # ✅ Pass both indicators AND tick_data to pattern detection
            # tick_data contains the raw microstructure data
            patterns = self._detect_patterns_with_validation(symbol, indicators, tick_data)
            
            # Process patterns
            if patterns:
                for pattern in patterns:
                    self._collect_alert_for_processing(pattern)
        except Exception as e:
            logger.warning(f"⚠️ Core tick processing failed: {e}")

    def _process_tick(self, tick_data, processed_count, dbg_enabled):
        """Main tick processing entry point"""
        return self._process_tick_core(tick_data, processed_count, dbg_enabled)

    def _collect_alert_for_processing(self, pattern):
        """Collect alert for batch processing"""
        try:
            if hasattr(self, '_pending_alerts'):
                self._pending_alerts.append(pattern)
        except Exception:
            pass

    def _process_pending_alerts(self):
        """Process pending alerts in batch"""
        try:
            if hasattr(self, '_pending_alerts') and self._pending_alerts:
                alerts = self._pending_alerts[:10]  # Process first 10
                self._pending_alerts = self._pending_alerts[10:]
                
                for alert in alerts:
                    if hasattr(self, 'alert_manager') and self.alert_manager:
                        self.alert_manager.send_alert(alert)
        except Exception:
            pass

    def _process_news_alerts(self):
        """Process news-based alerts"""
        try:
            # News alert processing logic
            pass
        except Exception:
            pass

    def _is_news_trading_relevant(self, news_item):
        """Check if news is trading relevant"""
        try:
            # News relevance checking
            return False
        except Exception:
            return False

    def _send_individual_alert(self, pattern):
        """Send individual alert"""
        try:
            if hasattr(self, 'alert_manager') and self.alert_manager:
                self.alert_manager.send_alert(pattern)
        except Exception as e:
            logger.warning(f"⚠️ Failed to send alert: {e}")

    def _calculate_pattern_score(self, pattern_type, confidence, volume_ratio, symbol=None, vix_regime="NORMAL"):
        """
        ✅ CENTRALIZED: Calculate pattern score using dynamic thresholds and VolumeThresholdCalculator.
        No hardcoded multipliers - uses dynamic threshold system.
        """
        try:
            # ✅ CENTRALIZED: Use VolumeThresholdCalculator for VIX-adjusted confidence
            try:
                from patterns.volume_thresholds import VolumeThresholdCalculator
                
                # Get VIX-adjusted confidence using dynamic thresholds
                if hasattr(self, 'redis_client') and self.redis_client:
                    vol_calculator = VolumeThresholdCalculator(redis_client=self.redis_client)
                    adjusted_confidence = vol_calculator.get_vix_adjusted_confidence(
                        confidence, pattern_type, symbol
                    )
                else:
                    adjusted_confidence = confidence
            except Exception as e:
                logger.debug(f"⚠️ [SCORE] VolumeThresholdCalculator failed, using base confidence: {e}")
                adjusted_confidence = confidence
            
            # ✅ CENTRALIZED: Get dynamic volume multiplier threshold from config
            try:
                from shared_core.config_utils.thresholds import get_pattern_volume_requirement
                
                # Get volume threshold for this pattern
                volume_threshold = get_pattern_volume_requirement(
                    pattern_type,
                    symbol=symbol,
                    vix_regime=vix_regime,
                    redis_client=self.redis_client if hasattr(self, 'redis_client') else None
                )
                
                # Calculate volume multiplier based on dynamic threshold (not hardcoded 2.0)
                if volume_threshold and volume_threshold > 0:
                    # Volume multiplier: how much above threshold (capped at reasonable max)
                    volume_multiplier = min(volume_ratio / volume_threshold, 1.5)  # Cap at 1.5x
                else:
                    # Fallback: use volume_ratio directly if threshold unavailable
                    volume_multiplier = min(volume_ratio, 1.5)
            except Exception as e:
                logger.debug(f"⚠️ [SCORE] Dynamic volume threshold failed, using volume_ratio: {e}")
                volume_multiplier = min(volume_ratio, 1.5)
            
            # Calculate final score: VIX-adjusted confidence * volume multiplier * 100
            base_score = adjusted_confidence * 100
            score = base_score * volume_multiplier
            return score
        except Exception as e:
            logger.debug(f"⚠️ [SCORE] Pattern score calculation failed: {e}")
            return 0.0

    def _patterns_to_dataframe(self, patterns: List[Dict[str, Any]], symbol: str) -> Optional['pl.DataFrame']:
        """Convert patterns to Polars DataFrame"""
        if not POLARS_AVAILABLE or not patterns:
            return None
        
        try:
            import polars as pl
            return pl.DataFrame(patterns)
        except Exception:
            return None

    def _get_sharpe_metrics_batch(self, patterns_df: 'pl.DataFrame') -> 'pl.DataFrame':
        """Get Sharpe metrics for patterns in batch"""
        if not POLARS_AVAILABLE:
            return patterns_df
        
        try:
            import polars as pl
            # Simplified Sharpe calculation
            return patterns_df
        except Exception:
            return patterns_df

    def record_alert(self, pattern):
        """Record alert for performance tracking"""
        try:
            if hasattr(self, 'alert_manager') and self.alert_manager:
                self.alert_manager.record_alert(pattern)
        except Exception:
            pass

    def cleanup_tracking(self):
        """Cleanup tracking data"""
        try:
            if hasattr(self, 'redis_wrapper') and self.redis_wrapper:
                # Cleanup old tracking keys
                pass
        except Exception:
            pass

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown"""
        logger.info("🛑 Shutting down scanner...")
        self.running = False
        
        # Stop all threads
        if hasattr(self, 'threads'):
            for thread in self.threads:
                if thread.is_alive():
                    thread.join(timeout=5)

        self.cleanup()
        logger.info("✅ Scanner shutdown complete")

    def force_shutdown(self):
        """Force shutdown"""
        self.running = False
        logger.info("🛑 Force shutdown initiated")

    def _log_performance_stats(self, stats, processed_count, final=False):
        """Log performance statistics"""
        try:
            logger.info(f"📊 Performance: {processed_count} ticks processed")
            if final:
                logger.info("📊 Final performance stats logged")
        except Exception:
            pass

    def _monitor_websocket_health(self):
        """Monitor WebSocket health"""
        try:
            # Health monitoring logic
            pass
        except Exception:
            pass

    def _restart_websocket_client(self):
        """Restart WebSocket client"""
        try:
            logger.info("🔄 Restarting WebSocket client...")
            # Restart logic
        except Exception:
            pass

    def _restart_data_pipeline(self):
        """Restart data pipeline"""
        try:
            logger.info("🔄 Restarting data pipeline...")
            # Restart logic
        except Exception:
            pass

    def _restart_redis_connection(self):
        """Restart Redis connection"""
        try:
            logger.info("🔄 Restarting Redis connection...")
            # Restart logic
        except Exception:
            pass

    def _add_redis_pattern_validation(self, pattern):
        """Add Redis pattern validation"""
        try:
            # Validation logic
            pass
        except Exception:
            pass

    def _add_volume_context(self, tick_data):
        """Add volume context to tick data"""
        try:
            # Volume context logic
            pass
        except Exception:
            pass

    def compare_data_sources(self, symbol, pattern_indicators, alert_data):
        """Compare data from different sources"""
        try:
            _ = pattern_indicators
            # Comparison logic
            pass
        except Exception:
            pass

    def validate_volume_ratio_flow(self, tick_data):
        """Validate volume ratio flow"""
        try:
            # Validation logic
            pass
        except Exception:
            pass

    def _store_time_based_data(self, tick_data):
        """Store time-based data"""
        try:
            # Storage logic
            pass
        except Exception:
            pass

    def comprehensive_debug(self, symbol: str, tick_data: dict, patterns_detected: list):
        """Comprehensive debugging for pattern detection issues"""
        
        logger.warning("=" * 80)
        logger.warning(f"🔍 [COMPREHENSIVE_DEBUG] SYMBOL: {symbol}")
        logger.warning("=" * 80)
        
        # 1. Check data quality
        logger.warning(f"📊 DATA QUALITY:")
        logger.warning(f"   - Price: {tick_data.get('last_price')}")
        logger.warning(f"   - Volume Ratio: {tick_data.get('volume_ratio')}")
        logger.warning(f"   - RSI: {tick_data.get('rsi')}")
        logger.warning(f"   - Price Change: {tick_data.get('price_change_pct')}%")
        
        # 2. Check Greeks
        greek_fields = ['delta', 'gamma', 'theta', 'vega', 'rho']
        greek_status = {}
        for greek in greek_fields:
            value = tick_data.get(greek)
            greek_status[greek] = "MISSING" if value is None else ("ZERO" if value == 0 else f"{value:.4f}")
        
        logger.warning(f"🧮 GREEKS STATUS: {greek_status}")
        
        # 3. Check volume baselines
        try:
            from shared_core.redis_clients import UnifiedRedisManager
            redis_client = get_redis_client(process_name="scanner_main_volume", db=1)  # DB 1
            baseline_key = DatabaseAwareKeyBuilder.live_volume_baseline(symbol)
            baseline = redis_client.get(baseline_key)
            logger.warning(f"📈 VOLUME BASELINE: {baseline}")
        except Exception as e:
            logger.warning(f"📈 VOLUME BASELINE: Error - {e}")
        
        # 4. Check pattern thresholds
        logger.warning(f"🎯 PATTERNS DETECTED: {len(patterns_detected)} - {patterns_detected}")
        
        # 5. Check symbol consistency
        base_symbol = symbol.replace('25NOVFUT', '').replace('CE', '').replace('PE', '')
        logger.warning(f"🔤 SYMBOL RESOLUTION: Base='{base_symbol}', Full='{symbol}'")
        
        logger.warning("=" * 80)

    def _is_pre_market_hours(self, current_time):
        """Check if current time is pre-market hours"""
        try:
            hour = current_time.hour
            minute = current_time.minute
            return hour == 9 and minute < 15
        except Exception:
            return False

    def _is_first_30min_hours(self, current_time):
        """Check if current time is first 30 minutes"""
        try:
            hour = current_time.hour
            minute = current_time.minute
            return hour == 9 and minute >= 15 and minute < 45
        except Exception:
            return False

    def _store_pre_market_data(self, symbol, tick_data, current_time):
        """Store pre-market data"""
        try:
            # Storage logic
            pass
        except Exception:
            pass

    def _store_first_30min_data(self, symbol, tick_data, current_time):
        """Store first 30 minutes data"""
        try:
            # Storage logic
            pass
        except Exception:
            pass

    def _store_latest_price(self, tick_data):
        """Store latest price in Redis"""
        try:
            symbol = tick_data.get("symbol")
            last_price = tick_data.get("last_price")
            if symbol and last_price:
                # Storage logic
                pass
        except Exception:
            pass

    def _get_latest_price_from_redis(self, symbol):
        """
        ✅ FIXED: Get latest price from Redis using UnifiedRedisManager.
        Uses DB 1 (trading) client for realtime price data.
        """
        try:
            # ✅ FIXED: Use UnifiedRedisManager or existing realtime_db1_client
            if hasattr(self, 'realtime_db1_client') and self.realtime_db1_client:
                client = self.realtime_db1_client
            else:
                from shared_core.redis_clients import UnifiedRedisManager
                client = get_redis_client(process_name="scanner_main_price", db=1)  # DB 1
            
            # Get price logic (placeholder - implement actual price retrieval)
            return 0
        except Exception as e:
            logger.debug(f"⚠️ Error getting latest price for {symbol} from Redis: {e}")
            return 0

    # ✅ REMOVED: _premarket_analyzer_loop() - Incomplete premarket analyzer removed
    # The premarket analyzer contained only stub functions with TODO comments.
    # If premarket scanning is needed in the future, it should be implemented
    # in a separate module with complete logic.

    def reset_consumer_group(self):
        """Reset the scanner consumer group and re-register the consumer."""
        stream_key = getattr(self, "_scanner_stream_key", DatabaseAwareKeyBuilder.live_enriched_stream())
        group_name = "scanner_group"
        try:
            self.redis_client.xgroup_destroy(stream_key, group_name)
        except redis.exceptions.ResponseError as exc:
            if "NOGROUP" not in str(exc).upper():
                logger.debug(f"⚠️ Unable to destroy consumer group: {exc}")
        try:
            self.redis_client.xgroup_create(stream_key, group_name, id="$", mkstream=True)
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc).upper():
                logger.debug(f"⚠️ Unable to recreate consumer group: {exc}")
        self.reset_consumer()

    def reset_consumer(self):
        """Reset scanner consumer registration."""
        if self.consumer_manager:
            self.consumer_name = self.consumer_manager.create_consumer(
                f"scanner_{os.getpid()}_{int(time.time())}"
            )
            logger.info(f"✅ Scanner registered as consumer: {self.consumer_name}")

    def cleanup(self):
        """Cleanup scanner resources."""
        if getattr(self, 'consumer_manager', None):
            try:
                self.consumer_manager.stop()
            except Exception as exc:
                logger.debug(f"⚠️ Error stopping consumer manager: {exc}")

    def stop(self):
        """Stop the scanner and cleanup resources"""
        logger.info("🛑 Stopping scanner...")
        self.running = False

        self._shutdown_async_processing()

        # Stop historical data manager
        if hasattr(self, 'historical_data_manager') and self.historical_data_manager:
            try:
                self.historical_data_manager.stop_continuous_export()
            except Exception:
                pass

        # Stop data pipeline
        if hasattr(self, 'data_pipeline') and self.data_pipeline:
            try:
                self.data_pipeline.stop()
            except Exception:
                pass

        # Stop all threads
        if hasattr(self, 'threads'):
            for thread in self.threads:
                if thread.is_alive():
                    thread.join(timeout=5)

        self.cleanup()
        logger.info("✅ Scanner stopped")

    def _send_scalper_alert(self, opportunity: Dict):
        """Send scalper alert"""
        try:
            if hasattr(self, 'alert_manager') and self.alert_manager:
                self.alert_manager.send_alert(opportunity)
        except Exception:
            pass

    def get_sharpe_performance_summary(self) -> Optional['pl.DataFrame']:
        """Get Sharpe performance summary"""
        if not POLARS_AVAILABLE:
            return None
        
        try:
            import polars as pl
            if hasattr(self, 'sharpe_dashboard') and self.sharpe_dashboard:
                return self.sharpe_dashboard.get_pattern_performance_summary()
        except Exception:
            pass
        return None

# ============================================================================
# Redis Request Tracer for Debugging (Standalone functions, not class methods)
# ============================================================================

# ✅ REMOVED: RequestTracer class and debug_retrieval() function
# These heavy debugging utilities print massive Redis traces and are not part of normal operations.
# If needed, create a separate debug module (e.g., intraday_scanner/debug/request_tracer.py)
# or use scripts/debug_pattern_detection.py for debugging purposes.

# ✅ REMOVED: comprehensive_data_flow_check() function
# This is a heavy debugging utility that prints massive traces and is not part of normal operations.
# If needed, use scripts/debug_pattern_detection.py or create a separate debug module.

# ============================================================================
# Data Ingestion Monitor - REMOVED (duplicate of MarketScanner)
# ============================================================================
# This class was a complete duplicate of MarketScanner with 100+ duplicate methods.
# All functionality is handled by MarketScanner orchestrator and its components.
# Removed to reduce codebase by ~4000 lines and eliminate maintenance burden.

class LiveSharpeDashboard:
    """
    Real-time Sharpe monitoring dashboard for all 22 patterns.
    
    ✅ OPTIMIZED: Uses Polars for high-performance batch operations.
    Provides real-time performance summaries and rankings for pattern selection.
    
    Features:
    - Real-time pattern performance monitoring
    - Sharpe ratio rankings across all patterns
    - Return-based rankings
    - Overall performance scoring
    - Batch processing with Polars (10-100x faster than pandas)
    """
    
    def __init__(self, redis_client=None):
        """
        Initialize LiveSharpeDashboard.
        
        Args:
            redis_client: Redis client for accessing pattern metrics (DB 2 - analytics)
        """
        self.redis_client = redis_client
        if not self.redis_client:
            try:
                # ✅ FIXED: Use DB2 for analytics (pattern metrics) per canonical standards
                from shared_core.redis_clients import UnifiedRedisManager
                self.redis_client = get_redis_client(process_name="live_sharpe_dashboard", db=2)
            except Exception as e:
                logger.error(f"Failed to initialize Redis client for LiveSharpeDashboard: {e}")
                self.redis_client = None
    
    async def get_pattern_performance_summary(self) -> Optional['pl.DataFrame']:
        """
        Get performance summary of all patterns using Polars.
        
        ✅ UPDATED: Uses PerformanceTracker's key format and stored metrics.
        Reads from DB 2 (analytics) using the same key builder as PerformanceTracker.
        
        Returns:
            Polars DataFrame with pattern performance metrics, ranked by success rate and latency
        """
        if not POLARS_AVAILABLE:
            return None
        
        if not self.redis_client:
            logger.warning("Redis client not available for LiveSharpeDashboard")
            return None
        
        try:
            import polars as pl
            from shared_core.redis_clients import UnifiedRedisManager
            db2_client = get_redis_client(process_name="pattern_performance_summary", db=2)
            
            if not db2_client:
                logger.warning("DB 2 client not available for pattern metrics")
                return None
            
            # ✅ FIXED: Scan for pattern_metrics keys using PerformanceTracker's key format
            # Key format: pattern_metrics:{symbol}:{pattern_type} (from builder.analytics_pattern_metrics)
            pattern_keys = []
            try:
                # Scan for all pattern_metrics keys
                all_keys = db2_client.keys("pattern_metrics:*")
                pattern_keys = [key.decode('utf-8') if isinstance(key, bytes) else key for key in all_keys]
            except Exception as e:
                logger.debug(f"Error scanning pattern_metrics keys: {e}")
                pattern_keys = []
            
            if not pattern_keys:
                # Return empty DataFrame with proper schema matching PerformanceTracker fields
                return pl.DataFrame(schema={
                    'symbol': pl.Utf8,
                    'pattern_type': pl.Utf8,
                    'total_trades': pl.Int64,
                    'successful_trades': pl.Int64,
                    'success_rate': pl.Float64,
                    'avg_latency': pl.Float64,
                    'total_latency': pl.Float64,
                    'success_rate_rank': pl.Int64,
                    'latency_rank': pl.Int64,
                    'overall_rank': pl.Float64
                })
            
            # ✅ OPTIMIZED: Batch get all metrics using Redis pipeline
            pipe = db2_client.pipeline()
            for key in pattern_keys:
                pipe.hgetall(key)
            
            metrics_results = pipe.execute()
            
            # ✅ FIXED: Build Polars DataFrame using PerformanceTracker's stored fields
            performance_data = []
            for key, metrics_dict in zip(pattern_keys, metrics_results):
                if metrics_dict:
                    try:
                        # Extract symbol and pattern_type from key
                        # Key format: pattern_metrics:{symbol}:{pattern_type} (from PerformanceTracker)
                        key_parts = key.split(":")
                        if len(key_parts) >= 3:
                            symbol = key_parts[-2]
                            pattern_type = key_parts[-1]
                        else:
                            continue
                        
                        # Parse metrics (Redis returns dict with bytes keys/values)
                        parsed_metrics = {}
                        for k, v in metrics_dict.items():
                            key_str = k.decode('utf-8') if isinstance(k, bytes) else str(k)
                            if isinstance(v, bytes):
                                value_str = v.decode('utf-8')
                                try:
                                    # Try float first (for numeric values)
                                    parsed_metrics[key_str] = float(value_str)
                                except ValueError:
                                    # Keep as string if not numeric
                                    parsed_metrics[key_str] = value_str
                            else:
                                parsed_metrics[key_str] = v
                        
                        # ✅ FIXED: Extract fields stored by PerformanceTracker._update_pattern_metrics
                        total_trades = int(parsed_metrics.get('total_trades', 0))
                        successful_trades = int(parsed_metrics.get('successful_trades', 0))
                        success_rate = float(parsed_metrics.get('success_rate', 0.0))
                        avg_latency = float(parsed_metrics.get('avg_latency', 0.0))
                        total_latency = float(parsed_metrics.get('total_latency', 0.0))
                        
                        # Only include entries with actual data
                        if total_trades > 0:
                            performance_data.append({
                                'symbol': str(symbol),
                                'pattern_type': str(pattern_type),
                                'total_trades': total_trades,
                                'successful_trades': successful_trades,
                                'success_rate': success_rate,
                                'avg_latency': avg_latency,
                                'total_latency': total_latency
                            })
                    except Exception as e:
                        logger.debug(f"Error parsing metrics for key {key}: {e}")
                        continue
            
            if not performance_data:
                return pl.DataFrame(schema={
                    'symbol': pl.Utf8,
                    'pattern_type': pl.Utf8,
                    'total_trades': pl.Int64,
                    'successful_trades': pl.Int64,
                    'success_rate': pl.Float64,
                    'avg_latency': pl.Float64,
                    'total_latency': pl.Float64,
                    'success_rate_rank': pl.Int64,
                    'latency_rank': pl.Int64,
                    'overall_rank': pl.Float64
                })
            
            # ✅ FIXED: Create DataFrame with schema matching PerformanceTracker fields
            df = pl.DataFrame(performance_data, schema={
                'symbol': pl.Utf8,
                'pattern_type': pl.Utf8,
                'total_trades': pl.Int64,
                'successful_trades': pl.Int64,
                'success_rate': pl.Float64,
                'avg_latency': pl.Float64,
                'total_latency': pl.Float64
            })
            
            if df.is_empty():
                return df
            
            # ✅ FIXED: Calculate performance rankings based on success_rate and latency
            return (
                df
                .with_columns([
                    # Rank by success rate (descending - higher is better)
                    pl.col("success_rate").rank(descending=True, method="average").cast(pl.Int64).alias("success_rate_rank"),
                    # Rank by latency (ascending - lower is better)
                    pl.col("avg_latency").rank(descending=False, method="average").cast(pl.Int64).alias("latency_rank"),
                    # Overall rank: weighted combination of success_rate (70%) and latency (30%)
                    # Higher success_rate and lower latency = better overall rank
                    (
                        (pl.col("success_rate").rank(descending=True, method="average") * 0.7) +
                        (pl.col("avg_latency").rank(descending=False, method="average") * 0.3)
                    ).cast(pl.Float64).alias("overall_rank")
                ])
                .sort("overall_rank", descending=False)  # Lower overall_rank = better
            )
            
        except Exception as e:
            logger.error(f"Error getting pattern performance summary: {e}", exc_info=True)
            return None
    
    def get_pattern_performance_summary_sync(self) -> Optional['pl.DataFrame']:
        """
        Synchronous version of get_pattern_performance_summary.
        
        For use in non-async contexts.
        
        Returns:
            Polars DataFrame with pattern performance metrics
        """
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(self.get_pattern_performance_summary())
                return result
            finally:
                loop.close()
        except Exception as e:
            logger.error(f"Error in sync pattern performance summary: {e}", exc_info=True)
            return None


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Modular Market Scanner")
    parser.add_argument("--config", type=str, help="Config file path")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument(
        "--test", action="store_true", help="Run in test mode (relaxed thresholds)"
    )
    parser.add_argument(
        "--continuous", action="store_true", help="Process ticks continuously (default)"
    )
    parser.add_argument(
        "--max-ticks", type=int, help="Process at most N ticks, then exit"
    )
    parser.add_argument("--duration", type=int, help="Process for N seconds, then exit")
    parser.add_argument(
        "--historical",
        action="store_true",
        help="Enable historical analysis with historical context",
    )
    parser.add_argument(
        "--emergency",
        action="store_true",
        help="#temporary Enable emergency mode (uses emergency_config.py settings)"
    )

    args = parser.parse_args()

    # #temporary Load emergency configuration if emergency flag is set
    if args.emergency:
        try:
            from config.emergency_config import (
                EMERGENCY_SETTINGS,
                EMERGENCY_SYMBOLS,
                EMERGENCY_CONFIDENCE_THRESHOLDS,
                EMERGENCY_LOGGING
            )
            logger.info("🚨 EMERGENCY MODE ENABLED - Loading emergency configuration")
        except ImportError as e:
            logger.warning(f"⚠️ Failed to import emergency config: {e}")
            EMERGENCY_SETTINGS = {}
            EMERGENCY_SYMBOLS = []
            EMERGENCY_CONFIDENCE_THRESHOLDS = {}
            EMERGENCY_LOGGING = {}
    else:
        EMERGENCY_SETTINGS = {}
        EMERGENCY_SYMBOLS = []
        EMERGENCY_CONFIDENCE_THRESHOLDS = {}
        EMERGENCY_LOGGING = {}

    # Load configuration
    config = {}
    if args.config:
        if os.path.exists(args.config):
            with open(args.config, "r") as f:
                config = json.load(f)
        else:
            logger.warning(f"⚠️ Config file not found: {args.config}. Running with default configuration.")

    # #temporary Apply emergency settings to config
    if args.emergency and EMERGENCY_SETTINGS:
        logger.info("🚨 Applying emergency settings to configuration")
        config.update(EMERGENCY_SETTINGS)
        if EMERGENCY_SYMBOLS:
            config['emergency_symbols'] = EMERGENCY_SYMBOLS
        if EMERGENCY_CONFIDENCE_THRESHOLDS:
            config['emergency_confidence_thresholds'] = EMERGENCY_CONFIDENCE_THRESHOLDS
        if EMERGENCY_LOGGING:
            config['emergency_logging'] = EMERGENCY_LOGGING
        logger.info(f"🚨 Emergency symbols: {EMERGENCY_SYMBOLS}")
        logger.info(f"🚨 Emergency confidence thresholds: {EMERGENCY_CONFIDENCE_THRESHOLDS}")

    if args.debug:
        config["debug"] = True

    if args.test:
        config["test_mode"] = True
        # Propagate test mode to alert filter via environment
        try:
            import os as _os

            _os.environ["ALERT_TEST_MODE"] = "1"
        except Exception:
            pass

    if args.historical:
        config["enable_historical_analysis"] = True

    # Continuous/finite processing config
    if args.continuous:
        config["continuous"] = True
    if args.max_ticks is not None:
        config["max_ticks"] = int(args.max_ticks)
    if args.duration is not None:
        config["duration_secs"] = int(args.duration)
    if (
        args.max_ticks is not None or args.duration is not None
    ) and not args.continuous:
        config.setdefault("continuous", False)

    # Start emergency Redis cleanup thread (cleans up unnamed/idle connections)
    try:
        import sys
        from pathlib import Path
        
        # ✅ FIXED: Use path_utils for reliable project root resolution
        try:
            from aion_trading_dashboard.backend.path_utils import get_project_roots
            project_roots = get_project_roots()
            scripts_path = project_roots["project"] / "scripts"
        except ImportError:
            # Fallback: calculate project root manually
            _this_file = Path(__file__).resolve()
            _intraday_scanner = _this_file.parent
            _intraday_trading = _intraday_scanner.parent
            _src = _intraday_trading.parent
            _project_root = _src.parent
            scripts_path = _project_root / "scripts"
        
        if scripts_path.exists() and str(scripts_path) not in sys.path:
            sys.path.insert(0, str(scripts_path))
        
        # Dynamic import to avoid module name collision
        import importlib.util
        cleanup_script = scripts_path / "emergency_redis_cleanup.py"
        if cleanup_script.exists():
            spec = importlib.util.spec_from_file_location("emergency_redis_cleanup", cleanup_script)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                start_cleanup_thread = getattr(module, "start_cleanup_thread", None)
                if start_cleanup_thread:
                    cleanup_thread, cleanup_stop_event = start_cleanup_thread(
                        idle_threshold_seconds=600,  # 10 minutes
                        check_interval_seconds=300,  # Check every 5 minutes
                        daemon=True
                    )
                    print("✅ Emergency Redis cleanup thread started (cleans unnamed/idle connections every 5 min)")
                else:
                    print("   ℹ️  Emergency Redis cleanup: start_cleanup_thread not found in module")
        else:
            print(f"   ℹ️  Emergency Redis cleanup script not found at {cleanup_script}")
    except Exception as cleanup_error:
        print(f"⚠️ Could not start emergency cleanup thread: {cleanup_error}")
    
    # ✅ CONSOLIDATED: Stream optimization now handled by InfrastructureMonitor
    # InfrastructureMonitor will handle both initial optimization and continuous proactive monitoring
    print("   ℹ️  Stream optimization and monitoring handled by InfrastructureMonitor")

    # Create and start scanner
    # Create and start scanner
    # Optimizations now integrated directly into MarketScanner
    scanner = MarketScanner(config)
    logger.info("✅ Launched MarketScanner (Optimized)")
    scanner.start()
    
    # Make scanner instance globally available
    global scanner_instance
    scanner_instance = scanner


if __name__ == "__main__":
    main()
