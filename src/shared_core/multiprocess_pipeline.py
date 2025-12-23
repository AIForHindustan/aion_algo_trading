from shared_core.redis_clients.redis_client import get_redis_client
# multiprocess_pipeline.py
import multiprocessing as mp
from multiprocessing import Queue, Process
from queue import Empty, Full
import time
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import struct
import json
import zlib
import os
import subprocess
import resource
import threading

import redis
from redis.connection import ConnectionPool, Connection, UnixDomainSocketConnection

# Your existing imports
from shared_core.redis_clients import UnifiedRedisManager
from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
from shared_core.volume_files.volume_computation_manager import get_volume_computation_manager
# ✅ FIXED: Import from write_first_cache (UnderlyingPriceService is alias to UniversalSymbolParser)
from shared_core.write_first_cache import WriteFirstCache, UnderlyingPriceServiceWithCache, UnderlyingPriceService, create_price_service

WORKER_REDIS_MAX_CONNECTIONS = int(os.environ.get("CALC_WORKER_MAX_CONNECTIONS", "10"))
WORKER_REDIS_SOCKET_TIMEOUT = float(os.environ.get("CALC_WORKER_SOCKET_TIMEOUT", "0.1"))
WORKER_REDIS_CONNECT_TIMEOUT = float(os.environ.get("CALC_WORKER_CONNECT_TIMEOUT", "0.3"))
WORKER_REDIS_DB = int(os.environ.get("CALC_WORKER_REDIS_DB", "1"))
WORKER_REDIS_HOST = os.environ.get("CALC_WORKER_REDIS_HOST", os.environ.get("REDIS_HOST", "localhost"))
WORKER_REDIS_PORT = int(os.environ.get("CALC_WORKER_REDIS_PORT", os.environ.get("REDIS_PORT", "6379")))
WORKER_REDIS_UNIX_SOCKET = os.environ.get("CALC_WORKER_REDIS_SOCKET", "/tmp/redis_trading.sock")

_WORKER_POOL_LOCK = threading.Lock()
_WORKER_REDIS_POOL: Optional[ConnectionPool] = None

def _get_worker_connection_pool() -> ConnectionPool:
    """Build a shared connection pool per worker process."""
    global _WORKER_REDIS_POOL
    if _WORKER_REDIS_POOL is not None:
        return _WORKER_REDIS_POOL
    with _WORKER_POOL_LOCK:
        if _WORKER_REDIS_POOL is not None:
            return _WORKER_REDIS_POOL
        pool_kwargs = {
            "db": WORKER_REDIS_DB,
            "max_connections": WORKER_REDIS_MAX_CONNECTIONS,
            "socket_timeout": WORKER_REDIS_SOCKET_TIMEOUT,
            "socket_connect_timeout": WORKER_REDIS_CONNECT_TIMEOUT,
            "decode_responses": True,
        }
        if WORKER_REDIS_UNIX_SOCKET and os.path.exists(WORKER_REDIS_UNIX_SOCKET):
            pool_kwargs["path"] = WORKER_REDIS_UNIX_SOCKET
            pool_kwargs["connection_class"] = UnixDomainSocketConnection
        else:
            pool_kwargs["host"] = WORKER_REDIS_HOST
            pool_kwargs["port"] = WORKER_REDIS_PORT
            pool_kwargs["connection_class"] = Connection
        _WORKER_REDIS_POOL = ConnectionPool(**pool_kwargs)
        logging.getLogger(__name__).info(
            "✅ Worker Redis pool initialized (db=%s, max_connections=%s, unix_socket=%s)",
            WORKER_REDIS_DB,
            WORKER_REDIS_MAX_CONNECTIONS,
            os.path.exists(WORKER_REDIS_UNIX_SOCKET),
        )
    return _WORKER_REDIS_POOL

class WorkerRedisManager:
    """Lightweight manager exposing get_client() for shared worker pool."""

    def __init__(self):
        self._client: Optional[redis.Redis] = None

    def get_client(self, db: int = WORKER_REDIS_DB) -> redis.Redis:
        if db != WORKER_REDIS_DB:
            raise ValueError(f"WorkerRedisManager only supports DB {WORKER_REDIS_DB}")
        if self._client is None:
            pool = _get_worker_connection_pool()
            self._client = redis.Redis(connection_pool=pool, decode_responses=True)
            try:
                self._client.client_setname(f"calc_worker_pool_{os.getpid()}")
            except Exception:
                pass
        return self._client

# Performance monitoring
try:
    from zerodha.crawlers.performance_monitor import PerformanceMonitor
except ImportError:
    PerformanceMonitor = None

# ==================== SYMBOL GUARD ====================
# ❌ REMOVED: sanitize_symbol() - We now use RedisKeyStandards.canonical_symbol() globally
# This ensures Single Source of Truth for symbol formatting.




# ==================== DATA STRUCTURES ====================
@dataclass
class RawPacket:
    """Raw packet from WebSocket"""
    data: bytes
    length: int
    receive_time: float
    packet_index: int

@dataclass
class ParsedTick:
    """Parsed tick without calculations"""
    symbol: str
    instrument_token: int
    segment: int
    last_price: float
    volume: int
    timestamp: float
    packet_type: str  # 'full', 'quote', 'ltp'
    raw_data: dict
    # Minimal metadata needed for calculations
    metadata: Dict[str, Any]

@dataclass
class CalculatedTick:
    """Tick with all calculations completed"""
    symbol: str
    data: Dict[str, Any]
    calculation_time: float

# ==================== CONFIGURATION ====================
class M4PipelineConfig:
    """M4 Pro/Max Optimized Pipeline Configuration"""
    
    @staticmethod
    def get_config():
        return {
            # ✅ STAGE 1: Parser Process
            'parser': {
                'num_workers': 1,  # Single dedicated parser
                'queue_size': 10000,
            },
            
            # ✅ STAGE 2: Calculation Workers
            'calculation': {
                'num_workers': 10,  # ✅ M4 Pro: 10 workers
                'queue_size': 10000,
                'max_symbols_per_worker': 75000,
            },
            
            # ✅ STAGE 3: Storage Process
            'storage': {
                'num_workers': 1,   # Single dedicated storage
                'batch_size': 50,   # ✅ 50 ticks per pipeline
                'max_wait_ms': 100,
                'queue_size': 10000,
            },
            
            # ✅ REDIS: Shared config for all stages
            'redis': {
                'unix_socket_path': '/tmp/redis_trading.sock',  # ✅ Unix socket
                'db': 1,
                'socket_timeout': 0.1,
                'max_connections': 50,
            },
            
            # ✅ INTER-PROCESS COMMUNICATION
            'ipc': {
                'queue_maxsize': 10000,        # Backpressure limit
                'poison_pill_timeout': 5,      # Shutdown timeout
                'queue_put_timeout': 0.01,     # Non-blocking puts
            }
        }

# ==================== STAGE 1: PARSER PROCESS ====================
class ParserProcess(Process):
    """
    Stage 1: Dedicated process for binary parsing only
    - Parses WebSocket messages into RawPacket objects
    - Minimal processing, just extract basic fields
    - High throughput (CPU-bound)
    """
    
    def __init__(self, 
                 input_queue: Queue,  # Raw WebSocket messages
                 output_queue: Queue,  # Parsed ticks
                 instrument_info: Dict[int, Dict],
                 indices_metadata: Dict[str, Any],
                 num_workers: int = 2,
                 worker_config: Optional[Dict] = None):
        super().__init__(name="ParserProcess")
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.instrument_info = instrument_info
        self.indices_metadata = indices_metadata
        self.num_workers = num_workers
        self.config = worker_config
        self.running = True
        
        # Initialize metadata cache
        self._init_metadata_cache()
        
        # Performance tracking
        self.parsed_count = 0
        self.last_stats_time = time.time()
        
    def _init_metadata_cache(self):
        """Initialize metadata lookup tables"""
        self.token_to_metadata = {}
        for token, info in self.instrument_info.items():
            symbol = info.get('symbol') or info.get('tradingsymbol')
            if symbol:
                self.token_to_metadata[token] = {
                    'symbol': symbol,
                    'segment': info.get('segment'),
                    'exchange': info.get('exchange'),
                    'instrument_type': info.get('instrument_type'),
                    'strike': info.get('strike'),
                    'expiry': info.get('expiry')
                }
    
    def run(self):
        """Main parsing loop"""
        # ✅ SYNC LOGGING
        pid = os.getpid()
        logging.basicConfig(
            level=logging.DEBUG,
            format=f'%(asctime)s - PARSER-{pid} - %(name)s - %(levelname)s - %(message)s',
            force=True
        )
        logging.getLogger().setLevel(logging.DEBUG)
        
        logging.info(f"🚀 ParserProcess started (PID: {pid})")
        
        while self.running:
            try:
                # Get raw message with timeout
                message_data = self.input_queue.get(timeout=0.1)
                if message_data is None:  # Poison pill
                    break
                    
                # Parse message
                parsing_start = time.perf_counter()
                logging.debug(f"[WS_RAW] Received {len(message_data)} bytes")
                
                parsed_ticks = self._parse_message_fast(message_data)
                
                logging.debug(f"[PARSED] Extracted {len(parsed_ticks)} ticks in {(time.perf_counter() - parsing_start)*1000:.2f}ms")
                
                # Send to output queue
                for tick in parsed_ticks:
                    self.output_queue.put(tick)
                    self.parsed_count += 1
                
                # Log stats every 5 seconds
                current_time = time.time()
                if current_time - self.last_stats_time > 5:
                    self._log_stats()
                    self.last_stats_time = current_time
                    
            except Empty:
                continue
            except Exception as e:
                logging.error(f"Parser error: {e}")
    
    def _parse_message_fast(self, message_data: bytes) -> List[ParsedTick]:
        """Fast parsing without calculations"""
        ticks = []
        
        if not message_data or len(message_data) < 4:
            return ticks
        
        try:
            # Check if compressed
            if self._is_compressed(message_data):
                try:
                    message_data = zlib.decompress(message_data)
                except:
                    return ticks
            
            # Parse number of packets
            num_packets = int.from_bytes(message_data[0:2], byteorder="big", signed=False)
            offset = 2
            
            for _ in range(num_packets):
                if offset + 2 > len(message_data):
                    break
                
                packet_length = int.from_bytes(
                    message_data[offset:offset+2], byteorder="big", signed=False
                )
                
                if packet_length <= 0 or offset + 2 + packet_length > len(message_data):
                    break
                
                packet_data = message_data[offset+2:offset+2+packet_length]
                
                # Parse based on length
                if packet_length == 184:
                    tick = self._parse_184byte_fast(packet_data)
                elif packet_length == 32:
                    tick = self._parse_32byte_fast(packet_data)
                elif packet_length == 8:
                    tick = self._parse_8byte_fast(packet_data)
                else:
                    offset += 2 + packet_length
                    continue
                
                if tick:
                    ticks.append(tick)
                
                offset += 2 + packet_length
                
        except Exception as e:
            logging.error(f"Parse error: {e}")
        
        return ticks
    
    def _parse_184byte_fast(self, data: bytes) -> Optional[ParsedTick]:
        """Fast 184-byte parsing"""
        try:
            if len(data) != 184:
                return None
            
            # Extract token (first 4 bytes)
            instrument_token = struct.unpack(">i", data[0:4])[0]
            segment = instrument_token & 0xFF
            
            # Get metadata
            metadata = self.token_to_metadata.get(instrument_token)
            if not metadata:
                return None
            
            # Parse basic fields
            divisor = 100.0 if segment in [3, 6] else 100.0  # Simplified
            fields = struct.unpack(">11I", data[0:44])
            
            return ParsedTick(
                symbol=metadata['symbol'],
                instrument_token=instrument_token,
                segment=segment,
                last_price=fields[1] / divisor if divisor else 0,
                volume=fields[4],
                timestamp=time.time(),
                packet_type='full',
                raw_data={
                    'ohlc': {
                        'open': fields[7] / divisor if divisor else 0,
                        'high': fields[8] / divisor if divisor else 0,
                        'low': fields[9] / divisor if divisor else 0,
                        'close': fields[10] / divisor if divisor else 0,
                    }
                },
                metadata=metadata
            )
            
        except Exception:
            return None
    
    def _parse_32byte_fast(self, data: bytes) -> Optional[ParsedTick]:
        """Fast 32-byte parsing"""
        try:
            if len(data) != 32:
                return None
            
            instrument_token = struct.unpack(">i", data[0:4])[0]
            segment = instrument_token & 0xFF
            metadata = self.token_to_metadata.get(instrument_token)
            
            if not metadata:
                return None
            
            divisor = 100.0
            last_price = struct.unpack(">I", data[4:8])[0] / divisor
            
            return ParsedTick(
                symbol=metadata['symbol'],
                instrument_token=instrument_token,
                segment=segment,
                last_price=last_price,
                volume=0,
                timestamp=time.time(),
                packet_type='quote',
                raw_data={},
                metadata=metadata
            )
            
        except Exception:
            return None
    
    def _parse_8byte_fast(self, data: bytes) -> Optional[ParsedTick]:
        """Fast 8-byte parsing"""
        try:
            if len(data) != 8:
                return None
            
            instrument_token = struct.unpack(">i", data[0:4])[0]
            segment = instrument_token & 0xFF
            metadata = self.token_to_metadata.get(instrument_token)
            
            if not metadata:
                return None
            
            divisor = 100.0
            last_price = struct.unpack(">I", data[4:8])[0] / divisor
            
            return ParsedTick(
                symbol=metadata['symbol'],
                instrument_token=instrument_token,
                segment=segment,
                last_price=last_price,
                volume=0,
                timestamp=time.time(),
                packet_type='ltp',
                raw_data={},
                metadata=metadata
            )
            
        except Exception:
            return None
    
    def _is_compressed(self, data: bytes) -> bool:
        return len(data) > 2 and data[0] == 0x78 and data[1] in [0x01, 0x9C, 0xDA]
    
    def _log_stats(self):
        logging.info(f"📊 Parser: {self.parsed_count} ticks parsed")
        
    def stop(self):
        self.running = False

# ==================== STAGE 2: CALCULATION WORKER POOL ====================
class CalculationWorker(Process):
    """
    Stage 2: Worker process for calculations
    Each worker handles:
    - Volume calculations
    - Greek calculations
    - Microstructure
    - USDINR indicators
    """
    
    def __init__(self,
                 input_queue: Queue,
                 output_queue: Queue,
                 worker_id: int,
                 worker_config: Optional[Dict] = None):
        super().__init__(name=f"CalcWorker-{worker_id}")
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.worker_id = worker_id
        self.config = worker_config
        self.running = True
        
        # Initialize services in worker process
        self.calculation_service = None
        self.price_service = None
        self.volume_manager = None
        
        self.processed_count = 0

    def _set_m4_performance_mode(self):
        """Set M4 Pro to performance cores (Apple Silicon Optimization)"""
        try:
            pid = os.getpid()
            # 1. Force to performance cores using taskpolicy
            subprocess.run(['taskpolicy', '-p', str(pid), '-c', 'performance'], check=False, capture_output=True)
            
            # 2. Increase file descriptor limits (vital for high concurrency)
            resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))
            
            # 3. Disable App Nap (Global default - idempotent)
            subprocess.run(['defaults', 'write', '-g', 'NSAppSleepDisabled', '-bool', 'YES'], check=False, capture_output=True)
            
            logging.info(f"🚀 Worker-{self.worker_id} (PID {pid}): M4 Performance Mode Enabled")
        except Exception as e:
            logging.warning(f"⚠️ Worker-{self.worker_id}: Performance tuning failed: {e}")
        
    def _init_services(self):
        """Initialize services in worker process - ONCE per worker, not per-tick"""
        from shared_core.redis_clients import UnifiedRedisManager
        # ✅ FIXED: Import from write_first_cache
        from shared_core.write_first_cache import WriteFirstCache, UnderlyingPriceServiceWithCache, UnderlyingPriceService
        from shared_core.calculation_service import CalculationService
        
        # Create Redis connection for this worker via shared pool
        self.redis_manager = WorkerRedisManager()
        self.redis_client = self.redis_manager.get_client(WORKER_REDIS_DB)
        
        # ✅ CRITICAL: Connect IndexManager to Redis for fallback price lookups
        # This enables workers to read index:NSENIFTY50, index:NSENIFTYBANK from gift_nifty_gap.py
        from shared_core.index_manager import index_manager
        index_manager.connect_redis(self.redis_client)
        logging.info(f"✅ Worker-{self.worker_id} IndexManager connected to Redis for fallback lookups")

        
        # 1. Create cache (once per worker, not per tick!)
        self.cache = WriteFirstCache()
        
        # 2. Create price service (UniversalSymbolParser singleton - doesn't take redis_client)
        self.price_service = create_price_service()
        
        # 3. Create price service wrapper with cache
        self.price_service_wrapper = UnderlyingPriceServiceWithCache(self.price_service, self.cache)
        
        # 4. Create calculation service WITH redis_client only (price_service is passed to execute_pipeline)
        self.calculation_service = CalculationService(
            redis_client=self.redis_client
        )
        
        # 5. Edge calculator factory (per-symbol, loaded lazily during calculation)
        self._edge_calculator_factory = None
        try:
            from intraday_trading.intraday_scanner.calculations_edge import get_edge_calculator
            self._edge_calculator_factory = get_edge_calculator
            logging.info(f"✅ Worker-{self.worker_id} edge calculator factory loaded")
        except ImportError as e:
            logging.debug(f"⚠️ Worker-{self.worker_id} edge calculator not available: {e}")
        
        # 6. Initialize volume manager
        self.volume_manager = get_volume_computation_manager(redis_client=self.redis_manager)
        
        logging.info(f"✅ Worker-{self.worker_id} services initialized")
        
    def run(self):
        """Worker main loop"""
        # ✅ SYNC LOGGING (M4 Pro Visibility Restoration)
        pid = os.getpid()
        logging.basicConfig(
            level=logging.DEBUG,
            format=f'%(asctime)s - WORKER-{pid} - %(name)s - %(levelname)s - %(message)s',
            force=True
        )
        
        # Apply DEBUG levels to critical modules for visibility
        debug_modules = [
            'shared_core.multiprocess_pipeline',
            'shared_core.redis_clients.unified_data_storage',
            'shared_core.calculation_service',
            'shared_core.index_manager'
        ]
        logging.getLogger().setLevel(logging.DEBUG)
        for module in debug_modules:
            logging.getLogger(module).setLevel(logging.DEBUG)

        logging.info(f"🚀 CalculationWorker-{self.worker_id} started (PID: {pid})")
        
        # Apply M4 Performance Optimizations
        self._set_m4_performance_mode()
        
        # Initialize services in worker process
        self._init_services()
        
        while self.running:
            try:
                # Get parsed tick
                parsed_tick = self.input_queue.get(timeout=0.1)
                if parsed_tick is None:  # Poison pill
                    break
                
                # Perform calculations
                logging.debug(f"[WORKFLOW_START] {getattr(parsed_tick, 'symbol', 'UNKNOWN')} - Worker: {self.worker_id}")
                calculated_tick = self._calculate_tick(parsed_tick)
                logging.debug(f"[WORKFLOW_END] {getattr(parsed_tick, 'symbol', 'UNKNOWN')} - Success: {calculated_tick is not None}")
                
                if calculated_tick:
                    self.output_queue.put(calculated_tick)
                    self.processed_count += 1
                    
            except Empty:
                continue
            except Exception as e:
                logging.error(f"Worker-{self.worker_id} error: {e}")
    
    def _calculate_tick(self, parsed_tick: ParsedTick) -> Optional[CalculatedTick]:
        """Perform all calculations on a tick"""
        start_time = time.perf_counter()
        
        try:
            # Convert to dict
            tick_data = {
                'symbol': parsed_tick.symbol,
                'instrument_token': parsed_tick.instrument_token,
                'segment': parsed_tick.segment,
                'last_price': parsed_tick.last_price,
                'volume': parsed_tick.volume,
                'timestamp': parsed_tick.timestamp,
                'packet_type': parsed_tick.packet_type,
                **parsed_tick.raw_data
            }
            
            # ✅ CRITICAL: Enforce canonical symbol format (NSE:NIFTY → NFONIFTY)
            tick_data['symbol'] = RedisKeyStandards.canonical_symbol(tick_data['symbol'])
            
            # Add metadata
            tick_data.update(parsed_tick.metadata)
            
            # ✅ Use pre-initialized price service wrapper (created once in _init_services)
            # Store tick in cache for write-first pattern
            self.cache.store_tick_for_calculations(parsed_tick.symbol, tick_data)
            
            # Load edge calculator for this symbol (lazy, cached per-symbol)
            if self._edge_calculator_factory:
                try:
                    edge_calc = self._edge_calculator_factory(parsed_tick.symbol, self.redis_client)
                    self.calculation_service.edge_calculator = edge_calc
                except Exception as e:
                    logging.debug(f"Edge calc error for {parsed_tick.symbol}: {e}")
            
            # ✅ CRITICAL: Compute volume metrics using VolumeComputationManager
            if self.volume_manager:
                try:
                    volume_metrics = self.volume_manager.calculate_volume_metrics(
                        parsed_tick.symbol,
                        tick_data
                    )
                    tick_data['volume_ratio'] = float(volume_metrics.get('volume_ratio', 0.0) or 0.0)
                except Exception as e:
                    logging.debug(f"Volume ratio error for {parsed_tick.symbol}: {e}")
                    tick_data['volume_ratio'] = 0.0
            
            # Execute calculation pipeline with pre-initialized wrapper
            tick_data = self.calculation_service.execute_pipeline(tick_data, self.price_service_wrapper)
            
            calculation_time = time.perf_counter() - start_time
            
            return CalculatedTick(
                symbol=parsed_tick.symbol,
                data=tick_data,
                calculation_time=calculation_time
            )
            
        except Exception as e:
            logging.error(f"Calculation error for {parsed_tick.symbol}: {e}")
            return None
    
    def stop(self):
        self.running = False

# ==================== STAGE 3: STORAGE PROCESS ====================
class StorageProcess(Process):
    """
    Stage 3: Dedicated storage process
    - Batches writes to Redis
    - Uses connection pooling
    - Handles compression
    """
    
    def __init__(self,
                 input_queue: Queue,
                 batch_size: int = 50,
                 max_wait_ms: int = 100):
        super().__init__(name="StorageProcess")
        self.input_queue = input_queue
        self.batch_size = batch_size
        self.max_wait_ms = max_wait_ms
        self.running = True
        
        self.batch = []
        self.last_flush = time.time()
        self.stored_count = 0
        
        # Redis connection will be initialized in run()
        self.redis_client = None
        self.logger = logging.getLogger(self.name)
        
    def _init_redis(self):
        """Initialize Redis connection in storage process"""
        from shared_core.redis_clients import UnifiedRedisManager
        from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
        
        self.redis_client = get_redis_client(
            process_name="storage_process",
            db=1,
            port=6379,
            max_connections=10
        )
        self.key_builder = DatabaseAwareKeyBuilder
        
    def run(self):
        """Storage main loop"""
        logging.info(f"🚀 StorageProcess started (PID: {mp.current_process().pid})")
        
        # Initialize Redis in this process
        self._init_redis()
        
        while self.running:
            try:
                # Get calculated tick with timeout
                calculated_tick = self.input_queue.get(timeout=self.max_wait_ms / 1000.0)
                if calculated_tick is None:  # Poison pill
                    self._flush_batch()  # Flush any remaining
                    break
                
                # Add to batch
                self.batch.append(calculated_tick)
                
                # Check if should flush
                should_flush = (
                    len(self.batch) >= self.batch_size or
                    (time.time() - self.last_flush) * 1000 > self.max_wait_ms
                )
                
                if should_flush:
                    self._flush_batch()
                    
            except Empty:
                # Timeout - flush if we have any data
                if self.batch:
                    self._flush_batch()
                continue
            except Exception as e:
                logging.error(f"Storage error: {e}")
    
    def _flush_batch(self):
        """Flush batch to Redis using pipeline - WITH VOLUME_RATIO FIX"""
        if not self.batch or not self.redis_client:
            return
        
        start_time = time.perf_counter()
        
        try:
            pipe = self.redis_client.pipeline()
            
            for tick in self.batch:
                symbol = RedisKeyStandards.canonical_symbol(tick.symbol)  # ✅ CRITICAL: Canonicalize symbol before Redis storage
                data = tick.data
                
                # ==================== CRITICAL FIX ====================
                # 1. GET VOLUME_RATIO FROM REDIS IF MISSING/0.0 IN TICK
                volume_ratio = data.get('volume_ratio', 0.0)
                
                if not volume_ratio or volume_ratio == 0.0:
                    # Try to get from ind:{symbol} HASH
                    hash_key = DatabaseAwareKeyBuilder.live_indicator_hash(symbol)
                    redis_volume_ratio = self.redis_client.hget(hash_key, 'volume_ratio')
                    if redis_volume_ratio:
                        try:
                            volume_ratio = float(redis_volume_ratio)
                            data['volume_ratio'] = volume_ratio  # Update tick data too
                            self.logger.debug(f"✅ [STREAM_FIX] {symbol} - Retrieved volume_ratio={volume_ratio} from Redis")
                        except (ValueError, TypeError):
                            pass
                
                # 2. BUILD STREAM DATA - ALL ZERODHA RAW + COMPUTED FIELDS
                stream_data = {
                    # === CORE IDENTIFIERS ===
                    'symbol': symbol,
                    'instrument_token': data.get('instrument_token'),
                    'instrument_type': data.get('instrument_type'),
                    'exchange': data.get('exchange'),
                    'timestamp': data.get('timestamp', time.time()),
                    'exchange_timestamp': data.get('exchange_timestamp'),
                    
                    # === ZERODHA RAW TICK DATA ===
                    'last_price': data.get('last_price'),
                    'last_quantity': data.get('last_quantity'),
                    'average_price': data.get('average_price'),
                    'volume': data.get('volume'),
                    'buy_quantity': data.get('buy_quantity'),
                    'sell_quantity': data.get('sell_quantity'),
                    'open_interest': data.get('open_interest'),
                    'change': data.get('change'),
                    'last_trade_time': data.get('last_trade_time'),
                    'oi_day_high': data.get('oi_day_high'),
                    'oi_day_low': data.get('oi_day_low'),
                    'oi_day_open': data.get('oi_day_open'),
                    'oi_day_close': data.get('oi_day_close'),
                    'oi_day_change': data.get('oi_day_change'),
                    'upper_circuit': data.get('upper_circuit'),
                    'lower_circuit': data.get('lower_circuit'),
                    
                    # === OHLC DATA ===
                    'open': data.get('open') or (data.get('ohlc', {}).get('open') if isinstance(data.get('ohlc'), dict) else None),
                    'high': data.get('high') or (data.get('ohlc', {}).get('high') if isinstance(data.get('ohlc'), dict) else None),
                    'low': data.get('low') or (data.get('ohlc', {}).get('low') if isinstance(data.get('ohlc'), dict) else None),
                    'close': data.get('close') or (data.get('ohlc', {}).get('close') if isinstance(data.get('ohlc'), dict) else None),
                    
                    # === COMPUTED VOLUME METRICS ===
                    'volume_ratio': volume_ratio,  # ← CRITICAL: Always include
                    'incremental_volume': data.get('incremental_volume'),
                    'bucket_incremental_volume': data.get('bucket_incremental_volume'),
                    'cumulative_volume': data.get('cumulative_volume'),
                    'bucket_cumulative_volume': data.get('bucket_cumulative_volume'),
                    'zerodha_cumulative_volume': data.get('zerodha_cumulative_volume'),
                    'normalized_volume': data.get('normalized_volume'),
                    'volume_spike': data.get('volume_spike'),
                    'volume_trend': data.get('volume_trend'),
                    
                    # === COMPUTED PRICE METRICS ===
                    'price_change': data.get('price_change'),
                    'price_change_pct': data.get('price_change_pct'),
                    'price_move': data.get('price_move'),
                    'price_movement': data.get('price_movement'),
                    'net_change': data.get('net_change'),
                    'vwap': data.get('vwap'),
                    
                    # === UNDERLYING/SPOT ===
                    'underlying_price': data.get('underlying_price'),
                    'underlying_symbol': data.get('underlying_symbol'),
                    'spot_price': data.get('spot_price'),
                    
                    # === GREEKS (COMPUTED) ===
                    'delta': data.get('delta'),
                    'gamma': data.get('gamma'),
                    'theta': data.get('theta'),
                    'vega': data.get('vega'),
                    'rho': data.get('rho'),
                    'iv': data.get('iv') or data.get('implied_volatility'),
                    'implied_volatility': data.get('implied_volatility') or data.get('iv'),
                    'gamma_exposure': data.get('gamma_exposure'),
                    
                    # === MICROSTRUCTURE INDICATORS (COMPUTED) ===
                    'microprice': data.get('microprice'),
                    'order_flow_imbalance': data.get('order_flow_imbalance'),
                    'order_book_imbalance': data.get('order_book_imbalance'),
                    'cumulative_volume_delta': data.get('cumulative_volume_delta'),
                    'spread_absolute': data.get('spread_absolute'),
                    'spread_bps': data.get('spread_bps'),
                    'depth_imbalance': data.get('depth_imbalance'),
                    'best_bid_price': data.get('best_bid_price'),
                    'best_ask_price': data.get('best_ask_price'),
                    'best_bid_size': data.get('best_bid_size'),
                    'best_ask_size': data.get('best_ask_size'),
                    'best_bid_quantity': data.get('best_bid_quantity'),
                    'best_ask_quantity': data.get('best_ask_quantity'),
                    'total_bid_depth': data.get('total_bid_depth'),
                    'total_ask_depth': data.get('total_ask_depth'),
                    'weighted_bid': data.get('weighted_bid'),
                    'weighted_ask': data.get('weighted_ask'),
                    'bid_ask_ratio': data.get('bid_ask_ratio'),
                    'order_imbalance': data.get('order_imbalance'),
                    'order_count_imbalance': data.get('order_count_imbalance'),
                    
                    # === TECHNICAL INDICATORS ===
                    'rsi': data.get('rsi'),
                    'atr': data.get('atr'),
                    'macd': data.get('macd'),
                    'macd_signal': data.get('macd_signal'),
                    'macd_histogram': data.get('macd_histogram'),
                    'ema_9': data.get('ema_9'),
                    'ema_21': data.get('ema_21'),
                    'sma_20': data.get('sma_20'),
                    'bollinger_upper': data.get('bollinger_upper'),
                    'bollinger_lower': data.get('bollinger_lower'),
                    'bollinger_mid': data.get('bollinger_mid'),
                    'vwap': data.get('vwap'),
                    'volume': data.get('volume'),
                    'volume_ratio': data.get('volume_ratio'),
                    
                    # === REGIME/VIX INDICATORS ===
                    'buy_pressure': data.get('buy_pressure'),
                    'vix_level': data.get('vix_level'),
                    'vix_value': data.get('vix_value'),
                    'vix': data.get('vix'),
                    'volatility_regime': data.get('volatility_regime'),
                    
                    # === CROSS-ASSET METRICS ===
                    'usdinr_sensitivity': data.get('usdinr_sensitivity'),
                    'usdinr_correlation': data.get('usdinr_correlation'),
                    'usdinr_theoretical_price': data.get('usdinr_theoretical_price'),
                    'usdinr_basis': data.get('usdinr_basis'),
                    
                    # === OPTION METADATA ===
                    'option_type': data.get('option_type'),
                    'strike': data.get('strike'),
                    'expiry': data.get('expiry'),
                    'lot_size': data.get('lot_size'),
                    'tick_size': data.get('tick_size'),
                    'risk_free_rate': data.get('risk_free_rate'),
                }
                
                # 3. ENRICH WITH OTHER INDICATORS FROM REDIS
                try:
                    # Get all hash fields for this symbol
                    hash_key = DatabaseAwareKeyBuilder.live_indicator_hash(symbol)
                    all_indicators = self.redis_client.hgetall(hash_key)
                    if all_indicators:
                        for field, value in all_indicators.items():
                            field_str = field.decode('utf-8') if isinstance(field, bytes) else field
                            value_str = value.decode('utf-8') if isinstance(value, bytes) else value
                            
                            # Add important indicators to stream
                            if field_str in ['price_change_pct', 'open_interest', 'microprice', 
                                            'spread_bps', 'rsi', 'atr', 'vwap']:
                                try:
                                    stream_data[field_str] = float(value_str)
                                except (ValueError, TypeError):
                                    stream_data[field_str] = value_str
                except Exception as e:
                    self.logger.debug(f"⚠️ Stream enrichment failed for {symbol}: {e}")
                
                # 4. PUBLISH TO STREAM
                stream_key = "ticks:intraday:processed"
                clean_stream_data = {}
                for field, value in stream_data.items():
                    if value is None:
                        continue
                    if isinstance(value, (dict, list)):
                        clean_stream_data[field] = json.dumps(value)
                    elif isinstance(value, bool):
                        clean_stream_data[field] = int(value)
                    else:
                        clean_stream_data[field] = value
                pipe.xadd(stream_key, clean_stream_data, maxlen=1000, approximate=True)
                
                # 5. STORE IN HASH (existing code)
                hash_mapping = {}
                for k, v in data.items():
                    if v is None:
                        continue
                    if isinstance(v, (dict, list)):
                        hash_mapping[k] = json.dumps(v)
                    elif isinstance(v, bool):
                        hash_mapping[k] = int(v)
                    else:
                        hash_mapping[k] = str(v)
                
                if hash_mapping:
                    ind_hash_key = DatabaseAwareKeyBuilder.live_indicator_hash(symbol)
                    pipe.hset(ind_hash_key, mapping=hash_mapping)
                    pipe.expire(ind_hash_key, 300)
                
                # Store Greeks (basic + advanced)
                greek_fields = ["delta", "gamma", "theta", "vega", "rho", "iv", "implied_volatility", "charm", "vanna", "volga", "speed", "zomma", "color", "vomma", "gamma_exposure"]
                greeks = {field: data[field] for field in greek_fields if field in data and data[field] is not None}
                if greeks:
                    greeks_key = DatabaseAwareKeyBuilder.live_greeks_hash(symbol)
                    pipe.hset(greeks_key, mapping=greeks)
                    pipe.expire(greeks_key, 300)
            
            pipe.execute()
            
            # Log success
            elapsed_us = (time.perf_counter() - start_time) * 1000000
            self.logger.info(
                f"✅ [STREAM_FLUSH] {len(self.batch)} ticks, "
                f"volume_ratio included: {'YES' if volume_ratio else 'NO'}, "
                f"{elapsed_us/1000:.2f}ms"
            )
            
            # Update stats
            batch_size = len(self.batch)
            self.stored_count += batch_size
            self.batch.clear()
            self.last_flush = time.time()
                
        except Exception as exc:
            self.logger.error(f"❌ [STREAM_FLUSH] Batch write failed: {exc}")
            # Clear batch on error to prevent memory leak
            self.batch.clear()
    
    def stop(self):
        self.running = False
        self._flush_batch()

# ==================== MAIN PIPELINE COORDINATOR ====================
class MultiProcessPipeline:
    """
    Main coordinator for the multi-process pipeline
    """
    
    def __init__(self,
                 instrument_info: Dict[int, Dict],
                 indices_metadata: Dict[str, Any],
                 num_calc_workers: int = None,
                 worker_config: Optional[Dict] = None):
        
        # ✅ Load Config
        self.config = worker_config or M4PipelineConfig.get_config()
        self.instrument_info = instrument_info
        self.indices_metadata = indices_metadata
        
        # Use config for worker count if not overridden
        self.num_calc_workers = num_calc_workers or self.config['calculation']['num_workers']
        
        # ✅ Initialize queues with M4-optimized sizes from IPC config
        ipc_config = self.config['ipc']
        self.raw_queue = Queue(maxsize=ipc_config['queue_maxsize'])
        self.parsed_queue = Queue(maxsize=ipc_config['queue_maxsize'])
        self.calculated_queue = Queue(maxsize=ipc_config['queue_maxsize'])
        
        # ✅ Create Processes (Instantiate but don't start)
        self.parser = ParserProcess(
            input_queue=self.raw_queue,
            output_queue=self.parsed_queue,
            instrument_info=self.instrument_info,
            indices_metadata=self.indices_metadata,
            worker_config=self.config  # ✅ Pass config
        )
        
        self.calc_workers = []
        for i in range(self.num_calc_workers):
            worker = CalculationWorker(
                input_queue=self.parsed_queue,
                output_queue=self.calculated_queue,
                worker_id=i,
                worker_config=self.config  # ✅ Pass config
            )
            self.calc_workers.append(worker)
            
        self.storage = None
        self.running = False
        self.logger = logging.getLogger(__name__)
        
    def start(self):
        """Start all processes"""
        self.logger.info(f"🚀 Starting multi-process pipeline with {self.num_calc_workers} calculation workers")
        
        # Storage creation remains in start() as it was not pre-created in the user snippet,
        # but we should use config values.
        storage_config = self.config['storage']
        self.storage = StorageProcess(
            input_queue=self.calculated_queue,
            batch_size=storage_config['batch_size'],
            max_wait_ms=storage_config['max_wait_ms']
        )
        
        # Start all processes
        self.parser.start()
        for worker in self.calc_workers:
            worker.start()
        self.storage.start()
        
        self.running = True
        self.logger.info("✅ All pipeline processes started")
    
    def feed_message(self, message_data: bytes):
        """Feed WebSocket message to pipeline."""
        if not self.running:
            return False
        try:
            self.raw_queue.put_nowait(message_data)
            return True
        except Full:
            return False
        except NotImplementedError:
            try:
                self.raw_queue.put(message_data, timeout=0.01)
                return True
            except Full:
                return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics"""
        return {
            'raw_queue_size': self._safe_qsize(self.raw_queue),
            'parsed_queue_size': self._safe_qsize(self.parsed_queue),
            'calculated_queue_size': self._safe_qsize(self.calculated_queue),
            'calc_workers': len(self.calc_workers),
            'running': self.running
        }

    @staticmethod
    def _safe_qsize(queue_obj: Queue) -> int:
        """Return queue size or -1 if unsupported (macOS)."""
        try:
            return queue_obj.qsize()
        except NotImplementedError:
            return -1
    
    def stop(self):
        """Stop all processes gracefully"""
        self.logger.info("🛑 Stopping pipeline...")
        self.running = False
        
        # Send poison pills
        self.raw_queue.put(None)
        for _ in range(len(self.calc_workers)):
            self.parsed_queue.put(None)
        self.calculated_queue.put(None)
        
        # Wait for processes
        if self.parser:
            self.parser.join(timeout=5)
        
        for worker in self.calc_workers:
            worker.join(timeout=5)
        
        if self.storage:
            self.storage.join(timeout=5)
        
        self.logger.info("✅ Pipeline stopped")

# ==================== USAGE ====================
def create_multiprocess_pipeline(instrument_info, indices_metadata=None):
    """Factory function to create pipeline"""
    return MultiProcessPipeline(
        instrument_info=instrument_info,
        indices_metadata=indices_metadata or {},
        num_calc_workers=12  # Optimal for M4
    )

# Example usage in crawler:
"""
# In your crawler initialization:
pipeline = create_multiprocess_pipeline(instrument_info, indices_metadata)
pipeline.start()

# In WebSocket message handler:
def on_websocket_message(message):
    pipeline.feed_message(message)

# On shutdown:
pipeline.stop()
"""
