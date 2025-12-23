"""
Optimized Async Stream Processor for Apple M4
High-performance worker pool for processing scanner:processed stream
"""
import sys
import os
from pathlib import Path

from shared_core.redis_clients.redis_client import get_redis_client
# Add parent directories to path for redis_files imports
script_dir = Path(__file__).parent
project_root = script_dir.parent.parent  # intraday_trading
parent_root = project_root.parent  # aion_algo_trading (where redis_files is)
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(parent_root))

import asyncio
import multiprocessing as mp
import time
import logging
from typing import Dict, List, Optional, Any
import redis

from shared_core.redis_clients.redis_key_standards import get_symbol_parser, UniversalSymbolParser, RedisKeyStandards
from shared_core.redis_clients.unified_tick_storage import UnifiedTickStorage
from shared_core.redis_clients.unified_data_storage import get_unified_storage
# ✅ MODULAR: Use new calculation services instead of HybridCalculations
from shared_core.calculation_service import create_calculation_service
# ✅ FIXED: Import from write_first_cache (UnderlyingPriceService is alias to UniversalSymbolParser)
from shared_core.write_first_cache import create_price_service

logger = logging.getLogger(__name__)


class OptimizedStreamProcessor:
    """
    Optimized async stream processor for Apple M4 with worker pool.
    
    Features:
    - Auto-scaling workers (70% of CPU cores, capped at 5 for M4)
    - Batch processing (50 ticks per batch)
    - Async/await for high throughput
    - Automatic ACK management
    - Performance monitoring
    """
    
    def __init__(self, redis_client=None, num_workers=None, batch_size=50):
        """
        Initialize optimized stream processor.
        
        Args:
            redis_client: Redis client instance (preferred - uses scanner's configured client)
            num_workers: Number of worker tasks (default: min(cpu_count, 8) for M4)
            batch_size: Batch size for processing (default: 50)
        """
        # ✅ FIXED: Use provided Redis client (from scanner config) or dedicated scanner pool
        # Scanner now runs its own connection pool instead of the shared pool
        if redis_client is not None:
            self.redis = redis_client
            logger.info(f"✅ [STREAM_PROCESSOR] Using provided Redis client (from scanner config)")
        else:
            self.redis = get_redis_client(
                process_name="scanner_processor",
                db=1,
                decode_responses=True,
                max_connections=10,
            )
            logger.info("✅ [STREAM_PROCESSOR] Using dedicated scanner Redis pool via get_redis_client()")
        
        # ✅ OPTIMIZATION: Cap at 5 workers to prevent over-subscription
        # Use 70% of cores, but cap at 5 for optimal performance
        cpu_count = mp.cpu_count() or 8
        if num_workers is None:
            # 70% of cores, capped at 5
            num_workers = min(int(cpu_count * 0.7), 5)
            num_workers = max(3, num_workers)  # Minimum 3 workers
        
        self.num_workers = num_workers
        self.batch_size = batch_size
        
        # ✅ MODULAR: Use new services instead of HybridCalculations
        trading_client = get_redis_client(process_name="optimized_stream_processor_storage", db=1)
        
        self.calc_service = create_calculation_service()
        self.price_service = create_price_service(redis_client=trading_client)
        self.symbol_parser = get_symbol_parser()  # UniversalSymbolParser singleton
        
        # ✅ FIXED: Connect IndexManager for underlying price lookups
        from shared_core.index_manager import index_manager
        index_manager.connect_redis(trading_client)
        
        # ✅ REDIS_FILES STANDARD: Use correct storage classes
        # UnifiedDataStorage for indicators/greeks, UnifiedTickStorage for tick hashes
        self.data_storage = get_unified_storage(redis_client=trading_client)  # For indicators/greeks
        self.tick_storage = UnifiedTickStorage(redis_client=trading_client)   # For tick hashes
        
        # ✅ NEW: Initialize PatternDetector for pattern detection
        try:
            from unified_pattern_init import init_scanner_worker
            worker_identifier = f"optimized_processor_{os.getpid()}"
            self.pattern_detector = init_scanner_worker(
                worker_identifier,
                config={},  # Use default config
                redis_client=trading_client,
            )
            logger.info(f"✅ [STREAM_PROCESSOR] PatternDetector initialized for worker {worker_identifier}")
        except Exception as e:
            logger.warning(f"⚠️ [STREAM_PROCESSOR] PatternDetector init failed: {e} - pattern detection disabled")
            self.pattern_detector = None
        
        # ✅ NEW: Initialize RedisNotifier for alert publishing
        try:
            from alerts.notifiers import RedisNotifier
            self.alert_notifier = RedisNotifier(redis_client=trading_client)
            logger.info("✅ [STREAM_PROCESSOR] RedisNotifier initialized for alert publishing")
        except Exception as e:
            logger.warning(f"⚠️ [STREAM_PROCESSOR] RedisNotifier init failed: {e} - alerts disabled")
            self.alert_notifier = None
        
        # Stream configuration
        # Use DatabaseAwareKeyBuilder for processed stream key
        from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
        self.stream_key = DatabaseAwareKeyBuilder.live_enriched_stream()
        self.group_name = 'scanner_group'
        self.consumer_name_prefix = f"scanner_processor_{os.getpid()}"
        
        # Performance tracking
        self.processed_count = 0
        self.last_report_time = time.time()
        self.worker_stats = {}  # Per-worker statistics
        
        # Ensure scanner consumer group exists
        self._ensure_scanner_group()
        
        logger.info(f"✅ [STREAM_PROCESSOR] Initialized with {self.num_workers} workers, batch_size={self.batch_size}")
        logger.info(f"✅ [STREAM_PROCESSOR] CPU cores: {cpu_count}, Workers: {self.num_workers} (capped at 5)")
        
        # Run pattern system diagnostics
        self._check_pattern_system()
    
    def _check_pattern_system(self):
        """Diagnose pattern detection issues"""
        try:
            # 1. Check if pattern_detector was initialized
            if self.pattern_detector:
                logger.info("✅ [PATTERN_DIAG] PatternDetector initialized successfully")
                # Check key attributes
                has_detect = hasattr(self.pattern_detector, 'detect_patterns')
                has_history = hasattr(self.pattern_detector, 'update_history')
                logger.info(f"   → detect_patterns: {has_detect}, update_history: {has_history}")
            else:
                logger.warning("⚠️ [PATTERN_DIAG] PatternDetector is None - pattern detection disabled")
            
            # 2. Check pattern detection module import
            try:
                from patterns.pattern_detector import PatternDetector
                logger.info("✅ [PATTERN_DIAG] patterns.pattern_detector module loaded")
            except ImportError as e:
                logger.error(f"❌ [PATTERN_DIAG] Cannot import PatternDetector: {e}")
            
            # 3. Check unified_pattern_init module
            try:
                from unified_pattern_init import init_scanner_worker
                logger.info("✅ [PATTERN_DIAG] unified_pattern_init module loaded")
            except ImportError as e:
                logger.error(f"❌ [PATTERN_DIAG] Cannot import unified_pattern_init: {e}")
                
        except Exception as e:
            logger.error(f"❌ [PATTERN_DIAG] Pattern diagnostic failed: {e}")
    
    def _ensure_scanner_group(self):
        """Create scanner-specific consumer group"""
        try:
            self.redis.xgroup_create(
                name=self.stream_key,
                groupname=self.group_name,
                id='0',
                mkstream=True
            )
            logger.info(f"✅ Created scanner group '{self.group_name}'")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Scanner group '{self.group_name}' exists")
            else:
                raise
    
    async def start(self):
        """Start the optimized stream processor"""
        logger.info(f"🚀 Starting optimized stream processor with {self.num_workers} workers")
        
        # Create worker tasks
        workers = []
        for i in range(self.num_workers):
            worker = asyncio.create_task(self.worker_loop(f"worker_{i}"))
            workers.append(worker)
        
        # Start performance monitor
        monitor = asyncio.create_task(self.monitor_performance())
        
        # Run all tasks concurrently
        try:
            await asyncio.gather(*workers, monitor)
        except asyncio.CancelledError:
            logger.info("Stream processor stopped")
    
    async def worker_loop(self, worker_id: str):
        """
        Individual worker processing loop.
        
        Each worker:
        - Reads batches from Redis stream
        - Processes ticks in batches
        - ACKs messages after successful processing
        """
        batch = []
        last_batch_time = time.time()
        batch_timeout = 0.01  # 10ms timeout for batch processing
        
        # Initialize worker stats
        self.worker_stats[worker_id] = {
            'processed': 0,
            'errors': 0,
            'last_update': time.time()
        }
        
        consumer_name = f"{self.consumer_name_prefix}_{worker_id}"
        logger.info(f"✅ Worker {worker_id} started as consumer {consumer_name}")
        
        loop = asyncio.get_event_loop()
        while True:
            try:
                # ✅ OPTIMIZED: Read batch of messages using XREADGROUP
                # This ensures proper consumer group handling and ACK management
                def _read_batch():
                    return self.redis.xreadgroup(
                        groupname=self.group_name,
                        consumername=consumer_name,
                        streams={self.stream_key: '>'},
                        count=self.batch_size,
                        block=1000  # 1 second block
                    )
                messages = await loop.run_in_executor(None, _read_batch)
                
                if messages:
                    # Parse messages from XREADGROUP format
                    for stream_data in messages:
                        if not stream_data or len(stream_data) < 2:
                            continue
                        
                        stream_name, message_list = stream_data[0], stream_data[1]
                        
                        # Add messages to batch
                        for message_id, data in message_list:
                            # data is already decoded if decode_responses=True
                            batch.append((message_id, data))
                    
                    # Process batch if ready
                    current_time = time.time()
                    should_process = (
                        len(batch) >= self.batch_size or
                        (batch and current_time - last_batch_time >= batch_timeout)
                    )
                    
                    if should_process:
                        await self.process_batch(batch, worker_id)
                        batch = []
                        last_batch_time = current_time
                
                else:
                    # No messages, small sleep to prevent CPU spinning
                    await asyncio.sleep(0.001)
                    
            except redis.exceptions.ConnectionError as e:
                logger.error(f"❌ Worker {worker_id} Redis connection error: {e}")
                await asyncio.sleep(1)  # Wait before retry
            except Exception as e:
                logger.error(f"❌ Worker {worker_id} error: {e}", exc_info=True)
                self.worker_stats[worker_id]['errors'] += 1
                await asyncio.sleep(0.1)  # Brief pause on error
    
    async def process_batch(self, batch: List[tuple], worker_id: str):
        """
        Process a batch of messages.
        
        Args:
            batch: List of (message_id, data) tuples
            worker_id: Worker identifier for statistics
        """
        start_time = time.time()
        ack_ids = []
        processed = 0
        
        for message_id, data in batch:
            try:
                symbol = data.get('symbol') or data.get('tradingsymbol')
                if not symbol:
                    logger.warning(f"⚠️ Missing symbol in message {message_id}")
                    continue
                
                # Parse price and volume
                try:
                    price = float(data.get('last_price') or data.get('price') or 0)
                    volume = int(data.get('volume') or data.get('bucket_incremental_volume') or 0)
                except (ValueError, TypeError) as e:
                    logger.warning(f"⚠️ Invalid price/volume in message {message_id}: {e}")
                    continue
                
                # ✅ FIXED: Use UniversalSymbolParser.parse() method
                parsed = self.symbol_parser.parse(symbol)
                
                # ✅ FIXED: Get canonical symbol for consistent storage/retrieval
                canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
                
                # ✅ FIXED: Preserve ALL incoming stream data, don't rebuild from scratch
                # The stream data already contains volume_ratio, delta, gamma, etc.
                tick_data = dict(data)  # Copy all fields from stream message
                tick_data.update({
                    'last_price': price,
                    'volume': volume,
                    'timestamp': time.time(),
                    'symbol': symbol
                })
                
                # Parse numeric fields that may come as strings from Redis stream
                for field in ['volume_ratio', 'delta', 'gamma', 'theta', 'vega', 'iv', 'underlying_price', 'gamma_exposure']:
                    if field in tick_data and tick_data[field] is not None:
                        try:
                            # Handle numpy string representation
                            val_str = str(tick_data[field])
                            if val_str.startswith('np.'):
                                val_str = val_str.split('(')[1].rstrip(')')
                            tick_data[field] = float(val_str)
                        except (ValueError, TypeError, IndexError):
                            pass
                
                # Calculate using modular CalculationService
                tick_data = self.calc_service.execute_pipeline(tick_data, self.price_service)
                indicators = tick_data  # Pipeline enriches tick_data in place
                
                # Add current price/volume to indicators
                if not isinstance(indicators, dict):
                    indicators = {}
                
                indicators.update({
                    'last_price': price,
                    'price': price,
                    'volume': volume,
                    'timestamp': time.time(),
                    'symbol': canonical_symbol  # ✅ FIXED: Use canonical_symbol
                })
                
                # ✅ MODULAR: Greeks calculated by CalculationService in execute_pipeline above
                greeks = {}
                # Safe check: parsed can be None if symbol doesn't match known patterns
                if parsed and getattr(parsed, 'instrument_type', None) == 'OPT' and getattr(parsed, 'strike', None) and getattr(parsed, 'option_type', None):
                    # Greeks are already in tick_data from execute_pipeline if it's an option
                    greeks = {
                        'delta': tick_data.get('delta'),
                        'gamma': tick_data.get('gamma'),
                        'theta': tick_data.get('theta'),
                        'vega': tick_data.get('vega'),
                        'iv': tick_data.get('iv'),
                    }
                    # Filter out None values
                    greeks = {k: v for k, v in greeks.items() if v is not None}
                
                # ✅ REDIS_FILES STANDARD: Use correct storage classes
                # UnifiedDataStorage for indicators/greeks, UnifiedTickStorage for tick hashes
                try:
                    # Store indicators using UnifiedDataStorage
                    if indicators:
                        self.data_storage.store_indicators(canonical_symbol, indicators)
                    
                    # Store Greeks using UnifiedDataStorage (if available)
                    if greeks:
                        self.data_storage.store_greeks(canonical_symbol, greeks)
                    
                    # Store tick hash using UnifiedTickStorage (standards-compliant keys)
                    # Uses ticks:{canonical_symbol} (plural) - NOT tick:{symbol} (singular)
                    self.tick_storage.store_tick(
                        canonical_symbol, 
                        tick_data, 
                        ttl=864000,  # ✅ INCREASED: 10 days TTL for ClickHouse recovery
                        publish_to_stream=False  # Don't republish - already consumed from stream
                    )
                    
                except Exception as storage_err:
                    logger.warning(f"⚠️ Storage error for {symbol}: {storage_err}")
                
                # ✅ FETCH FULL INDICATORS FROM REDIS: Pattern detectors need CVD, OFI, OBI, etc.
                # Use UnifiedDataStorage.get_indicators() - follows redis_files standards
                try:
                    stored_indicators = self.data_storage.get_indicators(canonical_symbol, indicator_names=None)
                    if stored_indicators:
                        for key, val in stored_indicators.items():
                            # Skip if already in indicators (tick data takes priority)
                            if key not in indicators:
                                indicators[key] = val
                        logger.debug(f"✅ [INDICATORS_ENRICHED] {canonical_symbol}: Added {len(stored_indicators)} fields via get_indicators()")
                except Exception as enrich_err:
                    logger.debug(f"⚠️ Could not fetch stored indicators for {canonical_symbol}: {enrich_err}")
                
                # ✅ NEW: Run pattern detection on the enriched tick data
                if self.pattern_detector and indicators:
                    try:
                        patterns = self.pattern_detector.detect_patterns(indicators, use_pipelining=True)
                        if patterns:
                            logger.info(f"🎯 [PATTERN_DETECTED] {canonical_symbol}: {[p.get('pattern_name') for p in patterns if isinstance(p, dict)]}")
                            for pattern in patterns:
                                if isinstance(pattern, dict):
                                    logger.info(
                                        "   → Pattern: %s (signal=%s)",
                                        pattern.get('pattern_name', pattern.get('pattern')),
                                        pattern.get('signal') or pattern.get('direction')
                                    )
                            
                            # ✅ PUBLISH ALERTS: Each detected pattern becomes an alert
                            # ✅ CRITICAL FIX: This was incorrectly in else block - moved to if block
                            if self.alert_notifier:
                                for pattern in patterns:
                                    if not isinstance(pattern, dict):
                                        continue
                                    try:
                                        # Build algo_alert using UnifiedAlertBuilder
                                        from alerts.unified_alert_builder import get_unified_alert_builder
                                        builder = get_unified_alert_builder(redis_client=self.redis)
                                        
                                        algo_alert = builder.build_algo_alert(
                                            symbol=canonical_symbol,
                                            pattern_type=pattern.get('pattern_name', pattern.get('pattern', 'unknown')),
                                            entry_price=tick_data.get('last_price', 0),
                                            stop_loss=pattern.get('stop_loss'),
                                            target_price=pattern.get('target_price', pattern.get('target')),
                                            confidence=pattern.get('confidence', 0.5),
                                            signal=pattern.get('signal', pattern.get('direction', 'BUY')),
                                            indicators=indicators,
                                            greeks=greeks,
                                            volume_ratio=tick_data.get('volume_ratio'),
                                            description=pattern.get('description', ''),
                                        )
                                        
                                        # Combine pattern + algo_alert for publishing
                                        alert_data = pattern.copy()
                                        alert_data['algo_alert'] = algo_alert
                                        alert_data['symbol'] = canonical_symbol
                                        
                                        # Publish to alerts:stream
                                        self.alert_notifier.send_alert(alert_data)
                                        logger.info(f"📢 [ALERT_PUBLISHED] {canonical_symbol}: {pattern.get('pattern_name', 'unknown')}")
                                        
                                    except Exception as alert_err:
                                        logger.warning(f"⚠️ Alert publish error for {canonical_symbol}: {alert_err}")
                        else:
                            logger.debug(f"⚠️ No patterns returned for {canonical_symbol}")
                    except Exception as pattern_err:
                        logger.debug(f"Pattern detection error for {symbol}: {pattern_err}")
                
                # Track for ACK
                ack_ids.append(message_id)
                processed += 1
                self.processed_count += 1
                
            except Exception as e:
                logger.error(f"❌ Error processing message {message_id}: {e}", exc_info=True)
                # Don't ACK failed messages (will retry)
        
        # ACK all successfully processed messages
        if ack_ids:
            try:
                # ACK messages synchronously (simpler and more reliable)
                for msg_id in ack_ids:
                    self.redis.xack(self.stream_key, self.group_name, msg_id)
            except Exception as ack_err:
                logger.error(f"❌ Error ACKing messages: {ack_err}")
        
        # Update worker statistics
        processing_time = time.time() - start_time
        self.worker_stats[worker_id]['processed'] += processed
        self.worker_stats[worker_id]['last_update'] = time.time()
        
        if processing_time > 0.1:  # Log slow batches
            logger.warning(f"⚠️ Slow batch processing in {worker_id}: {processing_time:.3f}s for {len(batch)} messages")
    
    def _get_underlying_price(self, base_symbol: str) -> float:
        """
        Get current price of underlying instrument using UnderlyingPriceService.
        
        ✅ MODULAR: Delegates to centralized price service instead of inline Redis logic.
        
        Args:
            base_symbol: Base symbol (e.g., 'NIFTY', 'BANKNIFTY', 'RELIANCE')
        
        Returns:
            Current underlying price, or 0.0 if not available
        """
        try:
            return self.price_service.get_price(base_symbol) or 0.0
        except Exception as e:
            logger.debug(f"Error getting underlying price for {base_symbol}: {e}")
            return 0.0
    
    async def monitor_performance(self):
        """Monitor system performance and log statistics"""
        while True:
            await asyncio.sleep(30)  # Report every 30 seconds
            
            current_time = time.time()
            elapsed = current_time - self.last_report_time
            
            if elapsed > 0:
                rate = self.processed_count / elapsed
                
                # Aggregate worker stats
                total_processed = sum(stats['processed'] for stats in self.worker_stats.values())
                total_errors = sum(stats['errors'] for stats in self.worker_stats.values())
                
                logger.info(
                    f"📊 [PERFORMANCE] Processed: {self.processed_count} messages "
                    f"({rate:.1f} msg/sec), Workers: {self.num_workers}, "
                    f"Errors: {total_errors}"
                )
                
                # Log per-worker stats if verbose
                if logger.isEnabledFor(logging.DEBUG):
                    for worker_id, stats in self.worker_stats.items():
                        logger.debug(
                            f"  {worker_id}: {stats['processed']} processed, "
                            f"{stats['errors']} errors"
                        )
            
            # Reset counters
            self.processed_count = 0
            self.last_report_time = current_time
            
            # Reset worker stats
            for stats in self.worker_stats.values():
                stats['processed'] = 0
                stats['errors'] = 0


# Convenience function to start the processor
async def start_optimized_processor(redis_client=None, num_workers=None, batch_size=50):
    """
    Start the optimized stream processor.
    
    Args:
        redis_client: Optional Redis client to reuse from scanner_main
        num_workers: Number of workers (default: auto-scaled for M4)
        batch_size: Batch size for processing
    
    Returns:
        OptimizedStreamProcessor instance
    """
    processor = OptimizedStreamProcessor(redis_client=redis_client, num_workers=num_workers, batch_size=batch_size)
    await processor.start()
    return processor


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    asyncio.run(start_optimized_processor())
