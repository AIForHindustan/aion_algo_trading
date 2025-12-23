#!/opt/homebrew/bin/python3.13
"""
Infrastructure Health Monitor - Single Source of Truth
======================================================

✅ CONSOLIDATED: Combines PerformanceMonitor and StreamMonitor into one unified monitor.
This is the ONLY infrastructure monitoring component - monitors and maintains Redis health.

Features:
- Proactive stream monitoring and trimming (prevents memory growth)
- Redis memory and connection monitoring
- Consumer group health checks
- Historical archive buffer monitoring
- Performance metrics logging
- Automatic stream optimization

Location: core/health/monitor.py (infrastructure monitoring belongs in core/health/)
"""

import logging
import time
import threading
from typing import Optional, Dict, Any, List
from datetime import datetime
from shared_core.redis_clients.redis_client import RedisClientFactory

logger = logging.getLogger(__name__)


class InfrastructureMonitor:
    """
    Unified infrastructure health monitor.
    
    ✅ CONSOLIDATED: Combines stream monitoring, trimming, memory monitoring, and archive monitoring.
    Monitors Redis streams, proactively trims them, checks consumer groups, and monitors system health.
    
    Features:
    - Proactive stream trimming (prevents unbounded growth)
    - Redis memory and connection monitoring
    - Consumer group health checks
    - Historical archive buffer monitoring
    - Performance metrics logging
    """
    
    def __init__(
        self,
        redis_db: int = 1,
        check_interval: int = 300,
        streams_to_monitor: Optional[List[str]] = None,
        historical_archive_instance: Optional[Any] = None
    ):
        """
        Initialize infrastructure monitor.
        
        Args:
            redis_db: Redis database number (default: 1 for realtime streams)
            check_interval: Monitoring interval in seconds (default: 300 = 5 minutes)
            streams_to_monitor: List of stream names to monitor (default: common streams)
            historical_archive_instance: HistoricalArchive instance to monitor
        """
        self.redis_db = redis_db
        self.check_interval = check_interval
        self.historical_archive = historical_archive_instance
        self.running = False
        self.monitor_thread: Optional[threading.Thread] = None
        
        # ✅ Stream configurations with target maxlen (proactive trimming)
        # ✅ FIXED: Only monitor ticks:intraday:processed (intraday crawler publishes here)
        # ticks:raw:binary is not used by intraday crawler
        self.stream_configs = {
            'ticks:intraday:processed': 5000,  # Keep last 5k messages
            'alerts:stream': 1000,             # Keep last 1k alerts
            'patterns:global': 5000,           # Keep last 5k patterns
        }
        
        # Use custom streams if provided, otherwise use defaults
        if streams_to_monitor:
            # Merge with defaults, keeping custom targets if specified
            for stream in streams_to_monitor:
                if stream not in self.stream_configs:
                    self.stream_configs[stream] = 5000  # Default target
        
        # Statistics
        self.stats = {
            'checks_performed': 0,
            'last_check_time': None,
            'streams_trimmed': 0,
            'errors': 0
        }
        
        # ✅ STANDARDIZED: Use RedisClientFactory for connection pooling
        self.redis_client = RedisClientFactory.get_trading_client()
        
        logger.info(f"✅ InfrastructureMonitor initialized (check interval: {check_interval}s)")
    
    def start(self):
        """Start the monitoring thread"""
        if self.running:
            logger.warning("⚠️ Infrastructure monitor already running")
            return
        
        self.running = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="InfrastructureMonitor",
            daemon=True
        )
        self.monitor_thread.start()
        logger.info(f"✅ Infrastructure monitor started (interval: {self.check_interval}s)")
    
    def stop(self):
        """Stop the monitoring thread"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)
        logger.info("📊 Infrastructure monitor stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while self.running:
            try:
                # Test connection
                self.redis_client.ping()
                consecutive_errors = 0  # Reset on success
                
                # Perform monitoring checks
                self._check_performance()
                self.stats['checks_performed'] += 1
                self.stats['last_check_time'] = datetime.now()
                
                # Sleep for check interval
                time.sleep(self.check_interval)
                
            except Exception as e:
                consecutive_errors += 1
                error_str = str(e).lower()
                
                if "connection" in error_str or "too many connections" in error_str:
                    logger.warning(f"⚠️ Redis connection error ({consecutive_errors}/{max_consecutive_errors}): {e}")
                    # Reconnect
                    try:
                        self.redis_client.close()
                    except:
                        pass
                    self.redis_client = RedisClientFactory.get_trading_client()
                else:
                    logger.error(f"❌ Infrastructure monitoring error ({consecutive_errors}/{max_consecutive_errors}): {e}")
                    self.stats['errors'] += 1
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("❌ Max consecutive errors reached. Stopping monitor.")
                    self.running = False
                    break
                
                # Wait before retry
                time.sleep(min(60, self.check_interval))
    
    def _check_performance(self):
        """Check Redis and archive performance, trim streams proactively"""
        try:
            if not self.redis_client:
                logger.warning("⚠️  Could not get Redis client for monitoring")
                return
            
            # Get Redis info
            redis_info = self.redis_client.info()
            memory_used = redis_info.get('used_memory_human', 'N/A')
            memory_peak = redis_info.get('used_memory_peak_human', 'N/A')
            connected_clients = redis_info.get('connected_clients', 0)
            memory_used_bytes = redis_info.get('used_memory', 0)
            maxmemory = redis_info.get('maxmemory', 0)
            
            # Monitor and trim streams proactively
            stream_info = {}
            for stream_name, target_maxlen in self.stream_configs.items():
                try:
                    if not self.redis_client.exists(stream_name):
                        stream_info[stream_name] = {'length': 0, 'status': 'not_exists'}
                        continue
                    
                    current_len = self.redis_client.xlen(stream_name)
                    
                    # ✅ PROACTIVE TRIMMING: Trim if stream is 10% over target
                    threshold = target_maxlen * 1.1
                    trimmed_count = 0
                    
                    if current_len > threshold:
                        # Trim stream to target
                        trimmed_count = self.redis_client.xtrim(
                            stream_name,
                            maxlen=target_maxlen,
                            approximate=True
                        )
                        new_len = self.redis_client.xlen(stream_name)
                        self.stats['streams_trimmed'] += 1
                        
                        logger.warning(
                            f"🔧 Proactively trimmed {stream_name}: "
                            f"{current_len:,} → {new_len:,} messages "
                            f"(removed ~{trimmed_count:,}, target: {target_maxlen:,})"
                        )
                        current_len = new_len
                    
                    # Get consumer group info
                    group_info = []
                    try:
                        groups = self.redis_client.xinfo_groups(stream_name)
                        for group in groups:
                            name_bytes = group.get('name', b'')
                            if isinstance(name_bytes, bytes):
                                group_name = name_bytes.decode('utf-8')
                            else:
                                group_name = str(name_bytes)
                            
                            pending = group.get('pending', 0)
                            if isinstance(pending, bytes):
                                pending = int(pending.decode('utf-8'))
                            
                            consumers = group.get('consumers', 0)
                            if isinstance(consumers, bytes):
                                consumers = int(consumers.decode('utf-8'))
                            
                            group_info.append(f"{group_name}(pending:{pending},consumers:{consumers})")
                            
                            # Warn if large backlog
                            if pending > 1000:
                                logger.warning(
                                    f"⚠️  Consumer group '{group_name}' in {stream_name}: "
                                    f"{pending:,} pending messages ({consumers} consumers)"
                                )
                    except Exception:
                        pass
                    
                    stream_info[stream_name] = {
                        'length': current_len,
                        'target': target_maxlen,
                        'groups': group_info,
                        'status': 'healthy' if current_len <= target_maxlen else 'over_target'
                    }
                    
                except Exception as e:
                    error_str = str(e).lower()
                    if "no such key" in error_str:
                        stream_info[stream_name] = {'length': 0, 'status': 'not_exists'}
                    else:
                        logger.warning(f"⚠️  Error monitoring {stream_name}: {e}")
                        stream_info[stream_name] = {'length': 0, 'status': 'error'}
            
            # Get archive buffer size
            archive_buffer_size = 0
            archive_stats = {}
            if self.historical_archive:
                try:
                    if hasattr(self.historical_archive, 'parquet_buffer'):
                        with getattr(self.historical_archive, 'parquet_batch_lock', threading.Lock()):
                            archive_buffer_size = len(self.historical_archive.parquet_buffer)
                    
                    if hasattr(self.historical_archive, 'get_stats'):
                        archive_stats = self.historical_archive.get_stats()
                    elif hasattr(self.historical_archive, 'stats'):
                        archive_stats = self.historical_archive.stats
                except Exception as e:
                    pass
            
            # Log performance metrics
            timestamp = datetime.now().strftime("%H:%M:%S")
            logger.info(f"📊 [{timestamp}] Infrastructure Monitor:")
            
            # Stream info
            for stream_name, info in stream_info.items():
                groups_str = ", ".join(info.get('groups', [])) if info.get('groups') else "no groups"
                status_icon = "✅" if info.get('status') == 'healthy' else "⚠️"
                logger.info(
                    f"   {status_icon} Stream {stream_name}: {info.get('length', 0):,} msgs "
                    f"(target: {info.get('target', 0):,}) | Groups: {groups_str}"
                )
            
            # Redis memory
            memory_percent = (memory_used_bytes / maxmemory * 100) if maxmemory > 0 else 0
            memory_status = "⚠️" if memory_percent > 90 else "✅"
            logger.info(
                f"   {memory_status} Redis Memory: {memory_used} (peak: {memory_peak}) | "
                f"Usage: {memory_percent:.1f}% | Connected Clients: {connected_clients}"
            )
            
            # Archive info
            if self.historical_archive:
                archive_status = "⚠️" if archive_buffer_size > 5000 else "✅"
                logger.info(
                    f"   {archive_status} Archive Buffer: {archive_buffer_size:,} ticks | "
                    f"Archived: {archive_stats.get('ticks_archived', 0):,} | "
                    f"Batches: {archive_stats.get('batches_written', 0):,} | "
                    f"Errors: {archive_stats.get('errors', 0):,}"
                )
            
            # Check for warnings
            self._check_warnings(stream_info, redis_info, archive_buffer_size, archive_stats)
            
        except Exception as e:
            logger.error(f"❌ Error checking performance: {e}")
            self.stats['errors'] += 1
    
    def _check_warnings(self, stream_info: Dict, redis_info: Dict, archive_buffer_size: int, archive_stats: Dict):
        """Check for performance warnings"""
        warnings = []
        
        # Check stream lengths
        for stream_name, info in stream_info.items():
            stream_len = info.get('length', 0)
            target = info.get('target', 0)
            
            if stream_name == 'ticks:intraday:processed' and stream_len > 10000:
                warnings.append(f"⚠️  {stream_name} too long: {stream_len:,} (target: {target:,})")
            # ✅ FIXED: ticks:raw:binary is not used by intraday crawler (removed)
            elif stream_name == 'alerts:stream' and stream_len > 2000:
                warnings.append(f"⚠️  {stream_name} too long: {stream_len:,} (target: {target:,})")
            elif stream_name == 'patterns:global' and stream_len > 10000:
                warnings.append(f"⚠️  {stream_name} too long: {stream_len:,} (target: {target:,})")
        
        # Check pending messages in consumer groups
        for stream_name, info in stream_info.items():
            for group_str in info.get('groups', []):
                if 'pending:' in group_str:
                    try:
                        pending = int(group_str.split('pending:')[1].split(',')[0])
                        if pending > 1000:
                            warnings.append(f"⚠️  {stream_name} has {pending:,} pending messages")
                    except:
                        pass
        
        # Check Redis memory
        memory_used_bytes = redis_info.get('used_memory', 0)
        maxmemory = redis_info.get('maxmemory', 0)
        if maxmemory > 0:
            memory_percent = (memory_used_bytes / maxmemory) * 100
            if memory_percent > 90:
                warnings.append(f"⚠️  Redis memory usage: {memory_percent:.1f}%")
        
        # Check archive buffer
        if archive_buffer_size > 5000:
            warnings.append(f"⚠️  Archive buffer large: {archive_buffer_size:,} ticks")
        
        # Check archive errors
        archive_errors = archive_stats.get('errors', 0)
        if archive_errors > 10:
            warnings.append(f"⚠️  Archive has {archive_errors:,} errors")
        
        # Log warnings
        if warnings:
            for warning in warnings:
                logger.warning(warning)
    
    def optimize_streams_once(self):
        """Perform one-time stream optimization (called on startup)"""
        logger.info("🔧 Performing initial stream optimization...")
        optimized = 0
        for stream_name, target_maxlen in self.stream_configs.items():
            try:
                if not self.redis_client.exists(stream_name):
                    continue
                
                current_len = self.redis_client.xlen(stream_name)
                if current_len > target_maxlen:
                    trimmed = self.redis_client.xtrim(
                        stream_name,
                        maxlen=target_maxlen,
                        approximate=True
                    )
                    new_len = self.redis_client.xlen(stream_name)
                    logger.info(
                        f"   ✅ {stream_name}: {current_len:,} → {new_len:,} "
                        f"(removed ~{trimmed:,}, target: {target_maxlen:,})"
                    )
                    optimized += 1
            except Exception as e:
                logger.warning(f"   ⚠️  {stream_name}: optimization failed: {e}")
        
        logger.info(f"✅ Initial stream optimization complete ({optimized} streams optimized)")
    
    def get_stats(self) -> Dict:
        """Get monitoring statistics"""
        return {
            **self.stats,
            'interval_seconds': self.check_interval,
            'streams_monitored': len(self.stream_configs),
            'archive_monitored': self.historical_archive is not None
        }


def monitor_infrastructure(
    redis_db: int = 1,
    check_interval: int = 300,
    historical_archive_instance: Optional[Any] = None
) -> InfrastructureMonitor:
    """
    Start infrastructure monitoring.
    
    Args:
        redis_db: Redis database number for streams
        check_interval: Monitoring interval in seconds (default: 300 = 5 minutes)
        historical_archive_instance: HistoricalArchive instance to monitor
    
    Returns:
        InfrastructureMonitor instance
    """
    monitor = InfrastructureMonitor(
        redis_db=redis_db,
        check_interval=check_interval,
        historical_archive_instance=historical_archive_instance
    )
    monitor.start()
    return monitor


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("📊 Starting Infrastructure Monitor...")
    monitor = monitor_infrastructure(check_interval=300)
    
    try:
        # Run until interrupted
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n🛑 Stopping infrastructure monitor...")
        monitor.stop()
        print("✅ Infrastructure monitor stopped")
