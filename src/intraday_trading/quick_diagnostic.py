# quick_diagnostic.py
import time
import redis
import threading
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QuickRedisDiagnostic:
    def __init__(self, host='localhost', port=6379, db=1):
        self.client = redis.Redis(host=host, port=port, db=db)
        self.operation_times = defaultdict(list)
        self.lock = threading.Lock()
        self.running = True
        
    def start_monitoring(self, duration=60):
        """Monitor Redis operations for specified duration"""
        logger.info(f"Starting Redis monitoring for {duration} seconds...")
        
        # Hook Redis client methods
        self._hook_redis_methods()
        
        # Start background thread to log stats
        monitor_thread = threading.Thread(target=self._log_stats_periodically)
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # Run for duration
        time.sleep(duration)
        self.running = False
        self._log_final_stats()
    
    def _hook_redis_methods(self):
        """Hook key Redis methods to track timing"""
        methods = ['get', 'hget', 'hgetall', 'mget', 'zrange', 'scan']
        
        for method_name in methods:
            if hasattr(self.client, method_name):
                original = getattr(self.client, method_name)
                
                def make_wrapper(orig, name):
                    def wrapper(*args, **kwargs):
                        start = time.perf_counter()
                        try:
                            return orig(*args, **kwargs)
                        finally:
                            duration = time.perf_counter() - start
                            with self.lock:
                                self.operation_times[name].append(duration)
                                
                            if duration > 0.05:  # 50ms threshold
                                key = args[0] if args else 'unknown'
                                logger.warning(
                                    f"🔄 SLOW REDIS: {name}('{key}') took {duration:.3f}s"
                                )
                    return wrapper
                
                setattr(self.client, method_name, make_wrapper(original, method_name))
    
    def _log_stats_periodically(self):
        """Log statistics every 10 seconds"""
        while self.running:
            time.sleep(10)
            self._log_current_stats()
    
    def _log_current_stats(self):
        """Log current statistics"""
        with self.lock:
            total_ops = sum(len(times) for times in self.operation_times.values())
            if total_ops == 0:
                return
            
            logger.info("📊 REDIS OPERATIONS (last 10s):")
            for method, times in self.operation_times.items():
                if times:
                    avg_time = sum(times) / len(times)
                    slow_count = sum(1 for t in times if t > 0.05)
                    logger.info(
                        f"  {method}: {len(times)} calls, "
                        f"avg: {avg_time:.3f}s, "
                        f"slow (>50ms): {slow_count}"
                    )
            
            # Clear for next interval
            self.operation_times.clear()
    
    def _log_final_stats(self):
        """Log final statistics"""
        logger.info("="*60)
        logger.info("FINAL REDIS DIAGNOSTIC REPORT")
        logger.info("="*60)
        
        # Check Redis health
        try:
            start = time.perf_counter()
            self.client.ping()
            ping_time = time.perf_counter() - start
            logger.info(f"Redis ping: {ping_time:.3f}s")
            
            # Check memory usage
            info = self.client.info('memory')
            used_memory = int(info.get('used_memory', 0)) / 1024 / 1024
            logger.info(f"Redis memory used: {used_memory:.1f} MB")
            
            # Check connected clients
            clients = self.client.info('clients').get('connected_clients', 0)
            logger.info(f"Connected clients: {clients}")
            
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
        
        logger.info("="*60)

if __name__ == "__main__":
    diagnostic = QuickRedisDiagnostic()
    diagnostic.start_monitoring(duration=60)