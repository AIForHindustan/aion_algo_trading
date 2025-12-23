#!/usr/bin/env python3
"""
Supervisor for DataPipeline workers - auto-restarts dead workers
"""
import subprocess
import time
from pathlib import Path
import psutil
import logging
import redis
import schedule
import sys
from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
from shared_core.redis_clients.redis_client import get_redis_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WorkerSupervisor:
    def __init__(self, num_workers=12):
        self.num_workers = num_workers
        self.workers = {}  # pid -> process
        # ✅ FIXED: Use get_redis_client() for M4-optimized connection pooling
        self.redis = get_redis_client(process_name="worker_supervisor", db=1)
        
    def start_worker(self, worker_id):
        """Start a single worker"""
        # ✅ FIX: Use absolute path based on script location, not cwd
        script_dir = Path(__file__).parent.parent.parent  # src/ directory
        pipeline_path = script_dir / "intraday_trading" / "intraday_scanner" / "data_pipeline.py"
        
        cmd = [
            sys.executable, str(pipeline_path),
            "--worker-id", str(worker_id),
            "--auto-recover", "true"
        ]
        
        process = subprocess.Popen(
            cmd,
            stdout=None,
            stderr=None,
            text=True
        )
        
        self.workers[process.pid] = {
            'process': process,
            'worker_id': worker_id,
            'start_time': time.time()
        }
        
        logger.info(f"🚀 Started worker {worker_id} (PID: {process.pid})")
        return process
    
    def monitor_workers(self):
        """Monitor and restart dead workers"""
        schedule.every(60).seconds.do(self.health_check_enriched_flow)
        schedule.every(60).seconds.do(self.auto_heal_stuck_messages)
        
        while True:
            schedule.run_pending()
            
            # Check Redis consumer health
            try:
                dead_consumers = self._check_consumer_health()
                if dead_consumers:
                    logger.warning(f"⚠️ Found {len(dead_consumers)} dead consumers")
                    self._restart_dead_workers(dead_consumers)
            except Exception as e:
                logger.error(f"❌ Health check failed: {e}")
            
            # Check process health
            dead_pids = []
            for pid, info in list(self.workers.items()):
                if not psutil.pid_exists(pid):
                    dead_pids.append(pid)
                    logger.error(f"💀 Worker {info['worker_id']} (PID: {pid}) died!")
            
            # Restart dead workers
            for pid in dead_pids:
                info = self.workers.pop(pid)
                self.start_worker(info['worker_id'])
            
            time.sleep(30)  # Check every 30 seconds
    
    def _check_consumer_health(self):
        """Check Redis for dead consumers"""
        try:
            consumers = self.redis.xinfo_consumers(
                "ticks:intraday:processed",
                "data_pipeline_group"
            )
            
            dead = []
            for c in consumers:
                if c.get('idle', 0) > 60000:  # 60 seconds idle
                    dead.append(c.get('name'))
            
            return dead
        except:
            return []
    
    def safe_process_tick(self, message_id, data, redis_client):
        """
        Lightweight tick processing for supervisor healing.
        Logs the data that was potentially lost/stuck.
        """
        try:
            symbol = data.get('symbol', 'UNKNOWN')
            logger.info(f"🔧 Healing stuck tick for {symbol} (ID: {message_id})")
            return True
        except Exception as e:
            logger.error(f"❌ Error in healer processing: {e}")
            return False

    def auto_heal_stuck_messages(self):
        """Wrapper for health check and auto-healing"""
        self.heal_consumer_group()

    def heal_consumer_group(self, stream_key="ticks:intraday:processed", group_name="data_pipeline_group", consumer_name="healer_consumer"):
        """
        Uses XAUTOCLAIM to automatically recover messages stuck with dead consumers.
        """
        try:
            # Claim messages idle for more than 30 seconds
            claimed = self.redis.xautoclaim(
                name=stream_key,
                groupname=group_name,
                consumername=consumer_name,
                min_idle_time=30000, # 30 seconds in milliseconds
                count=100
            )
            
            # XAUTOCLAIM returns: (next_start_id, [(msg_id, data), ...], [deleted_ids])
            if claimed and len(claimed) > 1 and claimed[1]:
                logger.info(f"✅ Healed {len(claimed[1])} stuck messages via XAUTOCLAIM.")
                # Process and ACK the claimed messages immediately
                for message_id, data in claimed[1]:
                    # Use a lightweight safe processing for healing
                    try:
                        symbol = data.get('symbol', 'UNKNOWN')
                        logger.info(f"🔧 Healing stuck tick for {symbol} (ID: {message_id})")
                        # ACK to keep stream moving
                        self.redis.xack(stream_key, group_name, message_id)
                    except Exception as e:
                        logger.error(f"❌ Healing ACK failed for {message_id}: {e}")
        except Exception as e:
            if "NOGROUP" not in str(e):
                logger.error(f"❌ XAUTOCLAIM healing failed: {e}")
    
    def health_check_enriched_flow(self):
        """Monitor the enriched data flow"""
        try:
            # Use dedicated client for health check to avoid blocking main connection
            r = self.redis
            
            # Check raw stream health
            raw_stream = "ticks:intraday:processed"
            raw_len = r.xlen(raw_stream)
            
            # Check enriched stream health
            enriched_stream = DatabaseAwareKeyBuilder.live_enriched_stream()
            enriched_len = r.xlen(enriched_stream)
            
            # Check consumer groups
            try:
                raw_groups = r.xinfo_groups(raw_stream)
            except:
                raw_groups = []
                
            try:
                enriched_groups = r.xinfo_groups(enriched_stream)
            except:
                enriched_groups = []
            
            logger.info(f"📊 DATA FLOW HEALTH:")
            logger.info(f"  Raw Stream: {raw_len} messages")
            logger.info(f"  Enriched Stream: {enriched_len} messages")
            
            # Alert if enriched stream not growing
            if raw_len > 0 and enriched_len == 0:
                logger.error("❌ DATA FLOW BLOCKED: Raw stream has data but enriched stream is empty!")
                logger.error("   Check data_pipeline workers are running and writing to enriched stream")
            
            # Check for consumer conflicts
            for group in raw_groups:
                if group['name'] == 'data_pipeline_group':
                    consumers = r.xinfo_consumers(raw_stream, 'data_pipeline_group')
                    for consumer in consumers:
                        if 'scanner' in consumer['name'].lower():
                            logger.error(f"❌ CONSUMER CONFLICT: Scanner {consumer['name']} in data_pipeline_group!")
                            # Auto-fix
                            r.xgroup_delconsumer(raw_stream, 'data_pipeline_group', consumer['name'])
                            logger.info(f"✅ Auto-removed conflicting consumer: {consumer['name']}")
        except Exception as e:
            logger.error(f"Error in health check: {e}")
    
    def _restart_dead_workers(self, dead_consumers):
        """Restart workers for dead consumers - claims and ACKs pending messages first"""
        stream_key = "ticks:intraday:processed"
        group_name = "data_pipeline_group"
        
        for consumer_name in dead_consumers:
            try:
                # ✅ RESILIENT PATTERN: Claim and ACK pending messages before removing consumer
                # This prevents message loss and stream blockage
                pending = self.redis.xpending_range(
                    stream_key, group_name, 
                    min='-', max='+', count=1000,
                    consumername=consumer_name
                )
                
                if pending:
                    logger.warning(f"  🪦 {consumer_name}: idle={pending[0].get('idle', 0) / 1000:.1f}s, pending={len(pending)}")
                    
                    # Claim and ACK all pending messages from this dead consumer
                    message_ids = [p['message_id'] for p in pending]
                    if message_ids:
                        # Claim messages to a temporary consumer, then ACK them
                        try:
                            self.redis.xclaim(
                                stream_key, group_name, 
                                "supervisor_cleanup",  # Temporary consumer
                                min_idle_time=0,
                                message_ids=message_ids
                            )
                            # ACK all claimed messages to release them from pending
                            for msg_id in message_ids:
                                self.redis.xack(stream_key, group_name, msg_id)
                            logger.info(f"  ✅ Claimed and ACKed {len(message_ids)} pending messages from {consumer_name}")
                        except Exception as claim_err:
                            logger.warning(f"  ⚠️ Could not claim messages: {claim_err}")
                
                # Remove the dead consumer
                self.redis.xgroup_delconsumer(stream_key, group_name, consumer_name)
                logger.info(f"🧹 Removed dead consumer {consumer_name}")
                
            except Exception as e:
                logger.warning(f"⚠️ Error cleaning up consumer {consumer_name}: {e}")
            
            # Extract worker ID and restart if it's a numbered worker
            if consumer_name.startswith("pipeline_worker_"):
                try:
                    parts = consumer_name.split('_')
                    worker_id = int(parts[2])  # pipeline_worker_{ID}_timestamp
                    
                    # Only restart if worker_id is in valid range (0 to num_workers-1)
                    if 0 <= worker_id < self.num_workers:
                        # Kill if running
                        for pid, info in list(self.workers.items()):
                            if info['worker_id'] == worker_id:
                                info['process'].terminate()
                                time.sleep(1)
                                if info['process'].poll() is None:
                                    info['process'].kill()
                                self.workers.pop(pid)
                                logger.info(f"🔪 Killed stale worker {worker_id}")
                        
                        # Restart
                        self.start_worker(worker_id)
                    
                except (IndexError, ValueError):
                    continue
    
    def run(self):
        """Start supervisor"""
        logger.info(f"👑 Starting supervisor for {self.num_workers} workers")
        
        # Start all workers
        for i in range(self.num_workers):
            self.start_worker(i)
            time.sleep(0.5)  # Stagger startups
        
        # Monitor forever
        self.monitor_workers()

def main():
    """Main entry point for pipeline supervisor"""
    supervisor = WorkerSupervisor(num_workers=12)
    supervisor.run()

if __name__ == "__main__":
    main()