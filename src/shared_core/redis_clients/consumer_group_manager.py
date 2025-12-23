"""
Auto-healing Redis Consumer Group Manager
Detects dead consumers and recreates them automatically
"""
import logging
import time
import threading
import redis
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ConsumerInfo:
    name: str
    pending: int
    idle_ms: int
    last_active: float  # timestamp

class ConsumerGroupManager:
    """Manages Redis consumer groups with auto-healing"""
    
    def __init__(self, redis_client, group_name: str, stream_key: str):
        self.redis = redis_client
        self.group_name = group_name
        self.stream_key = stream_key
        self.consumer_prefix = f"autoconsumer_{int(time.time())}_{id(self)}"
        self.consumer_counter = 0
        self.max_idle_ms = 30000  # 30 seconds max idle
        self.cleanup_interval = 60  # Check every 60 seconds
        
        # Ensure group exists
        self._ensure_consumer_group()
        
        # Start health monitor
        self._running = True
        self._monitor_thread = threading.Thread(
            target=self._health_monitor_loop,
            daemon=True,
            name=f"ConsumerGroupHealth-{group_name}"
        )
        self._monitor_thread.start()
        
        logger.info(f"✅ Auto-healing ConsumerGroupManager started for {group_name}")
    
    def _ensure_consumer_group(self):
        """Create consumer group if it doesn't exist"""
        try:
            # Try to create group
            self.redis.xgroup_create(
                name=self.stream_key,
                groupname=self.group_name,
                id='0',  # Start from beginning
                mkstream=True  # Create stream if doesn't exist
            )
            logger.info(f"✅ Created consumer group {self.group_name} on {self.stream_key}")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                # Group already exists - that's fine
                logger.debug(f"ℹ️ Consumer group {self.group_name} already exists")
            else:
                logger.error(f"❌ Failed to create consumer group: {e}")
                raise
    
    def create_consumer(self, consumer_name: Optional[str] = None) -> str:
        """Create a new consumer with auto-generated name"""
        if not consumer_name:
            self.consumer_counter += 1
            consumer_name = f"{self.consumer_prefix}_{self.consumer_counter}"
        
        # Consumer is automatically created when we read with XREADGROUP
        logger.info(f"✅ Created consumer {consumer_name} in group {self.group_name}")
        return consumer_name
    
    def get_dead_consumers(self) -> List[ConsumerInfo]:
        """Find consumers that appear to be dead"""
        try:
            consumers_info = self.redis.xinfo_consumers(
                self.stream_key, self.group_name
            )
            
            dead_consumers = []
            now = time.time() * 1000  # Current time in ms
            
            for info in consumers_info:
                idle_ms = info.get('idle', 0)
                pending = info.get('pending', 0)
                name = info.get('name', 'unknown')
                
                # Criteria for "dead":
                # 1. Idle > max_idle_ms AND pending = 0 (no active work)
                # 2. Idle > max_idle_ms * 10 (definitely dead)
                if (idle_ms > self.max_idle_ms and pending == 0) or idle_ms > (self.max_idle_ms * 10):
                    dead_consumers.append(ConsumerInfo(
                        name=name,
                        pending=pending,
                        idle_ms=idle_ms,
                        last_active=now - idle_ms
                    ))
            
            return dead_consumers
            
        except Exception as e:
            logger.error(f"❌ Failed to get consumer info: {e}")
            return []
    
    def remove_dead_consumer(self, consumer_name: str):
        """Remove a dead consumer from the group using the standard Redis API."""
        try:
            self.redis.xgroup_delconsumer(
                self.stream_key,
                self.group_name,
                consumer_name
            )
            logger.info(f"🧹 Removed dead consumer {consumer_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to remove consumer {consumer_name}: {e}")
            return False
    
    def _health_monitor_loop(self):
        """Background thread to monitor and heal consumer groups"""
        while self._running:
            try:
                # Check for dead consumers
                dead_consumers = self.get_dead_consumers()
                
                if dead_consumers:
                    logger.warning(f"⚠️ Found {len(dead_consumers)} dead consumers in {self.group_name}")
                    
                    for consumer in dead_consumers:
                        logger.warning(f"  🪦 {consumer.name}: idle={consumer.idle_ms/1000:.1f}s, pending={consumer.pending}")
                        self.remove_dead_consumer(consumer.name)
                
                # Check if group has ANY active consumers
                consumers_info = self.redis.xinfo_consumers(
                    self.stream_key, self.group_name
                )
                
                active_consumers = [
                    c for c in consumers_info 
                    if c.get('idle', 0) < self.max_idle_ms
                ]
                
                if not active_consumers and consumers_info:
                    logger.error(f"🚨 No active consumers in {self.group_name}! Creating emergency consumer...")
                    emergency_consumer = self.create_consumer(f"emergency_{int(time.time())}")
                    
                    # Start reading with emergency consumer
                    self._start_emergency_consumer(emergency_consumer)
                
                # Sleep until next check
                time.sleep(self.cleanup_interval)
                
            except Exception as e:
                logger.error(f"❌ Health monitor error: {e}")
                time.sleep(self.cleanup_interval)
    
    def _start_emergency_consumer(self, consumer_name: str):
        """Start emergency consumer to process backlog"""
        try:
            # Read pending messages
            pending = self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=consumer_name,
                streams={self.stream_key: '0'},  # Read pending
                count=100,
                block=0
            )
            
            if pending:
                logger.info(f"🚑 Emergency consumer {consumer_name} processing {len(pending[0][1])} pending messages")
                # Process messages here...
                
                # Acknowledge processed messages
                for stream, messages in pending:
                    for msg_id, _ in messages:
                        self.redis.xack(
                            self.stream_key,
                            self.group_name,
                            msg_id
                        )
            
            logger.info(f"✅ Emergency consumer {consumer_name} activated")
            
        except Exception as e:
            logger.error(f"❌ Emergency consumer failed: {e}")
    
    def stop(self):
        """Stop the health monitor"""
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        logger.info(f"🛑 ConsumerGroupManager stopped for {self.group_name}")
