# tick_history/ring_buffer.py
import numpy as np
from collections import deque
import threading
from typing import List, Dict, Any, Optional

class TickRingBuffer:
    """High-performance circular buffer for tick history"""
    
    def __init__(self, max_size: int = 1000, preallocate: bool = True):
        self.max_size = max_size
        self.buffer = deque(maxlen=max_size)
        self.lock = threading.RLock()
        
        # Pre-allocate memory for performance
        if preallocate:
            self._preallocate_memory()
        
        # Statistics
        self.stats = {
            'inserts': 0,
            'reads': 0,
            'overwrites': 0,
            'memory_bytes': 0
        }
    
    def _preallocate_memory(self):
        """Pre-allocate memory to avoid fragmentation"""
        # Create numpy arrays for numeric fields
        self.price_buffer = np.zeros(self.max_size, dtype=np.float32)
        self.volume_buffer = np.zeros(self.max_size, dtype=np.int32)
        self.timestamp_buffer = np.zeros(self.max_size, dtype=np.float64)
        
        self.current_idx = 0
        self.buffer_length = 0
    
    def add_tick(self, tick: Dict[str, Any]):
        """Add tick to ring buffer (thread-safe)"""
        with self.lock:
            if self.buffer_length < self.max_size:
                self.buffer_length += 1
            else:
                self.stats['overwrites'] += 1
            
            # Store in numpy arrays for fast access
            idx = self.current_idx
            self.price_buffer[idx] = tick.get('last_price', 0.0)
            self.volume_buffer[idx] = tick.get('volume', 0)
            self.timestamp_buffer[idx] = tick.get('timestamp', 0.0)
            
            # Also store in deque for complex objects
            self.buffer.append(tick.copy())
            
            self.current_idx = (self.current_idx + 1) % self.max_size
            self.stats['inserts'] += 1
    
    def get_last_n(self, n: int) -> List[Dict[str, Any]]:
        """Get last N ticks (most recent first)"""
        with self.lock:
            self.stats['reads'] += 1
            
            if n <= 0:
                return []
            
            # Use numpy slicing for fast numeric access
            if n <= self.buffer_length:
                # Get indices (wrapping around if needed)
                start_idx = (self.current_idx - n) % self.max_size
                indices = [(start_idx + i) % self.max_size for i in range(n)]
                
                # Reconstruct ticks from numpy arrays
                ticks = []
                for idx in indices:
                    tick = {
                        'last_price': float(self.price_buffer[idx]),
                        'volume': int(self.volume_buffer[idx]),
                        'timestamp': float(self.timestamp_buffer[idx])
                    }
                    # Merge with any additional fields from deque
                    deque_idx = (self.buffer_length - n + len(ticks)) % self.max_size
                    if deque_idx < len(self.buffer):
                        tick.update(list(self.buffer)[deque_idx])
                    ticks.append(tick)
                
                return list(reversed(ticks))
            
            return list(self.buffer)[-n:]
    
    def get_statistics(self) -> Dict[str, float]:
        """Calculate statistics on the buffer"""
        if self.buffer_length == 0:
            return {}
        
        with self.lock:
            # Use numpy for fast calculations
            prices = self.price_buffer[:self.buffer_length]
            volumes = self.volume_buffer[:self.buffer_length]
            
            return {
                'avg_price': float(np.mean(prices)),
                'std_price': float(np.std(prices)),
                'min_price': float(np.min(prices)),
                'max_price': float(np.max(prices)),
                'total_volume': int(np.sum(volumes)),
                'avg_volume': float(np.mean(volumes)),
                'buffer_fill_percent': (self.buffer_length / self.max_size) * 100
            }