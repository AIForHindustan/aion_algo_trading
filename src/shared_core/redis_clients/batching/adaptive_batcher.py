# batching/adaptive_batcher.py
import time
import statistics
from typing import List, Dict, Any
from collections import deque
import threading

class AdaptiveBatcher:
    """Adaptive batching based on system load"""
    
    def __init__(self, 
                 min_batch_size: int = 10,
                 max_batch_size: int = 200,
                 target_latency_ms: int = 20):
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.target_latency = target_latency_ms / 1000.0
        
        self.current_batch_size = min_batch_size
        self.latency_history = deque(maxlen=100)
        self.batch_history = deque(maxlen=100)
        
        # PID controller parameters
        self.kp = 0.8  # Proportional gain
        self.ki = 0.2  # Integral gain
        self.kd = 0.1  # Derivative gain
        
        self.integral = 0.0
        self.prev_error = 0.0
        
        self.lock = threading.RLock()
        
        # Statistics
        self.stats = {
            'total_batches': 0,
            'avg_batch_size': 0.0,
            'avg_latency': 0.0,
            'adjustments': 0
        }
    
    def record_batch(self, batch_size: int, processing_time: float):
        """Record batch performance"""
        with self.lock:
            self.latency_history.append(processing_time)
            self.batch_history.append(batch_size)
            self.stats['total_batches'] += 1
            
            # Update statistics
            if self.latency_history:
                self.stats['avg_latency'] = statistics.mean(self.latency_history)
            if self.batch_history:
                self.stats['avg_batch_size'] = statistics.mean(self.batch_history)
    
    def calculate_optimal_batch_size(self) -> int:
        """Calculate optimal batch size using PID controller"""
        with self.lock:
            if not self.latency_history:
                return self.current_batch_size
            
            # Calculate error (difference from target latency)
            current_latency = statistics.mean(self.latency_history) if self.latency_history else 0
            error = current_latency - self.target_latency
            
            # PID calculation
            self.integral += error
            derivative = error - self.prev_error
            
            # Calculate adjustment
            adjustment = (
                self.kp * error +
                self.ki * self.integral +
                self.kd * derivative
            )
            
            # Convert adjustment to batch size change
            # Each 0.01s error = ±5 batch size
            batch_adjustment = int(adjustment * 500)  # Scale factor
            
            new_batch_size = self.current_batch_size - batch_adjustment
            
            # Clamp to bounds
            new_batch_size = max(self.min_batch_size, min(self.max_batch_size, new_batch_size))
            
            # Only adjust if change is significant
            if abs(new_batch_size - self.current_batch_size) >= 5:
                self.current_batch_size = new_batch_size
                self.stats['adjustments'] += 1
                
                print(f"🔄 Adaptive batch size: {self.current_batch_size} "
                      f"(latency: {current_latency*1000:.1f}ms, target: {self.target_latency*1000:.1f}ms)")
            
            self.prev_error = error
            return self.current_batch_size
    
    def should_flush_batch(self, current_batch_size: int, time_since_last_flush: float) -> bool:
        """Determine if batch should be flushed"""
        with self.lock:
            # Rule 1: Batch is full
            if current_batch_size >= self.current_batch_size:
                return True
            
            # Rule 2: Timeout based on current latency
            avg_latency = self.stats['avg_latency']
            timeout = min(0.5, max(0.05, avg_latency * 2))
            
            if time_since_last_flush > timeout:
                return True
            
            # Rule 3: Emergency flush if latency is spiking
            if self.latency_history and len(self.latency_history) >= 5:
                recent = list(self.latency_history)[-5:]
                if max(recent) > self.target_latency * 3:
                    return True
            
            return False
    
    def get_recommendation(self) -> Dict[str, Any]:
        """Get batching recommendations"""
        with self.lock:
            return {
                'current_batch_size': self.current_batch_size,
                'recommended_batch_size': self.calculate_optimal_batch_size(),
                'avg_latency_ms': self.stats['avg_latency'] * 1000,
                'target_latency_ms': self.target_latency * 1000,
                'latency_violation': self.stats['avg_latency'] > self.target_latency,
                'batch_efficiency': (self.stats['avg_batch_size'] / self.current_batch_size) * 100
            }