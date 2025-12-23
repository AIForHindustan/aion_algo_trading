# caching/predictive_prefetch.py
import time
import numpy as np
from typing import Dict, List, Set, Optional, Any
from collections import defaultdict, deque
import threading

from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder

class PredictivePrefetcher:
    """Predict and pre-fetch data before it's needed"""
    
    def __init__(self, redis_client, lookahead_seconds: int = 5):
        self.redis = redis_client
        self.lookahead = lookahead_seconds
        self.access_patterns = defaultdict(lambda: deque(maxlen=1000))
        self.prediction_cache = {}
        self.prefetch_thread = None
        self.running = False
        
        # ML model for prediction (simple Markov chain)
        self.transition_matrix = defaultdict(lambda: defaultdict(int))
        self.symbol_weights = defaultdict(int)
        
        # Thread safety
        self.lock = threading.RLock()
    
    def record_access(self, symbol: str, data_type: str):
        """Record data access pattern"""
        with self.lock:
            key = f"{symbol}:{data_type}"
            self.access_patterns[symbol].append((data_type, time.time()))
            self.symbol_weights[symbol] += 1
            
            # Update transition matrix
            if len(self.access_patterns[symbol]) > 1:
                prev_type, _ = self.access_patterns[symbol][-2]
                self.transition_matrix[prev_type][data_type] += 1
    
    def predict_next_access(self, symbol: str) -> List[str]:
        """Predict next data types needed for a symbol"""
        with self.lock:
            if symbol not in self.access_patterns or not self.access_patterns[symbol]:
                return ['indicators', 'greeks']  # Default prediction
            
            last_access = self.access_patterns[symbol][-1]
            last_type = last_access[0]
            
            # Get most likely next access types
            transitions = self.transition_matrix.get(last_type, {})
            if transitions:
                sorted_types = sorted(transitions.items(), key=lambda x: x[1], reverse=True)
                return [t[0] for t in sorted_types[:3]]
            
            return ['indicators', 'greeks', 'tick']
    
    def prefetch_for_symbol(self, symbol: str):
        """Pre-fetch predicted data for a symbol"""
        predicted_types = self.predict_next_access(symbol)
        
        with self.redis.pipeline() as pipe:
            for data_type in predicted_types:
                if data_type == 'indicators':
                    pipe.hgetall(DatabaseAwareKeyBuilder.live_indicator_hash(symbol))
                elif data_type == 'greeks':
                    pipe.hgetall(DatabaseAwareKeyBuilder.live_greeks_hash(symbol))
                elif data_type == 'tick':
                    pipe.hgetall(DatabaseAwareKeyBuilder.live_ticks_latest(symbol))
                elif data_type == 'depth':
                    pipe.get(f"depth:compressed:{symbol}")
            
            results = pipe.execute()
            
            # Cache results
            cache_key = f"prefetch:{symbol}"
            self.prediction_cache[cache_key] = {
                'data': results,
                'timestamp': time.time(),
                'predicted_types': predicted_types
            }
    
    def get_prefetched(self, symbol: str, data_type: str) -> Optional[Any]:
        """Get pre-fetched data if available"""
        cache_key = f"prefetch:{symbol}"
        
        if cache_key in self.prediction_cache:
            cache_entry = self.prediction_cache[cache_key]
            
            # Check if cache is fresh (last 2 seconds)
            if time.time() - cache_entry['timestamp'] < 2.0:
                idx = cache_entry['predicted_types'].index(data_type)
                if idx < len(cache_entry['data']):
                    return cache_entry['data'][idx]
        
        return None
    
    def start_prefetch_worker(self):
        """Start background thread for prefetching"""
        self.running = True
        
        def worker():
            while self.running:
                try:
                    # Get hottest symbols (most accessed)
                    with self.lock:
                        hot_symbols = sorted(
                            self.symbol_weights.items(),
                            key=lambda x: x[1],
                            reverse=True
                        )[:20]  # Top 20 symbols
                    
                    # Pre-fetch for hot symbols
                    for symbol, _ in hot_symbols:
                        self.prefetch_for_symbol(symbol)
                    
                    time.sleep(0.1)  # 100ms interval
                    
                except Exception as e:
                    print(f"Prefetch worker error: {e}")
                    time.sleep(1)
        
        self.prefetch_thread = threading.Thread(target=worker, daemon=True)
        self.prefetch_thread.start()
    
    def stop_prefetch_worker(self):
        """Stop prefetch worker"""
        self.running = False
        if self.prefetch_thread:
            self.prefetch_thread.join(timeout=2.0)
