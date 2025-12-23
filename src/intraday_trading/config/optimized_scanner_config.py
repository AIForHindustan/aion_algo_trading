
# config/optimized_scanner_config.py
"""
Optimal configuration based on benchmark results
"""

PRODUCTION_CONFIG = {
    # Batch Processing
    'BATCH_SIZE': 200,  # Max throughput point
    'PROCESSING_INTERVAL_MS': 50,  # ~20Hz processing rate
    
    # Redis Optimization
    'PIPELINE_SIZE': 100,  # Commands per pipeline
    'CONNECTION_POOL_SIZE': 10,  # For concurrent access
    
    # Pattern Detection
    'MIN_CONFIDENCE': 0.65,  # Lower than test for production
    'MAX_PATTERNS_PER_SYMBOL': 3,  # Limit noise
    
    # Resource Management
    'MAX_MEMORY_MB': 500,
    'CPU_THREADS': 4,  # Match your M4 cores
    'QUEUE_SIZE': 5000,  # Tick buffer
}

# Scanner-specific tuning
SCANNER_TUNING = {
    'symbols_per_batch': 200,
    'batch_interval_ms': 50,
    'concurrent_batches': 2,  # Process 2 batches in parallel
    'redis_pipeline_size': 100,
    'enable_compression': True,  # For large indicator sets
}
