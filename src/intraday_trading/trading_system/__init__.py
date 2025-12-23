"""
Trading System - Unified Package Structure
==========================================

Core Components:
- core.data_ingestor: WebSocket → Redis tick processing
- redis_files.volume_manager: Volume ratios, profiles, baselines
- core.indicator_calculator: Technical indicators + Greeks
- core.pattern_detector: Pattern detection + alerts

Storage:
- storage.redis_manager: Redis connection management
- storage.data_migrator: Data migration utilities

Configuration:
- config.settings: System settings
- config.instruments: Instrument metadata
"""

__version__ = "1.0.0"

