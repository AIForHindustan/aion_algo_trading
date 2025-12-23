# Trading System Restructuring Plan

## Directory Structure

```
trading_system/
├── core/                           # MAIN COMPONENTS (4 core files)
│   ├── data_ingestor.py           # WebSocket → Redis (handles all tick processing)
│   ├── volume_engine.py           # Volume ratios, profiles, baselines (TA-LIB + vollib)
│   ├── indicator_calculator.py    # Technical indicators + Greeks (unified)
│   └── pattern_detector.py        # Pattern detection + alerts
├── storage/                       # REDIS MANAGEMENT (2 files)
│   ├── redis_manager.py           # Single Redis interface for ALL data
│   └── data_migrator.py           # Migration and backup utilities
├── config/                        # CONFIGURATION (2 files)
│   ├── settings.py                # All system settings
│   └── instruments.py             # Instrument metadata
└── scripts/                       # OPERATIONAL SCRIPTS (6 essential scripts)
    ├── start_system.py
    ├── stop_system.py
    ├── health_check.py
    ├── backup_data.py
    ├── migrate_redis.py
    └── debug_pipeline.py
```

## File Mapping

### core/data_ingestor.py
**Consolidates:**
- `crawlers/zerodha_websocket/intraday_crawler.py` - Main crawler class
- `crawlers/websocket_message_parser.py` - WebSocket message parsing
- `intraday_scanner/data_pipeline.py` - Data pipeline (Redis subscription)
- `crawlers/base_crawler.py` - Base crawler functionality (if needed)

**Key Responsibilities:**
- WebSocket connection management
- Binary message parsing
- Tick data processing
- Publishing to Redis streams
- Error handling and reconnection

### core/volume_engine.py
**Consolidates:**
- `redis_files/volume_manager.py` - Unified volume manager
- `utils/correct_volume_calculator.py` - Volume ratio calculations
- `utils/time_aware_volume_baseline.py` - Time-aware baselines
- `redis_files/volume_state_manager.py` - Volume state management

**Key Responsibilities:**
- Volume ratio calculation
- Volume profile management
- Baseline calculations
- Incremental volume tracking

### core/indicator_calculator.py
**Consolidates:**
- `intraday_scanner/calculations.py` - HybridCalculations class
- Technical indicator calculations (RSI, MACD, Bollinger, etc.)
- Option Greeks calculations

**Key Responsibilities:**
- Technical indicator calculations (TA-Lib + Polars)
- Option Greeks (delta, gamma, theta, vega, rho)
- Indicator caching
- Batch processing

### core/pattern_detector.py
**Moves:**
- `patterns/pattern_detector.py` → `core/pattern_detector.py`

**Key Responsibilities:**
- Pattern detection (8 core patterns)
- Alert generation
- Pattern validation

### storage/redis_manager.py
**Moves:**
- `redis_files/redis_manager.py` → `storage/redis_manager.py`

**Key Responsibilities:**
- Redis connection pooling
- Process-specific connection management
- Redis 8.2 optimizations

### storage/data_migrator.py
**Moves:**
- `safe_consolidation.py` → `storage/data_migrator.py`

**Key Responsibilities:**
- Redis data migration
- Database consolidation
- Backup utilities

### config/settings.py
**Consolidates:**
- `config/thresholds.py` - VIX regime thresholds
- `redis_files/redis_config.py` - Redis configuration
- `crawlers/crawler_config.py` - Crawler configuration
- Environment variable settings

**Key Responsibilities:**
- System-wide settings
- Redis configuration
- Process pool configuration
- Feature flags

### config/instruments.py
**Consolidates:**
- `core/data/token_lookup_enriched.json` - Token metadata
- `crawlers/metadata_resolver.py` - Metadata resolution
- `crawlers/hot_token_mapper.py` - Token mapping

**Key Responsibilities:**
- Instrument metadata
- Token-to-symbol mapping
- Exchange information

### scripts/
**Creates new operational scripts:**
- `start_system.py` - Start all system components
- `stop_system.py` - Graceful shutdown
- `health_check.py` - System health monitoring
- `backup_data.py` - Data backup utilities
- `migrate_redis.py` - Redis migration wrapper
- `debug_pipeline.py` - Pipeline debugging tools

## Migration Steps

1. ✅ Create directory structure
2. Move/consolidate core components
3. Move/consolidate storage components
4. Move/consolidate config files
5. Create operational scripts
6. Update all imports
7. Test system functionality
8. Remove old files (after verification)

