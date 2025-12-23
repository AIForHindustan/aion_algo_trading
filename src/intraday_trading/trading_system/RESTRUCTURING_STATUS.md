# Trading System Restructuring - Current Status

## ✅ Completed

1. **Directory Structure Created**
   - `trading_system/core/` - Core components
   - `trading_system/storage/` - Storage management
   - `trading_system/config/` - Configuration
   - `trading_system/scripts/` - Operational scripts

2. **Files Moved**
   - ✅ `redis_files/redis_manager.py` → `trading_system/storage/redis_manager.py`
   - ✅ `safe_consolidation.py` → `trading_system/storage/data_migrator.py`
   - ✅ `patterns/pattern_detector.py` → `trading_system/core/pattern_detector.py`

3. **Package Initialization**
   - Created `__init__.py` files for all packages

## 🔄 In Progress / Pending

### Core Components (Need Consolidation)

1. **core/data_ingestor.py** - Needs consolidation of:
   - `crawlers/zerodha_websocket/intraday_crawler.py`
   - `crawlers/websocket_message_parser.py`
   - `intraday_scanner/data_pipeline.py`
   - `crawlers/base_crawler.py` (base functionality)

2. **core/volume_engine.py** - Needs consolidation of:
   - `redis_files/volume_manager.py`
   - `utils/correct_volume_calculator.py`
   - `utils/time_aware_volume_baseline.py`
   - `redis_files/volume_state_manager.py`

3. **core/indicator_calculator.py** - Needs consolidation of:
   - `intraday_scanner/calculations.py` (HybridCalculations)
   - Option Greeks calculations

4. **core/pattern_detector.py** - ✅ Moved (needs import updates)

### Storage Components

1. **storage/redis_manager.py** - ✅ Moved (needs import updates)
2. **storage/data_migrator.py** - ✅ Moved (imports updated)

### Configuration

1. **config/settings.py** - Needs consolidation of:
   - `config/thresholds.py`
   - `redis_files/redis_config.py`
   - `crawlers/crawler_config.py`
   - Environment variable settings

2. **config/instruments.py** - Needs consolidation of:
   - `core/data/token_lookup_enriched.json` (reference)
   - `crawlers/metadata_resolver.py`
   - `crawlers/hot_token_mapper.py`

### Scripts

1. **scripts/start_system.py** - Create new
2. **scripts/stop_system.py** - Create new
3. **scripts/health_check.py** - Create new
4. **scripts/backup_data.py** - Create new
5. **scripts/migrate_redis.py** - Create new (wrapper for data_migrator)
6. **scripts/debug_pipeline.py** - Create new

## 📋 Next Steps

1. **Consolidate Core Components**
   - Merge data ingestion files into `core/data_ingestor.py`
   - Merge volume files into `core/volume_engine.py`
   - Merge indicator files into `core/indicator_calculator.py`

2. **Consolidate Configuration**
   - Merge config files into `config/settings.py`
   - Merge instrument files into `config/instruments.py`

3. **Create Operational Scripts**
   - Implement all 6 scripts in `scripts/` directory

4. **Update Imports**
   - Update all imports throughout codebase to use new structure
   - Update `trading_system/core/pattern_detector.py` imports
   - Update `trading_system/storage/redis_manager.py` if needed

5. **Testing & Verification**
   - Test each component after consolidation
   - Verify imports work correctly
   - Test operational scripts

## ⚠️ Important Notes

- **Do NOT delete old files yet** - Keep them until new structure is fully tested
- **Update imports gradually** - Test after each major import update
- **Maintain backward compatibility** - Consider keeping old imports working during transition

