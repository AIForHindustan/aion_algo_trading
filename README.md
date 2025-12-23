# Aion Algo Trading - Clean Repository

**Migration Date:** 2025-12-23 10:00:15

## Overview

This is a **clean, lean repository** containing only the essential working code from the Aion Algo Trading system, specifically:

- `shared_core/` - Core shared utilities, Redis clients, calculations
- `intraday_trading/` - Intraday scanner and trading logic

## What's Included

✅ All Python source code (`.py`)  
✅ Essential configuration files (`.json`, `.yaml`, `.toml`)  
✅ Documentation (`.md`)  
✅ Shell scripts (`.sh`)  
✅ Requirements files  

## What's Excluded

❌ Log files (`*.log`, `logs/`)  
❌ Database files (`*.rdb`, `*.duckdb`, `dump.rdb`)  
❌ Large data files (`*.parquet`, `*.csv`, `*.pkl`)  
❌ Legacy directories (`legacy_*`, `backup_*`)  
❌ Bayesian model snapshots (2GB of dated JSONs)  
❌ Redis dumps and AOF files (5+ GB)  
❌ Debug/test/emergency scripts  

## Repository Size

**Before:** ~12 GB (shared_core + intraday_trading)  
**After:** ~50-100 MB  
**Reduction:** ~99%  

## Purpose

This repository was created to:
1. Enable easy sharing and collaboration on intraday scanner issues
2. Remove all non-essential files, logs, and data dumps
3. Provide a clean starting point for development
4. Reduce repository size for faster cloning and CI/CD

## Migration Notes

See `MIGRATION_REPORT.md` for detailed statistics on what was migrated.

## Original Repository

The full repository with all data files remains at:
`/Users/lokeshgupta/Projects/aion_algo_trading/`

## Getting Started

```bash
# Navigate to the repository
cd /Users/lokeshgupta/Projects/aion_algo_trading_clean/src

# Check shared_core modules
ls shared_core/

# Check intraday_trading
ls intraday_trading/intraday_scanner/
```

## Key Components

### Shared Core
- `calculation_service.py` - Core calculation engine
- `redis_clients/` - Redis connection and client utilities
- `market_calendar.py` - Market timing and session management
- `index_manager.py` - Index data management

### Intraday Trading
- `intraday_scanner/data_pipeline.py` - Data processing pipeline
- `intraday_scanner/scanner_main.py` - Main scanner entry point
- `intraday_scanner/calculations.py` - Scanner calculations
- `patterns/` - Pattern detection modules
- `config/` - Configuration management

## Notes

⚠️ This repository contains **code only**. It does not include:
- Historical data
- Trained ML models
- Log files
- Database dumps
- Token/credential files

For a complete working environment, you'll need to set up:
- Redis databases
- Configuration files with your credentials
- Instrument data files
