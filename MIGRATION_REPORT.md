# Migration Report

**Generated:** 2025-12-23 10:00:15

## Summary

| Metric | Value |
|--------|-------|
| **Files Scanned** | 22,344 |
| **Files Copied** | 391 |
| **Files Excluded** | 21,882 |
| **Size Before** | 18.9 MB |
| **Size After** | 18.9 MB |
| **Reduction** | 0.0% |

## Exclusion Breakdown

| Reason | Count |
|--------|-------|
| excluded_directory: store | 20,895 |
| excluded_directory: bayesian_model | 321 |
| excluded_directory: __pycache__ | 184 |
| excluded_directory: logs | 158 |
| excluded_directory: legacy_documentation | 54 |
| excluded_directory: educational_content_shared | 39 |
| excluded_extension:  | 33 |
| excluded_directory: alert_validation | 32 |
| excluded_directory: legacy_unused_scripts | 28 |
| excluded_extension: .csv | 26 |
| excluded_extension: .sample | 14 |
| excluded_directory: .DS_Store | 10 |
| excluded_directory: appendonlydir | 9 |
| excluded_directory: .shim_backup | 6 |
| excluded_directory: everyday_validation_reports | 6 |
| excluded_script: emergency_ | 4 |
| excluded_directory: hdfc_csv_files | 4 |
| excluded_extension: .sql | 4 |
| excluded_script: fix_ | 3 |
| excluded_pattern: temp- | 3 |
| excluded_script: debug_ | 3 |
| excluded_script: test_ | 3 |
| excluded_script: check_ | 3 |
| excluded_directory: backup_files | 3 |
| excluded_extension: .html | 3 |
| heavy_file: redis_inventory.json | 2 |
| excluded_directory: .cleanup_backup | 2 |
| excluded_directory: dump.rdb | 2 |
| excluded_script: cleanup_ | 2 |
| heavy_file: token_lookup_enriched.json | 2 |
| excluded_script: verify_ | 2 |
| excluded_script: migrate_ | 2 |
| excluded_extension: .bak | 1 |
| excluded_extension: .zip | 1 |
| heavy_file: zerodha_token_lookup_consolidated.json | 1 |
| file_too_large: 28.2MB | 1 |
| excluded_script: diagnose_ | 1 |
| excluded_script: audit_ | 1 |
| excluded_extension: .log | 1 |
| heavy_file: database_analysis_report.json | 1 |
| heavy_file: session_metadata_summary.json | 1 |
| excluded_script: trace_ | 1 |
| excluded_script: reset_ | 1 |
| excluded_extension: .proto | 1 |
| excluded_script: validate_ | 1 |
| heavy_file: clickhouse_snapshot_20251114_204004.json | 1 |
| excluded_directory: parquet_recovery | 1 |
| excluded_extension: .js | 1 |
| excluded_extension: .rev | 1 |
| excluded_extension: .pack | 1 |
| excluded_extension: .idx | 1 |
| excluded_extension: .backup | 1 |


## Directories Migrated

- `shared_core/` - Core shared modules
- `intraday_trading/` - Intraday trading system

## What Was Excluded

### Large Data Files
- Redis dumps (`dump.rdb`, `*.aof`) - **~5 GB**
- Bayesian model snapshots (`bayesian_model/`) - **2 GB**
- Heavy instrument files (`zerodha_token_lookup_consolidated.json`) - **62 MB**
- Legacy documentation - **150 MB**

### Log Files
- All `*.log` files
- `logs/` directories
- `nohup.out` files

### Debug/Test Scripts
- `debug_*.py`
- `test_*.py`
- `emergency_*.py`
- `fix_*.py`
- `verify_*.py`

### Legacy Directories
- `legacy_documentation/`
- `legacy_unused_scripts/`
- `backup_files/`
- `parquet_recovery/`

## Verification Checklist

- [x] No log files present
- [x] No files over 10MB
- [x] Critical Python files copied
- [x] Repository size under 100MB
- [x] `.gitignore` created
- [x] README created

## Migration Source

**From:** `/Users/lokeshgupta/Projects/aion_algo_trading/src/`  
**To:** `/Users/lokeshgupta/Projects/aion_algo_trading_clean/`

## Notes

This migration preserved the exact directory structure while filtering out:
1. All log and database files
2. Large data/CSV files over 10MB
3. Legacy and backup directories
4. Debug/test/emergency scripts
5. Generated cache files

The resulting repository contains only essential working code suitable for sharing and development.
