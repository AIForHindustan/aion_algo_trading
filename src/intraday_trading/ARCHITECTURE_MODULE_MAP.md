# Architecture & Module Usage Map

This document explains how every major component in the intraday trading stack fits together, which helper modules it pulls in, and where to find the supporting scripts/configs. Use it as the source of truth while modularising the codebase.

---

## 1. Configuration & Field Mapping

| File / Dir | Purpose | Key Imports / Dependencies |
|------------|---------|----------------------------|
| `config/zerodha_config.json` (and friends) | Zerodha API credentials, instrument universes, session toggles, reconnection limits. Loaded by crawlers. | `crawlers/zerodha_websocket/intraday_crawler.py` |
| `config/scanner_config.json` | Scanner runtime wiring (Redis host/DB, detector toggles, alert routing). | `intraday_scanner/scanner_main.py` |
| `config/optimized_field_mapping.yaml` | Canonical schema for WebSocket → Redis → pipeline fields (core, redis session, indicator, volume profile). | `utils/yaml_field_loader.py`, `crawlers/websocket_message_parser.py`, `intraday_scanner/data_pipeline.py` |
| `utils/yaml_field_loader.py` | Loads mapping YAML, exposes helpers (`resolve_session_field`, `resolve_calculated_field`, etc.). | Used by crawlers, pipeline, redis storage. |

Helper scripts:  
`FIELD_MAPPING_ANALYSIS.md`, `CONFIGURATION_VERIFICATION_REPORT.md` document mapping expectations and sanity checks.

---

## 2. Data Acquisition (Zerodha WebSocket → Redis)

### `crawlers/zerodha_websocket/intraday_crawler.py`
- **Role**: Main real-time crawler. Authenticates via Zerodha config, connects to WebSocket, manages subscription list, processes ticks, publishes to Redis streams/hashes, writes Parquet snapshots.
- **Imports / Helpers**:
  - `ZerodhaWebSocketMessageParser`, `EnhancedTickParser`
  - `TimestampNormalizer` (`config/utils/timestamp_normalizer.py`)
  - `metadata_resolver`, `get_field_mapping_manager`
  - `redis_files.redis_client.redis_manager`
  - Publishes to `ticks:raw:binary`, `ticks:intraday:processed`, `live_ticks_hash`, `price:latest:*`

### `crawlers/websocket_message_parser.py`
- **Role**: Decodes 184-byte packets, enriches with Greeks/volume/price-change, handles symbol parsing and error tracking.
- **Imports**: `ZerodhaBinaryParser`, `VolumeManager`, `redis_key_standards`, `time_aware_volume_baseline`, `intraday_scanner.calculations`.

### Data helpers
- Token metadata (`crawlers/token_files/*.json`)
- Health scripts: `crawler_health_check.py`, `redis_audit*.py`, `cleanup_all_stale_consumers.py`

---

## 3. Redis Storage & Access Layer

| Module | Functionality | Depends On |
|--------|---------------|------------|
| `redis_files/redis_client.py` | Manages pooled clients (`RedisManager82`, `redis_gateway`, wrappers). All runtime components request DB clients through here. | `redis`, `config/scanner_config.json` |
| `redis_files/unified_data_storage.py` | Normalised read/write for ticks, indicators, Greeks, rolling windows; used by scanner and alert manager. | `redis_client`, `optimized_field_mapping` |
| `redis_files/redis_key_standards.py` | Canonical key builder + symbol parser; ensures consistent key names for ticks/indicators/streams. | All crawler & pipeline code. |
| `redis_files/volume_manager.py` | Calculates volume baselines, ratios, incremental volumes, logs deep debug info. | `utils/time_aware_volume_baseline`, `redis_client` |

Support scripts:  
`redis_audit.py`, `redis_audit2.py`, `redis_audit3.py`, `cleanup_all_stale_consumers.py`, `check_redis_stream.py`, `redis_migration_db1_to_db2.py`.

---

## 4. Scanner / Data Pipeline

### `intraday_scanner/scanner_main.py`
- Boots the runtime: loads config, creates Redis clients, instantiates `DataPipeline`, pattern detector, alert manager/notifier combo.
- CLI flags (`--config`, `--continuous`, `--debug`).

### `intraday_scanner/data_pipeline.py`
- Consumes `ticks:intraday:processed` via `XREADGROUP`.
- Cleans tick payloads (`_clean_tick_data_core`), merges Redis indicators (`_get_indicators_from_redis`), enforces freshness, then dispatches to pattern detector.
- Imports: `RedisManager82`, `PatternDetector`, `TimestampNormalizer`, `utils/time_aware_volume_baseline`, `redis_files.unified_data_storage`.

### `intraday_scanner/calculations.py`
- Central math/greeK library: implied vol, Greeks (`vollib`), expected move, ATR, EMA, underlying price lookup, futures symbol derivation.
- Used by `websocket_message_parser`, `pattern_detector`, `Game Theory`, `Alert templates`.

### MATH/dispatch helpers
- `MATH_DISPATCHER_FLOW_TRACE.md`
- `intraday_scanner/calculations_original.py` (legacy reference in parser logs)

---

## 5. Pattern Detection Layer

| Module | Purpose | Helper Imports |
|--------|---------|----------------|
| `patterns/pattern_detector.py` | Master orchestrator: pre-validates ticks, runs volume/momentum/ICT/delta/killzone detectors, logs decisions. | `VolumeProfileManager`, `VolumeManager`, `ICT_*` modules, `Game Theory`, `Kow Straddle`, `redis_files.unified_data_storage`. |
| `patterns/kow_signal_straddle.py` | VWAP-based straddle strategy, entry/exit logic, position management instructions. | `redis_files.unified_data_storage`, `time_aware_volume_baseline`, `intraday_scanner.calculations`. |
| `patterns/volume_profile_manager.py` | Builds price-volume histogram, persists to Redis, used by pattern detector + alerts. | `redis_client`, `polars` (if available). |
| `patterns/game_theory_alerts.py`, `patterns/ict_*`, `patterns/volume_patterns.py`, etc. | Specialised detectors for ICT killzones, premium/discount, spoofing, delta reversal, momentum, etc. | `intraday_scanner.calculations`, `utils/*`, `redis`. |

Tests & docs next to code: `patterns/test_*.py`, `PATTERN_SYSTEM_STATUS.md`, `PATTERN_WIRING_STATUS.md`.

---

## 6. Alerting Pipeline

### `alerts/alert_manager.py`
- Receives pattern outputs, applies filters (`alerts/filters.py`), groups duplicates, enriches with news (`alerts/news_enrichment_integration.py`), calculates risk/position size (`alerts/risk_manager.py`), then passes to notifiers.

### `alerts/notifiers.py`
- Houses `TelegramNotifier`, `RedisNotifier`, and `HumanReadableAlertTemplates`.
- Imports: `requests`, `redis_files.unified_data_storage`, `intraday_scanner.calculations` (for Greeks fallback), `config/telegram_config.json`.
- Formatting pipeline handles HTML/plain text, timestamp localisation, dedup, Telegram API posting.

### Config & Docs
- `alerts/config/telegram_config.json` – channel routing, tokens, `plain_text_mode`, min confidence filters.
- `ALERT_SYSTEM_ANALYSIS.md`, `ALERT_BLOCKING_COMPLETE_FIX.md`, `ALERT_SYSTEM_FIXES.md` – history of alert issues + fixes.
- Validation suite: `alert_validation/` (validators, dashboards, CLI testers).

Helper scripts: `debug_data_flow.py`, `alert_validator.out`, `monitor_and_process.sh`.

---

## 7. Utilities & Scripts

| Script / Dir | Usage |
|--------------|-------|
| `scripts/` | Operational automation (start/stop scanners, restart tunnels, ClickHouse ingestion, etc.). |
| `utils/time_aware_volume_baseline.py` | Calculates time-sliced baselines for volume manager, parser, pipeline. |
| `utils/update_all_20day_averages.py` | Refreshes baseline volumes (Redis DB1 + ClickHouse seeds). |
| `debug_redis_data.py`, `stream_debug.py`, `diagnose_binary_format.py` | Debug helpers for ops team. |
| `clickhouse_setup/` | Connectors that bridge Redis streams to ClickHouse, plus schema management. |
| `scripts/start_scanner_clean.sh`, `process_overnight.sh`, `setup_tunnels.sh`, `deploy_production.sh` | Deployment/playbook scripts. |

Documentation near each helper: `SERVICE_MONITORING.md`, `SCANNER_RESTART_PROTOCOL.md`, `DEPLOYMENT_GUIDE.md`.

---

## 8. Ops / Monitoring Artifacts

- **Logs** (`logs/scanner_detailed.log*`, `logs/alerts/*`, `logs/data_pipeline/*`) – each major component writes to its own file with timestamped debug lines (`[TICK_FRESHNESS]`, `[TELEGRAM]`, `[KOW_SIGNAL]`, etc.).
- **Dashboards** – `aion_alert_dashboard/`, `alert_validation/` output, `View Dashboard` link served via Cloudflare tunnel (`START_DASHBOARD.sh` uses `cloudflared`).
- **Audits & Reports** – numerous Markdown summaries (e.g., `COMPLETE_DATA_FLOW_TRACE.md`, `CRAWLER_HELPERS.md`, `REDIS_STREAM_FIX.md`) documenting previous incidents and their resolution.

---

## Quick Dependency Cheatsheet

- **Crawler stack** depends on: `config/zerodha_config.json`, `metadata_resolver`, `TimestampNormalizer`, `redis_key_standards`, `VolumeManager`, `intraday_scanner.calculations`.
- **Scanner/data pipeline** depends on: `RedisManager82`, `unified_data_storage`, `PatternDetector`, `TimestampNormalizer`, `time_aware_volume_baseline`.
- **Patterns** depend on: `intraday_scanner.calculations`, `VolumeProfileManager`, `redis_files` modules, `utils/yaml_field_loader`.
- **Alerts** depend on: `alerts/config/telegram_config.json`, `HumanReadableAlertTemplates`, `unified_data_storage`, `requests`, `alerts/filters`, `alerts/risk_manager`.
- **Utilities/scripts** provide supporting tooling for ingestion, deployment, and debugging; most rely on `redis_files.redis_client` and/or `config` modules.

---

Keep this map updated whenever new modules or scripts are added—modularisation work should reference this document to ensure dependencies remain explicit and shared helpers live in the correct package.

