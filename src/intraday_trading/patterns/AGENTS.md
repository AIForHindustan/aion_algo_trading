# Repository Guidelines

## Project Structure & Module Organization
- `patterns/` contains pattern detectors such as `kow_signal_straddle.py`; tests for each module live beside the implementation (`patterns/test_*`).
- `intraday_scanner/` is the realtime engine (data pipeline, math dispatcher, scanner entrypoints).
- `alerts/` houses alert manager, notifiers, risk filters, and templates; `alert_validation/` stores downstream validators.
- Config JSON/YAML files are under `config/`, while helper scripts live in `scripts/` or `utils/`.
- Logs and dashboards are placed in `logs/` and `aion_alert_dashboard/` respectively; Redis utilities reside in `redis_files/`.

## Build, Test, and Development Commands
- `python -m compileall path/to/file.py` – fast syntax check; run on every touched Python file (e.g., `python -m compileall patterns/kow_signal_straddle.py`).
- `python intraday_scanner/scanner_main.py --config configs/scanner.json` – launches the realtime scanner end-to-end.
- `python utils/update_all_20day_averages.py` – refreshes rolling volume baselines and syncs Redis DB1.
- `pytest patterns -k volume_spike` – run detector-specific suites; drop `-k` for the full battery.

## Coding Style & Naming Conventions
- Python 3.11+, 4-space indentation, Black-compatible formatting.
- Modules use `snake_case`, classes `CamelCase`, constants `UPPER_SNAKE`.
- Imports sorted `stdlib` → third-party → local. Prefer f-strings for logs/metrics.

## Testing Guidelines
- Use `pytest` with fixtures stored beside the feature (e.g., `patterns/test_kow_signal_straddle.py`).
- Tag Redis-heavy suites with `@pytest.mark.slow`.
- Name tests descriptively (`test_volume_breakout_confirms_price`). Document failing symbols for replay.

## Commit & Change Submission Guidelines
- Use the `component: summary` style for commits (e.g., `alerts: tighten notifier template`); keep each commit focused and squash noisy WIP history.
- Every change description should mention `compileall` + relevant `pytest` runs and include links to tracked issues or problem statements in your local tracker.
- When a change affects alerts, dashboards, or Redis schemas, attach screenshots/logs and explicitly note new keys/config migrations plus any manual follow-up (e.g., “run `python utils/update_all_20day_averages.py` after deploy”).

## Security & Configuration Tips
- Secrets belong in env vars (`REDIS_HOST`, `REDIS_PASSWORD`); never commit credentials or dumps.
- Redis DB1 is production-critical—reuse existing connection managers, and document new stream keys in `redis_files/redis_key_standards.py`.
