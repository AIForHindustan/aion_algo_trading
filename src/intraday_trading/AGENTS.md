# Repository Guidelines

## Project Structure & Module Organization
- **Core runtime**: `intraday_scanner/` (data pipeline, calculations, scanner orchestration). Feature detectors live in `patterns/`, alerting logic in `alerts/`, Redis helpers in `redis_files/`.
- **Data ingest**: `crawlers/` publishes ticks; configs under `config/`; reusable utilities in `utils/`; dashboards/UI assets in `aion_alert_dashboard/`.
- **Tests** stay next to the code they exercise (e.g., `patterns/test_option_patterns.py`). Logs stream to `logs/`, long-running scripts live in `scripts/`.

## Build, Test, and Development Commands
- `python -m compileall path/to/file.py` — mandatory syntax check for every touched Python file before committing.
- `python intraday_scanner/scanner_main.py --config configs/scanner.json` — run the live scanner (Redis streams + alert pipeline). Append `--continuous` when tailing ticks.
- `python utils/update_all_20day_averages.py` — refresh baseline volumes (updates Redis DB1 and ClickHouse seed data).
- `pytest patterns -k volume_spike` — targeted detector tests; drop `-k` for full suite. Use `pytest alerts` for notifier coverage.

## Coding Style & Naming Conventions
- Python 3.11+, 4-space indent, Black-compatible formatting. Imports ordered `stdlib → third-party → local`.
- Names: modules/functions `snake_case`, classes `CamelCase`, constants `UPPER_SNAKE`. Prefer f-strings for logs (e.g., `logger.info(f"🔍 [VOLUME_DEBUG] {symbol} …")`).
- Guard Redis/network calls with `try/except` and structured messages so alert pipelines can diff logs.

## Testing Guidelines
- Framework: `pytest`. Mirror tests beside implementation and name them descriptively (`test_volume_breakout_confirms_price`).
- Tag Redis-heavy or long tests with `@pytest.mark.slow`. Mock external services when feasible; otherwise hit staging Redis via env vars.
- Before pushing: `python -m compileall` on modified files plus the narrowest `pytest` command covering new logic.

## Commit & Pull Request Guidelines
- Commit style: `component: summary` (e.g., `patterns: enforce redis symbol variants`). Squash WIP commits before opening a PR.
- PRs should explain intent, list compileall/pytest commands + results, link tickets, attach logs/screenshots for alert or dashboard changes, and note any Redis key additions/migrations.
- Document operational steps (backfills, stream trims, ClickHouse migrations) in the PR description or linked runbook.

## Security & Configuration Tips
- Never commit secrets; keep `REDIS_HOST`, `REDIS_PASSWORD`, API tokens in env vars or vault. Validate new keys via `redis_files/redis_key_standards.py`.
- Redis DB1 powers live trading—reuse `RedisManager82`/`redis_gateway` for connections, and log risky write operations. Keep `configs/scanner.json` aligned with actual Redis ports/DBs before restarting live scanners.
