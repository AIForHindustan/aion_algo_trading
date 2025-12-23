Key import/legacy issues in aion_trading_dashboard (backend):
Package name: the live dashboard is aion_trading_dashboard; there is no aion_dashboard. Any references to aion_dashboard would be missing/legacy—none found; wiring must target aion_trading_dashboard.
Aggressive sys.path hacking in backend/optimized_main.py keeps adding parent roots (aion_algo_trading, intraday_trading, zerodha_websocket) instead of relying on package installs. This creates hidden coupling and mismatched import resolution order.
38:52:aion_trading_dashboard/backend/optimized_main.py
current_dir = ...
for path in (str(parent_project_root), str(project_root)):
    ...
    sys.path.insert(0, path)
38:52:aion_trading_dashboard/backend/optimized_main.pycurrent_dir = ...for path in (str(parent_project_root), str(project_root)):    ...    sys.path.insert(0, path)
Legacy marker still present in backend/sitecustomize.py: it looks for a redis_files directory to locate the project root, even though redis_files is deprecated in favor of shared_core/redis_clients.
8:15:aion_trading_dashboard/backend/sitecustomize.pymarkers = {"redis_files", "intraday_trading", "alerts"}...if markers.issubset(entries):    return candidate
Redis: comments note “redis_files is now replaced by shared_core/redis_clients”, but the code still carries fallbacks via sys.path gymnastics. Core Redis usage should go through backend/redis_bridge (which already prefers shared_core.redis_clients); remove legacy fallbacks to avoid ambiguity.
120:127:aion_trading_dashboard/backend/optimized_main.py# Note: redis_files is now replaced by shared_core/redis_clientsshared_core_path = project_root / "shared_core"...redis_clients_path = shared_core_path / "redis_clients"
Security import shim at repo root (security/__init__.py, security/credential_manager.py) exists solely to redirect legacy security.* imports to aion_trading_dashboard/security. This indicates lingering old import paths that should be migrated or the shim removed once all callers are updated.
1:4:security/__init__.pyCompatibility shim so legacy imports like `security.credential_manager`resolve to ... aion_trading_dashboard/security1:3:security/credential_manager.pyfrom aion_trading_dashboard.security.credential_manager import *  # bridge
Clean-up steps to align imports/exports:
1. [completed] Standardize on aion_trading_dashboard package name; remove any stale aion_dashboard references if they exist elsewhere.
Convert backend to run as an installed/editable package; drop sys.path injections for intraday_trading and zerodha_websocket once dependent imports are fixed.
Update backend/sitecustomize.py markers to rely on shared_core (not redis_files) or remove if no longer needed.
Ensure all Redis access goes via backend/redis_bridge (shared_core-backed) and delete any remaining redis_files fallbacks.
Retire the security shim after updating callers to import from aion_trading_dashboard.security.