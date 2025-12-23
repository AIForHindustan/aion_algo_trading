from __future__ import annotations

import sys
from pathlib import Path


def _find_project_root(start_dir: Path) -> Path | None:
    markers = {"redis_files", "intraday_trading", "alerts"}
    for candidate in (start_dir,) + tuple(start_dir.parents):
        try:
            entries = {p.name for p in candidate.iterdir() if p.is_dir()}
        except FileNotFoundError:
            continue
        if markers.issubset(entries):
            return candidate
    return None


def _ensure_project_root_on_path() -> None:
    project_root = _find_project_root(Path(__file__).resolve().parent)
    if not project_root:
        return
    path_str = str(project_root)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


_ensure_project_root_on_path()

