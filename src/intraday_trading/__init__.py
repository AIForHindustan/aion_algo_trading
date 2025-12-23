"""
Intraday Trading Package
"""

from pathlib import Path
import sys


def _ensure_shared_core_on_path() -> None:
    """Ensure shared_core is importable when this package loads."""
    shared_core_path = Path(__file__).resolve().parent.parent / "shared_core"
    shared_core_str = str(shared_core_path)
    if shared_core_path.exists() and shared_core_str not in sys.path:
        sys.path.insert(0, shared_core_str)


_ensure_shared_core_on_path()

