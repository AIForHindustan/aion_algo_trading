"""Utility helpers for sanitizing payloads before Redis/JSON serialization."""

from __future__ import annotations

from typing import Any

try:  # Optional dependency
    import numpy as _np
except ImportError:  # pragma: no cover - numpy optional
    _np = None


def fix_numpy_serialization(data: Any) -> Any:
    """
    Convert numpy scalars/containers to native Python types so Redis/JSON
    serialization never receives np.float64/np.int64 instances.
    """
    if isinstance(data, dict):
        return {k: fix_numpy_serialization(v) for k, v in data.items()}
    if isinstance(data, list):
        return [fix_numpy_serialization(v) for v in data]
    if isinstance(data, tuple):
        return tuple(fix_numpy_serialization(v) for v in data)
    if _np is not None and isinstance(data, _np.generic):
        return data.item()
    return data

