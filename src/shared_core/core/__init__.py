"""
Core package – permanent architecture building blocks.

Exports the primary façade classes lazily so callers can simply do:

    from core import StorageEngine, IndicatorLifecycleManager, \
        PatternDetectionEngine

The lazy import avoids issues when modules inside `core/` are executed as scripts.

⚠️ DEPRECATED: StorageEngine is deprecated. Use UnifiedDataStorage instead:
    from shared_core.redis_clients.unified_data_storage import UnifiedDataStorage
"""

__all__ = [
    "StorageEngine",  # ⚠️ DEPRECATED: Use UnifiedDataStorage instead
    "IndicatorLifecycleManager",
    "PatternDetectionEngine",
]


def __getattr__(name):
    # SymbolResolver removed - use RedisKeyStandards directly
    if name == "StorageEngine":
        # ⚠️ DEPRECATED: StorageEngine is deprecated. Use UnifiedDataStorage instead.
        # Kept for backward compatibility only.
        import warnings
        warnings.warn(
            "StorageEngine is deprecated. Use UnifiedDataStorage instead: "
            "from shared_core.redis_clients.unified_data_storage import UnifiedDataStorage",
            DeprecationWarning,
            stacklevel=2
        )
        from .storage.engine import StorageEngine
        return StorageEngine
    if name == "IndicatorLifecycleManager":
        from .indicators.lifecycle import IndicatorLifecycleManager

        return IndicatorLifecycleManager
    if name == "PatternDetectionEngine":
        from .patterns.engine import PatternDetectionEngine

        return PatternDetectionEngine
    raise AttributeError(f"module 'core' has no attribute {name}")


def __dir__():
    return sorted(__all__)
