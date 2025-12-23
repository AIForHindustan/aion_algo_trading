"""
Compatibility shim for legacy redis volume manager imports.

MIGRATION STATUS: Updated to expose both legacy and unified APIs.

Legacy API (backward compatible):
    from shared_core.redis_clients.redis_volume_gateway import RedisVolumeGateway
    gateway = get_redis_volume_gateway(redis_client)
    result = gateway.calculate_volume_metrics(symbol, tick_data)

NEW Unified API (recommended for new code):
    from shared_core.redis_clients.redis_volume_gateway import get_unified_engine
    engine = get_unified_engine(redis_client)
    result = engine.process_tick(symbol, tick_data)
"""

from shared_core.volume_files.volume_computation_manager import (
    VolumeComputationManager,
    get_volume_computation_manager,
)

# NEW: Export unified engine for migration
from shared_core.volume_files import (
    UnifiedVolumeEngine,
    BaselineService,
    InstrumentType,
    get_key_tracker,
)


class RedisVolumeGateway(VolumeComputationManager):
    """Backwards-compatible alias for VolumeComputationManager."""


def get_redis_volume_gateway(redis_client=None, token_resolver=None) -> RedisVolumeGateway:
    """Return shared VolumeComputationManager under the legacy name."""
    return get_volume_computation_manager(
        redis_client=redis_client,
        token_resolver=token_resolver,
    )


# NEW: Unified API accessors
_unified_engine_instance = None


def get_unified_engine(redis_client=None) -> UnifiedVolumeEngine:
    """
    Get the unified volume engine instance.
    
    This is the NEW recommended API for volume computation.
    
    Usage:
        from shared_core.redis_clients.redis_volume_gateway import get_unified_engine
        engine = get_unified_engine(redis_client)
        result = engine.process_tick('NFONIFTY27JAN26000CE', {
            'volume': 1000,
            'price': 150.0,
            'timestamp': datetime.now().isoformat()
        })
    """
    global _unified_engine_instance
    
    if _unified_engine_instance is None:
        if redis_client is None:
            from shared_core.redis_clients import get_redis_client
            redis_client = get_redis_client(process_name="unified_volume_engine", db=1)
        
        _unified_engine_instance = UnifiedVolumeEngine(redis_client)
    
    return _unified_engine_instance


def get_baseline_service(redis_client=None) -> BaselineService:
    """Get the baseline service for direct baseline access."""
    if redis_client is None:
        from shared_core.redis_clients import get_redis_client
        redis_client = get_redis_client(process_name="baseline_service", db=1)
    
    return BaselineService(redis_client)


__all__ = [
    # Legacy exports (backward compatible)
    "RedisVolumeGateway",
    "get_redis_volume_gateway",
    # NEW unified exports
    "UnifiedVolumeEngine",
    "get_unified_engine",
    "BaselineService",
    "get_baseline_service",
    "InstrumentType",
    "get_key_tracker",
]
