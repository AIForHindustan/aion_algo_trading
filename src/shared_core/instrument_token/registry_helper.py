"""
Registry Helper with Feature Flag Support

Provides a unified interface for getting instrument registries with:
- Feature flag support for gradual migration
- Performance monitoring
- Automatic registry type selection based on module context
"""
import time
import logging
from typing import Optional, Dict, Any
import functools

from .instrument_registry import (
    RegistryType,
    EnhancedInstrumentService,
    get_enhanced_instrument_service,
)
from .registry_config import (
    get_registry_type_for_module,
    record_lookup_metrics,
    get_registry_feature_flags,
)

logger = logging.getLogger(__name__)

# Cache for registry instances
_registry_cache: Dict[RegistryType, EnhancedInstrumentService] = {}


def get_registry_for_module(
    module_name: str,
    force_type: Optional[RegistryType] = None,
    enable_monitoring: Optional[bool] = None
) -> EnhancedInstrumentService:
    """
    Get instrument registry for a specific module with feature flag support.
    
    Args:
        module_name: Name of the module (e.g., 'scanner', 'pattern_detector', 'order_manager')
        force_type: Override feature flag and force a specific registry type
        enable_monitoring: Override monitoring flag (defaults to feature flag setting)
    
    Returns:
        EnhancedInstrumentService instance
    
    Examples:
        # Get registry for scanner (uses feature flag)
        registry = get_registry_for_module("scanner")
        
        # Force comprehensive registry
        registry = get_registry_for_module("scanner", force_type=RegistryType.COMPREHENSIVE)
    """
    # Determine registry type
    if force_type:
        registry_type = force_type
        logger.debug(f"🔧 Using forced registry type: {registry_type.value} for {module_name}")
    else:
        registry_type = get_registry_type_for_module(module_name)
        logger.debug(f"🔧 Using registry type: {registry_type.value} for {module_name} (from feature flags)")
    
    # Check cache if enabled
    flags = get_registry_feature_flags()
    use_cache = flags.get("enable_registry_caching", True)
    
    if use_cache and registry_type in _registry_cache:
        logger.debug(f"✅ Using cached registry for {module_name} ({registry_type.value})")
        return _registry_cache[registry_type]
    
    # Get registry instance
    registry_service = get_enhanced_instrument_service(registry_type)
    
    # Cache if enabled
    if use_cache:
        _registry_cache[registry_type] = registry_service
        logger.debug(f"✅ Cached registry for {module_name} ({registry_type.value})")
    
    return registry_service


def resolve_symbol_with_monitoring(
    registry_service: EnhancedInstrumentService,
    symbol: str,
    broker: str,
    module_name: str = "unknown"
) -> Optional[Dict[str, Any]]:
    """
    Resolve symbol with performance monitoring.
    
    Args:
        registry_service: EnhancedInstrumentService instance
        symbol: Symbol to resolve
        broker: Broker name
        module_name: Name of calling module (for logging)
    
    Returns:
        Resolved instrument dict or None
    """
    flags = get_registry_feature_flags()
    enable_monitoring = flags.get("enable_performance_monitoring", True)
    
    if not enable_monitoring:
        return registry_service.resolve_for_broker(symbol, broker)
    
    # Monitor lookup performance
    start_time = time.time()
    try:
        result = registry_service.resolve_for_broker(symbol, broker)
        lookup_time = time.time() - start_time
        
        # Record metrics
        record_lookup_metrics(lookup_time, cache_hit=(result is not None))
        
        if lookup_time > 0.01:  # Log slow lookups (>10ms)
            logger.warning(
                f"⚠️ Slow registry lookup: {symbol} took {lookup_time*1000:.2f}ms "
                f"(module: {module_name})"
            )
        
        return result
    except Exception as e:
        lookup_time = time.time() - start_time
        record_lookup_metrics(lookup_time, cache_hit=False)
        logger.error(f"❌ Registry lookup failed for {symbol}: {e}")
        raise


def clear_registry_cache():
    """Clear the registry cache (useful for testing or forced refresh)."""
    global _registry_cache
    _registry_cache.clear()
    logger.info("🧹 Registry cache cleared")


def get_registry_stats() -> Dict[str, Any]:
    """Get statistics about registry usage."""
    from .registry_config import get_performance_metrics
    
    stats = {
        "cached_registries": list(_registry_cache.keys()),
        "cache_size": len(_registry_cache),
        "performance_metrics": get_performance_metrics(),
    }
    
    return stats


# Convenience functions for common use cases
def get_scanner_registry(force_intraday: bool = True) -> EnhancedInstrumentService:
    """
    Get registry for scanner module.
    
    Scanner must always use the intraday focus registry to stay aligned with
    `intraday_crawler_instruments.json`, so we force that registry type by default.
    """
    if force_intraday:
        return get_registry_for_module("scanner", force_type=RegistryType.INTRADAY_FOCUS)
    return get_registry_for_module("scanner")


def get_pattern_detector_registry() -> EnhancedInstrumentService:
    """Get registry for pattern detector module (uses feature flags)."""
    return get_registry_for_module("pattern_detector")


def get_order_manager_registry() -> EnhancedInstrumentService:
    """Get registry for order manager (defaults to INTRADAY_FOCUS unless overridden)."""
    flags = get_registry_feature_flags()
    if flags.get("use_comprehensive_for_orders", False):
        forced_type = RegistryType.COMPREHENSIVE
    else:
        forced_type = RegistryType.INTRADAY_FOCUS
    return get_registry_for_module("order_manager", force_type=forced_type)


def get_alert_builder_registry() -> EnhancedInstrumentService:
    """Get registry for alert builder module (uses feature flags)."""
    return get_registry_for_module("alert_builder")
