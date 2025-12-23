"""
Registry Feature Flags and Configuration

Controls registry type selection via feature flags and environment variables.
Supports gradual migration from COMPREHENSIVE to INTRADAY_FOCUS registry.
"""
import os
import logging
from typing import Optional, Dict, Any
from enum import Enum
from pathlib import Path
import json

from .instrument_registry import RegistryType

logger = logging.getLogger(__name__)

# Feature flag keys
# Feature flag keys
REGISTRY_FEATURE_FLAGS = {
    "use_intraday_focus_for_scanner": True,  # Scanner uses INTRADAY_FOCUS if True
    "use_intraday_focus_for_patterns": True,  # Pattern detectors use INTRADAY_FOCUS if True
    "use_intraday_focus_for_alerts": True,  # Alert builders default to INTRADAY_FOCUS to avoid 60MB CSV loads
    "use_intraday_focus_for_executor": True, # Executor uses INTRADAY_FOCUS if True
    "use_comprehensive_for_orders": False,  # Order placement doesn't force COMPREHENSIVE if executor flag is set
    "enable_performance_monitoring": True,  # Track registry performance metrics
    "enable_registry_caching": True,  # Cache registry instances
}

# Performance monitoring
_performance_metrics: Dict[str, Any] = {
    "lookup_count": 0,
    "lookup_times": [],
    "cache_hits": 0,
    "cache_misses": 0,
    "registry_switches": 0,
}


def get_registry_feature_flags() -> Dict[str, bool]:
    """
    Get current feature flags, checking environment variables first.
    
    Environment variables override defaults:
    - REGISTRY_USE_INTRADAY_FOCUS_SCANNER=true
    - REGISTRY_USE_INTRADAY_FOCUS_PATTERNS=true
    - REGISTRY_USE_INTRADAY_FOCUS_ALERTS=true
    - REGISTRY_ENABLE_MONITORING=true
    """
    flags = REGISTRY_FEATURE_FLAGS.copy()
    
    # Check environment variables
    env_mappings = {
        "REGISTRY_USE_INTRADAY_FOCUS_SCANNER": "use_intraday_focus_for_scanner",
        "REGISTRY_USE_INTRADAY_FOCUS_PATTERNS": "use_intraday_focus_for_patterns",
        "REGISTRY_USE_INTRADAY_FOCUS_ALERTS": "use_intraday_focus_for_alerts",
        "REGISTRY_ENABLE_MONITORING": "enable_performance_monitoring",
        "REGISTRY_ENABLE_CACHING": "enable_registry_caching",
    }
    
    for env_key, flag_key in env_mappings.items():
        env_value = os.getenv(env_key)
        if env_value is not None:
            flags[flag_key] = env_value.lower() in ("true", "1", "yes", "on")
            logger.debug(f"🔧 Feature flag {flag_key} = {flags[flag_key]} (from {env_key})")
    
    return flags


def get_registry_type_for_module(module_name: str) -> RegistryType:
    """
    Determine which registry type to use for a given module.
    
    Args:
        module_name: Name of the module (e.g., 'scanner', 'pattern_detector', 'order_manager')
    
    Returns:
        RegistryType to use
    """
    flags = get_registry_feature_flags()
    module_lower = module_name.lower()
    
    # Order placement / Executor
    if "order" in module_lower or "executor" in module_lower:
        if flags.get("use_intraday_focus_for_executor", False):
            return RegistryType.INTRADAY_FOCUS
        return RegistryType.COMPREHENSIVE
    
    # Scanner
    if "scanner" in module_lower or "data_pipeline" in module_lower:
        if flags.get("use_intraday_focus_for_scanner", False):
            return RegistryType.INTRADAY_FOCUS
        return RegistryType.COMPREHENSIVE
    
    # Pattern detectors
    if "pattern" in module_lower or "detector" in module_lower:
        if flags.get("use_intraday_focus_for_patterns", False):
            return RegistryType.INTRADAY_FOCUS
        return RegistryType.COMPREHENSIVE
    
    # Alert builders
    if "alert" in module_lower or "builder" in module_lower:
        if flags.get("use_intraday_focus_for_alerts", False):
            return RegistryType.INTRADAY_FOCUS
        return RegistryType.COMPREHENSIVE
    
    # Default to COMPREHENSIVE for safety
    return RegistryType.COMPREHENSIVE


def record_lookup_metrics(lookup_time: float, cache_hit: bool = False):
    """Record performance metrics for registry lookups."""
    if not get_registry_feature_flags().get("enable_performance_monitoring", True):
        return
    
    _performance_metrics["lookup_count"] += 1
    _performance_metrics["lookup_times"].append(lookup_time)
    
    # Keep only last 1000 lookups
    if len(_performance_metrics["lookup_times"]) > 1000:
        _performance_metrics["lookup_times"] = _performance_metrics["lookup_times"][-1000:]
    
    if cache_hit:
        _performance_metrics["cache_hits"] += 1
    else:
        _performance_metrics["cache_misses"] += 1


def get_performance_metrics() -> Dict[str, Any]:
    """Get current performance metrics."""
    metrics = _performance_metrics.copy()
    
    if metrics["lookup_times"]:
        metrics["avg_lookup_time_ms"] = sum(metrics["lookup_times"]) / len(metrics["lookup_times"]) * 1000
        metrics["max_lookup_time_ms"] = max(metrics["lookup_times"]) * 1000
        metrics["min_lookup_time_ms"] = min(metrics["lookup_times"]) * 1000
    else:
        metrics["avg_lookup_time_ms"] = 0
        metrics["max_lookup_time_ms"] = 0
        metrics["min_lookup_time_ms"] = 0
    
    total_lookups = metrics["lookup_count"]
    if total_lookups > 0:
        metrics["cache_hit_rate"] = metrics["cache_hits"] / total_lookups
    else:
        metrics["cache_hit_rate"] = 0
    
    return metrics


def reset_performance_metrics():
    """Reset performance metrics (useful for testing)."""
    global _performance_metrics
    _performance_metrics = {
        "lookup_count": 0,
        "lookup_times": [],
        "cache_hits": 0,
        "cache_misses": 0,
        "registry_switches": 0,
    }


def load_feature_flags_from_file(config_path: Optional[Path] = None) -> Dict[str, bool]:
    """
    Load feature flags from a JSON config file.
    
    Args:
        config_path: Path to config file (defaults to registry_config.json in same directory)
    
    Returns:
        Dict of feature flags
    """
    if config_path is None:
        config_path = Path(__file__).parent / "registry_config.json"
    
    if not config_path.exists():
        logger.debug(f"Registry config file not found: {config_path}, using defaults")
        return REGISTRY_FEATURE_FLAGS.copy()
    
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
            flags = config.get("feature_flags", REGISTRY_FEATURE_FLAGS.copy())
            logger.info(f"✅ Loaded registry feature flags from {config_path}")
            return flags
    except Exception as e:
        logger.warning(f"⚠️ Failed to load registry config from {config_path}: {e}")
        return REGISTRY_FEATURE_FLAGS.copy()


def save_feature_flags_to_file(flags: Dict[str, bool], config_path: Optional[Path] = None):
    """
    Save feature flags to a JSON config file.
    
    Args:
        flags: Dict of feature flags to save
        config_path: Path to config file (defaults to registry_config.json in same directory)
    """
    if config_path is None:
        config_path = Path(__file__).parent / "registry_config.json"
    
    try:
        config = {
            "feature_flags": flags,
            "description": "Registry type feature flags for gradual migration",
        }
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)
        logger.info(f"✅ Saved registry feature flags to {config_path}")
    except Exception as e:
        logger.error(f"❌ Failed to save registry config to {config_path}: {e}")
