#!/usr/bin/env python3
"""
Registry Migration Status Checker

Shows current migration status, feature flags, and performance metrics.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from shared_core.instrument_token.registry_config import (
    get_registry_feature_flags,
    get_performance_metrics,
)
from shared_core.instrument_token.registry_helper import get_registry_stats
from shared_core.instrument_token.instrument_registry import RegistryType


def print_migration_status():
    """Print current migration status."""
    print("=" * 70)
    print("📊 REGISTRY MIGRATION STATUS")
    print("=" * 70)
    
    # Feature Flags
    print("\n🔧 Feature Flags:")
    flags = get_registry_feature_flags()
    for key, value in flags.items():
        status = "✅" if value else "❌"
        print(f"  {status} {key}: {value}")
    
    # Registry Type Usage
    print("\n📋 Registry Type Usage by Module:")
    from shared_core.instrument_token.registry_config import get_registry_type_for_module
    
    modules = [
        ("scanner", "Scanner/Data Pipeline"),
        ("pattern_detector", "Pattern Detectors"),
        ("alert_builder", "Alert Builders"),
        ("order_manager", "Order Manager"),
    ]
    
    for module_name, description in modules:
        reg_type = get_registry_type_for_module(module_name)
        icon = "⚡" if reg_type == RegistryType.INTRADAY_FOCUS else "📦"
        print(f"  {icon} {description}: {reg_type.value.upper()}")
    
    # Performance Metrics
    print("\n📈 Performance Metrics:")
    stats = get_registry_stats()
    perf = stats.get("performance_metrics", {})
    
    if perf.get("lookup_count", 0) > 0:
        print(f"  Total Lookups: {perf.get('lookup_count', 0)}")
        print(f"  Avg Lookup Time: {perf.get('avg_lookup_time_ms', 0):.2f} ms")
        print(f"  Max Lookup Time: {perf.get('max_lookup_time_ms', 0):.2f} ms")
        print(f"  Min Lookup Time: {perf.get('min_lookup_time_ms', 0):.2f} ms")
        print(f"  Cache Hit Rate: {perf.get('cache_hit_rate', 0):.1%}")
        print(f"  Cache Hits: {perf.get('cache_hits', 0)}")
        print(f"  Cache Misses: {perf.get('cache_misses', 0)}")
    else:
        print("  ⚠️  No performance data yet (no lookups recorded)")
    
    # Cached Registries
    print("\n💾 Cached Registries:")
    cached = stats.get("cached_registries", [])
    if cached:
        for reg_type in cached:
            print(f"  ✅ {reg_type.value.upper()}")
    else:
        print("  ⚠️  No registries cached yet")
    
    # Migration Progress
    print("\n🚀 Migration Progress:")
    focus_enabled = sum([
        flags.get("use_intraday_focus_for_scanner", False),
        flags.get("use_intraday_focus_for_patterns", False),
        flags.get("use_intraday_focus_for_alerts", False),
    ])
    total_modules = 3
    progress = (focus_enabled / total_modules) * 100
    
    print(f"  Modules using INTRADAY_FOCUS: {focus_enabled}/{total_modules}")
    print(f"  Migration Progress: {progress:.0f}%")
    
    if progress == 0:
        print("  📝 Status: Not started - all modules using COMPREHENSIVE")
    elif progress < 100:
        print(f"  📝 Status: In progress - {focus_enabled} module(s) migrated")
    else:
        print("  📝 Status: Complete - all eligible modules using INTRADAY_FOCUS")
    
    print("\n" + "=" * 70)
    print("💡 Tip: Use environment variables to enable feature flags:")
    print("  export REGISTRY_USE_INTRADAY_FOCUS_SCANNER=true")
    print("  export REGISTRY_USE_INTRADAY_FOCUS_PATTERNS=true")
    print("  export REGISTRY_USE_INTRADAY_FOCUS_ALERTS=true")
    print("=" * 70)


if __name__ == "__main__":
    try:
        print_migration_status()
    except Exception as e:
        print(f"❌ Error checking migration status: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

