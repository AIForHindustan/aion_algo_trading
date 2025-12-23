"""
Registry Performance Benchmarking

Benchmarks different registry configurations to measure:
- Memory usage
- Lookup performance
- Build time
"""
import time
import sys
import json
from pathlib import Path
from typing import Dict, Any, List
import logging

try:
    import psutil
    import os
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

from .instrument_registry import RegistryType
from .registry_factory import get_registry

logger = logging.getLogger(__name__)


class RegistryBenchmark:
    """Benchmark different registry configurations"""
    
    @staticmethod
    def run_benchmarks() -> Dict[str, Any]:
        """
        Run comprehensive performance benchmarks.
        
        Returns:
            Dict with benchmark results
        """
        print("=" * 60)
        print("=== Registry Performance Benchmarks ===")
        print("=" * 60)
        
        results = {}
        
        # Memory usage
        print("\n📊 Memory Usage:")
        comprehensive_memory = RegistryBenchmark._measure_memory(RegistryType.COMPREHENSIVE)
        focus_memory = RegistryBenchmark._measure_memory(RegistryType.INTRADAY_FOCUS)
        
        memory_reduction = ((comprehensive_memory - focus_memory) / comprehensive_memory * 100) if comprehensive_memory > 0 else 0
        
        print(f"  Comprehensive: {comprehensive_memory:.2f} MB")
        print(f"  Intraday Focus: {focus_memory:.2f} MB")
        print(f"  Reduction: {memory_reduction:.1f}%")
        
        results['memory'] = {
            'comprehensive_mb': comprehensive_memory,
            'focus_mb': focus_memory,
            'reduction_percent': memory_reduction
        }
        
        # Lookup performance
        print("\n⚡ Lookup Performance (avg over 1000 lookups):")
        comprehensive_lookup = RegistryBenchmark._measure_lookup_time(RegistryType.COMPREHENSIVE)
        focus_lookup = RegistryBenchmark._measure_lookup_time(RegistryType.INTRADAY_FOCUS)
        
        speedup = comprehensive_lookup / focus_lookup if focus_lookup > 0 else 0
        
        print(f"  Comprehensive: {comprehensive_lookup * 1000:.2f} ms")
        print(f"  Intraday Focus: {focus_lookup * 1000:.2f} ms")
        print(f"  Speedup: {speedup:.1f}x")
        
        results['lookup'] = {
            'comprehensive_ms': comprehensive_lookup * 1000,
            'focus_ms': focus_lookup * 1000,
            'speedup': speedup
        }
        
        # Build time
        print("\n🔨 Build Time:")
        comprehensive_build = RegistryBenchmark._measure_build_time(RegistryType.COMPREHENSIVE)
        focus_build = RegistryBenchmark._measure_build_time(RegistryType.INTRADAY_FOCUS)
        
        build_speedup = comprehensive_build / focus_build if focus_build > 0 else 0
        
        print(f"  Comprehensive: {comprehensive_build:.2f} seconds")
        print(f"  Intraday Focus: {focus_build:.2f} seconds")
        print(f"  Speedup: {build_speedup:.1f}x")
        
        results['build'] = {
            'comprehensive_sec': comprehensive_build,
            'focus_sec': focus_build,
            'speedup': build_speedup
        }
        
        # Instrument counts
        print("\n📈 Instrument Counts:")
        comprehensive_registry = get_registry(RegistryType.COMPREHENSIVE)
        focus_registry = get_registry(RegistryType.INTRADAY_FOCUS)
        
        comprehensive_count = comprehensive_registry.stats.get('total_unified_instruments', 0)
        focus_count = focus_registry.stats.get('total_unified_instruments', 0)
        
        print(f"  Comprehensive: {comprehensive_count} instruments")
        print(f"  Intraday Focus: {focus_count} instruments")
        print(f"  Ratio: {focus_count / comprehensive_count * 100:.1f}% of comprehensive")
        
        results['counts'] = {
            'comprehensive': comprehensive_count,
            'focus': focus_count,
            'ratio_percent': (focus_count / comprehensive_count * 100) if comprehensive_count > 0 else 0
        }
        
        print("\n" + "=" * 60)
        print("✅ Benchmark Complete")
        print("=" * 60)
        
        return results
    
    @staticmethod
    def _measure_memory(registry_type: RegistryType) -> float:
        """Measure memory usage of registry in MB"""
        try:
            if PSUTIL_AVAILABLE:
                process = psutil.Process(os.getpid())
                mem_before = process.memory_info().rss / 1024 / 1024  # MB
                
                # Clear any existing registry instance
                from .registry_factory import _registry_instances
                if registry_type in _registry_instances:
                    del _registry_instances[registry_type]
                
                # Force garbage collection
                import gc
                gc.collect()
                
                mem_after_clear = process.memory_info().rss / 1024 / 1024  # MB
                
                # Build registry
                registry = get_registry(registry_type)
                
                mem_after = process.memory_info().rss / 1024 / 1024  # MB
                
                registry_memory = mem_after - mem_after_clear
                return max(0, registry_memory)
            else:
                # Fallback: use sys.getsizeof (less accurate)
                registry = get_registry(registry_type)
                size_bytes = sys.getsizeof(registry)
                # Rough estimate: multiply by factor for nested structures
                estimated_size = size_bytes * 10  # Rough multiplier
                return estimated_size / 1024 / 1024  # MB
        except Exception as e:
            logger.warning(f"Error measuring memory for {registry_type.value}: {e}")
            return 0.0
    
    @staticmethod
    def _measure_lookup_time(registry_type: RegistryType, iterations: int = 1000) -> float:
        """Measure average lookup time in seconds"""
        try:
            registry = get_registry(registry_type)
            
            # Get test symbols from focus_instruments.json
            test_symbols = RegistryBenchmark._get_test_symbols()
            
            if not test_symbols:
                # Fallback: use some common symbols
                test_symbols = [
                    "NIFTY25DEC26000CE",
                    "BANKNIFTY25DEC60000CE",
                    "SENSEX25DEC85000CE",
                    "NIFTY25DECFUT",
                    "BANKNIFTY25DECFUT"
                ]
            
            # Warm up
            for symbol in test_symbols[:5]:
                try:
                    registry.resolve_symbol(symbol)
                except Exception:
                    pass
            
            # Measure lookup time
            start_time = time.perf_counter()
            
            for _ in range(iterations):
                # Pick a random symbol from test set
                import random
                symbol = random.choice(test_symbols)
                try:
                    registry.resolve_symbol(symbol)
                except Exception:
                    pass
            
            end_time = time.perf_counter()
            
            avg_time = (end_time - start_time) / iterations
            return avg_time
        except Exception as e:
            logger.warning(f"Error measuring lookup time for {registry_type.value}: {e}")
            return 0.0
    
    @staticmethod
    def _measure_build_time(registry_type: RegistryType) -> float:
        """Measure registry build time in seconds"""
        try:
            # Clear existing registry
            from .registry_factory import _registry_instances
            if registry_type in _registry_instances:
                del _registry_instances[registry_type]
            
            # Force garbage collection
            import gc
            gc.collect()
            
            # Measure build time
            start_time = time.perf_counter()
            registry = get_registry(registry_type)
            end_time = time.perf_counter()
            
            build_time = end_time - start_time
            return build_time
        except Exception as e:
            logger.warning(f"Error measuring build time for {registry_type.value}: {e}")
            return 0.0
    
    @staticmethod
    def _get_test_symbols() -> List[str]:
        """Get test symbols from focus_instruments.json"""
        try:
            focus_file = Path(__file__).parent / "focus_instruments.json"
            if focus_file.exists():
                with open(focus_file, 'r') as f:
                    data = json.load(f)
                    symbols = data.get('symbols', [])
                    # Return a sample of symbols for testing
                    return symbols[:50] if len(symbols) > 50 else symbols
        except Exception as e:
            logger.debug(f"Could not load test symbols: {e}")
        return []


def main():
    """Run benchmarks from command line"""
    logging.basicConfig(level=logging.INFO)
    results = RegistryBenchmark.run_benchmarks()
    
    # Save results to file
    results_file = Path(__file__).parent / "benchmark_results.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n📄 Results saved to: {results_file}")


if __name__ == "__main__":
    main()

