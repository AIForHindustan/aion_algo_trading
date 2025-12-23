# core/patterns/engine.py
"""
✅ FIXED: Pattern Detection Engine - Wrapper around centralized PatternDetector.

This module provides a lightweight wrapper around the centralized pattern detection
system (patterns.pattern_detector.PatternDetector) to maintain backward compatibility
with core/patterns/engine.py API while using the centralized infrastructure.

Centralized Components Used:
- patterns.pattern_detector.PatternDetector: Main pattern detection engine
- patterns.data.pattern_registry_config.json: Pattern registry (not hardcoded)
- Redis storage via redis_key_standards: Indicators, Greeks, volume profiles
- patterns.base_detector.BasePatternDetector: Base functionality
"""
import asyncio
import logging
from typing import Dict, List, Any, Optional
from ..indicators.lifecycle import IndicatorLifecycleManager
from shared_core.redis_clients.redis_client import RedisClientFactory
from shared_core.redis_clients.unified_data_storage import get_unified_storage
from patterns.singleton_factory import PatternDetectorSingletonFactory

logger = logging.getLogger(__name__)

class PatternDetectionEngine:
    """
    ✅ FIXED: Resilient pattern detection using centralized PatternDetector.
    
    This class wraps the centralized patterns.pattern_detector.PatternDetector
    instead of reimplementing pattern detection logic. It provides:
    - Backward compatibility with existing API
    - Integration with IndicatorLifecycleManager
    - Performance tracking
    """
    
    def __init__(self, lifecycle_manager: IndicatorLifecycleManager, redis_client=None):
        self.lifecycle_manager = lifecycle_manager
        self.redis_client = redis_client or getattr(lifecycle_manager, 'redis_client', None)
        
        # ✅ FIXED: Use centralized PatternDetector instead of duplicate implementation
        try:
            # PatternDetector loads pattern registry from patterns/data/pattern_registry_config.json
            # and fetches indicators/Greeks from Redis using redis_key_standards
            self.pattern_detector = PatternDetectorSingletonFactory.get_instance(
                caller="shared_core.core.patterns.engine"
            )
            logger.info("✅ Using centralized PatternDetector from intraday_trading.patterns.pattern_detector")
        except ImportError as e:
            logger.warning(f"⚠️ Centralized PatternDetector not available: {e}")
            self.pattern_detector = None
        
        self.performance_tracker = PatternPerformanceTracker()
    
    def _load_pattern_registry(self) -> Dict[str, Dict]:
        """
        ✅ DEPRECATED: Pattern registry is now loaded by centralized PatternDetector.
        
        This method is kept for backward compatibility but should not be used.
        Pattern registry is loaded from patterns/data/pattern_registry_config.json
        by patterns.pattern_detector.PatternDetector._load_pattern_registry()
        """
        if self.pattern_detector and hasattr(self.pattern_detector, 'pattern_registry'):
            return self.pattern_detector.pattern_registry.get('pattern_configs', {})
        
        # Fallback: return empty dict if centralized detector not available
        return {}
    
    async def evaluate_patterns(self, symbol: str, context_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        ✅ FIXED: Evaluate patterns using centralized PatternDetector.
        
        This method now delegates to patterns.pattern_detector.PatternDetector.detect_patterns()
        which:
        - Fetches indicators/Greeks from Redis using redis_key_standards
        - Uses pattern registry from patterns/data/pattern_registry_config.json
        - Handles volume profiles, Greeks, and all pattern types
        """
        if not self.pattern_detector:
            logger.warning("⚠️ PatternDetector not available, returning empty list")
            return []
        
        # ✅ FIXED: Get indicators using lifecycle manager (which uses Redis storage)
        symbol_type = self.lifecycle_manager._classify_symbol_type(symbol)
        required_indicators = self.lifecycle_manager.get_required_indicators(symbol)
        
        # Get indicators with lifecycle management (fetches from Redis if needed)
        indicators = await self.lifecycle_manager.get_indicators(
            symbol, required_indicators, context_data
        )
        
        # ✅ FIXED: Ensure symbol is in indicators dict (required by PatternDetector)
        if 'symbol' not in indicators:
            indicators['symbol'] = symbol
        
        # ✅ FIXED: Use centralized PatternDetector.detect_patterns()
        # This method:
        # - Uses pattern registry from patterns/data/pattern_registry_config.json
        # - Fetches missing indicators from Redis using redis_key_standards (line 3710-3760)
        # - Handles all pattern types (ICT, volume, Greeks, etc.)
        try:
            # PatternDetector.detect_patterns() expects indicators dict (must include 'symbol' key)
            # It automatically fetches missing indicators from Redis using redis_key_standards
            triggered_patterns = self.pattern_detector.detect_patterns(
                indicators=indicators,
                active_positions=None,
                use_pipelining=True
            )
            
            # Track performance
            self.performance_tracker.record_evaluation(symbol, len(triggered_patterns))
            
            return triggered_patterns or []
        except Exception as e:
            logger.error(f"❌ Pattern evaluation failed for {symbol}: {e}", exc_info=True)
            return []
    
    # ✅ REMOVED: Duplicate pattern evaluation methods
    # These are now handled by centralized PatternDetector.detect_patterns():
    # - _evaluate_single_pattern() - Handled by PatternDetector
    # - _check_conditions() - Handled by PatternDetector
    # - _check_single_condition() - Handled by PatternDetector
    # - _calculate_pattern_confidence() - Handled by PatternDetector
    # - _calculate_indicator_strength() - Handled by PatternDetector
    # - _calculate_context_boost() - Handled by PatternDetector
    # - _get_required_indicators_for_symbol() - Use lifecycle_manager.get_required_indicators()
    # - _get_pattern_indicators() - Pattern registry contains this info
    # - _check_price_trend() - Handled by PatternDetector
    # - _check_price_breakout() - Handled by PatternDetector

class PatternPerformanceTracker:
    """Track pattern detection performance"""
    
    def __init__(self):
        self.evaluation_count = 0
        self.trigger_count = 0
        self.symbol_stats = {}
    
    def record_evaluation(self, symbol: str, triggered_count: int):
        """Record pattern evaluation results"""
        self.evaluation_count += 1
        self.trigger_count += triggered_count
        
        if symbol not in self.symbol_stats:
            self.symbol_stats[symbol] = {'evaluations': 0, 'triggers': 0}
        
        self.symbol_stats[symbol]['evaluations'] += 1
        self.symbol_stats[symbol]['triggers'] += triggered_count
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Get performance statistics"""
        trigger_rate = self.trigger_count / self.evaluation_count if self.evaluation_count > 0 else 0
        
        return {
            'total_evaluations': self.evaluation_count,
            'total_triggers': self.trigger_count,
            'overall_trigger_rate': trigger_rate,
            'symbol_performance': self.symbol_stats
        }

# Test function
async def test_pattern_engine():
    """Test the pattern engine using centralized PatternDetector"""
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    redis_client = RedisClientFactory.get_trading_client()
    # ✅ DEPRECATED: StorageEngine replaced by UnifiedDataStorage
    # from shared_core.core.storage.engine import StorageEngine
    from shared_core.core.indicators.lifecycle import IndicatorLifecycleManager
    
    # ✅ CENTRALIZED: Use UnifiedDataStorage instead of legacy StorageEngine
    unified_storage = get_unified_storage(redis_client=redis_client)
    lifecycle = IndicatorLifecycleManager(unified_storage=unified_storage, redis_client=redis_client)
    pattern_engine = PatternDetectionEngine(lifecycle, redis_client=redis_client)
    
    # Test pattern evaluation (uses centralized PatternDetector)
    patterns = await pattern_engine.evaluate_patterns(
        'RELIANCE',
        {
            'timestamp': {'hour': 14}, 
            'market_regime': 'trending',
            'ticks': [{'last_price': 2500.0, 'volume': 1000}]
        }
    )
    
    logger.info(f"Triggered patterns: {[p.get('pattern', p.get('name', 'unknown')) for p in patterns]}")
    
    # Print performance report
    report = pattern_engine.performance_tracker.get_performance_report()
    logger.info(f"Performance: {report['overall_trigger_rate']:.1%} trigger rate")

if __name__ == "__main__":
    asyncio.run(test_pattern_engine())
