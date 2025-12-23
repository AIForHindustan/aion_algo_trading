"""
Edge Calculation Wrapper - Backward compatibility wrapper for RedisStatefulEdgeCalculator

This wrapper provides a simple interface to edge calculations for real-time use,
wrapping the stateful calculator from calculations_edge.py.
"""

import logging
import time
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class EdgeCalculationWrapper:
    """Wrapper for RedisStatefulEdgeCalculator for real-time use"""
    
    def __init__(self, redis_client=None):
        self.redis_client = redis_client
        self.logger = logging.getLogger(__name__)
        
        # State tracking for each symbol
        self.symbol_states: Dict[str, Any] = {}
        
    def compute_indicators(self, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compute edge indicators for single tick.
        
        Uses RedisStatefulEdgeCalculator which maintains state across ticks.
        
        Args:
            tick_data: Tick data with price, volume, greeks, etc.
            
        Returns:
            Dict with edge indicators
        """
        try:
            from intraday_trading.intraday_scanner.calculations_edge import (
                RedisStatefulEdgeCalculator,
                get_edge_calculator
            )
            
            symbol = tick_data.get('symbol')
            if not symbol:
                return {}
            
            # Get or create calculator for this symbol (using singleton factory)
            if symbol not in self.symbol_states:
                self.symbol_states[symbol] = get_edge_calculator(
                    symbol=symbol,
                    redis_client=self.redis_client
                )
                self.symbol_states[symbol].last_update = time.time()
            
            calculator = self.symbol_states[symbol]
            
            # Call the main calculation method
            enriched = calculator.calculate_and_store_edge_indicators(
                tick_data=tick_data,
                options_chain=None,  # Will fetch from Redis if needed
                orderbook=tick_data.get('depth') or tick_data.get('order_book')
            )
            
            # Update last access time
            calculator.last_update = time.time()
            
            # Extract and flatten edge indicators
            indicators = {}
            
            # Intent Layer
            if 'edge_intent' in enriched:
                intent = enriched['edge_intent']
                indicators['dhdi_intent'] = intent.get('dhdi')
                indicators['motive'] = intent.get('motive')
                indicators['hedge_pressure_ma'] = intent.get('hedge_pressure_ma')
            
            # Constraint Layer
            if 'edge_constraint' in enriched:
                constraint = enriched['edge_constraint']
                indicators['gamma_stress'] = constraint.get('gamma_stress')
                indicators['charm_ratio'] = constraint.get('charm_ratio')
                indicators['lci'] = constraint.get('lci')
            
            # Transition Layer
            if 'edge_transition' in enriched:
                transition = enriched['edge_transition']
                indicators['transition_probability'] = transition.get('transition_probability')
                indicators['time_compression'] = transition.get('time_compression')
                indicators['latency_collapse'] = transition.get('latency_collapse')
            
            # Regime Layer
            if 'edge_regime' in enriched:
                regime = enriched['edge_regime']
                indicators['regime_id'] = regime.get('regime_id')
                indicators['regime_label'] = regime.get('regime_label')
                indicators['should_trade'] = regime.get('should_trade')
            
            return indicators
            
        except ImportError as e:
            self.logger.warning(f"⚠️ calculations_edge module not available: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"❌ Edge calculation error: {e}")
            return {}
    
    def cleanup_old_states(self, max_age_seconds: int = 3600):
        """
        Clean up old symbol states to prevent memory leaks.
        
        Args:
            max_age_seconds: Maximum age of state before cleanup (default 1 hour)
        """
        current_time = time.time()
        old_symbols = []
        
        for symbol, calculator in self.symbol_states.items():
            # Check last update time if available
            if hasattr(calculator, 'last_update'):
                if current_time - calculator.last_update > max_age_seconds:
                    old_symbols.append(symbol)
        
        for symbol in old_symbols:
            del self.symbol_states[symbol]
            self.logger.debug(f"🧹 Cleaned up edge state for {symbol}")
        
        if old_symbols:
            self.logger.info(f"🧹 Cleaned up {len(old_symbols)} old edge calculator states")
    
    def get_active_symbols(self) -> list:
        """Get list of symbols with active edge calculators."""
        return list(self.symbol_states.keys())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get wrapper statistics."""
        return {
            'active_calculators': len(self.symbol_states),
            'symbols': list(self.symbol_states.keys())[:10],  # First 10
        }


# Singleton instance for shared use
_wrapper_instance: Optional[EdgeCalculationWrapper] = None


def get_edge_wrapper(redis_client=None) -> EdgeCalculationWrapper:
    """
    Get or create the singleton EdgeCalculationWrapper.
    
    Args:
        redis_client: Redis client for calculator initialization
        
    Returns:
        EdgeCalculationWrapper instance
    """
    global _wrapper_instance
    if _wrapper_instance is None:
        _wrapper_instance = EdgeCalculationWrapper(redis_client=redis_client)
        logger.info("✅ EdgeCalculationWrapper singleton created")
    return _wrapper_instance
