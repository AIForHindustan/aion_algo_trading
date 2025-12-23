# calculations_edge.py - Updated to use Dual Redis Connection System
"""
Edge Calculator - Real-time edge indicator calculations

Calculates Intent, Constraint, Transition, and Regime layer indicators
and stores them in Redis using DatabaseAwareKeyBuilder.

Uses DUAL Redis connections:
- string_redis: For JSON operations (indicators, metrics)
- binary_redis: For pickle operations (classifier state, gamma surface)
"""
import json
import pickle
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

# ✅ NEW: Dual Redis connection imports
from shared_core.redis_clients.redis_config import (
    get_string_redis,
    get_binary_redis,
    get_redis_client,
    save_pickled_object,
    load_pickled_object,
)
# Use DatabaseAwareKeyBuilder directly (static methods)
from shared_core.redis_clients.redis_key_standards import (
    DatabaseAwareKeyBuilder,
    RedisKeyStandards,
)
from shared_core.redis_clients.unified_data_storage import get_unified_storage

logger = logging.getLogger(__name__)

# Optional imports - graceful fallback if not available
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    np = None

try:
    from river import compose, preprocessing, cluster
    RIVER_AVAILABLE = True
except ImportError:
    RIVER_AVAILABLE = False
    logger.warning("⚠️ River not available - regime classification disabled")


class RedisStatefulEdgeCalculator:
    """
    Edge calculator integrated with Redis storage.
    
    Calculates and stores edge indicators in real-time:
    - Intent Layer: DHDI, motive classification, hedge pressure
    - Constraint Layer: Gamma stress, charm ratio, LCI
    - Transition Layer: Transition probability, time compression
    - Regime Layer: Current regime, confidence, should_trade gate
    """
    
    def __init__(self, symbol: str, window_size: int = 1000, redis_client=None):
        self.symbol = symbol
        self.window_size = window_size
        self.logger = logging.getLogger(__name__)
        
        # ✅ DUAL Redis clients - string for JSON, binary for pickle
        # String client for JSON operations (indicators, metrics)
        self.string_redis = get_string_redis("market_data")
        
        # Binary client for pickle operations (classifier, gamma surface)
        self.binary_redis = get_binary_redis("market_data")
        
        # Legacy compatibility - use string client as default
        self.redis_client = self.string_redis
        self.redis_client_raw = self.binary_redis  # Raw = binary for pickle
        
        # Unified storage for high-level operations
        self.unified_storage = get_unified_storage(redis_client=self.string_redis)
        
        # State windows (in-memory)
        self.intent_window: List[Dict] = []
        self.constraint_window: List[Dict] = []
        self.transition_window: List[Dict] = []
        self.regime_window: List[Dict] = []
        self.latency_measurements: List[Dict] = []
        self.gamma_surface: Dict = {}
        
        self.regime_classifier = None
        if RIVER_AVAILABLE:
            self.regime_classifier = self._load_or_create_classifier()
        else:
            self.regime_classifier = self._create_dummy_classifier()
        
        # Load existing state from Redis
        self._load_state_from_redis()
        
        self.logger.info(f"✅ EdgeCalculator initialized for {symbol} with DUAL Redis connections")
    
    def _load_or_create_classifier(self):
        """Load classifier state from Redis or create new."""
        if not RIVER_AVAILABLE:
            return self._create_dummy_classifier()
            
        classifier_key = DatabaseAwareKeyBuilder.regime_classifier_state(self.symbol)
        
        try:
            classifier_data = self.redis_client_raw.get(classifier_key)
            if classifier_data:
                classifier = pickle.loads(classifier_data)
                # Validate classifier is a valid tuple with both components
                if (isinstance(classifier, tuple) and 
                    len(classifier) == 2 and 
                    classifier[0] is not None and 
                    classifier[1] is not None):
                    self.logger.info(f"✅ Loaded existing classifier for {self.symbol}")
                    return classifier
                else:
                    self.logger.warning(f"⚠️ Invalid classifier format for {self.symbol}, creating new")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to load classifier: {e}")
        
        # Create new classifier
        return self._create_new_classifier()
    
    def _create_new_classifier(self):
        """Create new River DBSTREAM classifier."""
        if not RIVER_AVAILABLE:
            return self._create_dummy_classifier()
            
        feature_extractor = compose.Select(
            "dhdi", "gamma_stress", "lci", "tcs", "charm_ratio"
        ) | preprocessing.MinMaxScaler()
        
        clusterer = cluster.DBSTREAM(
            clustering_threshold=0.1,
            fading_factor=0.01,
            cleanup_interval=100,
            intersection_factor=0.5,
            minimum_weight=0.1
        )
        
        self.logger.info(f"[RIVER_INIT] Created new classifier for {self.symbol} (pid={os.getpid()})")
        return (feature_extractor, clusterer)

    def _create_dummy_classifier(self):
        """Return a no-op classifier pair."""
        class _DummyFeatureExtractor:
            def learn_one(self, x):
                return self

            def transform_one(self, x):
                return x

        class _DummyClusterer:
            def learn_one(self, x):
                return self

            def predict_one(self, x):
                return -1

        self.logger.info(f"[RIVER_INIT] Using dummy classifier for {self.symbol}")
        return (_DummyFeatureExtractor(), _DummyClusterer())

    def _ensure_classifier(self):
        """Lazy initialization guard."""
        if self.regime_classifier is None:
            if RIVER_AVAILABLE:
                self.regime_classifier = self._load_or_create_classifier()
            else:
                self.regime_classifier = self._create_dummy_classifier()
        return self.regime_classifier
    
    def _load_state_from_redis(self):
        """Load calculator state from Redis."""
        try:
            # Load latency measurements
            latency_key = DatabaseAwareKeyBuilder.latency_measurements(self.symbol)
            latency_data = self.redis_client_raw.get(latency_key)
            if latency_data:
                self.latency_measurements = pickle.loads(latency_data)
            
            # Load gamma surface
            gamma_key = DatabaseAwareKeyBuilder.gamma_surface(self.symbol)
            gamma_data = self.redis_client_raw.get(gamma_key)
            if gamma_data:
                self.gamma_surface = pickle.loads(gamma_data)
                
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to load state from Redis: {e}")

    def save_state_to_redis(self):
        """Save current state to Redis (synchronous)."""
        try:
            # Save latency measurements (limit to last 1000)
            latency_key = DatabaseAwareKeyBuilder.latency_measurements(self.symbol)
            self.redis_client_raw.setex(
                latency_key,
                86400,  # 24 hours TTL
                pickle.dumps(self.latency_measurements[-1000:])
            )
            
            # Save gamma surface
            gamma_key = DatabaseAwareKeyBuilder.gamma_surface(self.symbol)
            self.redis_client_raw.setex(
                gamma_key,
                3600,  # 1 hour TTL
                pickle.dumps(self.gamma_surface)
            )
            
            # Save classifier state every 100 updates
            if RIVER_AVAILABLE and self.regime_classifier and len(self.intent_window) % 100 == 0:
                classifier_key = DatabaseAwareKeyBuilder.regime_classifier_state(self.symbol)
                self.redis_client_raw.setex(
                    classifier_key,
                    86400,
                    pickle.dumps(self.regime_classifier)
                )
                
        except Exception as e:
            self.logger.error(f"❌ Failed to save state to Redis: {e}")
    
    def calculate_and_store_edge_indicators(
        self,
        tick_data: Dict[str, Any],
        options_chain: Optional[List[Dict]] = None,
        orderbook: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Main method: calculate all edge layers and store in Redis.
        
        Args:
            tick_data: Current tick data with price, volume, greeks, etc.
            options_chain: Optional option chain data (fetched from Redis if not provided)
            orderbook: Optional order book data (extracted from tick_data if not provided)
            
        Returns:
            Enriched tick data with edge features
        """
        # Extract orderbook from tick_data if not provided
        if orderbook is None:
            orderbook = tick_data.get('depth') or tick_data.get('order_book') or {}
        
        # Capture latency timestamps
        latency_data = self._capture_latency(tick_data)
        
        # Calculate Intent Layer
        intent_features = self._calculate_intent_layer(tick_data, options_chain, latency_data)
        self._store_intent_indicators(intent_features)
        
        # Calculate Constraint Layer
        price = tick_data.get('last_price') or tick_data.get('price') or 0
        constraint_features = self._calculate_constraint_layer(price, options_chain, orderbook)
        self._store_constraint_indicators(constraint_features)
        
        # Calculate Transition Layer
        transition_features = self._calculate_transition_layer(
            intent_features, constraint_features, latency_data
        )
        self._store_transition_indicators(transition_features)
        
        # Classify Regime
        regime_features = self._classify_regime(
            intent_features, constraint_features, transition_features
        )
        self._store_regime_indicators(regime_features)
        
        # Update performance metrics
        self._update_performance_metrics(regime_features)
        
        # Save state periodically
        if len(self.intent_window) % 10 == 0:
            self.save_state_to_redis()
        
        # Return enriched data
        return {
            **tick_data,
            'edge_intent': intent_features,
            'edge_constraint': constraint_features,
            'edge_transition': transition_features,
            'edge_regime': regime_features,
            'edge_should_trade': regime_features.get('should_trade', False),
            'edge_timestamp': datetime.now().isoformat()
        }
    
    def _capture_latency(self, tick_data: Dict) -> Dict:
        """Capture latency timestamps for transition detection."""
        now = datetime.now()
        exchange_time = tick_data.get('exchange_timestamp')
        
        if exchange_time:
            try:
                if isinstance(exchange_time, str):
                    exchange_dt = datetime.fromisoformat(exchange_time.replace('Z', '+00:00'))
                elif isinstance(exchange_time, (int, float)):
                    exchange_dt = datetime.fromtimestamp(exchange_time / 1000)
                else:
                    exchange_dt = exchange_time
                
                latency_ms = (now - exchange_dt).total_seconds() * 1000
            except Exception:
                latency_ms = 0
        else:
            latency_ms = 0
        
        latency_entry = {
            'timestamp': now.isoformat(),
            'latency_ms': latency_ms,
            'symbol': self.symbol
        }
        self.latency_measurements.append(latency_entry)
        
        return latency_entry
    
    def _calculate_intent_layer(
        self,
        tick_data: Dict,
        options_chain: Optional[List[Dict]],
        latency_data: Dict
    ) -> Dict:
        """Calculate Intent Layer indicators (DHDI, motive, hedge pressure)."""
        delta = tick_data.get('delta', 0) or 0
        gamma = tick_data.get('gamma', 0) or 0
        oi = tick_data.get('oi', 0) or tick_data.get('open_interest', 0) or 0
        volume = tick_data.get('volume', 0) or 0
        
        # Delta-Hedge Demand Index (DHDI)
        # Higher absolute delta with high volume suggests hedging activity
        dhdi = abs(delta) * (1 + volume / max(oi, 1)) if oi > 0 else abs(delta)
        
        # Motive classification
        if gamma > 0.01 and abs(delta) > 0.5:
            motive = 'hedge'  # Likely hedging activity
        elif volume > oi * 0.1:
            motive = 'spec'  # Speculative activity
        else:
            motive = 'mixed'
        
        # Hedge pressure (rolling average)
        self.intent_window.append({'dhdi': dhdi, 'motive': motive, 'timestamp': datetime.now()})
        self.intent_window = self.intent_window[-self.window_size:]
        
        recent_dhdi = [w['dhdi'] for w in self.intent_window[-20:]]
        hedge_pressure_ma = sum(recent_dhdi) / len(recent_dhdi) if recent_dhdi else 0
        
        return {
            'dhdi': dhdi,
            'motive': motive,
            'hedge_pressure_ma': hedge_pressure_ma,
            'window_size': len(self.intent_window)
        }
    
    def _calculate_constraint_layer(
        self,
        price: float,
        options_chain: Optional[List[Dict]],
        orderbook: Dict
    ) -> Dict:
        """Calculate Constraint Layer indicators (gamma stress, charm ratio, LCI)."""
        gamma_stress = 0.0
        charm_ratio = 0.0
        lci = 0.0
        session_mult = 1.0
        
        # Calculate gamma stress from options chain if available
        if options_chain and isinstance(options_chain, list):
            total_gamma = sum(opt.get('gamma', 0) for opt in options_chain)
            total_charm = sum(opt.get('charm', 0) for opt in options_chain)
            total_vanna = sum(opt.get('vanna', 0) for opt in options_chain)
            
            # Gamma stress combines gamma, charm, and vanna effects
            gamma_stress = abs(total_gamma) + abs(total_charm) * 0.5 + abs(total_vanna) * 0.3
            
            # Charm to gamma ratio
            charm_ratio = total_charm / total_gamma if total_gamma != 0 else 0
        
        # Liquidity Commitment Index (LCI) from orderbook
        if orderbook:
            buy_depth = orderbook.get('buy', []) or []
            sell_depth = orderbook.get('sell', []) or []
            
            total_bid = sum(level.get('quantity', 0) for level in buy_depth[:5])
            total_ask = sum(level.get('quantity', 0) for level in sell_depth[:5])
            
            # LCI = bid/ask imbalance weighted by depth
            lci = (total_bid - total_ask) / max(total_bid + total_ask, 1)
        
        # Session multiplier (time of day effect)
        now = datetime.now()
        hour = now.hour
        if 9 <= hour <= 10:  # Opening hour
            session_mult = 1.5
        elif 14 <= hour <= 15:  # Closing hour
            session_mult = 1.3
        else:
            session_mult = 1.0
        
        self.constraint_window.append({
            'gamma_stress': gamma_stress,
            'charm_ratio': charm_ratio,
            'lci': lci,
            'timestamp': datetime.now()
        })
        self.constraint_window = self.constraint_window[-self.window_size:]
        
        return {
            'gamma_stress': gamma_stress,
            'charm_ratio': charm_ratio,
            'lci': lci,
            'session_mult': session_mult
        }
    
    def _calculate_transition_layer(
        self,
        intent_features: Dict,
        constraint_features: Dict,
        latency_data: Dict
    ) -> Dict:
        """Calculate Transition Layer indicators (transition probability, time compression)."""
        transition_prob = 0.0
        time_compression = 0.0
        latency_collapse = False
        
        # Check for constraint changes
        if len(self.constraint_window) >= 2:
            prev = self.constraint_window[-2]
            curr = constraint_features
            
            gamma_change = abs(curr['gamma_stress'] - prev.get('gamma_stress', 0))
            lci_change = abs(curr['lci'] - prev.get('lci', 0))
            
            # Transition probability based on changes
            transition_prob = min(gamma_change + lci_change * 2, 1.0)
        
        # Time compression signal (TCS)
        if len(self.latency_measurements) >= 10:
            recent_latencies = [m['latency_ms'] for m in self.latency_measurements[-10:]]
            avg_latency = sum(recent_latencies) / len(recent_latencies)
            current_latency = latency_data.get('latency_ms', 0)
            
            # Time compression = reduction in latency variance
            time_compression = max(0, (avg_latency - current_latency) / max(avg_latency, 1))
            
            # Latency collapse detection
            if current_latency < avg_latency * 0.5:
                latency_collapse = True
        
        self.transition_window.append({
            'transition_prob': transition_prob,
            'time_compression': time_compression,
            'timestamp': datetime.now()
        })
        self.transition_window = self.transition_window[-self.window_size:]
        
        return {
            'transition_probability': transition_prob,
            'time_compression': time_compression,
            'latency_collapse': latency_collapse
        }
    
    def _classify_regime(
        self,
        intent_features: Dict,
        constraint_features: Dict,
        transition_features: Dict
    ) -> Dict:
        """Classify current market regime using River classifier or rule-based fallback."""
        regime_id = -1
        regime_label = 'unknown'
        regime_confidence = 0.0
        should_trade = False
        gate_reason = ''
        recommended_strategies: List[str] = []
        
        # Feature vector for classification
        features = {
            'dhdi': intent_features.get('dhdi', 0),
            'gamma_stress': constraint_features.get('gamma_stress', 0),
            'lci': constraint_features.get('lci', 0),
            'charm_ratio': constraint_features.get('charm_ratio', 0),
            'tcs': transition_features.get('time_compression', 0)
        }
        
        # Try River classifier first
        classifier_pair = self._ensure_classifier()
        if RIVER_AVAILABLE and classifier_pair:
            try:
                feature_extractor, clusterer = classifier_pair
                # Validate both components exist before using
                if feature_extractor is None or clusterer is None:
                    self.logger.debug(f"⚠️ Classifier components incomplete, reinitializing")
                    self.regime_classifier = self._create_new_classifier()
                    feature_extractor, clusterer = self.regime_classifier
                
                # ✅ FIX: River 0.23.0 API - learn_one() returns None, use separate calls
                feature_extractor.learn_one(features)  # Updates model in-place
                processed = feature_extractor.transform_one(features)
                
                clusterer.learn_one(processed)  # Updates model in-place
                regime_id = clusterer.predict_one(processed)
                regime_confidence = 0.7  # Placeholder confidence
            except Exception as e:
                self.logger.debug(f"⚠️ River classification failed: {e}")
        
        # Rule-based fallback
        if regime_id == -1:
            gamma_stress = constraint_features.get('gamma_stress', 0)
            lci = constraint_features.get('lci', 0)
            dhdi = intent_features.get('dhdi', 0)
            
            if gamma_stress > 0.5:
                regime_id = 1
                regime_label = 'high_gamma'
                recommended_strategies = ['gamma_scalping', 'iron_condor']
            elif abs(lci) > 0.3:
                regime_id = 2
                regime_label = 'directional'
                recommended_strategies = ['trend_following', 'momentum']
            elif dhdi > 0.5:
                regime_id = 3
                regime_label = 'hedging_activity'
                recommended_strategies = ['delta_neutral', 'straddle']
            else:
                regime_id = 0
                regime_label = 'neutral'
                recommended_strategies = ['range_trading']
            
            regime_confidence = 0.5
        
        # Trading gate logic
        transition_prob = transition_features.get('transition_probability', 0)
        if transition_prob > 0.7:
            should_trade = False
            gate_reason = 'high_transition_probability'
        elif regime_label == 'unknown':
            should_trade = False
            gate_reason = 'unknown_regime'
        else:
            should_trade = True
            gate_reason = 'regime_stable'
        
        self.regime_window.append({
            'regime_id': regime_id,
            'regime_label': regime_label,
            'timestamp': datetime.now()
        })
        self.regime_window = self.regime_window[-self.window_size:]
        
        return {
            'regime_id': regime_id,
            'regime_label': regime_label,
            'regime_confidence': regime_confidence,
            'should_trade': should_trade,
            'gate_reason': gate_reason,
            'recommended_strategies': recommended_strategies
        }
    
    def _store_intent_indicators(self, intent_features: Dict):
        """Store intent layer indicators in unified hash."""
        payload = {
            'edge_dhdi': intent_features.get('dhdi', 0.0),
            'edge_motive': intent_features.get('motive', 'unknown'),
            'edge_hedge_pressure': intent_features.get('hedge_pressure_ma', 0.0),
            'edge_intent_window': intent_features.get('window_size', 0),
        }
        try:
            self.unified_storage.store_indicators(self.symbol, payload, ttl=300)
        except Exception as e:
            self.logger.error(f"❌ Failed to store intent indicators: {e}")
    
    def _store_constraint_indicators(self, constraint_features: Dict):
        """Store constraint layer indicators in unified hash."""
        payload = {
            'edge_gamma_stress': constraint_features.get('gamma_stress', 0.0),
            'edge_charm_ratio': constraint_features.get('charm_ratio', 0.0),
            'edge_lci': constraint_features.get('lci', 0.0),
            'edge_session_mult': constraint_features.get('session_mult', 1.0),
        }
        try:
            self.unified_storage.store_indicators(self.symbol, payload, ttl=300)
        except Exception as e:
            self.logger.error(f"❌ Failed to store constraint indicators: {e}")
    
    def _store_transition_indicators(self, transition_features: Dict):
        """Store transition layer indicators in unified hash."""
        payload = {
            'edge_transition_probability': transition_features.get('transition_probability', 0.0),
            'edge_time_compression': transition_features.get('time_compression', 0.0),
            'edge_latency_collapse': 1 if transition_features.get('latency_collapse', False) else 0,
        }
        try:
            self.unified_storage.store_indicators(self.symbol, payload, ttl=300)
        except Exception as e:
            self.logger.error(f"❌ Failed to store transition indicators: {e}")
    
    def _store_regime_indicators(self, regime_features: Dict):
        """Store regime classification indicators in unified hash."""
        payload = {
            'edge_regime_id': regime_features.get('regime_id', -1),
            'edge_regime_label': regime_features.get('regime_label', 'unknown'),
            'edge_regime_confidence': regime_features.get('regime_confidence', 0.0),
            'edge_should_trade': 1 if regime_features.get('should_trade', False) else 0,
            'edge_regime_gate_reason': regime_features.get('gate_reason', ''),
            'edge_regime_strategies': ','.join(regime_features.get('recommended_strategies', [])),
        }
        try:
            self.unified_storage.store_indicators(self.symbol, payload, ttl=300)
        except Exception as e:
            self.logger.error(f"❌ Failed to store regime indicators: {e}")
    
    def _update_performance_metrics(self, regime_features: Dict):
        """Update performance metrics in Redis DB2."""
        try:
            # Track regime stability
            if len(self.regime_window) >= 2:
                stability = self._calculate_regime_stability()
                
                perf_key = DatabaseAwareKeyBuilder.regime_accuracy(self.symbol, "5min")
                self.redis_client.setex(
                    perf_key, 3600,
                    json.dumps({
                        'value': stability,
                        'timestamp': datetime.now().isoformat(),
                        'sample_size': len(self.regime_window)
                    })
                )
        except Exception as e:
            self.logger.error(f"❌ Failed to update performance metrics: {e}")
    
    def _calculate_regime_stability(self) -> float:
        """Calculate regime stability (how often regime stays the same)."""
        if len(self.regime_window) < 2:
            return 1.0
        
        transitions = 0
        for i in range(1, len(self.regime_window)):
            if self.regime_window[i]['regime_id'] != self.regime_window[i-1]['regime_id']:
                transitions += 1
        
        stability = 1.0 - (transitions / len(self.regime_window))
        return stability


# Singleton cache for edge calculators
_edge_calculator_cache: Dict[str, RedisStatefulEdgeCalculator] = {}


def get_edge_calculator(symbol: str, redis_client=None) -> RedisStatefulEdgeCalculator:
    """
    Get or create an edge calculator for a symbol.
    
    Args:
        symbol: Trading symbol
        redis_client: Optional Redis client
        
    Returns:
        RedisStatefulEdgeCalculator instance
    """
    if symbol not in _edge_calculator_cache:
        _edge_calculator_cache[symbol] = RedisStatefulEdgeCalculator(
            symbol=symbol,
            redis_client=redis_client
        )
    return _edge_calculator_cache[symbol]
