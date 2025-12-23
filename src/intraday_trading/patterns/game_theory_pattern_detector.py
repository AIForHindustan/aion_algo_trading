"""
Game Theory Pattern Detector
============================

Detects game theory signals - follows EXACT same pattern as other detectors.
"""

import logging
import asyncio
import json
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime

from .pattern_detector import BasePatternDetector
from .game_theory_engine import IntradayGameTheoryEngine
from .utils.pattern_schema import create_pattern



# ✅ Import centralized config utilities
try:
    from shared_core.config_utils.thresholds import get_instrument_config
except ImportError:
    get_instrument_config = None

# ✅ Import Redis key builder
from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder

logger = logging.getLogger(__name__)


class GameTheoryPatternDetector(BasePatternDetector):
    """Detects game theory signals - FIXED to work with available data"""
    
    def __init__(self, config: Dict[str, Any], redis_client, data_source=None):
        """
        Initialize Game Theory Pattern Detector
        
        Args:
            config: Configuration dictionary
            redis_client: Redis client for market data
            data_source: Optional data source with get_microstructure_data method (e.g., PatternDetector instance)
        """
        super().__init__(config, redis_client)
        self.pattern_type = "game_theory_signal"
        self.redis_client = redis_client
        self.data_source = data_source  # ✅ FIX: Store data_source for microstructure data access
        
        # ✅ FIX: Configurable thresholds (more lenient defaults)
        game_theory_config = config.get('game_theory', {}) if isinstance(config, dict) else {}
        self.min_tick_count = game_theory_config.get('min_tick_count', 16)  # Reduced from 32
        self.require_order_book = game_theory_config.get('require_order_book', False)  # Make optional
        self.min_volume_ratio = game_theory_config.get('min_volume_ratio', 1.5)  # Reduced threshold
        
        # Use DatabaseAwareKeyBuilder static methods - no instance needed
        
        self.logger.info(f"✅ [GAME_THEORY_INIT] Initialized with min_tick_count={self.min_tick_count}, require_order_book={self.require_order_book}")
    
    def detect(self, symbol: str, indicators: Dict[str, Any], 
               context: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        ✅ FIXED: Game theory detection with lenient data requirements
        
        Args:
            symbol: Trading symbol
            indicators: Market indicators dict
            context: Optional context dict
            
        Returns:
            Pattern dict or None
        """
        try:
            # 1. Get instrument config (same as other detectors)
            if not get_instrument_config:
                return None
            
            instrument_config = get_instrument_config(symbol)
            if not instrument_config:
                return None
            
            # 2. Check if pattern enabled (same as other detectors)
            enabled_patterns = instrument_config.get("enabled_patterns", [])
            if enabled_patterns and "game_theory_signal" not in enabled_patterns:
                return None
            
            # 3. Get required data from context (same structure as other detectors)
            context = context or {}
            
            # ✅ FIX: Get microstructure data if data_source available
            microstructure_data = {}
            if self.data_source and hasattr(self.data_source, 'get_microstructure_data'):
                try:
                    microstructure_data = self.data_source.get_microstructure_data(symbol, indicators)
                    self.logger.debug(f"✅ [GAME_THEORY] {symbol}: Fetched microstructure data: has_order_book={microstructure_data.get('has_order_book')}, has_ticks={microstructure_data.get('has_ticks')}")
                except Exception as micro_err:
                    self.logger.debug(f"⚠️ [GAME_THEORY] {symbol}: Could not fetch microstructure data: {micro_err}")
            
            # Merge microstructure data with indicators
            full_indicators = {**indicators, **microstructure_data}
            
            # ✅ FIX: Get order book (optional if require_order_book=False)
            order_book = (
                context.get("order_book")
                or microstructure_data.get("order_book")
                or self._get_order_book_from_redis(symbol)
            )
            
            # ✅ FIX: Only require order book if configured
            if self.require_order_book and not order_book:
                self.logger.debug(f"❌ [GAME_THEORY] {symbol}: Order book required but not available")
                return None
            elif order_book:
                context["order_book"] = order_book
                self.logger.debug(f"✅ [GAME_THEORY] {symbol}: Order book available")
            
            # ✅ FIX: Get ticks (more lenient - use what's available)
            ticks = (
                context.get("ticks")
                or context.get("tick_history")
                or microstructure_data.get("recent_ticks")
                or self._get_ticks_from_redis(symbol, window=self.min_tick_count)
            )
            
            tick_count = len(ticks) if ticks and isinstance(ticks, list) else 0
            
            # ✅ FIX: Don't fail if tick count is low - proceed anyway
            if tick_count < self.min_tick_count:
                self.logger.debug(f"⚠️ [GAME_THEORY] {symbol}: Low tick count {tick_count} < {self.min_tick_count}, proceeding anyway")
                # Don't fail - we can still work with other indicators
            
            if ticks:
                context["ticks"] = ticks
                context["tick_history"] = ticks
                self.logger.debug(f"✅ [GAME_THEORY] {symbol}: Using {tick_count} ticks")
            
            # ✅ FIX: Don't require full microstructure data
            has_sufficient_data = True
            
            # Only check if we're requiring order book
            if self.require_order_book and not order_book:
                has_sufficient_data = False
                self.logger.debug(f"❌ [GAME_THEORY] {symbol}: Order book required but not available")
            
            if not has_sufficient_data:
                return None
            
            # 6. Call your existing game theory engine
            # Initialize engine with proper thresholds
            min_confidence = instrument_config.get("game_theory_min_confidence", 0.7)
            
            engine = IntradayGameTheoryEngine(
                min_confidence=min_confidence,
                redis_client=self.redis_client
            )
            
            # Generate signal
            signal = asyncio.run(engine.generate_intraday_signals(symbol, order_book, ticks))
            if not signal:
                return None
            
            # Check confidence threshold
            confidence = signal.get("confidence", 0.0)
            if confidence < min_confidence:
                return None
            
            # 7. Convert to standard pattern (MUST match other patterns)
            pattern = self._create_standard_pattern(signal, symbol, indicators, instrument_config)
            return pattern
            
        except Exception as e:
            self.logger.error(f"Game theory detection failed for {symbol}: {e}")
            return None
    
    def _get_order_book_from_redis(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get order book from Redis DB1 - like other detectors do"""
        if not self.redis_client:
            return None
        
        try:
            # Get DB1 client
            db1_client = None
            if hasattr(self.redis_client, 'get_client'):
                db1_client = self.redis_client.get_client(1)
            elif hasattr(self.redis_client, 'redis'):
                db1_client = self.redis_client.redis
            elif hasattr(self.redis_client, 'get'):
                db1_client = self.redis_client
            
            if not db1_client:
                return None
            
            # Try key builder method first
            try:
                # Use standard order book key pattern
                from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
                canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
                key = f"order_book:{canonical_symbol}"
                data = db1_client.get(key)
                if data:
                    if isinstance(data, bytes):
                        data = data.decode('utf-8')
                    parsed = json.loads(data) if isinstance(data, str) else data
                    if isinstance(parsed, dict) and ('buy' in parsed or 'sell' in parsed):
                        return parsed
            except Exception:
                pass
            
            # Fallback: Try common key patterns
            for key_pattern in [f"depth:{symbol}", f"depth_data:{symbol}", f"market_depth:{symbol}"]:
                try:
                    data = db1_client.get(key_pattern)
                    if data:
                        if isinstance(data, bytes):
                            data = data.decode('utf-8')
                        parsed = json.loads(data) if isinstance(data, str) else data
                        if isinstance(parsed, dict) and ('buy' in parsed or 'sell' in parsed):
                            return parsed
                except Exception:
                    continue
            
            return None
        except Exception as e:
            self.logger.debug(f"Could not fetch order book from Redis for {symbol}: {e}")
            return None
    
    def _get_ticks_from_redis(self, symbol: str, window: int = 32) -> List[Dict[str, Any]]:
        """Get recent ticks from Redis - like other detectors do"""
        if not self.redis_client:
            return []
        
        try:
            # Get DB1 client
            db1_client = None
            if hasattr(self.redis_client, 'get_client'):
                db1_client = self.redis_client.get_client(1)
            elif hasattr(self.redis_client, 'redis'):
                db1_client = self.redis_client.redis
            elif hasattr(self.redis_client, 'get'):
                db1_client = self.redis_client
            
            if not db1_client:
                return []
            
            # Use DatabaseAwareKeyBuilder.live_ticks_stream() to get the stream key
            try:
                key = DatabaseAwareKeyBuilder.live_ticks_stream(symbol)
                # Get last N ticks from stream (xrevrange returns most recent first)
                ticks = db1_client.xrevrange(key, count=window)
                if ticks:
                    # Parse JSON strings to dicts
                    return [json.loads(tick) if isinstance(tick, (str, bytes)) else tick 
                            for tick in ticks]
            except Exception as e:
                self.logger.debug(f"Could not fetch ticks using live_ticks for {symbol}: {e}")
            
            return []
        except Exception as e:
            self.logger.debug(f"Could not fetch ticks from Redis for {symbol}: {e}")
            return []
    
    def _create_standard_pattern(self, gt_signal: Dict[str, Any], symbol: str,
                                 indicators: Dict[str, Any], 
                                 instrument_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create pattern in EXACT same format as your other 5 detectors
        
        Args:
            gt_signal: Game theory signal dict
            symbol: Trading symbol
            indicators: Market indicators dict
            instrument_config: Instrument configuration dict
            
        Returns:
            Standard pattern dict or None
        """
        # Convert signal direction (LONG/SHORT -> BUY/SELL)
        signal_map = {"LONG": "BUY", "SHORT": "SELL"}
        signal = signal_map.get(gt_signal.get("signal", "").upper())
        if not signal:
            return None
        
        # Get confidence with VIX adjustment (like other detectors)
        base_confidence = gt_signal.get("confidence", 0.0)
        vix_regime = indicators.get("vix_regime", "NORMAL")
        confidence_multipliers = {"HIGH": 0.8, "NORMAL": 1.0, "LOW": 1.2, "PANIC": 0.6}
        adjusted_confidence = min(0.95, base_confidence * confidence_multipliers.get(vix_regime, 1.0))
        
        # Get last price
        last_price = indicators.get("last_price", 0.0)
        if not last_price:
            last_price = gt_signal.get("reference_price", 0.0)
        
        if not last_price or last_price <= 0:
            return None
        
        # Get position size in lots (like other detectors)
        position_size = self._calculate_position_size(adjusted_confidence, symbol, instrument_config)
        
        # Get volume ratio
        volume_ratio = indicators.get("volume_ratio", 1.0)
        
        # Create pattern using the SAME function as other detectors
        pattern = create_pattern(
            symbol=symbol,
            pattern_type="game_theory_signal",  # Must match pattern registry
            signal=signal,
            confidence=adjusted_confidence,
            last_price=last_price,
            volume_ratio=volume_ratio,
            position_size=position_size,
            stop_loss=gt_signal.get("stop_loss"),
            target_price=gt_signal.get("take_profit"),
            alert_id=str(uuid.uuid4()),
            timestamp_ms=int(datetime.utcnow().timestamp() * 1000),
            timeframe="1-5min",  # Pass as kwarg
            metadata={
                "components": gt_signal.get("components", {}),
                "game_theory_components": gt_signal.get("components", {}), # ✅ Added for UnifiedAlertBuilder compatibility
                "nash_signal": gt_signal.get("components", {}).get("nash", {}),
                "fomo_signal": gt_signal.get("components", {}).get("fomo", {}),
                "prisoners_dilemma": gt_signal.get("components", {}).get("prisoners_dilemma", {}),
                "strategy_id": "game_theory_v1"
            }
        )
        
        return pattern
    
    def _calculate_position_size(self, confidence: float, symbol: str, 
                                 instrument_config: Dict[str, Any]) -> int:
        """Calculate position size like other detectors do"""
        # Get base position from confidence
        if confidence >= 0.85:
            size_multiplier = 1.0
        elif confidence >= 0.7:
            size_multiplier = 0.65
        elif confidence >= 0.55:
            size_multiplier = 0.4
        else:
            return 0
        
        # Get instrument-specific configuration from thresholds.py
        # instrument_config already comes from get_instrument_config() which uses thresholds.py
        base_lots = instrument_config.get("base_position", 1)
        max_lots = instrument_config.get("max_position", 10)
        
        # If not in instrument_config, try to get from thresholds.py directly
        if base_lots == 1 and max_lots == 10:
            try:
                from shared_core.config_utils.thresholds import get_instrument_config
                full_config = get_instrument_config(symbol)
                if full_config:
                    base_lots = full_config.get("base_position", instrument_config.get("lot_size", 1))
                    max_lots = full_config.get("max_position", 10)
            except Exception:
                pass
        
        # Calculate actual lots
        lots = int(base_lots * size_multiplier)
        return max(1, min(lots, max_lots))
    
    def _generate_simplified_signal(self, symbol: str, indicators: Dict[str, Any],
                                    order_book: Optional[Dict[str, Any]] = None,
                                    ticks: Optional[List[Dict[str, Any]]] = None) -> Optional[Dict[str, Any]]:
        """
        ✅ FIX: Simplified game theory signal generation when full engine unavailable
        
        Uses indicators and basic microstructure data to generate signals.
        """
        try:
            # Extract key indicators
            last_price = float(indicators.get("last_price", 0) or 0)
            underlying_price = float(indicators.get("underlying_price", 0) or 0)
            delta = float(indicators.get("delta", 0) or 0)
            gamma = float(indicators.get("gamma", 0) or 0)
            volume_ratio = float(indicators.get("volume_ratio", 1.0) or 1.0)
            order_flow_imbalance = float(indicators.get("order_flow_imbalance", 0) or 0)
            order_book_imbalance = float(indicators.get("order_book_imbalance", 0) or 0)
            microprice = float(indicators.get("microprice", last_price) or last_price)
            
            if last_price <= 0 or underlying_price <= 0:
                return None
            
            # ✅ FIX: Check min_volume_ratio from config
            if volume_ratio < self.min_volume_ratio:
                self.logger.debug(f"❌ [GAME_THEORY_SIMPLIFIED] {symbol}: Volume ratio {volume_ratio:.2f} < min_volume_ratio {self.min_volume_ratio}")
                return None
            
            signal = None
            confidence = 0.0
            
            # Rule 1: Delta-based positioning (high delta + high volume = buying pressure)
            # ✅ FIX: Use configurable min_volume_ratio instead of hardcoded 2.0
            min_volume_for_delta = max(self.min_volume_ratio, 2.0)  # At least 2.0x for delta-based signals
            if delta > 0.6 and volume_ratio > min_volume_for_delta:
                signal = "LONG"
                confidence = 0.65 + min(0.2, (delta - 0.6) * 0.5)
                self.logger.debug(f"✅ [GAME_THEORY_SIMPLIFIED] {symbol}: Delta-based signal (delta={delta:.3f}, vol={volume_ratio:.2f})")
            
            # Rule 2: Low delta + high volume = selling pressure
            # ✅ FIX: Use configurable min_volume_ratio instead of hardcoded 2.0
            elif delta < 0.4 and volume_ratio > min_volume_for_delta:
                signal = "SHORT"
                confidence = 0.65 + min(0.2, (0.4 - delta) * 0.5)
                self.logger.debug(f"✅ [GAME_THEORY_SIMPLIFIED] {symbol}: Delta-based signal (delta={delta:.3f}, vol={volume_ratio:.2f})")
            
            # Rule 3: Order flow imbalance
            elif abs(order_flow_imbalance) > 20:
                signal = "LONG" if order_flow_imbalance > 0 else "SHORT"
                confidence = 0.7 + min(0.25, abs(order_flow_imbalance) / 100)
                self.logger.debug(f"✅ [GAME_THEORY_SIMPLIFIED] {symbol}: Order flow-based signal (ofi={order_flow_imbalance:.2f})")
            
            # Rule 4: Microprice divergence
            elif abs(microprice - last_price) / last_price > 0.002:  # 0.2% divergence
                signal = "LONG" if microprice > last_price else "SHORT"
                confidence = 0.68
                self.logger.debug(f"✅ [GAME_THEORY_SIMPLIFIED] {symbol}: Microprice divergence signal (microprice={microprice:.2f} vs price={last_price:.2f})")
            
            # Rule 5: Gamma-based positioning
            elif gamma > 0.0001:
                gamma_exposure = indicators.get("gamma_exposure")
                if gamma_exposure and gamma_exposure > 0:
                    signal = "SHORT"  # Positive gamma = potential reversal down
                    confidence = 0.66
                elif gamma_exposure and gamma_exposure < 0:
                    signal = "LONG"  # Negative gamma = potential reversal up
                    confidence = 0.66
                self.logger.debug(f"✅ [GAME_THEORY_SIMPLIFIED] {symbol}: Gamma-based signal (gamma_exposure={gamma_exposure})")
            
            if not signal:
                return None
            
            # Check minimum confidence
            min_confidence = 0.6  # Lower threshold for simplified signals
            if confidence < min_confidence:
                return None
            
            # Build signal dict compatible with engine output
            return {
                "signal": signal,
                "confidence": confidence,
                "reference_price": last_price,
                "components": {
                    "simplified": True,
                    "delta": delta,
                    "volume_ratio": volume_ratio,
                    "order_flow_imbalance": order_flow_imbalance,
                    "order_book_imbalance": order_book_imbalance,
                },
                "timeframe": "1-5min"
            }
        except Exception as e:
            self.logger.error(f"❌ [GAME_THEORY_SIMPLIFIED] {symbol}: Error generating simplified signal: {e}", exc_info=True)
            return None
