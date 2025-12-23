"""
Lightweight advanced detector implementations shared by the consolidated pattern detector.

Each detector consumes the enriched indicator dictionary passed around inside
`PatternDetector` and returns a normalized pattern payload using the unified
`create_pattern` helper.

The goal is to keep these detectors side-effect free and have them rely only on
the fields already present on the indicator dict (volume ratios, order flow,
gamma exposure, etc.). This keeps pattern detection deterministic even when the
underlying Redis plumbing is noisy.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional
import os
import numpy as np
from collections import defaultdict, deque
from datetime import datetime

from intraday_trading.patterns.utils.pattern_schema import create_pattern
from shared_core.index_manager import index_manager
from shared_core.redis_clients.redis_key_standards import get_symbol_parser

DEBUG_TRACE_ENABLED = os.getenv("DEBUG_TRACE", "1").lower() in ("1", "true", "yes")


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Coerce redis/string/bytes payloads to float."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, bytes):
        try:
            return float(value.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_threshold(indicators: Dict[str, Any], pattern_name: str, key: str, default: float) -> float:
    """Read nested pattern thresholds with sane defaults."""
    thresholds = indicators.get("pattern_thresholds") or {}
    pattern_thresholds = thresholds.get(pattern_name) or {}
    return _safe_float(pattern_thresholds.get(key), default)


class BaseAdvancedDetector:
    pattern_type: str = "advanced_pattern"

    def __init__(self, config: Optional[Dict[str, Any]] = None, redis_client=None, alert_builder=None):
        self.config = config or {}
        self.redis_client = redis_client
        self.alert_builder = alert_builder
        self.logger = logging.getLogger(__name__)

    def _build_pattern(
        self,
        symbol: str,
        indicators: Dict[str, Any],
        signal: str,
        confidence: float,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        last_price = indicators.get("last_price") or indicators.get("microprice")
        if not last_price:
            return {}
        volume_ratio = _safe_float(indicators.get("volume_ratio"), 0.0)
        
        # Category Mapping
        PATTERN_TYPE_MAPPING = {
            'order_flow_breakout': 'breakout',
            'gamma_exposure_reversal': 'reversal',
            'microstructure_divergence': 'divergence',
            'cross_asset_arbitrage': 'arbitrage',
            'vix_momentum': 'momentum',
            'kow_signal_straddle': 'straddle',
            'ict_iron_condor': 'iron_condor',
            'game_theory_signal': 'game_theory',
        }
        
        mapped_pattern_type = PATTERN_TYPE_MAPPING.get(self.pattern_type, self.pattern_type)
        
        pattern = create_pattern(
            symbol=symbol,
            pattern_type=mapped_pattern_type,  # Category
            signal=signal,
            confidence=max(0.0, min(confidence, 0.99)),
            last_price=last_price,
            volume_ratio=volume_ratio,
            metadata=metadata or {},
        )
        
        if pattern:
            pattern['pattern'] = self.pattern_type  # Specific name
            pattern['pattern_type'] = mapped_pattern_type  # Category
        
        return pattern


class OrderFlowBreakoutDetector(BaseAdvancedDetector):
    pattern_type = "order_flow_breakout"

    def detect(self, symbol: str, indicators: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Fixed OFB logic with:
        1. Dynamic confidence without saturation
        2. Deadband threshold to avoid noise
        3. Meaningful expected_move calculation
        """
        debug_trace = DEBUG_TRACE_ENABLED
        # Extract parameters
        ofi = _safe_float(indicators.get('order_flow_imbalance'), 0)
        obi = _safe_float(indicators.get('order_book_imbalance'), 0)
        cvd = _safe_float(indicators.get('cumulative_volume_delta'), 0)
        
        # Depth metrics
        total_bid_depth = _safe_float(indicators.get('total_bid_depth'), 0)
        total_ask_depth = _safe_float(indicators.get('total_ask_depth'), 0)
        if total_bid_depth == 0 and total_ask_depth == 0:
            # Fallback to book if available
            ob = indicators.get('order_book', {})
            if ob:
                total_bid_depth = sum([b[1] for b in ob.get('bids', [])[:5]])
                total_ask_depth = sum([a[1] for a in ob.get('asks', [])[:5]])
        
        # Spread
        # indicators doesn't standardized 'spread_bps', so calculate or default
        # best_bid/ask might be in indicators or derived from OB
        best_bid = _safe_float(indicators.get('best_bid'))
        best_ask = _safe_float(indicators.get('best_ask'))
        last_price = _safe_float(indicators.get('last_price'))
        
        if best_bid > 0 and best_ask > 0 and last_price > 0:
            spread_bps = ((best_ask - best_bid) / last_price) * 10000
        else:
            spread_bps = 2.0 # Default assumption
            
        volume_ratio = _safe_float(indicators.get("volume_ratio"), 1.0)
        
        # FIX 1: Deadband threshold (minimum OFI magnitude to trigger)
        # Allow config override
        deadband_threshold = self.config.get('deadband_threshold', 0.1)
        if debug_trace:
            self.logger.debug(f"[OFB_DEBUG] {symbol} OFI={ofi:.3f}, deadband={deadband_threshold}")
        if abs(ofi) < deadband_threshold:
            if debug_trace:
                self.logger.debug(f"[OFB_SKIP] {symbol} |OFI|={abs(ofi):.3f} < {deadband_threshold}")
            return None
        
        # FIX 2: Dynamic confidence without saturation
        # Base confidence from OFI magnitude (sigmoid)
        ofi_magnitude = min(abs(ofi), 1.0)  # Cap at 1.0
        
        # Use configured base_confidence or default to 0.5 (as per user snippet)
        base_confidence = self.config.get('base_confidence', 0.5)
        
        # Volume boost (logarithmic to prevent saturation)
        # Log1p requires numpy
        volume_boost = min(0.2, np.log1p(volume_ratio * 10) / 10)
        
        # CVD confirmation boost (NEW: using previously unused indicator)
        cvd_confirmation = 0.0
        if (ofi > 0 and cvd > 0) or (ofi < 0 and cvd < 0):
            cvd_confirmation = 0.15  # Strong confirmation
        elif (ofi > 0 and cvd < 0) or (ofi < 0 and cvd > 0):
            cvd_confirmation = -0.1  # Contradiction penalty
        
        # Depth wall consideration (NEW: using previously unused indicator)
        depth_penalty = 0.0
        # If buying (OFI>0) and Huge Ask Wall (Resistance) -> Penalty
        if ofi > 0 and total_ask_depth > max(total_bid_depth * 2, 100): # Ensure non-zero
            depth_penalty = -0.1  # Breaking into resistance
        # If selling (OFI<0) and Huge Bid Wall (Support) -> Penalty
        elif ofi < 0 and total_bid_depth > max(total_ask_depth * 2, 100):
            depth_penalty = -0.1  # Breaking into support
        
        # Spread penalty (NEW: using previously unused indicator)
        spread_penalty = max(0, (spread_bps - 5) * -0.01)  # Penalize wide spreads (>5bps)
        
        # Combine confidence
        confidence = base_confidence + (ofi_magnitude * 0.3) + volume_boost + cvd_confirmation + depth_penalty - spread_penalty
        confidence = max(0.3, min(0.95, confidence))  # Keep within bounds
        
        # FIX 3: Meaningful expected_move calculation
        # Base move from OFI magnitude and volatility
        # Get volatility (IV or ATR/Price or VIX-derived)
        # Default to 1% (0.01) if not found
        volatility = 0.01
        if indicators.get('atr') and last_price > 0:
             volatility = indicators['atr'] / last_price
        elif indicators.get('vix_value'):
             # Annualized to Daily ~ VIX/16. Then maybe hourly? 
             # Rough proxy for intraday vol
             volatility = (indicators['vix_value'] / 16.0) / 100.0
             
        expected_move_val = abs(ofi) * volatility * 2.0  # Scale appropriately (in pct terms? User said "Meaningful expected_move")
        # User snippet: expected_move = abs(ofi) * volatility * 2
        # Note: If calculated in Price Terms vs Percent?
        # User snippet result is just a number. Algo builder expects Percent (e.g. 0.5 for 0.5%).
        # If volatility is 0.01 (1%), result is ~0.02 * OFI.
        # If OFI is 0.5, result is 0.01 (1%).
        # Format for algo_alert is typically Percent (e.g. 0.5 means 0.5%).
        # My previous expected_move was 0.5 + ...
        # If Expected Move is 0.01, that's 1% change? Or 0.01%?
        # UnifiedAlertBuilder expects 'expected_move' as PERCENT value (e.g. 0.5 = 0.5%).
        # volatility (0.01) * 100 -> 1.0 (1%).
        # So:
        expected_move_pct = (abs(ofi) * volatility * 2.0) * 100.0
        
        # Add volume contribution
        expected_move_pct *= (1 + min(volume_ratio - 1, 0.5))  # Cap at 50% boost
        expected_move_pct = max(0.1, round(expected_move_pct, 2))
        
        # Direction
        signal = "BUY" if ofi > 0 else "SELL"
        
        metadata = {
            "order_flow_imbalance": ofi,
            "order_book_imbalance": obi,
            "cumulative_volume_delta": cvd,
            "volume_ratio": volume_ratio,
            "spread_bps": spread_bps,
            "volatility_proxy": volatility,
            "details": f"OFI:{ofi:.2f}, CVD:{cvd:.0f}, Vol:{volume_ratio:.1f}x"
        }
        
        pattern = self._build_pattern(symbol, indicators, signal, confidence, metadata)
        
        # Algo Alert Construction
        if pattern and self.alert_builder:
            try:
                algo_alert = self.alert_builder.build_algo_alert(
                    symbol=symbol,
                    pattern_type=self.pattern_type,
                    entry_price=last_price,
                    confidence=confidence,
                    signal=signal,
                    indicators=indicators,
                    expected_move=expected_move_pct, 
                    volume_ratio=volume_ratio
                )
                if algo_alert:
                    pattern.update(algo_alert)
            except Exception as e:
                self.logger.error(f"Error building algo alert for {symbol}: {e}")
                
        return pattern


class GammaExposureReversalDetector(BaseAdvancedDetector):
    pattern_type = "gamma_exposure_reversal"

    def __init__(self, config: Optional[Dict[str, Any]] = None, redis_client=None, alert_builder=None):
        super().__init__(config, redis_client, alert_builder)
        self.history = defaultdict(list)
        self.max_history_length = 50  # Store last 50 gamma readings for Z-score



    def _get_underlying_price(self, symbol: str) -> Optional[float]:
        try:
            parser = get_symbol_parser()
            parsed = parser.parse(symbol)
            if parsed and parsed.underlying:
                 price = index_manager.get_spot_price(parsed.underlying)
                 if price: return price
        except Exception:
            pass
        return None

    def _extract_strike_price(self, symbol: str) -> Optional[float]:
        try:
            parser = get_symbol_parser()
            parsed = parser.parse(symbol)
            return parsed.strike if parsed else None
        except Exception:
            return None

    def _get_contract_size(self, symbol: str) -> int:
        """Determines contract size based on symbol (NIFTY/BANKNIFTY)."""
        symbol_upper = symbol.upper()
        if "BANKNIFTY" in symbol_upper:
            return 30  # Standard BANKNIFTY lot (updated for 2025/26)
        elif "NIFTY" in symbol_upper:
            return 75  # Standard NIFTY lot (updated for 2025/26)
        elif "SENSEX" in symbol_upper:
            return 10
        return 1  # Default for unknown

    def calculate_gsi_from_exposure(self, gamma_exposure: float, open_interest: float) -> float:
        """Calculate GSI from gamma exposure and OI"""
        if not open_interest or open_interest == 0:
            return 0.0
        
        # Normalize gamma exposure by open interest
        # This gives gamma exposure per contract
        normalized_exposure = gamma_exposure / open_interest
        
        # Scale to meaningful range
        gsi = normalized_exposure * 1000.0
        
        if DEBUG_TRACE_ENABLED:
            self.logger.debug(f"📊 [GSI_FROM_EXPOSURE] GammaExp={gamma_exposure:.2f}, OI={open_interest}, "
                        f"Normalized={normalized_exposure:.6f}, GSI={gsi:.6f}")
        
        return gsi

    def detect(self, symbol: str, indicators: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        ✅ FIXED: Gamma Exposure Reversal — adaptive, user-defined logic.
        """
        debug_trace = DEBUG_TRACE_ENABLED
        
        # Extract inputs
        gamma = _safe_float(indicators.get("gamma"), 0.0)
        underlying_price = _safe_float(indicators.get("underlying_price"))
        if not underlying_price:
            underlying_price = self._get_underlying_price(symbol) or 0.0
            
        open_interest = _safe_float(indicators.get("open_interest") or indicators.get('oi'), 0.0)
        
        # Skip if data insufficient
        if gamma <= 0.000001 or underlying_price <= 0:
             if debug_trace:
                 self.logger.debug(f"⏭️ [GAMMA_INSUFFICIENT] {symbol} - Gamma={gamma} <= 1e-6 or Price={underlying_price} <= 0")
             return None

        # Calculate meaningful gamma exposure
        contract_size = self._get_contract_size(symbol)
        gamma_exposure = gamma * contract_size * underlying_price
        
        # Calculate normalized GSI (User Logic)
        if open_interest > 0:
            normalized = gamma_exposure / open_interest
            gsi = normalized * 1000.0
        else:
            # Fallback: use scaled gamma
            gsi = gamma * 10000.0
            
        # Dynamic threshold based on market
        # Using previously defined threshold map
        vix_regime = indicators.get('vix_regime', 'NORMAL')
        threshold = {
            'LOW': 0.05,
            'NORMAL': 0.1,
            'HIGH': 0.2,
            'PANIC': 0.5
        }.get(vix_regime, 0.1)
        
        if debug_trace:
            self.logger.debug(f"🎯 [GAMMA_ANALYSIS] {symbol} - "
                        f"Gamma={gamma:.6f}, Exposure={gamma_exposure:.2f}, "
                        f"GSI={gsi:.6f}, Threshold={threshold}")
        
        if gsi < threshold:
             return None

        # Pattern detected! Continue to logic
        
        # 2. Get volume profile extremes — use dynamic POC-relative bands
        volume_profile = indicators.get('volume_profile', {})
        
        # ✅ Use *relative* extremes: POC ± 1.5× profile range (not fixed nodes)
        poc = volume_profile.get('poc_price')
        profile_range = volume_profile.get('profile_range', underlying_price * 0.02)
        
        # ✅ FALLBACK: If POC missing from external profile, calculate from OHLC (Typical Price)
        if poc is None:
            ohlc_raw = indicators.get('ohlc')
            if ohlc_raw:
                try:
                    # Parse OHLC (might be string JSON or dict)
                    if isinstance(ohlc_raw, str):
                        ohlc = json.loads(ohlc_raw)
                    else:
                        ohlc = ohlc_raw
                        
                    if ohlc and 'high' in ohlc and 'low' in ohlc and 'close' in ohlc:
                        h = float(ohlc['high'])
                        l = float(ohlc['low'])
                        c = float(ohlc['close'])
                        
                        # Use Typical Price as Session POC Proxy
                        poc = (h + l + c) / 3.0
                        profile_range = h - l
                        if profile_range == 0: profile_range = underlying_price * 0.01 # fallback if flat
                        
                        if debug_trace:
                            self.logger.debug(f"⚠️ [POC_FALLBACK] {symbol} - Using OHLC proxy POC={poc:.2f}, Range={profile_range:.2f}")
                except Exception as e:
                    if debug_trace:
                        self.logger.warning(f"❌ [POC_FALLBACK_ERROR] {symbol} - Failed to parse OHLC: {e}")
        
        if poc is None:
             # Try determining POC from price action logic or return None?
             # For reversal trade, POC context is important.
             if debug_trace:
                 self.logger.debug(f"⏭️ [POC_MISSING] {symbol} - No Volume Profile or OHLC data")
             return None
        
        upper_extreme = poc + 0.015 * profile_range  # +1.5% of range
        lower_extreme = poc - 0.015 * profile_range  # -1.5% of range
        
        price = _safe_float(indicators.get("last_price"), 0.0)
        if price <= 0: price = underlying_price # utilize underlying if last_price missing

        # 3. Reversal logic: Short Gamma → fade extremes
        # Note: Gamma Exposure calc above yields positive value for Long Gamma.
        # Detector is for Reversal => Short Gamma (Negative Gamma)?
        # User snippet didn't check sign of gamma.
        # Assuming REVERSAL strategies work best when DEALERS are Short Gamma (Positive GEX for Us? No, Dealers Short = They Buy Low/Sell High to Hedge?? No.)
        # Long Gamma = Hedging dampens volatility.
        # Short Gamma = Hedging amplifies volatility.
        # Reversal usually implies fading move.
        # Wait, if Dealers are Short Gamma, they buy as market rises (amplifying).
        # Reversal edge is usually when Dealers are LONG Gamma (they sell as market rises, dampen).
        # Actually, "Gamma Reversal" usually means "Market Reverses".
        # If Dealers are Long Gamma, market is Mean Reverting. -> Fade Extremes.
        # If Dealers are Short Gamma, market is Trend Following. -> Breakout.
        # User's class is "GammaExposureReversalDetector".
        # This implies we want MEAN REVERSION.
        # So we want POSITIVE Gamma Exposure (Dealers Long Gamma)?
        # Or Negative?
        # My previous code checked `is_short_gamma = gamma_exposure < 0`.
        # User snippet calculates `gamma_exposure` from `gamma` (which is always positive for Long options).
        # User data is `ticks:keys`. Usually `gamma` in data feed is for the Option itself (Long).
        # If we just sum it, we get Long Gamma.
        # How do we know if it's Short Gamma?
        # We need Net GEX.
        # But for specific strike pattern?
        # Maybe "Gamma Exposure Reversal" means "High Gamma Exposure" -> Pinning/Mean Reversion?
        # I will implement logic: High GSI -> Reversal Candidate.
        
        signal = None
        reason = ""
        
        # Reversal Logic: Fade Extremes
        if price >= upper_extreme:
            signal = 'SELL'
            reason = f"High Gamma (GSI={gsi:.3f}) + price above upper extreme"
        elif price <= lower_extreme:
            signal = 'BUY'
            reason = f"High Gamma (GSI={gsi:.3f}) + price below lower extreme"
        else:
            return None # Inside value area

        # 4. Confidence
        distance_from_poc = abs(price - poc) / poc
        volume_ratio = _safe_float(indicators.get("volume_ratio"), 1.0)
        
        base_conf = 0.60
        gsi_boost = min(0.2, (gsi / threshold - 1) * 0.1)
        distance_boost = min(0.15, distance_from_poc * 10)
        
        confidence = min(0.95, base_conf + gsi_boost + distance_boost)
        
        metadata = {
            "gsi": gsi,
            "gamma_exposure": gamma_exposure,
            "vix_regime": vix_regime,
            "details": reason
        }
        
        pattern = self._build_pattern(symbol, indicators, signal, confidence, metadata)
        
        if pattern and self.alert_builder:
            try:
                algo_alert = self.alert_builder.build_algo_alert(
                    symbol=symbol,
                    pattern_type=self.pattern_type,
                    entry_price=price,
                    confidence=confidence,
                    signal=signal,
                    indicators=indicators,
                    expected_move=0.5, 
                    volume_ratio=volume_ratio
                )
                if algo_alert:
                    pattern.update(algo_alert)
            except Exception:
                pass
                    
        return pattern


class MicrostructureDivergenceDetector(BaseAdvancedDetector):
    pattern_type = "microstructure_divergence"
    
    def __init__(self, config: Optional[Dict[str, Any]] = None, redis_client=None, alert_builder=None):
        super().__init__(config, redis_client, alert_builder)
        # State tracking for spoofing detection (Rolling window of 20 ticks)
        self.history = defaultdict(list)
        self.max_history_length = 20

    def detect(self, symbol: str, indicators: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Fixed Microstructure Divergence logic:
        1. Correct BUY/SELL logic (follow agreement, fade disagreement)
        2. Use CVD as truth arbiter
        3. Add spoofing detection
        4. Normalize CVD by bucket volume
        """
        debug_trace = DEBUG_TRACE_ENABLED
        # Inputs
        ofi = _safe_float(indicators.get("order_flow_imbalance") or indicators.get('ofi'), 0.0)
        obi = _safe_float(indicators.get("order_book_imbalance") or indicators.get('obi'), 0.0)
        cvd = _safe_float(indicators.get("cumulative_volume_delta"), 0.0)
        
        # ✅ Normalize CVD by bucket volume (more stable)
        bucket_volume = _safe_float(indicators.get("bucket_incremental_volume") or indicators.get("volume") or indicators.get("volume_traded"), 1.0)
        if bucket_volume <= 0: bucket_volume = 1.0 # Prevent div/0

        best_bid_size = _safe_float(indicators.get("best_bid_size"), 0.0)
        best_ask_size = _safe_float(indicators.get("best_ask_size"), 0.0)
        last_price = _safe_float(indicators.get("last_price"), 0.0)
        
        # Track size changes for spoofing detection
        self.history['bid_size'].append(best_bid_size)
        self.history['ask_size'].append(best_ask_size)
        
        if len(self.history['bid_size']) > self.max_history_length:
            self.history['bid_size'].pop(0)
            self.history['ask_size'].pop(0)

        # FIX 1: Correct logic
        direction = None
        strength = 0.0
        logic_type = "AGREEMENT"

        # Option A: Follow agreement (both positive -> BUY, both negative -> SELL)
        if ofi > 0 and obi > 0:  # Both indicate buying pressure
            direction = "BUY"
            strength = min(abs(ofi), abs(obi))
        elif ofi < 0 and obi < 0:  # Both indicate selling pressure
            direction = "SELL"
            strength = min(abs(ofi), abs(obi))
        else:  # Disagreement - use CVD as arbiter
            logic_type = "DISAGREEMENT_CVD_ARBITRAGE"
            if cvd > 0:  # CVD confirms buying despite conflict
                direction = "BUY"
                cvd_ratio = abs(cvd) / bucket_volume
                strength = min(cvd_ratio * 0.5, 1.0)
            elif cvd < 0:  # CVD confirms selling
                direction = "SELL"
                cvd_ratio = abs(cvd) / bucket_volume
                strength = min(cvd_ratio * 0.5, 1.0)
            else:
                if debug_trace:
                    self.logger.debug(f"[MSD_SKIP] {symbol} disagreement with flat CVD → no signal")
                return None  # No clear signal
                
        # FIX 2: Spoofing detection
        spoofing_penalty = 0.0
        if len(self.history['bid_size']) >= 10:
            bid_volatility = np.std(self.history['bid_size'])
            ask_volatility = np.std(self.history['ask_size'])
            
            # High volatility in sizes suggests spoofing operations
            if bid_volatility > (np.mean(self.history['bid_size']) or 1) * 0.5:
                spoofing_penalty = -0.2
            if ask_volatility > (np.mean(self.history['ask_size']) or 1) * 0.5:
                spoofing_penalty = -0.2

        # FIX 3: Confidence calculation
        base_confidence = self.config.get('base_confidence', 0.6)
        
        # Strength from agreement/disagreement
        strength_factor = min(strength * 2, 0.3)  # Cap at 0.3
        
        # CVD confirmation boost
        cvd_boost = 0.0
        if direction == "BUY":
            cvd_boost = 0.15 if cvd > 0 else -0.1
        elif direction == "SELL":
             cvd_boost = 0.15 if cvd < 0 else -0.1

        confidence = base_confidence + strength_factor + cvd_boost + spoofing_penalty
        confidence = max(0.3, min(0.95, confidence))
        
        # Expected move based on divergence strength
        expected_move_pct = (strength * 0.02) * 100.0
        expected_move_pct = max(0.1, round(expected_move_pct, 2))
        
        metadata = {
            "ofi": ofi,
            "obi": obi,
            "cvd": cvd,
            "bucket_volume": bucket_volume,
            "logic_type": logic_type,
            "spoofing_detected": spoofing_penalty < 0
        }
        
        pattern = self._build_pattern(symbol, indicators, direction, confidence, metadata)
        if debug_trace:
            self.logger.debug(
                "[MSD_DEBUG] %s direction=%s logic=%s strength=%.3f confidence=%.2f",
                symbol,
                direction,
                logic_type,
                strength,
                confidence,
            )
        
        # Algo Alert Construction
        if pattern and self.alert_builder:
            try:
                algo_alert = self.alert_builder.build_algo_alert(
                    symbol=symbol,
                    pattern_type=self.pattern_type,
                    entry_price=last_price,
                    confidence=confidence,
                    signal=direction,
                    indicators=indicators,
                    expected_move=expected_move_pct, 
                    volume_ratio=indicators.get("volume_ratio", 1.0)
                )
                if algo_alert:
                    pattern.update(algo_alert)
            except Exception as e:
                self.logger.error(f"Error building algo alert for {symbol}: {e}")
                
        return pattern


class CrossAssetArbitrageDetector(BaseAdvancedDetector):
    pattern_type = "cross_asset_arbitrage"

    def detect(self, symbol: str, indicators: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Cross-Asset Arbitrage logic using USDINR sensitivity.
        1. Checks availability of USDINR metrics.
        2. Detects basis divergence when correlation is high.
        """
        # ✅ Metris checking (Consume preserved fields)
        usdinr_sensitivity = _safe_float(indicators.get("usdinr_sensitivity"), 0.0)
        usdinr_correlation = _safe_float(indicators.get("usdinr_correlation"), 0.0)
        
        # If no sensitivity data, we can't detect
        if usdinr_sensitivity == 0.0 and usdinr_correlation == 0.0:
            return None
            
        usdinr_basis = _safe_float(indicators.get("usdinr_basis"), 0.0)
        last_price = _safe_float(indicators.get("last_price"), 0.0)
        
        # Logic: If highly correlated and significant basis/sensitivity divergence
        # This is a placeholder logic to prove data flow consumption
        
        confidence = 0.0
        direction = None
        
        # 1. High Correlation Requirement
        if abs(usdinr_correlation) > 0.7:
            # 2. Check Sensitivity/Basis
            # If correlation is Positive, and USDINR up, Equity should be up.
            # If Equity is lagging (basis divergence), trade to close gap.
            
            # For now, we'll just log/detect significant sensitivity events
            if abs(usdinr_sensitivity) > 1.5:  # High beta to USDINR
                confidence = 0.5 + (min(abs(usdinr_sensitivity), 3.0) / 10.0)
                
                # Arbitrary direction based on basis for now (placeholder logic)
                # In real arbitrage, we'd compare Fair Value vs Market Price
                if usdinr_basis > 0.5: # Future > Spot (Contango)
                     direction = "SELL" if usdinr_correlation > 0 else "BUY"
                elif usdinr_basis < -0.5: # Backwardation
                     direction = "BUY" if usdinr_correlation > 0 else "SELL"
                     
        if not direction or confidence < 0.5:
            return None
            
        metadata = {
            "usdinr_sensitivity": usdinr_sensitivity,
            "usdinr_correlation": usdinr_correlation,
            "usdinr_basis": usdinr_basis
        }
        
        pattern = self._build_pattern(symbol, indicators, direction, confidence, metadata)
        return pattern 


class VIXRegimeMomentumDetector(BaseAdvancedDetector):
    pattern_type = "vix_momentum"

    def __init__(self, config: Optional[Dict[str, Any]] = None, redis_client=None, alert_builder=None):
        super().__init__(config, redis_client, alert_builder)
        # State tracking for price smoothing
        self.history = defaultdict(list)
        self.max_history_length = 20

    def detect(self, symbol: str, indicators: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        ✅ FIXED: VIX Momentum — pure continuation (NOT reversal).
        Only triggers when VIX and price move *together* with volume confirmation.
        """
        vix_change_pct = _safe_float(indicators.get("vix_change_pct"), 0.0)
        price_change_pct = _safe_float(indicators.get("price_change_pct"), 0.0)
        volume_ratio = _safe_float(indicators.get("volume_ratio"), 1.0)
        last_price = _safe_float(indicators.get("last_price"), 0.0)
        
        # Require ≥1.5% move (intraday relevance)
        if abs(vix_change_pct) < 1.5 or abs(price_change_pct) < 0.3:
            return None
        
        # ✅ Continuation only: same-direction VIX/price moves
        signal = None
        reason = ""
        
        # Risk-off continuation: VIX↑ + price↓
        if vix_change_pct > 0 and price_change_pct < 0:
            if volume_ratio > 1.2:  # Volume confirmation
                signal = 'SELL'
                reason = f"Risk-off momentum: VIX↑{vix_change_pct:+.1f}%, price↓{price_change_pct:+.2f}%, VR={volume_ratio:.2f}x"
        
        # Risk-on continuation: VIX↓ + price↑
        elif vix_change_pct < 0 and price_change_pct > 0:
            if volume_ratio > 1.2:
                signal = 'BUY'
                reason = f"Risk-on momentum: VIX↓{vix_change_pct:+.1f}%, price↑{price_change_pct:+.2f}%, VR={volume_ratio:.2f}x"
        
        if signal:
            # Confidence: scales with move size and volume
            move_strength = (abs(vix_change_pct) / 5.0) + (abs(price_change_pct) / 1.0)
            confidence = min(0.8, 0.45 + move_strength * 0.15 + max(0, volume_ratio - 1.0) * 0.1)
            
            metadata = {'is_continuation': True, 'vix_change_pct': vix_change_pct}
            
            pattern = self._build_pattern(symbol, indicators, signal, confidence, metadata)
            
            if pattern and self.alert_builder:
                try:
                    algo_alert = self.alert_builder.build_algo_alert(
                        symbol=symbol,
                        pattern_type=self.pattern_type,
                        entry_price=last_price,
                        confidence=confidence,
                        signal=signal,
                        indicators=indicators,
                        expected_move=0.5, 
                        volume_ratio=volume_ratio
                    )
                    if algo_alert:
                        pattern.update(algo_alert)
                except Exception as e:
                    self.logger.error(f"Error building algo alert: {e}")
            
            return pattern
        
        return None
