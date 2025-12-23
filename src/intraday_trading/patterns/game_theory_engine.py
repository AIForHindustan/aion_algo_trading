"""
Intraday Game Theory Engine
===========================

Phase-1 implementation that fuses three microstructure/game-theory components:

- NashMarketMakerGame: predicts market-maker behaviour from order-book imbalance
- FOMOExploitationEngine: detects momentum rushes caused by retail FOMO
- PrisonersDilemmaOrderFlow: flags spoofing/defection in order flow

The combined signal is used by the scanner to confirm high-confidence entries.
"""

from __future__ import annotations

import logging
import statistics
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards

logger = logging.getLogger(__name__)

# ✅ VIX-based Threshold Manager
try:
    from shared_core.config_utils.thresholds import get_thresholds_for_regime
    VIX_THRESHOLDS_AVAILABLE = True
except ImportError:
    VIX_THRESHOLDS_AVAILABLE = False
    get_thresholds_for_regime = None
    logger.debug("⚠️ VIX-based thresholds not available, using hardcoded thresholds")
# ✅ FIXED: Use DatabaseAwareKeyBuilder directly instead of get_unified_key_builder alias
KEY_BUILDER = DatabaseAwareKeyBuilder

INTRADAY_PRIORITY: List[str] = [
    "NashMarketMakerGame",
    "FOMOExploitationEngine",
    "PrisonersDilemmaOrderFlow",
]

VALIDATION_SYMBOLS: Dict[str, List[str]] = {
    "NFO": ["NIFTY25NOV26000CE", "BANKNIFTY25NOV58000PE"],
    "CDS": ["USDINR25NOV84.50CE", "EURINR25NOV92.00PE"],
    "MCX": ["GOLD25DEC88000CE", "CRUDEOIL25NOV6500CE"],
}


def _normalize_symbol(symbol: str) -> str:
    if not symbol:
        return ""
    try:
        return RedisKeyStandards.canonical_symbol(symbol)
    except Exception:
        return symbol


def _parse_levels(order_book: Optional[Dict[str, Any]], side: str) -> List[Tuple[float, float]]:
    if not isinstance(order_book, dict):
        return []
    levels = order_book.get(side, [])
    parsed: List[Tuple[float, float]] = []
    if isinstance(levels, list):
        for entry in levels:
            if not isinstance(entry, dict):
                continue
            price = entry.get("price") or entry.get("last_price")
            qty = entry.get("quantity") or entry.get("qty") or entry.get("size")
            if price is None or qty is None:
                continue
            try:
                parsed.append((float(price), float(qty)))
            except (TypeError, ValueError):
                continue
    return parsed


def _extract_order_book_metrics(order_book: Optional[Dict[str, Any]]) -> Dict[str, float]:
    bids = _parse_levels(order_book, "buy")
    asks = _parse_levels(order_book, "sell")

    best_bid = bids[0][0] if bids else 0.0
    best_ask = asks[0][0] if asks else 0.0
    bid_qty = sum(q for _, q in bids)
    ask_qty = sum(q for _, q in asks)
    total_qty = bid_qty + ask_qty
    imbalance = (bid_qty - ask_qty) / total_qty if total_qty > 0 else 0.0
    spread = max(0.0, best_ask - best_bid) if best_bid and best_ask else 0.0
    mid = best_bid + spread / 2 if best_bid and best_ask else best_bid or best_ask
    spread_bps = (spread / mid) * 10_000 if mid else 0.0
    depth_ratio = (
        min(bid_qty, ask_qty) / max(bid_qty, ask_qty) if bid_qty and ask_qty else 0.0
    )
    top_bid_share = bids[0][1] / bid_qty if bids and bid_qty else 0.0
    top_ask_share = asks[0][1] / ask_qty if asks and ask_qty else 0.0
    spoof_ratio = max(top_bid_share, top_ask_share)

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": mid or 0.0,
        "spread": spread,
        "spread_bps": spread_bps,
        "bid_volume": bid_qty,
        "ask_volume": ask_qty,
        "imbalance": imbalance,
        "depth_ratio": depth_ratio,
        "top_heavy_ratio": spoof_ratio,
    }


class NashMarketMakerGame:
    """Simplified Nash equilibrium approximation for order-book driven markets."""

    def __init__(self, imbalance_threshold: float = 0.12):
        self.imbalance_threshold = imbalance_threshold

    async def calculate_nash_equilibrium(
        self,
        symbol: str,
        order_book: Optional[Dict[str, Any]],
        regime: str = "intraday",
    ) -> Dict[str, Any]:
        metrics = _extract_order_book_metrics(order_book)
        imbalance = metrics["imbalance"]
        spread_penalty = max(0.1, 1 - min(1.0, metrics["spread_bps"] / 40.0))
        depth_bonus = metrics["depth_ratio"]
        raw_conf = min(1.0, abs(imbalance) * 1.5 * spread_penalty + depth_bonus * 0.2)

        if imbalance > self.imbalance_threshold:
            predicted = "LONG"
        elif imbalance < -self.imbalance_threshold:
            predicted = "SHORT"
        else:
            predicted = "FLAT"
            raw_conf = min(raw_conf, 0.35)

        return {
            "symbol": symbol,
            "regime": regime,
            "predicted_moves": predicted,
            "confidence": raw_conf,
            "equilibrium_price": metrics["mid_price"],
            "spread_bps": metrics["spread_bps"],
            "depth_ratio": metrics["depth_ratio"],
            "imbalance": imbalance,
        }


class FOMOExploitationEngine:
    """Detects FOMO cycles using recent price/volume momentum."""

    def __init__(self, window: int = 32):
        self.window = window

    def detect_fomo_cycles(
        self,
        symbol: str,
        ticks: Sequence[Dict[str, Any]],
        social_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        window_ticks = list(ticks)[-self.window :]
        prices = [self._safe_float(t.get("last_price")) for t in window_ticks if t]
        volumes = [self._safe_float(t.get("volume") or t.get("last_traded_quantity")) for t in window_ticks if t]

        if len(prices) < 2:
            return {
                "symbol": symbol,
                "momentum_score": 0.0,
                "volume_ratio": 0.0,
                "fomo_phase": "BALANCED",
                "confidence": 0.2,
            }

        price_change = (prices[-1] - prices[0]) / prices[0] if prices[0] else 0.0
        avg_volume = statistics.fmean(volumes) if volumes else 0.0
        last_volume = volumes[-1] if volumes else 0.0
        volume_ratio = (last_volume / avg_volume) if avg_volume else 0.0

        sentiment_score = (social_data or {}).get("sentiment_score", 0.0)
        mentions = (social_data or {}).get("mentions", 0)

        momentum_score = (
            0.55 * price_change + 0.3 * min(volume_ratio - 1, 2.0) + 0.15 * sentiment_score
        )

        if momentum_score > 0.25:
            phase = "FOMO_BUY"
        elif momentum_score < -0.25:
            phase = "FOMO_SELL"
        else:
            phase = "BALANCED"

        confidence = min(1.0, abs(momentum_score) * 1.8 + min(volume_ratio, 3.0) * 0.1)

        return {
            "symbol": symbol,
            "momentum_score": momentum_score,
            "volume_ratio": volume_ratio,
            "fomo_phase": phase,
            "sentiment_score": sentiment_score,
            "mentions": mentions,
            "confidence": confidence,
        }

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0


class PrisonersDilemmaOrderFlow:
    """Flags defection/spoofing risk from order-book micro-structure."""

    def __init__(self, spoof_threshold: float = 0.45):
        self.spoof_threshold = spoof_threshold

    def analyze_order_flow_game(
        self, symbol: str, order_book: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        metrics = _extract_order_book_metrics(order_book)
        defection_signals: List[str] = []
        spoof_ratio = metrics["top_heavy_ratio"]

        if spoof_ratio > self.spoof_threshold:
            defection_signals.append("top_layer_spoofing")

        if metrics["spread_bps"] > 60:
            defection_signals.append("widening_spread")

        if abs(metrics["imbalance"]) > 0.35 and metrics["depth_ratio"] < 0.4:
            defection_signals.append("asymmetric_depth")

        cooperation_prob = max(
            0.0,
            1.0 - (0.6 * spoof_ratio + 0.3 * max(0.0, metrics["spread_bps"] - 20) / 80.0),
        )

        confidence = 1.0 - cooperation_prob

        return {
            "symbol": symbol,
            "defection_signals": defection_signals,
            "cooperation_prob": cooperation_prob,
            "confidence": confidence,
            "spoof_ratio": spoof_ratio,
        }


@dataclass
class GameTheoryComposite:
    direction: str
    confidence: float
    reference_price: Optional[float]
    components: Dict[str, Any]


class IntradayGameTheoryEngine:
    """Coordinator that fuses multiple game-theory signals."""

    def __init__(
        self,
        priority_components: Optional[Sequence[str]] = None,
        validation_symbols: Optional[Dict[str, List[str]]] = None,
        min_confidence: Optional[float] = None,
        redis_client=None,
    ):
        self.priority_components = list(priority_components or INTRADAY_PRIORITY)
        self.validation_symbols = validation_symbols or VALIDATION_SYMBOLS
        self.redis_client = redis_client
        
        # ✅ VIX-based Threshold Adjustment
        # Get min_confidence from pattern config or use default
        if min_confidence is None:
            try:
                from config.instrument_config import PATTERN_CONFIGS
                pattern_config = PATTERN_CONFIGS.get("game_theory_signal", {})
                min_confidence = pattern_config.get("min_confidence", 0.7)
            except ImportError:
                min_confidence = 0.7
        
        # Apply VIX-based threshold adjustment if available
        if VIX_THRESHOLDS_AVAILABLE and get_thresholds_for_regime:
            try:
                vix_thresholds = get_thresholds_for_regime(redis_client=redis_client)
                confidence_multiplier = vix_thresholds.get('confidence_multiplier', 1.0)
                min_confidence = min_confidence * confidence_multiplier
            except Exception as e:
                logger.debug(f"⚠️ Could not apply VIX thresholds: {e}")
        
        self.min_confidence = min_confidence
        
        # Initialize components with VIX-adjusted thresholds
        # Base thresholds - will be adjusted per-symbol during detection if needed
        base_nash_threshold = 0.12
        base_prisoners_threshold = 0.45
        
        # Apply VIX-based threshold adjustment if available
        if VIX_THRESHOLDS_AVAILABLE and get_thresholds_for_regime:
            try:
                vix_thresholds = get_thresholds_for_regime(redis_client=redis_client)
                move_multiplier = vix_thresholds.get('move_multiplier', 1.0)
                # Adjust thresholds based on VIX regime (higher VIX = more conservative)
                nash_imbalance_threshold = base_nash_threshold * move_multiplier
                prisoners_spoof_threshold = base_prisoners_threshold * move_multiplier
            except Exception as e:
                logger.debug(f"⚠️ Could not apply VIX thresholds: {e}")
                nash_imbalance_threshold = base_nash_threshold
                prisoners_spoof_threshold = base_prisoners_threshold
        else:
            nash_imbalance_threshold = base_nash_threshold
            prisoners_spoof_threshold = base_prisoners_threshold
        
        self.nash_mm = NashMarketMakerGame(imbalance_threshold=nash_imbalance_threshold)
        self.fomo_detector = FOMOExploitationEngine()
        self.prisoners_dilemma = PrisonersDilemmaOrderFlow(spoof_threshold=prisoners_spoof_threshold)

    async def generate_intraday_signals(
        self,
        symbol: str,
        order_book: Optional[Dict[str, Any]],
        ticks: Sequence[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if order_book is None:
            return None

        social_data = self._get_social_data(symbol)

        nash_signal = await self.nash_mm.calculate_nash_equilibrium(
            symbol, order_book, "intraday"
        )
        fomo_signal = self.fomo_detector.detect_fomo_cycles(symbol, ticks, social_data)
        pd_signal = self.prisoners_dilemma.analyze_order_flow_game(symbol, order_book)

        trading_signal = self._fuse_intraday_signals(
            nash_signal, fomo_signal, pd_signal
        )

        if not trading_signal:
            return None

        # Convert direction to BUY/SELL
        signal_direction = "BUY" if trading_signal.direction == "LONG" else "SELL"
        if trading_signal.direction == "FLAT":
            return None  # Don't return FLAT signals

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "pattern_type": "game_theory_signal",
            "pattern": "game_theory_signal",
            "signal": signal_direction,
            "action": signal_direction,
            "confidence": trading_signal.confidence,
            "last_price": trading_signal.reference_price,
            "position_size": trading_signal.confidence,  # Will be converted to lots later
            "stop_loss": self._calculate_gt_stop_loss(trading_signal),
            "target": self._calculate_gt_take_profit(trading_signal),  # Use 'target' not 'take_profit'
            "timeframe": "1-5min",
            "volume_ratio": fomo_signal.get("volume_ratio", 1.0),
            "alert_id": str(uuid.uuid4()),
            "timestamp_ms": int(datetime.utcnow().timestamp() * 1000),
            "metadata": {
                "components": trading_signal.components
            }
        }

    def _fuse_intraday_signals(
        self,
        nash_signal: Dict[str, Any],
        fomo_signal: Dict[str, Any],
        pd_signal: Dict[str, Any],
    ) -> Optional[GameTheoryComposite]:
        votes = {"LONG": 0.0, "SHORT": 0.0}

        nash_dir = nash_signal.get("predicted_moves", "FLAT")
        if nash_dir in votes:
            votes[nash_dir] += nash_signal.get("confidence", 0.0)

        fomo_phase = fomo_signal.get("fomo_phase")
        if fomo_phase == "FOMO_BUY":
            votes["LONG"] += fomo_signal.get("confidence", 0.0) * 0.8
        elif fomo_phase == "FOMO_SELL":
            votes["SHORT"] += fomo_signal.get("confidence", 0.0) * 0.8

        if pd_signal.get("defection_signals"):
            votes["SHORT"] += pd_signal.get("confidence", 0.0) * 0.6
        else:
            votes["LONG"] += pd_signal.get("cooperation_prob", 0.0) * 0.3

        if votes["LONG"] == votes["SHORT"]:
            direction = "FLAT"
        else:
            direction = "LONG" if votes["LONG"] > votes["SHORT"] else "SHORT"

        aggregate_conf = min(
            1.0,
            (votes["LONG"] + votes["SHORT"]) / max(len(self.priority_components), 1),
        )

        reference_price = nash_signal.get("equilibrium_price")
        components = {
            "nash": nash_signal,
            "fomo": fomo_signal,
            "prisoners_dilemma": pd_signal,
        }

        return GameTheoryComposite(
            direction=direction,
            confidence=aggregate_conf,
            reference_price=reference_price,
            components=components,
        )

    def _calculate_gt_position_size(self, confidence: float) -> float:
        if confidence >= 0.85:
            return 1.0  # full size (normalized)
        if confidence >= 0.7:
            return 0.65
        if confidence >= 0.55:
            return 0.4
        return 0.0

    def _calculate_gt_stop_loss(self, composite: GameTheoryComposite) -> Optional[float]:
        ref = composite.reference_price
        if not ref:
            return None
        buffer_bps = 25 if composite.confidence >= 0.8 else 40
        if composite.direction == "LONG":
            return ref * (1 - buffer_bps / 10_000)
        if composite.direction == "SHORT":
            return ref * (1 + buffer_bps / 10_000)
        return None

    def _calculate_gt_take_profit(self, composite: GameTheoryComposite) -> Optional[float]:
        ref = composite.reference_price
        if not ref:
            return None
        reward_bps = 45 if composite.confidence >= 0.8 else 65
        if composite.direction == "LONG":
            return ref * (1 + reward_bps / 10_000)
        if composite.direction == "SHORT":
            return ref * (1 - reward_bps / 10_000)
        return None

    def _get_social_data(self, symbol: str) -> Dict[str, Any]:
        normalized = _normalize_symbol(symbol)
        for exchange, symbols in self.validation_symbols.items():
            if normalized in {s.upper() for s in symbols}:
                return {
                    "sentiment_score": 0.35,
                    "mentions": 50,
                    "watchlist_tag": exchange,
                }
        return {"sentiment_score": 0.0, "mentions": 0}

    def is_validation_symbol(self, symbol: str) -> bool:
        normalized = _normalize_symbol(symbol)
        return any(
            normalized in {s.upper() for s in symbols}
            for symbols in self.validation_symbols.values()
        )


__all__ = [
    "IntradayGameTheoryEngine",
    "NashMarketMakerGame",
    "FOMOExploitationEngine",
    "PrisonersDilemmaOrderFlow",
    "INTRADAY_PRIORITY",
    "VALIDATION_SYMBOLS",
]
