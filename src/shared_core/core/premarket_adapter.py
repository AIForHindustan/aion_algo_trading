"""Adapter to construct standardized premarket_manipulation dicts

Tick processor returns a list of manipulated stocks; this adapter ensures the
detector receives consistent keys.
"""

from typing import Any, Dict, Optional


def build_premarket_indicator(stock_data: Dict[str, Any]) -> Dict[str, Any]:
    """Return a standardized premarket_manipulation dict for a single stock_data entry."""
    # Build a normalized dictionary for premarket manipulation signals.
    #
    # The tick processor returns entries where the historical 20‑day average
    # volume may be named `avg_volume` or `avg_volume_20d`.  Downstream
    # components expect this value as `avg_premarket_volume`, so we choose
    # whichever key exists.  If neither is present we leave it unset.
    avg_volume = None
    if "avg_premarket_volume" in stock_data:
        avg_volume = stock_data.get("avg_premarket_volume")
    elif "avg_volume" in stock_data:
        avg_volume = stock_data.get("avg_volume")
    elif "avg_volume_20d" in stock_data:
        avg_volume = stock_data.get("avg_volume_20d")

    # Handle new baseline/comparison structure
    price_0907 = stock_data.get("price_0907")
    price_0915 = stock_data.get("price_0915")
    volume_0907 = stock_data.get("volume_0907")
    volume_0915 = stock_data.get("volume_0915")

    # If we have baseline/comparison data, use it
    if price_0907 is not None and price_0915 is not None:
        # Calculate price change and volume change
        price_change_pct = (
            ((price_0915 - price_0907) / price_0907) * 100 if price_0907 > 0 else 0
        )
        volume_change_pct = (
            ((volume_0915 - volume_0907) / volume_0907) * 100
            if volume_0907 is not None and volume_0907 > 0
            else 0
        )

        # Determine direction
        direction = (
            "UP" if price_change_pct > 0 else "DOWN" if price_change_pct < 0 else "FLAT"
        )

        return {
            "symbol": stock_data.get("symbol"),
            "price_0907": price_0907,
            "price_0915": price_0915,
            "volume_0907": volume_0907,
            "volume_0915": volume_0915,
            "avg_premarket_volume": avg_volume,
            # Propagate calculated values
            "volume_ratio": stock_data.get("volume_ratio"),
            "price_change_pct": price_change_pct,
            "volume_change_pct": volume_change_pct,
            "direction": direction,
            # Add expected_move for AlertManager compatibility
            "expected_move": price_change_pct,
            # Default confidence for premarket manipulation when not provided
            "confidence": stock_data.get("confidence", 0.8),
        }

    # Fallback to old structure for backward compatibility
    return {
        "symbol": stock_data.get("symbol"),
        "price_0907": stock_data.get("price_0907"),
        "price_0915": stock_data.get("price_0915"),
        "volume_0907": stock_data.get("volume_0907"),
        "avg_premarket_volume": avg_volume,
        # Propagate volume_ratio and price_change_pct from stock_data if present
        "volume_ratio": stock_data.get("volume_ratio"),
        "price_change_pct": stock_data.get("price_change_pct"),
        "direction": stock_data.get("direction"),
        # Add expected_move for AlertManager compatibility
        "expected_move": stock_data.get("price_change_pct", 0),
        # Default confidence for premarket manipulation when not provided
        "confidence": stock_data.get("confidence", 0.8),
    }


def build_enhanced_premarket_indicator(
    stock_data: Dict[str, Any],
    order_flow_analysis: Optional[Dict[str, Any]] = None,
    base_indicator: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return an enhanced premarket indicator (order flow analysis removed)."""

    # Reuse provided base indicator or derive from stock data
    indicator = (base_indicator or {}).copy() or build_premarket_indicator(stock_data)

    # Default fallbacks before enhancement
    enhanced = indicator.copy()
    enhanced.setdefault("direction", "FLAT")

    # Order flow analysis removed - simplified logic
    enhanced.update(
        {
            "true_direction": enhanced.get("direction", "FLAT"),
            "manipulation_detected": False,
            "manipulation_score": 0.0,
            "order_flow_quality": "MISSING",
            "should_alert": enhanced.get("confidence", 0.0) > 0.7,
        }
    )

    return enhanced
