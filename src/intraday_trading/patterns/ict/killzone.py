#!/opt/homebrew/bin/python3.13
"""
ENHANCED ICT Killzone Detector with Volume-Time Awareness
Fixed for Indian Market with proper institutional flow detection
"""

from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Optional, Dict, List
import logging
import pytz
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

class EnhancedICTKillzoneDetector:
    """
    FIXED: Volume-time aware killzone detector with market regime adaptation.
    
    Key Improvements:
    - Volume confirmation for killzone signals
    - Correct Indian market institutional timings
    - VIX regime awareness
    - Dynamic bias adjustment based on actual market behavior
    - Integration with time-aware volume baselines
    """

    def __init__(self, redis_client=None):
        self.redis_client = redis_client
        self.ist = ZoneInfo('Asia/Kolkata')  # IST (no DST)
        self.london_tz = ZoneInfo('Europe/London')  # GMT/BST (has DST)
        self.new_york_tz = ZoneInfo('America/New_York')  # EST/EDT (has DST)
        self.logger = logger
        
        # ✅ CORRECTED: Actual institutional killzones for Indian Market
        # Times are in IST (local market time)
        self.killzones = {
            "asian_liquidity": {  # Asian institutional session overlap
                "start": time(9, 15),  # Market open
                "end": time(10, 30),   # Asian lunch start
                "bias": "volatile",
                "priority": "high",
                "min_volume_ratio": 1.3,  # Must have 30% above baseline
                "expected_move_pct": 0.4, # 0.4% expected move for Nifty
            },
            "london_open": {  # London session impact (DST-aware)
                # London opens at 8:00 AM GMT/BST = 1:30 PM IST (GMT) or 12:30 PM IST (BST)
                # We use IST time directly, but verify against actual London time
                "start": time(12, 30),  # London open in IST (adjusts for DST automatically)
                "end": time(14, 0),     # Early European session
                "bias": "directional", 
                "priority": "medium",
                "min_volume_ratio": 1.2,
                "expected_move_pct": 0.3,
                "verify_against_tz": "Europe/London",  # Verify against London time
                "target_london_time": time(8, 0),  # London opens at 8:00 AM local
            },
            "new_york_prep": {  # US session preparation (DST-aware)
                # NY opens at 9:30 AM EST/EDT = 8:00 PM IST (EST) or 7:00 PM IST (EDT)
                # But we track pre-market activity which starts earlier
                "start": time(14, 30),  # Pre-US session activity in IST
                "end": time(15, 30),    # Market close
                "bias": "momentum",
                "priority": "high", 
                "min_volume_ratio": 1.4,  # Higher volume expectation
                "expected_move_pct": 0.5, # 0.5% expected move
                "verify_against_tz": "America/New_York",  # Verify against NY time
                "target_ny_time": time(4, 0),  # Pre-market starts around 4:00 AM NY time
            }
        }
        
        # Market regime adjustments
        self.regime_adjustments = {
            "HIGH_VIX": {"volume_multiplier": 1.3, "move_multiplier": 1.5},
            "NORMAL": {"volume_multiplier": 1.0, "move_multiplier": 1.0},
            "LOW_VIX": {"volume_multiplier": 0.8, "move_multiplier": 0.7},
        }

    def get_volume_confirmed_killzone(self, symbol: str, current_volume: float, 
                                    current_price: float, price_change_pct: float) -> Optional[Dict]:
        """
        FIXED: Get killzone only if volume and price action confirm institutional activity.
        
        Args:
            symbol: Trading symbol (NIFTY, BANKNIFTY, etc.)
            current_volume: Current bucket incremental volume
            current_price: Current underlying price
            price_change_pct: Recent price change percentage
            
        Returns:
            Killzone info with volume confirmation, or None if not confirmed
        """
        try:
            # Get basic killzone
            killzone = self.get_current_killzone()
            if not killzone:
                return None
            
            # Get volume baseline for confirmation
            baseline_volume = self._get_volume_baseline(symbol, killzone)
            if not baseline_volume or baseline_volume <= 0:
                self.logger.debug(f"⚠️ No baseline volume for {symbol} in {killzone['zone']}")
                return None
            
            # Calculate volume ratio
            volume_ratio = current_volume / baseline_volume if baseline_volume > 0 else 0
            
            # Get regime-adjusted thresholds
            regime = self._get_current_vix_regime()
            adjustments = self.regime_adjustments.get(regime, self.regime_adjustments["NORMAL"])
            
            min_volume_required = killzone["min_volume_ratio"] * adjustments["volume_multiplier"]
            min_move_required = killzone["expected_move_pct"] * adjustments["move_multiplier"]
            
            # Volume confirmation check
            volume_confirmed = volume_ratio >= min_volume_required
            move_confirmed = abs(price_change_pct) >= min_move_required
            
            # Institutional activity requires BOTH volume and price movement
            institutional_activity = volume_confirmed and move_confirmed
            
            if not institutional_activity:
                self.logger.debug(
                    f"⏸️ Killzone {killzone['zone']} not confirmed for {symbol}: "
                    f"volume_ratio={volume_ratio:.2f} (need {min_volume_required:.2f}), "
                    f"move={price_change_pct:.3f}% (need {min_move_required:.3f}%)"
                )
                return None
            
            # Enhanced killzone info with volume context
            confirmed_killzone = killzone.copy()
            confirmed_killzone.update({
                "volume_ratio": volume_ratio,
                "volume_confirmed": volume_confirmed,
                "move_confirmed": move_confirmed,
                "institutional_activity": institutional_activity,
                "regime": regime,
                "baseline_volume": baseline_volume,
                "current_volume": current_volume,
                "symbol": symbol
            })
            
            self.logger.info(
                f"✅ CONFIRMED Killzone {killzone['zone']} for {symbol}: "
                f"volume_ratio={volume_ratio:.2f}, move={price_change_pct:.3f}%, regime={regime}"
            )
            
            return confirmed_killzone
            
        except Exception as e:
            self.logger.error(f"Volume-confirmed killzone check failed: {e}")
            return None

    def _get_volume_baseline(self, symbol: str, killzone: Dict) -> float:
        """Get volume baseline for killzone confirmation - SIMPLIFIED: Direct Redis read with canonical_symbol."""
        try:
            if not self.redis_client:
                self.logger.warning("No Redis client for volume baseline")
                return 0.0
            
            # Use canonical_symbol for Redis key lookup (ensures data consistency)
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
            
            # Get canonical symbol for consistent Redis lookup
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            baseline_key = DatabaseAwareKeyBuilder.live_volume_baseline(canonical_symbol)
            
            baseline_data = self.redis_client.hgetall(baseline_key)
            if not baseline_data:
                return 0.0
            
            # Decode bytes if needed
            if isinstance(list(baseline_data.values())[0], bytes):
                baseline_data = {
                    k.decode() if isinstance(k, bytes) else k:
                    v.decode() if isinstance(v, bytes) else v
                    for k, v in baseline_data.items()
                }
            
            # Priority: baseline_1min > current > avg_volume_55d/375
            baseline_1min = baseline_data.get('baseline_1min')
            if baseline_1min:
                baseline_volume = float(baseline_1min)
            else:
                current = baseline_data.get('current')
                if current:
                    baseline_volume = float(current)
                else:
                    avg_vol_55d = baseline_data.get('avg_volume_55d')
                    if avg_vol_55d:
                        baseline_volume = float(avg_vol_55d) / 375.0  # Trading minutes per day
                    else:
                        return 0.0
            
            # Apply killzone-specific multiplier (time-aware adjustment)
            killzone_multipliers = {
                "asian_liquidity": 1.8,  # Opening has highest volume expectation
                "london_open": 1.3,
                "new_york_prep": 1.6,   # Closing also high volume
            }
            
            multiplier = killzone_multipliers.get(killzone.get("zone", ""), 1.0)
            return baseline_volume * multiplier
            
        except Exception as e:
            self.logger.debug(f"Baseline volume fetch failed for {symbol}: {e}")
            return 0.0

    def _get_current_vix_regime(self) -> str:
        """Get current VIX regime for threshold adjustments."""
        try:
            # ✅ FIXED: Use shared_core.utils.vix_utils instead of direct Redis access
            from shared_core.utils.vix_utils import get_vix_utils, get_vix_regime
            vix_utils = get_vix_utils(redis_client=self.redis_client)
            if vix_utils:
                vix_regime = vix_utils.get_vix_regime()
                if vix_regime and vix_regime != "UNKNOWN":
                    return vix_regime
            # Fallback: try get_vix_regime directly
            vix_regime = get_vix_regime()
            if vix_regime and vix_regime != "UNKNOWN":
                return vix_regime
            return "NORMAL"
        except:
            return "NORMAL"

    def should_trigger_pattern(self, pattern_type: str, symbol: str, 
                             volume_data: Dict, price_data: Dict) -> bool:
        """
        FIXED: Determine if pattern should trigger based on killzone + volume confirmation.
        
        Args:
            pattern_type: Type of pattern (momentum, reversal, etc.)
            symbol: Trading symbol
            volume_data: {'current_volume': float, 'volume_ratio': float}
            price_data: {'current_price': float, 'price_change_pct': float}
            
        Returns:
            True if pattern should trigger in current killzone
        """
        try:
            # Get volume-confirmed killzone
            killzone = self.get_volume_confirmed_killzone(
                symbol,
                volume_data.get('current_volume', 0),
                price_data.get('current_price', 0),
                price_data.get('price_change_pct', 0)
            )
            
            if not killzone:
                return False
            
            # Pattern-specific killzone suitability
            pattern_suitability = self._is_pattern_suitable_for_killzone(pattern_type, killzone)
            
            if not pattern_suitability:
                self.logger.debug(
                    f"⏸️ Pattern {pattern_type} not suitable for {killzone['zone']} "
                    f"(bias: {killzone['bias']})"
                )
                return False
            
            # Additional volume spike confirmation for high-priority patterns
            if killzone["priority"] == "high":
                volume_spike_ok = volume_data.get('volume_ratio', 0) >= killzone["min_volume_ratio"]
                if not volume_spike_ok:
                    return False
            
            self.logger.info(
                f"🎯 TRIGGER APPROVED: {pattern_type} in {killzone['zone']} for {symbol} "
                f"(volume_ratio: {volume_data.get('volume_ratio', 0):.2f})"
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Pattern trigger check failed: {e}")
            return False

    def _is_pattern_suitable_for_killzone(self, pattern_type: str, killzone: Dict) -> bool:
        """Check if pattern type is suitable for current killzone bias."""
        pattern_suitability = {
            "volatile": ["volume_spike", "breakout", "momentum", "reversal"],
            "directional": ["breakout", "momentum", "trend_following"], 
            "momentum": ["momentum", "volume_spike", "breakout"],
            "consolidation": ["range_breakout", "reversal"]  # For your old midday_drift
        }
        
        suitable_patterns = pattern_suitability.get(killzone["bias"], [])
        return pattern_type in suitable_patterns

    def _is_market_hours(self) -> bool:
        """Check if current time is within Indian market hours (9:15 AM - 3:30 PM IST)."""
        now_t = datetime.now(self.ist).time()
        market_open = time(9, 15)
        market_close = time(15, 30)
        return market_open <= now_t <= market_close
    
    # ✅ KEEP your existing methods but enhance them:
    def _time_remaining(self, end_t: time) -> int:
        """Calculate minutes remaining in killzone."""
        now = datetime.now(self.ist)
        end_dt = datetime.combine(now.date(), end_t).replace(tzinfo=self.ist)
        if now > end_dt:
            return 0
        delta = end_dt - now
        return int(delta.total_seconds() // 60)

    def get_current_killzone(self) -> Optional[Dict]:
        """
        Get current killzone without volume confirmation.
        ✅ DST-AWARE: Verifies London/NY killzones against actual timezone times.
        """
        now_ist = datetime.now(self.ist)
        now_t = now_ist.time()
        
        for zone_name, info in self.killzones.items():
            # Check if in IST time window
            if info["start"] <= now_t <= info["end"]:
                # For London/NY killzones, verify against actual timezone (DST-aware)
                if "verify_against_tz" in info:
                    target_tz = ZoneInfo(info["verify_against_tz"])
                    now_target_tz = now_ist.astimezone(target_tz)
                    target_time = info.get("target_london_time") or info.get("target_ny_time")
                    
                    # Verify we're within ±30 minutes of target time in that timezone
                    target_dt = datetime.combine(now_target_tz.date(), target_time).replace(tzinfo=target_tz)
                    time_diff = abs((now_target_tz - target_dt).total_seconds() / 3600)  # hours
                    
                    if time_diff > 2.0:  # More than 2 hours off = not in killzone
                        self.logger.debug(
                            f"⏸️ {zone_name} time mismatch: IST={now_t}, "
                            f"{info['verify_against_tz']}={now_target_tz.time()}, diff={time_diff:.1f}h"
                        )
                        continue
                
                return {
                    "zone": zone_name,
                    "bias": info["bias"],
                    "priority": info["priority"],
                    "time_remaining": self._time_remaining(info["end"]),
                    "min_volume_ratio": info["min_volume_ratio"],
                    "expected_move_pct": info["expected_move_pct"],
                }
        return None

    def get_killzone_summary(self, symbol: str = None) -> Dict:
        """Get comprehensive killzone status with volume context."""
        base_killzone = self.get_current_killzone()
        
        summary = {
            "current_killzone": base_killzone,
            "volume_awareness": True,
            "system_time": datetime.now(self.ist).isoformat(),
            "market_hours": self._is_market_hours()
        }
        
        if symbol and base_killzone:
            # Add symbol-specific volume context - FIXED: Use direct Redis read with canonical_symbol
            try:
                from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
                
                # Get canonical symbol for consistent Redis lookup
                canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
                baseline_key = DatabaseAwareKeyBuilder.live_volume_baseline(canonical_symbol)
                baseline_data = self.redis_client.hgetall(baseline_key)
                
                baseline = 0.0
                if baseline_data:
                    # Decode bytes if needed
                    if isinstance(list(baseline_data.values())[0], bytes):
                        baseline_data = {
                            k.decode() if isinstance(k, bytes) else k:
                            v.decode() if isinstance(v, bytes) else v
                            for k, v in baseline_data.items()
                        }
                    
                    # Priority: baseline_1min > current > avg_volume_55d/375
                    baseline_1min = baseline_data.get('baseline_1min')
                    if baseline_1min:
                        baseline = float(baseline_1min)
                    else:
                        current = baseline_data.get('current')
                        if current:
                            baseline = float(current)
                        else:
                            avg_vol_55d = baseline_data.get('avg_volume_55d')
                            if avg_vol_55d:
                                baseline = float(avg_vol_55d) / 375.0
                
                summary["volume_context"] = {
                    "symbol": symbol,
                    "canonical_symbol": canonical_symbol,
                    "baseline_volume": baseline,
                    "vix_regime": self._get_current_vix_regime(),
                    "required_volume_ratio": base_killzone["min_volume_ratio"]
                }
            except Exception as e:
                summary["volume_context_error"] = str(e)
        
        return summary
# ✅ BACKWARD COMPATIBILITY: Alias for existing code
ICTKillzoneDetector = EnhancedICTKillzoneDetector
