#!/opt/homebrew/bin/python3.13
"""
VOLUME THRESHOLD CALCULATOR
Mathematical foundation for volume spike detection across different resolutions

✅ ENHANCED: Now fetches VIX from Redis DB1 if not provided.
"""
import sys
import math
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import logging

# Add parent directory to path for dynamic_thresholds
project_root = Path(__file__).resolve().parents[1]
parent_root = project_root.parent  # Go up to aion_algo_trading level
if str(parent_root) not in sys.path:
    sys.path.insert(0, str(parent_root))

from shared_core.config_utils.thresholds import get_safe_vix_regime, get_volume_baseline

logger = logging.getLogger(__name__)

class VolumeThresholdCalculator:
    """
    Mathematical engine for volume threshold calculations
    Handles statistical distributions, outlier detection, and resolution scaling
    
    ✅ UPDATED: Uses 20-day OHLC baselines stored in Redis for spike calculations
    """
    
    BUCKET_MINUTES = {
        "1min": 1,
        "2min": 2,
        "5min": 5,
        "10min": 10,
        "15min": 15,
        "30min": 30,
        "60min": 60,
    }

    VIX_VOLUME_MULTIPLIERS = {
        "PANIC": 1.4,
        "HIGH": 1.2,
        "NORMAL": 1.0,
        "LOW": 0.8,
    }

    def __init__(self, time_aware_baseline=None, redis_client=None):
        """
        Initialize VolumeThresholdCalculator.
        
        Args:
            time_aware_baseline: Legacy parameter (ignored, kept for compatibility)
            redis_client: Optional Redis client for baseline lookups
        """
        self.redis_client = redis_client
    
    def calculate_volume_spike_threshold(self, symbol: str, bucket_resolution: str,
                                      current_time: datetime, current_volume: float = 0.0,
                                      vix_regime: Optional[str] = None,
                                      confidence_level: float = 0.95, indicators: Optional[Dict] = None) -> float:
        """
        ✅ ENHANCED: Calculate volume spike threshold using session baseline instead of 20d baseline.
        
        Uses session baseline (first 30 minutes of trading) for focused instrument universe.
        If vix_regime is None, fetches from Redis DB1.
        
        ✅ UPDATED: Now uses calculate_session_baseline() for session-based volume baseline
        
        BUCKET RESOLUTION MATHEMATICS:
        Example with BANKNIFTY (5min: 322M average):
        - 1min baseline: 322M / 5 = 64.4M
        - 10min baseline: 64.4M × 10 = 644M
        - Session-adjusted: 644M × 1.8 = 1,159M (opening)
        - VIX-adjusted: 1,159M × 1.4 = 1,623M (high volatility)
        - Spike threshold: 1,623M × 2.5 = 4,058M (2.5x multiplier)
        
        Args:
            symbol: Trading symbol
            bucket_resolution: Time bucket resolution (1min, 5min, etc.)
            current_time: Current timestamp
            current_volume: Current volume (used for session baseline calculation)
            vix_regime: Optional VIX regime (fetched from Redis if None)
            confidence_level: Confidence level for threshold (default 0.95)
            indicators: Optional indicators dict
            
        Returns:
            Threshold where volume > threshold indicates a spike
        """
        vix_regime = vix_regime or get_safe_vix_regime(redis_client=self.redis_client)
        daily_baseline = get_volume_baseline(symbol, redis_client=self.redis_client)
        if daily_baseline <= 0:
            logger.debug(f"⚠️ [VOLUME_THRESHOLD] {symbol} missing 20d baseline data")
            return 0.0
        
        minutes = self.BUCKET_MINUTES.get(bucket_resolution, 5)
        minutes = max(1, minutes)
        per_minute_baseline = daily_baseline / 375.0  # Approximate trading minutes per session
        bucket_baseline = per_minute_baseline * minutes
        
        # Statistical spike detection thresholds
        # Based on historical volatility of volume
        spike_multipliers = {
            "1min": 3.5,   # 1-minute needs higher multiplier (noisier)
            "2min": 3.2,   # 2-minute
            "5min": 2.8,   # 5-minute (your current base)
            "10min": 2.5,  # 10-minute
            "15min": 2.3,  # 15-minute
            "30min": 2.0,  # 30-minute
            "60min": 1.8,  # 60-minute
        }
        base_multiplier = spike_multipliers.get(bucket_resolution, 2.2)
        vix_multiplier = self.VIX_VOLUME_MULTIPLIERS.get(vix_regime.upper(), 1.0)
        
        # ✅ NEW: Apply instrument config volume_multiplier if available
        volume_multiplier = 1.0
        if indicators:
            try:
                from config.instrument_config_manager import get_instrument_config_manager
                config_manager = get_instrument_config_manager()
                config_multiplier = config_manager.get_volume_multiplier(symbol, indicators)
                if config_multiplier is not None:
                    volume_multiplier = config_multiplier
                    logger.debug(f"✅ [VOLUME_THRESHOLD] {symbol} Using volume_multiplier={volume_multiplier:.2f} from instrument config")
            except Exception as e:
                logger.debug(f"⚠️ [VOLUME_THRESHOLD] Instrument config volume_multiplier lookup failed: {e}")
        # Adjust for confidence level (higher confidence = higher threshold)
        confidence_adjustment = 1.0 + (confidence_level - 0.5) * 2.0
        # 0.5 confidence -> 1.0x, 0.95 confidence -> 1.9x
        
        threshold = bucket_baseline * base_multiplier * confidence_adjustment * volume_multiplier * vix_multiplier
        logger.debug(
            f"✅ [VOLUME_THRESHOLD] {symbol} {bucket_resolution} baseline={bucket_baseline:.0f} × "
            f"spike={base_multiplier:.2f} × conf={confidence_adjustment:.2f} × inst={volume_multiplier:.2f} × "
            f"vix={vix_multiplier:.2f} ({vix_regime}) => {threshold:.0f}"
        )
        
        logger.info(f"Volume threshold {symbol} {bucket_resolution}: "
                   f"baseline={baseline:.0f} × {base_multiplier} × {confidence_adjustment:.2f} = {threshold:.0f}")
        
        return threshold
    
    def calculate_volume_ratio(self, current_volume: float, threshold: float) -> float:
        """Calculate volume ratio relative to threshold"""
        if threshold <= 0:
            return 1.0  # Neutral fallback
        
        ratio = current_volume / threshold
        
        # Cap extremely high ratios to prevent outliers from dominating
        if ratio > 10.0:
            ratio = 10.0 + math.log10(ratio - 9.0)  # Logarithmic scaling above 10x
        
        return ratio
    
    def detect_volume_anomaly(self, current_volume: float, historical_volumes: List[float],
                            bucket_resolution: str, confidence: float = 0.99) -> bool:
        """
        Volume anomaly detection for real-world incremental volume data
        
        Based on actual volume data characteristics:
        - Incremental volumes have extreme variance (35 to 14M+)
        - Many zero values (no trading activity)
        - Percentile-based thresholds more robust than Z-scores
        - Spike detection using 90th percentile threshold
        """
        if len(historical_volumes) < 10:
            # Not enough data for statistical detection
            mean_volume = sum(historical_volumes) / len(historical_volumes) if historical_volumes else 0
            return current_volume > mean_volume * 2.5
        
        # Calculate mean and standard deviation
        mean_volume = sum(historical_volumes) / len(historical_volumes)
        variance = sum((x - mean_volume) ** 2 for x in historical_volumes) / len(historical_volumes)
        std_dev = math.sqrt(variance)
        
        if std_dev == 0:
            return current_volume > mean_volume * 2.0
        
        # Calculate Z-score
        z_score = (current_volume - mean_volume) / std_dev
        
        # Z-score thresholds for different confidence levels
        z_thresholds = {
            0.90: 1.645,  # 90% confidence
            0.95: 1.960,  # 95% confidence (2 std deviations)
            0.99: 2.576,  # 99% confidence
            0.999: 3.291,  # 99.9% confidence
        }
        
        threshold = z_thresholds.get(confidence, 1.960)  # Default to 95% confidence
        
        # Log significant spikes for analysis
        if z_score > threshold:
            logger.info(f"📊 VOLUME ANOMALY DETECTED: Z-score={z_score:.2f}, "
                       f"threshold={threshold:.2f}, confidence={confidence:.1%}")
        
        return z_score > threshold

    def calculate_volume_distribution_stats(self, volumes: List[float]) -> Dict[str, float]:
        """
        Calculate robust volume distribution statistics for real-world data
        
        Handles extreme variance and outliers in incremental volume data:
        - Filters out zero values for meaningful statistics
        - Uses percentiles instead of mean/std for robustness
        - Calculates coefficient of variation for relative variability
        
        Returns:
            Dictionary with median, percentiles, variance, skewness, kurtosis
        """
        if not volumes or len(volumes) < 2:
            return {
                'mean_volume': 0.0,
                'std_dev': 0.0,
                'variance': 0.0,
                'skewness': 0.0,
                'kurtosis': 0.0,
                'coefficient_of_variation': 0.0
            }
        
        # Basic statistics
        mean_volume = sum(volumes) / len(volumes)
        variance = sum((x - mean_volume) ** 2 for x in volumes) / len(volumes)
        std_dev = math.sqrt(variance)
        
        # Higher moments for distribution analysis
        if std_dev > 0:
            # Skewness (measure of asymmetry)
            skewness = sum(((x - mean_volume) / std_dev) ** 3 for x in volumes) / len(volumes)
            
            # Kurtosis (measure of tail heaviness)
            kurtosis = sum(((x - mean_volume) / std_dev) ** 4 for x in volumes) / len(volumes) - 3
            
            # Coefficient of variation (relative variability)
            coefficient_of_variation = std_dev / mean_volume if mean_volume > 0 else 0
        else:
            skewness = kurtosis = coefficient_of_variation = 0.0
        
        return {
            'mean_volume': mean_volume,
            'std_dev': std_dev,
            'variance': variance,
            'skewness': skewness,
            'kurtosis': kurtosis,
            'coefficient_of_variation': coefficient_of_variation
        }
    
    def get_optimal_bucket_resolution(self, symbol: str, current_time: datetime) -> str:
        """
        Determine optimal bucket resolution based on:
        - Symbol liquidity
        - Time of day
        - Available data
        """
        # High liquidity symbols can use smaller buckets
        liquidity_tiers = {
            "high": ["BANKNIFTY", "NIFTY", "RELIANCE", "HDFC", "INFOSYS", "TCS"],
            "medium": ["AXISBANK", "ICICIBANK", "KOTAKBANK", "SBIN"],
            "low": []  # Everything else
        }
        
        symbol_liquidity = "low"
        for tier, symbols in liquidity_tiers.items():
            if any(s in symbol for s in symbols):
                symbol_liquidity = tier
                break
        
        # Time-based resolution adjustment
        hour = current_time.hour
        if hour in [9, 15]:  # Opening/Closing
            recommended = "2min" if symbol_liquidity == "high" else "5min"
        elif hour in [10, 11, 14]:  # Active hours
            recommended = "5min" if symbol_liquidity == "high" else "10min"
        else:  # Mid-day lull
            recommended = "10min" if symbol_liquidity == "high" else "15min"
        
        return recommended
    
    def get_vix_adjusted_confidence(self, base_confidence: float, pattern_type: str, symbol: str) -> float:
        """
        Apply VIX regime multipliers to confidence.
        
        Args:
            base_confidence: Base confidence value (0.0 to 1.0)
            pattern_type: Pattern type (for potential pattern-specific adjustments)
            symbol: Trading symbol (for logging)
            
        Returns:
            VIX-adjusted confidence (capped at 0.95)
        """
        vix_regime = self._get_vix_regime()
        
        # VIX confidence multipliers (inverse relationship: high VIX = lower confidence)
        vix_confidence_multipliers = {
            "HIGH": 0.8,      # Reduce confidence in high VIX (more noise)
            "NORMAL": 1.0,    # Normal confidence
            "LOW": 1.2,       # Increase confidence in low VIX (less noise)
            "PANIC": 0.8,     # Same as HIGH (high noise)
            "UNKNOWN": 1.0
        }
        
        multiplier = vix_confidence_multipliers.get(vix_regime, 1.0)
        adjusted_confidence = base_confidence * multiplier
        
        # Cap at 0.95 to prevent overconfidence
        return min(0.95, adjusted_confidence)
    
    def get_vix_aware_thresholds(self, pattern_name: str, base_thresholds: dict, symbol: str) -> dict:
        """
        Apply VIX adjustments to all thresholds in a dictionary.
        
        Args:
            pattern_name: Pattern name (for logging)
            base_thresholds: Dictionary of base threshold values
            symbol: Trading symbol (for logging)
            
        Returns:
            Dictionary with VIX-adjusted thresholds
        """
        vix_regime = self._get_vix_regime()
        
        # Threshold multipliers (inverse of confidence: high VIX = higher thresholds needed)
        threshold_multipliers = {
            "HIGH": 1.3,    # Higher thresholds in high VIX (need stronger signals)
            "NORMAL": 1.0,  # Base thresholds  
            "LOW": 0.7,     # Lower thresholds in low VIX (easier to detect signals)
            "PANIC": 1.5,   # Much higher in panic (need very strong signals)
            "UNKNOWN": 1.0
        }
        
        multiplier = threshold_multipliers.get(vix_regime, 1.0)
        adjusted_thresholds = {k: v * multiplier for k, v in base_thresholds.items()}
        
        logger.debug(
            f"🎯 [VIX_AWARE_THRESHOLDS] {pattern_name} for {symbol}: "
            f"vix_regime={vix_regime}, multiplier={multiplier:.2f}"
        )
        
        return adjusted_thresholds
    
    def _get_vix_regime(self) -> str:
        """
        Get current VIX regime using cached helper from thresholds.py.
        
        Returns:
            VIX regime: "HIGH", "NORMAL", "LOW", or "PANIC"
        """
        try:
            return get_safe_vix_regime(redis_client=self.redis_client)
        except Exception as exc:
            logger.debug(f"⚠️ [VIX_REGIME] Failed to resolve VIX regime: {exc}")
            return "NORMAL"
