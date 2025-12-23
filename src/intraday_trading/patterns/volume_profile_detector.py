#!/opt/homebrew/bin/python3.13
"""
Volume Profile Pattern Detector
===============================

Advanced pattern detection based on volume profile analysis.
Extracted from volume_profile_manager.py for specialized pattern recognition.

INTEGRATION:
- Uses volume profile data from VolumeProfileManager
- Publishes pattern signals to Redis for trading decisions
- Follows canonical field names from config/optimized_field_mapping.yaml

Author: AION Integration Team  
Date: October 26, 2025
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple, Optional
from enum import Enum

class VolumePatternType(Enum):
    """Volume Profile Pattern Types"""
    HIGH_VOLUME_NODE = "high_volume_node"
    LOW_VOLUME_NODE = "low_volume_node" 
    VOLUME_GAP = "volume_gap"
    POC_SHIFT = "poc_shift"
    VALUE_AREA_EXPANSION = "value_area_expansion"
    VALUE_AREA_CONTRACTION = "value_area_contraction"

class VolumeProfileDetector:
    """
    Advanced Volume Profile Pattern Detection
    
    Extracted from VolumeProfileManager to focus specifically on pattern recognition
    while maintaining integration with the existing volume profile system.
    """
    
    def __init__(self, redis_client, pattern_thresholds: Dict = None):
        """
        Initialize Volume Profile Pattern Detector
        
        Args:
            redis_client: Redis client for data access and pattern publishing
            pattern_thresholds: Custom thresholds for pattern detection
        """
        self.redis_client = redis_client
        self.thresholds = pattern_thresholds or self._get_default_thresholds()
        self.logger = logging.getLogger(__name__)
        
    def _get_default_thresholds(self) -> Dict:
        """Get default pattern detection thresholds"""
        return {
            'volume_node_threshold': 1.5,  # 150% of average volume
            'volume_gap_threshold': 0.3,   # 30% volume drop between levels
            'poc_shift_threshold': 0.02,   # 2% POC movement
            'va_expansion_threshold': 0.15, # 15% VA range increase
            'profile_strength_min': 0.3    # Minimum profile strength
        }
    
    def detect_patterns(self, symbol: str, current_profile: Dict, historical_data: List[Dict] = None) -> List[Dict]:
        """
        Detect volume profile patterns using standardized field names
        
        Args:
            symbol: Trading symbol
            current_profile: Current volume profile data from VolumeProfileManager
            historical_data: Previous profile data for trend analysis
            
        Returns:
            List of detected patterns with confidence scores
        """
        patterns = []
        
        try:
            # 1. Detect High Volume Nodes (Support/Resistance)
            patterns.extend(self._detect_volume_nodes(symbol, current_profile))
            
            # 2. Detect Volume Gaps (Low Volume Areas)
            patterns.extend(self._detect_volume_gaps(symbol, current_profile))
            
            # 3. Detect POC Shifts (if historical data available)
            if historical_data:
                patterns.extend(self._detect_poc_shifts(symbol, current_profile, historical_data))
                
            # 4. Detect Value Area Changes
            if historical_data:
                patterns.extend(self._detect_value_area_changes(symbol, current_profile, historical_data))
            
            # 5. Detect Profile Strength Patterns
            patterns.extend(self._detect_profile_strength_patterns(symbol, current_profile))
            
            # Publish patterns to Redis
            if patterns:
                self._publish_patterns_to_redis(symbol, patterns)
                
        except Exception as e:
            self.logger.error(f"Pattern detection error for {symbol}: {e}")
            
        return patterns
    
    def _detect_volume_nodes(self, symbol: str, profile: Dict) -> List[Dict]:
        """Detect high and low volume nodes as support/resistance levels"""
        patterns = []
        
        try:
            # Get volume distribution data
            price_volume_data = self._get_price_volume_data(symbol)
            if not price_volume_data:
                return patterns
            
            # ✅ FIX: Calculate avg_volume, use baseline if avg_volume is 0
            volume_values = list(price_volume_data.values())
            avg_volume = np.mean(volume_values) if volume_values else 0.0
            
            # ✅ FIX: Get baseline volume from config file when avg_volume is 0
            if avg_volume <= 0:
                baseline_volume = self._get_baseline_volume(symbol)
                if baseline_volume and baseline_volume > 0:
                    avg_volume = baseline_volume
                    self.logger.debug(f"⚠️ [VOLUME_PROFILE] {symbol} Using baseline volume {baseline_volume:.0f} (avg_volume was 0)")
                else:
                    # If no baseline available, skip detection to avoid division by zero
                    return patterns
            
            volume_threshold = avg_volume * self.thresholds['volume_node_threshold']
            
            # ✅ FIX: Ensure volume_threshold is not zero before division
            if volume_threshold <= 0:
                return patterns
            
            # Find high volume nodes (potential support/resistance)
            for price, volume in price_volume_data.items():
                if volume >= volume_threshold:
                    # Classify as support or resistance relative to current price
                    current_price = profile.get('poc_price', 0)
                    pattern_type = (VolumePatternType.HIGH_VOLUME_NODE.value + 
                                  ("_support" if price < current_price else "_resistance"))
                    
                    # ✅ FIX: Safe division with check
                    confidence = min(volume / volume_threshold, 1.0) if volume_threshold > 0 else 0.0
                    
                    patterns.append({
                        'pattern_type': pattern_type,
                        'price_level': float(price),
                        'volume': int(volume),
                        'confidence': confidence,
                        'detection_time': datetime.now().isoformat(),
                        'symbol': symbol
                    })
                    
        except Exception as e:
            self.logger.error(f"Volume node detection error for {symbol}: {e}")
            
        return patterns
    
    def _detect_volume_gaps(self, symbol: str, profile: Dict) -> List[Dict]:
        """Detect low volume areas (gaps) that could indicate breakout zones"""
        patterns = []
        
        try:
            price_volume_data = self._get_price_volume_data(symbol)
            if len(price_volume_data) < 3:
                return patterns
            
            sorted_prices = sorted(price_volume_data.keys())
            # ✅ FIX: Calculate avg_volume, use baseline if avg_volume is 0
            volume_values = list(price_volume_data.values())
            avg_volume = np.mean(volume_values) if volume_values else 0.0
            
            # ✅ FIX: Get baseline volume from config file when avg_volume is 0
            if avg_volume <= 0:
                baseline_volume = self._get_baseline_volume(symbol)
                if baseline_volume and baseline_volume > 0:
                    avg_volume = baseline_volume
                    self.logger.debug(f"⚠️ [VOLUME_PROFILE] {symbol} Using baseline volume {baseline_volume:.0f} for gap detection (avg_volume was 0)")
                else:
                    # If no baseline available, skip detection to avoid division by zero
                    return patterns
            
            gap_threshold = avg_volume * self.thresholds['volume_gap_threshold']
            
            # ✅ FIX: Ensure gap_threshold is not zero before division
            if gap_threshold <= 0:
                return patterns
            
            for i in range(1, len(sorted_prices) - 1):
                current_vol = price_volume_data[sorted_prices[i]]
                prev_vol = price_volume_data[sorted_prices[i-1]]
                next_vol = price_volume_data[sorted_prices[i+1]]
                
                # Check if current level has significantly lower volume than neighbors
                if (current_vol <= gap_threshold and 
                    prev_vol > gap_threshold and 
                    next_vol > gap_threshold):
                    
                    # ✅ FIX: Safe division with check
                    confidence = 1.0 - (current_vol / gap_threshold) if gap_threshold > 0 else 0.0
                    
                    patterns.append({
                        'pattern_type': VolumePatternType.VOLUME_GAP.value,
                        'price_level': float(sorted_prices[i]),
                        'gap_volume': int(current_vol),
                        'confidence': confidence,
                        'detection_time': datetime.now().isoformat(),
                        'symbol': symbol
                    })
                    
        except Exception as e:
            self.logger.error(f"Volume gap detection error for {symbol}: {e}")
            
        return patterns
    
    def _detect_poc_shifts(self, symbol: str, current_profile: Dict, historical_data: List[Dict]) -> List[Dict]:
        """Detect significant Point of Control shifts"""
        patterns = []
        
        try:
            if not historical_data:
                return patterns
                
            current_poc = current_profile.get('poc_price', 0)
            previous_poc = historical_data[-1].get('poc_price', 0)
            
            if current_poc > 0 and previous_poc > 0:
                poc_change = abs(current_poc - previous_poc) / previous_poc
                
                if poc_change >= self.thresholds['poc_shift_threshold']:
                    direction = "up" if current_poc > previous_poc else "down"
                    
                    patterns.append({
                        'pattern_type': f"{VolumePatternType.POC_SHIFT.value}_{direction}",
                        'current_poc': float(current_poc),
                        'previous_poc': float(previous_poc),
                        'change_percent': float(poc_change * 100),
                        'confidence': min(poc_change / self.thresholds['poc_shift_threshold'], 1.0),
                        'detection_time': datetime.now().isoformat(),
                        'symbol': symbol
                    })
                    
        except Exception as e:
            self.logger.error(f"POC shift detection error for {symbol}: {e}")
            
        return patterns
    
    def _detect_value_area_changes(self, symbol: str, current_profile: Dict, historical_data: List[Dict]) -> List[Dict]:
        """Detect Value Area expansion or contraction"""
        patterns = []
        
        try:
            if not historical_data:
                return patterns
                
            current_va_high = current_profile.get('value_area_high', 0)
            current_va_low = current_profile.get('value_area_low', 0)
            previous_va_high = historical_data[-1].get('value_area_high', 0)
            previous_va_low = historical_data[-1].get('value_area_low', 0)
            
            if all([current_va_high > 0, current_va_low > 0, previous_va_high > 0, previous_va_low > 0]):
                current_range = current_va_high - current_va_low
                previous_range = previous_va_high - previous_va_low
                
                if previous_range > 0:
                    range_change = (current_range - previous_range) / previous_range
                    
                    if abs(range_change) >= self.thresholds['va_expansion_threshold']:
                        pattern_type = (VolumePatternType.VALUE_AREA_EXPANSION.value 
                                      if range_change > 0 else 
                                      VolumePatternType.VALUE_AREA_CONTRACTION.value)
                        
                        patterns.append({
                            'pattern_type': pattern_type,
                            'current_range': float(current_range),
                            'previous_range': float(previous_range),
                            'change_percent': float(range_change * 100),
                            'confidence': min(abs(range_change) / self.thresholds['va_expansion_threshold'], 1.0),
                            'detection_time': datetime.now().isoformat(),
                            'symbol': symbol
                        })
                        
        except Exception as e:
            self.logger.error(f"Value area change detection error for {symbol}: {e}")
            
        return patterns
    
    def _detect_profile_strength_patterns(self, symbol: str, profile: Dict) -> List[Dict]:
        """Detect patterns based on overall profile strength"""
        patterns = []
        
        try:
            profile_strength = profile.get('profile_strength', 0)
            total_volume = profile.get('total_volume', 0)
            
            if profile_strength >= self.thresholds['profile_strength_min']:
                patterns.append({
                    'pattern_type': 'strong_profile_formation',
                    'strength_score': float(profile_strength),
                    'total_volume': int(total_volume),
                    'confidence': float(profile_strength),
                    'detection_time': datetime.now().isoformat(),
                    'symbol': symbol
                })
                
        except Exception as e:
            self.logger.error(f"Profile strength detection error for {symbol}: {e}")
            
        return patterns
    
    def _get_price_volume_data(self, symbol: str) -> Dict[float, int]:
        """Get price-volume distribution data from Redis using correct key patterns"""
        try:
            # Use DatabaseAwareKeyBuilder instead of hardcoded key
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            session_date = datetime.now().strftime('%Y-%m-%d')
            session_key = DatabaseAwareKeyBuilder.live_volume_profile(symbol, "session", session_date)
            price_volume_data = self.redis_client.hgetall(session_key)
            
            # Convert Redis data to price-volume dictionary
            result = {}
            for key, value in price_volume_data.items():
                key_str = key.decode() if isinstance(key, bytes) else key
                value_str = value.decode() if isinstance(value, bytes) else value
                
                # Look for price-volume data in the session hash
                if key_str.startswith('price_volume_'):
                    try:
                        price = float(key_str.replace('price_volume_', ''))
                        volume = int(value_str)
                        result[price] = volume
                    except (ValueError, TypeError):
                        continue
                        
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting price-volume data for {symbol}: {e}")
            return {}
    
    def _publish_patterns_to_redis(self, symbol: str, patterns: List[Dict]):
        """Publish detected patterns to Redis using standardized keys"""
        try:
            pattern_key = f"patterns:volume_profile:{symbol}"
            
            for pattern in patterns:
                # Store each pattern with timestamp
                pattern_id = f"{pattern['pattern_type']}_{datetime.now().strftime('%H%M%S')}"
                self.redis_client.hset(pattern_key, pattern_id, json.dumps(pattern))
            
            # Set expiration for pattern key (24 hours)
            self.redis_client.expire(pattern_key, 86400)
            
        except Exception as e:
            self.logger.error(f"Error publishing patterns to Redis for {symbol}: {e}")