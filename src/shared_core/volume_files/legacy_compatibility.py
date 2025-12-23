# shared_core/volume/legacy_compatibility.py
from typing import Dict, Any

class LegacyKeyCompatibility:
    """Ensures downstream consumers keep working during refactor."""
    
    @staticmethod
    def ensure_legacy_keys(redis_client, symbol: str, new_data: Dict[str, Any]):
        """Write to both new and legacy keys during transition period."""
        # 1. Write to new unified keys
        new_key = f"volume:unified:{symbol}:{date}"
        redis_client.hset(new_key, mapping=new_data)
        
        # 2. CRITICAL: Also write to legacy keys for backward compatibility
        legacy_keys = [
            f"vol:baseline:{symbol}",           # Daily baselines
            f"ind:{symbol}",                    # Indicator hash
            f"volume_profile:poc:{symbol}",     # POC
        ]
        
        for key in legacy_keys:
            # Transform data to match legacy format
            legacy_mapping = LegacyKeyCompatibility._transform_to_legacy(key, new_data)
            redis_client.hset(key, mapping=legacy_mapping)
        
        # 3. Set appropriate TTLs
        redis_client.expire(new_key, 86400)  # New key: 24 hours
        # Legacy keys keep their existing TTLs (already set by current code)
    
    @staticmethod
    def _transform_to_legacy(key: str, data: Dict) -> Dict:
        """Transform unified format to legacy format based on key pattern."""
        if "baseline" in key:
            return {
                'avg_volume_20d': str(data.get('baselines', {}).get('historical_20d', 0)),
                'avg_volume_55d': str(data.get('baselines', {}).get('historical_55d', 0)),
                'baseline_1min': str(data.get('baselines', {}).get('static_1min', 0)),
                'last_updated': data.get('timestamp')
            }
        elif "ind:" in key:
            return {
                'volume_ratio': str(data.get('ratios', {}).get('cumulative_ratio', 0)),
                'per_minute_ratio': str(data.get('ratios', {}).get('minute_ratio', 0))
            }
        return data