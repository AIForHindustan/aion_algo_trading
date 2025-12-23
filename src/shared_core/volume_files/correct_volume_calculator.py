"""
Volume Resolver - Static Utility for Volume Field Extraction
============================================================

⚠️ LEGACY: CorrectVolumeCalculator class has been removed.
All volume calculations now use the unified VolumeComputationManager (redis_files/volume_manager.py).

VolumeResolver provides static utility methods for extracting volume fields from tick data.
This is used by VolumeComputationManager and other components that need to read pre-computed volume values.

Used by:
- redis_files/volume_manager.py (unified volume manager)
- intraday_scanner/scanner_main.py (volume field extraction)
- patterns/pattern_detector.py (volume field extraction)
- alert_validation/alert_validator.py (volume field extraction)
"""

import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)

class VolumeResolver:
    """
    ONE way to resolve volume fields across entire system
    """
    
    @staticmethod
    def get_incremental_volume(tick_data: dict) -> float:
        """Get pre-calculated incremental volume from WebSocket parser (single source of truth)"""
        try:
            # ✅ SINGLE SOURCE OF TRUTH: Use pre-calculated volume data from WebSocket parser
            # The WebSocket parser has already calculated incremental volume using VolumeComputationManager
            # Do NOT recalculate - this violates the single source of truth principle
            
            if 'bucket_incremental_volume' in tick_data and tick_data['bucket_incremental_volume'] is not None:
                return float(tick_data['bucket_incremental_volume'])
            
            # Fallback to other volume fields if bucket_incremental_volume not available
            if 'incremental_volume' in tick_data and tick_data['incremental_volume'] is not None:
                return float(tick_data['incremental_volume'])
            
            return 0.0
            
        except Exception as e:
            return 0.0
    
    @staticmethod
    def get_cumulative_volume(tick_data: dict) -> float:
        """Get cumulative volume from ANY data source"""
        try:

            # PRIMARY: Direct from Zerodha
            if 'zerodha_cumulative_volume' in tick_data and tick_data['zerodha_cumulative_volume'] is not None:
                value = float(tick_data['zerodha_cumulative_volume'])
                if value > 0:
                    return value
            
            # SECONDARY: From bucket (calculated from price changes) 
            if 'bucket_cumulative_volume' in tick_data and tick_data['bucket_cumulative_volume'] is not None:
                value = float(tick_data['bucket_cumulative_volume'])
                if value > 0:
                    return value
            
            # FALLBACK: Any cumulative field
            for field in ['volume_traded', 'cumulative_volume', 'volume']:
                if field in tick_data and tick_data[field] is not None:
                    value = float(tick_data[field])
                    if value > 0:
                        return value
            
            return 0.0
            
        except (TypeError, ValueError) as e:
            return 0.0
    
    @staticmethod
    def get_volume_ratio(tick_data: dict) -> float:
        """Get pre-calculated volume ratio if available"""
        try:
            # Check for pre-calculated ratio
            for field in ['volume_ratio', 'normalized_volume']:
                if field in tick_data and tick_data[field] is not None:
                    return float(tick_data[field])
            
            return 0.0
            
        except (TypeError, ValueError) as e:
            return 0.0
            
    @staticmethod
    def verify_volume_consistency(tick_data: dict, stage_name: str = "unknown") -> Dict[str, Any]:
        """
        Verify that volume metrics remain consistent through pipeline stages.
        This ensures the single source of truth principle is maintained.
        
        Returns:
            Dict with verification results and any inconsistencies found
        """
        try:
            verification = {
                "stage": stage_name,
                "timestamp": datetime.now().isoformat(),
                "consistent": True,
                "issues": [],
                "metrics": {}
            }
            
            # Get volume metrics using centralized resolver
            incremental = VolumeResolver.get_incremental_volume(tick_data)
            cumulative = VolumeResolver.get_cumulative_volume(tick_data)
            volume_ratio = VolumeResolver.get_volume_ratio(tick_data)
            
            verification["metrics"] = {
                "incremental_volume": incremental,
                "cumulative_volume": cumulative,
                "volume_ratio": volume_ratio
            }
            
            # Check for data consistency
            if incremental < 0:
                verification["issues"].append("Negative incremental volume detected")
                verification["consistent"] = False
                
            if cumulative < 0:
                verification["issues"].append("Negative cumulative volume detected")
                verification["consistent"] = False
                
            if volume_ratio < 0:
                verification["issues"].append("Negative volume ratio detected")
                verification["consistent"] = False
                
            # Check for reasonable volume ratio bounds (0-100x is reasonable)
            if volume_ratio > 100:
                verification["issues"].append(f"Extremely high volume ratio: {volume_ratio:.1f}x")
                verification["consistent"] = False
                
            # Log verification results
            if not verification["consistent"]:
                logger.warning(f"Volume consistency issues at {stage_name}: {verification['issues']}")
            else:
                pass
                
            return verification
            
        except Exception as e:
            logger.error(f"Volume verification failed at {stage_name}: {e}")
            return {
                "stage": stage_name,
                "timestamp": datetime.now().isoformat(),
                "consistent": False,
                "issues": [f"Verification error: {str(e)}"],
                "metrics": {}
            }
