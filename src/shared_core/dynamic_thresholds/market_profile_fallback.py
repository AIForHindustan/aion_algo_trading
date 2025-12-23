# shared_core/dynamic_thresholds/market_profile_fallback.py
"""
Fallback implementation for market_profile when not available.
"""
import logging

logger = logging.getLogger(__name__)

class MarketProfileFallback:
    """Fallback when market_profile package is not available"""
    
    def __init__(self):
        self.available = False
        logger.warning("market_profile package not available - using fallback implementation")
    
    def calculate_profile(self, data):
        """Basic volume profile calculation fallback"""
        logger.warning("Using fallback market profile calculation")
        return {
            'poc': data.get('close', 0) if data else 0,
            'value_area_high': data.get('high', 0) if data else 0,
            'value_area_low': data.get('low', 0) if data else 0,
            'volume_profile': {},
            'fallback': True
        }

# Try to import real market_profile, fallback if not available
try:
    import market_profile
    MarketProfile = market_profile.MarketProfile
    MARKET_PROFILE_AVAILABLE = True
except ImportError:
    MarketProfile = MarketProfileFallback
    MARKET_PROFILE_AVAILABLE = False
    logger.warning("market_profile package not installed - using fallback implementation")