# shared_core/volume/time_aware.py
from datetime import time

class IndianMarketMultiplier:
    """Time-aware adjustments specific to Indian market structure."""
    
    # Indian market session multipliers
    SESSION_MULTIPLIERS = {
        'pre_open': (time(9, 0), time(9, 15), 0.1),      # Auction only
        'opening_frenzy': (time(9, 15), time(10, 0), 1.8),
        'morning_session': (time(10, 0), time(11, 30), 1.2),
        'mid_session': (time(11, 30), time(13, 0), 0.9),
        'afternoon_session': (time(13, 0), time(15, 0), 1.1),
        'closing_auction': (time(15, 0), time(15, 30), 2.0),
    }
    
    @classmethod
    def get_multiplier(cls, current_time: time, instrument_type: InstrumentType) -> float:
        """Get time multiplier based on Indian market sessions."""
        
        # Different patterns for F&O vs Equity
        if instrument_type == InstrumentType.FNO_INDEX:
            # F&O indices often have higher afternoon activity
            adjusted_multipliers = cls._adjust_for_fno_patterns()
        else:
            adjusted_multipliers = cls.SESSION_MULTIPLIERS
        
        for session, (start, end, multiplier) in adjusted_multipliers.items():
            if start <= current_time < end:
                return multiplier
        
        return 1.0