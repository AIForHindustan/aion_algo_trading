#!/opt/homebrew/bin/python3.13
"""
Market Calendar Utility - SINGLE SOURCE OF TRUTH
================================================

✅ CONSOLIDATED: All calendar logic uses pandas_market_calendars via this module.
This is the ONLY place calendar logic should exist - all other files should import from here.

Provides trading day checking and expiry date calculation functionality 
for NSE (National Stock Exchange) India using pandas_market_calendars.

Features:
- Global calendar caching (thread-safe)
- Trading day validation
- Expiry date calculations (weekly/monthly)
- DTE (Days To Expiry) calculations
- Trading days between dates
"""

from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from threading import Lock
import logging

logger = logging.getLogger(__name__)

# Try to import pandas_market_calendars
try:
    import pandas_market_calendars as mcal
    CALENDAR_AVAILABLE = True
except ImportError:
    CALENDAR_AVAILABLE = False
    logger.warning("⚠️ pandas_market_calendars not available, using basic weekday check")

# ✅ GLOBAL CALENDAR CACHE - Single source of truth for calendar caching
# Thread-safe calendar cache shared across all instances
_calendar_cache: Dict[str, Any] = {}
_cache_lock = Lock()

_CALENDAR_ALIASES = {
    'NSE': 'XNSE',
    'XNSE': 'XNSE',
    'BSE': 'XBOM',
    'XBOM': 'XBOM',
    'NFO': 'XNSE',
    'BFO': 'XNSE',
    'CDS': 'XNSE',
    'MCX': 'XBOM',  # MCX follows BSE holiday schedule closely; use XBOM calendar
}

def get_cached_calendar(exchange: str = 'NSE'):
    """
    Get cached market calendar for exchange.
    
    ✅ SINGLE SOURCE OF TRUTH: This function provides global calendar caching.
    Used by:
    - MarketCalendar (this file)
    - ExpiryCalculator (calculations.py)
    - Any other components needing market calendar
    
    Args:
        exchange: Exchange name (default: 'NSE')
        
    Returns:
        Cached pandas_market_calendars calendar object, or None if unavailable
    """
    if not CALENDAR_AVAILABLE:
        return None
    
    exchange_key = (exchange or 'NSE').upper()
    calendar_name = _CALENDAR_ALIASES.get(exchange_key, exchange_key)
    
    with _cache_lock:
        if exchange_key not in _calendar_cache:
            try:
                _calendar_cache[exchange_key] = mcal.get_calendar(calendar_name)
                if calendar_name != exchange_key:
                    logger.debug(f"✅ Market calendar initialized for {exchange_key} (using cached calendar via alias {calendar_name})")
                else:
                    logger.debug(f"✅ Market calendar initialized for {exchange_key} (using cached calendar)")
            except Exception as e:
                logger.warning(f"⚠️ Failed to load {calendar_name} calendar for {exchange_key}: {e}")
                # Fallback to NSE calendar to avoid crashes
                if calendar_name != 'XNSE':
                    try:
                        _calendar_cache[exchange_key] = mcal.get_calendar('XNSE')
                        logger.info(f"ℹ️ Using NSE calendar as fallback for {exchange_key}")
                    except Exception as fallback_error:
                        logger.warning(f"⚠️ Failed to load fallback NSE calendar for {exchange_key}: {fallback_error}")
                        _calendar_cache[exchange_key] = None
                else:
                    _calendar_cache[exchange_key] = None
        
        return _calendar_cache.get(exchange_key)


class MarketCalendar:
    """
    Market calendar for checking trading days and calculating expiry dates.
    
    ✅ OPTIMIZED: Uses global cached calendar from calculations.py for performance.
    Uses pandas_market_calendars for accurate NSE holiday calendar.
    Includes methods for weekly and monthly expiry date calculations.
    """
    
    def __init__(self, exchange: str = 'NSE'):
        """
        Initialize market calendar.
        
        ✅ CONSOLIDATED: Uses global cached calendar from this module (single source of truth).
        
        Args:
            exchange: Exchange name (default: 'NSE' for National Stock Exchange India)
        """
        self.exchange = (exchange or 'NSE').upper()
        # ✅ Use global cached calendar (defined in this module)
        # Log message is only printed once when calendar is first cached (in get_cached_calendar)
        self.calendar = get_cached_calendar(self.exchange)
        if not self.calendar:
            logger.warning(f"⚠️ Calendar not available for {exchange} (pandas_market_calendars may not be installed)")
    
    def is_trading_day(self, date: Optional[datetime] = None) -> bool:
        """
        Check if a given date is a trading day.
        
        Args:
            date: Datetime object to check. If None, uses current date.
            
        Returns:
            True if the date is a trading day, False otherwise.
        """
        if date is None:
            date = datetime.now()
        
        # Use calendar if available
        if self.calendar:
            try:
                schedule = self.calendar.schedule(
                    start_date=date.date(),
                    end_date=date.date()
                )
                return len(schedule) > 0
            except Exception:
                # Fallback to basic weekday check
                return date.weekday() < 5
        
        # Fallback: Basic weekday check (Monday=0 to Friday=4)
        return date.weekday() < 5
    
    def get_next_weekly_expiry(self, from_date: Optional[datetime] = None, symbol: str = None) -> Optional[datetime]:
        """
        Get the next weekly expiry date based on underlying instrument.
        
        ✅ SYMBOL-AWARE EXPIRY DAYS:
        - NIFTY: Tuesday (as of Sep 2025)
        - BANKNIFTY: Wednesday (monthly only, no weekly)
        - SENSEX: Friday (BFO segment)
        - Default: Tuesday (NSE)
        
        Args:
            from_date: Starting date (default: current date)
            symbol: Optional symbol to determine correct expiry day
            
        Returns:
            Next weekly expiry datetime, or None if not found
        """
        if from_date is None:
            from_date = datetime.now()
        
        # ✅ SYMBOL-AWARE: Determine expiry weekday based on underlying
        # Weekday: Monday=0, Tuesday=1, Wednesday=2, Thursday=3, Friday=4
        expiry_weekday = 1  # Default: Tuesday (NIFTY)
        
        if symbol:
            symbol_upper = symbol.upper()
            if "SENSEX" in symbol_upper:
                expiry_weekday = 4  # Friday for SENSEX (BFO)
            elif "BANKNIFTY" in symbol_upper:
                expiry_weekday = 2  # Wednesday for BANKNIFTY (monthly only)
            # NIFTY and others use Tuesday (default)
        
        # Find next expiry day
        days_ahead = expiry_weekday - from_date.weekday()
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        
        potential_expiry = from_date + timedelta(days=days_ahead)
        
        # Ensure it's a trading day
        if self.calendar:
            # Find next trading day if expiry falls on holiday
            max_attempts = 10
            attempts = 0
            while attempts < max_attempts and not self.is_trading_day(potential_expiry):
                potential_expiry += timedelta(days=1)
                attempts += 1
        else:
            # Basic check: if expiry day is weekend, find next trading day
            while potential_expiry.weekday() >= 5:
                potential_expiry += timedelta(days=1)
        
        return potential_expiry
    
    def get_next_monthly_expiry(self, from_date: Optional[datetime] = None) -> Optional[datetime]:
        """
        Get the next monthly expiry date (last Tuesday of month - India F&O expiry).
        
        ✅ USES PANDAS_MARKET_CALENDARS: Uses calendar schedule to find actual last Tuesday
        that's a trading day, rather than manually calculating.
        
        Args:
            from_date: Starting date (default: current date)
            
        Returns:
            Next monthly expiry datetime, or None if not found
        """
        if from_date is None:
            from_date = datetime.now()
        
        # Use pandas_market_calendars to get trading schedule for the month
        if self.calendar:
            try:
                # Get first and last day of next month
                if from_date.month == 12:
                    next_month_start = from_date.replace(year=from_date.year + 1, month=1, day=1)
                    next_month_end = from_date.replace(year=from_date.year + 1, month=1, day=31)
                else:
                    next_month_start = from_date.replace(month=from_date.month + 1, day=1)
                    # Get last day of next month
                    if next_month_start.month == 12:
                        next_month_end = next_month_start.replace(day=31)
                    else:
                        next_month_end = (next_month_start.replace(month=next_month_start.month + 1, day=1) - timedelta(days=1))
                
                # Get trading schedule for the month
                schedule = self.calendar.schedule(
                    start_date=next_month_start.date(),
                    end_date=next_month_end.date()
                )
                
                # Filter for Tuesdays (weekday 1) and get the last one
                tuesdays = [date for date in schedule.index if date.weekday() == 1]
                if tuesdays:
                    return datetime.combine(tuesdays[-1], datetime.min.time())
            except Exception as e:
                logger.warning(f"⚠️ Failed to get monthly expiry from calendar: {e}, falling back to manual calculation")
        
        # Fallback: Manual calculation if calendar unavailable
        # Start from first day of next month
        if from_date.month == 12:
            next_month = from_date.replace(year=from_date.year + 1, month=1, day=1)
        else:
            next_month = from_date.replace(month=from_date.month + 1, day=1)
        
        # Get last day of that month
        if next_month.month == 12:
            last_day = next_month.replace(day=31)
        else:
            last_day = (next_month.replace(month=next_month.month + 1, day=1) - timedelta(days=1))
        
        # Find last Tuesday (weekday 1 = Tuesday)
        current = last_day
        while current.weekday() != 1:  # 1 = Tuesday
            current -= timedelta(days=1)
        
        # Ensure it's a trading day
        if not self.is_trading_day(current):
            # Move to previous trading day
            while not self.is_trading_day(current):
                current -= timedelta(days=1)
        
        return current
    
    def get_monthly_expiry_for_month(self, month_date: datetime) -> Optional[datetime]:
        """
        Get the monthly expiry (last Tuesday) for a specific month.
        
        ✅ USES PANDAS_MARKET_CALENDARS: Uses calendar schedule to find actual last Tuesday
        that's a trading day for the given month.
        
        Args:
            month_date: Any date in the target month
            
        Returns:
            Monthly expiry datetime (last Tuesday) for that month, or None if not found
        """
        if self.calendar:
            try:
                from calendar import monthrange
                # Get first and last day of the month
                first_day = month_date.replace(day=1)
                last_day_num = monthrange(month_date.year, month_date.month)[1]
                last_day = month_date.replace(day=last_day_num)
                
                # Get trading schedule for the month
                schedule = self.calendar.schedule(
                    start_date=first_day.date(),
                    end_date=last_day.date()
                )
                
                # Filter for Tuesdays (weekday 1) and get the last one
                tuesdays = [date for date in schedule.index if date.weekday() == 1]
                if tuesdays:
                    return datetime.combine(tuesdays[-1], datetime.min.time())
            except Exception:
                logger.debug(f"Failed to get monthly expiry from calendar for {month_date}")
        
        # Fallback: Manual calculation
        from calendar import monthrange
        last_day_num = monthrange(month_date.year, month_date.month)[1]
        last_date = datetime(month_date.year, month_date.month, last_day_num)
        weekday = last_date.weekday()
        if weekday == 1:  # Tuesday
            days_back = 0
        elif weekday == 0:  # Monday
            days_back = 6
        else:  # Wednesday-Sunday
            days_back = weekday - 1
        return last_date - timedelta(days=days_back)
    
    def get_all_upcoming_expiries(self, days_lookahead: int = 30, 
                                  from_date: Optional[datetime] = None) -> List[datetime]:
        """
        Get all upcoming expiry dates (weekly and monthly) within lookahead period.
        
        Args:
            days_lookahead: Number of days to look ahead
            from_date: Starting date (default: current date)
            
        Returns:
            List of expiry datetime objects
        """
        if from_date is None:
            from_date = datetime.now()
        
        end_date = from_date + timedelta(days=days_lookahead)
        expiries = []
        
        if self.calendar:
            try:
                schedule = self.calendar.schedule(
                    start_date=from_date.date(),
                    end_date=end_date.date()
                )
                
                # Filter Tuesdays (India F&O expiry days as of Sep 2025)
                for date in schedule.index:
                    if date.weekday() == 1:  # Tuesday
                        expiries.append(datetime.combine(date, datetime.min.time()))
            except Exception:
                pass
        else:
            # Fallback: Find all Tuesdays in range (India F&O expiry)
            current = from_date
            while current <= end_date:
                if current.weekday() == 1:  # Tuesday
                    expiries.append(current)
                current += timedelta(days=1)
        
        return expiries
    
    def is_expiry_day(self, date: Optional[datetime] = None) -> bool:
        """
        Check if a given date is an expiry day (Tuesday - India F&O expiry as of Sep 2025).
        
        Args:
            date: Datetime object to check. If None, uses current date.
            
        Returns:
            True if the date is an expiry day, False otherwise.
        """
        if date is None:
            date = datetime.now()
        
        # Expiry days are Tuesdays (India F&O expiry as of Sep 2025)
        return date.weekday() == 1 and self.is_trading_day(date)
    
    def get_trading_days_between(self, start_date: datetime, end_date: datetime) -> int:
        """
        Get number of trading days between two dates.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            Number of trading days (inclusive)
        """
        if self.calendar:
            try:
                schedule = self.calendar.schedule(
                    start_date=start_date.date(),
                    end_date=end_date.date()
                )
                return len(schedule)
            except Exception:
                pass
        
        # Fallback: Count weekdays
        count = 0
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:
                count += 1
            current += timedelta(days=1)
        return count
    
    def calculate_trading_dte(self, expiry_date: datetime, current_date: Optional[datetime] = None) -> int:
        """
        Calculate trading days to expiry (more accurate for options).
        
        ✅ CONSOLIDATED: This method replaces duplicate logic in ExpiryCalculator.
        
        Args:
            expiry_date: Expiry date
            current_date: Current date (default: now)
            
        Returns:
            Number of trading days to expiry
        """
        if current_date is None:
            current_date = datetime.now()
        
        if not self.calendar:
            # Fallback to calendar days
            return max(0, (expiry_date - current_date).days)
        
        try:
            # Get trading days between current date and expiry
            schedule = self.calendar.schedule(
                start_date=current_date.date(),
                end_date=expiry_date.date()
            )
            return len(schedule) - 1  # Exclude current day
        except Exception:
            # Fallback to calendar days
            return max(0, (expiry_date - current_date).days)
    
    def parse_expiry_from_symbol(self, symbol: str) -> Optional[datetime]:
        """
        Parse expiry date from option/future symbol.
        
        ✅ CONSOLIDATED: This method replaces duplicate logic in ExpiryCalculator.
        
        ✅ FIXED: Handles both Zerodha naming formats:
        - Old format: DDMMM (e.g., "25NOV" = November 25, 2025)
        - New format: YYMMM (e.g., "26JAN" = January 2026, expiry Jan 27, 2026)
        
        For YYMMM format, calculates the last Tuesday of the month (India F&O expiry as of Sep 2025).
        
        Examples:
            - "NIFTY25NOV26400CE" -> datetime(2025, 11, 25)  # 25NOV (old format)
            - "BANKNIFTY26JAN58600PE" -> datetime(2026, 1, 27)  # 26JAN = 2026, last Tuesday
            - "BANKNIFTY25NOVFUT" -> datetime(2025, 11, 25)
        
        Args:
            symbol: Option or future symbol
            
        Returns:
            Expiry datetime or None if parsing fails
        """
        try:
            import re
            from calendar import monthrange
            symbol_upper = symbol.upper()
            
            # ✅ FIXED: Pattern to extract expiry date (DDMMM or YYMMM) without strike price
            # For options: NIFTY25NOV26400CE -> match "25NOV" (before strike price)
            # For futures: BANKNIFTY25NOVFUT -> match "25NOV" (before FUT)
            # Pattern: 1-2 digits + 3 letters (month) - this is the expiry date part
            # Stop before strike price (digits) or FUT/CE/PE
            
            # Try to match expiry pattern: DDMMM or YYMMM (e.g., 25NOV, 26JAN)
            match = re.search(r'(\d{1,2})([A-Z]{3})(?=\d|FUT|CE|PE|$)', symbol_upper)
            if match:
                day_str = match.group(1)
                month_str = match.group(2)
                day_num = int(day_str)
                
                month_map = {
                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                }
                
                if month_str not in month_map:
                    return None
                
                month_num = month_map[month_str]
                current_year = datetime.now().year
                
                # ✅ FIXED: Detect if "day" is actually a year indicator (YYMMM format)
                # Heuristic: If day > 31, it's definitely a year
                # If day is 20-31 and in current/future year range, likely a year (e.g., 24, 25, 26, 27)
                if day_num > 31:
                    # Definitely a year (e.g., "26JAN" = 2026)
                    year = 2000 + day_num
                    # Use MarketCalendar to get actual last Tuesday from trading schedule
                    target_month = datetime(year, month_num, 1)
                    expiry_date = self.get_monthly_expiry_for_month(target_month)
                    if expiry_date:
                        return expiry_date
                    # Fallback if calendar unavailable
                    last_day = monthrange(year, month_num)[1]
                    last_date = datetime(year, month_num, last_day)
                    weekday = last_date.weekday()
                    if weekday == 1:  # Tuesday
                        days_back = 0
                    elif weekday == 0:  # Monday
                        days_back = 6
                    else:  # Wednesday-Sunday
                        days_back = weekday - 1
                    expiry_date = last_date - timedelta(days=days_back)
                    return expiry_date
                elif day_num >= 20 and day_num <= 31:
                    # Could be either format - check if it's a valid day for the month
                    try:
                        # Try as day first
                        test_date = datetime(current_year, month_num, day_num)
                        # If date is in the past and we're in the same year, might be old format
                        # But if it's clearly a year indicator (24, 25, 26, 27), treat as year
                        if day_num in [24, 25, 26, 27, 28, 29, 30, 31] and current_year >= 2024:
                            # Likely a year indicator (e.g., "26JAN" = 2026)
                            year = 2000 + day_num
                            # Use MarketCalendar to get actual last Tuesday from trading schedule
                            target_month = datetime(year, month_num, 1)
                            expiry_date = self.get_monthly_expiry_for_month(target_month)
                            if expiry_date:
                                return expiry_date
                            # Fallback if calendar unavailable
                            last_day = monthrange(year, month_num)[1]
                            last_date = datetime(year, month_num, last_day)
                            weekday = last_date.weekday()
                            if weekday == 1:  # Tuesday
                                days_back = 0
                            elif weekday == 0:  # Monday
                                days_back = 6
                            else:  # Wednesday-Sunday
                                days_back = weekday - 1
                            expiry_date = last_date - timedelta(days=days_back)
                            return expiry_date
                        else:
                            # Valid day, use as-is (old format: DDMMM)
                            return test_date
                    except ValueError:
                        # Invalid day for this month, must be a year
                        year = 2000 + day_num
                        # Use MarketCalendar to get actual last Tuesday from trading schedule
                        target_month = datetime(year, month_num, 1)
                        expiry_date = self.get_monthly_expiry_for_month(target_month)
                        if expiry_date:
                            return expiry_date
                        # Fallback if calendar unavailable
                        last_day = monthrange(year, month_num)[1]
                        last_date = datetime(year, month_num, last_day)
                        weekday = last_date.weekday()
                        if weekday == 1:  # Tuesday
                            days_back = 0
                        elif weekday == 0:  # Monday
                            days_back = 6
                        else:  # Wednesday-Sunday
                            days_back = weekday - 1
                        expiry_date = last_date - timedelta(days=days_back)
                        return expiry_date
                else:
                    # Day is 1-19, definitely a day (old format: DDMMM)
                    return datetime(current_year, month_num, day_num)
        except Exception as e:
            logger.debug(f"Failed to parse expiry from symbol {symbol}: {e}")
        return None
    
    def is_weekly_expiry(self, expiry_date: datetime) -> bool:
        """
        Check if expiry is weekly (Tuesday) vs monthly (last Tuesday of month).
        
        ✅ CONSOLIDATED: This method replaces duplicate logic in ExpiryCalculator.
        ✅ UPDATED: India F&O expiry changed to Tuesday as of Sep 2025.
        
        Args:
            expiry_date: Expiry date to check
            
        Returns:
            True if weekly expiry, False if monthly
        """
        # Weekly expiries are Tuesdays (India F&O expiry as of Sep 2025)
        if expiry_date.weekday() != 1:  # Not Tuesday
            return False
        
        # Additional logic to distinguish weekly vs monthly
        # Monthly expiries are typically last Thursday of month
        next_week = expiry_date + timedelta(days=7)
        return next_week.month == expiry_date.month  # If next week is same month, it's weekly
    
    def get_expiry_series(self, expiry_date: datetime) -> str:
        """
        Get expiry series (WEEKLY or MONTHLY).
        
        ✅ CONSOLIDATED: This method replaces duplicate logic in ExpiryCalculator.
        
        ✅ FIX: Only monthly options are used, so always return "MONTHLY"
        
        Args:
            expiry_date: Expiry date
            
        Returns:
            "MONTHLY" (only monthly options are used)
        """
        # ✅ FIX: Only monthly options are used, so always return "MONTHLY"
        return "MONTHLY"
