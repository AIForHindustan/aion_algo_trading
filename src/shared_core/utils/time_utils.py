"""
Time utilities shared across trading services.
Centralizes Indian market timestamp parsing so both scanner and shared_core can
depend on the same implementation.
"""

from datetime import datetime, timedelta
from typing import Iterable, Optional
import pytz
import numpy as np

# IST timezone
IST = pytz.timezone("Asia/Kolkata")


class IndianMarketTimeParser:
    """High-performance timestamp parser for Indian market data."""

    def parse_to_epoch_ms(self, timestamp_str: str) -> int:
        """Convert to epoch milliseconds."""
        dt_obj = self._parse_flexible(timestamp_str)
        return int(dt_obj.timestamp() * 1000)

    def parse_to_numpy_datetime64(self, timestamp_str: str) -> np.datetime64:
        dt_obj = self._parse_flexible(timestamp_str)
        return np.datetime64(dt_obj)

    def parse_many_to_epoch_ms(self, timestamps: Iterable[str]) -> np.ndarray:
        return np.array([self.parse_to_epoch_ms(ts) for ts in timestamps], dtype="int64")

    def _parse_flexible(self, timestamp_str: str) -> datetime:
        ts_clean = (timestamp_str or "").strip()

        if not ts_clean:
            raise ValueError("Empty timestamp string")

        if "-" in ts_clean and ":" in ts_clean:
            return self._parse_iso_format(ts_clean)
        if "/" in ts_clean:
            return self._parse_slash_format(ts_clean)

        return datetime.fromisoformat(ts_clean.replace(" ", "T"))

    def _parse_iso_format(self, ts_str: str) -> datetime:
        try:
            date_part, time_part = ts_str.split(" ", 1)
            year, month, day = map(int, date_part.split("-"))

            time_parts = time_part.split(":")
            hour = int(time_parts[0])
            minute = int(time_parts[1]) if len(time_parts) > 1 else 0
            if len(time_parts) > 2:
                second_parts = time_parts[2].split(".")
                second = int(second_parts[0])
                microsecond = int(second_parts[1][:6].ljust(6, "0")) if len(second_parts) > 1 else 0
            else:
                second = 0
                microsecond = 0

            return datetime(year, month, day, hour, minute, second, microsecond)
        except (ValueError, IndexError) as e:
            raise ValueError(f"Invalid ISO format: {ts_str}") from e

    def _parse_slash_format(self, ts_str: str) -> datetime:
        try:
            # Handle DD/MM/YYYY HH:MM:SS format
            date_part, time_part = ts_str.split(" ", 1)
            day, month, year = map(int, date_part.split("/"))

            time_parts = time_part.split(":")
            hour = int(time_parts[0])
            minute = int(time_parts[1]) if len(time_parts) > 1 else 0
            second = int(time_parts[2]) if len(time_parts) > 2 else 0

            return datetime(year, month, day, hour, minute, second)
        except (ValueError, IndexError) as e:
            raise ValueError(f"Invalid slash format: {ts_str}") from e


# Global instance for easy access
INDIAN_TIME_PARSER = IndianMarketTimeParser()


def get_current_ist_time() -> datetime:
    """Get current time in IST timezone"""
    return datetime.now(IST)


def get_current_ist_timestamp() -> str:
    """Get current timestamp in IST as ISO string"""
    return get_current_ist_time().isoformat()


def parse_ist_timestamp(timestamp_str: str) -> datetime:
    """Parse timestamp string to IST datetime"""
    return INDIAN_TIME_PARSER._parse_flexible(timestamp_str)


def get_market_session_key() -> str:
    """Generate a unique key for the current market session (date + session type)"""
    now = get_current_ist_time()
    date_str = now.strftime("%Y-%m-%d")

    # Determine session type based on time
    hour = now.hour
    if 9 <= hour < 15:
        session_type = "market_hours"
    elif 8 <= hour < 9:
        session_type = "pre_market"
    elif 15 <= hour < 16:
        session_type = "post_market"
    else:
        session_type = "closed"

    return f"{date_str}_{session_type}"


def is_market_hours() -> bool:
    """Check if current time is during market hours (9:15 AM - 3:30 PM IST)"""
    now = get_current_ist_time()
    return 9 <= now.hour < 15 or (now.hour == 9 and now.minute >= 15)


def is_premarket_hours() -> bool:
    """Check if current time is during premarket hours (8:59 AM - 9:15 AM IST)"""
    now = get_current_ist_time()
    return (now.hour == 8 and now.minute >= 59) or (now.hour == 9 and now.minute < 15)


def is_postmarket_hours() -> bool:
    """Check if current time is during postmarket hours (3:30 PM - 4:00 PM IST)"""
    now = get_current_ist_time()
    return (now.hour == 15 and now.minute >= 30) or (now.hour == 16 and now.minute == 0)


def get_market_session_start_end() -> tuple[datetime, datetime]:
    """Get market session start and end times for current day"""
    now = get_current_ist_time()
    date = now.date()

    start_time = datetime.combine(date, datetime.min.time().replace(hour=9, minute=15))
    end_time = datetime.combine(date, datetime.min.time().replace(hour=15, minute=30))

    return start_time, end_time


def format_timestamp_for_redis(timestamp: datetime) -> str:
    """Format timestamp for Redis storage"""
    return timestamp.isoformat()


def parse_redis_timestamp(timestamp_str: str) -> datetime:
    """Parse timestamp from Redis storage"""
    return datetime.fromisoformat(timestamp_str)

