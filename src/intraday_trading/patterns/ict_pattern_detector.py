"""
Backward-compatible ICT pattern detector entry point.
Re-exports ICTPatternDetector from intraday_trading.patterns.ict package.
Updated: 2025-10-27
"""

from .ict import ICTPatternDetector

__all__ = ["ICTPatternDetector"]
