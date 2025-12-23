"""
Centralized PatternDetector exports
USE THESE, NOT DIRECT IMPORTS!
"""

from .singleton_factory import PatternDetectorSingletonFactory
from .pattern_detector import BasePatternDetector
from .ict import ICTPatternDetector
from .game_theory_engine import (
    IntradayGameTheoryEngine,
    INTRADAY_PRIORITY,
    VALIDATION_SYMBOLS,
)


def get_pattern_detector(config_path=None, caller: str = "unknown"):
    """PUBLIC API: Get the singleton PatternDetector."""
    return PatternDetectorSingletonFactory.get_instance(config_path, caller=caller)


# Alias for backwards compatibility; returns the singleton instance
PatternDetector = get_pattern_detector

__all__ = [
    "PatternDetectorSingletonFactory",
    "get_pattern_detector",
    "PatternDetector",
    "BasePatternDetector",
    "ICTPatternDetector",
    "IntradayGameTheoryEngine",
    "INTRADAY_PRIORITY",
    "VALIDATION_SYMBOLS",
]
