"""
Straddle Strategy Patterns

Contains strategy-based pattern detectors that require their own tick listeners
and state management.
"""

from .kow_signal_straddle import KowSignalStraddleStrategy

__all__ = ['KowSignalStraddleStrategy']

