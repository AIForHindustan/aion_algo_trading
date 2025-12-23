"""
Theta Decay Pattern Configuration
=================================

Enhanced configuration for theta decay alert pattern with additional validation conditions.
"""

THETA_DECAY_ENHANCED = {
    'name': 'theta_decay_enhanced',
    'enabled': True,
    'conditions': {
        'all': [
            {'indicator': 'dte_years', 'operator': '<', 'value': 0.1},  # ~37 days
            {'indicator': 'theta', 'operator': '<', 'value': -0.05},
            {'indicator': 'volume_ratio', 'operator': '>', 'value': 0.1},  # Minimal activity
            {'indicator': 'implied_volatility', 'operator': '>', 'value': 0.15},  # Some IV
        ]
    },
    'alert_priority': 'medium',
    'confidence_boost': {
        'dte_days': {
            '< 7': 0.1,      # Weekly options: +10% confidence
            '< 14': 0.05,    # Bi-weekly: +5% confidence
            '< 21': 0.02     # Three weeks: +2% confidence
        },
        'theta_severity': {
            '< -0.1': 0.1,   # Very high decay: +10% confidence
            '< -0.08': 0.05, # High decay: +5% confidence
            '< -0.05': 0.0   # Standard threshold: no boost
        },
        'volume_ratio': {
            '> 1.5': 0.05,  # High volume: +5% confidence
            '> 1.0': 0.02   # Normal volume: +2% confidence
        }
    }
}

# Base thresholds (can be overridden by pattern registry)
THETA_DECAY_THRESHOLDS = {
    'dte_threshold_days': 37,      # Less than 37 days = short-dated
    'dte_threshold_years': 0.1,    # Equivalent in years
    'theta_threshold': -0.05,       # More negative = higher decay
    'min_volume_ratio': 0.1,       # Minimal activity required
    'min_implied_volatility': 0.15, # Some IV required (15%)
    'confidence_base': 0.8,         # Base confidence
    'confidence_max': 0.95         # Maximum confidence
}

