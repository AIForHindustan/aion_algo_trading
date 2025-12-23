"""
Interest Rate Service for Currency Derivatives - Single Source of Truth
=======================================================================

✅ WIRED: Properly integrated with UnifiedGreeksCalculator for currency derivatives (CDS).

The service provides cached access to risk-free rates for multiple currencies
and exposes helpers to fetch domestic/foreign legs for currency pairs.
Used by UnifiedGreeksCalculator for Garman-Kohlhagen pricing of currency options.

Features:
- Properly parses CDS symbols (CDS:USDINR25DECFUT, EURINR25NOV87.5CE, etc.)
- Cached rate lookups with configurable refresh intervals
- Graceful fallbacks to default rates when upstream data unavailable
- Supports multiple currencies (INR, USD, EUR, GBP, JPY)
- Multiple tenors (overnight, 1w, 1m, 3m, 6m, 1y)

Integration:
- UnifiedGreeksCalculator uses get_currency_pair_rates() for CDS segment
- update_all_20day_averages.py stores rates in Redis (currency_domestic_rate, currency_foreign_rate)
- Rates are retrieved from Redis when available, falling back to service defaults

The default implementation uses static fallbacks (kept in sync with Unified
Greeks calculator defaults) but centralizes the lookup so that future data
sources (RBI/NSE feeds, SOFR/ESTR APIs, etc.) can plug in without touching
the pricing code-paths.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple


class InterestRateService:
    """Cached risk-free-rate provider with graceful fallbacks."""

    DEFAULT_RATES: Dict[str, float] = {
        'INR': 0.065,   # 6.5% MIBOR (approx)
        'USD': 0.024,   # 2.4% SOFR (approx)
        'EUR': 0.015,   # 1.5% €STR (approx)
        'GBP': 0.020,   # 2.0% SONIA (approx)
        'JPY': 0.001,   # 0.1% TONAR
    }

    SUPPORTED_TENORS = ('overnight', '1w', '1m', '3m', '6m', '1y')

    def __init__(self, refresh_interval_minutes: int = 60):
        self.logger = logging.getLogger(__name__)
        self.refresh_interval = timedelta(minutes=refresh_interval_minutes)
        self.rate_cache: Dict[Tuple[str, str], float] = {}
        self.last_update: Dict[Tuple[str, str], datetime] = {}

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def get_risk_free_rate(
        self,
        currency: str,
        tenor: str = '1y',
        default: Optional[float] = None
    ) -> float:
        """
        Return the annualized risk-free rate for the given currency/tenor.

        Rates are cached until refresh_interval elapses. If no upstream data
        is available the service falls back to DEFAULT_RATES or the provided
        default value.
        """
        currency_key = (currency or 'INR').upper()
        tenor_key = self._normalize_tenor(tenor)
        cache_key = (currency_key, tenor_key)

        if self._should_refresh(cache_key):
            fetched_rate = self._fetch_rate_from_source(currency_key, tenor_key)
            if fetched_rate is None:
                fetched_rate = self._default_rate(currency_key, default)
            self.rate_cache[cache_key] = fetched_rate
            self.last_update[cache_key] = datetime.utcnow()

        return self.rate_cache.get(cache_key, self._default_rate(currency_key, default))

    def get_currency_pair_rates(
        self,
        pair: str,
        tenor: str = '1y',
        domestic_default: Optional[float] = None,
        foreign_default: Optional[float] = None,
    ) -> Tuple[float, float]:
        """
        Return (domestic_rate, foreign_rate) for a currency pair (e.g., USDINR).
        """
        base_currency, quote_currency = self._parse_currency_pair(pair)
        domestic_rate = self.get_risk_free_rate(
            quote_currency,
            tenor=tenor,
            default=domestic_default
        )
        foreign_rate = self.get_risk_free_rate(
            base_currency,
            tenor=tenor,
            default=foreign_default
        )
        return domestic_rate, foreign_rate

    def set_manual_rate(self, currency: str, rate: float, tenor: str = '1y') -> None:
        """
        Manually override a cached rate (useful for tests or emergency fixes).
        """
        currency_key = (currency or 'INR').upper()
        tenor_key = self._normalize_tenor(tenor)
        cache_key = (currency_key, tenor_key)
        self.rate_cache[cache_key] = float(rate)
        self.last_update[cache_key] = datetime.utcnow()

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _fetch_rate_from_source(self, currency: str, tenor: str) -> Optional[float]:
        """
        Placeholder for future integrations (RBI, SOFR, €STR feeds, etc.).
        Currently returns None so defaults are used, but logging keeps the
        code-path discoverable.
        """
        try:
            # TODO: Integrate with external data sources.
            return None
        except Exception as exc:
            self.logger.debug("Failed to fetch rate for %s/%s: %s", currency, tenor, exc)
            return None

    def _default_rate(self, currency: str, override: Optional[float]) -> float:
        if override is not None and override > 0:
            return float(override)
        return self.DEFAULT_RATES.get(currency.upper(), self.DEFAULT_RATES['INR'])

    def _should_refresh(self, cache_key: Tuple[str, str]) -> bool:
        last = self.last_update.get(cache_key)
        if last is None:
            return True
        return datetime.utcnow() - last >= self.refresh_interval

    def _normalize_tenor(self, tenor: Optional[str]) -> str:
        tenor_key = (tenor or '1y').lower()
        if tenor_key not in self.SUPPORTED_TENORS:
            tenor_key = '1y'
        return tenor_key

    def _parse_currency_pair(self, pair: str) -> Tuple[str, str]:
        """
        Extract (base, quote) currencies from instruments like:
        - USDINR
        - CDS:USDINR25DECFUT
        - EURINR25NOV87.5CE
        """
        symbol = (pair or 'USDINR').upper()
        if ':' in symbol:
            symbol = symbol.split(':', 1)[1]
        # Trim after first non-alpha character to isolate pair
        trimmed = ''.join(ch for ch in symbol if ch.isalpha())
        if len(trimmed) >= 6:
            base = trimmed[:3]
            quote = trimmed[3:6]
            return base, quote
        return trimmed[:3] or 'USD', trimmed[3:6] or 'INR'
