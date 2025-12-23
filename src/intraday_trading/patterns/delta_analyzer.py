"""
Delta Analyzer - Tier 1 Priority
Analyzes option chain for delta-based reversal signals using standardized field names.
Integrates with PatternDetector and AlertManager for end-to-end alerting.
"""
import logging
import time
from typing import Dict, List, Optional
import numpy as np
from shared_core.redis_clients.unified_data_storage import get_unified_storage

try:
    import pandas_market_calendars as mcal
    _PMC_AVAILABLE = True
    _NSE_CAL = mcal.get_calendar("NSE")
except Exception:
    _PMC_AVAILABLE = False
    _NSE_CAL = None

logger = logging.getLogger(__name__)

class DeltaAnalyzer:
    """
    ✅ TIER 1 PRIORITY: Delta-based option chain analyzer for reversal signals.
    
    Analyzes option chain data to detect potential reversals based on:
    - Net delta (weighted by open interest)
    - Put-Call Ratio (PCR)
    - Delta-Price divergence
    - Extreme delta values
    
    Uses standardized field names from config/optimized_field_mapping.yaml.
    """
    
    def __init__(self, window: int = 10):
        """
        Initialize Delta Analyzer.
        
        Args:
            window: Number of historical data points to maintain (default: 10)
        """
        self.window = window
        self.delta_history = {}
        self.oi_history = {}
        self.logger = logger
    
    def _fetch_fresh_greeks_from_redis(self, symbol: str, greek_names: list = None) -> dict:
        """
        ✅ CENTRALIZED: Fetch fresh Greeks from Redis using UnifiedDataStorage.
        
        Fetches fresh Greeks from Redis for a symbol with DTE-aware delta adjustments
        for near-expiry contracts. Uses UnifiedDataStorage for consistent Redis operations.
        
        Args:
            symbol: Trading symbol (e.g., 'BANKNIFTY25NOV59100CE')
            greek_names: List of Greek names to fetch (default: ['delta', 'gamma', 'theta', 'vega', 'rho'])
        
        Returns:
            Dict with Greek values (e.g., {'delta': 0.5, 'gamma': 0.02, 'dte_years': 0.02, ...})
        """
        if greek_names is None:
            greek_names = ['delta', 'gamma', 'theta', 'vega', 'rho', 'dte_years', 'trading_dte', 'expiry_series']
        
        greeks = {}
        
        try:
            if not hasattr(self, 'unified_storage') or self.unified_storage is None:
                self.unified_storage = get_unified_storage()
            
            # Get current expiry information for DTE calculations
            current_expiry = self._get_current_expiry(symbol)
            dte = None
            if current_expiry:
                dte = self._calculate_dte(current_expiry)
                if dte <= 0:
                    self.logger.warning(f"⚠️ [DELTA_ANALYZER] Expired contract: {symbol}, DTE: {dte}")
                    return greeks
                
                # Add DTE to Greek names if not present
                if 'dte_years' not in greek_names:
                    greek_names.append('dte_years')
                if 'trading_dte' not in greek_names:
                    greek_names.append('trading_dte')
            else:
                self.logger.warning(f"⚠️ [DELTA_ANALYZER] No expiry found for {symbol}")
            
            # Fetch all Greeks from UnifiedDataStorage
            fetched_greeks = self.unified_storage.get_greeks(symbol)
            
            # Process fetched Greeks and handle DTE calculations
            for greek in greek_names:
                # Handle DTE calculations separately (not stored in Redis)
                if greek == 'dte_years':
                    if dte is not None:
                        greeks['dte_years'] = dte / 365.0
                    continue
                elif greek == 'trading_dte':
                    if dte is not None:
                        greeks['trading_dte'] = dte
                    continue
                
                # Get Greek from fetched data
                if greek in fetched_greeks and fetched_greeks[greek] is not None:
                    try:
                        # Try to convert to float for numeric Greeks
                        if greek in ['delta', 'gamma', 'theta', 'vega', 'rho', 'dte_years']:
                            greeks[greek] = float(fetched_greeks[greek])
                        elif greek == 'trading_dte':
                            greeks[greek] = int(fetched_greeks[greek])
                        else:
                            greeks[greek] = fetched_greeks[greek]
                    except (ValueError, TypeError):
                        greeks[greek] = fetched_greeks[greek]
            
            # ✅ CRITICAL FIX: Adjust delta based on DTE
            if 'delta' in greeks and dte is not None and dte < 3:
                # Near expiry: delta becomes more binary (approaches 0 or 1)
                current_delta = greeks['delta']
                if dte == 1:
                    # On expiry day, delta approaches 0 for OTM, 1 for ITM
                    if abs(current_delta) < 0.3:
                        greeks['delta'] = current_delta * 0.7  # Reduce OTM delta sensitivity
                    else:
                        greeks['delta'] = current_delta * 1.2  # Increase ITM delta sensitivity
                elif dte == 2:
                    greeks['delta'] = current_delta * 0.9  # Slight adjustment
                
                self.logger.debug(f"✅ [DELTA_FIX] {symbol} DTE:{dte} Delta:{greeks.get('delta', 'N/A')}")
        
        except Exception as e:
            self.logger.debug(f"⚠️ [DELTA_ANALYZER] Failed to fetch Greeks from UnifiedDataStorage for {symbol}: {e}")
        
        return greeks
    
    def analyze_option_chain(self, symbol: str, option_chain: dict, underlying_price: float) -> dict:
        """CORRECTED: Proper delta and PCR calculation"""

        try:
            total_call_delta = 0.0
            total_put_delta = 0.0
            total_call_oi = 0
            total_put_oi = 0
            
            # Get correct lot size
            lot_size = 75 if 'NIFTY' in symbol else 35
            
            # Process Call options (CE) - POSITIVE DELTA
            for strike, data in option_chain.get('CE', {}).items():
                delta_val = data.get('delta', 0)
                oi_val = data.get('oi', 0) or data.get('open_interest', 0)
                
                if delta_val and oi_val:
                    # ✅ Calls have POSITIVE delta exposure
                    total_call_delta += abs(float(delta_val)) * int(oi_val) * lot_size
                    total_call_oi += int(oi_val)
            
            # Process Put options (PE) - NEGATIVE DELTA  
            for strike, data in option_chain.get('PE', {}).items():
                delta_val = data.get('delta', 0)
                oi_val = data.get('oi', 0) or data.get('open_interest', 0)
                
                if delta_val and oi_val:
                    # ✅ Puts have NEGATIVE delta exposure (delta_val is already negative)
                    total_put_delta += float(delta_val) * int(oi_val) * lot_size
                    total_put_oi += int(oi_val)
            
            # ✅ NET DELTA: Calls (positive) + Puts (negative)
            net_delta = total_call_delta + total_put_delta
            
            # ✅ PCR: Put OI / Call OI
            pcr = total_put_oi / total_call_oi if total_call_oi > 0 else 0
            
            # ✅ CORRECT PCR INTERPRETATION (Indian markets):
            # High PCR (>1.5) = More puts = Bullish sentiment
            # Low PCR (<0.7) = More calls = Bearish sentiment
            reversal_signal = self._analyze_delta_reversal_corrected(
                symbol, net_delta, pcr, underlying_price, total_call_oi, total_put_oi
            )
            
            return {
                'symbol': symbol,
                'net_delta': net_delta,
                'put_call_ratio': pcr,
                'total_call_oi': total_call_oi,
                'total_put_oi': total_put_oi,
                'reversal_alert': reversal_signal['reversal_alert'],
                'confidence': reversal_signal['confidence'],
                'direction': reversal_signal['direction'],
                'reason': reversal_signal['reason'],
                'timestamp': time.time()
            }
        except Exception as e:
            self.logger.debug(f"⚠️ [DELTA_ANALYZER] Failed to analyze option chain for {symbol}: {e}")
            return {
                'symbol': symbol,
                'net_delta': 0,
                'put_call_ratio': 0,
                'total_call_oi': 0,
                'total_put_oi': 0,
                'reversal_alert': False,
                'confidence': 0,
                'direction': "none",
                'reason': f"Error analyzing option chain: {e}",
                'timestamp': time.time()
            }   
        
    def _analyze_delta_reversal_corrected(
        self,
        symbol: str,
        net_delta: float,
        pcr: float,
        underlying_price: float,
        call_oi: int,
        put_oi: int,
    ) -> dict:
        """
        CORRECTED: Proper PCR and delta confirmation logic for Indian indices.
        PCR > 1.5 with negative net delta = bullish reversal risk.
        PCR < 0.7 with positive net delta = bearish reversal risk.
        """
        bullish_pcr = pcr > 1.5
        bearish_pcr = pcr < 0.7

        pcr_bullish_confirmed = bullish_pcr and net_delta < 0
        pcr_bearish_confirmed = bearish_pcr and net_delta > 0

        confidence = min(0.8, abs(net_delta) / 1_000_000) if net_delta else 0.0

        if pcr_bullish_confirmed:
            return {
                "reversal_alert": True,
                "confidence": confidence,
                "direction": "bullish",
                "reason": f"High PCR ({pcr:.2f}) with negative net delta ({net_delta:,.0f})",
            }
        if pcr_bearish_confirmed:
            return {
                "reversal_alert": True,
                "confidence": confidence,
                "direction": "bearish",
                "reason": f"Low PCR ({pcr:.2f}) with positive net delta ({net_delta:,.0f})",
            }

        return {
            "reversal_alert": False,
            "confidence": 0,
            "direction": "none",
            "reason": "No confirmed signal",
        }
        
    def _analyze_delta_reversal(self, symbol: str, net_delta: float, pcr: float, underlying_price: float) -> dict:
        """FIXED: Add DTE-aware reversal detection"""
        
        # Get DTE for current symbol
        dte = self._get_current_dte(symbol)
        
        # Initialize history
        if symbol not in self.delta_history:
            self.delta_history[symbol] = {
                'net_deltas': [],
                'pcrs': [],
                'prices': [],
                'timestamps': [],
                'dtes': []
            }
        
        hist = self.delta_history[symbol]
        hist['net_deltas'].append(net_delta)
        hist['pcrs'].append(pcr)
        hist['prices'].append(underlying_price)
        hist['timestamps'].append(time.time())
        hist['dtes'].append(dte)
        
        # Keep only recent history
        if len(hist['net_deltas']) > self.window:
            for key in ['net_deltas', 'pcrs', 'prices', 'timestamps', 'dtes']:
                hist[key] = hist[key][-self.window:]
        
        # Need minimum data for analysis
        if len(hist['net_deltas']) < 5:
            return {"reversal_alert": False, "confidence": 0, "direction": "none", "reason": "Insufficient data"}
        
        # ✅ DTE-AWARE SIGNAL ADJUSTMENT
        current_dte = hist['dtes'][-1]
        
        # Adjust sensitivity based on DTE
        if current_dte > 10:  # Far expiry - need stronger signals
            pcr_threshold_bullish = 0.6  # More sensitive for far expiry
            pcr_threshold_bearish = 1.4
            trend_threshold = 0.08
        elif current_dte <= 3:  # Near expiry - more sensitive to changes
            pcr_threshold_bullish = 0.8  # Less sensitive for near expiry
            pcr_threshold_bearish = 1.2  
            trend_threshold = 0.15  # Higher trend threshold for noise
        else:  # Medium expiry
            pcr_threshold_bullish = 0.7
            pcr_threshold_bearish = 1.3
            trend_threshold = 0.1
        
        # Calculate signals with DTE-adjusted thresholds
        current_delta = hist['net_deltas'][-1]
        current_pcr = hist['pcrs'][-1]
        current_price = hist['prices'][-1]
        
        # Signal 1: Extreme PCR values (DTE-adjusted)
        pcr_extreme = current_pcr > pcr_threshold_bearish or current_pcr < pcr_threshold_bullish
        
        # Signal 2: Delta-Price divergence
        price_trend = self._calculate_trend(hist['prices'][-5:])
        delta_trend = self._calculate_trend(hist['net_deltas'][-5:])
        pcr_trend = self._calculate_trend(hist['pcrs'][-5:])
        
        # High PCR + Rising prices = Potential bearish reversal
        bearish_signal = (current_pcr > pcr_threshold_bearish and 
                         abs(price_trend) > trend_threshold and 
                         pcr_trend > 0.1)
        
        # Low PCR + Falling prices = Potential bullish reversal  
        bullish_signal = (current_pcr < pcr_threshold_bullish and 
                         abs(price_trend) > trend_threshold and 
                         pcr_trend < -0.1)
        
        # Extreme delta values (DTE-adjusted)
        delta_magnitude = abs(current_delta)
        extreme_delta = delta_magnitude > np.percentile([abs(x) for x in hist['net_deltas']], 80)
        
        # Near-expiry special handling
        if current_dte <= 2:
            # Near expiry: be more cautious with signals
            confidence_multiplier = 0.7
            extreme_delta = delta_magnitude > np.percentile([abs(x) for x in hist['net_deltas']], 90)
        else:
            confidence_multiplier = 1.0
        
        # Determine final signal
        if bearish_signal and extreme_delta:
            return {
                "reversal_alert": True, 
                "confidence": 0.8 * confidence_multiplier, 
                "direction": "bearish",
                "reason": f"High PCR ({current_pcr:.2f}) with DTE:{current_dte} days"
            }
        elif bullish_signal and extreme_delta:
            return {
                "reversal_alert": True,
                "confidence": 0.8 * confidence_multiplier,
                "direction": "bullish", 
                "reason": f"Low PCR ({current_pcr:.2f}) with DTE:{current_dte} days"
            }
        
        return {"reversal_alert": False, "confidence": 0, "direction": "none", "reason": f"No clear signal (DTE:{current_dte})"}
    
    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate normalized trend of values"""
        if len(values) < 2:
            return 0
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]
        return slope / np.mean(values) if np.mean(values) != 0 else 0
    
    def _get_current_expiry(self, symbol: str) -> str:
        """Extract expiry from symbol or get from Redis"""
        try:
            # Try to parse expiry from symbol (format: BANKNIFTY25NOV59100CE)
            import re
            match = re.search(r'(\d{2}[A-Z]{3})', symbol)
            if match:
                return match.group(1)
            
            # Fallback: get from Redis via UnifiedDataStorage
            storage = get_unified_storage()
            indicators = storage.get_indicators(symbol, indicator_names=['expiry_series'])
            expiry = indicators.get('expiry_series')
            if expiry:
                return str(expiry)
            
            return ""
            
        except Exception as e:
            self.logger.debug(f"Failed to get expiry for {symbol}: {e}")
            return ""
    
    def _calculate_dte(self, expiry_str: str) -> int:
        """Calculate days to expiry from string like '25NOV'"""
        try:
            from datetime import datetime

            if not expiry_str:
                return 7  # Default fallback

            # Parse expiry string (assuming format: DDMMM, e.g., '25NOV')
            # Try different formats
            expiry_date = None
            now = datetime.now()
            current_year = now.year

            # Format 1: DDMMM (e.g., '25NOV')
            try:
                expiry_date = datetime.strptime(expiry_str, '%d%b')
                expiry_date = expiry_date.replace(year=current_year)
            except ValueError:
                # Format 2: DDMMMYY (e.g., '25NOV25')
                try:
                    expiry_date = datetime.strptime(expiry_str, '%d%b%y')
                except ValueError:
                    # Format 3: DD-MMM-YYYY (e.g., '25-NOV-2025')
                    try:
                        expiry_date = datetime.strptime(expiry_str, '%d-%b-%Y')
                    except ValueError:
                        self.logger.debug(f"Could not parse expiry format: {expiry_str}")
                        return 7  # Default fallback

            if expiry_date:
                # Handle year rollover
                if expiry_date < now:
                    expiry_date = expiry_date.replace(year=current_year + 1)

                if _PMC_AVAILABLE and _NSE_CAL:
                    try:
                        schedule = _NSE_CAL.schedule(
                            start_date=now.date(),
                            end_date=expiry_date.date(),
                        )
                        trading_days = len(schedule.index)
                        if trading_days:
                            return max(0, trading_days)
                    except Exception as cal_error:
                        self.logger.debug(f"PMC schedule lookup failed: {cal_error}")

                dte = (expiry_date - now).days
                return max(0, dte)

            return 7  # Default fallback

        except Exception as e:
            self.logger.debug(f"DTE calculation failed for {expiry_str}: {e}")
            return 7  # Default fallback
    
    def _get_current_dte(self, symbol: str) -> int:
        """Get current days to expiry for symbol"""
        try:
            greeks = self._fetch_fresh_greeks_from_redis(symbol, ['trading_dte'])
            dte = greeks.get('trading_dte')
            if dte is not None:
                return int(dte)
            
            # Fallback: calculate from expiry string
            expiry = self._get_current_expiry(symbol)
            if expiry:
                return self._calculate_dte(expiry)
            
            return 7  # Default fallback
            
        except Exception as e:
            self.logger.debug(f"Failed to get DTE for {symbol}: {e}")
            return 7  # Default fallback
