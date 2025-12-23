#!/opt/homebrew/bin/python3.13
"""
ICT Iron Condor Strategy for Nifty & Bank Nifty
Optimized for range-bound markets with institutional flow detection

✅ ALERT PAYLOAD INTEGRATION:
- Uses UnifiedAlertBuilder.build_iron_condor_algo_payload() for algo-ready payloads
- Creates complete algo order payload with 4-leg structure (Short Put, Long Put, Short Call, Long Call)
- Stores payload in Redis DB1 with key: alert_payload:{symbol}:{pattern}:{timestamp_ms}
- Publishes to alerts:stream for real-time processing
- All alerts use standard human-readable format from HumanReadableAlertTemplates
"""

from __future__ import annotations
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
import numpy as np

logger = logging.getLogger(__name__)

class ICTIronCondor:
    """
    ICT-Inspired Iron Condor Strategy for Nifty & Bank Nifty
    
    Key Features:
    - Uses FVG (Fair Value Gap) for strike selection
    - ICT Kill Zone timing integration
    - Volume-profile based position sizing
    - Institutional flow detection
    - Dynamic adjustment based on market regime
    """
    
    def __init__(self, redis_client=None, require_kill_zone: bool = False):
        self.redis_client = redis_client
        self.logger = logger
        # Toggle to require ICT kill zone for signal generation (disabled by default for testing)
        self.require_kill_zone = require_kill_zone
        
        # ✅ Initialize unified_alert_builder for algo-ready payloads
        try:
            from alerts.unified_alert_builder import get_unified_alert_builder
            self.alert_builder = get_unified_alert_builder(redis_client=redis_client)
        except ImportError as e:
            self.logger.warning(f"⚠️ UnifiedAlertBuilder not available: {e}")
            self.alert_builder = None
        
        # ✅ Position state tracking for alert_id and strategy_id linking
        # Format: {symbol: {'entry_alert_id': str, 'strategy_id': str, 'entry_time': datetime, ...}}
        self.position_states = {}
        
        # Strategy parameters for Nifty (Lot Size: 75)
        self.NIFTY_PARAMS = {
            'lot_size': 75,
            'min_credit': 1.0,  # Minimum premium to collect
            'max_risk_per_trade': 15000,  # Max loss per trade
            'strike_distance_otm': 200,  # Points OTM for short strikes
            'spread_width': 100,  # Width between short and long strikes
            'min_dte': 1,  # Minimum days to expiry
            'max_dte': 7,  # Maximum days to expiry (weekly options)
        }
        
        # Strategy parameters for Bank Nifty (Lot Size: 35)
        self.BANKNIFTY_PARAMS = {
            'lot_size': 35,
            'min_credit': 1.5,
            'max_risk_per_trade': 12000,
            'strike_distance_otm': 400,
            'spread_width': 200,
            'min_dte': 1,
            'max_dte': 7,
        }
        
        # ICT Kill Zones (Indian Market Times)
        self.KILL_ZONES = {
            'asian_range': {"start": "09:15", "end": "10:30"},
            'london_kill_zone': {"start": "11:30", "end": "14:00"},
            'new_york_kill_zone': {"start": "14:30", "end": "16:00"},
        }

    def is_in_kill_zone(self) -> bool:
        """
        ✅ ENHANCED: Check if current time is in ICT Kill Zone with volume confirmation.
        
        Uses EnhancedICTKillzoneDetector with volume-time awareness.
        """
        try:
            # ✅ Use enhanced killzone detector with volume confirmation
            from .killzone import EnhancedICTKillzoneDetector
            
            if not hasattr(self, 'killzone_detector') or not self.killzone_detector:
                self.killzone_detector = EnhancedICTKillzoneDetector(redis_client=self.redis_client)
            
            # Get current price and volume for volume confirmation
            symbol = self.symbol if hasattr(self, 'symbol') else "NIFTY"
            current_price = self._get_current_price(self.symbol if hasattr(self, 'symbol') else "NIFTY")
            current_volume = self._get_current_volume()
            price_change_pct = self._get_recent_price_change()
            
            # Get volume-confirmed killzone
            killzone = self.killzone_detector.get_volume_confirmed_killzone(
                symbol=symbol,
                current_volume=current_volume,
                current_price=current_price,
                price_change_pct=price_change_pct
            )
            
            if killzone:
                self.logger.info(
                    f"✅ Killzone confirmed: {killzone['zone']} "
                    f"(volume_ratio={killzone.get('volume_ratio', 0):.2f})"
                )
                return True
            else:
                # Fallback: check basic killzone without volume confirmation
                basic_killzone = self.killzone_detector.get_current_killzone()
                if basic_killzone:
                    self.logger.debug(
                        f"⏸️ Killzone {basic_killzone['zone']} detected but not volume-confirmed"
                    )
                return False
                
        except Exception as e:
            self.logger.debug(f"ICT Killzone detector not available, using basic check: {e}")
            # Fallback: basic kill zone check
            current_time = datetime.now().time()
            
            for zone_name, zone_times in self.KILL_ZONES.items():
                start_time = datetime.strptime(zone_times["start"], "%H:%M").time()
                end_time = datetime.strptime(zone_times["end"], "%H:%M").time()
                
                if start_time <= current_time <= end_time:
                    return True
            return False
    
    def _get_current_volume(self) -> float:
        """Get current bucket incremental volume for killzone confirmation."""
        try:
            if not self.redis_client:
                return 0.0
            
            symbol = self.symbol if hasattr(self, 'symbol') else "NIFTY"
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            
            # Get latest bucket volume
            volume_key = DatabaseAwareKeyBuilder.live_bucket_history(symbol, "1min")
            volume_data = self.redis_client.hgetall(volume_key)
            
            if volume_data:
                return float(volume_data.get(b'volume', volume_data.get('volume', 0)) or 0)
            return 0.0
        except Exception as e:
            self.logger.debug(f"Failed to get current volume: {e}")
            return 0.0
    
    def _get_recent_price_change(self) -> float:
        """Get recent price change percentage for killzone confirmation."""
        try:
            symbol = self.symbol if hasattr(self, 'symbol') else "NIFTY"
            
            # ✅ FIXED: Use UnifiedDataStorage (HGETALL on ind:{symbol} hash)
            from shared_core.redis_clients.unified_data_storage import get_unified_storage
            storage = get_unified_storage()
            indicators = storage.get_indicators(symbol, ['price_change_pct', 'price_change'])
            
            if indicators:
                price_change = indicators.get('price_change_pct') or indicators.get('price_change')
                if price_change:
                    return float(price_change)
            return 0.0
        except Exception as e:
            self.logger.debug(f"Failed to get price change: {e}")
            return 0.0

    def detect_range_bound_market(self, symbol: str, lookback_days: int = 5) -> Dict:
        """
        Detect if market is range-bound using recent price action
        """
        try:
            # Get recent OHLC data (you'll need to implement this based on your data source)
            ohlc_data = self._get_recent_ohlc(symbol, lookback_days)
            if not ohlc_data:
                return {"is_range_bound": False, "support": 0, "resistance": 0, "confidence": 0}
            
            highs = [float(candle['high']) for candle in ohlc_data]
            lows = [float(candle['low']) for candle in ohlc_data]
            
            current_high = max(highs)
            current_low = min(lows)
            range_width = (current_high - current_low) / current_low * 100
            
            # Range-bound if width is less than 2% for Nifty, 3% for Bank Nifty
            max_range_width = 2.0 if symbol == "NIFTY" else 3.0
            is_range_bound = range_width <= max_range_width
            
            return {
                "is_range_bound": is_range_bound,
                "support": current_low,
                "resistance": current_high,
                "range_width_pct": range_width,
                "confidence": max(0, 1 - (range_width / max_range_width))
            }
            
        except Exception as e:
            self.logger.error(f"Range detection failed for {symbol}: {e}")
            return {"is_range_bound": False, "support": 0, "resistance": 0, "confidence": 0}

    def calculate_fvg_levels(self, candles: List[Dict]) -> Dict[str, List[float]]:
        """
        Calculate Fair Value Gaps for strike selection
        Uses existing ICT FVG detector if available, otherwise falls back to basic calculation
        """
        try:
            # Try to use existing ICT FVG detector
            from .fvg import EnhancedICTFVGDetector
            fvg_detector = EnhancedICTFVGDetector()
            fvgs = fvg_detector.detect_volume_confirmed_fvg(candles) if hasattr(fvg_detector, 'detect_volume_confirmed_fvg') else []
            
            bullish_fvgs = []
            bearish_fvgs = []
            
            for fvg in fvgs:
                if isinstance(fvg, dict):
                    fvg_type = fvg.get('type', '')
                    gap_low = fvg.get('gap_low', fvg.get('low', 0))
                    gap_high = fvg.get('gap_high', fvg.get('high', 0))
                    if fvg_type == 'bullish' and gap_low > 0 and gap_high > 0:
                        bullish_fvgs.append((gap_low, gap_high))
                    elif fvg_type == 'bearish' and gap_low > 0 and gap_high > 0:
                        bearish_fvgs.append((gap_low, gap_high))
            
            if bullish_fvgs or bearish_fvgs:
                return {"bullish": bullish_fvgs, "bearish": bearish_fvgs}
        except Exception as e:
            self.logger.debug(f"ICT FVG detector not available, using basic calculation: {e}")
        
        # Fallback: basic FVG calculation
        bullish_fvgs = []
        bearish_fvgs = []
        
        if len(candles) < 3:
            return {"bullish": bullish_fvgs, "bearish": bearish_fvgs}
        
        for i in range(2, len(candles)):
            try:
                prior = candles[i-2]
                current = candles[i]
                
                prior_high = float(prior.get('high', 0))
                current_low = float(current.get('low', 0))
                prior_low = float(prior.get('low', 0))
                current_high = float(current.get('high', 0))
                
                # Bullish FVG
                if current_low > prior_high and prior_high > 0:
                    bullish_fvgs.append((prior_high, current_low))
                
                # Bearish FVG
                elif current_high < prior_low and prior_low > 0:
                    bearish_fvgs.append((current_high, prior_low))
                    
            except Exception:
                continue
        
        return {"bullish": bullish_fvgs, "bearish": bearish_fvgs}

    def select_iron_condor_strikes(self, symbol: str, current_price: float, 
                                 fvg_levels: Dict, range_analysis: Dict) -> Dict[str, int]:
        """
        Select optimal strikes for Iron Condor using FVG levels
        """
        params = self.NIFTY_PARAMS if symbol == "NIFTY" else self.BANKNIFTY_PARAMS
        
        # Use FVG levels for strike selection if available
        if fvg_levels["bullish"]:
            # Use the most recent bullish FVG for put strike selection
            latest_bullish_fvg = fvg_levels["bullish"][-1]
            put_strike = int(round(latest_bullish_fvg[0] / 50) * 50)  # Round to nearest 50
        else:
            # Fallback: use support level from range analysis
            put_strike = int(round((range_analysis["support"] - params['strike_distance_otm']) / 50) * 50)
        
        if fvg_levels["bearish"]:
            # Use the most recent bearish FVG for call strike selection
            latest_bearish_fvg = fvg_levels["bearish"][-1]
            call_strike = int(round(latest_bearish_fvg[1] / 50) * 50)  # Round to nearest 50
        else:
            # Fallback: use resistance level from range analysis
            call_strike = int(round((range_analysis["resistance"] + params['strike_distance_otm']) / 50) * 50)
        
        # Ensure strikes are reasonable distance from current price
        min_distance = params['strike_distance_otm'] * 0.5
        if abs(put_strike - current_price) < min_distance:
            put_strike = int(current_price - min_distance)
        if abs(call_strike - current_price) < min_distance:
            call_strike = int(current_price + min_distance)
        
        # Calculate long protective strikes
        long_put_strike = put_strike - params['spread_width']
        long_call_strike = call_strike + params['spread_width']
        
        return {
            "short_put": put_strike,
            "long_put": long_put_strike,
            "short_call": call_strike,
            "long_call": long_call_strike,
            "current_price": current_price
        }

    def calculate_position_size(self, symbol: str, max_loss_per_condor: float) -> int:
        """
        Calculate number of lots based on risk management
        """
        params = self.NIFTY_PARAMS if symbol == "NIFTY" else self.BANKNIFTY_PARAMS
        
        max_lots = params['max_risk_per_trade'] / max_loss_per_condor
        return max(1, min(5, int(max_lots)))  # Limit to 5 lots max

    def check_volatility_regime(self, symbol: str) -> Dict[str, bool]:
        """
        Check if volatility regime is suitable for Iron Condor
        """
        try:
            # Get India VIX or calculate historical volatility
            # You'll need to implement this based on your data source
            current_vix = self._get_current_vix()
            historical_vol = self._calculate_historical_vol(symbol, 20)
            
            # Iron Condor works best in moderate to high volatility
            is_vol_suitable = current_vix > 12 and historical_vol > 8
            
            return {
                "is_suitable": is_vol_suitable,
                "current_vix": current_vix,
                "historical_vol": historical_vol,
                "reason": "Volatility too low" if not is_vol_suitable else "Volatility suitable"
            }
            
        except Exception as e:
            self.logger.error(f"Volatility check failed: {e}")
            return {"is_suitable": False, "reason": f"Error: {str(e)}"}

    def generate_iron_condor_signal(self, symbol: str) -> Dict:
        """
        Generate complete Iron Condor trade signal
        """
        try:
            # Get current market data
            current_price = self._get_current_price(symbol)
            if not current_price:
                return self._create_no_signal("Price data unavailable")
            
            # Check market conditions
            range_analysis = self.detect_range_bound_market(symbol)
            if not range_analysis["is_range_bound"]:
                return self._create_no_signal("Market not range-bound")
            
            volatility_regime = self.check_volatility_regime(symbol)
            if not volatility_regime["is_suitable"]:
                return self._create_no_signal(volatility_regime["reason"])
            
            # Enforce ICT Kill Zone only when enabled
            if self.require_kill_zone and not self.is_in_kill_zone():
                return self._create_no_signal("Not in ICT Kill Zone")
            
            # Get FVG levels for strike selection
            candles = self._get_recent_ohlc(symbol, 10)
            fvg_levels = self.calculate_fvg_levels(candles)
            
            # Select strikes
            strikes = self.select_iron_condor_strikes(symbol, current_price, fvg_levels, range_analysis)
            
            # Calculate theoretical premiums (you'll need real option chain data)
            premium_calculation = self._estimate_premiums(symbol, strikes, range_analysis["range_width_pct"])
            
            # Risk management
            max_loss = (strikes["short_call"] - strikes["long_call"]) * 75 - premium_calculation["net_credit"]
            position_size = self.calculate_position_size(symbol, max_loss)
            
            # ✅ Generate UUID-based alert_id and strategy_id for entry signal
            import uuid
            strategy_id = str(uuid.uuid4())
            alert_id = str(uuid.uuid4())
            
            # Generate signal
            signal = {
                "pattern": "ict_iron_condor",
                "symbol": symbol,
                "action": "SELL_IRON_CONDOR",
                "strikes": strikes,
                "premiums": premium_calculation,
                "position_size": position_size,
                "risk_management": {
                    "max_loss": max_loss * position_size,
                    "max_profit": premium_calculation["net_credit"] * position_size,
                    "breakeven_upper": strikes["short_call"] + premium_calculation["net_credit"],
                    "breakeven_lower": strikes["short_put"] - premium_calculation["net_credit"],
                },
                "market_context": {
                    "range_analysis": range_analysis,
                    "volatility_regime": volatility_regime,
                    "in_kill_zone": True,
                    "fvg_levels_used": len(fvg_levels["bullish"]) + len(fvg_levels["bearish"]) > 0
                },
                "confidence": min(0.85, range_analysis["confidence"] * volatility_regime.get("confidence", 0.7)),
                "timestamp": datetime.now().isoformat(),
                "expiry": self._get_next_expiry(symbol),
                # ✅ UUID-based alert_id and strategy_id for traceability
                "alert_id": alert_id,
                "strategy_id": strategy_id
            }
            
            # Check if trade meets minimum criteria
            if (premium_calculation["net_credit"] >= 
                (self.NIFTY_PARAMS['min_credit'] if symbol == "NIFTY" else self.BANKNIFTY_PARAMS['min_credit'])):
                signal["trigger"] = True
                signal["reason"] = "Iron Condor conditions met with FVG-based strikes"
                
                # ✅ Use unified builder for algo_alert
                if self.alert_builder:
                    try:
                        algo_alert = self.alert_builder.build_iron_condor_algo_payload(signal)
                        signal['algo_alert'] = algo_alert
                    except Exception as e:
                        self.logger.error(f"❌ UnifiedAlertBuilder failed for {symbol}: {e}")
                
                # ✅ Track position state for future exit/reentry signals
                if symbol not in self.position_states:
                    self.position_states[symbol] = {}
                self.position_states[symbol].update({
                    'entry_alert_id': alert_id,
                    'strategy_id': strategy_id,
                    'entry_time': datetime.now(),
                    'strikes': strikes,
                    'premiums': premium_calculation,
                    'position_size': position_size
                })
            else:
                signal["trigger"] = False
                signal["reason"] = "Insufficient premium credit"
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Iron Condor signal generation failed for {symbol}: {e}")
            return self._create_no_signal(f"Error: {str(e)}")

    def _create_no_signal(self, reason: str) -> Dict:
        """Create a no-trade signal"""
        return {
            "pattern": "ict_iron_condor",
            "trigger": False,
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }

    # ✅ IMPLEMENTED: Data source methods integrated with system
    def _get_recent_ohlc(self, symbol: str, days: int) -> List[Dict]:
        """Get recent OHLC data from Redis (5-10 days)"""
        candles = []
        try:
            if not self.redis_client:
                from shared_core.redis_clients.redis_client import get_redis_client
                self.redis_client = get_redis_client(db=1)
            
            if not self.redis_client:
                return candles
            
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            
            # Get OHLC daily data from Redis
            ohlc_daily_key = DatabaseAwareKeyBuilder.live_ohlc_daily(canonical_symbol)
            
            # Get last N days of data
            entries = self.redis_client.zrange(ohlc_daily_key, -days, -1) if hasattr(self.redis_client, 'zrange') else []
            
            import json
            for entry in entries:
                if isinstance(entry, bytes):
                    entry = entry.decode('utf-8')
                try:
                    payload = json.loads(entry) if isinstance(entry, str) else entry
                    candles.append({
                        'open': float(payload.get('o', payload.get('open', 0))),
                        'high': float(payload.get('h', payload.get('high', 0))),
                        'low': float(payload.get('l', payload.get('low', 0))),
                        'close': float(payload.get('c', payload.get('close', 0))),
                        'timestamp': payload.get('timestamp', payload.get('date', '')),
                        'volume': float(payload.get('v', payload.get('volume', 0)))
                    })
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue
            
            # If no daily data, try to get from latest OHLC
            if not candles:
                ohlc_latest_key = DatabaseAwareKeyBuilder.live_ohlc_latest(canonical_symbol)
                ohlc_data = self.redis_client.hgetall(ohlc_latest_key) if hasattr(self.redis_client, 'hgetall') else {}
                if ohlc_data:
                    try:
                        candles.append({
                            'open': float(ohlc_data.get(b'open', ohlc_data.get('open', 0))),
                            'high': float(ohlc_data.get(b'high', ohlc_data.get('high', 0))),
                            'low': float(ohlc_data.get(b'low', ohlc_data.get('low', 0))),
                            'close': float(ohlc_data.get(b'close', ohlc_data.get('close', 0))),
                            'timestamp': datetime.now().isoformat(),
                            'volume': float(ohlc_data.get(b'volume', ohlc_data.get('volume', 0)))
                        })
                    except (ValueError, TypeError):
                        pass
                        
        except Exception as e:
            self.logger.error(f"Failed to get recent OHLC for {symbol}: {e}")
        
        return candles

    def _get_current_price(self, symbol: str) -> float:
        """Get current underlying price from Redis"""
        try:
            if not self.redis_client:
                from shared_core.redis_clients.redis_client import get_redis_client
                self.redis_client = get_redis_client(db=1)
            
            if not self.redis_client:
                return 0.0
            
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder, RedisKeyStandards
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            
            # Try realtime price (string) first
            realtime_key = DatabaseAwareKeyBuilder.live_price_realtime(canonical_symbol)
            if hasattr(self.redis_client, 'get'):
                try:
                    price_data = self.redis_client.get(realtime_key)
                    if price_data:
                        if isinstance(price_data, bytes):
                            price_data = price_data.decode('utf-8')
                        price = float(price_data)
                        if price > 0:
                            return price
                except (ValueError, TypeError):
                    pass
            
            # Fallback: read tick hash via HGETALL
            tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(canonical_symbol)
            if hasattr(self.redis_client, 'hgetall'):
                try:
                    tick_hash = self.redis_client.hgetall(tick_key)
                    if tick_hash:
                        last_price = (
                            tick_hash.get(b'last_price') or tick_hash.get('last_price') or
                            tick_hash.get(b'close') or tick_hash.get('close')
                        )
                        if last_price:
                            if isinstance(last_price, bytes):
                                last_price = last_price.decode('utf-8')
                            price = float(last_price)
                            if price > 0:
                                return price
                except Exception:
                    pass
            
            # Fallback: try OHLC latest
            ohlc_key = DatabaseAwareKeyBuilder.live_ohlc_latest(canonical_symbol)
            ohlc_data = self.redis_client.hgetall(ohlc_key) if hasattr(self.redis_client, 'hgetall') else {}
            if ohlc_data:
                close_price = ohlc_data.get(b'close', ohlc_data.get('close'))
                if close_price:
                    if isinstance(close_price, bytes):
                        close_price = close_price.decode('utf-8')
                    price = float(close_price)
                    if price > 0:
                        return price
                        
        except Exception as e:
            self.logger.error(f"Failed to get current price for {symbol}: {e}")
        
        return 0.0

    def _get_current_vix(self) -> float:
        """Get current India VIX from Redis"""
        try:
            from shared_core.utils.vix_utils import get_current_vix
            vix_data = get_current_vix()
            if vix_data and isinstance(vix_data, dict):
                return float(vix_data.get('value', 15.0))
            elif isinstance(vix_data, (int, float)):
                return float(vix_data)
        except Exception as e:
            self.logger.debug(f"Failed to get VIX: {e}")
        
        return 15.0  # Default fallback

    def _calculate_historical_vol(self, symbol: str, period: int) -> float:
        """Calculate historical volatility from recent OHLC data"""
        try:
            candles = self._get_recent_ohlc(symbol, period)
            if len(candles) < 2:
                return 12.0  # Default fallback
            
            # Calculate returns
            returns = []
            for i in range(1, len(candles)):
                prev_close = candles[i-1].get('close', 0)
                curr_close = candles[i].get('close', 0)
                if prev_close > 0:
                    ret = (curr_close - prev_close) / prev_close
                    returns.append(ret)
            
            if not returns:
                return 12.0
            
            # Calculate standard deviation of returns
            import statistics
            mean_return = statistics.mean(returns)
            variance = statistics.variance(returns, mean_return) if len(returns) > 1 else 0
            std_dev = variance ** 0.5
            
            # Annualize (assuming daily data)
            annualized_vol = std_dev * (252 ** 0.5) * 100  # Convert to percentage
            
            return max(8.0, min(annualized_vol, 50.0))  # Clamp between 8% and 50%
            
        except Exception as e:
            self.logger.error(f"Failed to calculate historical vol for {symbol}: {e}")
        
        return 12.0  # Default fallback

    def _estimate_premiums(self, symbol: str, strikes: Dict, range_width_pct: float) -> Dict:
        """Estimate option premiums using option chain data from Redis"""
        try:
            # Try to get real option chain data
            option_chain = self._get_option_chain(symbol)
            if option_chain:
                # Use real premiums from option chain
                short_put_strike = strikes.get("short_put", 0)
                long_put_strike = strikes.get("long_put", 0)
                short_call_strike = strikes.get("short_call", 0)
                long_call_strike = strikes.get("long_call", 0)
                
                # Find premiums from chain
                short_put_premium = self._find_premium_from_chain(option_chain, short_put_strike, "PE")
                long_put_premium = self._find_premium_from_chain(option_chain, long_put_strike, "PE")
                short_call_premium = self._find_premium_from_chain(option_chain, short_call_strike, "CE")
                long_call_premium = self._find_premium_from_chain(option_chain, long_call_strike, "CE")
                
                if all(p > 0 for p in [short_put_premium, long_put_premium, short_call_premium, long_call_premium]):
                    net_credit = (short_put_premium + short_call_premium) - (long_put_premium + long_call_premium)
                    return {
                        "short_put_premium": short_put_premium,
                        "long_put_premium": long_put_premium,
                        "short_call_premium": short_call_premium,
                        "long_call_premium": long_call_premium,
                        "net_credit": max(0, net_credit)
                    }
        except Exception as e:
            self.logger.debug(f"Option chain premium estimation failed: {e}")
        
        # Fallback: heuristic estimation
        params = self.NIFTY_PARAMS if symbol == "NIFTY" else self.BANKNIFTY_PARAMS
        base_premium = range_width_pct * 0.1  # Simple heuristic
        
        return {
            "short_put_premium": base_premium * 0.6,
            "long_put_premium": base_premium * 0.3,
            "short_call_premium": base_premium * 0.6,
            "long_call_premium": base_premium * 0.3,
            "net_credit": base_premium * 0.6,  # short - long
        }

    def _get_option_chain(self, symbol: str) -> Optional[Dict]:
        """Get option chain data from Redis"""
        try:
            if not self.redis_client:
                from shared_core.redis_clients.redis_client import get_redis_client
                self.redis_client = get_redis_client(db=1)
            
            if not self.redis_client:
                return None
            
            from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
            
            # Get option chain key
            option_chain_key = DatabaseAwareKeyBuilder.get_option_chain_key(symbol)
            chain_data = self.redis_client.get(option_chain_key) if hasattr(self.redis_client, 'get') else None
            
            if chain_data:
                import json
                if isinstance(chain_data, bytes):
                    chain_data = chain_data.decode('utf-8')
                return json.loads(chain_data) if isinstance(chain_data, str) else chain_data
                
        except Exception as e:
            self.logger.debug(f"Failed to get option chain for {symbol}: {e}")
        
        return None

    def _find_premium_from_chain(self, option_chain: Dict, strike: int, option_type: str) -> float:
        """Find premium for a specific strike from option chain"""
        try:
            chain_section = option_chain.get(option_type, {})
            strike_key = str(strike)
            option_data = chain_section.get(strike_key, {})
            
            # Try multiple premium field names
            premium = option_data.get('last_price') or option_data.get('premium') or option_data.get('ltp') or 0.0
            return float(premium) if premium else 0.0
            
        except Exception:
            return 0.0

    def _get_next_expiry(self, symbol: str) -> str:
        """Get next expiry date from Redis or calculate"""
        try:
            # Try to get from option chain
            option_chain = self._get_option_chain(symbol)
            if option_chain:
                expiry = option_chain.get('expiry') or option_chain.get('expiry_date')
                if expiry:
                    return expiry
            
            # Fallback: calculate next weekly expiry (Thursday for Nifty/BankNifty)
            today = datetime.now()
            days_until_thursday = (3 - today.weekday()) % 7
            if days_until_thursday == 0 and today.hour >= 15:  # If Thursday after 3 PM, next week
                days_until_thursday = 7
            next_expiry = today + timedelta(days=days_until_thursday)
            return next_expiry.strftime("%Y-%m-%d")
            
        except Exception as e:
            self.logger.debug(f"Failed to get expiry for {symbol}: {e}")
        
        return (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d")

# ✅ DEPRECATED: IRON_CONDOR_ALERT_TEMPLATE removed
# Alert templates are now handled by HumanReadableAlertTemplates in alerts/notifiers.py
# All patterns should use the centralized template system to ensure consistent formatting
# and proper inclusion of target_price and stop_loss for aion_algo_executor
