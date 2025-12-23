# kite_spoofing_detector.py
import time
import numpy as np
from collections import deque
from typing import Dict, List, Optional

class KiteSpoofingDetector:
    def __init__(self, symbol: str, config: dict = None):
        self.symbol = symbol
        self.config = config or {
            'large_order_threshold': 100000,  # Minimum quantity to consider as large order
            'vanishing_timeframe': 30,  # Seconds to track vanishing orders
            'price_approach_percent': 0.02,  # 2% price movement considered "approaching"
            'min_confidence': 0.6  # Minimum confidence to trigger alert
        }
        
        # Tracking state
        self.previous_depth = {'buy': [], 'sell': []}
        self.large_orders_tracked = {}  # price_side -> order_info
        self.trade_history = deque(maxlen=100)
        self.alert_history = deque(maxlen=50)
        self.last_tick_time = 0
        
    def analyze_kite_quote(self, kite_quote: dict) -> Optional[dict]:
        """
        Analyze Kite quote data for spoofing patterns
        
        Args:
            kite_quote: Full Kite quote response for one symbol
            
        Returns:
            Spoofing alert dict or None
        """
        try:
            current_time = time.time()
            self.last_tick_time = current_time
            
            # Extract depth and trade data
            depth = kite_quote.get('depth', {'buy': [], 'sell': []})
            last_trade = self._extract_last_trade(kite_quote)
            
            # Add to trade history if new trade
            if last_trade and self._is_new_trade(last_trade):
                self.trade_history.append(last_trade)
            
            # Analyze for spoofing patterns
            alerts = []
            
            # Pattern 1: Vanishing large orders
            vanishing_alert = self._detect_vanishing_orders(depth, current_time)
            if vanishing_alert:
                alerts.append(vanishing_alert)
            
            # Pattern 2: Large orders without trade volume
            no_trade_alert = self._detect_no_trade_volume(depth, current_time)
            if no_trade_alert:
                alerts.append(no_trade_alert)
            
            # Pattern 3: Sudden large order walls
            wall_alert = self._detect_sudden_walls(depth, current_time)
            if wall_alert:
                alerts.append(wall_alert)
            
            # Update previous depth for next comparison
            self.previous_depth = depth
            
            # Return highest confidence alert
            if alerts:
                best_alert = max(alerts, key=lambda x: x.get('confidence', 0))
                if best_alert['confidence'] >= self.config['min_confidence']:
                    # Avoid duplicate alerts
                    if not self._is_duplicate_alert(best_alert):
                        self.alert_history.append(best_alert)
                        return best_alert
            
            return None
            
        except Exception as e:
            print(f"Error in spoofing analysis for {self.symbol}: {e}")
            return None
    
    def _extract_last_trade(self, kite_quote: dict) -> Optional[dict]:
        """Extract last trade from Kite quote"""
        last_price = kite_quote.get('last_price')
        last_quantity = kite_quote.get('last_quantity', 0)
        last_trade_time = kite_quote.get('last_trade_time')
        
        # ✅ FIX: Ensure last_quantity is numeric before comparison
        try:
            last_quantity = float(last_quantity) if last_quantity else 0
        except (ValueError, TypeError):
            last_quantity = 0
        
        if last_price and last_quantity > 0:
            return {
                'price': float(last_price) if last_price else 0,
                'quantity': int(last_quantity),
                'timestamp': time.time(),  # Use current time as approximation
                'exchange_timestamp': last_trade_time
            }
        return None
    
    def _is_new_trade(self, trade: dict) -> bool:
        """Check if trade is new (basic duplicate check)"""
        if not self.trade_history:
            return True
        
        # ✅ FIX: Ensure values are numeric before comparison
        try:
            trade_price = float(trade.get('price', 0))
            trade_quantity = float(trade.get('quantity', 0))
            last_trade = self.trade_history[-1]
            last_price = float(last_trade.get('price', 0))
            last_quantity = float(last_trade.get('quantity', 0))
            return (trade_price != last_price or trade_quantity != last_quantity)
        except (ValueError, TypeError, KeyError, IndexError):
            return True  # If we can't compare, assume it's new
    
    def _detect_vanishing_orders(self, current_depth: dict, current_time: float) -> Optional[dict]:
        """Detect large orders that disappeared from previous depth"""
        vanished_orders = []
        
        # Check each side (buy/sell)
        for side in ['buy', 'sell']:
            previous_orders = self.previous_depth.get(side, [])
            current_orders = current_depth.get(side, [])
            
            # Find large orders that vanished
            for prev_order in previous_orders:
                # ✅ FIX: Ensure quantity is numeric before comparison
                try:
                    quantity = float(prev_order.get('quantity', 0))
                    if quantity >= self.config['large_order_threshold']:
                        # Check if this order still exists in current depth
                        if not self._order_exists_in_depth(prev_order, current_orders):
                            # Check if price was approaching this order
                            order_price = float(prev_order.get('price', 0))
                            if self._was_price_approaching(order_price, side):
                                vanished_orders.append({
                                    'side': side,
                                    'price': order_price,
                                    'quantity': int(quantity),
                                    'orders': int(prev_order.get('orders', 1))
                                })
                except (ValueError, TypeError):
                    continue  # Skip invalid orders
        
        if vanished_orders:
            total_quantity = sum(order['quantity'] for order in vanished_orders)
            avg_confidence = min(0.9, len(vanished_orders) * 0.3)
            
            return {
                'type': 'vanishing_large_orders',
                'symbol': self.symbol,
                'confidence': avg_confidence,
                'direction': 'bullish' if vanished_orders[0]['side'] == 'buy' else 'bearish',
                'details': {
                    'vanished_count': len(vanished_orders),
                    'total_quantity': total_quantity,
                    'orders': vanished_orders
                },
                'timestamp': current_time
            }
        
        return None
    
    def _detect_no_trade_volume(self, current_depth: dict, current_time: float) -> Optional[dict]:
        """Detect large orders with no corresponding trade volume"""
        large_orders = self._find_large_orders(current_depth)
        
        for order in large_orders:
            # ✅ FIX: Ensure order values are numeric
            try:
                order_price = float(order.get('price', 0))
                order_quantity = float(order.get('quantity', 0))
            except (ValueError, TypeError):
                continue  # Skip invalid orders
            
            # Check if there were significant trades near this price recently
            if not self._has_recent_trades_near_price(order_price, order.get('side', 'buy')):
                return {
                    'type': 'no_trade_volume',
                    'symbol': self.symbol,
                    'confidence': 0.7,
                    'direction': 'bullish' if order.get('side') == 'buy' else 'bearish',
                    'details': {
                        'order_side': order.get('side', 'buy'),
                        'order_price': order_price,
                        'order_quantity': int(order_quantity),
                        'time_without_trades': 60  # Assuming 60 seconds without trades
                    },
                    'timestamp': current_time
                }
        
        return None
    
    def _detect_sudden_walls(self, current_depth: dict, current_time: float) -> Optional[dict]:
        """Detect sudden appearance of large order walls"""
        large_orders = self._find_large_orders(current_depth)
        sudden_walls = []
        
        for order in large_orders:
            # ✅ FIX: Ensure order values are numeric
            try:
                order_side = order.get('side', 'buy')
                order_quantity = float(order.get('quantity', 0))
            except (ValueError, TypeError):
                continue  # Skip invalid orders
            
            # Check if this is a new large order (not in previous depth)
            if not self._order_exists_in_depth(order, self.previous_depth.get(order_side, [])):
                sudden_walls.append(order)
        
        if sudden_walls:
            wall_side = sudden_walls[0].get('side', 'buy')
            total_wall_quantity = sum(float(order.get('quantity', 0)) for order in sudden_walls)
            
            return {
                'type': 'sudden_order_wall',
                'symbol': self.symbol,
                'confidence': 0.65,
                'direction': 'bearish' if wall_side == 'sell' else 'bullish',
                'details': {
                    'wall_side': wall_side,
                    'wall_count': len(sudden_walls),
                    'total_quantity': int(total_wall_quantity),
                    'orders': sudden_walls
                },
                'timestamp': current_time
            }
        
        return None
    
    def _order_exists_in_depth(self, order: dict, depth_side: list) -> bool:
        """Check if order exists in depth side"""
        # ✅ FIX: Ensure order values are numeric
        try:
            order_price = float(order.get('price', 0))
            order_quantity = float(order.get('quantity', 0))
        except (ValueError, TypeError):
            return False
        
        for depth_order in depth_side:
            try:
                depth_price = float(depth_order.get('price', 0))
                depth_quantity = float(depth_order.get('quantity', 0))
                if (depth_price == order_price and depth_quantity == order_quantity):
                    return True
            except (ValueError, TypeError):
                continue  # Skip invalid depth orders
        return False
    
    def _was_price_approaching(self, order_price: float, side: str) -> bool:
        """Check if recent price was approaching the order price"""
        if not self.trade_history:
            return False
        
        # ✅ FIX: Ensure order_price is numeric
        try:
            order_price = float(order_price)
        except (ValueError, TypeError):
            return False
        
        # Get recent trades (last 30 seconds)
        recent_trades = [
            t for t in self.trade_history 
            if time.time() - t['timestamp'] < self.config['vanishing_timeframe']
        ]
        
        if not recent_trades:
            return False
        
        # For buy orders: price was approaching from above
        # For sell orders: price was approaching from below
        if side == 'buy':
            # Price was falling toward buy order
            price_approach = any(
                float(trade.get('price', 0)) <= order_price * (1 + self.config['price_approach_percent']) 
                for trade in recent_trades
            )
        else:  # sell
            # Price was rising toward sell order  
            price_approach = any(
                float(trade.get('price', 0)) >= order_price * (1 - self.config['price_approach_percent'])
                for trade in recent_trades
            )
        
        return price_approach
    
    def _find_large_orders(self, depth: dict) -> List[dict]:
        """Find orders larger than threshold in depth"""
        large_orders = []
        threshold = self.config['large_order_threshold']
        
        for side in ['buy', 'sell']:
            for order in depth.get(side, []):
                # ✅ FIX: Ensure quantity is numeric before comparison
                try:
                    quantity = float(order.get('quantity', 0))
                    if quantity >= threshold:
                        large_orders.append({
                            'side': side,
                            'price': float(order.get('price', 0)),
                            'quantity': int(quantity),
                            'orders': int(order.get('orders', 1))
                        })
                except (ValueError, TypeError):
                    continue  # Skip invalid orders
        
        return large_orders
    
    def _has_recent_trades_near_price(self, price: float, side: str) -> bool:
        """Check if there were recent trades near the given price"""
        # ✅ FIX: Ensure price is numeric
        try:
            price = float(price)
        except (ValueError, TypeError):
            return False
        
        recent_trades = [
            t for t in self.trade_history 
            if time.time() - t['timestamp'] < 60  # Last 60 seconds
        ]
        
        if not recent_trades:
            return False
        
        # Calculate price range for "near" (1% for stocks, adjust as needed)
        price_range = price * 0.01
        
        for trade in recent_trades:
            try:
                trade_price = float(trade.get('price', 0))
                trade_quantity = float(trade.get('quantity', 0))
                if abs(trade_price - price) <= price_range:
                    if trade_quantity > self.config['large_order_threshold'] * 0.1:
                        return True
            except (ValueError, TypeError):
                continue  # Skip invalid trades
        
        return False
    
    def _is_duplicate_alert(self, alert: dict) -> bool:
        """Check if this is a duplicate recent alert"""
        if not self.alert_history:
            return False
        
        # Check last few alerts for similar type and direction
        recent_alerts = list(self.alert_history)[-5:]  # Last 5 alerts
        
        for recent_alert in recent_alerts:
            if (recent_alert['type'] == alert['type'] and 
                recent_alert['direction'] == alert['direction'] and
                time.time() - recent_alert['timestamp'] < 300):  # 5 minutes
                return True
        
        return False