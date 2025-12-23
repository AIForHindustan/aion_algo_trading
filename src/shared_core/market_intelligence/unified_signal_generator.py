# shared_core/market_intelligence/unified_signal_generator.py
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from enum import Enum

class MarketRegime(Enum):
    HIGH_VOLATILITY = "high_volatility"
    LOW_VOLATILITY = "low_volatility" 
    TRENDING_UP = "trending_up"
    TRENDING_DOWN = "trending_down"
    SIDEWAYS = "sideways"
    BREAKOUT = "breakout"
    BREAKDOWN = "breakdown"

class InstitutionalFlow(Enum):
    STRONG_BUYING = "strong_buying"
    MODERATE_BUYING = "moderate_buying"
    NEUTRAL = "neutral"
    MODERATE_SELLING = "moderate_selling"
    STRONG_SELLING = "strong_selling"

@dataclass
class SignalConfidence:
    score: float  # 0-100
    factors: List[str]  # Which factors contributed
    regime: MarketRegime
    institutional_flow: InstitutionalFlow
    volume_anomaly: bool
    options_flow: bool
    sector_alignment: bool

@dataclass
class EnhancedSignal:
    symbol: str
    signal_type: str  # 'BUY', 'SELL', 'HOLD'
    confidence: SignalConfidence
    price_levels: Dict[str, float]
    timeframe: str
    factors: Dict[str, Any]
    suppression_reasons: List[str]
    edge_score: float  # 0-1 probability of success

class UnifiedSignalGenerator:
    """
    Sophisticated signal generator that combines:
    - VIX regime analysis
    - Institutional flows (DII/FII, block deals)
    - Sectoral volatility 
    - Volume anomalies
    - Options flow analysis
    - Market microstructure
    """
    
    def __init__(self, clickhouse_client, redis_client=None):
        self.clickhouse = clickhouse_client
        self.redis = redis_client
        self.logger = logging.getLogger(__name__)
        
        # Signal thresholds (dynamic based on regime)
        self.thresholds = {
            'vix_high': 20.0,
            'vix_low': 12.0,
            'volume_spike': 2.5,  # 2.5x average volume
            'institutional_flow_significant': 500,  # crore
            'options_flow_significant': 3.0,  # 3x average volume
            'sector_deviation': 0.15  # 15% from sector average
        }
    
    def get_market_regime(self) -> MarketRegime:
        """Determine current market regime using multiple factors"""
        vix_regime = self._get_vix_regime()
        sector_regime = self._get_sector_regime()
        institutional_regime = self._get_institutional_regime()
        
        # Combine regimes with weights
        regime_scores = {
            MarketRegime.HIGH_VOLATILITY: 0,
            MarketRegime.LOW_VOLATILITY: 0,
            MarketRegime.TRENDING_UP: 0,
            MarketRegime.TRENDING_DOWN: 0,
            MarketRegime.SIDEWAYS: 0
        }
        
        # VIX is primary indicator (40% weight)
        if vix_regime == MarketRegime.HIGH_VOLATILITY:
            regime_scores[MarketRegime.HIGH_VOLATILITY] += 0.4
        else:
            regime_scores[MarketRegime.LOW_VOLATILITY] += 0.4
        
        # Sector momentum (30% weight)
        if sector_regime == MarketRegime.TRENDING_UP:
            regime_scores[MarketRegime.TRENDING_UP] += 0.3
        elif sector_regime == MarketRegime.TRENDING_DOWN:
            regime_scores[MarketRegime.TRENDING_DOWN] += 0.3
        else:
            regime_scores[MarketRegime.SIDEWAYS] += 0.3
        
        # Institutional flow (30% weight)
        if institutional_regime == InstitutionalFlow.STRONG_BUYING:
            regime_scores[MarketRegime.TRENDING_UP] += 0.3
        elif institutional_regime == InstitutionalFlow.STRONG_SELLING:
            regime_scores[MarketRegime.TRENDING_DOWN] += 0.3
        else:
            regime_scores[MarketRegime.SIDEWAYS] += 0.3
        
        return max(regime_scores, key=regime_scores.get)
    
    def _get_vix_regime(self) -> MarketRegime:
        """Get VIX-based market regime"""
        try:
            query = """
            SELECT last_price, timestamp
            FROM indices_data 
            WHERE symbol = 'NSE:INDIA VIX' 
            ORDER BY timestamp DESC 
            LIMIT 1
            """
            result = self.clickhouse.execute(query)
            
            if result:
                vix_value = result[0][0]
                if vix_value > self.thresholds['vix_high']:
                    return MarketRegime.HIGH_VOLATILITY
                elif vix_value < self.thresholds['vix_low']:
                    return MarketRegime.LOW_VOLATILITY
            
            return MarketRegime.SIDEWAYS
            
        except Exception as e:
            self.logger.error(f"Error getting VIX regime: {e}")
            return MarketRegime.SIDEWAYS
    
    def _get_sector_regime(self) -> MarketRegime:
        """Get sector-based market regime"""
        try:
            query = """
            SELECT index_name, volatility_20d_pct, current_price
            FROM sectoral_volatility 
            WHERE date = today()
            """
            sectors = self.clickhouse.execute(query)
            
            if not sectors:
                return MarketRegime.SIDEWAYS
            
            # Calculate sector momentum
            up_sectors = 0
            total_sectors = len(sectors)
            
            for sector in sectors:
                # Simple momentum: if volatility > 15% and price > avg, consider trending
                if sector[1] > 15 and sector[2] > self._get_sector_average(sector[0]):
                    up_sectors += 1
            
            if up_sectors / total_sectors > 0.7:
                return MarketRegime.TRENDING_UP
            elif up_sectors / total_sectors < 0.3:
                return MarketRegime.TRENDING_DOWN
            else:
                return MarketRegime.SIDEWAYS
                
        except Exception as e:
            self.logger.error(f"Error getting sector regime: {e}")
            return MarketRegime.SIDEWAYS
    
    def _get_institutional_regime(self) -> InstitutionalFlow:
        """Get institutional flow regime"""
        try:
            query = """
            SELECT category, net_value
            FROM nse_dii_fii_trading 
            WHERE date = today()
            """
            flows = self.clickhouse.execute(query)
            
            dii_flow = 0
            fii_flow = 0
            
            for category, net_value in flows:
                if category == 'DII':
                    dii_flow = net_value
                elif category == 'FII':
                    fii_flow = net_value
            
            total_flow = dii_flow + fii_flow
            
            if total_flow > self.thresholds['institutional_flow_significant']:
                return InstitutionalFlow.STRONG_BUYING
            elif total_flow > self.thresholds['institutional_flow_significant'] * 0.5:
                return InstitutionalFlow.MODERATE_BUYING
            elif total_flow < -self.thresholds['institutional_flow_significant']:
                return InstitutionalFlow.STRONG_SELLING
            elif total_flow < -self.thresholds['institutional_flow_significant'] * 0.5:
                return InstitutionalFlow.MODERATE_SELLING
            else:
                return InstitutionalFlow.NEUTRAL
                
        except Exception as e:
            self.logger.error(f"Error getting institutional regime: {e}")
            return InstitutionalFlow.NEUTRAL
    
    def analyze_scanner_signal(self, scanner_signal: Dict) -> EnhancedSignal:
        """
        Enhance scanner signal with market intelligence
        """
        symbol = scanner_signal.get('symbol')
        original_signal = scanner_signal.get('signal_type')
        original_confidence = scanner_signal.get('confidence', 50)
        
        # Get all intelligence factors
        market_regime = self.get_market_regime()
        institutional_flow = self._get_institutional_regime()
        volume_anomaly = self._check_volume_anomaly(symbol)
        options_flow = self._check_options_flow(symbol)
        sector_alignment = self._check_sector_alignment(symbol)
        
        # Calculate suppression factors
        suppression_reasons = self._get_suppression_factors(
            symbol, original_signal, market_regime, institutional_flow
        )
        
        # Calculate enhanced confidence
        enhanced_confidence = self._calculate_enhanced_confidence(
            original_confidence, market_regime, institutional_flow,
            volume_anomaly, options_flow, sector_alignment,
            suppression_reasons
        )
        
        # Calculate edge score (probability of success)
        edge_score = self._calculate_edge_score(
            symbol, original_signal, enhanced_confidence,
            market_regime, institutional_flow
        )
        
        # Final signal decision
        final_signal_type = self._get_final_signal_type(
            original_signal, enhanced_confidence, suppression_reasons
        )
        
        return EnhancedSignal(
            symbol=symbol,
            signal_type=final_signal_type,
            confidence=SignalConfidence(
                score=enhanced_confidence,
                factors=self._get_confidence_factors(
                    volume_anomaly, options_flow, sector_alignment
                ),
                regime=market_regime,
                institutional_flow=institutional_flow,
                volume_anomaly=volume_anomaly,
                options_flow=options_flow,
                sector_alignment=sector_alignment
            ),
            price_levels=self._calculate_price_levels(symbol),
            timeframe=scanner_signal.get('timeframe', 'intraday'),
            factors={
                'market_regime': market_regime.value,
                'institutional_flow': institutional_flow.value,
                'volume_anomaly': volume_anomaly,
                'options_flow': options_flow,
                'sector_alignment': sector_alignment,
                'original_confidence': original_confidence,
                'suppression_count': len(suppression_reasons)
            },
            suppression_reasons=suppression_reasons,
            edge_score=edge_score
        )
    
    def _get_suppression_factors(self, symbol: str, signal_type: str, 
                               regime: MarketRegime, flow: InstitutionalFlow) -> List[str]:
        """Determine reasons to suppress a signal"""
        suppression_factors = []
        
        # Regime-based suppression
        if (signal_type == 'BUY' and regime == MarketRegime.TRENDING_DOWN and 
            flow == InstitutionalFlow.STRONG_SELLING):
            suppression_factors.append("Strong bearish regime")
        
        if (signal_type == 'SELL' and regime == MarketRegime.TRENDING_UP and 
            flow == InstitutionalFlow.STRONG_BUYING):
            suppression_factors.append("Strong bullish regime")
        
        # VIX-based suppression
        vix_regime = self._get_vix_regime()
        if vix_regime == MarketRegime.HIGH_VOLATILITY and signal_type in ['BUY', 'SELL']:
            suppression_factors.append("High volatility regime - avoid directional trades")
        
        # Check for conflicting institutional activity
        block_deals = self._get_recent_block_deals(symbol)
        if block_deals:
            recent_deal = block_deals[0]
            if ((signal_type == 'BUY' and recent_deal['deal_type'] == 'SELL') or
                (signal_type == 'SELL' and recent_deal['deal_type'] == 'BUY')):
                suppression_factors.append("Conflicting institutional activity")
        
        return suppression_factors
    
    def _calculate_enhanced_confidence(self, original_confidence: float,
                                    regime: MarketRegime, flow: InstitutionalFlow,
                                    volume_anomaly: bool, options_flow: bool,
                                    sector_alignment: bool, 
                                    suppression_reasons: List[str]) -> float:
        """Calculate enhanced confidence score"""
        base_confidence = original_confidence
        
        # Apply regime multipliers
        regime_multiplier = 1.0
        if regime in [MarketRegime.TRENDING_UP, MarketRegime.TRENDING_DOWN]:
            regime_multiplier = 1.2  # 20% boost in trending markets
        elif regime == MarketRegime.HIGH_VOLATILITY:
            regime_multiplier = 0.8  # 20% reduction in high volatility
        
        # Apply institutional flow multipliers
        flow_multiplier = 1.0
        if flow in [InstitutionalFlow.STRONG_BUYING, InstitutionalFlow.STRONG_SELLING]:
            flow_multiplier = 1.15
        
        # Apply factor bonuses
        factor_bonus = 0
        if volume_anomaly:
            factor_bonus += 5
        if options_flow:
            factor_bonus += 8
        if sector_alignment:
            factor_bonus += 7
        
        # Apply suppression penalties
        suppression_penalty = len(suppression_reasons) * 10
        
        enhanced_confidence = (base_confidence * regime_multiplier * flow_multiplier + 
                             factor_bonus - suppression_penalty)
        
        return max(0, min(100, enhanced_confidence))
    
    def _calculate_edge_score(self, symbol: str, signal_type: str, 
                            confidence: float, regime: MarketRegime, 
                            flow: InstitutionalFlow) -> float:
        """Calculate probability of success (edge score)"""
        base_probability = confidence / 100
        
        # Historical success rates by regime
        regime_edges = {
            MarketRegime.TRENDING_UP: 0.65,
            MarketRegime.TRENDING_DOWN: 0.63,
            MarketRegime.HIGH_VOLATILITY: 0.45,
            MarketRegime.LOW_VOLATILITY: 0.55,
            MarketRegime.SIDEWAYS: 0.50
        }
        
        # Institutional flow edges
        flow_edges = {
            InstitutionalFlow.STRONG_BUYING: 0.70,
            InstitutionalFlow.MODERATE_BUYING: 0.60,
            InstitutionalFlow.NEUTRAL: 0.50,
            InstitutionalFlow.MODERATE_SELLING: 0.40,
            InstitutionalFlow.STRONG_SELLING: 0.30
        }
        
        regime_edge = regime_edges.get(regime, 0.5)
        flow_edge = flow_edges.get(flow, 0.5)
        
        # Combine probabilities (weighted average)
        edge_score = (base_probability * 0.4 + regime_edge * 0.3 + flow_edge * 0.3)
        
        return edge_score
    
    def _get_final_signal_type(self, original_signal: str, 
                             enhanced_confidence: float, 
                             suppression_reasons: List[str]) -> str:
        """Determine final signal type after analysis"""
        if enhanced_confidence < 30 or len(suppression_reasons) >= 2:
            return 'HOLD'
        
        if enhanced_confidence < 50:
            return 'WEAK_' + original_signal
        
        return original_signal
    
    def _check_volume_anomaly(self, symbol: str) -> bool:
        """Check for unusual volume activity"""
        try:
            # Query directly from tick_data (no need for aggregation table)
            query = """
            SELECT 
                max(volume) as today_volume,
                avg(max(volume)) OVER (
                    PARTITION BY symbol ORDER BY date 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as avg_volume_20d
            FROM tick_data
            WHERE symbol = %(symbol)s AND date >= today() - 20
            GROUP BY symbol, date
            ORDER BY date DESC
            LIMIT 1
            """
            
            result = self.clickhouse.execute(query, {'symbol': symbol})
            if result and len(result) > 0:
                today_volume, avg_volume_20d = result[0]
                if today_volume and avg_volume_20d:
                    volume_ratio = today_volume / avg_volume_20d
                    return volume_ratio > self.thresholds['volume_spike']
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking volume anomaly for {symbol}: {e}")
            return False
    
    def _check_options_flow(self, symbol: str) -> bool:
        """Check for unusual options activity"""
        try:
            # For equity signals, check if there's unusual options activity
            query = """
            SELECT 
                symbol,
                max(volume) as volume,
                avg(max(volume)) OVER (
                    PARTITION BY symbol ORDER BY date 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as avg_volume_20d
            FROM tick_data
            WHERE date = today() 
                AND instrument_type IN ('CE', 'PE')
                AND (symbol LIKE %(pattern)s OR tradingsymbol LIKE %(pattern)s)
            GROUP BY symbol, date
            HAVING volume / avg_volume_20d > %(threshold)s
            LIMIT 5
            """
            
            # Look for options on this underlying
            pattern = f"%{symbol}%"
            result = self.clickhouse.execute(query, {
                'pattern': pattern,
                'threshold': self.thresholds['options_flow_significant']
            })
            
            return len(result) > 0
            
        except Exception as e:
            self.logger.error(f"Error checking options flow for {symbol}: {e}")
            return False
    
    def _check_sector_alignment(self, symbol: str) -> bool:
        """Check if signal aligns with sector movement"""
        try:
            # Get sector for symbol
            sector_query = """
            SELECT sector 
            FROM tick_data 
            WHERE symbol = %(symbol)s 
            LIMIT 1
            """
            sector_result = self.clickhouse.execute(sector_query, {'symbol': symbol})
            
            if not sector_result:
                return False
            
            sector = sector_result[0][0]
            
            # Get sector performance
            sector_perf_query = """
            SELECT volatility_20d_pct
            FROM sectoral_volatility 
            WHERE date = today() AND index_name = %(sector)s
            """
            perf_result = self.clickhouse.execute(sector_perf_query, {'sector': sector})
            
            if perf_result:
                sector_vol = perf_result[0][0]
                # If sector is trending (high volatility), consider aligned
                return sector_vol > 12  # Adjust threshold as needed
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking sector alignment for {symbol}: {e}")
            return False
    
    def _get_recent_block_deals(self, symbol: str) -> List[Dict]:
        """Get recent block deals for symbol"""
        try:
            query = """
            SELECT symbol, client_name, deal_type, quantity, price, value, date
            FROM nse_block_deals 
            WHERE symbol = %(symbol)s AND date >= today() - 3
            ORDER BY date DESC, value DESC
            LIMIT 5
            """
            results = self.clickhouse.execute(query, {'symbol': symbol})
            
            deals = []
            for row in results:
                deals.append({
                    'symbol': row[0],
                    'client_name': row[1],
                    'deal_type': row[2],
                    'quantity': row[3],
                    'price': row[4],
                    'value': row[5],
                    'date': row[6]
                })
            
            return deals
            
        except Exception as e:
            self.logger.error(f"Error getting block deals for {symbol}: {e}")
            return []
    
    def _calculate_price_levels(self, symbol: str) -> Dict[str, float]:
        """Calculate key price levels for the symbol"""
        try:
            query = """
            SELECT 
                max(high_price) as high,
                min(low_price) as low,
                avg(close_price) as avg_close
            FROM tick_data
            WHERE symbol = %(symbol)s AND date >= today() - 20
            """
            result = self.clickhouse.execute(query, {'symbol': symbol})
            
            if result:
                high, low, avg_close = result[0]
                return {
                    'resistance': high,
                    'support': low,
                    'pivot': (high + low + avg_close) / 3,
                    'current': avg_close  # You might want last_price instead
                }
            
            return {}
            
        except Exception as e:
            self.logger.error(f"Error calculating price levels for {symbol}: {e}")
            return {}
    
    def _get_confidence_factors(self, volume_anomaly: bool, 
                              options_flow: bool, sector_alignment: bool) -> List[str]:
        """Get list of confidence-boosting factors"""
        factors = []
        if volume_anomaly:
            factors.append("volume_anomaly")
        if options_flow:
            factors.append("options_flow")
        if sector_alignment:
            factors.append("sector_alignment")
        return factors
    
    def _get_sector_average(self, sector: str) -> float:
        """Get average price for sector (simplified)"""
        # This would typically query historical sector data
        # For now, return a placeholder
        return 10000.0  # Adjust based on actual data

# Integration with existing scanner
class ScannerSignalEnhancer:
    """
    Wrapper to integrate with existing scanner system
    """
    
    def __init__(self, signal_generator: UnifiedSignalGenerator):
        self.signal_generator = signal_generator
        self.logger = logging.getLogger(__name__)
    
    def process_scanner_output(self, scanner_signals: List[Dict]) -> List[EnhancedSignal]:
        """Process scanner output and enhance with market intelligence"""
        enhanced_signals = []
        
        for signal in scanner_signals:
            try:
                enhanced_signal = self.signal_generator.analyze_scanner_signal(signal)
                
                # Only return signals that pass suppression filters
                if enhanced_signal.signal_type != 'HOLD':
                    enhanced_signals.append(enhanced_signal)
                    
            except Exception as e:
                self.logger.error(f"Error enhancing signal {signal}: {e}")
                continue
        
        # Sort by edge score (highest probability first)
        enhanced_signals.sort(key=lambda x: x.edge_score, reverse=True)
        
        return enhanced_signals
    
    def get_signal_summary(self, enhanced_signals: List[EnhancedSignal]) -> Dict:
        """Get summary of enhanced signals"""
        summary = {
            'total_signals': len(enhanced_signals),
            'high_confidence_count': len([s for s in enhanced_signals if s.confidence.score > 70]),
            'average_edge_score': np.mean([s.edge_score for s in enhanced_signals]) if enhanced_signals else 0,
            'market_regime': enhanced_signals[0].confidence.regime.value if enhanced_signals else 'unknown',
            'institutional_flow': enhanced_signals[0].confidence.institutional_flow.value if enhanced_signals else 'unknown',
            'suppressed_count': len([s for s in enhanced_signals if s.suppression_reasons])
        }
        
        return summary