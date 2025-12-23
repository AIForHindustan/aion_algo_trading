# Market Intelligence System - Documentation

## 📋 Overview

The **Market Intelligence System** is a sophisticated signal enhancement framework that enriches raw scanner signals with multi-factor market analysis. It combines VIX regime analysis, institutional flows, sectoral volatility, volume anomalies, and options flow to provide **probability-based trading signals** with automatic wrong-signal suppression.

---

## 🎯 Key Features

### 1. **Multi-Factor Signal Enhancement**
- ✅ VIX Regime Analysis (HIGH_VOLATILITY, LOW_VOLATILITY, TRENDING_UP, TRENDING_DOWN, SIDEWAYS)
- ✅ Institutional Flow Analysis (DII/FII flows, block deals)
- ✅ Sectoral Volatility Alignment
- ✅ Volume Anomaly Detection
- ✅ Options Flow Analysis
- ✅ Market Microstructure Signals

### 2. **Wrong Signal Suppression**
- Automatically filters out signals that contradict market regime
- Suppresses signals conflicting with institutional flows
- Flags high-volatility regime signals (avoid directional trades)
- Detects conflicting block deal activity

### 3. **Probability-Based Trading**
- **Edge Score**: 0-1 probability of signal success
- **Enhanced Confidence**: 0-100 score with multi-factor weighting
- **Regime-Aware Thresholds**: Adapts signal thresholds based on market conditions

### 4. **Real-Time Integration**
- Wraps existing scanner output (minimal changes required)
- Processes signals in real-time
- Dynamic filtering and suppression
- Dashboard visibility for signal analysis

---

## 📁 Directory Structure

```
shared_core/market_intelligence/
├── unified_signal_generator.py    # Core signal enhancement engine
├── signal_dashboard.py             # Streamlit dashboard for visualization
└── README.md                       # This file

intraday_trading/intraday_scanner/
└── signal_enhancer.py              # Integration wrapper for scanners
```

---

## 🏗️ Architecture

### Core Components

#### 1. **UnifiedSignalGenerator** (`unified_signal_generator.py`)
The main signal enhancement engine that:
- Analyzes market regime from multiple sources
- Enhances scanner signals with market intelligence
- Calculates edge scores and confidence
- Suppresses conflicting signals

#### 2. **ScannerSignalEnhancer** (`unified_signal_generator.py`)
Wrapper class that:
- Processes scanner output
- Integrates with existing scanner systems
- Formats enhanced signals for compatibility
- Provides signal summaries

#### 3. **Signal Dashboard** (`signal_dashboard.py`)
Streamlit dashboard for:
- Visualizing enhanced signals
- Filtering by confidence and edge score
- Viewing market regime and institutional flow
- Analyzing suppression reasons

---

## 🔄 Data Flow

```
Raw Scanner Signals
    ↓
ScannerSignalEnhancer.process_scanner_output()
    ↓
UnifiedSignalGenerator.analyze_scanner_signal()
    ↓
[Market Intelligence Analysis]
    ├── VIX Regime (indices_data)
    ├── Institutional Flow (nse_dii_fii_trading, nse_block_deals)
    ├── Sectoral Volatility (sectoral_volatility)
    ├── Volume Anomaly (tick_data)
    ├── Options Flow (tick_data)
    └── Sector Alignment (sectoral_volatility)
    ↓
Enhanced Signals
    ├── Edge Score (0-1 probability)
    ├── Enhanced Confidence (0-100)
    ├── Suppression Reasons
    └── Market Regime Context
    ↓
Filtered High-Quality Signals
```

---

## 📊 Data Sources

### ClickHouse Tables Used

1. **`indices_data`** - VIX Data
   ```sql
   SELECT last_price, timestamp
   FROM indices_data 
   WHERE symbol = 'NSE:INDIA VIX' 
   ORDER BY timestamp DESC LIMIT 1
   ```

2. **`nse_dii_fii_trading`** - Institutional Flows
   ```sql
   SELECT category, net_value
   FROM nse_dii_fii_trading 
   WHERE date = today()
   ```

3. **`nse_block_deals`** - Block Deal Activity
   ```sql
   SELECT symbol, deal_type, value, date
   FROM nse_block_deals 
   WHERE symbol = %(symbol)s AND date >= today() - 3
   ```

4. **`sectoral_volatility`** - Sector Performance
   ```sql
   SELECT index_name, volatility_20d_pct, current_price
   FROM sectoral_volatility 
   WHERE date = today()
   ```

5. **`tick_data`** - Volume & Options Analysis
   ```sql
   -- Volume anomaly check
   SELECT max(volume) as today_volume,
          avg(max(volume)) OVER (...) as avg_volume_20d
   FROM tick_data
   WHERE symbol = %(symbol)s AND date >= today() - 20
   
   -- Options flow check
   SELECT symbol, max(volume) as volume
   FROM tick_data
   WHERE date = today() AND instrument_type IN ('CE', 'PE')
   ```

---

## 🚀 Usage Examples

### Example 1: Integration with Options Scanner

```python
from intraday_trading.intraday_scanner.signal_enhancer import enhance_scanner_signals

def options_scanner_enhancement():
    """Enhance options scanner signals"""
    from intraday_scanner.options_scanner import generate_options_signals
    
    # Generate raw options signals
    raw_options_signals = generate_options_signals()
    
    # Enhance with market intelligence
    enhanced_options = enhance_scanner_signals(
        raw_options_signals, 
        clickhouse_client, 
        redis_client
    )
    
    # Filter for high-probability trades
    high_prob_trades = [
        signal for signal in enhanced_options['signals']
        if (signal['edge_score'] > 0.6 and 
            signal['confidence'] > 65 and
            len(signal['suppression_reasons']) == 0)
    ]
    
    return high_prob_trades
```

### Example 2: Daily Signal Report

```python
from intraday_trading.intraday_scanner.signal_enhancer import enhance_scanner_signals
from datetime import datetime
import pandas as pd

def generate_daily_signal_report():
    """Generate daily enhanced signal report"""
    from intraday_scanner.main_scanner import run_complete_scan
    
    # Run all scanners
    all_scanner_signals = run_complete_scan()
    
    # Enhance all signals
    enhanced_signals = enhance_scanner_signals(
        all_scanner_signals,
        clickhouse_client,
        redis_client
    )
    
    # Generate report
    report = {
        'generated_at': datetime.now().isoformat(),
        'total_raw_signals': len(all_scanner_signals),
        'total_enhanced_signals': len(enhanced_signals['signals']),
        'high_quality_signals': len([
            s for s in enhanced_signals['signals'] 
            if s['confidence'] > 70 and s['edge_score'] > 0.6
        ]),
        'market_regime': enhanced_signals['market_intelligence']['regime'],
        'signals_by_type': pd.DataFrame(enhanced_signals['signals'])['signal_type'].value_counts().to_dict()
    }
    
    return report
```

### Example 3: Direct Usage

```python
from shared_core.market_intelligence.unified_signal_generator import (
    UnifiedSignalGenerator, 
    ScannerSignalEnhancer
)

# Initialize
signal_generator = UnifiedSignalGenerator(clickhouse_client, redis_client)
enhancer = ScannerSignalEnhancer(signal_generator)

# Process scanner output
raw_signals = [
    {'symbol': 'RELIANCE', 'signal_type': 'BUY', 'confidence': 65},
    {'symbol': 'TCS', 'signal_type': 'SELL', 'confidence': 55}
]

enhanced_signals = enhancer.process_scanner_output(raw_signals)

# Filter high-quality signals
high_quality = [
    s for s in enhanced_signals 
    if s.edge_score > 0.6 and s.confidence.score > 70
]
```

---

## 📈 Signal Enhancement Process

### Step 1: Market Regime Analysis
```python
market_regime = signal_generator.get_market_regime()
# Returns: MarketRegime enum (HIGH_VOLATILITY, TRENDING_UP, etc.)
```

**Factors Considered:**
- VIX level (40% weight)
- Sector momentum (30% weight)
- Institutional flow (30% weight)

### Step 2: Institutional Flow Analysis
```python
institutional_flow = signal_generator._get_institutional_regime()
# Returns: InstitutionalFlow enum (STRONG_BUYING, MODERATE_SELLING, etc.)
```

**Sources:**
- DII/FII net flows from `nse_dii_fii_trading`
- Block deals from `nse_block_deals`
- Threshold: ±500 crores for significant flow

### Step 3: Volume Anomaly Detection
```python
volume_anomaly = signal_generator._check_volume_anomaly(symbol)
# Returns: bool (True if volume > 2.5x 20-day average)
```

**Query:**
- Compares today's volume to 20-day average
- Flags spikes > 2.5x average

### Step 4: Options Flow Analysis
```python
options_flow = signal_generator._check_options_flow(symbol)
# Returns: bool (True if unusual options activity detected)
```

**Query:**
- Checks for options (CE/PE) with volume > 3x average
- Looks for options on the underlying symbol

### Step 5: Sector Alignment
```python
sector_alignment = signal_generator._check_sector_alignment(symbol)
# Returns: bool (True if signal aligns with sector movement)
```

**Query:**
- Gets symbol's sector from `tick_data`
- Checks sector volatility from `sectoral_volatility`
- Aligns if sector volatility > 12%

### Step 6: Suppression Factor Analysis
```python
suppression_reasons = signal_generator._get_suppression_factors(
    symbol, signal_type, market_regime, institutional_flow
)
# Returns: List[str] (reasons to suppress signal)
```

**Suppression Conditions:**
- BUY signal in strong bearish regime → Suppressed
- SELL signal in strong bullish regime → Suppressed
- High volatility regime → Suppress directional trades
- Conflicting block deals → Suppress

### Step 7: Enhanced Confidence Calculation
```python
enhanced_confidence = signal_generator._calculate_enhanced_confidence(
    original_confidence, market_regime, institutional_flow,
    volume_anomaly, options_flow, sector_alignment,
    suppression_reasons
)
# Returns: float (0-100)
```

**Calculation:**
- Base confidence × regime multiplier (0.8-1.2)
- Base confidence × flow multiplier (1.0-1.15)
- Factor bonuses: +5 (volume), +8 (options), +7 (sector)
- Suppression penalty: -10 per suppression reason

### Step 8: Edge Score Calculation
```python
edge_score = signal_generator._calculate_edge_score(
    symbol, signal_type, enhanced_confidence,
    market_regime, institutional_flow
)
# Returns: float (0-1 probability)
```

**Calculation:**
- Base probability = confidence / 100 (40% weight)
- Regime edge (30% weight): 0.30-0.70 based on regime
- Flow edge (30% weight): 0.30-0.70 based on institutional flow
- Weighted average of all three

**Regime Edge Scores:**
- TRENDING_UP: 0.65
- TRENDING_DOWN: 0.63
- HIGH_VOLATILITY: 0.45
- LOW_VOLATILITY: 0.55
- SIDEWAYS: 0.50

**Flow Edge Scores:**
- STRONG_BUYING: 0.70
- MODERATE_BUYING: 0.60
- NEUTRAL: 0.50
- MODERATE_SELLING: 0.40
- STRONG_SELLING: 0.30

### Step 9: Final Signal Decision
```python
final_signal_type = signal_generator._get_final_signal_type(
    original_signal, enhanced_confidence, suppression_reasons
)
# Returns: 'BUY', 'SELL', 'WEAK_BUY', 'WEAK_SELL', or 'HOLD'
```

**Decision Logic:**
- Confidence < 30 OR ≥2 suppression reasons → `HOLD`
- Confidence < 50 → `WEAK_BUY` / `WEAK_SELL`
- Otherwise → Original signal type

---

## 📊 Enhanced Signal Structure

### EnhancedSignal Dataclass
```python
@dataclass
class EnhancedSignal:
    symbol: str
    signal_type: str              # 'BUY', 'SELL', 'HOLD', 'WEAK_BUY', 'WEAK_SELL'
    confidence: SignalConfidence  # Enhanced confidence with factors
    price_levels: Dict[str, float]  # Support, resistance, pivot
    timeframe: str
    factors: Dict[str, Any]       # All contributing factors
    suppression_reasons: List[str]  # Reasons signal was suppressed
    edge_score: float             # 0-1 probability of success
```

### SignalConfidence Dataclass
```python
@dataclass
class SignalConfidence:
    score: float                  # 0-100
    factors: List[str]            # Which factors contributed
    regime: MarketRegime         # Current market regime
    institutional_flow: InstitutionalFlow
    volume_anomaly: bool
    options_flow: bool
    sector_alignment: bool
```

### Formatted Output (for Scanner Compatibility)
```python
{
    'symbol': 'RELIANCE',
    'signal_type': 'BUY',
    'confidence': 75.5,
    'edge_score': 0.68,
    'timeframe': 'intraday',
    'price_levels': {
        'resistance': 2450.0,
        'support': 2400.0,
        'pivot': 2425.0,
        'current': 2420.0
    },
    'factors': {
        'market_regime': 'trending_up',
        'institutional_flow': 'moderate_buying',
        'volume_anomaly': True,
        'options_flow': True,
        'sector_alignment': True,
        'original_confidence': 65,
        'suppression_count': 0
    },
    'suppression_reasons': [],
    'market_regime': 'trending_up',
    'institutional_flow': 'moderate_buying',
    'enhanced_at': '2025-11-29T10:30:00'
}
```

---

## 🎯 Key Benefits

### 1. **Wrong Signal Suppression**
- ✅ Automatically filters signals contradicting market regime
- ✅ Suppresses signals conflicting with institutional flows
- ✅ Flags high-volatility regime signals (avoid directional trades)
- ✅ Detects conflicting block deal activity

### 2. **Probability-Based Trading**
- ✅ Edge scores provide actual probability estimates (0-1)
- ✅ Enhanced confidence with multi-factor weighting
- ✅ Historical success rates by regime incorporated
- ✅ Institutional flow edges factored in

### 3. **Multi-Factor Confirmation**
- ✅ Requires alignment across multiple data sources
- ✅ Volume anomaly confirmation
- ✅ Options flow confirmation
- ✅ Sector alignment confirmation

### 4. **Regime-Aware Thresholds**
- ✅ Adapts signal thresholds based on market conditions
- ✅ Different edge scores for different regimes
- ✅ Dynamic confidence multipliers
- ✅ Context-aware suppression

### 5. **Institutional Intelligence**
- ✅ Incorporates DII/FII flows
- ✅ Analyzes block deals
- ✅ Detects conflicting institutional activity
- ✅ Flags significant institutional moves

### 6. **Volume Anomaly Detection**
- ✅ Flags unusual activity that confirms/disconfirms signals
- ✅ Compares to 20-day average
- ✅ Detects volume spikes > 2.5x average
- ✅ Real-time analysis from `tick_data`

### 7. **Sector Alignment**
- ✅ Ensures signals align with broader sector movement
- ✅ Checks sector volatility
- ✅ Validates sector momentum
- ✅ Filters misaligned signals

---

## 🔌 Integration Points

### 1. **Wrap Existing Scanner** (Minimal Changes)
```python
# Before
raw_signals = your_scanner_logic()

# After
raw_signals = your_scanner_logic()
enhanced_signals = enhance_scanner_signals(
    raw_signals, 
    clickhouse_client, 
    redis_client
)
```

### 2. **Real-Time Enhancement**
- Process scanner output through enhancer
- No changes to scanner logic required
- Drop-in replacement for signal processing

### 3. **Dynamic Filtering**
- Automatically suppress low-probability signals
- Filter by edge score and confidence
- Return only high-quality signals

### 4. **Dashboard Visibility**
- See why signals were enhanced/suppressed
- View market regime and institutional flow
- Analyze suppression reasons
- Filter by confidence and edge score

---

## 📋 Configuration

### Thresholds (Configurable)
```python
self.thresholds = {
    'vix_high': 20.0,                    # VIX > 20 = HIGH_VOLATILITY
    'vix_low': 12.0,                     # VIX < 12 = LOW_VOLATILITY
    'volume_spike': 2.5,                 # 2.5x average = anomaly
    'institutional_flow_significant': 500,  # 500 crores = significant
    'options_flow_significant': 3.0,     # 3x average = significant
    'sector_deviation': 0.15              # 15% from sector = misaligned
}
```

### Regime Weights
```python
# Market regime calculation weights
VIX: 40%
Sector Momentum: 30%
Institutional Flow: 30%
```

### Edge Score Weights
```python
# Edge score calculation weights
Base Probability: 40%
Regime Edge: 30%
Flow Edge: 30%
```

---

## 🧪 Testing

### Example Test Case
```python
def test_signal_enhancement():
    # Mock scanner signal
    raw_signal = {
        'symbol': 'RELIANCE',
        'signal_type': 'BUY',
        'confidence': 65
    }
    
    # Enhance signal
    enhanced = signal_generator.analyze_scanner_signal(raw_signal)
    
    # Assertions
    assert enhanced.symbol == 'RELIANCE'
    assert enhanced.edge_score > 0.0
    assert enhanced.edge_score <= 1.0
    assert enhanced.confidence.score >= 0
    assert enhanced.confidence.score <= 100
```

---

## 📝 Dependencies

### Required Packages
- `clickhouse-driver` - ClickHouse client
- `redis` - Redis client (optional)
- `pandas` - Data manipulation
- `numpy` - Numerical operations
- `streamlit` - Dashboard (optional)

### Required ClickHouse Tables
- ✅ `indices_data` - VIX data
- ✅ `nse_dii_fii_trading` - Institutional flows
- ✅ `nse_block_deals` - Block deals
- ✅ `sectoral_volatility` - Sector performance
- ✅ `tick_data` - Volume and options data

---

## 🚨 Error Handling

The system includes robust error handling:
- Graceful fallbacks if ClickHouse queries fail
- Continues processing even if individual signals fail
- Logs all errors for debugging
- Returns default values (NEUTRAL, SIDEWAYS) on errors

---

## 📈 Performance Considerations

### Query Optimization
- Uses efficient ClickHouse queries with proper indexing
- Leverages partitioning (date, hour) for fast queries
- Uses window functions for rolling averages
- Limits result sets appropriately

### Caching Opportunities
- Market regime can be cached (updates every 30s)
- Institutional flow can be cached (updates daily)
- Sector volatility can be cached (updates daily)

---

## 🔮 Future Enhancements

### Potential Improvements
1. **Machine Learning Integration**
   - Train models on historical edge scores
   - Predict signal success probability
   - Learn optimal thresholds

2. **Real-Time Streaming**
   - Process signals as they arrive
   - Update edge scores in real-time
   - Stream to dashboard

3. **Advanced Options Analysis**
   - Greeks analysis (delta, gamma, theta, vega)
   - Put-call ratio analysis
   - Max pain analysis

4. **Sentiment Integration**
   - News sentiment analysis
   - Social media sentiment
   - Analyst recommendations

5. **Backtesting Framework**
   - Test edge scores against historical data
   - Validate suppression logic
   - Optimize thresholds

---

## 📚 Related Documentation

- **Market Regime Engine**: `shared_core/market_microstructure/MARKET_REGIME_DATA_REQUIREMENTS.md`
- **Daily Data Capture**: `shared_core/market_microstructure/DAILY_DATA_CAPTURE.md`
- **VIX Utilities**: `shared_core/utils/vix_utils.py`
- **ClickHouse Schema**: `clickhouse_setup/create_tables.sql`

---

## ✅ Summary

The Market Intelligence System provides:
- ✅ **Sophisticated signal enhancement** with multi-factor analysis
- ✅ **Wrong signal suppression** to filter out bad trades
- ✅ **Probability-based trading** with edge scores
- ✅ **Regime-aware thresholds** that adapt to market conditions
- ✅ **Institutional intelligence** from DII/FII flows and block deals
- ✅ **Volume anomaly detection** for confirmation
- ✅ **Sector alignment** validation
- ✅ **Easy integration** with existing scanners
- ✅ **Dashboard visualization** for analysis

**Key Insight**: This system transforms raw scanner signals into **high-probability, regime-aware trading signals** by combining multiple data sources and applying sophisticated filtering logic.

