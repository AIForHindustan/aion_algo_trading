# News Symbol Parsing Flow: Name & Sector-Based Matching

This document explains how news symbols are parsed by **company name** and **sector** in the system, and how this data flows downstream.

## Overview

The system uses a **two-stage symbol parsing approach**:
1. **Upstream (Ingestion)**: `gift_nifty_gap.py` extracts symbols from news using company name matching and sectoral mapping
2. **Downstream (Consumption)**: `data_pipeline.py` retrieves news and calculates relevance using sector-based matching

---

## Stage 1: Upstream Symbol Extraction (`gift_nifty_gap.py`)

### 1.1 Company Name to Symbol Mapping

**Location**: `crawlers/gift_nifty_gap.py` (lines 148-217)

**Process**:
- Loads `token_lookup_enriched.json` to build a mapping of company names → symbols
- Creates variations of company names (removes "BANK", "LTD", "LIMITED", etc.)
- Only processes instruments from intraday crawler tokens
- Maps company name variations to symbols with metadata (exchange, instrument_type)

**Key Method**: `_load_company_symbol_mapping()`

```python
# Example mapping structure:
{
    "HDFC BANK": [
        {'symbol': 'HDFCBANK', 'full_symbol': 'NSE:HDFCBANK', 'exchange': 'NSE', 'instrument_type': 'EQ'},
        {'symbol': 'HDFCBANK', 'full_symbol': 'NSE:HDFCBANK-FUT', 'exchange': 'NSE', 'instrument_type': 'FUT'}
    ],
    "HDFC": [
        {'symbol': 'HDFCBANK', 'full_symbol': 'NSE:HDFCBANK', ...}
    ]
}
```

### 1.2 Symbol Extraction from News Text

**Location**: `crawlers/gift_nifty_gap.py` (lines 744-856)

**Method**: `_extract_symbols_from_news(news_item)`

**Process**:

#### Step 1: Company Name Matching
- Searches news text (title + content) for company names from `company_symbol_map`
- Matches company name variations (e.g., "HDFC BANK", "HDFC", "HDFC BANK LIMITED")
- **Only matches equity stocks** (excludes options, futures, indices)
- Extracts sector from `token_lookup_enriched.json` for matched symbols

```python
# Example: News title "HDFC Bank reports strong Q3 earnings"
# Matches: "HDFC BANK" → ['NSE:HDFCBANK']
# Sector extracted: "BANK"
```

#### Step 2: Sector-Based Index Routing
- **Banking sector** → Routes to `NSE:NIFTY BANK`
- **NIFTY 50 constituents** → Routes to `NSE:NIFTY 50`
- **Explicit index mentions** → Routes to specific indices (BANKNIFTY, FINNIFTY, NIFTY)

```python
# Example: Banking sector companies → NSE:NIFTY BANK
if 'BANK' in sectors:
    result_symbols.append('NSE:NIFTY BANK')

# Example: NIFTY 50 stocks → NSE:NIFTY 50
if stock in nifty50_stocks:
    result_symbols.append('NSE:NIFTY 50')
```

#### Step 3: Default Fallback
- If no symbols matched → Defaults to `['NSE:NIFTY BANK', 'NSE:NIFTY 50']`

### 1.3 Publishing to Redis

**Location**: `crawlers/gift_nifty_gap.py` (lines 611-663)

**Method**: `_publish_news_to_symbols(news_item, redis_client)`

**Storage Pattern**:
- For each extracted symbol, creates Redis key: `news:symbol:{symbol}`
- Stores news in **sorted set** (ZADD) with timestamp as score
- Also publishes to Redis channel: `news:symbol:{symbol}`
- TTL: 300 seconds (5 minutes)

**Redis Storage**:
```python
# Redis Key: news:symbol:NSE:HDFCBANK
# Redis Type: Sorted Set (ZSET)
# Score: timestamp
# Value: JSON string with news data
{
    "data": {
        "title": "...",
        "content": "...",
        "sentiment": 0.8,  # Numeric sentiment
        "volume_trigger": false
    },
    "source": "zerodha_pulse",
    "timestamp": "2025-01-07T10:30:00",
    "sentiment": 0.8
}
```

---

## Stage 2: Downstream News Retrieval (`data_pipeline.py`)

### 2.1 Symbol-to-News Key Mapping

**Location**: `intraday_scanner/data_pipeline.py` (lines 2383-2423)

**Method**: `_get_news_keys_for_symbol(symbol)`

**Process**:
- Maps trading symbols to multiple Redis keys for news lookup
- Handles futures/options by extracting underlying symbol
- Maps symbols to parent indices based on sector patterns

**Key Resolution Strategy**:
1. **F&O Instruments** (Futures/Options):
   - ✅ **ARCHITECTURAL FIX**: F&O instruments don't have their own news
   - News happens for underlying equity, which affects F&O markets
   - **Always map to underlying equity symbol** (e.g., `NSE:HDFCBANK-FUT` → `news:symbol:NSE:HDFCBANK`)
   - **Never try exact match for F&O** (news doesn't exist for F&O instruments)
   - Pattern detection will decide how to use that news for F&O trading actions

2. **Equity Symbols**:
   - **Exact match**: `news:symbol:{symbol}` (e.g., `news:symbol:NSE:HDFCBANK`)

3. **Sector-based index mapping** (for both equity and underlying of F&O):
   - Banking stocks → `news:symbol:NSE:NIFTY BANK`
   - NIFTY 50 stocks → `news:symbol:NSE:NIFTY 50`

```python
# Example: For F&O symbol "NSE:HDFCBANK-FUT"
# ✅ FIX: Never try exact match for F&O (news doesn't exist for F&O)
keys_to_try = [
    "news:symbol:NSE:HDFCBANK",      # Underlying equity symbol (primary)
    "news:symbol:NSE:NIFTY BANK",    # Banking sector index
    "news:symbol:NSE:NIFTY 50"       # NIFTY 50 index
]

# Example: For equity symbol "NSE:HDFCBANK"
keys_to_try = [
    "news:symbol:NSE:HDFCBANK",      # Exact match (primary)
    "news:symbol:NSE:NIFTY BANK",    # Banking sector index
    "news:symbol:NSE:NIFTY 50"       # NIFTY 50 index
]
```

### 2.2 Sector-Based Relevance Calculation

**Location**: `intraday_scanner/data_pipeline.py` (lines 2503-2571)

**Method**: `_calculate_news_relevance(symbol, news_data)`

**Process**:

#### Step 1: Extract Underlying Symbol
- ✅ **ARCHITECTURAL FIX**: For F&O instruments, always extract underlying equity symbol
- News relevance is calculated against underlying equity, not F&O instrument
- Pattern detection will decide how to use that news for F&O trading actions

#### Step 2: Direct Symbol Match
- Checks if underlying equity symbol name appears in news text (title + content)
- Returns **1.0** (highest relevance) if direct match

#### Step 3: Sector-Based Matching
- Extracts sector from symbol using `_get_sector_from_symbol_name()` (lines 2458-2501)
- Maps symbol to sector using keyword matching:
  - **BANK**: ['BANK', 'HDFC', 'ICICI', 'SBI', 'AXIS', 'KOTAK', ...]
  - **IT**: ['INFOSYS', 'TCS', 'WIPRO', 'HCL', 'TECHMAHINDRA', ...]
  - **AUTO**: ['MARUTI', 'TATA', 'MOTORS', 'BAJAJ', 'HERO', ...]
  - **PHARMA**: ['SUNPHARMA', 'DRREDDY', 'CIPLA', 'LUPIN', ...]
  - **FMCG**: ['HUL', 'ITC', 'NESTLE', 'DABUR', ...]
  - **ENERGY**: ['RELIANCE', 'ONGC', 'GAIL', 'IOC', ...]
  - **METAL**: ['TATASTEEL', 'JSWSTEEL', 'SAIL', ...]

- Checks if news text contains sector keywords
- Returns **0.8** (high relevance) for sector match

#### Step 4: Company-Specific Matching
- For specific sectors, checks for company name mentions
- Returns **0.9** (very high relevance) for specific company match

```python
# Example: F&O Symbol "NSE:HDFCBANK-FUT" → Underlying "HDFCBANK" (BANK sector)
# News: "Banking sector sees strong growth"
# Relevance: 0.8 (sector match)
# Pattern detection will use this news for F&O trading decisions

# Example: Equity Symbol "NSE:HDFCBANK" (BANK sector)
# News: "HDFC Bank reports strong earnings"
# Relevance: 0.9 (company-specific match)

# Example: F&O Symbol "NFO:BANKNIFTY25OCT54900CE" → Underlying "BANKNIFTY"
# News: "Banking sector sees strong growth"
# Relevance: 0.8 (sector match)
# Pattern detection will use this news for options trading decisions
```

### 2.3 News Retrieval Flow

**Location**: `intraday_scanner/data_pipeline.py` (lines 2294-2381)

**Method**: `_get_news_for_symbol(symbol)`

**Process**:
1. Checks cache first (5-minute expiry)
2. Tries multiple Redis keys from `_get_news_keys_for_symbol()`
3. For each news item found, calculates relevance score
4. Returns news with highest relevance score (> 0.0)

**Redis Retrieval**:
```python
# For each key in keys_to_try:
#   - ZREVRANGEBYSCORE news:symbol:{symbol} +inf -inf WITHSCORES LIMIT 0 10
#   - Parse JSON news data
#   - Calculate relevance_score = _calculate_news_relevance(symbol, news_data)
#   - Keep news with highest relevance_score
```

---

## Stage 3: Downstream Flow to Alert Manager

### 3.1 News Detection in Data Pipeline

**Location**: `intraday_scanner/data_pipeline.py` (lines 3735-3789)

**Method**: `_process_news_from_channel(data)`

**Process**:
- Receives news from Redis channel: `news:symbol:{symbol}`
- Extracts symbols from news data: `data.get("symbols", [])`
- If no symbols, infers from title (NIFTY, BANKNIFTY → `['NIFTY']`)
- **Always forwards** to `alert_manager.send_news_alert()` (no pre-filtering)

### 3.2 Alert Manager Processing

**Location**: `alerts/alert_manager.py` (lines 2661-2703)

**Method**: `ProductionAlertManager.send_news_alert(symbol, news_data)`

**Process**:
1. Creates news alert payload with:
   - `pattern: "NEWS_ALERT"`
   - `confidence: 0.9` (high confidence)
   - `action: "WATCH"` (informational)
   - `news_context`: Full news data
   - `news_impact`, `news_sentiment`, `news_title`, `news_source`, `news_link`

2. Calculates risk metrics via `RiskManager` (if available)

3. Routes through full production pipeline:
   - `send_alert()` → `RetailAlertFilter.should_send_alert()` → Filtering/Cooldowns
   - `_send_notifications()` → `notifiers.py` → Telegram/Redis/MacOS

### 3.3 Notifier Formatting

**Location**: `alerts/notifiers.py` (lines 1008-1016, 1070-1079)

**Process**:
- `NEWS_ALERT` pattern has dedicated template in `PATTERN_DESCRIPTIONS`
- `format_actionable_alert()` detects `NEWS_ALERT` and sets `has_trading_params = False`
- News alerts are treated as **informational** (no trading parameters required)

---

## Data Flow Summary

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. UPSTREAM: Symbol Extraction (gift_nifty_gap.py)            │
├─────────────────────────────────────────────────────────────────┤
│ News Item → _extract_symbols_from_news()                        │
│   ├─ Company Name Matching (company_symbol_map)                 │
│   ├─ Sector Extraction (token_lookup_enriched.json)             │
│   └─ Index Routing (BANK → NIFTY BANK, NIFTY 50 stocks → NIFTY)│
│                                                                 │
│ Result: ['NSE:HDFCBANK', 'NSE:NIFTY BANK']                      │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. REDIS STORAGE (gift_nifty_gap.py)                           │
├─────────────────────────────────────────────────────────────────┤
│ _publish_news_to_symbols()                                      │
│   ├─ Redis Key: news:symbol:NSE:HDFCBANK (ZSET)               │
│   ├─ Redis Key: news:symbol:NSE:NIFTY BANK (ZSET)              │
│   └─ Redis Channel: news:symbol:{symbol} (PUB/SUB)              │
│                                                                 │
│ Storage: Sorted Set with timestamp score, 5min TTL              │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. DOWNSTREAM: News Retrieval (data_pipeline.py)               │
├─────────────────────────────────────────────────────────────────┤
│ Trading Symbol: "NSE:HDFCBANK-FUT" (F&O instrument)           │
│   ↓                                                             │
│ _extract_underlying_from_symbol() → "HDFCBANK"                 │
│   ↓                                                             │
│ _get_news_keys_for_symbol()                                    │
│   ├─ ✅ Skip exact match (F&O has no news)                     │
│   ├─ news:symbol:NSE:HDFCBANK (underlying equity - PRIMARY)    │
│   ├─ news:symbol:NSE:NIFTY BANK (sector)                        │
│   └─ news:symbol:NSE:NIFTY 50 (index)                           │
│   ↓                                                             │
│ _get_news_for_symbol()                                         │
│   ├─ ZREVRANGEBYSCORE for each key                             │
│   └─ _calculate_news_relevance() for each news item            │
│       ├─ Uses underlying "HDFCBANK" for relevance              │
│       ├─ Direct match: 1.0                                     │
│       ├─ Sector match: 0.8                                      │
│       └─ Company match: 0.9                                    │
│   ↓                                                             │
│ Returns: News for underlying equity with highest relevance     │
│   ↓                                                             │
│ Pattern Detection: Uses news to make F&O trading decisions     │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ 4. NEWS ALERT CREATION (data_pipeline.py)                       │
├─────────────────────────────────────────────────────────────────┤
│ _process_news_from_channel()                                    │
│   ├─ Extract symbols from news: data.get("symbols", [])        │
│   ├─ Infer symbols from title if missing                       │
│   └─ Forward to alert_manager.send_news_alert(symbol, data)     │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ 5. ALERT MANAGER (alert_manager.py)                             │
├─────────────────────────────────────────────────────────────────┤
│ send_news_alert()                                               │
│   ├─ Create news alert payload                                  │
│   ├─ Calculate risk metrics (RiskManager)                      │
│   └─ send_alert() → Full production pipeline                   │
│       ├─ RetailAlertFilter.should_send_alert()                 │
│       ├─ Validation/Cooldowns                                  │
│       └─ _send_notifications() → notifiers.py                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ 6. NOTIFIERS (notifiers.py)                                    │
├─────────────────────────────────────────────────────────────────┤
│ format_actionable_alert()                                       │
│   ├─ NEWS_ALERT template (informational)                        │
│   └─ has_trading_params = False (no trading params required)   │
│                                                                 │
│ Output: Telegram/Redis/MacOS notification                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Methods Summary

### Upstream (Ingestion)
- **`gift_nifty_gap.py`**:
  - `_load_company_symbol_mapping()`: Loads company name → symbol mapping
  - `_extract_symbols_from_news()`: Extracts symbols by name and routes by sector
  - `_get_sector_from_symbol()`: Maps symbol to sector (BANK, IT, AUTO, etc.)
  - `_publish_news_to_symbols()`: Publishes to Redis keys and channels

### Downstream (Consumption)
- **`data_pipeline.py`**:
  - `_extract_underlying_from_symbol()`: Extracts underlying equity from F&O symbols
  - `_get_news_keys_for_symbol()`: Maps symbol to multiple Redis keys (F&O → underlying equity)
  - `_get_sector_from_symbol_name()`: Maps symbol to sector (same logic as upstream)
  - `_calculate_news_relevance()`: Calculates relevance using underlying equity for F&O
  - `_get_news_for_symbol()`: Retrieves news for underlying equity with highest relevance
  - `_process_news_from_channel()`: Forwards news to alert_manager

### Alert Processing
- **`alert_manager.py`**:
  - `send_news_alert()`: Creates news alert payload and routes through pipeline

- **`notifiers.py`**:
  - `format_actionable_alert()`: Formats NEWS_ALERT as informational

---

## Sector Mapping Reference

### Symbol → Sector Keywords

| Sector | Keywords |
|--------|----------|
| **BANK** | BANK, HDFC, ICICI, SBI, AXIS, KOTAK, INDUSIND, YESBANK, FEDERAL, IDFC, BANDHAN, RBL, UNION, PNB, CANARA, BANKOFBARODA, BANKOFINDIA |
| **IT** | INFOSYS, TCS, WIPRO, HCL, TECHMAHINDRA, LTIM, LTTS, PERSISTENT, MINDTREE, COFORGE, MPHASIS |
| **AUTO** | MARUTI, M&M, TATA, MOTORS, BAJAJ, HERO, EICHER, ASHOK, LEYLAND, TVS, MOTHERSUM |
| **PHARMA** | SUNPHARMA, DRREDDY, CIPLA, LUPIN, TORRENT, AUROBINDO, DIVIS, GLENMARK, CADILA |
| **FMCG** | HUL, ITC, NESTLE, DABUR, MARICO, BRITANNIA, GODREJ, TATA, CONSUMER |
| **ENERGY** | RELIANCE, ONGC, GAIL, IOC, BPCL, HPCL, OIL, PETRONET |
| **METAL** | TATASTEEL, JSWSTEEL, SAIL, JINDAL, VEDANTA, HINDALCO, NMDC |

### Sector → News Keywords (for relevance matching)

| Sector | News Keywords |
|--------|---------------|
| **BANK** | BANKING, BANK, FINANCIAL, CREDIT, LOAN, LENDING, INTEREST RATE |
| **AUTO** | AUTOMOTIVE, AUTO, VEHICLE, CAR, BIKE, MOTORCYCLE, AUTOMOBILE |
| **PHARMA** | PHARMACEUTICAL, DRUG, MEDICINE, HEALTHCARE, MEDICAL |
| **IT** | TECHNOLOGY, SOFTWARE, IT, DIGITAL, TECH, COMPUTER |
| **ENERGY** | OIL, GAS, ENERGY, POWER, PETROLEUM, REFINERY, CRUDE |
| **METAL** | STEEL, METAL, MINING, ALUMINIUM, IRON, COPPER |
| **FMCG** | FMCG, CONSUMER, FMCG PRODUCTS, RETAIL |
| **CEMENT** | CEMENT, CONSTRUCTION, BUILDING, INFRASTRUCTURE |

---

## Redis Storage Format

### Key Pattern
```
news:symbol:{symbol}
```

### Examples
- `news:symbol:NSE:HDFCBANK`
- `news:symbol:NSE:NIFTY BANK`
- `news:symbol:NSE:NIFTY 50`

### Data Structure
- **Type**: Sorted Set (ZSET)
- **Score**: Timestamp (float)
- **Value**: JSON string with news data
- **TTL**: 300 seconds (5 minutes)

### News Data Format
```json
{
    "data": {
        "title": "HDFC Bank reports strong Q3 earnings",
        "content": "...",
        "sentiment": 0.8,
        "volume_trigger": false
    },
    "source": "zerodha_pulse",
    "timestamp": "2025-01-07T10:30:00",
    "sentiment": 0.8,
    "volume_trigger": false
}
```

---

## Summary

✅ **Symbols are parsed by name**: Company name matching via `company_symbol_map` (loaded from `token_lookup_enriched.json`)

✅ **Symbols are parsed by sector**: Sector extraction from token lookup + sector-based index routing (BANK → NIFTY BANK, NIFTY 50 stocks → NIFTY 50)

✅ **F&O Architectural Fix**: F&O instruments (futures/options) don't have their own news. News happens for underlying equity, which affects F&O markets. System always maps F&O symbols to underlying equity for news lookup. Pattern detection decides how to use that news for F&O trading actions.

✅ **Downstream relevance**: `data_pipeline.py` uses underlying equity symbol for F&O instruments and sector-based matching to calculate news relevance scores (0.0-1.0)

✅ **Flow**: `gift_nifty_gap.py` (extract) → Redis (store) → `data_pipeline.py` (retrieve/relevance - F&O → underlying equity) → Pattern Detection (use news for F&O decisions) → `alert_manager.py` (create alert) → `notifiers.py` (format/notify)

