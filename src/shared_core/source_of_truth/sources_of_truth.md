# AION Trading System - Single Source of Truth Matrix

> **Last Updated:** 2025-12-19
> **Purpose:** Define who computes, where data is stored, and who consumes each calculation type.

## Overview

This document establishes the canonical source of truth for all calculations in the AION Trading System.
**Rule:** Each calculation type has exactly ONE authoritative source. Downstream consumers read only—never recalculate.

---

## Calculation Matrix

| Calculation Type | Who Computes | Where Stored | Consumers | TTL |
|-----------------|--------------|--------------|-----------|-----|
| **Greeks (Delta, Gamma, Theta, Vega, IV)** | `websocket_message_parser.py` → `GreekCalculator` | `ind:greeks:{symbol}` (Hash) | PatternDetector, DataPipeline, Scanner | 300s |
| **Volume Ratio** | `websocket_message_parser.py` → `VolumeComputationManager` | `volume_state:{token}` (Hash), tick_data | Scanner, PatternDetector | 300s |
| **Symbol Parsing** | `redis_key_standards.py` → `UniversalSymbolParser` | In-memory singleton | All modules | N/A |
| **Underlying Price** | `UniversalSymbolParser.get_underlying_price()` | `index:NSE*`, `index_hash:*` (Hash) | GreekCalculator, Calculations | 60s |
| **Technical Indicators (EMA, RSI, MACD, ATR)** | `calculations.py` → `HybridCalculations` | `ind:{symbol}` (Hash) | PatternDetector, Scanner | 300s |
| **Microstructure (OFI, OBI, CVD)** | `websocket_message_parser.py` | `ind:{symbol}` (Hash) | PatternDetector | 60s |
| **ATM Strike Resolution** | `intraday_crawler_instruments.json` | JSON file | Crawler, Scanner | Static |
| **Instrument Tokens** | `intraday_crawler_instruments.json` | JSON file + `instrument_info` | All crawlers | Static |
| **VIX Regime** | `thresholds.py` → `get_safe_vix_regime()` | In-memory cache (180s TTL) | AlertBuilder, PatternDetector | 180s |
| **Market Calendar** | `market_calendar.py` → `MarketCalendar` | In-memory + pandas_market_calendars | All time-aware modules | Daily |
| **Redis Key Standards** | `redis_key_standards.py` → `DatabaseAwareKeyBuilder` | N/A (generates keys) | All Redis writers | N/A |
| **Lot Sizes** | `instrument_config.py` → `INSTRUMENT_CONFIGS` | Hardcoded config | AlertBuilder, Executor | Static |

---

## Key Files

| Component | File Path |
|-----------|-----------|
| Symbol Parser | `shared_core/redis_clients/redis_key_standards.py` |
| Greek Calculator | `zerodha/crawlers/greeks.py` |
| Volume Manager | `shared_core/volume_files/volume_computation_manager.py` |
| Technical Calculations | `intraday_trading/intraday_scanner/calculations.py` |
| VIX Thresholds | `shared_core/config_utils/thresholds.py` |
| Market Calendar | `shared_core/market_calendar.py` |
| Instrument Config | `intraday_trading/config/instrument_config.py` |
| Redis Key Builder | `shared_core/redis_clients/redis_key_standards.py` |

---

## Redis Key Patterns (DB 1)

| Key Pattern | Type | Description |
|-------------|------|-------------|
| `ind:greeks:{symbol}` | Hash | Greeks: delta, gamma, theta, vega, iv |
| `ind:{symbol}` | Hash | Technical indicators: ema_*, rsi, macd, atr |
| `index:NSE{INDEX}` | String/Hash | Index spot prices (NIFTY 50, BANK NIFTY) |
| `index_hash:NSE{INDEX}` | Hash | Index data with last_price, volume |
| `volume_state:{token}` | Hash | Cumulative volume, baseline, ratio |
| `tick:{symbol}:latest` | Hash | Latest tick data |
| `ticks:intraday:processed` | Stream | Processed ticks for scanner |
| `alerts:stream` | Stream | Trading alerts |

---

## Anti-Patterns (DO NOT)

1. ❌ **Never recalculate Greeks in PatternDetector** - Read from `ind:greeks:{symbol}`
2. ❌ **Never recalculate volume_ratio in calculations.py** - Read from tick_data
3. ❌ **Never import from `shared_core.universal_symbol_parser`** - DELETED, use `redis_key_standards`
4. ❌ **Never create Redis clients without UnifiedRedisManager** - Causes connection leaks
5. ❌ **Never store user data in DB 1** - DB 0 is for users, DB 1 is for market data

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     ZERODHA WEBSOCKET                            │
└─────────────────────────────┬───────────────────────────────────┘
                              │ Binary Packets
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              websocket_message_parser.py                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │ GreekCalculator │  │ VolumeManager   │  │ Microstructure  │  │
│  │ (delta,gamma,   │  │ (volume_ratio)  │  │ (OFI,OBI,CVD)   │  │
│  │  theta,vega,iv) │  │                 │  │                 │  │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘  │
└───────────┼─────────────────────┼─────────────────────┼──────────┘
            │                     │                     │
            ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                         REDIS DB 1                               │
│  ind:greeks:{symbol}    volume_state:{token}    ind:{symbol}     │
│  ticks:intraday:processed (Stream)                               │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              DataPipeline → PatternDetector → AlertBuilder       │
│                    (READ ONLY from Redis)                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Maintenance

When adding new calculations:
1. Add entry to the matrix above
2. Define ONE authoritative source
3. Store in Redis with appropriate TTL
4. Document consumers
5. Update this file
