# Unified Volume & Profile System

## Overview
This module (`shared_core/volume_files/`) acts as the **Single Source of Truth** for all volume computations and profile generation. It unifies legacy logic into a coherent engine that handles:
1.  **Real-time Volume Metrics**: Cumulative volume, relative volume ratio (RVOL), and flow rates.
2.  **Baselines**: Asset-aware baselines (Equity vs F&O) with time-of-day, expiry, and moneyness adjustments.
3.  **Volume Profiles**: Real-time Point of Control (POC), Value Area (VA), and price-volume distribution.

---

## 🗺️ Architecture & Data Flow

```ascii
┌──────────────┐          ┌─────────────────────┐          ┌────────────────────────┐
│  Ingestion   │          │  UnfiedVolumeEngine │          │      Redis (DB 1)      │
│ (Crawlers)   │─────────►│  (Tick Processing)  │─────────►│    (Unified Storage)   │
└──────────────┘          └──────────┬──────────┘          └───────────┬────────────┘
       │                             │                                ▲
       │                             ▼                                │
       │                  ┌─────────────────────┐                     │
       │                  │   BaselineService   │                     │
       │                  │ (F&O/Equity Logic)  │─────────────────────┘
       │                  └─────────────────────┘
       │
                          ┌──────────────────────┐         ┌────────────────────────┐
       └─────────────────►│ VolumeProfileManager │────────►│  volume_profile:poc:*  │
                          │ (POC/VA Calculation) │         │ volume_profile:sess:*  │
                          └──────────────────────┘         └────────────────────────┘
```

---

## 📂 Module Components

| Script | Purpose |
|--------|---------|
| `unified_engine.py` | **Core Engine**. Processes ticks, utilizes `SessionTracker`, and orchestrates dual-writes (Legacy + Unified). |
| `baseline_service.py` | **Brain**. Calculates expected volume (baselines) applying F&O multipliers (Time, Expiry, Moneyness). |
| `session_tracker.py` | **State**. Manages in-memory state of the current trading session (cumulative volume, minutes elapsed). |
| `volume_computation_manager.py` | **Adapter**. Wrapper class ensuring backward compatibility for legacy consumers. |
| `key_usage_tracker.py` | **Monitor**. Tracks migration progress by monitoring unified vs legacy key writes. |
| `redis_volume_gateway.py` | **Gateway**. Exposes volume data to the rest of the system via a clean API. |

---

## 💾 Redis Data Structures (DB 1)

### 1. Volume State Keys
| Key Pattern | Type | Content |
|-------------|------|---------|
| `vol:session:{symbol}:{date}` | Hash | `cumulative_volume`, `minutes_elapsed`, `last_price`, `tick_count` |
| `vol:baseline:{symbol}` | Hash | `baseline_1min`, `avg_volume_20d`, `fno_adjustment`, `is_fno` |

### 2. Volume Profile Keys (Hash Format)

> [!NOTE]
> Volume Profiles currently use the `volume_profile:` prefix (Standard) and do NOT dual-write.

#### **Primary POC Key** (`volume_profile:poc:{symbol}`)
Stores the critical "Point of Control" and Value Area metrics for high-speed access.
```json
{
    "poc_price": "24500.50",       // Price level with highest volume
    "poc_volume": "150000",        // Volume traded at POC
    "value_area_high": "24600.00", // 70% Volume High Bound
    "value_area_low": "24400.00",  // 70% Volume Low Bound
    "profile_strength": "0.85",    // Concentration metric (0-1)
    "exchange_timestamp": "..."    // Last update time
}
```

#### **Session Profile Key** (`volume_profile:session:{symbol}:{date}`)
Stores the full profile, including distribution buckets and computed levels.
```json
{
    "poc_price": "...",
    "price_volume_distribution": "{'24500.00': 5000, '24500.05': 200...}", // JSON serialized
    "support_levels": "[24400, 24350]",      // JSON Array
    "resistance_levels": "[24600, 24750]"    // JSON Array
}
```

---

## ⚡ Usage Examples

### 1. Processing Ticks (New API)
```python
from shared_core.volume_files import UnifiedVolumeEngine

engine = UnifiedVolumeEngine(redis_client)
# Process a tick
result = engine.process_tick("NSENIFTY27JAN25000CE", {
    "volume": 500, 
    "price": 120.5, 
    "timestamp": "2025-12-21T10:00:00"
})

print(f"RVOL: {result['ratios']['cumulative']}") 
# Output: RVOL: 1.5 (Running at 1.5x expected volume)
```

### 2. Fetching Baselines (Legacy Wrapper)
```python
from shared_core.volume_files import VolumeComputationManager

manager = VolumeComputationManager(redis_client)
metrics = manager.calculate_volume_metrics("NSENIFTY", tick_data)
# Returns legacy dict structure compatible with all consumers
```

---

## 🛠️ Debugging & Verification

### 1. Volume State (Cumulative & Ratios)
Dual-writes ensure backward compatibility. Check both keys match:
```bash
# ✅ Legacy (Existing Dashboards)
redis-cli -n 1 HGETALL ind:NSENIFTY...

# 🆕 Unified (New Standard)
redis-cli -n 1 HGETALL vol:session:NSENIFTY...:{DATE}
```

### 2. Volume Profiles (POC & Levels)
Profile data is stored in unified DB 1 with `volume_profile:` prefix:
```bash
# Check Point of Control
redis-cli -n 1 HGETALL volume_profile:poc:NSENIFTY...
```

### 3. F&O Baselines
Verify adjustments for Options/Futures:
```bash
redis-cli -n 1 HGETALL vol:baseline:NSENIFTY...
# Look for: fno_adjustment > 1.0
```
