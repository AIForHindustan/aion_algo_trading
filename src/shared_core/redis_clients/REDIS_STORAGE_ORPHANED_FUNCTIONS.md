# Redis Storage - Orphaned Functions & Dead Code Analysis

## 🔴 CRITICAL: Missing Function Definitions (Will Cause Runtime Errors)

### 1. **`detect_market_maker_traps()` - CALLED BUT NOT DEFINED**
- **Line 1379**: Called in `run_pattern_detection()`
- **Status**: ❌ **Function is NOT defined** - will cause `AttributeError` at runtime
- **Action**: **REMOVE the call** (lines 1377-1379) or **ADD the function definition**

### 2. **`detect_premium_collection()` - CALLED BUT NOT DEFINED**
- **Line 1382**: Called in `run_pattern_detection()`
- **Status**: ❌ **Function is NOT defined** - will cause `AttributeError` at runtime
- **Action**: **REMOVE the call** (lines 1380-1382) or **ADD the function definition**

---

## ⚠️ ORPHANED Functions (Defined but Never Called)

### 1. **`detect_kow_signal_straddle()` (Line 1470)**
- **Status**: Defined but never called
- **Usage Check**: Only referenced in comments, never actually invoked
- **Action**: **REMOVE** (orphaned code)

### 2. **`publish_indicators_stream()` (Line 1288)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 3. **`subscribe_to_patterns()` (Line 1673)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 4. **`subscribe_to_indicators()` (Line 1697)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 5. **`get_latest_indicators()` (Line 1722)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 6. **`get_latest_patterns()` (Line 1754)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 7. **`get_active_categories()` (Line 1804)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 8. **`get_registry_metadata()` (Line 1816)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 9. **`get_data_summary()` (Line 541)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 10. **`cleanup_old_data()` (Line 507)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Note**: Has incomplete implementation (line 515: `pass` for ticks cleanup)
- **Action**: **REMOVE** (orphaned code) OR complete implementation and use

### 11. **`get_stored_data()` (Line 460)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 12. **`store_ohlc_data()` (Line 368)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 13. **`store_market_depth()` (Line 401)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 14. **`store_derivatives_data()` (Line 429)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 15. **`store_volume_bucket()` (Line 292)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 16. **`store_session_data()` (Line 332)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Action**: **REMOVE** (orphaned code) OR keep if intended for future use

### 17. **`store_tick_with_indicators()` (Line 588)**
- **Status**: Defined but never called
- **Usage Check**: No external calls found
- **Note**: Similar functionality exists in `detect_and_store_patterns()` + `publish_indicators_to_redis()`
- **Action**: **REMOVE** (orphaned code, duplicate functionality)

---

## ⚠️ UNUSED IMPORTS

### 1. **`resolve_indicator_field` (Line 34)**
- **Status**: Imported as `resolve_indicator_field` but **NEVER USED**
- **Action**: **REMOVE** unused import

---

## 📊 SUMMARY

### Critical Issues (Will Cause Runtime Errors):
- **2 functions called but not defined**: `detect_market_maker_traps()`, `detect_premium_collection()`

### Orphaned Functions (Defined but Never Used):
- **17 functions** that are defined but never called anywhere in the codebase

### Unused Imports:
- **1 import**: `resolve_indicator_field`

### Total Dead Code:
- **~1,200+ lines** of unused/orphaned code (estimated)

---

## 🎯 RECOMMENDED ACTIONS

### Immediate (Remove to Fix Runtime Errors):
1. Remove calls to undefined functions (`detect_market_maker_traps`, `detect_premium_collection`)
2. Remove unused import (`resolve_indicator_field`)

### Cleanup (Optional - Orphaned Code):
1. Remove all 17 orphaned functions if not needed for future use
2. If keeping any functions, add `# TODO: Future use` comments and document why they're kept

---

## ✅ VERIFIED ACTIVE FUNCTIONS (Keep These)

These functions ARE used and should be kept:
- `store_tick()` - Used in data pipeline
- `get_recent_ticks()` - Used internally
- `publish_indicators_to_redis()` - Used in scanner_main.py
- `batch_publish_all_indicators()` - Used internally
- `detect_and_store_patterns()` - Used internally
- `run_pattern_detection()` - Used internally
- `detect_straddle_patterns()` - Used internally
- `detect_range_bound_straddle()` - Used internally
- `detect_core_patterns()` - Used internally
- `store_patterns_in_redis()` - Used internally
- `publish_patterns_stream()` - Used internally
- All indicator calculation methods (RSI, EMA, MACD, etc.) - Used internally
- `get_patterns_from_registry()` - Used internally
- `get_pattern_config()` - Used internally
- `_resolve_token_to_symbol_for_storage()` - Used internally

