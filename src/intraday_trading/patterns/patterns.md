Order Flow Breakout
- Trigger: `volume_ratio >= min_volume` AND `abs(order_flow_imbalance) >= min_ofi`.
- Signal: BUY if `order_flow_imbalance > 0`, else SELL.
- Confidence: `0.55 + min(0.35, abs(ofi) - min_ofi) + min(0.1, (volume_ratio - min_volume)/max(min_volume,1))`, capped at 0.99 via builder.
- Defaults: `min_ofi=0.35`, `min_volume=1.0` unless overridden by `pattern_thresholds`.
- Metadata: `{order_flow_imbalance, order_book_imbalance, volume_ratio}`.
- Code: `src/intraday_trading/patterns/advanced.py:108` class `OrderFlowBreakoutDetector.detect`.

Gamma Exposure Reversal
- Trigger: `abs(gamma_exposure) >= min_gamma` AND `volume_ratio >= min_volume`.
- Signal: SELL if `gamma_exposure > 0` else BUY.
- Confidence: `0.6 + min(0.3, abs(gamma_exposure) / (min_gamma*5))`.
- Defaults: `min_gamma=1e-5`, `min_volume=0.5` via `pattern_thresholds`.
- Metadata: `{gamma_exposure, delta, volume_ratio}`.
- Code: `src/intraday_trading/patterns/advanced.py:169` class `GammaExposureReversalDetector.detect`.

Microstructure Divergence
- Compute: `divergence = order_flow_imbalance * order_book_imbalance`.
- Trigger: `abs(divergence) >= min_divergence` (magnitude check) and `last_price` present.
- Signal: SELL if `divergence > 0` (alignment → reversal risk), else BUY.
- Confidence: `min(0.99, 0.55 + abs(divergence)/100.0)`.
- Default: `min_divergence=0.05` via `pattern_thresholds`.
- Metadata: `{order_flow_imbalance, order_book_imbalance, microprice, volume_ratio, divergence}`.
- Code: `src/intraday_trading/patterns/advanced.py:206` class `MicrostructureDivergenceDetector.detect`.

Cross-Asset Arbitrage
- Compute: `spread = last_price - theoretical_price` (USDINR/Gold linkage).
- Trigger: `abs(spread) >= min_spread` AND `abs(correlation) >= 0.3` and prices present.
- Signal: SELL if `spread > 0` (rich vs theoretical), else BUY.
- Confidence: `0.55 + min(0.3, abs(spread)/max(min_spread,1))`.
- Default: `min_spread=15.0` via `pattern_thresholds`.
- Metadata: `{spread, sensitivity, correlation, theoretical_price}`.
- Code: `src/intraday_trading/patterns/advanced.py:330` class `CrossAssetArbitrageDetector.detect`.

VIX Momentum
- Regime: `vix_regime ∈ {LOW,NORMAL,HIGH,PANIC}`; `min_volume` from centralized thresholds or `pattern_thresholds`.
- Trigger: `volume_ratio >= min_volume` AND `price_change_pct` present.
- Direction filter: Bullish if `(vix_regime in {LOW,NORMAL} and price_change_pct > 0)` OR `(vix_regime in {HIGH,PANIC} and price_change_pct > 0.75)`; bearish mirror when `price_change_pct < 0`.
- Signal: BUY if `price_change_pct > 0` else SELL.
- Confidence: `0.55 + min(0.3, abs(price_change_pct)/2)`.
- Metadata: `{vix_value, vix_regime, price_change_pct, volume_ratio}`.
- Code: `src/intraday_trading/patterns/advanced.py:356` class `VIXRegimeMomentumDetector.detect`.

ICT Iron Condor (Strategy)
- Preconditions: price available; market is range-bound (narrow recent range vs instrument thresholds); volatility suitable (elevated enough for premium); within ICT Kill Zone (volume-confirmed if available).
- Inputs: FVG levels from recent candles; support/resistance from range analysis.
- Strikes: Use latest bullish FVG for put, latest bearish FVG for call; fall back to support/resistance. Ensure min OTM distance; long wings offset by `spread_width`.
- Premiums: Prefer option chain premiums; else heuristic estimate from range width. Compute `net_credit`.
- Risk: `max_loss = (short_call - long_call)*lot_size - net_credit`; size lots via `max_risk_per_trade / max_loss` (cap 5 lots).
- Trigger: `net_credit >= min_credit` (instrument-specific). On trigger, attach algo payload via UnifiedAlertBuilder and record `alert_id`/`strategy_id`.
- Metadata: full strikes, premiums, position_size, risk, context (range, vol, killzone, fvg_used), expiry.
- Code: `src/intraday_trading/patterns/ict/ict_iron_condor.py:342` method `generate_iron_condor_signal`; strike logic `:266`; premiums `:520` approx; killzone `:64`.

Game Theory Signal
- Data: optionally order book and recent ticks; lenient if unavailable; uses instrument config `game_theory_min_confidence`.
- Trigger flow: fetch instrument config; gather order_book (optional) and `min_tick_count` ticks (proceed if fewer); run `IntradayGameTheoryEngine.generate_intraday_signals` or simplified rules.
- Confidence gate: require `confidence >= min_confidence`.
- Signal mapping: LONG→BUY, SHORT→SELL; adjust confidence by VIX regime multiplier; require valid `last_price`.
- Simplified rules (fallback):
  - BUY if `delta > 0.6 and volume_ratio > max(min_volume_ratio, 2.0)`; SELL if `delta < 0.4 and same vol`.
  - Else if `abs(order_flow_imbalance) > 20` → direction by sign.
  - Else if `abs(microprice - last_price)/last_price > 0.002` → direction by microprice vs price.
  - Else if `gamma_exposure` sign → SHORT if positive, LONG if negative.
  - Require `confidence >= 0.6` in fallback.
- Output: standard `create_pattern(...)` payload plus `algo_alert` via UnifiedAlertBuilder.
- Code: `src/intraday_trading/patterns/game_theory_pattern_detector.py:31` class `GameTheoryPatternDetector.detect`; standardization at `:180`; simplified generator `:212`.

Notes
- Excluded: `kow_signal_straddle` is disabled per registry and not documented here.
