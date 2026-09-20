# 2026-07-20 Strategy Validation Round 2 — Situation × Strategy Grammar

## Fixed objective

Test whether a fixed strategy grammar has incremental candidate-basket value inside an ex-ante market situation. This is the next registered hierarchy after Round 1 single-axis exploration; it is not an operating-rule search.

## Fixed contract

- Same universe, price contract, actual-session execution, h5 holding period, costs, Train interval, and minimum coverage rule as Round 1 (`20260720_strategy_validation_round1.md`).
- Population: point-in-time KOSPI/KOSDAQ common-stock membership joined by (as_of_date, code).
- Primary metric: signal-date equal-weight net h5 candidate basket return.
- Incremental comparison: same-date eligible-universe basket return. Since a situation is date-level, this is also the strategy result relative to that situation's eligible universe.

## Pre-registered combinations

- Situations: BULL, BEAR, SIDEWAYS, STRESS, TRANSITION.
- Strategy grammars: MR_BOLLINGER, MA_CROSS_UP, BREAKOUT_252D.
- Exactly 15 situation × strategy combinations. No signal, Top-N, parameter, or liquidity-rule additions.
- Train-only exploratory output. No automatic selection, Validation/OOS reading, promotion, candidate-generator change, or operating application.

## Minimum descriptive cell

Same as Round 1: >=30 unique signal dates, >=60 calendar days between first/last signal, and >=2 half-year calendar blocks containing >=15 signal days each. Otherwise `DEFERRED_INSUFFICIENT_SAMPLE`.

## Outputs

- `2_Logs/strategy_validation_round2_train_regime_strategy_20260720.csv`
- `2_Logs/strategy_validation_round2_train_regime_strategy_20260720.json`
- `2_Logs/strategy_validation_round2_train_regime_strategy_20260720.md`
## Execution result — 2026-07-20

- Final run: `strategy_validation_round2_train_regime_strategy_20260720.run2.log`.
- Final artifact: all 15 registered combinations generated with JSON `status=OK`; eligible rows=415,001 and signal dates=987.
- Regime truncate-replay: PASS at all three registered cutoffs, mismatch=0.
- Result: no sufficient-coverage combination had positive cost-adjusted h5 absolute daily-basket return. STRESS × MR_BOLLINGER was +67.1883bp but deferred for 29 signal dates and zero qualifying half-year coverage blocks.
- No Validation/OOS selection, signal addition, candidate-generation change, or operating application occurred.
- Detailed result: `docs/03-operations/strategy_validation_round2_train_result_20260720.md`.