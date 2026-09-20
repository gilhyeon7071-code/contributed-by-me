# 2026-07-20 Strategy Validation Round 1 — Exploratory Independent-Axis Test

## Fixed objective

Validate independent effects of the already-defined situation, strategy-grammar, and atomic-signal axes without using the existing candidate generator, final score, Gate, orders, fills, or paper trade record as an input or benchmark.

## Scope

- Round: exploratory only; no promotion, policy change, or automatic hypothesis selection.
- Universe: KOSPI and KOSDAQ common-stock memberships from `krx_population_static_membership_latest.parquet`, joined by (as_of_date, code) to rows passing `PRICE_HISTORY_INTEGRITY_V1`; signal-day close/open > 0, close >= KRW 1,000, and daily traded value >= KRW 1,000,000,000.
- Signal knowledge: values observed on signal-day close only.
- Execution: next actual global trading-session open entry; h5 actual global trading-session close exit.
- Costs: buy fee 0.5%, sell fee 0.5%, buy slippage 0.1%, sell slippage 0.1%, sell tax 0.2%.
- Primary metric: each signal-date equal-weight candidate basket net h5 return. Same-date full-universe basket net h5 excess is recorded as a comparison, not a promotion metric.
- Time partitions: Train = 2020-01-02 to 2023-12-29; Validation = 2024-01-01 to 2024-12-31; OOS = 2025-01-01 to 2026-06-30. Round 1 produces Train only; Validation/OOS are not read for selection in this round.
- Minimum descriptive cell: >=30 unique signal dates, >=60 calendar days between first/last signal, and >=2 half-year calendar blocks containing >=15 signal days each. This measures distinct calendar coverage; gaps between consecutive signal dates are not used because a daily signal is not a one-episode sample. Any smaller cell is `DEFERRED_INSUFFICIENT_SAMPLE`.

## Pre-registered axes for Round 1

1. Situation: BULL, BEAR, SIDEWAYS, STRESS, TRANSITION using the existing ex-ante rolling regime function; truncate-replay check is mandatory.
2. Strategy grammar: BOLLINGER mean reversion, MA20/MA60 cross-up, prior-252-session breakout.
3. Atomic signal: stretch lowest quintile, relative strength highest quintile, relative-strength slope highest quintile, value acceleration highest quintile.
4. Candidate-pool axis is fixed to the universe above for this round. No two-axis or three-axis conjunction is tested in Round 1.

## Outputs

- `2_Logs/strategy_validation_round1_train_axis_summary_20260720.csv`
- `2_Logs/strategy_validation_round1_train_axis_summary_20260720.json`
- `2_Logs/strategy_validation_round1_train_axis_summary_20260720.md`

## Non-goals

- No use of current candidate output, preserved candidates, final score, Gate, paper orders, paper trades, or broker data.
- No Top-N addition, parameter search, threshold relaxation, operating code change, or promotion.
- No Validation/OOS decision in this exploratory round.
## Execution result — 2026-07-20

- Final run: `strategy_validation_round1_train_axis_summary_20260720.run4.log`.
- Final artifact: CSV, JSON, Markdown generated with `status=OK`; eligible rows=415,001 and signal dates=987.
- Regime truncate-replay: PASS at all three registered cutoffs, mismatch=0.
- Result: no registered single axis had positive cost-adjusted h5 absolute daily basket return with sufficient coverage. STRESS had +34.3084bp but only one qualifying half-year block and remains `DEFERRED_INSUFFICIENT_SAMPLE`.
- No Validation/OOS selection, axis conjunction, candidate-generation change, or operating application occurred.
- Detailed result: `docs/03-operations/strategy_validation_round1_train_result_20260720.md`.