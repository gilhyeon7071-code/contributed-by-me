# ExecPlan — Liquidity-Leader Continuation Exploration

## Fixed question

Can a single, independently computed liquidity-leader continuation rule produce a daily Top-10 candidate basket whose next-session net h1/h2/h5 path is better than the same-day eligible KOSPI/KOSDAQ common-stock universe?

## Scope

- Research-only exploratory calculation; no candidate-generator, Gate, LOCK, order, fill, ledger, broker, or paper-runtime change.
- Reuse only the existing integrity-filtered price loader, point-in-time KOSPI/KOSDAQ common-stock membership, actual global-session execution convention, and fixed costs.
- Do not use current RS, v_accel, final score, Gate, candidate files, or paper records.

## Pre-registered rule

- Base universe: KOSPI/KOSDAQ COMMON, close >= KRW 1,000, current daily value >= KRW 1bn, valid next-session execution.
- Structural liquidity: prior 20-session average daily value >= KRW 2bn.
- Leadership: independently computed 20-session close return, same-day cross-sectional rank >= 80th percentile.
- Liquidity expansion: current value / prior 20-session mean value, same-day cross-sectional rank >= 80th percentile.
- Trend continuation: close > 20-session moving average and MA20 > its value five sessions earlier.
- Selection: maximum ten eligible names per signal date, descending average of return-rank and value-acceleration-rank; code is the deterministic tie-breaker.
- Execution: signal-day close information only; next actual global-session open entry; h1/h2/h5 actual global-session close exits; fee 0.5% each side, slippage 0.1% each side, 0.2% sell tax.
- Primary metric: signal-date equal-weight Top-10 basket net h5 return and same-date eligible-universe excess. h1/h2 are diagnostics.
- Reporting partitions: Train 2020-01-02~2023-12-29, Validation 2024, Assessment 2025, Recent 2026-01-01~2026-06-30. These are reported without selecting or promoting a rule.

## Steps

1. Back up RootA PLANS. (completed)
2. Add an isolated research runner and execute it. (completed)
3. Verify syntax, output contracts, price-integrity metadata, partition coverage, and no operating-path edits. (completed)

## Acceptance evidence

- Candidate records contain only as-of signal inputs and actual-session h1/h2/h5 net returns.
- Summary contains candidate count, signal-date count, candidate net return, baseline net return, and excess by fixed partition.
- The result is explicitly exploratory and is never used to change operations.
