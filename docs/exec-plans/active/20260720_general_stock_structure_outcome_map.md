# ExecPlan — General Stock Structural Outcome Map

## Fixed question

Before choosing an entry rule, which broad observable states in ordinary KOSPI/KOSDAQ common stocks are associated with becoming a same-date top-decile performer over the following 20, 60, or 120 actual trading sessions?

## Scope

- Candidate-quality mapping only. It is not an execution backtest and does not charge trading costs.
- Use integrity-filtered OHLCV, point-in-time KOSPI/KOSDAQ common-stock membership, and signal-date observable inputs only.
- Exclude current candidate generator, RS/v_accel/final score/Gate, orders, fills, ledger, broker, and paper records.

## Pre-registered broad states

- Base universe: KOSPI/KOSDAQ common stocks, close >= KRW 1,000, daily value >= KRW 50m, valid forward outcome.
- Market breadth (within each market): share of base stocks above MA60: `BROAD_UP` >=60%, `MIXED` 40~60%, `BROAD_WEAK` <40%.
- Stock trend: `UPTREND` = close>MA60 and MA60 above its ten-session lag; `WEAK_TREND` = close<MA60 and MA60 not above its ten-session lag; otherwise `TRANSITION`.
- Structural liquidity: prior 20-session mean daily value: `HIGH_LIQUID` >=KRW 2bn; `MID_LIQUID` KRW 300m~2bn; `LOW_LIQUID` KRW 50m~300m.
- Outcomes: next actual session open to h20/h60/h120 actual-session close gross return. A leader is same-date, same-market top-decile gross return.
- Report each broad axis alone against its 10% same-date baseline, overall and in fixed chronological partitions. No conjunction, threshold search, strategy selection, or operating promotion.

## Steps

1. Back up RootA PLANS. (completed)
2. Add and execute an isolated structural outcome-map runner. (completed)
3. Verify outcome ranks, row/date counts, summary recomputation, and scope. (completed)

## Acceptance evidence

- The output distinguishes broad candidate quality from entry/exit profitability.
- Every leader label uses only future outcome; every state uses only signal-date or prior data.
- Results show leader rate, lift versus baseline, and raw forward return by broad state without choosing a strategy.
