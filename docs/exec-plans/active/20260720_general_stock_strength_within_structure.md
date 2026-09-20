# ExecPlan — General Stock Price Strength within Structural Cohort

## Fixed question

Within the already observed `UPTREND × HIGH_LIQUID` general-stock cohort, does pre-signal 20-session price strength identify a higher share of future leaders?

## Round and scope

- Exploratory research only. This is the third-axis signal map after the two-axis structural result; it is not confirmation or operational selection.
- No candidate-generator, current score, Gate, LOCK, order, fill, ledger, broker, paper runtime, entry, exit, cost, or position-sizing change.

## Frozen data and definitions

- `PRICE_HISTORY_INTEGRITY_V1`; point-in-time KOSPI/KOSDAQ common-stock membership; existing fixed periods: 2020-21, 2022-23, 2024, 2025.
- Cohort: signal-date `UPTREND` and `HIGH_LIQUID` from the prior outcome-map contract.
- Signal: each stock's close-to-close return over the prior 20 valid sessions. It is ranked only among the cohort in the same signal date and market, then divided into ten fixed equal-count deciles (`S1` weakest through `S10` strongest).
- Outcome: next actual global-session open to h20/h60/h120 close gross return; future leader means top decile within all eligible stocks on the same date and market.
- Primary: h60 future-leader rate for every fixed strength decile, compared with the cohort baseline on the same signal dates. h20/h120 are diagnostics.
- Minimum: fewer than 30 unique signal dates is `DEFERRED_INSUFFICIENT_SAMPLE`. No decile, threshold, or signal is promoted from this run.

## Acceptance checks

1. Strength uses only current and earlier prices and is absent where fewer than 20 prior valid sessions exist.
2. Future leader labels use the full same-date/same-market eligible universe, not the selected cohort.
3. Each decile comparison uses its own dates' cohort baseline.
4. Outputs explicitly prohibit operational use.

## Steps

1. Back up RootA PLANS. (completed)
2. Add isolated research runner and fixed plan. (completed)
3. Execute, independently recalculate headline ratios, and record result. (completed)

## Result

- Execution exited 0 and generated the fixed 165-cell price-strength map.
- S10 was above the structural cohort baseline in 4/4 periods at h20 and h60, and 3/4 at h120. Overall h60 S10 leader rate was 15.5683%; the full-cohort rate across all dates was 12.5703%, while the same-S10-date cohort comparison produced the 1.2290 lift.
- All 165 stored period × horizon × decile cells were independently reconstructed from raw inputs and matched exactly.
- This closes the exploratory signal-map question only. No threshold selection, trade rule, or operating application follows from this plan.