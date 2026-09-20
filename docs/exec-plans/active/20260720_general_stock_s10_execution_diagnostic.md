# ExecPlan — S10 Structural Candidate Execution Diagnostic

## Fixed question

Does the exploratory S10 price-strength segment retain positive cost-adjusted daily-basket returns when entered at the next actual global-session open and held to h60?

## Round and boundary

- Exploratory execution diagnostic, not a Validation/OOS confirmation: S10 was selected in the preceding exploratory map using these historical periods.
- No current candidate-generator, score, Gate, LOCK, order, fill, ledger, broker, paper runtime, entry policy, exit policy, or sizing change.

## Frozen contract

- Population and states: `PRICE_HISTORY_INTEGRITY_V1`, point-in-time KOSPI/KOSDAQ common stocks, `UPTREND × HIGH_LIQUID`, and S10 as already computed by the prior research runner.
- Comparison: S10 daily equal-weight basket versus the full structural cohort daily equal-weight basket on the same S10 signal dates.
- Entry: next actual global-session open after the signal date.
- Exit: actual-session close at h20/h60/h120. Primary h60; h20/h120 diagnostics only.
- Costs: buy fee 0.5% + buy slippage 0.1%; sell fee 0.5% + sell slippage 0.1% + sell tax 0.2%. Net return = `(exit_close × 0.992) / (entry_open × 1.006) - 1`.
- Fixed partitions: 2020-21, 2022-23, 2024, 2025. Fewer than 30 unique signal dates is deferred.
- Primary measure: mean of the daily equal-weight S10 net-return baskets. Positive-day rate and S10 minus same-date cohort net return are diagnostics.

## Acceptance checks

1. Entry and exit use actual global-session indexes and the price-integrity entities.
2. Costs are applied to each constituent before daily equal-weight aggregation.
3. S10 and cohort comparison joins only on S10 signal dates.
4. Output states explicitly that it cannot promote or operate a rule.

## Steps

1. Back up RootA PLANS. (completed)
2. Add isolated execution-diagnostic runner and fixed plan. (completed)
3. Execute, independently recompute daily-basket headline values, and record result. (completed)

## Result

- Execution exited 0 and produced all 15 fixed period × horizon daily-basket cells.
- Primary h60 cost-adjusted S10 daily-basket return was negative in all fixed periods: -19.7287bp, -602.3140bp, -471.4715bp, and -165.0430bp. The all-period result was -329.1263bp.
- S10 was also below the same-S10-date structural cohort in every h60 period.
- The output is an exploratory execution diagnostic only. It rejects this fixed next-open-to-h60 execution rule, not the broader structural candidate-quality observation.