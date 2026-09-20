# ExecPlan — General Stock Trend × Liquidity Incremental Test

## Fixed question

Does structural liquidity add candidate-quality information after a stock is already in the pre-registered UPTREND state?

## Scope

- Research-only candidate-quality interaction test; no entry, exit, cost, or operating-policy evaluation.
- Reuse the already-validated general-stock outcome-map loader, point-in-time market join, observable states, h20/h60/h120 gross leader label, and fixed chronological partitions.
- No market-breadth conjunction, threshold search, HPO, current logic input, or operating application.

## Pre-registered comparison

- Reference: `UPTREND` alone.
- Interactions: `UPTREND × HIGH_LIQUID`, `UPTREND × MID_LIQUID`, `UPTREND × LOW_LIQUID`.
- Primary: h60 same-date/same-market top-decile leader rate, compared on the same signal dates to the UPTREND reference.
- Diagnostics: h20/h120, full baseline lift, row/date counts, and the four fixed chronological periods.
- A condition is descriptive only when it has at least 30 signal dates. No condition is selected or promoted from this run.

## Steps

1. Back up RootA PLANS. (completed)
2. Add and run isolated interaction runner. (completed)
3. Verify same-date reference alignment, output counts, and scope. (completed)

## Acceptance evidence

- Interaction states use only pre-signal UPTREND and liquidity labels.
- Incremental lift is compared with UPTREND on exactly the dates where the interaction exists.
- The result differentiates an additive quality filter from a redundant one without creating a trading rule.

## Result

- Execution completed with exit code 0; JSON status is `OK` and stderr is empty.
- At h60, `UPTREND × HIGH_LIQUID` has a 12.5703% leader rate versus 11.7877% for UPTREND alone, an incremental lift of 1.0664.
- HIGH_LIQUID was above the UPTREND reference in 4/4 fixed periods at h20, 3/4 at h60, and 2/4 at h120. MID_LIQUID and LOW_LIQUID were below the UPTREND reference in the all-period h60 result.
- This closes the descriptive interaction question only. No selection, promotion, entry/exit rule, or operating application follows from this plan.
