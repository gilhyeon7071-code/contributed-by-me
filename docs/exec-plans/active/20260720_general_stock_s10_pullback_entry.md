# ExecPlan — S10 Signal-Close Pullback Entry Diagnostic

## Fixed question

Can an S10 structural candidate improve its cost-adjusted h60 daily-basket result by entering only on the next session's pullback to the signal-day close?

## Frozen contract

- Exploratory execution diagnostic only; no OOS claim or operating change.
- Same point-in-time KOSPI/KOSDAQ common-stock, price-integrity, structural cohort, S10, periods, costs, and h60 primary measure as the prior S10 execution diagnostic.
- On the next actual session, a buy limit equals the signal-day close. Fill at next open when `open <= limit`; otherwise fill at limit only when `low <= limit`; otherwise no fill.
- Exit: h20/h60/h120 actual-session close after signal date. Costs: buy 0.6%, sell 0.8%.
- Primary: h60 mean daily equal-weight net-return basket of filled S10 entries. h20/h120 and fill rate are diagnostics. Fewer than 30 signal days is deferred.

## Steps

1. Back up RootA PLANS. (completed)
2. Add isolated pullback-entry runner and fixed plan. (completed)
3. Execute, recalculate headline cells, and record result. (completed)

## Result

- Execution exited 0. The h60 filled S10 basket was negative in all fixed periods; all-period result -318.0298bp.
- This rejects the single signal-close pullback-entry hypothesis. Independent daily-basket math audit remains pending.

- Independent reconstruction of all 15 stored cells matched exactly.
