# Runtime E2E Audit

Use this lane for operational, date, SSOT, batch, or readiness claims.

## Canonical contract

The RootA chain is:

```text
orders(D) -> fills(D) -> ledger -> stats
```

Derive `D` from the latest `BUY ymd` in `E:\1_Data\paper\fills.csv`; if no BUY exists, use the latest first eight digits of `datetime` as specified by `AGENTS.md`.

Immediately report STOP when any of these holds:

- `orders_exec` missing
- `exec_date != D`
- `as_of` or `run_id` mismatch
- paper and broker dates mixed

Do not let another health card, dashboard PASS, or zero-row success override a STOP condition.

## Evidence sequence

1. Resolve the exact active batch and process command line.
2. Identify the canonical writers for orders, fills, ledger, and stats.
3. Compare row counts only after aligning market, date window, strategy/path tag, and aggregation grain.
4. Trace stable identifiers across the chain: order id, fill id, code, side, quantity, price, timestamp, `as_of`, `run_id`, and strategy/source tag where available.
5. Verify duplicates, omissions, partial fills, cancellations, and restart behavior.
6. Verify that regenerated stats use the same D and authoritative ledger.
7. Separate premarket, intraday-only, and after-hours checks. Do not mark an intraday item PASS outside its observable window.

Relevant existing tools may include:

- `tools\verify_orders_exec.py`
- `tools\verify_fills_ledger.py`
- `tools\build_order_fill_e2e_chain_report.py`
- `tools\build_order_fill_required_checklist_report.py`
- `tools\build_ssot_health_card.py`
- `tools\build_trading_stage_validation_report.py`
- `tools\reconcile_paper_state_from_fills.py`

Inspect each tool's current arguments and meaning before execution. Prefer official read-only or dry-run paths. If a batch can overwrite live artifacts, state the risk and test order before running it.

## Six required verdicts

Report evidence-backed `PASS / FAIL / NA` for:

1. functional
2. consistency
3. operational reflection
4. policy
5. FAIL-CLOSED
6. regression

No runtime evidence means operational reflection is not PASS. No full chain evidence means E2E is not PASS.
