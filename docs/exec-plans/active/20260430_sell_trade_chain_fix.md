# 2026-04-30 Sell Trade Chain Fix

## Scope
- Resolve latest main logic validation failures:
  - `sell_fill_trade_mismatch:3!=2`
  - `unreconciled_sell_rows:1`
  - `lifecycle:sell_trade_chain_unmatched_rows:1`

## Root Cause
- `paper_engine.py` registered `source_order_id` from partial-exit trades into `committed_source_order_ids`.
- Later full-exit trades from the same original BUY order were blocked by `_src_oid_blocked`.
- This left a SELL fill without a corresponding closed trade in `paper\trades.csv`.

## Changes
- `paper_engine.py`
  - Only full exits reserve `source_order_id` / `entry_order_id` in `committed_source_order_ids`.
  - Partial exits no longer block later full-exit trade generation.
- `paper\trades.csv`
  - Backfilled the missing `095910` `20260430` `FUNDAMENTAL_CRITICAL` closed trade from existing `fills.csv`.

## Validation
- Syntax check:
  - `python -m py_compile paper_engine.py`
- Data check:
  - `095910` has one SELL fill on `20260430` and one matching closed trade on `20260430`.
- Runtime check:
  - `paper_engine.py` rerun regenerated validation reports.
  - Second engine run read latest reports and printed:
    - `EXECUTION_GUARD decision=ALLOW reason=ok lifecycle=PASS state_machine=PASS`
- Recovery checks:
  - `tools\build_recovery_ssot_chain_report.py`
  - `tools\nightly_data_integrity_check.py --warn-only`
- Official full batch:
  - `run_paper_daily.bat`
  - wrapper `rc=0`
  - `paper_order_validation_report_latest.json status=PASS issues=[]`
  - `sell_validation_report_latest.json status=PASS issues=[]`
  - `sell_fill_rows=3`, `sell_trade_rows=3`
  - `execution_safety_contract_latest.json status=PASS D=20260429`
  - `recovery_ssot_chain_latest.json status=PASS D=20260429`
  - `nightly_data_integrity_latest.json ok=True`

## Safety
- No Gate, LOCK, broker mode, or D rule meaning changed.
- The fix prevents future full-exit trade suppression while keeping full-exit duplicate protection.

## Remaining
- `run_backtest_validation_real.bat` still returns `rc=2` and is treated by the daily batch as WARN/NO_GO.
- DART refresh still warns due `ModuleNotFoundError: build_disclosure_risk_log`; candidate generation completed and the full batch returned `rc=0`.
