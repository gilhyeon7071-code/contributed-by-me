# 2026-04-30 Blocked Entry Leak Fix

## Scope
- RootA only.
- Fix blocked BUY leakage from `orders_exec` into ledger append path.
- No policy threshold, Gate meaning, LOCK meaning, D rule, broker mode, or score change.

## Root Cause
- `tools\ledger_append_from_orders_exec.py` parsed `entry_blocked`, but still included blocked non-stop BUY rows in `buys`.
- This allowed rows marked `CAP_SIGNALDATE_TOP3_BY_SCORE` / `DISCLOSURE_NEGATIVE` to be reflected in ledger append outputs.

## Change
- Exclude `side=BUY`, `is_stop=False`, `entry_blocked=True` rows before grouping BUY rows for ledger append.
- Add explicit metrics/report fields for excluded blocked BUY rows and notional.
- Keep SELL behavior out of scope.

## Validation
- Syntax validation:
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile tools\ledger_append_from_orders_exec.py`
  - result: PASS
- Historical execution validation:
  - `E:\1_Data\_runtime\python312-embed\python.exe tools\ledger_append_from_orders_exec.py 20260416`
  - report: `E:\1_Data\2_Logs\ledger_append_report_20260416.json`
  - result: PASS dry-run
  - `input_buy_rows_total=17`
  - `blocked_buy_rows=17`
  - `blocked_buy_notional=44738095.0`
  - `input_buy_rows=0`
  - `to_append_rows=0`
  - `blocked_buy_excluded=true`
- Current-D regression dry-run:
  - `E:\1_Data\_runtime\python312-embed\python.exe tools\ledger_append_from_orders_exec.py 20260430`
  - report: `E:\1_Data\2_Logs\ledger_append_report_20260430.json`
  - result: PASS dry-run
  - `input_buy_rows_total=2`
  - `blocked_buy_rows=2`
  - `blocked_buy_notional=106910.0`
  - `input_buy_rows=0`
  - `to_append_rows=0`
  - `blocked_buy_excluded=true`

## Safety
- No policy threshold, Gate, LOCK, D rule, broker mode, or score changed.
- No ledger/fills data was modified.
- Dry-run reports were regenerated after backing up prior reports.

## Remaining
- This fixes the ledger append path only.
- It does not repair historical `fills.csv` or RootB ledger rows.
- Follow-up trace found no direct `paper_engine.py` blocked BUY materialization path:
  - evidence: `E:\1_Data\2_Logs\paper_engine_blocked_fill_path_analysis_20260430_1342.json`
  - `paper_engine.py` records `CAP_SIGNALDATE_TOP*` as `HOLD` and continues before `fills_new.append`.
  - `entry_blocked` is created later by `tools\p0_onepass_from_fills.py` while rebuilding `orders_exec` from already materialized `fills.csv`.
  - Latest `entry_signal_snapshot_latest.csv` has one `CAP_SIGNALDATE_TOP3_BY_SCORE` row with `signal=HOLD`.
- Remaining data issue:
  - historical `20260416` fills/ledger rows are not repaired.

## Follow-up: Problem 1/2
- Historical repair decision:
  - evidence: `E:\1_Data\2_Logs\historical_20260416_repair_decision_20260430_1348.json`
  - decision: `NO_DATA_MUTATION`
  - reason: `20260416` fills/ledger rows are materialized historical facts, and the latest trace shows post-hoc policy marking rather than a proven `paper_engine.py` cap-continue failure.
- `tools\p0_onepass_from_fills.py` now separates:
  - `posthoc_policy_violation`
  - `posthoc_policy_reason`
  - `execution_blocked`
  - `execution_block_reason`
- Compatibility retained:
  - existing `entry_blocked` and `entry_block_reason` remain populated.
- Validation:
  - temp historical `20260416` p0 run wrote to `E:\1_Data\tmp\p0_posthoc_verify_20260416`
  - current `20260430` p0 run updated official `orders_20260430_exec.xlsx`
  - current `20260430` ledger append dry-run excluded blocked BUY and appended zero rows.
