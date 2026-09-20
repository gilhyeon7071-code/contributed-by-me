# ExecPlan: preserve fill lineage in paper state reconcile

Date: 2026-04-29

## 1. Goal
- Fix the state reconcile path so `tools/reconcile_paper_state_from_fills.py` preserves entry lineage fields already present in BUY fill notes.
- Target symptom: `paper_state.json.open_positions` is rebuilt as `RECOVERED_FROM_FILLS` with empty `source_intent_id/source_trace_id`, even when the BUY fill note has `ENTRY_INTENT`, `entry_trace_id`, and `replay_chain_id`.

## 2. Non-goals
- No STOP_GAP policy value change.
- No stop loss threshold, sell ratio, Gate, LOCK, D rule, or order policy change.
- No historical PnL backfill.
- No manual edit of `paper/fills.csv`, `paper/trades.csv`, or ledgers.

## 3. Scope
- Modify only `E:\1_Data\tools\reconcile_paper_state_from_fills.py`.
- Update `E:\1_Data\.agent\PLANS.md` with this active plan reference.

## 4. Current Evidence
- `run_paper_daily.bat:451-452` executes `tools\reconcile_paper_state_from_fills.py`.
- `tools/reconcile_paper_state_from_fills.py` currently writes open positions with recovered lineage defaults.
- Current `paper_state.json.open_positions`: 32 rows, all `RECOVERED_FROM_FILLS`, all empty `source_intent_id/source_trace_id`.
- BUY fill notes contain original fields such as `entry_intent_id=ENTRY_INTENT_*`, `entry_trace_id=*`, and `replay_chain_id=*`.

## 5. Risks
- State rebuild could alter lineage fields used by sell notes.
- If parsing note fields is wrong, state lineage may be incomplete.
- SSOT chain must stay `orders(D) -> fills(D) -> ledger -> stats`.

## 6. Design
- Parse semicolon-delimited BUY fill `note` fields.
- When rebuilding open positions:
  - preserve `entry_intent_id`, `entry_trace_id`, `replay_chain_id` from the BUY fill note when present;
  - set `lineage_origin=FRESH_SIGNAL` for normal BUY fills unless source/replay fields indicate replay;
  - preserve explicit source fields from the note if present;
  - fallback to existing `RECOVERED_*` values only when note fields are absent.
- Do not change quantities, prices, dates, processed signals, or stop policy values.

## 7. Implementation Steps
1. Add a small note parser helper if not already present.
2. Capture lineage fields while scanning BUY fills.
3. Use captured fields in the rebuilt `open_positions`.
4. Keep fallback behavior for old fills without lineage fields.

## 8. Verification Plan
1. Syntax verification: `py_compile tools/reconcile_paper_state_from_fills.py`.
2. Execution verification: run the reconcile script through the same Python path.
3. Result artifact verification:
   - `paper_state.json.open_positions` no longer all forced to `RECOVERED_FROM_FILLS` when BUY note has entry lineage.
   - `source_intent_id/source_trace_id` are no longer empty where note fields exist.
   - `2_Logs/reconcile_paper_state_from_fills_latest.json` exists and reports success.
4. Policy verification:
   - Confirm no stop loss/sell ratio/Gate/LOCK config changed.
5. FAIL-CLOSED verification:
   - Confirm `2_Logs/recovery_ssot_chain_latest.json.status=PASS` remains available or rerun official path if needed.
6. Regression verification:
   - Confirm `paper/fills.csv` and `paper/trades.csv` row counts are not directly changed by this code edit.

## 9. Rollback Plan
- Restore:
  - `E:\1_Data\backup\20260429_reconcile_lineage_preserve\20260429_1300\reconcile_paper_state_from_fills.py.bak`
  - `E:\1_Data\backup\20260429_reconcile_lineage_preserve\20260429_1300\PLANS.md.bak`
- Re-run syntax and reconcile checks after rollback.

## 10. Evidence To Collect
- Backup paths.
- Command exit codes.
- Reconcile status JSON path.
- Core counts before/after:
  - open position count
  - lineage_origin distribution
  - empty source_intent_id/source_trace_id count
  - fills/trades row counts
