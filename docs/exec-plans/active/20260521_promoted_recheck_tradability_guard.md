# 2026-05-21 promoted recheck tradability guard

## Scope
- RootA only.
- Fix `tools/build_promoted_recheck_candidates.py` so observation/recheck rows are not promoted into paper-engine candidate input unless the source row is already tradable.
- No order, fill, broker, STOP, LOCK, risk threshold, or hard Gate meaning change.

## Backup
- `E:\1_Data\backup\20260521_promoted_recheck_tradability_guard\20260521_113900`

## Root Cause
- `candidate_action_recheck_queue_latest.json` can contain observation rows with:
  - `trading_allowed=false`
  - `execution_pool=false`
  - `sector_entry_allowed=false`
- The promotion bridge previously skipped hard blocked rows, but did not require those source tradability flags.
- `_candidate_row()` then emitted promoted rows with `execution_pool=true` and `sector_entry_allowed=true`, which could turn observation rows into executable candidate input.

## Change
- Add `_truthy()`.
- In `main()`, skip due `PROMOTE_TO_RECHECK` rows unless all are true:
  - `trading_allowed`
  - `execution_pool`
  - `sector_entry_allowed`
- Add `skipped_not_tradable` to JSON summary and console output.

## Validation Evidence
- Syntax:
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile E:\1_Data\tools\build_promoted_recheck_candidates.py`
  - PASS
- Execution:
  - `E:\1_Data\_runtime\python312-embed\python.exe E:\1_Data\tools\build_promoted_recheck_candidates.py`
  - output: `rows=0`, `skipped_blocked=15`, `skipped_not_tradable=3`
- Artifact:
  - `E:\1_Data\2_Logs\promoted_recheck_candidates_latest.json`
  - `summary.rows=0`
  - `summary.skipped_not_tradable=3`
  - `rows=[]`
- Mojibake:
  - `E:\1_Data\2_Logs\mojibake_text_scan_promoted_recheck_tradability_guard_20260521.json`
  - `issue_count=0`
- Live loop reflection:
  - `E:\1_Data\2_Logs\promoted_recheck_candidates_latest.json`
  - latest artifact refreshed after the change; `summary.rows=0`, `summary.skipped_blocked=18`, `summary.skipped_not_tradable=4`, `rows=[]`
  - `E:\1_Data\2_Logs\entry_decision_layers_runtime_latest.json`
  - `status=PASS`, `d_ref=20260521`, `candidate_rows=1`, `decision_rows=1`, `alpha_layer_counts={"ALPHA_ELIGIBLE":1}`, `risk_layer_counts={"REDUCED":1}`, `execution_layer_counts={"BLOCKED_OR_HELD":1}`, `policy_change=false`
  - current decision row is normal candidate `049960`; no `ACTIVE_RECHECK_PROMOTION` row entered the latest decision layer.
  - `E:\1_Data\2_Logs\candidates_latest_data.with_final_score.csv`
  - `rows=2`, `candidate_origin` counts: `NATURAL_PASS=1`, `BALANCED_RESCUE=1`, `ACTIVE_RECHECK_PROMOTION=0`
  - `E:\1_Data\2_Logs\paper_order_validation_report_latest.json`
  - `status=PASS`, `candidates_after_caps=1`, `entry_ready=0`, `effective_entry_ready=3`, `buy_fill_rows=3`, `entry_decision_reason_counts={"SAME_CODE_DAY_ALREADY_BUY":1}`
- Final mojibake:
  - `E:\1_Data\2_Logs\mojibake_text_scan_promoted_recheck_tradability_guard_final4_20260521.json`
  - `files=3`, `issue_files=0`, `issues=0`, `repairable=0`

## 6 Validation Items
1. Functional validation: PASS
2. Consistency validation: PASS
3. Operational reflection validation: PASS
4. Policy validation: PASS
5. FAIL-CLOSED validation: PASS
6. Regression validation: PASS

## Remaining Boundary
- `policy_effect=true` remains correct for the promoted candidate artifact because the bridge can affect candidate input.
- `trading_effect=false` remains correct because this tool does not send orders.
- Manual full paper-engine rerun was not started in this step.
- Live latest runtime artifacts refreshed after the change and show the promoted bridge no longer injecting non-tradable recheck rows into the latest decision layer.
- Current entry block remains `SAME_CODE_DAY_ALREADY_BUY`; that is separate from the promoted recheck tradability guard.
