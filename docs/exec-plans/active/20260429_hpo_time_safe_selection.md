# 20260429 HPO Time-Safe Selection

## Goal
- Add time-aware candidate selection metrics to `E:\1_Data\optimize_params_v41_1.py`.
- Keep the existing optimizer entrypoint and search loop.

## Non-Goals
- No Ray, BOHB, dependency, batch, order, fill, ledger, stats, Gate, LOCK, or live trading change.
- No output JSON/history backfill.

## Scope
- Modify only:
  - `E:\1_Data\optimize_params_v41_1.py`
  - `E:\1_Data\tests\test_optimize_hypertime_selection.py`
  - `E:\1_Data\.agent\PLANS.md`
- Add this plan file.

## Current Behavior
- The optimizer records yearly windows, but candidate selection score does not separately report `avg`, `worst_fold`, and `recent_weighted`.
- A candidate can rank well by aggregate score even when one time fold is weak.

## Risk
- Parameter selection can become stricter because weak worst folds are blocked.
- Existing output schema gains additive fields only.

## Design
- Compute `avg`, `worst_fold`, and `recent_weighted` from chronological window PF values.
- Apply fail-closed candidate score when fewer than 3 folds are available.
- Apply fail-closed candidate score when `worst_fold < 0.75`.
- Keep final scalar `hypertime_score` as the single optimizer score.

## Verification Plan
- Syntax validation: `python -m py_compile optimize_params_v41_1.py tests\test_optimize_hypertime_selection.py`
- Runtime validation: `python -m unittest tests.test_optimize_hypertime_selection`
- Result validation: inspect test output and changed code paths.
- Policy validation: confirm no batch/Gate/LOCK/order/fill/ledger/stats change.
- FAIL-CLOSED validation: test insufficient folds and weak worst fold.
- Regression validation: run focused existing optimizer-related tests if available.

## Rollback
- Restore from:
  - `E:\1_Data\backup\20260429_hpo_time_safe_selection\20260429_1515\optimize_params_v41_1.py.before_hpo_time_safe_selection`
  - `E:\1_Data\backup\20260429_hpo_time_safe_selection\20260429_1515\PLANS.md.before_hpo_time_safe_selection`
