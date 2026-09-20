# Existing Second-Chance Data Audit

## Scope
- Verify what existing RootA logs can and cannot prove about second-chance entry policy calibration.
- Keep this read-only for trading.
- Do not change Gate, LOCK, STOP, risk thresholds, candidates, orders, fills, ledger, or policy values.

## Backup
- `E:\1_Data\backup\20260521_existing_second_chance_data_audit\20260521_125824`

## Files
- Add: `E:\1_Data\tools\audit_existing_second_chance_data.py`
- Output JSON: `E:\1_Data\2_Logs\existing_second_chance_data_audit_latest.json`
- Output CSV: `E:\1_Data\2_Logs\existing_second_chance_data_audit_latest.csv`

## Validation Plan
1. Syntax validation with `python -m py_compile`.
2. Standalone execution validation for artifact generation.
3. Result validation by reading summary and capability rows.
4. Mojibake scan for the new tool, this ExecPlan, and PLANS entry.

## Policy Boundary
- `trading_effect=false`
- `policy_effect=false`
- `policy_change_applied=false`
- This artifact is only an evidence audit, not an entry approval path.

## Result
- Syntax validation: PASS.
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile E:\1_Data\tools\audit_existing_second_chance_data.py`
- Standalone execution validation: PASS.
  - `candidate_review_rows=83`
  - `candidate_review_policy_rows=11`
  - `shadow_history_rows=5`
  - `surge_snapshot_files=94`
  - `surge_snapshot_rows=4149`
  - `surge_unique_codes=143`
  - `target_codes_with_surge_series=5`
  - `intraday_price_history_files=14`
  - `p1_history_rows=3969`
  - `followthrough_realtime_rows=838`
  - `followthrough_mid_rows=7140`
- Artifact validation: PASS.
  - `capability_status_counts`: `PASS=3`, `PARTIAL=4`, `NA=1`
  - `existing_data_verdict=PARTIAL_VALIDATION_ONLY`
  - Existing data can validate missed-move observations, blocked reason frequency, and limited surge or overheat time-series coverage.
  - Existing data cannot validate final policy thresholds, exact hypothetical fill quality, or real order/fill/ledger E2E under a changed policy.
- Policy validation: PASS.
  - `trading_effect=false`
  - `policy_effect=false`
  - `policy_change_applied=false`
