# Second-Chance Sample Collection Design

## Scope
- Convert the existing data audit and second-chance shadow evaluation into a sample collection readiness judgment.
- Keep this read-only for trading and policy.
- Do not change Gate, LOCK, STOP, risk thresholds, candidates, orders, fills, ledger, or policy values.

## Backup
- `E:\1_Data\backup\20260521_second_chance_sample_design\20260521_130240`

## Files
- Add: `E:\1_Data\tools\design_second_chance_sample_collection.py`
- Output JSON: `E:\1_Data\2_Logs\second_chance_sample_collection_design_latest.json`
- Output CSV: `E:\1_Data\2_Logs\second_chance_sample_collection_design_latest.csv`

## Validation Plan
1. Syntax validation with `python -m py_compile`.
2. Standalone execution validation for artifact generation.
3. Result validation by reading recommendation, readiness, and options.
4. Mojibake scan for the new tool, this ExecPlan, and PLANS entry.

## Policy Boundary
- `trading_effect=false`
- `policy_effect=false`
- `policy_change_applied=false`
- This artifact is a design/readiness judgment only.

## Result
- Syntax validation: PASS.
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile E:\1_Data\tools\design_second_chance_sample_collection.py`
- Standalone execution validation: PASS.
  - `recommendation=KEEP_SHADOW_ONLY`
  - `readiness_status=NOT_READY_FOR_POLICY_CHANGE`
  - `failed_checks=shadow_history_baseline, second_chance_ready_rows`
- Artifact validation: PASS.
  - `shadow_history_baseline`: `actual=5`, `required=40`, `status=FAIL`
  - `second_chance_ready_rows`: `actual=0`, `required=10`, `status=FAIL`
  - `target_surge_series_coverage`: `actual=5`, `required=5`, `status=PASS`
  - `surge_snapshot_depth`: `actual=4149`, `required=300`, `status=PASS`
  - `real_order_fill_baseline`: `status=NA`
- Policy validation: PASS.
  - `trading_effect=false`
  - `policy_effect=false`
  - `policy_change_applied=false`
- Interpretation:
  - Current state is not ready for minimum-entry probe or gradual reduction.
  - Current structure should remain shadow-only sample collection until baseline and review-ready rows exist.
