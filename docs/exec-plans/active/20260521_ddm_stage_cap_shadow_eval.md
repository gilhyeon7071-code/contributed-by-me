# 2026-05-21 ddm stage cap shadow eval

## Scope
- RootA only.
- Add a read-only evaluator for `ddm_stage_cap` shadow blocks.
- No order, fill, broker, STOP, LOCK, DDM setting, risk threshold, hard Gate, candidate input, or score file is changed.

## Backup
- `E:\1_Data\backup\20260521_ddm_stage_cap_shadow_eval\20260521_124726`

## Reason
- Today shadow promotion rows were all blocked by `p1_gate_closed:ddm_stage_cap`.
- The question is whether DDM is excessively blocking sample collection, or whether it is aligned with concurrent surge/overheat risk.

## Planned Change
- Add `tools/evaluate_ddm_stage_cap_shadow.py`.
- Output:
  - `E:\1_Data\2_Logs\ddm_stage_cap_shadow_eval_latest.json`
  - `E:\1_Data\2_Logs\ddm_stage_cap_shadow_eval_latest.csv`
- Classify rows as:
  - `DDM_BLOCK_ALIGNED_WITH_RISK`
  - `DDM_BLOCK_PARTIAL_RISK`
  - `DDM_BLOCK_NEEDS_REVIEW`
  - `NOT_DDM_BLOCKED`

## Policy Boundary
- This is shadow evaluation only.
- `trading_effect=false`, `policy_effect=false`, and `policy_change_applied=false`.
- Any DDM relaxation remains a separate policy change.

## Validation Evidence
- Syntax:
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile E:\1_Data\tools\evaluate_ddm_stage_cap_shadow.py`
  - PASS
- Pre mojibake:
  - `E:\1_Data\2_Logs\mojibake_text_scan_ddm_stage_cap_shadow_eval_20260521_pre.json`
  - `files=2`, `issues=0`, `repairable=0`
- Standalone execution:
  - `E:\1_Data\_runtime\python312-embed\python.exe E:\1_Data\tools\evaluate_ddm_stage_cap_shadow.py`
  - `history_rows=5`
  - `ddm_blocked_rows=5`
  - `aligned_with_risk_rows=5`
  - `ddm_needs_review_rows=0`
  - `assessment_counts={"DDM_BLOCK_ALIGNED_WITH_RISK":5}`
- Output:
  - `E:\1_Data\2_Logs\ddm_stage_cap_shadow_eval_latest.json`
  - `trading_effect=false`
  - `policy_effect=false`
  - `policy_change_applied=false`
  - interpretation: `ddm_stage_cap aligned with concurrent surge/overheat risk`
  - sample warning: `single-day/small-sample`
- Final mojibake:
  - `E:\1_Data\2_Logs\mojibake_text_scan_ddm_stage_cap_shadow_eval_20260521_final.json`
  - `files=3`, `issues=0`, `repairable=0`

## 6 Validation Items
1. Functional validation: PASS
2. Consistency validation: PASS
3. Operational reflection validation: PASS, standalone artifact generation only
4. Policy validation: PASS
5. FAIL-CLOSED validation: PASS
6. Regression validation: PASS

## Remaining Boundary
- This does not prove DDM is always correctly calibrated.
- Current evidence is one day and five rows only.
- Any DDM relaxation remains a separate policy change.
