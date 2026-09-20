# 2026-05-21 second chance shadow eval

## Scope
- RootA only.
- Add a read-only evaluator for second-chance entry readiness after overheat/DDM blocks.
- No order, fill, broker, STOP, LOCK, DDM setting, risk threshold, hard Gate, candidate input, or score file is changed.

## Backup
- `E:\1_Data\backup\20260521_second_chance_shadow_eval\20260521_125229`

## Reason
- Shadow candidates can move strongly after being blocked, but today's blocked rows also carried overheat and LOB risk.
- The next safe step is to evaluate whether later conditions cooled down enough for review, not to relax the gates.

## Planned Change
- Add `tools/evaluate_second_chance_shadow_entry.py`.
- Output:
  - `E:\1_Data\2_Logs\second_chance_shadow_entry_latest.json`
  - `E:\1_Data\2_Logs\second_chance_shadow_entry_latest.csv`
- Evaluate:
  - entry change cooldown
  - ATR cooldown
  - pullback from intraday high
  - RVOL overheat
  - LOB availability

## Policy Boundary
- This is shadow evaluation only.
- `second_chance_trading_allowed=false`, `trading_effect=false`, and `policy_effect=false`.
- Actual second-chance entry remains a separate policy change.

## Validation Evidence
- Syntax:
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile E:\1_Data\tools\evaluate_second_chance_shadow_entry.py`
  - PASS
- Pre mojibake:
  - `E:\1_Data\2_Logs\mojibake_text_scan_second_chance_shadow_eval_20260521_pre.json`
  - `files=2`, `issues=0`, `repairable=0`
- Standalone execution:
  - `E:\1_Data\_runtime\python312-embed\python.exe E:\1_Data\tools\evaluate_second_chance_shadow_entry.py`
  - `source_rows=5`
  - `eval_rows=5`
  - `verdict_counts={"SECOND_CHANCE_BLOCKED_OVERHEAT":5}`
  - `review_ready_rows=0`
- Output:
  - `E:\1_Data\2_Logs\second_chance_shadow_entry_latest.json`
  - `trading_effect=false`
  - `policy_effect=false`
  - `policy_change_applied=false`
  - all rows keep `second_chance_trading_allowed=false`
  - common failed checks: `entry_change_cooled`, `pullback_observed`, `lob_available`
- Final mojibake:
  - `E:\1_Data\2_Logs\mojibake_text_scan_second_chance_shadow_eval_20260521_final.json`
  - `files=3`, `issues=0`, `repairable=0`

## 6 Validation Items
1. Functional validation: PASS
2. Consistency validation: PASS
3. Operational reflection validation: PASS, standalone artifact generation only
4. Policy validation: PASS
5. FAIL-CLOSED validation: PASS
6. Regression validation: PASS

## Remaining Boundary
- The evaluator is not wired into the live batch chain.
- No second-chance entry policy is approved.
- Actual second-chance entry remains a separate policy change.
