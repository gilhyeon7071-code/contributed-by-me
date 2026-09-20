# 20260504 Safe Exploration Advisory

## Scope
- Add a review-only Safe Exploration evidence path for threshold candidates.
- Add an evidence-only apply gate that blocks unless review evidence is `SAFE_PASS` and an explicit approval flag is present.
- No real apply, no order, no fill, no ledger, no stats, no Gate/STOP/LOCK meaning change.

## Files
- `E:\1_Data\tools\safe_exploration_review.py`
- `E:\1_Data\tools\safe_exploration_apply_gate.py`
- `E:\1_Data\run_safe_exploration_review.bat`
- `E:\1_Data\run_safe_exploration_apply_gate.bat`
- `E:\1_Data\tests\test_safe_exploration_review.py`
- `E:\1_Data\tests\test_safe_exploration_apply_gate.py`
- `E:\1_Data\docs\exec-plans\active\20260504_safe_exploration_advisory.md`
- `E:\1_Data\.agent\PLANS.md`

## Design
- Read latest auto signal tune candidate and existing calibration/ARL/pending evidence.
- Emit review-only fields: `safe_bo_lower`, `bandit_dual`, `K`, `live_pct`, `beta`, `gamma`, `eta_t`, `max_safe_pct`.
- Force `apply_status=WAITING_APPROVAL` and `live_pct=0.0`.
- Fail closed when K is below minimum, current gate is blocked, calibration/ARL fails, or advisory lower bound/dual gate fails.
- Apply gate writes JSON/TXT evidence only.
- Apply gate returns block when `--approve` is missing, review is not `SAFE_PASS`, review is not `WAITING_APPROVAL`, or `live_pct` is not `0.0`.

## Verification Plan
- Syntax validation: py_compile the new tool and test.
- Runtime validation: run focused unittest.
- Result validation: run `run_safe_exploration_review.bat` and inspect latest JSON/TXT.
- Apply-gate validation: run `run_safe_exploration_apply_gate.bat --approve` against current `SAFE_FAIL` and confirm it blocks.
- Policy validation: confirm no operational state flags were modified.
- FAIL-CLOSED validation: unit tests for low K and current gate blocked.
- Regression validation: no existing apply path or config lock command is invoked.

## Backup
- `E:\1_Data\backup\20260504_safe_exploration_advisory\20260504_090016\.agent_PLANS.before_safe_exploration_advisory.md`
- `E:\1_Data\backup\20260504_safe_exploration_apply_gate\20260504_090610`
