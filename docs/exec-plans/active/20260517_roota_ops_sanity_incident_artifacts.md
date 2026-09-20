# 20260517_roota_ops_sanity_incident_artifacts

## Scope
- Create RootA-only quick read-only sanity artifact.
- Create RootA incident report template.
- Do not change trading policy, gate meaning, risk lock, score, threshold, order, fill, ledger, or stats logic.

## Files
- New: `E:\1_Data\run_ops_sanity_quick.bat`
- New: `E:\1_Data\docs\references\ROOTA_INCIDENT_REPORT_TEMPLATE.md`
- Record: `E:\1_Data\.agent\PLANS.md`

## Procedure
1. Use existing read-only RootA checks where available.
2. Exclude `/admin/pause` or any state-changing endpoint from the quick sanity path.
3. Preserve non-zero exit propagation for hard failures.
4. Write evidence under `E:\1_Data\2_Logs`.
5. Verify syntax, execution, and output artifacts.

## Verification Plan
- Syntax: batch parse/smoke help by direct execution path, markdown file existence.
- Execution: run `E:\1_Data\run_ops_sanity_quick.bat`.
- Output: inspect `E:\1_Data\2_Logs\ops_sanity_quick_latest.json` and related evidence paths.
- Policy: confirm no state-changing endpoint or gate/score hardcoding is present.
- FAIL-CLOSED: confirm non-zero branches exist for gateway, SSOT, fills, idempotency, and replay failures.
- Regression: existing scripts are only called, not modified.

## Current Status
- Applied and verified for the requested artifacts plus the blocking 20260515 canonical replay inconsistency.
- `run_ops_sanity_quick.bat` now exits `0` on the current evidence set.
- Current RootA replay comparison is PASS.

## Remaining Issues
- None in the requested scope.
- Not tested: remote CI service integration outside this Windows host.

## Verification Result
- Syntax / batch path: PASS. `run_ops_sanity_quick.bat` executed through step 5.
- Execution: PASS. `E:\1_Data\run_ops_sanity_quick.bat` exited with `LASTEXITCODE=0`.
- Output artifact: PASS. `E:\1_Data\2_Logs\ops_sanity_quick_latest.json` was written with `status=PASS`, `reason=all_checks_passed`, `exit_code=0`, `D=20260515`.
- Policy: PASS. No `/admin/pause`, autotrade, score, threshold, Gate, LOCK, order, fill, ledger, or stats policy change was added.
- FAIL-CLOSED: PASS. Before repair, canonical replay failure was propagated as process exit code 10; after repair, all checks pass.
- Regression: PASS for source changes. Existing scripts were called, not modified.

## Root Cause / Repair
- Cause: the prior canonical shadow summary for D `20260515` used `E:\1_Data\paper\kis_fills_api_20260515.csv` with `input_rows_for_D=0`, while the replay compare checks `E:\1_Data\paper\fills.csv`.
- Repair: ran `E:\1_Data\tools\canonical_fills_shadow.py --date 20260515 --fills E:\1_Data\paper\fills.csv` after backup.
- Result: `events_appended=16`, `duplicates_ignored=6`, `conflicts=0`; canonical log line count is now 22.
- Replay verification: `source_events=22`, `canonical_events=22`, `source_keys_equal_canonical=true`, `state_hash_equal=true`, `chain_hash_equal=true`.

## CI / Scheduler / Incident Automation
- Added `E:\1_Data\run_ops_sanity_ci.bat`.
  - Calls `run_ops_sanity_quick.bat`.
  - Preserves the quick sanity exit code.
  - Generates an incident report on non-zero exit.
- Added `E:\1_Data\tools\build_ops_incident_report.py`.
  - Reads `E:\1_Data\docs\references\ROOTA_INCIDENT_REPORT_TEMPLATE.md`.
  - Fills incident fields from `ops_sanity_quick_latest.json` and linked evidence JSONs.
  - Writes incident markdown under `E:\1_Data\2_Logs\incidents`.
- Added `E:\1_Data\tools\register_ops_sanity_scheduler.ps1`.
  - Default mode is dry-run only.
  - `-Apply` registers `Buffett-Ops-Sanity-Quick` to run `E:\1_Data\run_ops_sanity_ci.bat` daily at `08:45`.
- Validation:
  - `py_compile` PASS for `build_ops_incident_report.py`.
  - `run_ops_sanity_ci.bat` PASS with `LASTEXITCODE=0`.
  - Scheduler dry-run PASS: `E:\1_Data\2_Logs\ops_sanity_scheduler_plan_latest.json` has `status=DRY_RUN`, command `cmd.exe /c call "E:\1_Data\run_ops_sanity_ci.bat"`.
  - Incident auto-substitution PASS for PASS and synthetic FAIL samples; no `{{...}}` placeholders remained, and six verification rows were filled.
  - Scheduler apply PASS after explicit approval: task `Buffett-Ops-Sanity-Quick` is `Ready`, daily at `08:45`, command `cmd.exe /c call E:\1_Data\run_ops_sanity_ci.bat`.
  - Scheduler one-shot run PASS: `LastRunTime=2026-05-17 14:16:46`, `LastTaskResult=0`, `NextRunTime=2026-05-18 08:45:00`.
  - Scheduled-run artifact PASS: `E:\1_Data\2_Logs\ops_sanity_quick_latest.json` has `status=PASS`, `reason=all_checks_passed`, `exit_code=0`, `D=20260515`, run log `E:\1_Data\2_Logs\ops_sanity_quick_20260517_141647.log`.
