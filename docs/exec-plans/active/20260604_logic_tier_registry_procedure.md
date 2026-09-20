# 2026-06-04 Logic Tier Registry Procedure

## Scope
- RootA procedure implementation only.
- Adapt `D:\개발절차.txt` into RootA-specific operating-impact classification.
- Add a reference procedure and initial registry.
- Do not change Gate, LOCK, STOP, risk thresholds, score weights, order routing, fills, ledger, broker dispatch, or runtime entry behavior.

## Backup
- `E:\1_Data\backup\20260604_logic_tier_procedure\20260604_093800`
  - `PLANS.md.bak`

## Change
- Added `E:\1_Data\docs\references\ROOTA_LOGIC_TIER_PROCEDURE.md`.
- Added `E:\1_Data\config\logic_tier_registry.json`.
- Updated `E:\1_Data\.agent\PLANS.md`.

## RootA Procedure Decision
Use these tiers:
- `stable`: current operating path or artifact with possible trading, policy, score, order, fill, ledger, or risk effect.
- `candidate`: proposed operating change requiring same-condition comparison before promotion.
- `research`: idea or experiment with no operating effect.
- `shadow`: observe-only runtime artifact, not buy or policy approval.
- `hotfix`: narrow operational-correctness fix, separate from strategy/policy improvement.
- `deprecated`: retained for audit only.

## Required Registry Fields
- `logic_id`
- `logic_tier`
- `status`
- `owner_path`
- `source_artifacts`
- `output_artifacts`
- `trading_effect`
- `policy_effect`
- `order_path_effect`
- `score_effect`
- `stable_comparison_required`
- `promotion_status`
- `promotion_blockers`
- `validation_evidence`
- `notes`

## Initial Registry Entries
- `roota_daily_paper_stable_chain`: Stable but currently blocked by official batch failure.
- `news_signal_shadow_stage`: Shadow-only, no trading or policy effect.
- `surge_entry_change_exception_policy_candidate`: Candidate, plan-only.
- `intraday_surge_policy_group_separation`: Candidate/read-only partial.
- `logic_tier_registry_procedure`: Hotfix/procedure-only entry.

## Phase 2 Registry Expansion
- Expansion date: 2026-06-04
- Added current high-impact RootA entries:
  - `final_score_merge_stable_scoring_chain`
  - `risk_orchestration_stable_guard`
  - `p1_entry_gate_stable_guard`
  - `kis_order_dispatch_stable_path`
  - `backtest_validation_acceptance_gate`
  - `trading_stage_validation_transition_gate`
  - `future_signal_preview_shadow`
  - `surge_realtime_detector_stable_observer`
  - `surge_lob_orderflow_shadow_guard`
- No runtime code or policy values changed.
- Important current-state blockers kept visible:
  - official daily batch is still failed.
  - `backtest_validation_latest.json` has `passed=false`.
  - `trading_stage_validation_latest.json` remains HOLD / paper-stage hold.
  - current dispatch evidence is mock dry-run, not live execution.

### Phase 2 Validation
- Functional: PASS
  - `config\logic_tier_registry.json` parsed successfully after expansion.
  - Registry has `14` entries.
- Consistency: PASS
  - Required fields exist for every entry.
  - All `logic_tier` values are allowed.
  - Duplicate `logic_id` count is `0`.
- Operational reflection: PASS for registry/procedure artifacts only.
  - Top-level registry runtime effect flags remain all `false`.
  - No runtime code or policy values changed.
- Policy: PASS
  - Stable entries keep current blockers visible instead of promoting readiness.
  - Shadow entries remain non-trading.
- FAIL-CLOSED: PASS
  - Dispatch evidence is marked mock/dry-run only.
  - Validation failures and HOLD states remain blockers.
- Regression: PASS
  - `git diff --check` returned no findings for changed files.
- Mojibake scan: PASS for new procedure files.
  - `E:\1_Data\2_Logs\mojibake_text_scan_logic_tier_phase2_newfiles_20260604.json`
  - `files_scanned=3`, `issue_file_count=0`, `issue_count=0`, `repairable_count=0`

## Phase 3 Registry Validator
- Added read-only validator:
  - `E:\1_Data\tools\validate_logic_tier_registry.py`
- Output artifacts:
  - `E:\1_Data\2_Logs\logic_tier_registry_validation_latest.json`
  - `E:\1_Data\2_Logs\logic_tier_registry_validation_latest.csv`
- Added registry entry:
  - `logic_tier_registry_validator`
- Runtime effect:
  - `trading_effect=false`
  - `policy_effect=false`
  - `order_path_effect=false`
  - `score_effect=false`
- Validation status:
  - Functional: PASS
    - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile E:\1_Data\tools\validate_logic_tier_registry.py`
  - Execution: PASS_WITH_WARNINGS
    - `E:\1_Data\_runtime\python312-embed\python.exe E:\1_Data\tools\validate_logic_tier_registry.py`
    - `status=PASS_WITH_WARNINGS`, `entries=15`, `errors=0`, `warnings=1`
  - Artifact: PASS
    - `E:\1_Data\2_Logs\logic_tier_registry_validation_latest.json`
    - `E:\1_Data\2_Logs\logic_tier_registry_validation_latest.csv`
  - Warning retained by design:
    - `kis_order_dispatch_stable_path`: order path effect exists, but current evidence remains mock/dry-run/read-only.
  - Mojibake scan: PASS
    - `E:\1_Data\2_Logs\mojibake_text_scan_logic_tier_phase3_newfiles_20260604.json`
    - `files_scanned=3`, `issue_file_count=0`, `issue_count=0`, `repairable_count=0`

## Phase 4 Batch Post-Chain Hook
- Added read-only post-chain call in `E:\1_Data\run_paper_daily.bat`:
  - `[16.959/16] tools\validate_logic_tier_registry.py`
- Updated `logic_tier_registry_validator` registry entry to include the batch hook as a source artifact.
- Runtime effect:
  - `trading_effect=false`
  - `policy_effect=false`
  - `order_path_effect=false`
  - `score_effect=false`
- Batch behavior:
  - Registry errors return non-zero from the validator and are routed through existing `POST_CHAIN_STEP_FAIL`.
  - Current warning-only state remains warning-only and does not hard-fail the post-chain.
- Full `run_paper_daily.bat` was not rerun for this hook validation in this phase.

## Validation Plan
1. Parse `config\logic_tier_registry.json`.
2. Confirm required fields exist in all entries.
3. Confirm procedure files exist.
4. Confirm registry has no runtime effect flags set at top level.
5. Confirm no trading code, config thresholds, order, fill, ledger, broker, Gate, or LOCK files were changed.
6. Run markdown/string scan if available for the new and changed text files.

## Validation Result
- Functional: PASS
  - `config\logic_tier_registry.json` parsed successfully.
  - Registry has `5` initial entries.
- Consistency: PASS
  - All entries include the required fields.
  - All `logic_tier` values are in the allowed tier set.
- Operational reflection: PASS for procedure artifacts only.
  - Procedure files exist at the expected paths.
  - Top-level registry effect flags are all `false`.
- Policy: PASS
  - This change is procedure-only and does not change Gate, LOCK, STOP, risk, score, order, fill, ledger, broker, or dashboard behavior.
- FAIL-CLOSED: PASS
  - Candidate and shadow entries remain non-trading by default.
  - Stable chain current blocker remains visible instead of being hidden.
- Regression: PASS for changed-file whitespace check.
  - `git diff --check` returned no findings for the changed files.
- Mojibake scan: PASS for the new procedure files.
  - `E:\1_Data\2_Logs\mojibake_text_scan_logic_tier_newfiles_20260604.json`
  - `files_scanned=3`, `issue_file_count=0`, `issue_count=0`, `repairable_count=0`
- Mojibake scan: FAIL for the broader changed-file set because `.agent\PLANS.md` has pre-existing non-repairable issues.
  - `E:\1_Data\2_Logs\mojibake_text_scan_logic_tier_procedure_20260604.json`
  - `files_scanned=4`, `issue_file_count=1`, `issue_count=1059`, `repairable_count=0`
  - issue file: `E:\1_Data\.agent\PLANS.md`

## Remaining Actual Problem
- This procedure does not fix the current `run_paper_daily.bat` failure.
- This procedure does not promote any Candidate or Shadow output to Stable.
- Future changes must keep this registry current.
- Existing `.agent\PLANS.md` mojibake issues remain outside this procedure scope.
