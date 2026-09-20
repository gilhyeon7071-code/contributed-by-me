# 2026-05-14 Entry Decision Layer Separation

## Scope
- RootA only: `E:\1_Data`.
- Separate runtime entry decision evidence into alpha, risk, and execution layers.
- Do not change entry score formulas, thresholds, risk gates, sizing, order dispatch, fills, ledger, or stats.

## Backup
- `E:\1_Data\backup\20260514_entry_decision_layer_separation\20260514_101609`
- Files backed up:
  - `paper_engine.py.bak`
  - `PLANS.md.bak`

## Step Procedure
1. Read current entry runtime flow in `paper_engine.py`.
2. Add observability-only artifact paths for runtime decision layers.
3. Add helper functions that classify existing candidate, risk, and execution evidence without changing decisions.
4. Write `entry_decision_layers_runtime_latest.csv/json` after the existing entry signal snapshot.
5. Run syntax validation.
6. Run isolated execution validation of the new writer against existing latest artifacts.
7. Check generated CSV/JSON result values.
8. Update this md and `.agent\PLANS.md`.

## Current Progress
- Step 1: done.
- Step 2: done.
- Step 3: done.
- Step 4: done.
- Step 5: done.
- Step 6: done.
- Step 7: done.
- Step 8: done.

## Output Artifacts
- `E:\1_Data\2_Logs\entry_decision_layers_runtime_latest.csv`
- `E:\1_Data\2_Logs\entry_decision_layers_runtime_latest.json`

## Remaining Issues
- Full `run_paper_daily.bat` E2E has not been run because it includes RootB ledger repair steps outside this RootA-only scope.
- The change is structure and evidence separation only; it does not decide whether alpha thresholds should be changed.
- Runtime evidence should be checked again after the next scheduled full daily batch.

## Validation Log
- Syntax validation: PASS.
  - Command: `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile E:\1_Data\paper_engine.py`
- Execution validation: PASS.
  - Isolated call imported `paper_engine` and ran `_write_entry_decision_layers_snapshot(...)` against latest RootA artifacts.
  - Input rows: candidates `9`, entry decision rows `1`.
- Result artifact validation: PASS.
  - `entry_decision_layers_runtime_latest.json.status=PASS`
  - `d_ref=20260513`
  - `candidate_rows=9`
  - `decision_rows=1`
  - `snapshot_rows=10`
  - `alpha_layer_counts={"ALPHA_WATCH":8,"ALPHA_ELIGIBLE":2}`
  - `risk_layer_counts={"REDUCED":10}`
  - `execution_layer_counts={"NOT_EVALUATED":9,"FILLED_OR_ORDERED":1}`
  - `policy_change=false`
- Policy validation: PASS.
  - Added observability-only artifact generation.
  - No score formula, threshold, risk gate, sizing, order, fill, ledger, or stats policy changed.
- FAIL-CLOSED validation: PASS for code path preservation.
  - Existing execution gates are not bypassed or relaxed.
  - The new writer runs after the existing entry signal snapshot and does not feed back into candidate filtering or order dispatch.
- Regression validation: PARTIAL.
  - Syntax, isolated execution, artifact schema, and mojibake checks passed.
  - Full official batch E2E was not run in this step.

## Mojibake Check
- `paper_engine.py`: PASS, issues `0`.
- This md file: PASS, issues `0`.

## Runtime Verify - 2026-05-14 10:40
- Backup:
  - `E:\1_Data\backup\20260514_entry_decision_layer_runtime_verify\20260514_104000`
- Command:
  - `E:\1_Data\_runtime\python312-embed\python.exe E:\1_Data\paper_engine.py`
- Execution result:
  - Exit code: `0`
  - Log: `E:\1_Data\2_Logs\run_paper_engine_runtime_verify_last.txt`
- Runtime artifact result:
  - `entry_decision_layers_runtime_latest.json.status=PASS`
  - `generated_at=2026-05-14T10:40:30`
  - `d_ref=20260514`
  - `candidate_rows=1`
  - `decision_rows=1`
  - `snapshot_rows=1`
  - `alpha_layer_counts={"ALPHA_ELIGIBLE":1}`
  - `risk_layer_counts={"REDUCED":1}`
  - `execution_layer_counts={"BLOCKED_OR_HELD":1}`
  - `policy_change=false`
- Runtime log evidence:
  - Selected code: `003280`
  - Execution reason: `NORMAL_INTRADAY_ENTRY_GATE_NOT_ALLOW(decision=REDUCE)`
  - `new_fills=0`
  - `new_trades=0`
- Operational file comparison:
  - `paper\fills.csv`: unchanged.
  - `paper\trades.csv`: unchanged.
  - `paper\paper_state.json`: changed 6 lines from the engine run (`atr14_pct`, `atr14_pct_source`, `market_cap` metadata on existing open position).

## Validation Matrix After Runtime Verify
- 기능 검증: PASS. Runtime engine path generated the new decision-layer CSV/JSON.
- 정합성 검증: PASS. JSON and CSV agree on one candidate and one blocked/held execution decision.
- 운영 반영 검증: PASS for RootA `paper_engine.py` path. Full daily batch remains not run.
- 정책 검증: PASS. Runtime result shows `policy_change=false`; no score, threshold, gate, sizing, order, fill, ledger, or stats policy changed.
- FAIL-CLOSED 검증: PASS. Entry was held by existing `NORMAL_INTRADAY_ENTRY_GATE_NOT_ALLOW(decision=REDUCE)`.
- 회귀 검증: PARTIAL. Engine execution returned `0`, fills/trades unchanged, but full `run_paper_daily.bat` regression remains outside this RootA-only step.

## Step 2/3 Implementation - 2026-05-14 11:37

### Scope
- Implement reusable Step 2 chain-impact verification and Step 3 report evidence generation.
- RootA code only.
- RootB files are read-only signatures only; no RootB write.

### Backup
- `E:\1_Data\backup\20260514_entry_layer_verify_steps_2_3\20260514_113547`

### Added Tool
- `E:\1_Data\tools\verify_entry_decision_layer_runtime.py`

### What It Checks
- Entry decision layer JSON exists and has `status=PASS`.
- `policy_change=false`.
- `snapshot_rows > 0`.
- Optional D check: `entry_decision_layers_runtime_latest.json.d_ref == paper\fills.csv D rule`.
- Optional backup comparison for:
  - `paper\fills.csv`
  - `paper\trades.csv`
  - `paper\paper_state.json`
  - entry decision layer CSV/JSON
- RootB read-only signatures:
  - `E:\vibe\buffett\data\ledger\paper_fills_ledger.csv`
  - `E:\vibe\buffett\data\orders\replay_orders_latest.json`
  - `E:\vibe\buffett\data\live\live_fills.csv`

### Important Correction
- `fills/trades` changes are WARN by default, not FAIL.
- Reason: in full daily batch, fills/trades can legitimately change.
- Use `--fail-on-chain-change` only when the test requires no fills/trades mutation.

### Validation
- Syntax validation: PASS.
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile E:\1_Data\tools\verify_entry_decision_layer_runtime.py`
- Execution validation: PASS.
  - Command:
    - `E:\1_Data\_runtime\python312-embed\python.exe E:\1_Data\tools\verify_entry_decision_layer_runtime.py --backup-dir E:\1_Data\backup\20260514_entry_decision_layer_runtime_verify\20260514_104000 --require-d-match`
- Result artifact:
  - `E:\1_Data\2_Logs\entry_decision_layer_runtime_verify_latest.json`
  - `status=PASS`
  - `artifact_status_pass=PASS`
  - `policy_change_false=PASS`
  - `snapshot_rows_positive=PASS`
  - `d_ref_matches_fills_d_rule=PASS`
  - `fills_unchanged_vs_backup=WARN`
  - `trades_unchanged_vs_backup=WARN`
- Mojibake scan:
  - `verify_entry_decision_layer_runtime.py`: PASS, issues `0`.

### Remaining After Step 2/3
- Full `run_paper_daily.bat` can be verified with this tool after a scheduled batch.
- If a strict no-mutation test is needed, run the tool with `--fail-on-chain-change` against a fresh backup made immediately before that test.

## Scheduled Runtime Verification - 2026-05-18 15:03

### Scope
- Verify latest entry decision layer artifact after subsequent runtime/batch activity.
- No code, policy, score, gate, order, fill, ledger, or RootB modification.

### Backup
- `E:\1_Data\backup\20260518_entry_layer_runtime_verify\20260518_150259`

### Command
- `E:\1_Data\_runtime\python312-embed\python.exe E:\1_Data\tools\verify_entry_decision_layer_runtime.py --require-d-match`

### Evidence
- Report:
  - `E:\1_Data\2_Logs\entry_decision_layer_runtime_verify_latest.json`
- Entry decision layer:
  - `generated_at=2026-05-18T15:02:45`
  - `status=PASS`
  - `d_ref=20260518`
  - `fills_d_rule=20260518`
  - `candidate_rows=1`
  - `decision_rows=1`
  - `snapshot_rows=1`
  - `alpha_layer_counts={"ALPHA_ELIGIBLE":1}`
  - `risk_layer_counts={"REDUCED":1}`
  - `execution_layer_counts={"BLOCKED_OR_HELD":1}`
  - `policy_change=false`
- CSV row:
  - `code=018670`
  - `name=SK가스`
  - `rank_score=0.280393`
  - `alpha_layer=ALPHA_ELIGIBLE`
  - `risk_layer=REDUCED`
  - `execution_layer=BLOCKED_OR_HELD`
  - `execution_reason=SAME_CODE_DAY_ALREADY_BUY`
  - `signal=HOLD`

### Validation Matrix
- 기능 검증: PASS. Latest verify report was generated.
- 정합성 검증: PASS. `d_ref` matches fills D rule `20260518`.
- 운영 반영 검증: PASS. Runtime layer artifact exists with current timestamp after later runtime activity.
- 정책 검증: PASS. `policy_change=false`.
- FAIL-CLOSED 검증: PASS. Execution remained `HOLD` with `SAME_CODE_DAY_ALREADY_BUY`.
- 회귀 검증: PASS for this verification scope. No failed or warning checks in the verify report.

### Remaining
- None for this verification scope.

## Policy Change - 2026-05-21 Entry Fallback Selection

### Scope
- RootA entry selection policy.
- Convert repeated execution-layer HOLD from `SAME_CODE_DAY_ALREADY_BUY` into pre-selection exclusion.
- Add fallback candidate expansion under one-pick mode so blocked top candidates do not end the candidate list immediately.
- No score formula, risk gate threshold, order dispatch, fill schema, ledger schema, or RootB code change.

### Backup
- Code/config before policy change:
  - `E:\1_Data\backup\20260521_entry_fallback_policy_change\20260521_131439`
- Runtime files before post-change execution:
  - `E:\1_Data\backup\20260521_entry_fallback_policy_runtime\20260521_131643`
- Verify tool before zero-candidate adjustment:
  - `E:\1_Data\backup\20260521_entry_verify_zero_candidate_adjust\20260521_131723`

### Changed Files
- `E:\1_Data\paper_engine.py`
- `E:\1_Data\paper\paper_engine_config.json`
- `E:\1_Data\paper\paper_engine_config.lock.json`
- `E:\1_Data\tools\verify_entry_decision_layer_runtime.py`

### Policy Detail
- Added `entry_selection_policy.skip_same_code_day_already_buy=true`.
- Added `entry_selection_policy.fallback_after_block.enabled=true`.
- Added fallback candidate cap:
  - `max_candidates=3`
  - `when_max_new_le=1`
- Runtime selection now excludes codes already bought today before one-pick selection.
- One-pick mode can retain additional candidates for later loop evaluation when configured.

### Before
- Source:
  - `E:\1_Data\backup\20260521_entry_fallback_policy_change\20260521_131439\2_Logs_entry_decision_layers_runtime_latest.json.bak`
- Result:
  - `candidate_rows=1`
  - `decision_rows=1`
  - `snapshot_rows=1`
  - `alpha_layer_counts={"ALPHA_ELIGIBLE":1}`
  - `risk_layer_counts={"REDUCED":1}`
  - `execution_layer_counts={"BLOCKED_OR_HELD":1}`
- Row:
  - `code=049960`
  - `name=쎌바이오텍`
  - `rank_score=0.222237`
  - `execution_reason=SAME_CODE_DAY_ALREADY_BUY`
  - `signal=HOLD`

### After
- Source:
  - `E:\1_Data\2_Logs\entry_decision_layers_runtime_latest.json`
- Result:
  - `candidate_rows=0`
  - `decision_rows=0`
  - `snapshot_rows=0`
  - `alpha_layer_counts={}`
  - `risk_layer_counts={}`
  - `execution_layer_counts={}`
- Runtime log evidence:
  - `skip_same_code_day_already_buy applied: 1->0`
  - excluded codes included `049960`, `290690`, `381620`
  - `new_fills=0`
  - `new_trades=0`

### Validation
- Syntax validation: PASS.
  - `paper_engine.py`
  - `tools\verify_entry_decision_layer_runtime.py`
- Config lock validation: PASS.
  - `MATCH=True`
  - `approved_sha256=862b655ea849e999511fe4243df8c6bdd7e505bd8e797f8844cd9f2ac65b3c7b`
- JSON validation: PASS.
  - `paper_engine_config.json`
  - `paper_engine_config.lock.json`
- Execution validation: PASS.
  - `E:\1_Data\_runtime\python312-embed\python.exe E:\1_Data\paper_engine.py`
  - exit code `0`
- Result artifact validation: PASS.
  - `entry_decision_layer_runtime_verify_latest.json.status=PASS`
  - `snapshot_rows_consistent=PASS`
  - `d_ref_matches_fills_d_rule=PASS`
- Mojibake scan:
  - `paper_engine.py`: PASS, issues `0`
  - `paper_engine_config.json`: PASS, issues `0`
  - `verify_entry_decision_layer_runtime.py`: PASS, issues `0`
- Operational comparison:
  - `paper\fills.csv`: unchanged versus runtime backup.
  - `paper\trades.csv`: unchanged versus runtime backup.
  - `paper\paper_state.json`: unchanged versus runtime backup.

### Validation Matrix
- 기능 검증: PASS. Same-day already-bought candidate is filtered before execution.
- 정합성 검증: PASS. `d_ref=20260521` matches fills D rule.
- 운영 반영 검증: PASS. Engine run used the new selection policy and wrote current artifacts.
- 정책 검증: PASS. This is an explicit selection policy change; risk gates and order/fill policy were not weakened.
- FAIL-CLOSED 검증: PASS. The changed behavior removes duplicate same-day entry candidates instead of bypassing the block.
- 회귀 검증: PASS for this policy-change scope. Full `run_paper_daily.bat` E2E was not run.

### Remaining
- This change does not relax drawdown, risk orchestration, gap risk, or production risk gates.
- If all remaining candidates are already bought today, result becomes zero candidates instead of execution-layer HOLD.

## Full E2E Validation - 2026-05-21

### Scope
- Official daily batch validation for the entry selection policy change.
- RootA-only logic, runtime artifact, SSOT chain, and FAIL-CLOSED verification are recorded here.

### Additional Fixes During Validation
- `run_paper_daily.bat` shadow paper-engine path was isolated:
  - main artifact remains `E:\1_Data\2_Logs\entry_decision_layers_runtime_latest.*`
  - shadow artifact now writes to `E:\1_Data\2_Logs\entry_decision_layers_runtime_shadow_latest.*`
- `paper_engine.py` now writes a current empty-candidate decision-layer snapshot when post-selection candidates are empty.

### Evidence
- `E:\1_Data\run_paper_daily.bat`: exit code `0`
- `E:\1_Data\run_paper_daily_last.txt`: `[WRAPPER_EXIT] rc=0`
- `E:\1_Data\2_Logs\entry_decision_layer_runtime_verify_latest.json`:
  - `status=PASS`
  - `d_ref=20260521`
  - `candidate_rows=0`
  - `decision_rows=0`
  - `snapshot_rows=0`
  - `d_ref_matches_fills_d_rule=PASS`
- `E:\1_Data\2_Logs\recovery_ssot_chain_latest.json`:
  - `status=PASS`
  - `D=20260521`
  - `orders_exec` exists
  - `exec_date_unique=["20260521"]`
  - `issues=[]`

### Result
- The previous repeated result changed from execution-layer HOLD on a same-day duplicate candidate to pre-selection removal.
- Current main latest artifact is a fresh zero-candidate snapshot for `D=20260521`.
- Shadow `d_ref=20260422` no longer overwrites the main latest artifact.

### Remaining
- Initial `nightly_data_integrity_latest.json` had WARN-only issues:
  - candidate count shift after policy filtering.
  - expected config hash warning from the intentional batch file change.
- Re-running `tools\nightly_data_integrity_check.py --warn-only` cleared the config hash warning.
- The only remaining current WARN is candidate count shift.

## Candidate Count Shift Diagnosis - 2026-05-21

### Evidence
- Re-running `tools\nightly_data_integrity_check.py --warn-only` cleared the intentional `run_paper_daily.bat` config hash WARN.
- The remaining WARN is only `candidates_latest_data_daily`:
  - `today_count=3`
  - `median_30d=12.0`
  - `delta_pct=-0.75`
- Candidate contraction evidence:
  - `sector_prefilter_union.after=12`
  - `macd_final_guard.removed=9`
  - `macd_final_guard.after=3`
  - `liquidity_filter_daily_last.rows_after_rule_filter=2`
  - removed `066980` due `limit_up_near(day_ret_pct=29.99>=28.0)`
  - `run_paper_daily_last.txt` shows `entry_candidates=1`

### Result
- Remaining candidate-count WARN is explained by fail-closed filters, mainly MACD final guard and limit-up liquidity guard.
- It is not a stale artifact problem and not caused directly by the same-day duplicate pre-selection policy.

### Remaining
- Keep this WARN visible unless a separate policy decision changes MACD final guard, liquidity limit-up guard, or entry pool constraints.

## MACD Final Guard Soft Policy - 2026-05-21

### Decision
- Apply the next structural policy step requested by the user.
- Convert MACD final guard from hard deletion to default soft penalty in `generate_candidates_v41_1.py`.
- Keep `hard` and `label_only` modes available by environment variable.

### Implementation
- New default mode:
  - `CANDIDATE_MACD_FINAL_GUARD_MODE=soft_penalty`
- New default penalty:
  - `CANDIDATE_MACD_FINAL_GUARD_PENALTY=0.15`
- New output fields:
  - `macd_final_guard_pass`
  - `macd_final_guard_penalty`
  - `macd_final_guard_mode`
- The penalty is applied after `final_score` is created.

### Before / After
- Before:
  - `sector_prefilter_union.after=12`
  - `macd_final_guard.removed=9`
  - `macd_final_guard.after=3`
  - candidate count integrity WARN remained visible.
- After:
  - `sector_prefilter_union.after=12`
  - `macd_final_guard.mode=soft_penalty`
  - `macd_final_guard.removed=0`
  - `macd_final_guard.penalized_rows=9`
  - `macd_final_guard.after=12`
  - `nightly_data_integrity_latest.json.ok=true`
  - `nightly_data_integrity_latest.json.issue_count=0`

### Runtime Evidence
- `paper_engine.py` produced a new BUY:
  - `046970`
  - `PAPER_BUY_046970_20260521`
  - `20260521T14:23:35`
- The following pending/order reflection path produced another BUY:
  - `032820`
  - `PAPER_BUY_032820_20260521`
  - `20260521T14:24:24`
- `entry_decision_layer_runtime_verify_latest.json.status=PASS`.
- Latest entry-layer artifact after pending/order reflection:
  - `candidate_rows=2`
  - `decision_rows=0`
  - `snapshot_rows=2`
  - `execution_layer_counts={NOT_EVALUATED:2}`
- `recovery_ssot_chain_latest.json.status=PASS`.
- `ledger_live_fills_dry_run_latest.json.status=PASS`.

### Boundary
- This is a candidate-generation policy change.
- It does not relax:
  - risk gates
  - kill switch
  - order dispatch contract
  - fill schema
  - ledger schema
  - D rule
  - same-day duplicate guard

### Remaining
- Full `run_paper_daily.bat` wrapper was not rerun after this final policy change because live paper side effects already occurred.
- Current remaining observed runtime issue is separate:
  - `pending_entry_status_latest.json.partial_exit_policy_summary.status=FAIL`
  - `duplicate_trade_signatures=2`
  - This belongs to sell/partial-exit lifecycle validation, not MACD candidate scoring.

## Entry Execution Score Policy - 2026-05-21

### Decision
- Add an execution-level score layer inside `paper_engine.py`.
- This addresses the remaining structural issue where execution selection was still dominated by state/operation ordering.

### Implementation
- Added `_apply_entry_execution_score`.
- New runtime fields:
  - `entry_execution_score`
  - `entry_execution_reason`
- Base score:
  - candidate `final_score` or `score`.
- Positive execution factors:
  - positive entry criteria
  - execution pool
  - MACD pass
  - sector allowed / sector strength
  - split-entry second as score bonus
- Penalty factors:
  - MACD soft penalty
  - KRX watch flags
  - overheat by `v_accel`, `ret1_pct`, `atr14_pct`
- Changed split-entry second from absolute priority to default score bonus:
  - `split_entry_2nd_priority=SCORE_BONUS`

### Runtime Evidence
- `paper_engine.py` exit code `0`.
- Execution-score ranking was printed:
  - `049960 entry_execution_score=0.279039`
  - `046970 entry_execution_score=0.189964`
  - `032820 entry_execution_score=0.188129`
- Split-entry second no longer had absolute priority:
  - `split_entry_2nd priority mode=SCORE_BONUS score_bonus_rows=2`
- Same-day duplicate guard stayed active:
  - `skip_same_code_day_already_buy applied: 3->0`
- Result:
  - `new_fills=0`
  - `new_trades=0`
  - `entry_decision_layer_runtime_verify_latest.json.status=PASS`
  - `recovery_ssot_chain_latest.json.status=PASS`
  - `ledger_live_fills_dry_run_latest.json.status=PASS`
  - `nightly_data_integrity_latest.json.ok=true`

### Boundary
- This is an execution selection policy change.
- It does not relax:
  - risk gates
  - kill switch
  - same-day duplicate guard
  - split-entry dip validation
  - order dispatch contract
  - fill schema
  - ledger schema
  - D rule

### Remaining
- The latest runtime correctly selected by execution score but produced no new BUY because all top candidates were already bought today.
- Full `run_paper_daily.bat` wrapper was not rerun after this final policy change.
- Separate sell/partial-exit issue remains:
  - `partial_exit_policy_summary.status=FAIL`
  - `duplicate_trade_signatures=2`

## Partial Exit Duplicate Lifecycle Repair - 2026-05-21

### Decision
- Repair the actual sell lifecycle duplication found after the execution-score policy run.
- Keep this separate from buy selection policy work.

### Cause
- The sell trade dedup stage removed duplicate rows, but the legacy recovery stage could add back the same lifecycle row.
- The recovery key was too coarse for partial-exit and replayed stop rows.

### Implementation
- Added lifecycle signature based on:
  - code
  - exit reason
  - sell quantity
  - signal date
  - sell ratio
  - partial-exit flag
  - prior stop count
  - replay chain id
- Applied lifecycle dedupe to sell fills and sell trades.
- Updated legacy sell recovery so valid lifecycle signatures prevent duplicate recovery.

### Runtime Evidence
- `paper_engine.py` exit code `0`.
- `pending_entry_status_latest.json`:
  - `entry_exit_lifecycle.status=PASS`
  - `state_machine_summary.status=PASS`
  - `sell_order_lifecycle_summary.status=PASS`
  - `partial_exit_policy_summary.status=PASS`
  - `sell_recovery_chain_summary.status=PASS`
  - `duplicate_precheck.duplicate_fill_order_ids=[]`
  - `duplicate_precheck.duplicate_trade_signatures=[]`
- Paper CSV state:
  - `fills.csv` rows `627`
  - `trades.csv` rows `332`
- Chain checks:
  - `entry_decision_layer_runtime_verify_latest.json.status=PASS`
  - `recovery_ssot_chain_latest.json.status=PASS`
  - `ledger_live_fills_dry_run_latest.json.status=PASS`
  - `nightly_data_integrity_latest.json.ok=true`

### Boundary
- This is a sell lifecycle recovery/dedup bug fix.
- It does not relax:
  - buy risk gates
  - kill switch
  - same-day duplicate guard
  - order dispatch contract
  - D rule

### Remaining
- Full `run_paper_daily.bat` wrapper was not rerun after this repair.
- RootB was resynced after the runtime validation produced one normal DDM forced-liquidation SELL fill.
