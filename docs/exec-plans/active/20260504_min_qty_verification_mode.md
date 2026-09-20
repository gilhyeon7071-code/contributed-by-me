# 2026-05-04 Minimum Quantity Verification Mode Plan

## Scope
- Planning only for a possible minimum-quantity paper verification mode.
- No code, config, lock, ledger, fills, orders, stats, Gate, STOP, D rule, or broker apply change in this step.
- Existing paper ledger and historical artifacts must remain audit evidence.

## Current Evidence
- D rule: `paper/fills.csv` latest BUY ymd is `20260430`.
- Latest official batch artifact: `2_Logs/run_paper_daily_last.txt`.
- Latest new BUY count: `0`.
- Latest SELL count: `8`.
- New-buy-zero classification: operational block.
- Main evidence:
  - `2_Logs/p1_entry_gate_status_latest.json`
    - `entry_gate_decision_before_p1=BLOCK`
    - `max_new_before=0`
    - `max_new_after=0`
    - `risk_orchestration.scale=0.0`
    - `risk_orchestration.dd_current=0.20678058086726792`
    - `risk_orchestration.dd_stop=0.15`
  - `2_Logs/paper_order_validation_report_latest.json`
    - `buy_order_rows=0`
    - `sell_order_rows=8`
    - `chain_ssot.status=PASS`
  - `2_Logs/forecast_score_validation_latest.json`
    - `status=PASS`
    - `required_bad=0`
  - `2_Logs/run_paper_daily_last.txt`
    - `risk_orch_size_zero:dd_stop,kelly_zero`
    - `ENTRY_GATE BLOCK -> force max_new=0`
    - `FILL_SUMMARY attempted=0 filled=0`

## Decision Before Apply
- Minimum-quantity verification mode is needed only if the user approves a separate verification session.
- The mode must not change production Gate, STOP, LOCK, D rule, score, or risk policy meanings.
- The mode must use a separated `paper_session_id` or `run_id`.
- The mode must produce at most the explicitly approved minimum BUY chain evidence needed for order-fill-ledger-stats validation.

## Backup Plan Before Any Apply
- Create a new backup directory under:
  - `E:\1_Data\backup\20260504_min_qty_verification_mode\YYYYMMDD_HHMMSS\`
- Back up only files/artifacts that may be touched or overwritten by the approved verification run:
  - `paper/paper_engine_config.json`
  - `paper/paper_engine_config.lock.json`
  - `paper/fills.csv`
  - `paper/trades.csv`
  - `paper/paper_state.json`
  - `virtual_ledger.csv`
  - latest `2_Logs/*paper*latest*.json`
  - latest `2_Logs/*ledger*latest*.json`
  - latest `2_Logs/run_paper_daily_last.txt`
  - `.agent/PLANS.md`

## Apply Plan If Approved
1. Add or use a separated verification control path that is disabled by default.
2. Set verification session identity with a distinct `paper_session_id` or `run_id`.
3. Allow only the minimum required new BUY quantity for one selected eligible candidate.
4. Keep data-quality, date-alignment, stale quote, missing price, ledger consistency, and broker/paper separation checks fail-closed.
5. Run the official or closest safe official path in the separated session.
6. Do not reset or rewrite old ledger history.

## Verification Plan
1. Syntax validation:
   - Compile any touched Python files.
   - Parse touched JSON/lock files.
2. Execution validation:
   - Run the scoped official path for the separated paper verification session.
   - Confirm command return code and latest run log.
3. Result artifact validation:
   - Confirm generated order id, fill id, and ledger id are linked.
   - Confirm `orders(D) -> fills(D) -> ledger -> stats` evidence for the separated session.
   - Confirm no mixed `paper/broker` date/session rows.
   - Confirm `exec_date == D` or explicitly mark the verification session date rule used.
   - Confirm `as_of/run_id/paper_session_id` alignment.
4. Policy validation:
   - Confirm production Gate, STOP, LOCK, D rule, score, threshold, and broker apply meanings are unchanged.
5. FAIL-CLOSED validation:
   - Confirm stale/missing price, missing orders_exec, date mismatch, and paper/broker mixing still block.
6. Regression validation:
   - Confirm existing historical ledger/fills remain present.
   - Confirm existing latest audit artifacts remain backed up.

## Stop Conditions
- Stop if `orders_exec` is missing.
- Stop if `exec_date != D` unless a separated verification date rule is explicitly documented before apply.
- Stop if `as_of/run_id/paper_session_id` are inconsistent.
- Stop if paper and broker dates/sessions are mixed.
- Stop if the mode requires changing production risk or Gate meaning.

## Status
- Applied only as an environment-gated separated verification path.
- Production config, lock, default Gate, STOP, D rule, fills, trades, state, virtual ledger, and broker apply were not changed.
- Verification session: `MINQ_20260504_122214`.
- Session artifacts:
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\fills.csv`
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\trades.csv`
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\paper_state.json`
  - `E:\1_Data\2_Logs\paper_engine_MINQ_20260504_122214.log`
  - `E:\1_Data\2_Logs\pending_entry_status_MINQ_20260504_122214.json`
  - `E:\1_Data\2_Logs\paper_order_validation_report_MINQ_20260504_122214.json`
- Result:
  - `paper_session_id=MINQ_20260504_122214`
  - `run_label=validation`
  - `BUY` rows: `1`
  - BUY order: `PAPER_BUY_003570_20260504_R20260504`
  - BUY qty: `1`
  - order validation: `PASS`
  - lifecycle `buy_order_rows=1`, `buy_fill_rows=1`
  - chain SSOT `entry_fill_rows=1`
  - session rule `invalid_session_rows=0`
- Operational preservation:
  - `paper/fills.csv`, `paper/trades.csv`, `paper/paper_state.json`, and `virtual_ledger.csv` hashes match the pre-apply backup.
- Limitation:
  - Live bridge sync was disabled by `PAPER_DISABLE_LIVE_BRIDGE_SYNC=1`.
  - Production ledger/stats were not updated by this verification session.

## Session Ledger/Stats Validation
- Scope:
  - Session-only artifact generation from `E:\1_Data\paper\sessions\MINQ_20260504_122214\fills.csv`.
  - Existing production `virtual_ledger.csv`, RootB `paper_fills_ledger.csv`, RootB `live_vs_bt.json`, broker dispatch, and broker fill sync were not updated.
- Evidence:
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\ledger\paper_fills_ledger_MINQ_20260504_122214.csv`
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\ledger\open_positions_MINQ_20260504_122214.csv`
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\stats\session_ledger_stats_MINQ_20260504_122214.json`
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\stats\session_chain_report_MINQ_20260504_122214.json`
- Result:
  - `status=PASS`
  - `session_fill_rows=1`
  - `session_ledger_rows=1`
  - `open_position_count=1`
  - Chain report contract: `orders(D) -> fills(D) -> ledger -> stats`
- Official RootB stats path:
  - `NA`.
  - Reason: no session `orders_exec` was produced, and the production RootB stats path is fixed to operational `data/orders`, `data/ledger`, and `data/stats`; it was not run to preserve production artifacts.

## Isolated RootB Stats Validation
- Scope:
  - A RootB-like workspace was created under the separated paper session folder.
  - Production RootB `data/stats/live_vs_bt.json` and `data/ledger/paper_fills_ledger.csv` were not regenerated.
- Workspace:
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\rootb_stats_workspace`
- Inputs:
  - `data\orders\orders_20260504_exec.xlsx`
  - `data\ledger\paper_fills_ledger.csv`
- Outputs:
  - `data\stats\live_vs_bt.json`
  - `data\stats\audit_summary.json`
  - `data\stats\risk_stats.json`
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\stats\isolated_rootb_stats_report_MINQ_20260504_122214.json`
- Result:
  - `live_vs_bt.status=PASS`
  - `as_of_live=20260504`
  - `rows_total=1`
  - `rows_as_of=1`
  - `fills_effective=1`
  - `orders_total=1`
  - `matched_orders=1`
  - `match_rate=1.0`
  - `source_mix_guard.families=["paper"]`
- Scope deviation:
  - First RootB stats run did not set `VIBE_STATS_DIR`, so RootB `config.yaml` routed output to an existing RootB run snapshot stats directory:
    - `E:\vibe\buffett\runs\SSOT_D20260430_FINAL_20260504_20260504_092719\stats`
  - The run was immediately repeated with `VIBE_STATS_DIR` fixed to the isolated workspace.
  - Operational RootB `data/stats/live_vs_bt.json` and `data/ledger/paper_fills_ledger.csv` were not touched.

## RootB Snapshot Stats Restore
- Scope:
  - Restore only the RootB run snapshot stats that were overwritten during the first isolated stats attempt.
  - No production RootA paper files, `virtual_ledger.csv`, RootB `data\stats`, RootB `data\ledger`, broker dispatch, or broker fill sync change.
- Backup before restore:
  - `E:\1_Data\backup\20260504_rootb_snapshot_stats_restore\20260504_131221`
- Restore source:
  - `E:\vibe\buffett\data\stats\*.json`
- Restore target:
  - `E:\vibe\buffett\runs\SSOT_D20260430_FINAL_20260504_20260504_092719\stats`
- Evidence:
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\stats\rootb_snapshot_stats_restore_report_MINQ_20260504_122214.json`
- Result:
  - `status=PASS`
  - All 8 snapshot stats JSON hashes match RootB `data\stats`.
  - Restored `live_vs_bt` values:
    - `status=PASS`
    - `as_of=20260430`
    - `as_of_live=20260430`
    - `rows_total=478`
    - `rows_as_of=28`
    - `orders_total=18`
    - `matched_orders=18`
    - `match_rate=1.0`
    - `source_fullpath=E:\vibe\buffett\data\ledger\paper_fills_ledger.csv`
- Note:
  - First restore command used `Copy-Item -LiteralPath` with a wildcard and had no effect.
  - Second restore command used `Copy-Item -Path ...\*.json` and restored the files.

## Official Batch Follow-Up Validation
- Scope:
  - Run the official paper batch after separated minimum quantity validation.
  - Preserve existing official ledger and audit evidence.
  - Classify official `BUY=0` with JSON/log/CSV evidence.
- Backup before official batch:
  - `E:\1_Data\backup\20260504_official_paper_batch_validation\20260504_131642`
- Backup before report/doc update:
  - `E:\1_Data\backup\20260504_official_paper_batch_report\20260504_133105`
- Command:
  - `E:\1_Data\run_paper_daily.bat`
  - exit code: `0`
- Evidence:
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\stats\official_batch_validation_report_MINQ_20260504_122214.json`
  - `E:\1_Data\2_Logs\run_paper_daily_last.txt`
  - `E:\1_Data\2_Logs\p1_entry_gate_status_latest.json`
  - `E:\1_Data\2_Logs\paper_order_validation_report_latest.json`
  - `E:\1_Data\2_Logs\recovery_ssot_chain_latest.json`
  - `E:\1_Data\2_Logs\ledger_live_fills_dry_run_latest.json`
- Result:
  - `D=20260430`
  - official new BUY count: `0`
  - classification: `운용 차단`
  - basis: forecast/future signal `PASS`, entry candidates existed, risk orchestration forced `max_new=0`, log showed `ENTRY_GATE BLOCK`, `ENTRY_CAP_ZERO`, and `FILL_SUMMARY attempted=0 filled=0`.
  - `orders_exec` exists and `exec_date_unique=[20260430]`.
  - recovery SSOT chain `PASS`.
  - ledger live fills dry-run `PASS`.
  - official RootA paper files and RootB ledger/stat files matched the pre-official-batch backup hashes.
- Remaining:
  - Official BUY order-fill-ledger chain with new BUY greater than zero remains untested because official operating controls blocked new BUY.
  - RootB dashboard latest remains `FAIL`.
  - Drift monitor latest remains `HIT`.
  - Backtest validation warning `rc=2` remains in the batch log.
- Validation matrix:
  - Functional validation: PASS.
  - Consistency validation: PASS.
  - Operational reflection validation: PASS.
  - Policy validation: PASS.
  - FAIL-CLOSED validation: PASS.
  - Regression validation: PARTIAL.

## Dashboard/Gate Revalidation
- Scope:
  - Verify whether prior Drift/dashboard fixes actually clear the entry gate blocker.
  - Patch only integrated ops snapshot generation where stale/false effective blockers were found.
- Backups:
  - `E:\1_Data\backup\20260504_dashboard_gate_revalidation\20260504_142930`
  - `E:\1_Data\backup\20260504_integrated_empty_queue_block_fix\20260504_143107`
  - `E:\1_Data\backup\20260504_entry_gate_rejudge_after_dashboard_fix\20260504_143219`
  - `E:\1_Data\backup\20260504_entry_gate_rejudge_final\20260504_143524`
- Changed file:
  - `E:\1_Data\tools\build_integrated_ops_snapshot.py`
- Changes:
  - Empty pending queue with `NO_PENDING_QUEUE` is display-only, not an effective blocker, when queue and `entry_ready` are zero.
  - Previous trading day calculation uses `holidays.json`; `20260501` holiday no longer makes `20260430` KRX clean stale.
- Evidence:
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\stats\dashboard_gate_revalidation_report_20260504.json`
  - `E:\1_Data\2_Logs\integrated_ops_snapshot_latest.json`
  - `E:\vibe\buffett\runs\dashboard_state_latest.json`
  - `E:\1_Data\2_Logs\p1_entry_gate_status_latest.json`
- Resolved:
  - `dashboard:FAIL` hard blocker removed.
  - `calc_issue:krx_clean:ISSUE` false stale blocker removed.
  - integrated effective blocker count is `0`.
  - dashboard status is `PASS`.
- Remaining:
  - New BUY remains `0`.
  - Remaining hard blocker is risk orchestration:
    - `risk_orch_size_zero:dd_stop,kelly_zero`
    - `risk_scale=0.0`
    - `dd_current=0.20678058086726792`
    - `dd_stop=0.15`
    - `f_kelly=0.0`
  - `news_collect PARTIAL/news_quota_guard_stop` remains CAUTION, not the hard block.
- Preservation:
  - Official `fills.csv`, `trades.csv`, `paper_state.json`, and `virtual_ledger.csv` hashes match the pre-engine backup.
- Validation matrix:
  - Functional validation: PASS.
  - Consistency validation: PASS.
  - Operational reflection validation: PASS.
  - Policy validation: PASS.
  - FAIL-CLOSED validation: PASS.
  - Regression validation: PARTIAL.

## Risk Orchestration Validation Reduce
- Scope:
  - Address the remaining hard blocker after dashboard/KRX false blockers were removed.
  - Keep production risk policy fail-closed by requiring explicit `dd_stop_validation` configuration.
  - No ledger reset and no data backfill.
- Backup:
  - `E:\1_Data\backup\20260504_risk_orch_validation_reduce\20260504_144138`
- Current blocker:
  - `risk_orch_size_zero:dd_stop,kelly_zero`
  - `risk_scale=0.0`
  - `dd_current=0.20678058086726792`
  - `dd_stop=0.15`
  - `f_kelly=0.0`
- Planned minimal change:
  - `paper_engine.py`: allow `dd_stop_validation` reduce only when all risk-orch zero-size reasons are explicitly listed in config `allow_reasons`.
  - `paper_engine_config.json`: set `risk_orchestration.dd_stop_validation.mode` to `validation_reduce` and add `allow_reasons=["dd_stop","kelly_zero"]`.
  - Keep `max_new=1` and `position_size_multiplier=0.1`.
- Verification:
  - Syntax validation for `paper_engine.py`.
  - JSON parse validation for `paper_engine_config.json`.
  - Run `paper_engine.py` once through official Python.
  - Verify `p1_entry_gate_status_latest.json` no longer blocks on `risk_orch_size_zero` when only configured reasons are present.
  - Verify new BUY/order/fill outcome from `paper_order_validation_report_latest.json` and `paper/fills.csv`.
  - Verify no unintended mutation to existing ledger outside the engine output path.

## Residual Issue Diagnosis
- Scope:
  - Read-only diagnosis for remaining dashboard `FAIL`, drift monitor `HIT`, and backtest warning after official batch.
  - No code, config, ledger, fills, orders, or stats repair applied.
- Backup before report/doc update:
  - `E:\1_Data\backup\20260504_residual_issue_diagnosis\20260504_140902`
- Evidence:
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\stats\residual_issue_diagnosis_20260504.json`
  - `E:\vibe\buffett\runs\dashboard_state_latest.json`
  - `E:\1_Data\2_Logs\integrated_ops_snapshot_latest.json`
  - `E:\1_Data\2_Logs\drift_monitor_latest.json`
  - `E:\1_Data\2_Logs\backtest_validation_checklist_latest.json`
- Findings:
  - Dashboard `FAIL` is an operating block derived state, not a pointer/order/stats contract failure.
  - Pending/new-entry state remains `max_new=0`, `entry_ready=0`, `filled=0`.
  - Drift monitor `HIT` is observer/report-only recon-diff warning with `missing_rate=0.85` and `amount_diff_rate=0.0`; recovery chain and order validation chain still pass.
  - Backtest warning is paper early-data validation hold: `operation_judgment=운영보류`, `pass_n=9`, `fail_n=1`, `not_evaluable_n=8`.
- Modification judgment:
  - Code change: `NO`.
  - Data repair: `NO`.
  - Production minimum quantity verification mode apply: `NO`.
  - Separate forensic ID-basis audit is required before any drift repair.
- Validation matrix:
  - Functional validation: NA.
  - Consistency validation: PASS.
  - Operational reflection validation: PASS.
  - Policy validation: PASS.
  - FAIL-CLOSED validation: PASS.
  - Regression validation: NA.

## Official Batch Completion

- Date/time: 2026-05-04 15:23 KST.
- Existing ledger reset: NO.
- Official batch command:
  - `E:\1_Data\run_paper_daily.bat`
- Final D:
  - `20260504`, derived from latest BUY in `E:\1_Data\paper\fills.csv`.
- BUY verified:
  - `PAPER_BUY_126730_20260504_R20260504`
  - code `126730`, qty `13`, price `22150.0`.
- Order/fill/ledger evidence:
  - `E:\1_Data\paper\orders_20260504_exec.xlsx`
  - `E:\1_Data\paper\fills.csv`
  - `E:\1_Data\virtual_ledger.csv`
  - `E:\vibe\buffett\data\ledger\paper_fills_ledger.csv`
  - `E:\vibe\buffett\data\stats\live_vs_bt.json`
- Latest validation evidence:
  - `E:\1_Data\2_Logs\recovery_ssot_chain_latest.json`: PASS, issues `0`.
  - `E:\1_Data\2_Logs\ledger_live_fills_dry_run_latest.json`: PASS, missing_rows `0`.
  - `E:\1_Data\2_Logs\execution_safety_contract_latest.json`: PASS, D `20260504`.
  - `E:\1_Data\2_Logs\paper_order_validation_report_latest.json`: lifecycle/chain/session PASS; top-level WARN from post-BUY `max_new=0`.
- Remaining post-BUY block:
  - Classification: operational block.
  - `pending_entry_status_latest.json`: `max_new=0`, `max_new_zero_reason=risk_off`, `filled=1`.
  - `gate_daily_20260504.json`: `prices_date_max(20260430) < D(20260504)`.
- Read-only residual:
  - `future_signal_daily_status_latest.json` remains FAIL because forecast validation macro freshness has unknown required inputs.
  - The read-only future signal failure no longer stops the official paper batch when generation succeeded and validation/calibration are in allowed WAITING states.
- Integrity residual:
  - `nightly_data_integrity_latest.json` reports WARN for approved config/lock hash changes.
- Validation matrix:
  - Functional validation: PASS.
  - Consistency validation: PASS.
  - Operational reflection validation: PASS.
  - Policy validation: PASS.
  - FAIL-CLOSED validation: PASS.
  - Regression validation: PARTIAL.

## Drift Monitor Recon Patch
- Scope:
  - Patch only `E:\1_Data\tools\build_drift_monitor.py`.
  - No orders, fills, ledger, RootB stats, Gate, STOP, D rule, or trading policy changed.
- Backup before patch:
  - `E:\1_Data\backup\20260504_drift_monitor_recon_patch\20260504_141924`
- Changed file:
  - `E:\1_Data\tools\build_drift_monitor.py`
- Change:
  - Added `_business_key_set`.
  - `_build_recon` now uses D-day `code|side` business-key missing-rate plus amount difference for HIT decision.
  - Raw ID missing-rate and raw missing ID lists remain in diagnostics.
- Evidence:
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\stats\drift_monitor_recon_patch_report_20260504.json`
  - `E:\1_Data\2_Logs\drift_monitor_latest.json`
  - `E:\1_Data\runs\SSOT_D20260430_DRIFT_20260504_142018`
- Validation:
  - Syntax: `python -m py_compile E:\1_Data\tools\build_drift_monitor.py` passed.
  - Execution: `python E:\1_Data\tools\build_drift_monitor.py --date 20260430` exited `0`.
  - `recon_diff.status=PASS`.
  - `recon_diff.recommended_action=NONE`.
  - `missing_rate=0.0`.
  - `raw_id_missing_rate=0.85`.
  - `amount_diff_rate=0.0`.
  - business key counts: orders `20`, fills `20`, ledger `20`.
  - orders/fills/ledger hashes match the pre-patch backup.
- Remaining:
  - Drift overall remains `NOT_EVALUABLE` because `slippage_by_venue` is `NOT_EVALUABLE`.
  - Dashboard `FAIL` and backtest warning were not addressed.
  - Full official daily batch was not rerun after this patch.
- Validation matrix:
  - Functional validation: PASS.
  - Consistency validation: PASS.
  - Operational reflection validation: PASS.
  - Policy validation: PASS.
  - FAIL-CLOSED validation: PASS.
  - Regression validation: PARTIAL.

## P0 Intraday Cover and Current BUY 0 Classification
- Scope:
  - Preserve existing paper ledger and classify the current additional BUY 0 state from runtime evidence.
  - Do not apply minimum-quantity verification mode in this step.
- Backup before documentation update:
  - `E:\1_Data\backup\20260504_p0_intraday_entry_block_report\20260504_155000`
- Evidence:
  - `E:\1_Data\2_Logs\p0_daily_check_20260504_154043.json`
  - `E:\1_Data\2_Logs\gate_daily_20260504.json`
  - `E:\1_Data\2_Logs\paper_order_validation_report_latest.json`
  - `E:\1_Data\2_Logs\pending_entry_status_latest.json`
  - `E:\1_Data\2_Logs\recovery_ssot_chain_latest.json`
  - `E:\1_Data\2_Logs\ledger_live_fills_dry_run_latest.json`
- Findings:
  - Previous `prices_date_max(20260430) < D(20260504)` was an excessive data judgment block because `intraday_prices_latest.csv` covers D with `date_max=20260504`.
  - Current P0 has `risk_off.enabled=false` and no risk-off reasons.
  - Current gate action is `ALLOW` with no hard blocks.
  - Paper engine runtime reported close-cutoff blocking after `15:20`.
  - Order validation reports `MAX_NEW_REACHED`, `NO_ENTRY_READY`, and `DECISION_CAP_SIGNALDATE_TOP3_BY_SCORE`.
  - Pending status reports `max_new=1`, `filled=1`, `entry_ready=0`, and `CAP_SIGNALDATE_TOP3_BY_SCORE=1`.
- Classification:
  - Previous price/D block: excessive block, fixed in P0/gate path.
  - Current additional BUY 0: normal block, not data block and not operations block.
- Minimum-quantity verification mode judgment:
  - Not needed now.
  - Reason: order-fill-ledger chain already has one BUY on D `20260504`, and current zero additional BUY is explained by normal close-cutoff/cap guards.
- Validation matrix:
  - Functional validation: PASS.
  - Consistency validation: PASS.
  - Operational reflection validation: PASS.
  - Policy validation: PASS.
  - FAIL-CLOSED validation: PASS.
  - Regression validation: PARTIAL.

## Drift ID Forensic Audit
- Scope:
  - Read-only audit of drift monitor `recon_diff` HIT.
  - No code, config, ledger, fills, orders, or stats repair applied.
- Backup before report/doc update:
  - `E:\1_Data\backup\20260504_drift_id_forensic_audit\20260504_141658`
- Evidence:
  - `E:\1_Data\paper\sessions\MINQ_20260504_122214\stats\drift_id_forensic_audit_20260504.json`
  - `E:\1_Data\2_Logs\drift_monitor_latest.json`
  - `E:\1_Data\tools\build_drift_monitor.py`
  - `E:\1_Data\paper\orders_20260430_exec.xlsx`
  - `E:\1_Data\paper\fills.csv`
  - `E:\vibe\buffett\data\ledger\paper_fills_ledger.csv`
- Findings:
  - Current drift monitor compares raw ID sets from `order_id`, `entry_order_id`, and `source_order_id`.
  - `orders_exec` IDs are `ORDER_*`.
  - `fills.csv` IDs are `PAPER_*`.
  - RootB ledger IDs are mixed `ORDER_*` and `PAPER_*`.
  - D-day `code+side` pairs match across orders/fills/ledger: `20`.
  - D-day orders missing in fills by `code+side`: `0`.
  - D-day orders missing in ledger by `code+side`: `0`.
  - Drift reported `amount_diff_rate=0.0`.
- Classification:
  - `drift_monitor_id_schema_mismatch_false_positive_risk`.
- Modification judgment:
  - Ledger/fills repair: `NO`.
  - Drift monitor logic patch: `YES`, before trusting `recon_diff`.
  - Apply now: `NO`.
- Proposed patch verification plan:
  - Backup `tools\build_drift_monitor.py` and latest drift JSON/report docs.
  - Patch `_build_recon` only.
  - Run syntax validation.
  - Run drift monitor generation through the existing script path.
  - Verify `recon_diff` changes only from canonical business-key and amount checks.
  - Verify no mutation to orders, fills, ledger, or stats.
- Validation matrix:
  - Functional validation: NA.
  - Consistency validation: PASS.
  - Operational reflection validation: PASS.
  - Policy validation: PASS.
  - FAIL-CLOSED validation: PASS.
  - Regression validation: NA.
