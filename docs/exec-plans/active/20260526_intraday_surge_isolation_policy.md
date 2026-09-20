# 2026-05-26 Intraday / Surge Isolation Policy Plan

## Status
- Status: PLAN ONLY
- trading_effect: false
- policy_effect: false
- policy_change_applied: false
- Root: `E:\1_Data`

## Scope
- Define a structure-change plan for separating normal-entry policy evaluation from `intraday_realtime` and `surge_immediate` paths.
- Do not change config, Gate, LOCK, DDM, risk thresholds, scoring, orders, fills, ledger, broker dispatch, or runtime entry behavior in this plan.
- Do not use shadow, observe, probe, validation-reduce, or fallback what-if rows as direct proof for live policy promotion.

## Current Evidence
- Latest decision layer runtime showed no current entry sample to promote:
  - `E:\1_Data\2_Logs\entry_decision_layers_runtime_latest.json`
  - `candidate_rows=0`
  - `decision_rows=0`
  - `snapshot_rows=0`
  - `policy_change=false`
- Current risk state remains defensive:
  - `E:\1_Data\2_Logs\risk_orchestration_latest.json`
  - `dd_stop_triggered=true`
  - `position_size_multiplier=0.0`
- Current production risk playbook remains blocked:
  - `E:\1_Data\2_Logs\production_risk_playbook_latest.json`
  - `action=HARD`
  - `blocked=true`
- Existing normalized label report separates historical rows by policy-like path:
  - `E:\1_Data\2_Logs\normal_entry_baseline_label_report_latest.json`
  - `ISOLATE_SURGE_IMMEDIATE`: 150 rows, average return about -2.20%, win rate about 24.7%
  - `SEPARATE_INTRADAY_NON_SURGE`: 40 rows, average return about -3.08%, win rate about 17.5%
  - `INCLUDE_NORMAL_BASELINE`: 29 rows, average return about -0.44%, win rate about 37.9%
- Current fallback what-if does not support stage promotion:
  - `E:\1_Data\2_Logs\entry_fallback_stage_whatif_latest.json`
  - `current_policy_stage1_allowed=0`
  - `current_policy_stage2_allowed=0`
  - `counts={"NOT_STAGE_FALLBACK_CANDIDATE":16}`

## Interpretation
- The current weak point is not that normal fallback stages are obviously too strict.
- The stronger evidence is that `intraday_realtime` and `surge_immediate` rows should not be mixed into the normal-entry baseline.
- Therefore the first structure change should be separation of policy groups, not fallback expansion.

## Proposed Structure
1. Normal entry baseline
   - Include only normal decision paths such as same-day close, close auction, and explicitly normal next-open candidates.
   - Exclude `intraday_realtime`, `surge_immediate`, validation-reduce, probe, shadow, observe, legacy replay, and unclear rows.
   - Use this group only to judge normal alpha and normal execution policy.

2. Intraday realtime group
   - Treat as a separate policy group with separate metrics and separate promotion rules.
   - Do not allow this group to improve or justify normal-entry policy values.
   - Promotion requires fresh runtime samples with normalized policy labels.

3. Surge immediate group
   - Treat as a high-risk separate policy group.
   - Default structure should remain hold/block/shadow unless explicit future approval changes it.
   - Any promotion requires independent evidence for gap risk, overheating, follow-through, and post-entry drawdown.

4. Fallback stage policy
   - Keep current fallback policy unchanged until fresh what-if rows show real missed normal-entry opportunities.
   - Do not use old stage 1/2 returns as direct current-policy proof because current config is `max_stage=0` and `strict_same_day_only=true`.

## Non-Goals
- No relaxation of DDM, risk-off, kill-switch, P1 entry gate, or hard block behavior.
- No order, fill, ledger, broker, or stats chain changes.
- No score/status hardcoding.
- No policy value change from historical mixed rows alone.
- No conversion of shadow/observe/probe output into live permission.

## Future Implementation Gate
Before any operating behavior changes, create or confirm normalized runtime fields for:
- `entry_policy_group`
- `entry_policy_path`
- `entry_decision_source`
- `risk_mode`
- `sample_eligibility`
- `promotion_candidate=false` by default

Then run at least these checks:
1. Normal baseline metrics excluding intraday and surge rows.
2. Intraday realtime metrics in isolation.
3. Surge immediate metrics in isolation.
4. Fresh fallback stage what-if rows with clean opportunity labels.
5. Runtime D consistency against fills-derived D.
6. FAIL-CLOSED behavior preserved when risk state is HARD, risk_off, dd_stop, or kill_switch.

## Required Validation For Any Later Code Change
1. Functional validation: required
2. Consistency validation: required
3. Operational reflection validation: required
4. Policy validation: required
5. FAIL-CLOSED validation: required
6. Regression validation: required

## Current Decision
- Apply no policy change now.
- Use this plan to prevent mixed historical rows from setting normal-entry policy values.
- Next implementation, if explicitly approved, should be read-only normalized runtime labeling first.

## Step 3 Final Structure Decision
- Decision date: 2026-05-26
- Decision status: STRUCTURE DECIDED, NOT APPLIED
- Operating policy change: none
- Trading effect: false

The structure change direction is confirmed as policy-group separation before any entry expansion.

### Final Policy Groups
1. `normal_entry`
   - Purpose: judge normal alpha and normal execution policy.
   - Eligible evidence: clean normal same-day close, close auction, and explicitly normal next-open style rows.
   - Excluded evidence: intraday realtime, surge immediate, validation reduce, paper probe, shadow/observe, split follow-up, legacy replay, and unknown metadata.

2. `intraday_realtime`
   - Purpose: judge realtime intraday entry behavior only.
   - Eligible evidence: rows explicitly marked as intraday realtime.
   - Not allowed to justify normal-entry thresholds.

3. `surge_immediate`
   - Purpose: judge high-risk surge entry behavior only.
   - Eligible evidence: rows explicitly marked as surge immediate or surge type.
   - Default future action should remain separate hold/block/shadow unless separately approved.

4. `validation_reduce`
   - Purpose: collect limited validation evidence when defensive gates reduce size.
   - Not policy proof for normal entry, surge entry, or intraday entry.
   - May be analyzed only as validation-mode behavior.

5. `shadow_probe_observe`
   - Purpose: observe missed opportunities or test diagnostics.
   - Not trading-performance proof.
   - Not eligible for direct policy promotion.

6. `legacy_unknown`
   - Purpose: preserve historical auditability.
   - Excluded from policy-value decisions unless later normalized with verifiable metadata.

### Final Ordering
1. Add read-only normalized labels to runtime/report artifacts.
2. Regenerate group-separated summaries.
3. Re-evaluate normal-entry policy using only `normal_entry`.
4. Re-evaluate intraday and surge policies separately.
5. Only after the above, propose any policy-value change.

### Rejected Paths For Now
- Do not expand fallback stages from the current mixed sample.
- Do not treat `dd_stop_validation` rows as normal-entry success/failure proof.
- Do not relax DDM, risk orchestration, production risk, P1 gate, or kill switch to force samples.
- Do not use shadow/probe rows as direct promotion evidence.

## Step 4 Read-Only Label Implementation
- Implementation date: 2026-05-26
- Implementation status: READ-ONLY ARTIFACT ADDED
- Operating policy change: none
- Trading effect: false

Added a read-only report builder:
- `E:\1_Data\tools\build_entry_policy_group_report.py`

Outputs:
- `E:\1_Data\2_Logs\entry_policy_group_report_latest.json`
- `E:\1_Data\2_Logs\entry_policy_group_report_latest.csv`
- `E:\1_Data\2_Logs\entry_policy_group_report_summary_latest.csv`

The report labels existing rows into:
- `normal_entry`
- `intraday_realtime`
- `surge_immediate`
- `validation_reduce`
- `shadow_probe_observe`
- `legacy_unknown`

The report explicitly sets:
- `trading_effect=false`
- `policy_effect=false`
- `policy_change_applied=false`

## Step 5 Policy Change Candidate Review
- Review date: 2026-05-26
- Review status: NO LIVE POLICY VALUE CHANGE
- Operating policy change: none
- Trading effect: false

Added a read-only candidate review builder:
- `E:\1_Data\tools\build_entry_policy_change_candidate_review.py`

Outputs:
- `E:\1_Data\2_Logs\entry_policy_change_candidate_review_latest.json`
- `E:\1_Data\2_Logs\entry_policy_change_candidate_review_latest.csv`

Current decisions:
- Hold normal-entry policy value changes because clean normal evidence is small and weak.
- Reject fallback stage expansion now because current what-if has zero stage 1/2 allowed rows.
- Reject intraday realtime promotion now.
- Reject surge immediate promotion now.
- Do not use validation-reduce rows as normal-entry policy proof.
- Approve only the next read-only implementation step: runtime normalized policy-group fields.

## Normal Entry Path Bottleneck Audit
- Audit date: 2026-05-26
- Audit status: READ-ONLY AUDIT
- Operating policy change: none
- Trading effect: false

Added a read-only audit builder:
- `E:\1_Data\tools\build_normal_entry_path_bottleneck_audit.py`

Outputs:
- `E:\1_Data\2_Logs\normal_entry_path_bottleneck_audit_latest.json`
- `E:\1_Data\2_Logs\normal_entry_path_bottleneck_audit_latest.csv`

Scope:
- Candidate source rows
- P1 entry gate pre-entry state
- Entry decision layer
- Pending entry state
- Risk orchestration
- Production risk playbook

Rule:
- No alternative path.
- No policy relaxation.
- No sample-forcing route.

## Entry Layer Input Trace Audit
- Audit date: 2026-05-26
- Audit status: READ-ONLY AUDIT
- Operating policy change: none
- Trading effect: false

Added a read-only audit builder:
- `E:\1_Data\tools\build_entry_layer_input_trace_audit.py`

Outputs:
- `E:\1_Data\2_Logs\entry_layer_input_trace_audit_latest.json`
- `E:\1_Data\2_Logs\entry_layer_input_trace_audit_latest.csv`

Purpose:
- Separate same-cycle candidate-to-entry-layer loss from artifact freshness mismatch.
- Record current candidate row exclusion reasons without creating a new entry path.

## Same-Cycle Entry Layer Verification
- Verification date: 2026-05-26
- Verification status: PASS
- Operating policy change: none
- Trading effect: false

Execution:
- Ran `paper_engine.py` once after backing up operational chain files.
- Set `PAPER_DISABLE_LIVE_BRIDGE_SYNC=1`.
- No policy value, Gate, risk, DDM, P1, or kill-switch setting was changed.

Result:
- Same-cycle comparability restored:
  - `same_cycle_comparable=true`
  - `candidate_newer_than_entry_layer=false`
- Entry decision layer:
  - `candidate_rows=0`
  - `decision_rows=0`
  - `snapshot_rows=0`
  - `policy_change=false`
- Current candidate trace:
  - `candidate_rows_now=16`
  - `current_candidate_viable_rows=0`
  - `execution_pool_false=16`
  - `sector_entry_not_allowed=10`
  - `already_bought_on_D=4`
  - `already_open=2`
  - `candidate_date_missing=3`
- Engine trace showed the normal pool reached 2 rows before final selection, then same-day already-bought filtering reduced selected rows to 0.
- New BUY/fill count remained 0.

Preservation:
- `paper/fills.csv` unchanged versus backup.
- `paper/trades.csv` unchanged by hash versus backup.
- `paper/paper_state.json` changed during the engine run and was restored from backup.
- Restore verification confirmed `paper_state.json` hash matches the pre-run backup.

## Final Candidate Elimination Audit
- Audit date: 2026-05-26
- Audit status: READ-ONLY AUDIT
- Operating policy change: none
- Trading effect: false

Added a read-only audit builder:
- `E:\1_Data\tools\build_final_candidate_elimination_audit.py`

Outputs:
- `E:\1_Data\2_Logs\final_candidate_elimination_audit_latest.json`
- `E:\1_Data\2_Logs\final_candidate_elimination_audit_latest.csv`

Purpose:
- Verify the final two normal-entry candidates that reached selection.
- Confirm whether they were already bought on D.
- Confirm no alternative entry path or policy relaxation is needed for this finding.

## Sector Union Entry Path Audit
- Audit date: 2026-05-26
- Audit status: READ-ONLY AUDIT
- Operating policy change: none
- Trading effect: false

Added a read-only audit builder:
- `E:\1_Data\tools\build_sector_union_entry_path_audit.py`

Outputs:
- `E:\1_Data\2_Logs\sector_union_entry_path_audit_latest.json`
- `E:\1_Data\2_Logs\sector_union_entry_path_audit_latest.csv`

Purpose:
- Verify why `raw_execution_pool=false` candidates can enter the entry pool.
- Confirm this is the existing `SECTOR_PREFILTER_UNION` conditional inclusion path, not a new route.

Result:
- `status=PASS`
- `sector_union_rows=13`
- `status_counts={"UNION_INCLUDED_DESPITE_EXECUTION_POOL_FALSE":4,"UNION_NOT_ELIGIBLE":9}`
- Config readback confirmed `allow_sector_union=true`, `require_execution_pool_when_present=true`, `require_sector_entry_when_present=true`, and `union_entry_strength_min=0.63`.
- Finalist codes were `001740` and `382800`.
- Finalist elimination status was `PASS`.
- Decision remains `NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION`.
- `trading_effect=false`, `policy_effect=false`, `policy_change_applied=false`.

## Sector Union Sample Separation Audit
- Audit date: 2026-05-26
- Audit status: READ-ONLY AUDIT
- Operating policy change: none
- Trading effect: false

Added a read-only audit builder:
- `E:\1_Data\tools\build_sector_union_sample_separation_audit.py`

Outputs:
- `E:\1_Data\2_Logs\sector_union_sample_separation_audit_latest.json`
- `E:\1_Data\2_Logs\sector_union_sample_separation_audit_latest.csv`

Purpose:
- Decide whether `SECTOR_PREFILTER_UNION` rows can be mixed into `normal_entry` policy proof.
- Keep the decision separate from policy relaxation or alternative route creation.

Result:
- `status=PASS`
- `policy_group_rows=687`
- `normal_entry_rows_in_policy_group_report=60`
- `normal_entry_return_rows_in_policy_group_report=29`
- `sector_union_rows=13`
- `sector_union_return_rows=0`
- `decision_counts={"NOT_ENTRY_SAMPLE":9,"SEPARATE_SECTOR_UNION_SAMPLE":4}`
- `mix_with_normal_entry_proof_counts={"false":13}`
- `sample_policy_decision=SEPARATE_SECTOR_UNION_FROM_NORMAL_ENTRY_PROOF`
- `normal_entry_mixing_allowed=false`
- `decision=NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION`
- `trading_effect=false`, `policy_effect=false`, `policy_change_applied=false`

## Pure Normal Entry Sample Audit
- Audit date: 2026-05-26
- Audit status: READ-ONLY AUDIT
- Operating policy change: none
- Trading effect: false

Added a read-only audit builder:
- `E:\1_Data\tools\build_pure_normal_entry_sample_audit.py`

Outputs:
- `E:\1_Data\2_Logs\pure_normal_entry_sample_audit_latest.json`
- `E:\1_Data\2_Logs\pure_normal_entry_sample_audit_latest.csv`
- `E:\1_Data\2_Logs\pure_normal_entry_sample_audit_summary_latest.csv`

Purpose:
- Separate pure current-policy normal-entry proof from historical fallback and non-realized normal rows.
- Use current config readback, especially `strict_same_day_only` and `entry_fallback_policy.max_stage`, instead of assuming all `normal_entry` rows are policy proof.

Result:
- `status=PASS`
- Config readback confirmed `market_ops_policy.strict_same_day_only=true` and `market_ops_policy.entry_fallback_policy.max_stage=0`.
- `policy_group_rows=687`
- `normal_entry_rows=60`
- `realized_normal_entry_rows=29`
- `pure_current_normal_proof_rows=17`
- `historical_fallback_separate_rows=12`
- `decision_counts={"HISTORICAL_FALLBACK_SEPARATE":12,"NOT_NORMAL_ENTRY":627,"NOT_REALIZED_RETURN_PROOF":31,"PURE_CURRENT_NORMAL_PROOF":17}`
- `realized_normal_by_fallback_stage={"":14,"0(close_auction)":3,"1(next_open_limit)":10,"2(intraday_limit)":2}`
- `pure_current_by_fallback_stage={"":14,"0(close_auction)":3}`
- `pure_current_stats.n=17`, `avg=-0.021252943335994635`, `median=-0.02080821917808222`, `win_rate=0.23529411764705882`
- `sample_policy_decision=USE_ONLY_PURE_CURRENT_NORMAL_PROOF_FOR_CURRENT_POLICY_VALUE_REVIEW`
- `decision=NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION`
- `trading_effect=false`, `policy_effect=false`, `policy_change_applied=false`

## Pure Normal Loss Driver Audit
- Audit date: 2026-05-26
- Audit status: READ-ONLY AUDIT
- Operating policy change: none
- Trading effect: false

Added a read-only audit builder:
- `E:\1_Data\tools\build_pure_normal_loss_driver_audit.py`

Outputs:
- `E:\1_Data\2_Logs\pure_normal_loss_driver_audit_latest.json`
- `E:\1_Data\2_Logs\pure_normal_loss_driver_audit_latest.csv`
- `E:\1_Data\2_Logs\pure_normal_loss_driver_audit_summary_latest.csv`

Purpose:
- Decompose the 17 pure current-policy normal-entry proof rows by date cluster, loss bucket, gross/net return, holding days, and cost drag.
- Keep this as evidence classification only; no operating policy values are changed.

Result:
- `status=PASS`
- `pure_current_normal_rows=17`
- Net stats: `n=17`, `avg=-0.021252943335994635`, `median=-0.02080821917808222`, `win_rate=0.23529411764705882`
- Gross stats: `n=17`, `avg=-0.007296723678063013`, `median=-0.006849315068493178`, `win_rate=0.35294117647058826`
- `loss_bucket_counts={"COST_FLIP_LOSS":2,"FLAT_AFTER_COST":1,"LARGE_PRICE_LOSS":3,"SMALL_PRICE_LOSS":7,"WIN":4}`
- `date_cluster_counts={"APR01_02_CLUSTER":13,"APR15_16_CLUSTER":3,"OTHER_DATE":1}`
- `entry_day_counts={"20260331":1,"20260401":4,"20260402":9,"20260415":1,"20260416":2}`
- `cost_flip_rows=2`
- `price_loss_rows=10`
- `apr01_02_cluster_rows=13`
- APR01/02 cluster net stats: `n=13`, `avg=-0.026948003395202744`, `median=-0.022719298245614063`, `win_rate=0.15384615384615385`
- `primary_loss_driver=DATE_CLUSTER_AND_PRICE_LOSS`
- `decision=NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION`
- `trading_effect=false`, `policy_effect=false`, `policy_change_applied=false`

## Cluster Candidate Quality Audit
- Audit date: 2026-05-26
- Audit status: READ-ONLY AUDIT
- Operating policy change: none
- Trading effect: false

Added a read-only audit builder:
- `E:\1_Data\tools\build_cluster_candidate_quality_audit.py`

Outputs:
- `E:\1_Data\2_Logs\cluster_candidate_quality_audit_latest.json`
- `E:\1_Data\2_Logs\cluster_candidate_quality_audit_latest.csv`
- `E:\1_Data\2_Logs\cluster_candidate_quality_audit_summary_latest.csv`

Purpose:
- Match the 2026-04-01 to 2026-04-02 pure-normal loss cluster against same-day candidate backup files.
- Separate score/quality weakness from runtime behavior changes.

Result:
- `status=PASS`
- Candidate backups used:
  - `candidates_latest_data.bak_20260401_165747.csv`
  - `candidates_latest_data.bak_20260402_175706.csv`
- `apr01_02_cluster_rows=13`
- `candidate_matched_rows=13`
- `quality_bucket_counts={"L3_RELAXED":1,"L3_WITH_RISK_FLAGS":8,"RISK_FLAGGED_NATURAL_PASS":1,"SECTOR_UNION_NOT_EXECUTION_POOL":3}`
- `candidate_origin_counts={"BALANCED_RESCUE":5,"NATURAL_PASS":5,"SECTOR_PREFILTER_UNION":3}`
- `relax_level_counts={"L0":4,"L3":9}`
- `execution_pool_counts={"false":3,"true":10}`
- `natural_pass_counts={"false":8,"true":5}`
- `junk_flagged_rows=11`
- `sector_union_not_execution_pool_rows=3`
- `l3_with_risk_flags_rows=8`
- Net stats: `n=13`, `avg=-0.026948003395202744`, `median=-0.022719298245614063`, `win_rate=0.15384615384615385`
- `primary_quality_driver=MIXED_WEAK_QUALITY_CLUSTER`
- `decision=NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION`
- `trading_effect=false`, `policy_effect=false`, `policy_change_applied=false`

## Strict Normal Policy Proof Audit
- Audit date: 2026-05-26
- Audit status: READ-ONLY AUDIT
- Operating policy change: none
- Trading effect: false

Added a read-only audit builder:
- `E:\1_Data\tools\build_strict_normal_policy_proof_audit.py`

Outputs:
- `E:\1_Data\2_Logs\strict_normal_policy_proof_audit_latest.json`
- `E:\1_Data\2_Logs\strict_normal_policy_proof_audit_latest.csv`
- `E:\1_Data\2_Logs\strict_normal_policy_proof_audit_summary_latest.csv`

Purpose:
- Exclude `L3_WITH_RISK_FLAGS` and `SECTOR_UNION_NOT_EXECUTION_POOL` rows from pure current normal-policy proof.
- Determine the remaining strict normal-policy proof sample size and performance without changing live behavior.

Result:
- `status=PASS`
- `pure_current_normal_rows=17`
- `excluded_weak_quality_rows=11`
- `strict_policy_proof_rows=6`
- `decision_counts={"EXCLUDE_FROM_STRICT_NORMAL_POLICY_PROOF":11,"KEEP_BUT_MARK_RELAXED":1,"KEEP_BUT_MARK_RISK_FLAGGED":1,"KEEP_STRICT_NORMAL_POLICY_PROOF":4}`
- `exclusion_counts={"L3_WITH_RISK_FLAGS":8,"SECTOR_UNION_NOT_EXECUTION_POOL":3}`
- `excluded_quality_buckets=["L3_WITH_RISK_FLAGS","SECTOR_UNION_NOT_EXECUTION_POOL"]`
- Strict proof stats: `n=6`, `avg=-0.008885945993432124`, `median=-0.0014530154277699594`, `win_rate=0.5`
- Pure current normal stats: `n=17`, `avg=-0.021252943335994635`, `median=-0.02080821917808222`, `win_rate=0.23529411764705882`
- `sample_policy_decision=STRICT_NORMAL_POLICY_PROOF_TOO_SMALL_FOR_POLICY_VALUE_CHANGE`
- `decision=NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION`
- `trading_effect=false`, `policy_effect=false`, `policy_change_applied=false`

## Runtime Normal Quality Separation Report
- Audit date: 2026-05-26
- Audit status: READ-ONLY REPORT
- Operating policy change: none
- Trading effect: false

Added a read-only report builder:
- `E:\1_Data\tools\build_runtime_normal_quality_separation_report.py`

Outputs:
- `E:\1_Data\2_Logs\runtime_normal_quality_separation_report_latest.json`
- `E:\1_Data\2_Logs\runtime_normal_quality_separation_report_latest.csv`

Purpose:
- Label current runtime candidates with weak-quality separation groups.
- Keep `L3_WITH_RISK_FLAGS` and `SECTOR_UNION_NOT_EXECUTION_POOL` visible without changing live entry behavior.

Result:
- `status=PASS`
- `candidate_rows=16`
- `entry_layer_rows=0`
- `entry_layer_status=EMPTY`
- `runtime_quality_label_counts={"NON_NATURAL_PASS":3,"SECTOR_UNION_NOT_EXECUTION_POOL":13}`
- `strict_normal_proof_usable_counts={"false":13,"true":3}`
- Strict proof reference:
  - `strict_policy_proof_rows=6`
  - `excluded_weak_quality_rows=11`
  - `sample_policy_decision=STRICT_NORMAL_POLICY_PROOF_TOO_SMALL_FOR_POLICY_VALUE_CHANGE`
- `sample_policy_decision=RUNTIME_SEPARATION_LABELS_ONLY_NO_POLICY_VALUE_CHANGE`
- `decision=NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION`
- `trading_effect=false`, `policy_effect=false`, `policy_change_applied=false`

## Policy Group Quality Link Report
- Audit date: 2026-05-26
- Audit status: READ-ONLY REPORT
- Operating policy change: none
- Trading effect: false

Added a read-only report builder:
- `E:\1_Data\tools\build_policy_group_quality_link_report.py`

Outputs:
- `E:\1_Data\2_Logs\policy_group_quality_link_report_latest.json`
- `E:\1_Data\2_Logs\policy_group_quality_link_report_latest.csv`

Purpose:
- Link `entry_policy_group_report` with runtime quality separation labels.
- Clarify that `entry_policy_group=normal_entry` alone is not strict policy-value proof.

Result:
- `status=PASS`
- `entry_policy_group_counts={"intraday_realtime":63,"legacy_unknown":303,"normal_entry":60,"surge_immediate":201,"validation_reduce":60}`
- `normal_entry_rows=60`
- `normal_entry_realized_rows=29`
- `strict_policy_proof_rows=6`
- `runtime_candidate_rows=16`
- `runtime_quality_label_counts={"NON_NATURAL_PASS":3,"SECTOR_UNION_NOT_EXECUTION_POOL":13}`
- `runtime_quality_bucket_counts={"runtime_marked_not_clean":3,"runtime_not_strict_proof":13}`
- Reporting rules:
  - Do not use `entry_policy_group=normal_entry` alone as policy-value proof.
  - Use `strict_normal_proof_usable=false` to exclude weak-quality runtime rows from normal-entry policy statistics.
  - Keep `SECTOR_UNION_NOT_EXECUTION_POOL` outside strict normal-entry proof.
  - Keep `L3_WITH_RISK_FLAGS` outside strict normal-entry proof.
  - Treat remaining strict proof as review evidence only when sample size is small.
- `sample_policy_decision=LINK_POLICY_GROUP_TO_QUALITY_LABELS_NO_POLICY_VALUE_CHANGE`
- `decision=NO_ALTERNATIVE_PATH_NO_POLICY_RELAXATION`
- `trading_effect=false`, `policy_effect=false`, `policy_change_applied=false`

## Quality Reports Autorun Audit
- Audit date: 2026-05-26
- Audit status: READ-ONLY AUDIT
- Operating policy change: none
- Trading effect: false

Added a read-only audit builder:
- `E:\1_Data\tools\build_quality_reports_autorun_audit.py`

Outputs:
- `E:\1_Data\2_Logs\quality_reports_autorun_audit_latest.json`
- `E:\1_Data\2_Logs\quality_reports_autorun_audit_latest.csv`

Purpose:
- Check whether the quality-separation reports are already included in official autorun entrypoints.
- Identify the correct fail-soft read-only insertion point without editing the batch.

Result:
- `status=PASS`
- Checked tools:
  - `tools\build_runtime_normal_quality_separation_report.py`
  - `tools\build_policy_group_quality_link_report.py`
  - `tools\build_strict_normal_policy_proof_audit.py`
  - `tools\build_pure_normal_entry_sample_audit.py`
  - `tools\build_cluster_candidate_quality_audit.py`
- `included_any_count=0`
- `missing_from_run_paper_daily=5`
- Recommended entrypoint: `run_paper_daily.bat`
- Recommended position: after `[7.08a/9] shadow_promotion_report` and before optional `shadow_collect`
- `recommendation=NOT_AUTORUN_YET_ADD_ONLY_WITH_EXPLICIT_APPROVAL`
- `decision=NO_AUTORUN_CHANGE_APPLIED`
- `trading_effect=false`, `policy_effect=false`, `policy_change_applied=false`

## Quality Reports Autorun Apply
- Apply date: 2026-05-26
- Apply status: BATCH UPDATED, PARTIAL RUNTIME VERIFIED
- Operating policy change: none
- Trading effect: false

Updated:
- `E:\1_Data\run_paper_daily.bat`

Inserted fail-soft read-only calls after `[7.08a/9] shadow_promotion_report` and before `[7.09/9] candidate_recheck_queue`:
- `tools\build_entry_policy_group_report.py`
- `tools\build_pure_normal_entry_sample_audit.py`
- `tools\build_pure_normal_loss_driver_audit.py`
- `tools\build_cluster_candidate_quality_audit.py`
- `tools\build_strict_normal_policy_proof_audit.py`
- `tools\build_runtime_normal_quality_separation_report.py`
- `tools\build_policy_group_quality_link_report.py`

Result:
- `run_paper_daily.bat` insertion verified at `[7.08b/9]` through `[7.08h/9]`.
- Added chain:
  - `[7.08b/9] tools\build_entry_policy_group_report.py`
  - `[7.08c/9] tools\build_pure_normal_entry_sample_audit.py`
  - `[7.08d/9] tools\build_pure_normal_loss_driver_audit.py`
  - `[7.08e/9] tools\build_cluster_candidate_quality_audit.py`
  - `[7.08f/9] tools\build_strict_normal_policy_proof_audit.py`
  - `[7.08g/9] tools\build_runtime_normal_quality_separation_report.py`
  - `[7.08h/9] tools\build_policy_group_quality_link_report.py`
- Standalone execution PASS for all seven report builders.
- Autorun audit updated and rerun:
  - `status=PASS`
  - `included_any_count=7`
  - `missing_from_run_paper_daily=[]`
  - `recommendation=AUTORUN_INCLUDED_READ_ONLY_FAIL_SOFT`
  - `decision=AUTORUN_INCLUDED_READ_ONLY`
- Full `run_paper_daily.bat` execution was not run in this step.
- `trading_effect=false`, `policy_effect=false`, `policy_change_applied=false`
