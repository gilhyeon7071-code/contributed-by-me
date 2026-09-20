# Incident {{INCIDENT_ID}} - {{short_title}}

- Time (KST): {{timestamp_kst}}
- Component: {{component_name}}
- Environment: {{paper|broker|dashboard|batch}}
- Root: E:\1_Data
- Related D: {{D_or_unknown}}
- Deploy tag / commit: {{commit_or_unknown}}

## Summary
{{one_line_observed_failure_and_impact}}

## Facts
- Trigger: {{ci_failure|batch_failure|manual_detection|dashboard_alert}}
- Failed command: `{{command}}`
- Exit code: {{exit_code}}
- Evidence path: `{{primary_evidence_path}}`
- Latest sanity summary: `E:\1_Data\2_Logs\ops_sanity_quick_latest.json`
- Run log: `{{run_log_path}}`

## Interpretation
{{what_the_facts_mean_without_expanding_policy}}

## Impact
- Affected strategies: {{strategies_or_unknown}}
- Orders affected: {{orders_or_unknown}}
- Fills affected: {{fills_or_unknown}}
- Ledger/stats affected: {{ledger_stats_or_unknown}}
- PnL impact estimate: {{pnl_or_unknown}}

## Quick Reproduction
1. Run `E:\1_Data\run_ops_sanity_quick.bat`
2. Check `E:\1_Data\2_Logs\ops_sanity_quick_latest.json`
3. Check `E:\1_Data\2_Logs\canonical_replay_compare_latest.json`
4. Check `E:\1_Data\2_Logs\ssot_health_card_latest.json`

## Evidence Bundle
- Sanity summary: `E:\1_Data\2_Logs\ops_sanity_quick_latest.json`
- Sanity log: `{{run_log_path}}`
- Gateway health: `E:\1_Data\2_Logs\gateway_health_*_latest.json`
- SSOT health: `E:\1_Data\2_Logs\ssot_health_card_latest.json`
- Fills summary: `E:\1_Data\2_Logs\ops_sanity_fills_latest.json`
- Replay compare: `E:\1_Data\2_Logs\canonical_replay_compare_latest.json`
- Additional logs: `{{additional_logs_or_na}}`

## Actions Taken
- t+0: {{action}}
- t+N: {{action}}

## Mitigation / Rollback
- Temporary mitigation: {{mitigation_or_na}}
- Rollback target: {{rollback_target_or_na}}
- State-changing action approved by: {{approver_or_na}}

## Verification Matrix
- Functional verification: PASS / FAIL / NA - {{evidence_or_reason}}
- Consistency verification: PASS / FAIL / NA - {{evidence_or_reason}}
- Operating reflection verification: PASS / FAIL / NA - {{evidence_or_reason}}
- Policy verification: PASS / FAIL / NA - {{evidence_or_reason}}
- FAIL-CLOSED verification: PASS / FAIL / NA - {{evidence_or_reason}}
- Regression verification: PASS / FAIL / NA - {{evidence_or_reason}}

## Tested
- {{what_was_tested}}

## Not Tested
- {{what_was_not_tested}}

## Owner / RACI
- Incident owner: {{name}}
- Scribe: {{name}}
- RCA owner: {{name}}
- Follow-up due: {{date}}

## Final Judgment
{{complete|partial|incomplete}} - {{short_reason}}
