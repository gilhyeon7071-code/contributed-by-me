# Current Operational Classification

generated_at: 2026-08-31T10:05:37
scope: `llm_wiki_operational_classification`
policy_effect: `false`
trading_effect: `false`
summary_status: `REVIEW`

| area | status | reason | evidence |
|---|---|---|---|
| RootA score context | `ACTIVE` | orders_exec_exists=True, score_asof_matches_expected=True | `E:\1_Data\docs\llm_wiki\00_Current_State\system_state_latest.md` |
| RootA order/fill/ledger/stat interpretation | `REVIEW` | ssot=PASS, ledger_dry=PASS, ledger_repair=NOOP, reconcile_state=PASS, live_vs_bt=NA, date_note=REVIEW_STRICT_D_MISMATCH, ledger_repair_effective_clean=True | `E:\1_Data\2_Logs\ssot_health_card_latest.json` |
| RootA score date strict-D note | `NOTE` | strict_asof_matches_D=False, date_asof_interpretation_status=REVIEW_STRICT_D_MISMATCH | `E:\1_Data\docs\llm_wiki\01_Policies\date_asof_interpretation.md` |
| RootA intraday loop summary | `ACTIVE` | intraday_steps=1/1, intraday_effective_clean=True | `E:\1_Data\docs\llm_wiki\00_Current_State\system_state_latest.md` |
| RootB dashboard context | `REVIEW` | status_overall=FAIL, health_alerts_count=1 | `E:\vibe\buffett\docs\llm_wiki\00_Current_State\dashboard_state_latest.md` |
| Slack and meeting external API | `DEFERRED` | blockers=SLACK_TOKEN_NOT_CONFIGURED,SLACK_CHANNELS_NOT_CONFIGURED,MEETING_SOURCE_DIR_NOT_CONFIGURED | `E:\1_Data\docs\llm_wiki\05_Logs\external_readiness_latest.json` |
| Trading logic behavior | `NOT_CHANGED` | Wiki/reporting layer only; no Gate, STOP, LOCK, risk, score, order, fill, ledger, stats behavior changed. | `E:\1_Data\docs\llm_wiki\wiki_index_latest.md` |

## Boundary

- This report classifies current Wiki/operations context for Codex reading.
- It does not approve trading or change runtime behavior.
- `REVIEW` means inspect canonical artifacts before interpreting the related area.
- `NOTE` means the condition should be visible in reporting but is not currently a blocker.
- `DEFERRED` means intentionally not connected or not configured.
