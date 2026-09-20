# System State Latest

generated_at: 2026-08-31T10:05:37
root: `E:\1_Data`
mode: read-only wiki snapshot

## Facts

| key | value | evidence |
|---|---:|---|
| D | `20260824` | `paper\fills.csv` latest BUY ymd |
| orders_exec_exists | `true` | `E:\1_Data\paper\orders_20260824_exec.xlsx` |
| asof_ymd | `20260828` | `2_Logs\final_score_merge_status_latest.json` |
| score_expected_ymd | `20260828` | `E:\1_Data\2_Logs\freshness_source_20260831_085851.json` |
| score_expected_mode | `session_previous` | `E:\1_Data\2_Logs\freshness_source_20260831_085851.json` |
| score_asof_matches_expected | `true` | compare score_expected_ymd vs asof_ymd |
| strict_asof_matches_D | `false` | compare fills D vs asof_ymd |
| strict_asof_D_stop | `true` | strict order/fill/ledger/stat interpretation guard |
| date_asof_interpretation_status | `REVIEW_STRICT_D_MISMATCH` | `docs\llm_wiki\01_Policies\date_asof_interpretation.md` |
| paper_open_positions | `0` | `paper\paper_state.json` |
| paper_next_trade_seq | `706` | `paper\paper_state.json` |
| config_lock_ts | `20260826_090529` | `paper\paper_engine_config.lock.json` |
| intraday_ts | `2026-08-31T10:04:44+09:00` | `2_Logs\intraday_loop_status_latest.json` |
| intraday_steps | `1/1` | `2_Logs\intraday_loop_status_latest.json` |
| final_score_generated_at | `2026-08-31T09:57:53` | `2_Logs\final_score_merge_status_latest.json` |
| final_score_rows | `11` | `2_Logs\final_score_merge_status_latest.json` |
| score_regime | `CAUTION` | `2_Logs\final_score_merge_status_latest.json` |
| macro_freshness_status | `PASS` | `2_Logs\final_score_merge_status_latest.json` |
| news_gate | `OPEN` | `2_Logs\final_score_merge_status_latest.json` |
| forecast_validation_status | `PASS` | `2_Logs\final_score_merge_status_latest.json` |

## Interpretation

- This snapshot is a context artifact for LLM reading.
- It does not approve trading and does not modify any policy or runtime state.
- Candidate/score freshness is checked against `score_expected_ymd`.
- `strict_asof_matches_D=false` remains a STOP concern for order/fill/ledger/stat interpretation.
- `date_asof_interpretation_status` is a reporting label only, not a trading approval.

## Validation Matrix

| item | status | note |
|---|---|---|
| function | PASS | generator wrote this snapshot from current artifacts |
| consistency | PASS | orders_exec_exists=true, score_asof_matches_expected=true, strict_asof_matches_D=false |
| operations reflected | NA | no scheduler or trading runtime was changed |
| policy | PASS | read-only docs only; Gate/STOP/LOCK semantics unchanged |
| FAIL-CLOSED | PASS | strict D mismatch remains visible for order/fill/ledger/stat interpretation |
| regression | NA | no trading code path changed |

