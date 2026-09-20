# Roota Current State 2026-05-18

date: 2026-07-23
channel: document
tags: #document #export

## Content

# RootA Current State 2026-05-18

date: 2026-05-18
channel: roota-local-system
tags: #roota #system-state #score-freshness #risk

## Facts

- Root: `E:\1_Data`
- LLM Wiki snapshot: `E:\1_Data\docs\llm_wiki\00_Current_State\system_state_latest.md`
- Final score status: `E:\1_Data\2_Logs\final_score_merge_status_latest.json`
- Freshness evidence: `E:\1_Data\2_Logs\freshness_source_last.json`
- Risk orchestration: `E:\1_Data\2_Logs\risk_orchestration_latest.json`
- Google RSS probe: `E:\1_Data\2_Logs\google_news_rss_probe_latest.json`
- `score_asof_matches_expected=true`
- `strict_asof_matches_D=false`
- `dd_stop_triggered=true`
- `es_triggered=true`
- `position_size_multiplier=0.0`
- Google RSS JSON parses with default PowerShell and explicit UTF-8 reads after the UTF-8-SIG writer fix.

## Interpretation Boundary

- This note is a retrieval source for LLM Wiki only.
- It does not approve trading.
- It does not change Gate, STOP, LOCK, risk, score, order, fill, ledger, or stats behavior.
- `strict_asof_matches_D=false` remains a strict order/fill/ledger/stat interpretation guard.
