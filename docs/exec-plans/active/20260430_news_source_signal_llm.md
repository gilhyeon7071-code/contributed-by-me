# 2026-04-30 News Source Signal and LLM Scoring ExecPlan

## Scope
- Add a separate original-source signal layer for DART disclosure events.
- Add optional LLM scoring for `news_articles_naver` without replacing existing keyword scores.
- Wire both into the news pipeline before `news_score_daily.py`.
- Keep existing news fallback behavior if DART or LLM credentials are unavailable.

## Non-Scope
- No broker, order, fill, ledger, D rule, Gate, or LOCK semantic changes.
- No dashboard UI change in this pass.
- No full-text crawling.
- No KRX/KIND web scraping in this pass; existing KRX watchlist guards remain unchanged.

## Backup
- `E:\1_Data\backup\20260430_news_source_llm\20260430_090618`

## Files
- Add `E:\1_Data\tools\news_source_signal_daily.py`
- Add `E:\1_Data\tools\news_llm_score_daily.py`
- Update `E:\1_Data\tools\news_score_daily.py`
- Update `E:\1_Data\run_news_pipeline_once.bat`
- Update `E:\1_Data\.agent\PLANS.md`

## Design
- DART events are stored in `source_signals_daily`.
- News article LLM scores are stored in new nullable columns on `news_articles_naver`.
- `news_score_daily.py` reads:
  - `llm_score` when present
  - otherwise existing `article_score`
  - plus bounded source signal overlay from `source_signals_daily`
- If DART API key or LLM API key is missing, the scripts emit status JSON and exit `0`.

## Validation
- Syntax check all changed/new Python files.
- Run source signal script in current environment.
- Run LLM script with default disabled/missing-key behavior.
- Run `news_score_daily.py`.
- Verify status JSON/CSV artifacts.
- Run mojibake scan on changed text files.
