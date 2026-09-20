# 2026-04-30 News Alignment and LLM Status Fix

## Scope
- Fix the full daily batch news date-alignment issue observed after E2E verification.
- Prevent disabled LLM invocations from overwriting the latest active LLM scoring status.

## Changes
- `tools/news_score_daily.py`
  - Default `NEWS_SCORE_MAX_FORWARD_DAYS` changes from `3` to `0`.
  - Effect: news scoring no longer uses articles dated after the candidate as-of date unless explicitly overridden by environment.
- `tools/news_llm_score_daily.py`
  - Disabled runs write to `news_llm_score_status_disabled_*.json`.
  - Active latest status remains reserved for enabled scoring runs and real enabled-run failures.
- `tools/final_score_merge_daily.py`
  - Preserve `used_lag_days=0` instead of converting it to `-1` during status/dynamic-weight reporting.

## Validation Plan
- Python syntax check for touched tools.
- Run disabled LLM path and confirm active latest is not overwritten.
- Run `news_score_daily.py` and confirm `used_date8 <= asof_ymd`.
- Run `final_score_merge_daily.py` and confirm `news_gate.used_lag_days=0`.
- Run official `run_paper_daily.bat` with LLM enabled and confirm downstream status artifacts.

## Safety
- No Gate, D rule, LOCK, broker, order/fill/ledger, or SSOT policy meaning is changed.
- Date alignment is fail-closed relative to candidate as-of: future news is excluded by default.
