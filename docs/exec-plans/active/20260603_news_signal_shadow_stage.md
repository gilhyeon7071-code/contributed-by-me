# 2026-06-03 News Signal Shadow Stage ExecPlan

## Scope
- RootA news logic only.
- Add a shadow-only structured news signal stage.
- Output diagnostic CSV/JSON under `2_Logs`.
- Do not change entry score, Gate, LOCK, order, fill, ledger, broker dispatch, sizing, or live trading policy.

## Rationale
- Current news logic mostly contributes `news_score` and implication columns into candidate/final score artifacts.
- The requested direction is a more structured pipeline:
  - provenance/source context
  - preliminary signal
  - market/LOB corroboration
  - confirmed/block/watch stage
- This implementation records those stages without consuming them for trading.

## Planned Changes
1. Add `tools/build_news_signal_shadow_stage.py`.
2. Read existing `candidates_latest_data.with_final_score.csv` or fallback `with_news_score.csv`.
3. Read latest status JSONs and available execution/LOB columns already present in candidate artifacts.
4. Produce:
   - `2_Logs/news_signal_shadow_stage_latest.csv`
   - `2_Logs/news_signal_shadow_stage_latest.json`
5. Add a fail-soft call in `run_news_pipeline_once.bat` after `final_score_merge_daily.py`.

## Non-Goals
- No `CONFIRMED_SIGNAL` consumption by `paper_engine.py`.
- No buy/sell decision change.
- No score or threshold relaxation.
- No source-weight promotion.
- No FinBERT dependency addition.

## Validation Plan
- Syntax validation: `python -m py_compile tools\build_news_signal_shadow_stage.py`.
- Execution validation: run the script directly.
- Artifact validation: confirm latest CSV/JSON exist and report `trading_effect=false`.
- Policy validation: confirm script is not imported by `paper_engine.py` and output is shadow-only.
- FAIL-CLOSED validation: missing inputs produce `NO_TEXT_SIGNAL` or `WATCH`, not buy approval.
- Regression validation: run the pipeline with collect/final/integration skips only if needed, or validate the batch reference and script standalone without broad artifact overwrite.
