# 2026-04-30 Surge News Fallback and Weight Fix

## Scope
- RootA only.
- Add `signals_naver_daily` fallback to surge news score loading.
- Make `weight_news` contribute proportionally to `surge_score_final` when news score is non-zero.
- Keep `NEWS_NEGATIVE` threshold and positive news bonus policy unchanged.
- Do not change Gate, LOCK, D rule, broker mode, order/fill/ledger chain, or news collection policy.

## Backup
- `E:\1_Data\backup\20260430_surge_news_fallback_weight\20260430_151939`

## Plan
1. Update `tools\surge_detector_realtime.py`.
2. Run syntax validation.
3. Run surge detector.
4. Verify coverage, `001510`, `027360`, and latest JSON metrics.
5. Record results.

## Validation
- Syntax:
  - `python -m py_compile tools\surge_detector_realtime.py`
  - PASS.
- Direct map check:
  - `_load_news_score_map("20260430")`
  - `map_count=174`
  - `001510=1.0`
  - `027360=-1.0`
- Runtime:
  - `python tools\surge_detector_realtime.py`
  - exit `0`
  - latest `surge_realtime_latest.json`:
    - `input_rows=40`
    - `evaluated_rows=40`
    - `news_score_map_count=174`
    - `news_score_covered_rows=20`
    - `news_score_nonzero_rows=7`
- Result checks:
  - `001510 news_score=1.0`
  - `027360 news_score=-1.0`
  - `027360 exclude_reasons` includes `NEWS_NEGATIVE:-1.000<-0.300`
  - `016380 news_score=0.3 surge_score_final=97.2`
  - `263020 news_score=1.0 surge_score_final=92.74560000000001`
- Mojibake:
  - full repo scan still reports existing issues.
  - touched target files had `target_issue_files=0`.

## Status
- Coverage gap: improved from candidate CSV coverage `11/40` to signal-backed coverage `20/40`.
- `weight_news`: now participates proportionally for non-zero news scores.
- `NEWS_NEGATIVE`: can now use same-day `signals_naver_daily` negative scores even when the candidate CSV has stale/zero news.

## Remaining
- Full intraday loop was not rerun in this validation; only the detector entrypoint was run.
- Surge-dedicated news collection is not required for the current coverage expansion.

## 2026-04-30 Coverage Expansion

### Scope
- RootA only.
- Extend the existing Naver news collector coverage by including the full `surge_realtime_latest.csv` monitored universe.
- Add status JSON coverage metrics for surge universe coverage in `signals_naver_daily`.
- Do not create a separate surge-only pipeline.

### Backup
- `E:\1_Data\backup\20260430_news_coverage_expansion\20260430_155753`

### Change
- `tools\news_collect_naver_daily.py`
  - Removed the `surge_flag=True` filter from `_load_surge_symbols()`.
  - Added `coverage.surge_signal_coverage` to `news_collect_status_latest.json`.

### Evidence
- Syntax:
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile tools\news_collect_naver_daily.py`: exit `0`.
- Direct function check:
  - `_load_surge_symbols()` returned `40`.
  - `_compute_signal_coverage("20260430", surge_symbols)` returned:
    - `surge_universe_symbols=40`
    - `signals_covered_symbols=20`
    - `signals_missing_symbols=20`
- Runtime:
  - `E:\1_Data\_runtime\python312-embed\python.exe tools\news_collect_naver_daily.py --session-name evening --window-start 15:30 --window-end 20:00`: exit `0`.
  - `E:\1_Data\2_Logs\news_collect_status_latest.json`:
    - `generated_at=2026-04-30 16:00:38`
    - `reason=ok`
    - `symbols=88`
    - `fetched=88`
    - `saved=88`
    - `article_rows_saved=39`
    - `quality=PASS`
    - `scope.surge_symbols=40`
    - `coverage.surge_signal_coverage.surge_universe_symbols=40`
    - `coverage.surge_signal_coverage.signals_covered_symbols=40`
    - `coverage.surge_signal_coverage.signals_missing_symbols=0`
- Mojibake:
  - `E:\1_Data\2_Logs\mojibake_text_scan_latest.json`
  - full repo existing issues remain: `issue_file_count=133`, `issue_count=850`, `repairable_count=0`.
  - touched target issue count: `0`.

### Validation Matrix
- Functional validation: PASS for symbol expansion and coverage metric generation.
- Consistency validation: PASS; collector scope now matches the 40-row surge monitor universe.
- Operational reflection validation: PASS; latest status JSON and DB signal coverage show `40/40`.
- Policy validation: PASS; source, tier policy, Gate, LOCK, and scoring semantics were not changed.
- FAIL-CLOSED validation: PASS; existing quota guard path was observed before the later successful run and no guard semantics changed.
- Regression validation: PASS for syntax, direct function check, collector runtime path, JSON parse, and target mojibake check.

### Remaining
- None in this requested coverage-expansion scope.

## 2026-04-30 News Effect Validation

### Scope
- RootA only.
- Validate whether the current news signal is ready for tuning/high-automation before LLM/body-crawling upgrades.
- Do not change trading policy thresholds.

### Backup
- `E:\1_Data\backup\20260430_news_effect_validation\20260430_160930`

### Change
- Added `E:\1_Data\tools\diagnose_surge_news_effect.py`.
- Generated:
  - `E:\1_Data\2_Logs\surge_news_effect_latest.json`
  - `E:\1_Data\2_Logs\surge_news_effect_rows_latest.csv`

### Evidence
- Surge detector refresh:
  - `E:\1_Data\_runtime\python312-embed\python.exe tools\surge_detector_realtime.py`: exit `0`.
  - `E:\1_Data\2_Logs\surge_realtime_latest.json`:
    - `status=OK`
    - `input_rows=40`
    - `evaluated_rows=40`
    - `news_score_map_count=194`
    - `news_score_covered_rows=40`
    - `news_score_nonzero_rows=9`
- Syntax:
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile tools\diagnose_surge_news_effect.py`: exit `0`.
- Runtime:
  - `E:\1_Data\_runtime\python312-embed\python.exe tools\diagnose_surge_news_effect.py`: exit `0`.
  - `rows=40`
  - `news_nonzero_rows=9`
  - `positive_news_rows=8`
  - `negative_news_rows=1`
  - `news_bonus_rows=6`
  - `news_negative_block_rows=1`
  - `blocked_only_by_news_rows=0`
  - `eligible_rows=11`
  - `eligible_news_nonzero_rows=4`
  - `alerts_rows=1`
  - `alerts_news_nonzero_rows=0`
  - `avg_abs_delta_pts_nonzero=7.226917`
  - `max_positive_delta_pts=13.0`
  - `max_negative_delta_pts=-5.148`

### Validation Matrix
- Functional validation: PASS for current news effect measurement.
- Consistency validation: PASS; detector coverage and diagnosis input both use 40-row surge universe.
- Operational reflection validation: PASS; JSON/CSV diagnosis artifacts were written.
- Policy validation: PASS; no threshold or gate policy was changed.
- FAIL-CLOSED validation: PASS; `NEWS_NEGATIVE` remains a block reason and no bypass was added.
- Regression validation: PASS for syntax, detector runtime, diagnosis runtime, JSON/CSV parse.

### Decision
- Current stage: effect measurement.
- Tuning readiness: NO.
- Reason: only `9` non-zero news rows and `1` negative block row in the current surge sample.
- Next: accumulate more dated diagnosis samples before changing `news_block_threshold`, `news_bonus_threshold`, or `news_bonus_pts`.

## 2026-05-02 News Credential Status Fix

### Scope
- RootA news collector status recording only.
- Fixed `credential_source` status on early-return guard paths.
- No API call policy, weekend guard, quota guard, source tier, or scoring logic changed.

### Backup
- `E:\1_Data\backup\20260502_news_credential_status_fix\20260502_133349`

### Change
- `tools\news_collect_naver_daily.py`
  - Load credentials before guard early returns.
  - Keep the existing missing-key check in the original execution path.

### Evidence
- Syntax:
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile tools\news_collect_naver_daily.py`: exit `0`.
- Runtime:
  - `E:\1_Data\_runtime\python312-embed\python.exe tools\news_collect_naver_daily.py --session-name intraday --window-start 09:00 --window-end 15:30`: exit `0`.
  - Guard path observed: `weekend_guard_skip`.
- Result:
  - `E:\1_Data\2_Logs\news_collect_status_latest.json`:
    - `reason=weekend_guard_skip`
    - `weekend_skip=True`
    - `credential_source=env`
  - Direct credential check:
    - `src=env`
    - `client_id_present=True`
    - `secret_present=True`

### Validation Matrix
- Functional validation: PASS.
- Consistency validation: PASS.
- Operational reflection validation: PASS.
- Policy validation: PASS; guard semantics unchanged.
- FAIL-CLOSED validation: PASS; weekend guard still skips collection.
- Regression validation: PASS for syntax, guard runtime, status JSON parse, and direct credential check.

### Remaining
- None in this requested scope.
