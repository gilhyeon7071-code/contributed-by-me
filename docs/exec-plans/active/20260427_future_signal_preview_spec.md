# 2026-04-27 future signal preview spec

## Goal
- Fix the contract for a future-prediction preview layer before adding any new model behavior.
- Separate the current `FORECAST_BLEND_V1` proxy score from a future ML prediction model.

## Non-Goals
- Do not change order, fill, ledger, or stats logic.
- Do not change Gate, LOCK, risk, entry, or exit semantics.
- Do not change the current `forecast_score` weight in this step.
- Do not claim ML prediction quality from the current proxy score.

## Current Facts
- Current forecast artifact:
  - `E:\1_Data\2_Logs\forecast_score_validation_latest.json`
  - `validation_type=FORWARD_PROXY_V1`
  - `status=PASS`
  - `score_asof_ymd=20260424`
  - `forward_asof_ymd=20260424`
  - `asof_alignment=MATCH`
  - `forward_proxy_rate=1.0`
- Current candidate output:
  - `E:\1_Data\2_Logs\candidates_latest_data.with_final_score.csv`
  - columns include `forecast_score`, `forecast_label`, `forecast_score_source`
  - `forecast_score_source=FORECAST_BLEND_V1`
- Current final-score status:
  - `E:\1_Data\2_Logs\final_score_merge_status_latest.json`
  - current effective `forecast` weight observed as `0.159729`
- Current missing artifact:
  - no `future_signal_preview_YYYYMMDD.csv`
  - no `future_signal_preview_YYYYMMDD.json`

## Decision
- Treat current `FORECAST_BLEND_V1` as a forecast proxy score, not as a trained future-prediction model.
- The next model output must be preview/shadow first.
- Preview output must not write or modify:
  - orders
  - fills
  - ledger
  - stats
  - Gate state
  - risk/LOCK state
- Paper ranking influence requires separate shadow evidence and policy approval.

## Preview Output Contract
- CSV:
  - `E:\1_Data\2_Logs\future_signal_preview_YYYYMMDD.csv`
- JSON:
  - `E:\1_Data\2_Logs\future_signal_preview_YYYYMMDD.json`
  - `E:\1_Data\2_Logs\future_signal_preview_latest.json`

Required columns:
- `as_of`
- `run_id`
- `D`
- `code`
- `name`
- `horizon`
- `pred_up_prob`
- `expected_return`
- `downside_q10`
- `confidence`
- `abstain`
- `reason`
- `model_version`
- `feature_version`
- `feature_asof`
- `train_window_start`
- `train_window_end`
- `calibration_version`

## STOP / Abstain Conditions
- `D` missing.
- `as_of` missing.
- `run_id` missing.
- `feature_asof > D`.
- model artifact missing.
- feature schema mismatch.
- calibration artifact missing for probabilistic output.
- prediction row count is zero.
- duplicate `D, code, horizon` rows.

On any STOP condition:
- write status `FAIL` or row-level `abstain=true`
- do not alter order/risk/Gate artifacts
- keep existing trading path unchanged

## Validation Plan
1. Functional validation:
   - preview CSV/JSON is generated with required columns.
2. Consistency validation:
   - `as_of`, `D`, `run_id`, `feature_asof` are aligned.
3. Operational reflection validation:
   - preview artifacts are written under `E:\1_Data\2_Logs`.
   - no order/fill/ledger/stats files are modified.
4. Policy validation:
   - preview remains read-only for trading decisions.
5. FAIL-CLOSED validation:
   - missing model/features/calibration produces `FAIL` or `abstain=true`.
6. Regression validation:
   - existing `final_score_merge_daily.py` output remains unchanged unless explicitly requested later.

## Evidence To Collect Later
- `future_signal_preview_latest.json`
- `future_signal_preview_YYYYMMDD.csv`
- model metadata
- feature metadata
- calibration metadata
- shadow realized performance after at least 20 trading days

## 2026-04-27 Generator Step
- Added read-only generator:
  - `E:\1_Data\tools\build_future_signal_preview.py`
- Current first-run behavior:
  - if no model metadata is supplied, write preview rows with `abstain=true`
  - set JSON status to `FAIL`
  - set reason to `MODEL_ARTIFACT_MISSING`
  - do not modify order/fill/ledger/stats/Gate/risk/LOCK artifacts
- This is intentional for the current pre-model stage.
- Verification:
  - syntax check PASS with `E:\1_Data\_runtime\python312-embed\python.exe`
  - standalone run rc=1 because of intentional fail-closed `MODEL_ARTIFACT_MISSING`
  - `future_signal_preview_latest.json.status=FAIL`
  - `rows=11`
  - `abstain_rows=11`
  - `duplicate_d_code_horizon_rows=0`
  - CSV required columns count=18

## 2026-04-27 Model Meta / Feature Step
- Added model metadata contract:
  - `E:\1_Data\docs\references\FUTURE_SIGNAL_MODEL_META_CONTRACT.json`
- Added read-only feature generator:
  - `E:\1_Data\tools\build_future_signal_features.py`
- Added preview validation for:
  - model metadata required keys
  - model path existence
  - calibration path existence
  - train window after D
  - feature artifact existence
  - feature status
  - feature version/schema mismatch
- Feature artifacts:
  - `E:\1_Data\2_Logs\future_signal_features_YYYYMMDD.csv`
  - `E:\1_Data\2_Logs\future_signal_features_YYYYMMDD.json`
  - `E:\1_Data\2_Logs\future_signal_features_latest.json`
- The feature generator remains read-only for trading.
- Verification:
  - syntax check PASS for feature and preview scripts
  - feature run rc=0
  - `future_signal_features_latest.json.status=PASS`
  - `D=20260424`
  - `feature_asof=20260424`
  - `feature_version=FUTURE_SIGNAL_FEATURES_V1`
  - `rows=11`
  - `duplicate_d_code_rows=0`
  - preview run rc=1 due to `MODEL_META_MISSING`
  - `future_signal_preview_latest.json.status=FAIL`
  - `abstain_rows=11`
  - model meta contract JSON parses with `ConvertFrom-Json`

## 2026-04-27 Baseline Predictor Step
- Add read-only baseline predictor:
  - `E:\1_Data\tools\build_future_signal_baseline.py`
- Baseline outputs:
  - `E:\1_Data\2_Logs\future_signal_baseline_YYYYMMDD.csv`
  - `E:\1_Data\2_Logs\future_signal_baseline_YYYYMMDD.json`
  - `E:\1_Data\2_Logs\future_signal_baseline_latest.json`
  - `E:\1_Data\_cache\future_signal\future_signal_rule_baseline_model_meta_latest.json`
- Baseline policy:
  - `trading_approved=false`
  - `calibrated=false`
  - read-only for orders/fills/ledger/stats/Gate/risk/LOCK
- Preview can read `prediction_path` from model metadata and fill prediction columns.
- Verification:
  - syntax check PASS for baseline and preview scripts
  - baseline run rc=0
  - `future_signal_baseline_latest.json.status=PASS`
  - `rows=11`
  - `abstain_rows=5`
  - `model_version=RULE_BASELINE_V0`
  - `calibrated=false`
  - `trading_approved=false`
  - preview with model metadata rc=0
  - `future_signal_preview_latest.json.status=PASS`
  - prediction-filled rows=11
  - preview abstain rows=5

## 2026-04-27 Validation Harness Step
- Add read-only validation harness:
  - `E:\1_Data\tools\validate_future_signal_preview.py`
- Validation outputs:
  - `E:\1_Data\2_Logs\future_signal_validation_YYYYMMDD.json`
  - `E:\1_Data\2_Logs\future_signal_validation_latest.json`
- Behavior:
  - if `D+horizon` realized price data is unavailable, emit `status=FAIL`, `validation_state=WAITING`
  - do not treat missing future data as model success
  - when target data exists, compute realized return coverage, hit rate, Spearman, top-k metrics
- The validator remains read-only for trading.
- Verification:
  - syntax check PASS
  - run rc=1 due to `TARGET_DATE_NOT_AVAILABLE`
  - `future_signal_validation_latest.json.status=FAIL`
  - `validation_state=WAITING`
  - `D=20260424`
  - `horizon=5`
  - `rows=11`
  - `realized_rows=0`
  - available price date range `20260403..20260424`

## 2026-04-27 Calibration Validation Step
- Add read-only calibration validator:
  - `E:\1_Data\tools\validate_future_signal_calibration.py`
- Calibration outputs:
  - `E:\1_Data\2_Logs\future_signal_calibration_YYYYMMDD.json`
  - `E:\1_Data\2_Logs\future_signal_calibration_latest.json`
- Behavior:
  - if realized validation is not evaluated, emit `status=FAIL`, `calibration_state=WAITING`
  - if model is uncalibrated, emit `status=FAIL`, `calibration_state=UNCALIBRATED`
  - do not treat heuristic baseline probabilities as calibrated probabilities
  - when realized row-level data exists and calibration is fitted, compute Brier score and calibration bins
- The validator remains read-only for trading.
- Verification:
  - syntax check PASS
  - run rc=1 due to `REALIZED_VALIDATION_NOT_EVALUATED:WAITING`
  - `future_signal_calibration_latest.json.status=FAIL`
  - `calibration_state=WAITING`
  - `model_version=RULE_BASELINE_V0`
  - `calibration_version=RULE_BASELINE_UNCALIBRATED_V0`
  - `calibrated=false`
  - `trading_approved=false`
  - `brier_score=null`
  - `bin_count=0`

## 2026-04-27 Shadow History Step
- Add read-only shadow history updater:
  - `E:\1_Data\tools\update_future_signal_shadow_history.py`
- Shadow history outputs:
  - `E:\1_Data\2_Logs\future_signal_preview_history.csv`
  - `E:\1_Data\2_Logs\future_signal_shadow_history_latest.json`
- Behavior:
  - upsert by `run_id, code, horizon`
  - preserve preview, validation, and calibration state per row
  - keep `trading_approved=false` in history when calibration says false
  - do not modify orders/fills/ledger/stats/Gate/risk/LOCK
- This is the data collection basis for 20/60 trading-day shadow review.
- Verification:
  - syntax check PASS
  - run rc=0
  - `future_signal_shadow_history_latest.json.status=PASS`
  - `history_rows=11`
  - `unique_D=1`
  - `latest_D=20260424`
  - `validation_state=WAITING`
  - `calibration_state=WAITING`
  - `trading_approved=false`
  - history CSV rows=11
  - unique `run_id,code,horizon` keys=11

## 2026-04-27 Dashboard State Connection Step
- Add read-only dashboard state summary:
  - `E:\vibe\buffett\tools\build_dashboard_state_v2.py`
- Summary key:
  - `future_signal_summary`
- Source artifacts:
  - `E:\1_Data\2_Logs\future_signal_preview_latest.json`
  - `E:\1_Data\2_Logs\future_signal_validation_latest.json`
  - `E:\1_Data\2_Logs\future_signal_calibration_latest.json`
  - `E:\1_Data\2_Logs\future_signal_shadow_history_latest.json`
- Behavior:
  - expose preview, validation, calibration, and shadow collection state in dashboard state JSON
  - keep orders/fills/ledger/stats/Gate/risk/LOCK unchanged
  - preserve `trading_approved=false` from calibration state
- Verification:
  - syntax check PASS
  - dashboard state build rc=0
  - `E:\vibe\buffett\runs\dashboard_state_latest.json` JSON parse PASS
  - `future_signal_summary.status=WAITING`
  - `preview_status=PASS`
  - `D=20260424`
  - `horizon=5`
  - `model_version=RULE_BASELINE_V0`
  - `feature_version=FUTURE_SIGNAL_FEATURES_V1`
  - `preview_rows=11`
  - `preview_abstain_rows=5`
  - `validation_state=WAITING`
  - `calibration_state=WAITING`
  - `trading_approved=false`
  - `shadow_unique_D=1`
  - `shadow_ready_20d=false`
  - `shadow_ready_60d=false`

## 2026-04-27 Dashboard UI Step
- Add read-only UI cards in:
  - `E:\vibe\buffett\dashboard.py`
- UI location:
  - main tab `운용단계`
  - code block `main_tabs[4]`
- UI labels:
  - `미래예측 미리보기`
  - `예측 산출물`
  - `검증/승인`
- Behavior:
  - render `future_signal_summary` values only
  - keep trading path unchanged
  - keep `trading_approved=false` visible as not approved
- Verification:
  - syntax check PASS
  - static code evidence:
    - `future_signal_summary` read at `dashboard.py:12303`
    - UI labels at `dashboard.py:12579`, `dashboard.py:12591`, `dashboard.py:12603`
  - Streamlit AppTest did not reach UI rendering because the copied app test environment failed on the existing `root_config` import.
  - bare `runpy.run_path(E:\vibe\buffett\dashboard.py)` rc=0 with `DASHBOARD_RUNPATH_OK`.
  - Browser rendering was not tested in this step.

## 2026-04-27 Daily Batch Step
- Add orchestrator:
  - `E:\1_Data\tools\run_future_signal_daily.py`
- Connect orchestrator to:
  - `E:\1_Data\run_paper_daily.bat`
  - after `final_score_merge_daily.py`
  - before `build_integrated_ops_snapshot.py`
- Orchestrated steps:
  - features
  - baseline
  - preview with latest baseline model metadata
  - realized validation
  - calibration validation
  - shadow history update
- Waiting states:
  - `validation_state=WAITING` is allowed for daily collection when target date data is unavailable.
  - `calibration_state=WAITING/UNCALIBRATED` is allowed for daily collection.
  - `trading_approved=false` remains unchanged.
- Shadow history idempotence:
  - upsert by `D, code, horizon`
  - normalize codes to 6-character strings before dedupe
- Verification:
  - syntax check PASS for `run_future_signal_daily.py` and `update_future_signal_shadow_history.py`
  - standalone daily run rc=0
  - `future_signal_daily_status_latest.json.status=PASS`
  - `D=20260424`
  - `preview_status=PASS`
  - `preview_rows=11`
  - `preview_abstain_rows=7`
  - `validation_state=WAITING`
  - `calibration_state=WAITING`
  - `trading_approved=false`
  - `shadow_history_rows=11`
  - `shadow_unique_D=1`
  - dashboard state JSON parse PASS
  - `future_signal_summary.preview_abstain_rows=7`
  - `future_signal_summary.shadow_history_rows=11`
  - `future_signal_preview_history.csv` rows=11
  - duplicate `D,code,horizon` rows=0
- Full `run_paper_daily.bat` was not executed in this step because it refreshes broad operating artifacts.

## 2026-04-27 Official Batch Verification
- Ran:
  - `E:\1_Data\run_paper_daily.bat`
- Future signal step evidence:
  - `run_paper_daily_last.txt` contains `[6.968/9] tools\run_future_signal_daily.py START`
  - `[6.968/9] END rc=0 elapsed_s=7`
  - `future_signal_daily_status_latest.json.status=PASS`
  - `D=20260424`
  - `preview_status=PASS`
  - `preview_rows=11`
  - `preview_abstain_rows=8`
  - `validation_state=WAITING`
  - `calibration_state=WAITING`
  - `trading_approved=false`
  - `shadow_history_rows=11`
  - `shadow_unique_D=1`
- Dashboard state evidence:
  - JSON parse PASS
  - `future_signal_summary.preview_abstain_rows=8`
  - `future_signal_summary.shadow_history_rows=11`
- Full batch result:
  - `run_paper_daily_last.txt` reports `[FAILED] STEP=[16/16] pending_report verdict`
  - `[WRAPPER_EXIT] rc=1`
  - latest pending report has `pending=9`, `active=20`
  - same-day rows are `SAME_DAY_ENTRY_WAIT_NEXT_SESSION`
  - older rows include `NO_PRICES_AFTER_ENTRY_DATE`
- Conclusion:
  - future signal daily batch connection is verified in the official batch path.
  - the full official batch is not a full PASS because pending report verdict failed.

## 2026-05-13 Moirai / Uni2TS Read-Only Experiment Step
- Add Moirai/Uni2TS experiment scaffolding only:
  - `E:\1_Data\tools\future_signal_prob_metrics.py`
  - `E:\1_Data\tools\run_moirai_uni2ts_smoke.py`
- Scope:
  - no order/fill/ledger/stats/Gate/risk/LOCK changes
  - no daily batch connection
  - no `final_score` or `forecast_score` weight change
  - no `trading_approved=true`
- Behavior:
  - dependency-only mode fails closed when `uni2ts`, `gluonts`, `torch`, or `huggingface_hub` is unavailable
  - precomputed sample mode validates probabilistic forecast samples with shape, CRPS, PIT KS, and deterministic sample fingerprint checks
  - offline self-test validates the local CRPS/PIT implementation without claiming Moirai inference quality
- Current official source notes:
  - Moirai 2.0 model cards direct inference through Uni2TS
  - Uni2TS exposes the Moirai 1.x `MoiraiForecast` / `MoiraiModule` flow and supports source/PyPI installation

## 2026-05-13 Moirai / Uni2TS Inference Adapter Attempt
- Added an optional `SERIES_INFERENCE` path to `E:\1_Data\tools\run_moirai_uni2ts_smoke.py`.
- The path accepts a local series CSV and can call Uni2TS `moirai` / `moirai2` model classes when dependencies are present.
- Current environment state:
  - `setuptools=82.0.1`
  - `wheel=0.47.0`
  - `uni2ts`, `gluonts`, `torch`, `huggingface_hub` unavailable
- Actual inference remains blocked with `UNI2TS_RUNTIME_DEPENDENCY_MISSING`.
- Offline self-test still passes and does not imply model inference quality.

## 2026-05-14 Moirai2 Shadow Preview Step
- Add separate read-only Moirai2 shadow preview generator:
  - `E:\1_Data\tools\build_future_signal_moirai2_preview.py`
- Output contract:
  - `E:\1_Data\2_Logs\future_signal_moirai2_preview_YYYYMMDD.csv`
  - `E:\1_Data\2_Logs\future_signal_moirai2_preview_YYYYMMDD.json`
  - `E:\1_Data\2_Logs\future_signal_moirai2_preview_latest.json`
- Scope:
  - no daily batch connection
  - no `RULE_BASELINE_V0` replacement
  - no order/fill/ledger/stats/Gate/risk/LOCK/score changes
  - `trading_approved=false`
- Inputs:
  - latest `future_signal_features_latest.json`
  - candidate codes from latest feature CSV
  - historical closes from `krx_daily_archive`
- Current purpose:
  - generate shadow Moirai2 zero-shot preview rows for later realized validation
  - not a trading signal
