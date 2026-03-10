# Indicator Auto-Run Integration (2026-03-05)

## What was added
- Multi-horizon diagnostic runner:
  - `E:\1_Data\tools\indicator_factor_diagnostic.py` (`--target-horizon`, `--tag`)
- Combined runner + parameter recommendation:
  - `E:\1_Data\tools\indicator_diag_and_recommend.py`
- Manual batch entry:
  - `E:\1_Data\run_indicator_diag_and_recommend.bat`
- Main pipeline hook (fail-soft):
  - `E:\1_Data\run_paper_daily.bat` step `[15.5/16]`

## Runtime behavior
- Default: auto enabled (`INDICATOR_DIAG_AUTO=1`)
- Set `INDICATOR_DIAG_AUTO=0` to skip recommendation step.
- If recommendation step fails, pipeline continues (`WARN`, no hard fail).

## Artifacts
- Horizon summaries:
  - `E:\1_Data\2_Logs\indicator_diag_summary_latest_h1.json`
  - `E:\1_Data\2_Logs\indicator_diag_summary_latest_h2.json`
  - `E:\1_Data\2_Logs\indicator_diag_summary_latest_h5.json`
- Parameter candidates:
  - `E:\1_Data\12_Risk_Controlled\param_candidates_v41_1_latest.json`

## Current recommendation
- `recommended_profile = neutral`
