# Technical Indicator Analysis (2026-03-05)

## Run config
- Script: `E:\1_Data\tools\indicator_factor_diagnostic.py`
- Data window: 2025-11-05 ~ 2026-03-05
- Universe rule: `min_universe=2000`
- Target: `fwd_ret_1d`

## Artifacts (SSOT)
- Summary: `E:\1_Data\2_Logs\indicator_diag_summary_latest.json`
- Continuous factors: `E:\1_Data\2_Logs\indicator_diag_continuous_latest.csv`
- Binary filters: `E:\1_Data\2_Logs\indicator_diag_binary_latest.csv`
- Daily combo: `E:\1_Data\2_Logs\indicator_diag_daily_combo_latest.csv`

## Key findings
1. Current full-combo filter(`all_pass`) is too sparse.
- `selected_day_ratio=0.65`, `avg_selected_n=1.71`
- `avg_alpha_bps=-76.03`, `positive_alpha_day_ratio=0.20`

2. Strong/weak binary filters (current threshold set)
- Positive delta: `stretch_pass`, `listing_pass`, `rs_pass`, `rsi_pass`
- Negative delta: `volcorr_pass`, `high52_pass`, `all_pass`

3. Continuous factor ranking (q5-q1 bps 기준)
- Positive: `atr14_pct`, `listing_days`, `high_52w_gap`, `rs`
- Negative: `rsi14`, `rs_slope`, `vol_close_corr20`

## Interpretation (operational)
- Single-day target 기준으로는 `vol_close_corr_min >= 0`와 `near_52w_high_gap_max <= 0.05` 조합이 과도하게 보수적으로 작동.
- `all_pass`의 기대수익이 음수로 나온 핵심 원인은 "필터 교집합 과도 축소".

## Immediate tuning candidates
1. Keep (or strengthen slightly)
- `rs_lim` (signal quality 유지)
- `rsi_max` (과열 억제 유지)

2. Relax / redesign candidates
- `vol_close_corr_min`: `0.0 -> -0.1~-0.2` 탐색
- `near_52w_high_gap_max`: `0.05 -> 0.08~0.12` 탐색
- `min_listing_days`: `126 -> 150/180` 비교 (안정성 중시 시)

3. Validation protocol
- Horizon split: `fwd_ret_1d`, `fwd_ret_2d`, `fwd_ret_5d` 모두 동일 방식 비교
- Gate: `avg_alpha_bps > 0`, `positive_alpha_day_ratio > 0.5`, `selected_day_ratio >= 0.4`

## Notes
- `generate_candidates_v41_1.py` 내부 concat FutureWarning이 분석 실행 시 출력됨 (기능 영향 없음, 추후 정리 권장).
