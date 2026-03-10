# Integrity Gate Report (2026-03-09)

- source_overlay: `E:\1_Data\2_Logs\integrity_overlay_latest.json`
- gate_ok: **False**
- pass_n/fail_n: **0 / 3**

## 샘플링검증 (FAIL)

- total=7 pass=4 warn=1 fail=0 skip=2 pass_rate=57.1%
- checks
  - pass_rate: FAIL (actual=57.1 expected=>= 65.0)
  - fail_count: OK (actual=0 expected=<= 0)
  - warn_skip_count: FAIL (actual=3 expected=<= 2)
- issue_items
  - [DESIGN] Cross-check / WARNING / Cross-check reconciliation validated
  - [STRATEGY] Overfitting / SKIPPED / Insufficient OOS trade sample: out_trades=42, in_trades=398
  - [STRATEGY] Walk-forward regime robustness / SKIPPED / Insufficient risk-adjusted sample: sample=36

## 비즈니스규칙검증 (FAIL)

- total=23 pass=17 warn=4 fail=0 skip=2 pass_rate=73.9%
- checks
  - pass_rate: FAIL (actual=73.9 expected=>= 75.0)
  - fail_count: OK (actual=0 expected=<= 0)
  - warn_skip_count: OK (actual=6 expected=<= 6)
- issue_items
  - [PLANNING] Cost structure / WARNING / Round-trip with slippage=0.537%
  - [PLANNING] Architecture suitability / WARNING / Viable but not optimal
  - [STRATEGY] Overfitting / SKIPPED / Insufficient OOS trade sample: out_trades=42, in_trades=398
  - [STRATEGY] Max drawdown / WARNING / Drawdown within limit
  - [RISK] Portfolio exposure limits / WARNING / Exposure limits validated
  - [RISK] Cost optimization / SKIPPED / Insufficient realized-trade sample: sample_trades=36

## 도메인무결성 (FAIL)

- total=13 pass=10 warn=3 fail=0 skip=0 pass_rate=76.9%
- checks
  - pass_rate: FAIL (actual=76.9 expected=>= 80.0)
  - fail_count: OK (actual=0 expected=<= 0)
  - warn_skip_count: OK (actual=3 expected=<= 4)
- issue_items
  - [PLANNING] Cost structure / WARNING / Round-trip with slippage=0.537%
  - [STRATEGY] Max drawdown / WARNING / Drawdown within limit
  - [RISK] Portfolio exposure limits / WARNING / Exposure limits validated

