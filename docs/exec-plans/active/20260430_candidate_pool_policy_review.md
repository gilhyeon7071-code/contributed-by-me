# ExecPlan: candidate pool policy review

Date: 2026-04-30

## 1. Goal
- Review the structural candidate shortage without immediately changing trading policy.
- Current observed state:
  - latest candidate date: 20260429
  - chosen level: L7
  - universe after critical fundamental filter: 1101
  - `all_pass`: 1
  - final candidate CSV rows: 11
  - `NATURAL_PASS`: 1
  - `BALANCED_RESCUE`: 5
  - `SECTOR_PREFILTER_UNION`: 5

## 2. Non-goals
- No immediate threshold change.
- No `value_min`, `v_accel_lim`, RS, disparity, high52, Gate, LOCK, D rule, order, fill, ledger, or stats change.
- No historical candidate or trade rewrite.

## 3. Scope
- Read and evaluate:
  - `E:\1_Data\2_Logs\candidates_latest_meta.json`
  - `E:\1_Data\2_Logs\candidates_latest_data.csv`
  - `E:\1_Data\2_Logs\indicator_diag_summary_latest_h5.json`
  - `E:\1_Data\12_Risk_Controlled\param_candidates_v41_1_latest.json`
  - candidate selection logic in `E:\1_Data\generate_candidates_v41_1.py`

## 4. Current Evidence
- Current `value_min` is 1,000,000,000 KRW, not the old 19.2B KRW.
- `value_pass=408`, so value is not the primary current bottleneck.
- L7 single-filter counts:
  - `rs_pass=117`
  - `v_accel_pass=329`
  - `value_pass=408`
  - `stoch_pass=806`
  - `all_pass=1`
- Selected candidates:
  - `NATURAL_PASS=1`
  - `BALANCED_RESCUE=5`
  - `SECTOR_PREFILTER_UNION=5`
- 120-day h5 indicator diagnostics:
  - `value_pass` pass-minus-fail: +646.96 bps
  - `high52_pass` pass-minus-fail: +475.70 bps
  - `all_pass` pass-minus-fail: +283.20 bps
  - `v_accel_pass` pass-minus-fail: -59.67 bps

## 5. Risks
- Relaxing `v_accel_lim` may increase count but has weak current diagnostic support.
- Relaxing `disparity20/60` may increase count but could admit extended names.
- Expanding sector union can increase candidates while bypassing natural filter quality.
- Reducing `value_min` is not justified by current evidence.

## 6. Design
- Treat candidate shortage as a quality/count tradeoff, not a simple value-min issue.
- Prefer evidence-preserving options:
  1. keep `value_min` unchanged;
  2. avoid broad `v_accel` relaxation unless new diagnostics support it;
  3. evaluate a controlled natural-pass target using shadow simulation;
  4. cap sector-union dependence by monitoring natural/rescue/union ratios.

## 7. Implementation Steps
1. Produce read-only diagnostic split by origin and failed filter.
2. Compare current profile with latest recommended profiles.
3. If implementation is selected, create a separate change step with fresh backup:
   - exact target file;
   - exact threshold keys;
   - before/after expected candidate counts;
   - no order execution until validation artifacts pass.

## 8. Verification Plan
1. Syntax verification if code changes.
2. Execution verification through the official candidate generation path.
3. Result artifact verification:
   - `candidates_latest_meta.json`
   - `candidates_latest_data.csv`
   - candidate origin counts
4. Policy verification:
   - confirm only selected candidate policy keys changed.
5. FAIL-CLOSED verification:
   - confirm no Gate/LOCK/D/order/fill/ledger/stats semantics changed.
6. Regression verification:
   - compare prior and new candidate counts and origin distribution.

## 9. Rollback Plan
- Restore changed target files from the backup made immediately before implementation.
- Re-run candidate generation and compare `chosen_level`, `all_pass`, and origin counts.

## 10. Evidence To Collect
- Candidate count by origin.
- Failed-filter distribution.
- Indicator diagnostic support for changed thresholds.
- Before/after JSON and CSV artifact paths.
