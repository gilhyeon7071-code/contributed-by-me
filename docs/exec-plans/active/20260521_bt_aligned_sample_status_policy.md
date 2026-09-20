# 2026-05-21 BT Aligned Sample Status Policy

## Scope
- Target issue: `live_vs_bt_feedback_latest.json` reports `bt_too_few_trades_in_window`.
- Current evidence:
  - `run_ymd=20260521`
  - aligned window `20260305 ~ 20260521`
  - live trades in window: `279`
  - BT trades in window: `4`
  - current minimum shared trades: `10`
- This plan is for status classification only.

## Policy Change Candidate
- Separate structural BT sample shortage from generic integrated ops failure.
- Do not convert the condition to `PASS`.
- Do not lower `min_shared_trades`.
- Do not enable `allow_summary_fallback`.
- Do not change `PAPER_OPER_START_YMD`.
- Do not change order, fill, ledger, stats, Gate, LOCK, score, broker, or risk behavior.

## Intended Behavior
- Keep `live_vs_bt` gate as not ready when BT sample count is below the required minimum.
- Reflect the condition as a sample-collection/baseline-insufficient state instead of a generic defect.
- Keep FAIL-CLOSED behavior:
  - optimization trigger remains blocked.
  - integrated ops remains non-PASS until sufficient evidence exists.
  - dashboard must not imply operational readiness from this condition.

## Allowed Edits
- Status wording or classification code that distinguishes:
  - stale/missing/malformed artifacts
  - current but insufficient BT aligned sample
- Reporting text in RootA/RootB status artifacts.
- PLANS documentation.

## Disallowed Edits
- Threshold relaxation.
- Summary fallback activation.
- Any hardcoded status or score improvement.
- Any change that causes `bt_too_few_trades_in_window` to become `PASS`.
- Any live trading or broker execution behavior change.

## Required Validation
1. Functional validation:
   - Generate integrated ops snapshot and dashboard state.
2. Consistency validation:
   - Confirm `live_vs_bt_feedback_latest.json run_ymd=20260521`.
   - Confirm `alignment_ready=false` when BT count remains below threshold.
3. Operational reflection validation:
   - Confirm RootB dashboard/ops data display the new classification.
4. Policy validation:
   - Confirm no threshold, fallback, Gate, LOCK, score, broker, order, fill, ledger, or stats behavior changed.
5. FAIL-CLOSED validation:
   - Confirm optimization remains blocked and non-PASS state remains visible.
6. Regression validation:
   - JSON parse checks.
   - Python syntax check for changed Python files.
   - React `prepare-ops` and build if RootB ops data changes.

## Current Decision
- Proceed only with the safe classification split.
- Do not apply threshold/fallback policy relaxation in this step.

## 2026-05-21 Follow-up Verification
- Official BT validation was rerun after the RootA policy/runtime repairs.
- The aligned sample shortage remained:
  - `bt_n=4`
  - `required_min=10`
  - `alignment_ready=false`
  - `reason=bt_too_few_trades_in_window`
- Official full wrapper was rerun after the posthoc temporal-source repair:
  - `run_paper_daily.bat` exit code `0`
  - wrapper status `rc=0`
- Interpretation:
  - The previous posthoc blocked-fill STOP was a code/data-temporal bug and was repaired separately.
  - The remaining BT aligned sample shortage is not a code defect under this policy.
  - It remains a fail-closed baseline insufficiency until enough aligned BT evidence exists or a separate policy change is explicitly approved.
