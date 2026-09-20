# 2026-05-14 Backtest Stable Param Gate Consistency

## Scope
- Address the mismatch where `report_backtest_v41_1.py` consumes `12_Risk_Controlled\stable_params_v41_1.json` even when the stable params are not promoted and have a floor score.
- This plan is for RootA backtest reporting and quality-gate consistency only.
- No live order, broker dispatch, fill, ledger, stats, dashboard, React, score, threshold, LOCK, STOP, or risk-policy change is included.

## Current Evidence
- `12_Risk_Controlled\stable_params_v41_1.json`
  - `promoted=false`
  - `best_score=-100000000.0`
  - `require_macd_golden=1.0`
  - `selection_metrics.worst_fold_pass=false`
  - `selection_metrics.hypertime_reason=worst_fold_below_hurdle`
- `paper\paper_engine_config.json`
  - `stable_params_quality_gate.require_promoted=true`
  - `stable_params_quality_gate.min_oos_trades=20`
  - `stable_params_quality_gate.min_oos_pf=0.75`
  - `stable_params_quality_gate.min_stable_score=-20.0`
- `paper_engine.py`
  - `_stable_params_usable(...)` rejects unpromoted or low-score stable params.
- `generate_candidates_v41_1.py`
  - falls back to default params when stable quality is below gate.
- `report_backtest_v41_1.py`
  - currently reads `stable_params_v41_1.json` directly and does not check `promoted`, `best_score`, or `stable_params_quality_gate`.

## Problem Statement
- Refreshed backtest summary can be produced from a stable-param file that operational candidate and engine paths would reject.
- This can make `report_backtest_summary_v41_1.json` reflect an invalid stable state rather than the active operational fallback behavior.
- The issue is a consumer-path consistency problem, not proof that the stable params are usable.

## Stop Conditions
- Stop if `stable_params_quality_gate` is missing or cannot be parsed.
- Stop if fallback behavior cannot be determined from existing operational code.
- Stop if a proposed code change would alter Gate thresholds, STOP/LOCK meaning, risk policy, order generation, fills, ledger, or stats.
- Stop if verification cannot distinguish display/report changes from source behavior changes.

## Candidate Fix Options

### Option A: Shared Stable Gate With Fallback
- Add the same stable quality gate to `report_backtest_v41_1.py`.
- If stable params fail the gate, use `DEFAULT` report params or a clearly named fallback params set.
- Report metadata must include:
  - `param_source`
  - `stable_gate_ok`
  - `stable_gate_reason`
  - `stable_promoted`
  - `stable_best_score`
- This is closest to operational candidate behavior.

### Option B: Fail-Closed Report Without Fallback
- If stable params fail the gate, do not generate a normal backtest summary.
- Write a blocked status artifact or return non-zero.
- This is stricter, but may break existing batch expectations that rely on summary files.

### Option C: Diagnostic-Only Annotation
- Keep the current params, but add metadata warning that the stable state is invalid.
- This is least disruptive but does not solve active-input inconsistency.

## Preferred Direction
- Prefer Option A only if fallback params can be aligned with existing `generate_candidates_v41_1.py` default behavior without changing policy thresholds.
- If fallback cannot be safely aligned, use Option B or stop for explicit approval.
- Do not use Option C as the final fix unless the user explicitly wants report-only annotation.

## Minimal Implementation Boundary
- Candidate edit file:
  - `E:\1_Data\report_backtest_v41_1.py`
- Optional read-only comparison files:
  - `E:\1_Data\generate_candidates_v41_1.py`
  - `E:\1_Data\paper_engine.py`
  - `E:\1_Data\paper\paper_engine_config.json`
- No config mutation.
- No stable params mutation.
- No threshold mutation.

## Required Validation
1. Syntax validation:
   - `python -m py_compile report_backtest_v41_1.py report_if_due_v41_1.py`
2. Execution validation:
   - Run report generation through the official path.
   - Capture exit code and generated artifact paths.
3. Result validation:
   - Confirm `report_backtest_summary_v41_1.json` contains stable-gate metadata.
   - Confirm behavior when current invalid stable params are present.
   - Confirm `require_macd_golden` in output follows the selected valid source, not the rejected stable file.
4. Policy validation:
   - Confirm no Gate, STOP, LOCK, risk-policy, score, or threshold values changed.
5. FAIL-CLOSED validation:
   - Confirm invalid stable params are not silently treated as valid.
6. Regression validation:
   - Re-run live-vs-BT feedback and trading-stage validation.
   - Confirm final state remains blocked unless refreshed artifacts genuinely pass existing gates.

## Evidence To Report
- Backup path.
- Changed files.
- Stable gate inputs:
  - `promoted`
  - `best_score`
  - OOS trade count/PF
  - configured gate thresholds
- Selected param source.
- Backtest summary `as_of`, OOS `n`, OOS `pf`.
- live-vs-BT `gate_ok`, `quality_gate.ok`, reasons.
- Trading-stage current blockers.

## Current Status
- Option A was implemented in `E:\1_Data\report_backtest_v41_1.py`.
- Current invalid stable params are no longer silently treated as valid by the report path.
- The refreshed summary records `param_gate.param_source=default_fallback`.
- The quality gate remains blocked by existing OOS PF and final-output validation.
