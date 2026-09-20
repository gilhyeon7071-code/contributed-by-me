# 2026-05-26 Normal Entry Baseline Policy Separation ExecPlan

## Scope
- RootA only: `E:\1_Data`.
- Purpose: define the policy-verification baseline before any operating-policy change.
- This plan does not change code, config, thresholds, gates, orders, fills, ledger, broker dispatch, or dashboard output.

## Background Evidence
- `E:\1_Data\paper\trades_calc.csv`
  - total rows checked: 399
  - `DELAYED_OR_FALLBACK`: 36 rows, average return about `+0.40%`, win rate about `38.9%`
  - `INTRADAY_NON_SURGE`: 40 rows, average return about `-3.08%`, win rate about `17.5%`
  - `INTRADAY_REDUCED`: 20 rows, average return about `-0.27%`, win rate about `25.0%`
  - `INTRADAY_SURGE_IMMEDIATE`: 143 rows, average return about `-2.49%`, win rate about `23.8%`
  - `LEGACY_REPLAY_UNCLEAR`: 62 rows, positive but not usable as a clean current-policy baseline.
  - `UNKNOWN_OR_LEGACY`: 98 rows, excluded from policy proof.
- `E:\1_Data\paper\fills.csv`
  - BUY rows checked: 272
  - main execution metadata is mostly inside `note`, not normalized columns.
- `E:\1_Data\2_Logs\surge_realtime_shadow_guard_latest.json`
  - recommendation status: `SHADOW_ONLY`
  - primary condition: `surge_immediate`
  - primary mode: `shadow_block`
- `E:\1_Data\2_Logs\gradual_reduction_axis_review_latest.json`
  - recommendation: `DO_NOT_APPLY_POLICY_REDUCTION_YET`
  - first axis: `DATA_COMPLETENESS_AND_DUPLICATE_REVIEW`
  - `policy_effect=false`
  - `policy_change_applied=false`

## Problem Statement
Current performance evidence mixes at least five different policy strata:
- normal delayed/fallback entries
- normal intraday realtime entries
- surge immediate entries
- reduced-size entries
- shadow/probe/observe/review-only rows

Using this mixed evidence to tune entry thresholds, risk gates, or minimum-entry sample policy would create circular validation. The first required step is not relaxing gates or waiting for more mixed samples. The first required step is to separate the baseline used for policy verification.

## Baseline Classification Rule

### Include In Normal Baseline Candidate
- `entry_timing=same_close`
- `fallback_stage=0(close_auction)`
- `fallback_stage=1(next_open_limit)`
- `fallback_stage=2(intraday_limit)`, but only as a small-sample sub-bucket.
- actual BUY/SELL/trade rows with concrete fill evidence.

### Separate Policy Group
- `entry_timing=intraday_realtime`
- `fallback_stage=0(intraday_realtime)`
- non-surge intraday realtime rows.

### Isolate / Do Not Use For Normal Baseline Proof
- `surge_immediate=1`
- `surge_type=*` realtime rows
- `validation_reduce`
- `paper_probe`
- `shadow_*`
- `observe_only`
- `policy_review_only_no_trading_effect`
- `candidate_action_review_*`
- `WAIT_LOB observe`
- `split_entry=2nd` or later when evaluating new-entry quality.
- `overheat_reduce` and `sector_hrp_reduce` rows when evaluating original unmodified entry quality.
- legacy/replay/unknown rows without enough runtime metadata.

## Proposed Policy Direction
1. Treat `same_close / close_auction / next_open_limit` as the normal baseline candidate.
2. Treat `intraday_realtime` as a separate policy group, not as normal entry evidence.
3. Treat `surge_immediate` as an isolated high-risk group. It may continue shadow logging, but should not support normal-entry threshold proof.
4. Keep defensive gates (`kill_switch`, DDM, production risk, risk orchestration hard/zero-size causes) separate from alpha-entry calibration.
5. Do not use shadow/probe/observe rows as direct policy proof.

## Implementation Boundary
This plan is only a prerequisite document. Any later implementation must be scoped separately and must not be described as a bug fix if it changes operating behavior.

Possible later implementation options:
- add a read-only normalized entry-policy label report from `fills.csv` and `trades_calc.csv`;
- add normalized labels to future artifacts without changing trading behavior;
- add an operating policy only after explicit approval, backup, and full validation.

## Explicit Non-Goals
- No risk gate relaxation.
- No DDM threshold relaxation.
- No production-risk HARD or BLOCK meaning change.
- No broker dispatch change.
- No order/fill/ledger mutation.
- No hardcoded PASS/status improvement.
- No use of shadow/probe samples as normal policy proof.

## Validation Requirements For Any Future Code Change
Any future implementation under this plan must pass:
1. Functional validation: label classification or policy separation works on real artifacts.
2. Consistency validation: row counts reconcile with source `fills.csv` / `trades_calc.csv`.
3. Operational reflection validation: official runtime artifact or official batch path reflects the change.
4. Policy validation: policy effect is explicitly reported as true or false.
5. FAIL-CLOSED validation: excluded or unknown rows do not create entry permission.
6. Regression validation: existing gates, D rule, orders, fills, ledger, and stats are not silently changed.

## Current Status
- Status: PLAN ONLY.
- trading_effect: false
- policy_effect: false
- policy_change_applied: false
- No source/config/runtime artifact was modified by this plan.

## Next Valid Step
Create a read-only normalized policy-label report for existing `fills.csv` and `trades_calc.csv`, then use that report to confirm the clean baseline sample before any operating-policy proposal.
