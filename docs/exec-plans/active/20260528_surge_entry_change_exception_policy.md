# 2026-05-28 Surge ENTRY_CHANGE_BLOCK Conditional Exception Policy Plan

## Status
- Status: PLAN ONLY
- trading_effect: false
- policy_effect: false
- policy_change_applied: false
- Root: `E:\1_Data`

## Scope
- Define a narrow policy-change candidate for `ENTRY_CHANGE_BLOCK` only.
- Do not change `max_entry_change_pct`, Gate, LOCK, DDM, risk orchestration, order routing, fills, ledger, broker dispatch, or dashboard approval state in this plan.
- Do not use this plan as entry approval.

## Problem Statement
- Confirmed surge trading stopped after the last surge trade entry date `2026-05-20`.
- Detector did not stop: `2026-05-21` onward still had `172` post-transition date-code first-detected surge events.
- Post-transition entry allowance dropped to `0` in the audited unique-event population.
- `ENTRY_CHANGE_BLOCK` is the first blocker with expected-value conflict evidence.

## Evidence
- `E:\1_Data\2_Logs\surge_no_trade_transition_audit_latest.json`
  - detector snapshots audited: `2206`
  - date-code first-detected surge events: `350`
  - pre last-trade events: `178`
  - post no-trade events: `172`
  - pre inferred/explicit allowed events: `178`
  - post allowed events: `0`
- `E:\1_Data\2_Logs\surge_entry_change_block_audit_latest.json`
  - `ENTRY_CHANGE_BLOCK` post events: `34`
  - 15m-evaluable rows: `5`
  - avg 15m return: `+1.888085%`
  - median 15m return: `+1.057402%`
  - 15m win rate: `60%`
  - profit missed: `3`
  - loss avoided: `1`
- `E:\1_Data\2_Logs\surge_entry_change_conditional_rule_audit_latest.json`
  - base conditional candidates: `1`
  - strict conditional candidates: `1`
  - conditional candidate `001740` at `2026-05-27T09:17:54`
  - change pct: `+21.7033%`
  - LOB: `OK`
  - spread: `16.3132 bps`
  - high drawdown: `-0.0752%`
  - RVOL20: `1.06249`
  - 15m return: `+5.050505%`
  - 30m/EOD return: `+3.496503%`
- `E:\1_Data\2_Logs\surge_entry_change_threshold_sensitivity_latest.json`
  - simple cap raise from `16%` to `20%` newly unblocks `25` rows
  - simple cap raise from `16%` to `22%` newly unblocks `33` rows
  - conditional exception admits `1` controlled row

## Interpretation
- The evidence does not support raising the global `max_entry_change_pct` from `16%`.
- A narrow exception path is more consistent with controlled expected-value design.
- The proposed exception is not a bypass because it requires independent risk controls to pass.
- The largest practical constraint is LOB quality: `33/34` `ENTRY_CHANGE_BLOCK` rows failed `LOB OK`.

## Proposed Policy Candidate
Keep global cap unchanged:
- `max_entry_change_pct = 0.16`

Add a separate exception candidate only when all of these are true:
1. Current blocker is `ENTRY_CHANGE_BLOCK` only, or `ENTRY_CHANGE_BLOCK` with no other hard blocker.
2. `is_realtime_surge=true`.
3. `lob_available=true` and `lob_status=OK`.
4. `spread_bps <= 20` for strict mode. A looser review mode may record `spread_bps <= 40`, but should not enter without explicit approval.
5. `intraday_high_drawdown_pct > -0.025`.
6. `rvol20 <= 5`.
7. Not `surge_score_final >= 90 and rvol20 >= 3`.
8. KRX hard warnings, news negative blocks, trading value hard floor, ATR cap, orderflow hard blocks, and feature-missing blocks remain hard blocks.
9. Output must mark the row as policy-candidate or paper-shadow first, not live approval.

## Proposed Runtime Behavior If Later Approved
Phase 1, read-only candidate emission:
- Add a field such as `entry_change_exception_candidate=true/false`.
- Keep `entry_allowed=false`.
- Write a dated/latest audit artifact.
- Do not create paper orders.

Phase 2, paper-only micro route, separate approval required:
- Allow only micro-sized paper entries.
- Require explicit stop/timebox contract.
- Keep broker/live route disabled.

Phase 3, live route, not in current scope:
- Requires separate approval, larger sample, slippage evidence, broker-market baseline, and fail-closed regression.

## Non-Goals
- No global `max_entry_change_pct` raise.
- No relaxation of `HIGH_REJECTION_ENTRY_BLOCK`.
- No relaxation of `NO_LOB_BLOCK`.
- No relaxation of RVOL or score/RVOL overheat blocks.
- No relaxation of KRX, news, feature, orderflow, DDM, production risk, P1 gate, or kill switch.
- No change to order, fill, ledger, broker, or dashboard approval state.

## Required Validation For Any Later Code Change
1. Functional validation: generated candidate artifact and expected fields.
2. Consistency validation: exception candidate rows match the exact rule above.
3. Operational reflection validation: official intraday loop produces the artifact after restart or normal reload.
4. Policy validation: `entry_allowed`, order route, and broker/live flags remain unchanged unless separately approved.
5. FAIL-CLOSED validation: missing LOB, stale data, high-rejection, RVOL overheat, KRX hard block, orderflow risk, or feature gap must block the exception.
6. Regression validation: existing surge detector, sanity labeler, wait-LOB observe, EV shadow, and dispatch artifacts remain compatible.

## Current Decision
- Apply no code or policy change now.
- Preferred next implementation, if explicitly approved, is Phase 1 read-only candidate emission only.
- Actual paper/live entry approval remains out of scope.
