# 2026-05-21 Session-Only Intraday Gate Exception ExecPlan

## Scope
- RootA only.
- Planning only.
- Target issue: `MINQ_20260518_103719` failed to produce BUY chain evidence because normal intraday policy blocked `entry_gate_decision=REDUCE`.
- No code, config, lock, Gate, STOP, D rule, risk policy, score, threshold, broker dispatch, fills, ledger, stats, or dashboard change in this step.
- This plan is not approval to apply or run the exception.

## Evidence
- Failed session:
  - `E:\1_Data\paper\sessions\MINQ_20260518_103719\stats\session_chain_report_MINQ_20260518_103719.json`
- Observed values:
  - `status=FAIL`
  - `reason=no_buy_chain_evidence`
  - `buy_fill_rows=0`
  - `block_evidence.normal_intraday_policy_block=true`
  - `block_evidence.normal_intraday_reason=NORMAL_INTRADAY_ENTRY_GATE_NOT_ALLOW(decision=REDUCE)`
- Code location:
  - `E:\1_Data\paper_engine.py`
  - `_normal_intraday_realtime_block_reason()`
  - caller logs `[NORMAL_INTRADAY_POLICY_BLOCK]`

## Problem Definition
- The previous minimum-quantity session correctly preserved FAIL-CLOSED behavior.
- It also proved that the existing verification path cannot create fresh BUY chain evidence when normal intraday policy blocks `REDUCE`.
- If chain evidence is still required, a narrower session-only exception is needed.

## Non-Negotiable Boundaries
- Do not relax production `main` behavior.
- Do not allow `BLOCK` through.
- Do not allow stale price, missing price, missing `orders_exec`, D mismatch, `as_of/run_id` mismatch, paper/broker mixing, or broker apply.
- Do not write to production fills/trades/state/ledger/stats.
- Do not reset or rewrite historical paper artifacts.

## Candidate Exception Design
The only allowable exception candidate is:

1. `PAPER_RUN_LABEL=validation`
2. non-empty `PAPER_SESSION_ID`
3. `minimum_quantity_verification.enabled=true`
4. `entry_gate_decision=REDUCE`
5. block reason exactly starts with `NORMAL_INTRADAY_ENTRY_GATE_NOT_ALLOW(decision=REDUCE)`
6. max new BUY count `<= 1`
7. max BUY quantity `<= 1`
8. `PAPER_DISABLE_LIVE_BRIDGE_SYNC=1`
9. all session output paths point under `E:\1_Data\paper\sessions\<paper_session_id>\`

The exception must not apply to:

- `entry_gate_decision=BLOCK`
- `entry_gate_decision=CAUTION`
- `RUN_LABEL=main`
- empty `PAPER_SESSION_ID`
- broker apply paths
- production output paths

## Apply Options If Approved
### Option A: Env-only attempt first
- Use `PAPER_CFG_OVERRIDES_JSON` only.
- Try to make the existing `allow_dd_stop_validation_reduce` branch pass if the `entry_gate_reason` contains `validation_reduce` and `position_size_multiplier > 0`.
- No source code change.
- If it still blocks, stop and report.

### Option B: Source patch after Option A fails
- Add a narrowly scoped session-only branch inside `_normal_intraday_realtime_block_reason()`.
- Branch condition must require:
  - `RUN_LABEL=validation`
  - non-empty `PAPER_SESSION_ID`
  - `minimum_quantity_verification.enabled=true`
  - `decision == "REDUCE"`
  - `position_size_multiplier > 0`
- Return empty block reason only for that branch.
- Add explicit log text such as `minimum_quantity_verification_intraday_reduce_exception`.
- Keep default production config unchanged.

## Required Backup Before Apply
- Create backup under:
  - `E:\1_Data\backup\20260521_session_only_intraday_gate_exception_apply\YYYYMMDD_HHMMSS\`
- Back up:
  - `E:\1_Data\paper_engine.py`
  - `E:\1_Data\paper\paper_engine_config.json`
  - `E:\1_Data\paper\paper_engine_config.lock.json`
  - `E:\1_Data\paper\fills.csv`
  - `E:\1_Data\paper\trades.csv`
  - `E:\1_Data\paper\paper_state.json`
  - `E:\1_Data\virtual_ledger.csv`
  - latest relevant `E:\1_Data\2_Logs\*latest*`
  - `E:\1_Data\.agent\PLANS.md`

## Validation Plan
1. Syntax validation:
   - Compile `paper_engine.py` if Option B is applied.
   - Parse JSON config if env/config artifacts are produced.
2. Execution validation:
   - Run one separated session only.
   - Confirm exit code.
3. Result artifact validation:
   - Confirm at most one BUY fill.
   - Confirm BUY quantity is `1`.
   - Confirm session chain report links order, fill, ledger, and stats.
   - Confirm `paper_session_id/run_id/as_of` alignment.
4. Policy validation:
   - Confirm production config and lock are unchanged.
   - Confirm `RUN_LABEL=main` behavior remains blocked.
5. FAIL-CLOSED validation:
   - Confirm `BLOCK` is still blocked.
   - Confirm stale/missing price and paper/broker mixing still block.
6. Regression validation:
   - Compare production fills/trades/state/ledger/log hashes against backup.
   - Confirm only intended PLANS/report files changed outside the isolated session folder.

## Stop Conditions
- Stop if the exception would allow `BLOCK`.
- Stop if production output paths are not isolated.
- Stop if any broker apply path is enabled.
- Stop if more than one BUY or quantity greater than one is possible.
- Stop if production config/lock must be changed to run the session.
- Stop if session artifacts cannot prove separation from production ledger/stats.

## Current Status
- Plan only.
- No exception applied.
- No session run.
- Explicit approval is required before Option A or Option B.
