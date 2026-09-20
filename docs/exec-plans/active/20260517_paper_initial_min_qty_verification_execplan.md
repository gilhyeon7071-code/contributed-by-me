# 2026-05-17 Paper Initial Minimum-Quantity Verification ExecPlan

## Scope
- RootA only.
- Planning for a possible separated minimum-quantity paper verification session.
- No code, config, lock, Gate, STOP, D rule, risk policy, score, threshold, broker dispatch, fills, ledger, stats, or dashboard change in this step.
- This plan is not approval to run the verification session.

## Problem Definition
- Current production buy logic and current paper-chain verification must stay separate.
- If production entry remains blocked, the order-fill-ledger-stats chain can lack fresh BUY evidence.
- The documented initial paper-stage handling is not to relax production Gate or risk policy, but to use a separated verification session when explicitly approved.

## Decision Rule
- Production policy path:
  - Keep normal Gate, STOP, LOCK, D rule, risk orchestration, and order-selection policy unchanged.
  - Continue buy-logic validation with real production decision artifacts.
- Verification-session path:
  - Use only for proving the chain `orders(D) -> fills(D) -> ledger -> stats`.
  - Must use a distinct `paper_session_id` or `run_id`.
  - Must be isolated from production paper ledger, production stats, broker apply, and RootB operational data.

## Preconditions Before Any Apply
- User explicitly approves the session scope, date rule, and maximum BUY quantity.
- Backup target is created under:
  - `E:\1_Data\backup\20260517_paper_initial_min_qty_session\YYYYMMDD_HHMMSS\`
- Back up all potentially touched artifacts before any run:
  - `E:\1_Data\paper\paper_engine_config.json`
  - `E:\1_Data\paper\paper_engine_config.lock.json`
  - `E:\1_Data\paper\fills.csv`
  - `E:\1_Data\paper\trades.csv`
  - `E:\1_Data\paper\paper_state.json`
  - `E:\1_Data\virtual_ledger.csv`
  - latest relevant `E:\1_Data\2_Logs\*paper*latest*.json`
  - latest relevant `E:\1_Data\2_Logs\*ledger*latest*.json`
  - `E:\1_Data\2_Logs\run_paper_daily_last.txt`
  - `E:\1_Data\.agent\PLANS.md`

## Apply Design If Approved
1. Create a new separated verification identity:
   - `paper_session_id=MINQ_YYYYMMDD_HHMMSS`
   - `run_label=validation`
2. Select at most one eligible candidate.
3. Allow only the approved minimum BUY quantity for that session.
4. Keep stale price, missing price, missing `orders_exec`, D mismatch, `as_of/run_id` mismatch, and paper/broker mixing fail-closed.
5. Write session artifacts under:
   - `E:\1_Data\paper\sessions\<paper_session_id>\`
6. Keep production artifacts unchanged unless a separate explicit production update is approved.
7. Do not reset, rewrite, or migrate existing production fills/trades/ledger/history for convenience.

## Validation Plan
1. Syntax validation:
   - Compile any touched Python files.
   - Parse any touched JSON/lock files.
2. Execution validation:
   - Run only the approved separated verification path.
   - Confirm command return code and session log.
3. Result artifact validation:
   - Confirm one linked BUY order, fill, ledger row, and stats row for the session.
   - Confirm chain report for `orders(D) -> fills(D) -> ledger -> stats`.
   - Confirm `paper_session_id/run_id/as_of` alignment.
   - Confirm no paper/broker family mixing.
4. Policy validation:
   - Confirm production Gate, STOP, LOCK, D rule, score, thresholds, risk policy, and broker apply semantics are unchanged.
5. FAIL-CLOSED validation:
   - Confirm missing/stale price, missing `orders_exec`, date mismatch, and paper/broker mixing still block.
6. Regression validation:
   - Compare production fills/trades/state/ledger hashes against backup.
   - Confirm production latest stats are not silently overwritten.

## Stop Conditions
- Stop if user approval is not explicit.
- Stop if `orders_exec` is missing and the session date rule is not explicitly documented.
- Stop if `exec_date != D` without a documented separated-session exception.
- Stop if `as_of`, `run_id`, or `paper_session_id` are inconsistent.
- Stop if paper and broker rows are mixed.
- Stop if running the session requires changing production Gate, STOP, LOCK, D rule, score, threshold, or risk-policy meaning.

## Current Status
- Plan only.
- No session run.
- No code/config/data/policy change.
- Next action requires explicit user approval before apply.
