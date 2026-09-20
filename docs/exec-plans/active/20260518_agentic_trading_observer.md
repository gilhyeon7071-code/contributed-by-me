# 2026-05-18 Agentic Trading Observer ExecPlan

## Scope

- RootA only.
- Add a read-only observer that maps useful agentic-trading ideas into current RootA evidence.
- Files in scope: `tools\build_agentic_trading_observer.py`, `run_agentic_trading_observer.bat`, generated observer artifacts, `.agent\PLANS.md`.
- No order, fill, ledger, stats, Gate, STOP, LOCK, score, threshold, risk policy, broker dispatch, or daily batch behavior changes.

## Plan

1. Read current RootA artifacts only.
2. Generate deterministic Planner / Alpha / Risk / News / Memory / Audit role summaries.
3. Surface STOP concerns and policy boundaries without turning them into approval.
4. Write JSON and Markdown evidence artifacts.
5. Verify syntax, execution, output artifacts, and mojibake scan.

## Completion Rule

- This is complete only for read-only observer scope after syntax, execution, output, and string-scan validation pass.
- It is not trading approval and not an execution-path change.
