# RootA Operating Contract

Canonical source files:

- `E:\1_Data\AGENTS.md`
- `E:\1_Data\docs\references\WORK_PROMPT_MANDATORY.md`
- `E:\1_Data\docs\references\FINAL_REPORT_TEMPLATE.md`
- `E:\1_Data\.agent\PLANS.md`

Non-negotiable boundaries:

- Keep `orders(D) -> fills(D) -> ledger -> stats` as the SSOT chain.
- Compute `D` from the latest BUY `ymd` in `paper\fills.csv`; if no BUY exists,
  use the latest `datetime` ymd.
- Stop on missing `orders_exec`, `exec_date != D`, `as_of / run_id` mismatch,
  or paper/broker date mixing.
- Preserve FAIL-CLOSED behavior.
- Do not change Gate, STOP, LOCK, risk, score, threshold, order, fill, ledger,
  or stats semantics from this wiki layer.

This file is a pointer document only. The canonical sources above win if there
is any conflict.

