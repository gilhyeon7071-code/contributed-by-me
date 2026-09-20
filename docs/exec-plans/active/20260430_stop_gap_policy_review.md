# ExecPlan: STOP_GAP policy review

Date: 2026-04-30

## 1. Goal
- Review STOP_GAP loss concentration and define a safe, evidence-based policy change path.
- Current observed data:
  - STOP_GAP rows: 34
  - average return: -11.53%
  - total PnL: -8,253,577 KRW
  - recovered lineage bucket: 16 rows, -5,271,465 KRW, 63.9% of STOP_GAP loss
  - non-recovered bucket: 18 rows, -2,982,112 KRW, 36.1% of STOP_GAP loss

## 2. Non-goals
- No immediate change to `stop_gap_sell_ratio_pct`, stop thresholds, Gate, LOCK, D rule, or broker dispatch.
- No historical PnL rewrite.
- No manual edit of `paper/fills.csv`, `paper/trades.csv`, ledgers, or stats.

## 3. Scope
- Read and evaluate:
  - `E:\1_Data\paper\paper_engine_config.json`
  - `E:\1_Data\paper\trades.csv`
  - `E:\1_Data\paper\fills.csv`
  - STOP_GAP logic in `E:\1_Data\paper_engine.py`
- If a code/config change is later selected, create a separate implementation step with fresh backup.

## 4. Current Evidence
- Current general stop config:
  - `sell_rules.stop_loss.default_pct=-8`
  - `sell_rules.stop_loss.stop_sell_ratio_pct=30`
  - `sell_rules.stop_loss.stop_gap_sell_ratio_pct=70`
  - `sell_rules.stop_loss.preemptive_sell_ratio_pct=40`
- Current surge exit config:
  - `surge_exit_policy.stop_loss_pct=-0.05`
  - `surge_exit_policy.stop_sell_ratio_pct=40`
  - `surge_exit_policy.dynamic_exit_ratio.enabled=true`
- Latest 20260430 STOP_GAP row:
  - `078150`, `sell_ratio_pct=75.0`, `lineage_origin=FRESH_SIGNAL`

## 5. Risks
- Lowering STOP_GAP sell ratio can reduce realized loss but may leave residual position risk.
- Tightening entry filters can reduce gap losses but may reduce candidate count further.
- Changing stop thresholds can alter strategy semantics and must be separated from data lineage fixes.

## 6. Design
- Separate three causes:
  1. historical recovered lineage STOP_GAP rows;
  2. current FRESH_SIGNAL STOP_GAP rows;
  3. surge dynamic exit ratio STOP_GAP rows.
- Do not tune policy using recovered historical rows alone.
- Evaluate current FRESH_SIGNAL rows first, then decide whether a policy change is justified.

## 7. Implementation Steps
1. Produce read-only STOP_GAP diagnostic by lineage, surge flag, sell ratio, date, and severity.
2. Identify whether current FRESH_SIGNAL STOP_GAP rows are dominated by surge dynamic exit or general stop rules.
3. If policy change is justified, define the smallest candidate:
   - general `stop_gap_sell_ratio_pct` adjustment, or
   - surge dynamic exit ratio adjustment, or
   - pre-entry gap risk filter.
4. Back up exact target files before any implementation.

## 8. Verification Plan
1. Syntax verification if code changes.
2. Execution verification through the official script/path affected by the change.
3. Result artifact verification using latest logs/JSON/CSV.
4. Policy verification: confirm only selected STOP_GAP policy changed.
5. FAIL-CLOSED verification: confirm SSOT chain and gates remain PASS.
6. Regression verification: compare rows/counts and ensure no historical rewrite unless explicitly requested.

## 9. Rollback Plan
- Restore any changed target file from the backup made immediately before implementation.
- Re-run the same verification steps and compare core values.

## 10. Evidence To Collect
- Diagnostic table path or command output.
- Config before/after values if changed.
- STOP_GAP counts/PnL by current lineage bucket.
- SSOT chain status.
