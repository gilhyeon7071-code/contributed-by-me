# 2026-04-30 Low Turnover Validation Profile

## Scope
- Add a validation-only low-turnover profile to `run_backtest_validation_real.bat`.
- Set daily gross traded notional cap to `1%` of capital through strategy params.
- Do not change the acceptance threshold `monthly_turnover_limit=0.20`.
- Do not change Gate, LOCK, D rule, broker mode, order/fill/ledger chain, or live trading execution.

## Backup
- `E:\1_Data\backup\20260430_low_turnover_validation_profile\20260430_130850`

## Plan
1. Add a param-driven daily turnover cap in `tools\backtest_real_strategy_adapter.py`.
2. Add `daily_gross_turnover_cap_pct: 0.01` to `run_backtest_validation_real.bat` params.
3. Run syntax validation.
4. Run `run_backtest_validation_real.bat`.
5. Verify latest JSON artifacts and record PASS/FAIL.

## Validation
- Syntax:
  - `python -m py_compile tools\backtest_real_strategy_adapter.py`
  - PASS.
- Direct cap reproduction:
  - `daily_gross_turnover_cap_pct=0.01`
  - `monthly_turnover_capped=0.1831094`
  - `max_daily_turnover=0.01`
  - `annual_turnover=2.1973128`
- Official backtest validation batch:
  - `run_backtest_validation_real.bat`
  - non-zero exit remained because the framework still returns NO_GO.
  - Latest `backtest_validation_latest.json`:
    - `acceptance_pnl_turnover passed=True`
    - `turnover_monthly=0.1831094`
    - `turnover_limit_monthly=0.2`
    - `base_meta.execution.daily_gross_turnover_cap_pct=0.01`
  - Latest final output:
    - `final_gate_decision=NO_GO`
    - `checklist total=18 pass=10 fail=0 not_evaluable=8`
- Mojibake:
  - full repo scan still reports existing issues.
  - touched target files had `target_issue_files=0`.

## Remaining
- Overall backtest NO_GO is no longer caused by `acceptance_pnl_turnover`.
- Remaining blockers are data/sample sufficiency and not-evaluable gates:
  - `look_ahead_proxy`
  - `signal_quality_ic_ir`
  - `walk_forward`
  - `market_regime_response`
  - `historical_scenario_response`
  - `temporal_consistency`
  - `cpcv_pbo`
