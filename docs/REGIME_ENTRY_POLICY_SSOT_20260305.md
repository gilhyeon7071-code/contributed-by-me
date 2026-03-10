# Regime Entry Policy SSOT (2026-03-05)

## Scope
- Engine: `E:\1_Data\paper_engine.py`
- Config: `E:\1_Data\paper\paper_engine_config.json`
- Purpose: Separate operational behavior for `NORMAL / RALLY / CRASH` while keeping core risk controls.

## Decision Inputs
- `p0_daily_check_*.json` -> `crash_risk_off.metrics.day_ret`
- `macro_signal_latest.json` -> `regime`, `risk_on`, `market_metrics.ret1`

## Regime Rules
- `CRASH`
  - Trigger: `macro_regime in {CRASH, RATE_HIKE_FEAR}` OR `day_ret <= crash_day_ret_max`
  - Action: force `max_new=0` when `crash_force_block=true`
- `RALLY`
  - Trigger: `day_ret >= rally_day_ret_min` AND macro not crash/blocked
  - Action:
    - If kill_switch BLOCK (non-hard-data) and `rally_probe_under_kill_switch_block=true`: reopen with `max_new=min(base, rally_probe_max_new)`
    - Tighten runtime caps: gross/daily exposure, sector concentration, gap filters
- `NORMAL`
  - Default engine behavior

## Runtime Overrides in RALLY
- `max_gross_exposure_pct` -> min(current, `rally_max_gross_exposure_pct`)
- `max_daily_new_exposure_pct` -> min(current, `rally_max_daily_new_exposure_pct`)
- `max_per_sector` -> stricter of current vs `rally_max_per_sector`
- `gap_up_max_pct` -> stricter of current vs `rally_gap_up_max_pct`
- `entry_gap_down_stop_pct` -> stronger stop via max(current, `rally_entry_gap_down_stop_pct`)

## Current Config Keys
- `regime_entry_policy.enabled=true`
- `rally_day_ret_min=0.025`
- `crash_day_ret_max=-0.025`\n- `allow_rally_on_macro_volatile=true`\n- `allow_rally_when_macro_risk_off=true`
- `rally_probe_max_new=1`
- `rally_max_per_sector=1`
- `rally_max_gross_exposure_pct=0.60`
- `rally_max_daily_new_exposure_pct=0.15`
- `rally_gap_up_max_pct=0.015`
- `rally_entry_gap_down_stop_pct=0.03`
- `crash_force_block=true`

## Notes
- Relax-ladder cap still applies after regime decision (`L4/L5/L6` safety retained).
- This policy is operationally dynamic; values can be adjusted through config lock tool.

