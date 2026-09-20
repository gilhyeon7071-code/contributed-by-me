# 2026-05-04 Crash Trigger Explicit

## Scope
- RootA paper engine config only.
- Add explicit `crash_risk_off.trigger_max_dd_pct=0.12`.
- Add explicit `crash_risk_off.trigger_day_ret_pct=0.05`.
- Use `tools\paper_engine_config_lock.py` so the config lock is updated with the approved hash.
- No change to Gate, LOCK semantics, D rule, orders, fills, ledger, sizing, take-profit policy, or crash trigger values.

## Backup
- `E:\1_Data\backup\20260504_crash_trigger_explicit\20260504_110256\paper_engine_config.json.bak`
- `E:\1_Data\backup\20260504_crash_trigger_explicit\20260504_110256\paper_engine_config.lock.json.bak`
- `E:\1_Data\backup\20260504_crash_trigger_explicit\20260504_110256\PLANS.md.bak`

## Validation Plan
1. Syntax validation:
   - Parse `paper\paper_engine_config.json`.
   - Parse `paper\paper_engine_config.lock.json`.
   - `py_compile p0_daily_check.py`.
2. Execution validation:
   - Run `tools\paper_engine_config_lock.py status`.
   - Run `_eval_crash_risk_off` with `enabled=false` to verify limits are resolved without market data dependency.
3. Result validation:
   - Confirm `trigger_max_dd_pct=0.12`.
   - Confirm `trigger_day_ret_pct=0.05`.
   - Confirm config hash matches lock approved hash.
4. Policy validation:
   - Confirm only explicit trigger keys were added and values match previous code defaults.
5. FAIL-CLOSED validation:
   - Confirm missing lock mismatch is not introduced.
6. Regression validation:
   - Confirm config and lock JSON parse, status exits `0`, and no order/fill/ledger file was modified.

## Validation Result
- Config lock status before change:
  - `CFG_SHA256=57b91a8ece3624cf6bc0152fa855570a823d84c9d6fbc7de56abb0795a88ecd3`.
  - `APPROVED_SHA256=57b91a8ece3624cf6bc0152fa855570a823d84c9d6fbc7de56abb0795a88ecd3`.
  - `MATCH=True`.
- Applied by lock tool:
  - `tools\paper_engine_config_lock.py set --set crash_risk_off.trigger_max_dd_pct=0.12 --set crash_risk_off.trigger_day_ret_pct=0.05`.
  - Tool backup: `E:\1_Data\paper\paper_engine_config.json.bak_20260504_110339`.
  - Change log: `E:\1_Data\2_Logs\paper_engine_config.change_20260504_110340.json`.
- Config values after change:
  - `trigger_max_dd_pct=0.12`.
  - `trigger_day_ret_pct=0.05`.
  - `fallback_trigger_max_dd_pct=0.35`.
  - `fallback_trigger_day_ret_pct=0.15`.
- Config lock status after change:
  - `CFG_SHA256=039261b0600c07f804a582b9ea996c591ca3706d700b50cf412cb446b14c34b1`.
  - `APPROVED_SHA256=039261b0600c07f804a582b9ea996c591ca3706d700b50cf412cb446b14c34b1`.
  - `MATCH=True`.
- Structural config diff from backup:
  - added `crash_risk_off.trigger_day_ret_pct=0.05`.
  - added `crash_risk_off.trigger_max_dd_pct=0.12`.
- Syntax:
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile .\p0_daily_check.py`: exit `0`.
- Execution:
  - `_eval_crash_risk_off("20260504", cfg_with_enabled_false)` returned:
    - `enabled=False`.
    - `triggered=False`.
    - `metrics_status=disabled_by_config`.
    - limits included `trigger_max_dd_pct=0.12`, `trigger_day_ret_pct=0.05`.
- Source hashes after change:
  - `E:\1_Data\paper\fills.csv`: `66CE895303F6BB270E204AA45C1CDCBDD1411290B1F1C79EAB5F6E553A65BAC9`.
  - `E:\1_Data\paper\orders_20260430_exec.xlsx`: `16EDE48BFE437899AB04FC45F83AC69830CA89F43F1824E257B6867CEF78EA35`.
  - `E:\1_Data\virtual_ledger.csv`: `354FCB478B63F22A94C19AB3518F798A460899B151D591BAD8580C8035FF6C40`.

## Validation Matrix
- Functional validation: PASS; crash trigger limits now resolve from explicit config keys.
- Consistency validation: PASS; added values match the previous code defaults `0.12` and `0.05`.
- Operational reflection validation: PASS; config lock was updated and the change log was written.
- Policy validation: PASS; only explicit keys were added, with no trigger value change.
- FAIL-CLOSED validation: PASS; config hash matches lock approved hash after the change.
- Regression validation: PASS; config/lock parse, `p0_daily_check.py` syntax, lock status, structural diff, and source hash checks passed.
