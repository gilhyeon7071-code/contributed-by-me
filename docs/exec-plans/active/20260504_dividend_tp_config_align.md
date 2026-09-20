# 2026-05-04 Dividend TP Config Align

## Scope
- RootA paper engine config only.
- Keep current dividend-stock behavior: no take-profit before 30%.
- Align confusing config with current behavior:
  - `sell_rules.asset_type_rules.배당주.take_profit_levels`: `[8, 18, 30]` -> `[30]`.
  - `sell_rules.asset_type_rules.배당주.take_profit_ratios`: `[30, 40, 30]` -> `[30]`.
- Use `tools\paper_engine_config_lock.py` so the config lock is updated with the approved hash.
- No change to `paper_engine.py`, Gate, LOCK semantics, D rule, orders, fills, ledger, sizing, crash policy, or actual dividend TP behavior.

## Backup
- `E:\1_Data\backup\20260504_dividend_tp_config_align\20260504_110951\paper_engine_config.json.bak`
- `E:\1_Data\backup\20260504_dividend_tp_config_align\20260504_110951\paper_engine_config.lock.json.bak`
- `E:\1_Data\backup\20260504_dividend_tp_config_align\20260504_110951\PLANS.md.bak`

## Validation Plan
1. Syntax validation:
   - Parse `paper\paper_engine_config.json`.
   - Parse `paper\paper_engine_config.lock.json`.
   - `py_compile paper_engine.py`.
2. Execution validation:
   - Run `tools\paper_engine_config_lock.py status`.
   - Import `paper_engine` and evaluate dividend TP plan/adjusted ratio.
3. Result validation:
   - Confirm dividend TP levels are `[30]`.
   - Confirm dividend TP ratios are `[30]`.
   - Confirm adjusted dividend TP ratio at 30% is `15.0`, same as before.
   - Confirm 8% and 18% dividend TP levels are absent from config.
4. Policy validation:
   - Confirm behavior is aligned to existing 30%-only policy, not changed to earlier TP.
5. FAIL-CLOSED validation:
   - Confirm config hash matches lock approved hash.
6. Regression validation:
   - Confirm config and lock JSON parse, status exits `0`, and no order/fill/ledger file was modified.

## Validation Result
- Config lock status before change:
  - `CFG_SHA256=039261b0600c07f804a582b9ea996c591ca3706d700b50cf412cb446b14c34b1`.
  - `APPROVED_SHA256=039261b0600c07f804a582b9ea996c591ca3706d700b50cf412cb446b14c34b1`.
  - `MATCH=True`.
- Applied by lock tool:
  - `tools\paper_engine_config_lock.py set --set sell_rules.asset_type_rules.배당주.take_profit_levels=[30] --set sell_rules.asset_type_rules.배당주.take_profit_ratios=[30]`.
  - Tool backup: `E:\1_Data\paper\paper_engine_config.json.bak_20260504_111039`.
  - Change log: `E:\1_Data\2_Logs\paper_engine_config.change_20260504_111040.json`.
- Config values after change:
  - `배당주.take_profit_levels=[30]`.
  - `배당주.take_profit_ratios=[30]`.
  - `배당주.trailing_stop_pct=-8`.
  - `배당주.dividend_yield_floor=3.0`.
  - `배당주.max_hold_days=30`.
- Config lock status after change:
  - `CFG_SHA256=d2163acd513f4d36639044f9dbc0dd6415f202417040124463d3ac48c6fd9b58`.
  - `APPROVED_SHA256=d2163acd513f4d36639044f9dbc0dd6415f202417040124463d3ac48c6fd9b58`.
  - `MATCH=True`.
- Intended config changes:
  - `배당주.take_profit_levels`: `[8, 18, 30]` -> `[30]`.
  - `배당주.take_profit_ratios`: `[30, 40, 30]` -> `[30]`.
  - After replacing those two intended fields for comparison, backup/current config matched: `only_intended_changes=True`.
- Syntax:
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile .\paper_engine.py`: exit `0`.
- Execution:
  - Imported `paper_engine`.
  - `_resolve_take_profit_plan("배당주", sell_rules, tp_cfg)` returned levels `[30]`, ratios `[30]`.
  - `_adjust_take_profit_ratio(30, "배당주", 30, tp_cfg)` returned `15.0`.
  - `has_8_or_18=False`.
- Source hashes after change:
  - `E:\1_Data\paper\fills.csv`: `66CE895303F6BB270E204AA45C1CDCBDD1411290B1F1C79EAB5F6E553A65BAC9`.
  - `E:\1_Data\paper\orders_20260430_exec.xlsx`: `16EDE48BFE437899AB04FC45F83AC69830CA89F43F1824E257B6867CEF78EA35`.
  - `E:\1_Data\virtual_ledger.csv`: `354FCB478B63F22A94C19AB3518F798A460899B151D591BAD8580C8035FF6C40`.

## Validation Matrix
- Functional validation: PASS; dividend TP config now contains only the active 30% level.
- Consistency validation: PASS; current behavior remains 30% TP with adjusted ratio `15.0`.
- Operational reflection validation: PASS; config lock was updated and the change log was written.
- Policy validation: PASS; no new earlier dividend TP was enabled.
- FAIL-CLOSED validation: PASS; config hash matches lock approved hash after the change.
- Regression validation: PASS; config/lock parse, `paper_engine.py` syntax, lock status, intended-change comparison, and source hash checks passed.
