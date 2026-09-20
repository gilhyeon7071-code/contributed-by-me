# 2026-05-21 news recheck shadow policy eval

## Scope
- RootA only.
- Add a read-only evaluator for draft promotion policies using `news_recheck_shadow_promotion_latest.json`.
- No order, fill, broker, STOP, LOCK, risk threshold, hard Gate, candidate input, score file, or execution flag is changed.

## Backup
- `E:\1_Data\backup\20260521_news_recheck_shadow_policy_eval\20260521_123650`

## Reason
- `NEWS_ONLY` and missed-move candidates can be observed, but actual promotion is a policy change.
- The safe next step is to compare strict and broad draft rules without allowing trading.

## Planned Change
- Add `tools/evaluate_news_recheck_shadow_policy.py`.
- Output:
  - `E:\1_Data\2_Logs\news_recheck_shadow_policy_eval_latest.json`
  - `E:\1_Data\2_Logs\news_recheck_shadow_policy_eval_latest.csv`
- Draft policies:
  - `NEWS_ONLY_STRICT_REVIEW`
  - `MISSED_MOVE_STRICT_REVIEW`
  - `MISSED_MOVE_BROAD_REVIEW`

## Policy Boundary
- This is policy review only.
- `trading_allowed=false`, `policy_effect=false`, and `policy_change_applied=false` are fixed in the output.
- Actual promotion remains a separate policy change.

## Validation Evidence
- Syntax:
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile E:\1_Data\tools\evaluate_news_recheck_shadow_policy.py`
  - PASS
- Pre mojibake:
  - `E:\1_Data\2_Logs\mojibake_text_scan_news_recheck_shadow_policy_eval_20260521_pre.json`
  - `files=2`, `issues=0`, `repairable=0`
- Standalone execution:
  - `E:\1_Data\_runtime\python312-embed\python.exe E:\1_Data\tools\evaluate_news_recheck_shadow_policy.py`
  - `source_rows=9`, `eval_rows=9`
  - `policy_counts={"NEWS_ONLY_STRICT_REVIEW":1,"MISSED_MOVE_STRICT_REVIEW":3,"MISSED_MOVE_BROAD_REVIEW":5}`
  - `unique_codes=["032580","036710","066570","080220","119830"]`
- Output:
  - `E:\1_Data\2_Logs\news_recheck_shadow_policy_eval_latest.json`
  - `trading_effect=false`
  - `policy_effect=false`
  - `policy_change_applied=false`
  - all rows keep `trading_allowed=false`
- Final mojibake:
  - `E:\1_Data\2_Logs\mojibake_text_scan_news_recheck_shadow_policy_eval_20260521_final.json`
  - `files=3`, `issues=0`, `repairable=0`

## 6 Validation Items
1. Functional validation: PASS
2. Consistency validation: PASS
3. Operational reflection validation: PASS, standalone artifact generation only
4. Policy validation: PASS
5. FAIL-CLOSED validation: PASS
6. Regression validation: PASS

## Remaining Boundary
- The evaluator is not wired into the live batch chain.
- The evaluated rules are not approved trading rules.
- Actual promotion from this artifact to executable candidate input remains a separate policy change.
