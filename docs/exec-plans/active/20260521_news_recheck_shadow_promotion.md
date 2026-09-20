# 2026-05-21 news recheck shadow promotion

## Scope
- RootA only.
- Add a read-only shadow artifact that records how `NEWS_ONLY` and missed-move recheck rows would be reviewed if a future promotion policy were considered.
- No order, fill, broker, STOP, LOCK, risk threshold, hard Gate, execution flag, candidate source, or score file is changed.

## Backup
- `E:\1_Data\backup\20260521_news_recheck_shadow_promotion\20260521_122500`

## Reason
- Current runtime has external observation candidates such as `066570`, but they remain non-executable because `execution_pool` and `sector_entry_allowed` are not set.
- Directly promoting them would be a policy change.
- The safe next step is to record a shadow-only review artifact with `trading_effect=false` and `policy_effect=false`.

## Planned Change
- Add `tools/build_news_recheck_shadow_promotion.py`.
- Output:
  - `E:\1_Data\2_Logs\news_recheck_shadow_promotion_latest.json`
  - `E:\1_Data\2_Logs\news_recheck_shadow_promotion_latest.csv`
- Include rows from:
  - `NEWS_ONLY` final-score candidates
  - `RECHECK_MISSED_MOVE_CANDIDATE`
  - `WATCH_MISSED_MOVE_CANDIDATE`
  - `BLOCKED_MOVED_REVIEW`

## Validation Plan
1. Syntax validation.
2. Standalone execution validation.
3. Output artifact validation.
4. Mojibake scan.
5. PLANS record.

## Validation Evidence
- Syntax:
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile E:\1_Data\tools\build_news_recheck_shadow_promotion.py`
  - PASS
- Pre mojibake:
  - `E:\1_Data\2_Logs\mojibake_text_scan_news_recheck_shadow_promotion_20260521_pre.json`
  - `files=2`, `issues=0`, `repairable=0`
- Standalone execution:
  - `E:\1_Data\_runtime\python312-embed\python.exe E:\1_Data\tools\build_news_recheck_shadow_promotion.py`
  - `rows=9`, `shadow_review_ready=9`
- Output:
  - `E:\1_Data\2_Logs\news_recheck_shadow_promotion_latest.json`
  - `trading_effect=false`
  - `policy_effect=false`
  - `policy_change_applied=false`
  - `bucket_counts={"WATCH_MISSED_MOVE_CANDIDATE":4,"RECHECK_MISSED_MOVE_CANDIDATE":4,"NEWS_ONLY":1}`
  - `066570` appears as `NEWS_ONLY` shadow review row, but `shadow_trading_allowed=false`

## Policy Boundary
- The artifact is observational only.
- `shadow_trading_allowed` is always `false`.
- The tool does not write to candidate input files or paper engine files.

## 6 Validation Items
1. Functional validation: PASS
2. Consistency validation: PASS
3. Operational reflection validation: PASS, standalone artifact generation only
4. Policy validation: PASS
5. FAIL-CLOSED validation: PASS
6. Regression validation: PASS

## Remaining Boundary
- The tool is not wired into the live batch chain in this step.
- This does not approve `NEWS_ONLY` or missed-move rows for trading.
- Any actual promotion from this artifact to executable candidate input remains a separate policy change.
