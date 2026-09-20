# 2026-04-30 Candidate Disparity L8 Apply

## Scope
- RootA candidate generation policy only.
- Apply the shadow-validated `disparity_l8_like` candidate expansion to the actual relax ladder.
- Target file: `E:\1_Data\generate_candidates_v41_1.py`.
- No change to value minimum, v_accel, sector rescue, balanced rescue, Gate, LOCK, D rule, orders, fills, ledger, or stats.

## Backup
- `E:\1_Data\backup\20260430_candidate_disparity_l8_apply\20260430_143000\generate_candidates_v41_1.py.bak`
- `E:\1_Data\backup\20260430_candidate_disparity_l8_apply\20260430_143000\PLANS.md.bak`
- `E:\1_Data\backup\20260430_candidate_disparity_l8_apply\20260430_143000\candidates_latest_meta.json.bak`
- `E:\1_Data\backup\20260430_candidate_disparity_l8_apply\20260430_143000\candidates_latest_data.csv.bak`
- `E:\1_Data\backup\20260430_candidate_disparity_l8_apply\20260430_143000\candidates_latest_data.filtered.csv.bak`
- `E:\1_Data\backup\20260430_candidate_disparity_l8_apply\20260430_143000\candidates_v41_1_20260429.csv.bak`
- `E:\1_Data\backup\20260430_candidate_disparity_l8_apply\20260430_143000\candidates_latest.csv.bak`

## Change
- L7 `disparity20_max`: `1.18` -> `1.25`
- L7 `disparity60_max`: `1.26` -> `1.36`

## Validation Plan
1. Syntax validation:
   - `py_compile generate_candidates_v41_1.py`
2. Execution validation:
   - Run official candidate generator.
3. Result validation:
   - Confirm candidate latest meta and CSV artifacts are regenerated.
   - Confirm selected ladder params include the new L7 disparity limits when L7 is chosen.
4. Policy validation:
   - Confirm no value_min, v_accel, sector rescue, Gate, LOCK, D rule, order, fill, ledger, or stats policy was changed.
5. FAIL-CLOSED validation:
   - Confirm order/fill/ledger files are unchanged by candidate generation.
6. Regression validation:
   - Confirm generated candidate artifacts are non-empty and parseable.

## Validation Evidence
- Syntax validation:
  - `py_compile generate_candidates_v41_1.py` exit `0`.
- Execution validation:
  - Official candidate generator exit `0`.
- Result validation:
  - `E:\1_Data\2_Logs\candidates_latest_meta.json`
    - `as_of=2026-04-30 14:28:42`
    - `latest_date=2026-04-29`
    - `chosen_level=L7`
    - `chosen_params.disparity20_max=1.25`
    - `chosen_params.disparity60_max=1.36`
    - `attempts[-1].diag.all_pass=6`
    - `execution_pool.natural_pass_count=6`
    - `execution_pool.balanced_rescue_count=5`
    - `execution_pool.before_union=11`
    - `execution_pool.after_union=12`
  - `E:\1_Data\2_Logs\candidates_latest_data.csv`: `12` rows.
  - `E:\1_Data\2_Logs\candidates_latest_data.filtered.csv`: `12` rows.
  - `E:\1_Data\2_Logs\candidates_v41_1_20260429.csv`: `12` rows.
  - `E:\1_Data\2_Logs\candidates_latest.csv`: `candidates_v41_1_20260429.csv`.
- Policy validation:
  - Backup-to-current compare for `generate_candidates_v41_1.py` shows only:
    - line `3261`: L7 `disparity20_max` `1.18` -> `1.25`
    - line `3262`: L7 `disparity60_max` `1.26` -> `1.36`
- FAIL-CLOSED validation:
  - `E:\1_Data\paper\fills.csv` hash unchanged: `D1E3207E4C28EFE1200F8285EFEE22619291633D1960D52DC9D9F528F939E11E`
  - `E:\1_Data\paper\trades.csv` hash unchanged: `5AEE8683943892238FCB968DC620C4C2C3D7EAB3F0818E6E01CED0A6DA7024D9`
  - `E:\1_Data\virtual_ledger.csv` hash unchanged: `354FCB478B63F22A94C19AB3518F798A460899B151D591BAD8580C8035FF6C40`
- Regression validation:
  - Candidate JSON parsed.
  - Candidate CSV outputs parsed and are non-empty.
  - Generated `E:\1_Data\__pycache__\generate_candidates_v41_1.cpython-312.pyc` removed after validation.

## Validation Matrix
- Functional validation: PASS.
- Consistency validation: PASS.
- Operational reflection validation: PASS for candidate generation artifacts.
- Policy validation: PASS.
- FAIL-CLOSED validation: PASS.
- Regression validation: PASS.

## Full Batch Verification Addendum
- Backup:
  - `E:\1_Data\backup\20260430_candidate_disparity_full_batch_verify\20260430_143500`
- Official execution:
  - `E:\1_Data\run_paper_daily.bat` exit `0`.
  - `E:\1_Data\2_Logs\run_paper_daily_last.txt` contains `[OK] finished` and `[WRAPPER_EXIT] rc=0`.
- Batch candidate evidence:
  - `E:\1_Data\2_Logs\candidates_latest_meta.json`
    - `as_of=2026-04-30 14:36:02`
    - `latest_date=2026-04-29`
    - `chosen_level=L7`
    - `chosen_params.disparity20_max=1.25`
    - `chosen_params.disparity60_max=1.36`
    - `attempts[-1].diag.all_pass=3`
    - `execution_pool.natural_pass_count=3`
    - `execution_pool.balanced_rescue_count=5`
    - `execution_pool.before_union=8`
    - `execution_pool.after_union=12`
  - `E:\1_Data\2_Logs\candidates_v41_1_20260429.csv`: `12` rows.
  - `E:\1_Data\2_Logs\candidates_latest_data.csv`: `11` rows after liquidity filter.
  - origin counts after liquidity filter:
    - `NATURAL_PASS=3`
    - `BALANCED_RESCUE=5`
    - `SECTOR_PREFILTER_UNION=3`
- SSOT / operational evidence:
  - `E:\1_Data\paper\orders_20260430_exec.xlsx`: `5` rows, `exec_date_unique=['20260430']`.
  - `E:\1_Data\paper\fills.csv`: `430` rows, latest BUY `ymd=20260430`, D rule latest `ymd=20260430`.
  - `E:\1_Data\2_Logs\execution_safety_contract_latest.json`: `status=WARN`, `D=20260430`, orders `5`, decisions `ALLOW=4`, `BLOCK=1`.
  - `E:\1_Data\2_Logs\recovery_ssot_chain_latest.json`: `status=PASS`, `D=20260430`, `issues={}`.
  - `E:\1_Data\2_Logs\ledger_live_fills_dry_run_latest.json`: `status=PASS`.
  - `E:\1_Data\2_Logs\validation_scope_audit_latest.json`: `status=PASS`.
  - `E:\1_Data\2_Logs\nightly_data_integrity_latest.json`: `ok=true`, `issues={}`.
  - `E:\1_Data\2_Logs\paper_pending_report_20260430_143831.json`: `verdict.ok=true`, `pending_total=6`, `pending_blocking_count=0`.
  - `E:\1_Data\2_Logs\trading_stage_validation_latest.json`: paper stage `운영가능`; overall `실전준비 조건부`.
- Cleanup:
  - Generated `E:\1_Data\__pycache__\generate_candidates_v41_1.cpython-312.pyc` removed after validation.

## Full Batch Validation Matrix
- Functional validation: PASS.
- Consistency validation: PASS.
- Operational reflection validation: PASS.
- Policy validation: PASS.
- FAIL-CLOSED validation: PASS.
- Regression validation: PASS.

## Remaining
- `execution_safety_contract_latest.json` remains `WARN` because one order decision was `BLOCK`.
- Trading-stage overall remains `실전준비 조건부`; paper stage itself is `운영가능`.
