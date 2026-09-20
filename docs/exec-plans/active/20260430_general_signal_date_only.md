# 2026-04-30 General Signal Date Only

## Scope
- RootA paper sync only.
- General signal rows must not use hour/minute/second from `paper\fills.csv.datetime`.
- Surge rows keep intraday timestamp behavior.
- Target file: `E:\1_Data\paper_sync.py`.
- No change to Gate, LOCK, D rule, orders, fills, paper engine entry/exit generation, broker/KIS source files, or Surge logic.

## Backup
- `E:\1_Data\backup\20260430_general_signal_date_only\20260430_173000\paper_sync.py.bak`
- `E:\1_Data\backup\20260430_general_signal_date_only\20260430_173000\PLANS.md.bak`
- `E:\1_Data\backup\20260430_general_signal_date_only\20260430_173000\trades_calc.csv.bak`
- `E:\1_Data\backup\20260430_general_signal_date_only\20260430_173000\fills_norm.csv.bak`
- `E:\1_Data\backup\20260430_general_signal_date_only\20260430_173000\paper_pnl_summary_last.json.bak`

## Change
- In `paper_sync.py`, classify Surge rows by note:
  - `surge_immediate=1`
  - non-empty `surge_type=...`
- For non-Surge rows, normalize parsed `datetime` to the date only at `00:00:00`.
- For Surge rows, preserve existing intraday timestamp.

## Validation Plan
1. Syntax validation:
   - `py_compile paper_sync.py`
2. Execution validation:
   - Run `paper_sync.py`.
3. Result validation:
   - Confirm general-signal `trades_calc.csv` rows have `entry_ts/exit_ts` time `00:00:00`.
   - Confirm Surge rows keep non-midnight intraday time.
4. Policy validation:
   - Confirm no Gate, LOCK, D rule, order, fill, broker/KIS, or Surge generation logic changed.
5. FAIL-CLOSED validation:
   - Confirm `fills.csv`, `orders_20260430_exec.xlsx`, and `virtual_ledger.csv` hashes do not change.
6. Regression validation:
   - Confirm `paper_sync.py` exits `0`, output CSV parses, and PnL summary JSON parses.

## Validation Result
- Syntax:
  - `E:\1_Data\_runtime\python312-embed\python.exe -m py_compile .\paper_sync.py`: exit `0`.
- Execution:
  - `E:\1_Data\_runtime\python312-embed\python.exe .\paper_sync.py`: exit `0`.
  - `fills_norm rows=430`.
  - `trades_calc rows=244`.
  - output JSON: `E:\1_Data\2_Logs\paper_sync_pnl_summary_20260430_173154.json`.
- Result artifacts:
  - `trades_rows=244`.
  - `non_surge_rows=193`.
  - `non_surge_bad_entry_time=0`.
  - `non_surge_bad_exit_time=0`.
  - `surge_rows=51`.
  - `surge_entry_non_midnight=51`.
  - `surge_exit_non_midnight=51`.
  - `fills_norm_internal_cols=[]`.
- PnL summary JSON parse:
  - `as_of=20260430`.
  - `trades_used=244`.
  - `rows_total=244`.
  - `rows_as_of=3`.
- Hash checks:
  - `E:\1_Data\paper\fills.csv`: `D1E3207E4C28EFE1200F8285EFEE22619291633D1960D52DC9D9F528F939E11E`.
  - `E:\1_Data\paper\orders_20260430_exec.xlsx`: `83F7CDC25CE17F5F5CC01F7B318344FDEC26C8ACB9EA8BBC7F2B101D82A3E045`.
  - `E:\1_Data\virtual_ledger.csv`: `354FCB478B63F22A94C19AB3518F798A460899B151D591BAD8580C8035FF6C40`.
  - These three source files matched the pre-run hashes.
- Modified output hashes:
  - `E:\1_Data\paper_sync.py`: `0A600FA58D01C8B5004E559F409757EB746743DB6CA79DC9088FA7DA6BF65D43`.
  - `E:\1_Data\paper\trades_calc.csv`: `32833F742BEECAD7F60D51BDC8A61901E30B3434742F219CCB5EF0CA51E3202F`.
  - `E:\1_Data\paper\fills_norm.csv`: `992C308261ABE55D48F7D5F826FD0AC0EF752F7880C4C2648CE855A9E911ABA1`.
- Cleanup:
  - Removed generated `E:\1_Data\__pycache__\paper_sync.cpython-312.pyc`.

## Validation Matrix
- Functional validation: PASS; `paper_sync.py` regenerated the paper sync outputs.
- Consistency validation: PASS; general signal rows have date-only `entry_ts/exit_ts`, and Surge rows keep intraday timestamps.
- Operational reflection validation: PASS; `trades_calc.csv`, `fills_norm.csv`, and dated PnL summary JSON were written.
- Policy validation: PASS; Gate, LOCK, D rule, order/fill generation, broker/KIS source, and Surge generation policy were not changed.
- FAIL-CLOSED validation: PASS; source `fills.csv`, `orders_20260430_exec.xlsx`, and `virtual_ledger.csv` hashes did not change.
- Regression validation: PASS; syntax check passed, runtime exited `0`, CSV outputs parsed, and JSON output parsed.
