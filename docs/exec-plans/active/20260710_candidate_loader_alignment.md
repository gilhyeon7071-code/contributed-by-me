# 2026-07-10 Candidate Loader Alignment

## Scope

- Align `generate_candidates_v41_1.py` KRX parquet discovery and duplicate handling with the existing backtest/HPO path.
- Keep candidate thresholds, relax policy, scoring, regime policy, and order behavior unchanged.

## Evidence Before Change

- Candidate generation used unrestricted recursive parquet discovery without `(code, date)` deduplication.
- Backtest and HPO used bounded canonical sources with newest-source deduplication.

## Backup

- `E:\1_Data\backup\20260710_candidate_loader_alignment\20260710_090500\`

## Validation

1. Python syntax compilation.
2. Isolated loader source and duplicate check.
3. Candidate generator execution.
4. Candidate output reflection and FAIL-CLOSED check.
## Result

- Syntax PASS.
- Candidate/report loader contract aligned at 139 files and 2,080,512 deduplicated rows.
- Candidate execution PASS with `chosen_level=NONE`, `all_pass=0`.
- h1/h2/h5 diagnostics regenerated from the corrected loader.
- Remaining data-quality issue: implausible forward-return outliers require a separate adjusted-price/corporate-action audit.