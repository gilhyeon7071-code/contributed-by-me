# Daily Runbook (RootA)

Last updated: 2026-03-05

## 1. Standard Daily Sequence
From `E:\1_Data`:
1. Update prices/parquet and candidate generation.
2. Run macro + p0 + gate.
3. Run survivorship + liquidity filter.
4. Run sector/news/final score sidecars.
5. Run paper engine.
6. Run integration/live-vs-bt checks.

Primary orchestrator:
- `E:\1_Data\run_paper_daily.bat`

## 2. Runtime Requirements
- Python runtime is auto-resolved by batch (venv/absolute path first).
- Config lock file must exist and sha must match:
  - `paper\paper_engine_config.lock.json`

## 3. Fast Validation Commands
- Check latest p0 output:
  - `Get-ChildItem E:\1_Data\2_Logs\p0_daily_check_*.json | Sort LastWriteTime -Desc | Select -First 1`
- Check candidate chain alignment:
  - verify dates in filtered/sector/news/final sidecars.
- Check integration status:
  - `E:\1_Data\2_Logs\signal_integration_status_YYYYMMDD.json`

## 4. Failure Handling
- Macro/API fetch errors:
  - confirm fallback status in p0 and macro logs before rerun.
- Gate fail (`risk_off`):
  - expected behavior is block/reduce according to config mode.
- Missing sidecar data:
  - fail-soft is allowed for optional phases, but status file must record it.

## 5. Evidence Discipline
After each major change, store evidence in `2_Logs` and note:
- run timestamp
- as-of date
- pass/fail summary
- affected artifacts
