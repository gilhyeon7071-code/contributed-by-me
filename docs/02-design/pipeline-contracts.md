# Pipeline Data Contracts

Last updated: 2026-03-05

## 1. Contract Intent
Define required fields, date semantics, and PASS criteria for major artifacts.

## 2. Artifact Contracts
### Candidates base
Path: `E:\1_Data\2_Logs\candidates_latest_data.csv`
Required columns:
- `date`, `code`, `name`, `score`, `relax_level`
Date semantics:
- `date` is candidate date (`YYYY-MM-DD`).

### Candidate meta
Path: `E:\1_Data\2_Logs\candidates_latest_meta.json`
Required keys:
- `latest_date`, `chosen_level`, `attempts[]`
Use:
- `paper_engine.py` reads `chosen_level` for relax safety caps.

### Candidate sidecars
Paths:
- `...with_sector_score.csv`
- `...with_news_score.csv`
- `...with_final_score.csv`
PASS criteria:
- row count must match filtered candidate row count.
- dates should align with filtered candidate date for same run.

### Paper execution outputs
Paths:
- `E:\1_Data\paper\fills.csv`
- `E:\1_Data\paper\trades.csv`
Required checks:
- trades closed rows parseable.
- `pnl_pct` exists.
- `pnl_krw` should be populated for closed trades.

### Risk/gate outputs
Paths:
- `p0_daily_check_*.json`
- `gate_daily_*.json`
Required checks:
- explicit `risk_off.enabled` and reason list.
- `kill_switch.triggered` and computed source evidence.

### Signal integration outputs
Paths:
- `joined_trades_latest.csv`
- `joined_trades_final_latest.csv`
- `signal_integration_status_YYYYMMDD.json`
PASS criteria:
- join artifacts generated.
- source fields (`*_source`) present.
- status records include phase2/phase3/final sections.

## 3. Date Alignment Rules
- Candidate chain date should be the same day across filtered/sector/news/final files.
- Trades join range can be historical; this does not imply candidate date mismatch bug by itself.
- Reports must explicitly show date windows to avoid misinterpretation.

## 4. Contract Change Procedure
When adding/removing columns or changing date rules:
1. Update this file.
2. Update corresponding script comments/help.
3. Generate one evidence artifact in `2_Logs` proving new contract is satisfied.
