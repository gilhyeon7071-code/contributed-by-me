# 1_Data Documentation SSOT

Last updated: 2026-03-05
Owner: RootA (`E:\1_Data`)

## Purpose
This folder is the operational and design SSOT for the RootA trading pipeline.
Use this as the first entry point before changing code, config, or schedules.

## Recommended Reading Order
1. `02-design/system-architecture.md`
2. `02-design/pipeline-contracts.md`
3. `03-operations/daily-runbook.md`
4. `03-operations/root-cleanup-policy.md`

## Folder Map
- `01-plan/`: planned work and feature plans.
- `02-design/`: architecture and technical design.
- `03-operations/`: runbook, incident response, operational policy.
- hidden (`.pdca-*`, `.bkit-*`): tool metadata. Do not edit manually.

## Current Pipeline Snapshot (2026-03-05)
- Candidate chain outputs aligned to `2026-03-05`:
  - `candidates_latest_data.filtered.csv`
  - `candidates_latest_data.with_sector_score.csv`
  - `candidates_latest_data.with_news_score.csv`
  - `candidates_latest_data.with_final_score.csv`
- Core risk controls in config:
  - `kill_switch.mode=BLOCK`, `reduce_factor=0.3`, `min_new=1`
  - `crash_risk_off.enabled=true`
  - `max_per_sector=2`
  - `max_gross_exposure_pct=0.8`
  - `max_daily_new_exposure_pct=0.4`
  - `entry_gap_down_stop_pct=0.03`

## Documentation Rules
- Keep docs additive and versioned by date when behavior changes.
- Do not overwrite evidence artifacts in `2_Logs`.
- Prefer absolute paths in examples.
- When a contract changes, update both:
  - `02-design/pipeline-contracts.md`
  - relevant runbook section in `03-operations/`.

## Legacy References
Legacy snapshots kept in docs root:
- `directory_map_ssot_last.md`
- `folder_audit_ssot_last.md`
- `folder_audit_review_queue_last.md`
