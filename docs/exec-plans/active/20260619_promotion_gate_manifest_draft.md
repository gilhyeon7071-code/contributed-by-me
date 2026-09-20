# Promotion Gate Manifest Draft

Generated: 2026-06-19 KST
Scope: draft only
Trading effect: false
Policy effect: false
Order path effect: false

## 1. Purpose

This draft maps the proposed shadow -> small-canary -> stage1 -> full-promote gate to current RootA files and evidence.

It does not change capital, gates, CI, order routing, broker routing, score, LOCK, or FAIL-CLOSED behavior.

## 2. Current Evidence Snapshot

Checked artifacts:

- `E:\1_Data\2_Logs\trading_stage_validation_latest.json`
  - `generated_at`: `2026-06-19 09:12:14+0900`
  - `paper.judgment`: `운영가능`
  - `transition_gate.paper_to_live.status`: `HOLD`
  - blockers: `paper_quality_gate`, `live_canary_gate`, `live_preflight_health`, `canary_execute_mode`
- `E:\1_Data\2_Logs\paper_pnl_summary_last.json`
  - `generated_at`: `2026-06-19T09:14:55`
  - `as_of`: `20260618`
  - `as_of_ymd`: `20260618`
  - `run_id`: `20260619_084218`
- `E:\1_Data\2_Logs\live_vs_bt_feedback_latest.json`
  - `as_of`: `20260618`
  - `run_id`: `20260619_084218`
- `E:\1_Data\2_Logs\kis_live_canary_first_latest.json`
  - `generated_at`: `2026-06-09T09:59:00`
  - `ok`: `false`
  - `mode`: `DRY`
  - `mock`: `false`
  - `execute`: `false`
- `E:\1_Data\2_Logs\runtime_chain_status_latest.json`
  - `recorded_at`: `2026-06-19 09:15:28`
  - `overall`: `WARN`
  - issues: `dashboard_state:warn`, `dashboard_alerts=1`
- `E:\1_Data\2_Logs\ledger_live_fills_dry_run_latest.json`
  - `generated_at`: `2026-06-18T18:00:22`
  - `status`: `PASS`
  - `missing_rows`: `0`

Current judgment:

- Shadow/paper operation can continue under current RootA policy.
- Paper -> live remains blocked.
- Small-canary, stage1, and full-promote are not currently approved.

## 3. Capital Mapping Draft

```yaml
capital_mapping:
  shadow:
    capital_pct: 0
    paper_order_route: true
    broker_order_route: false
    notes: "paper/shadow only; no real capital"

  small-canary:
    capital_pct: 0.5
    per_trade_cap_pctADV: 0.02
    aggregate_exposure_pct: 0.5
    broker_order_route: "blocked until explicit approval and all gate checks PASS"

  stage1:
    capital_pct: 2
    per_trade_cap_pctADV: 0.05
    aggregate_exposure_pct: 2
    broker_order_route: "not implemented in current RootA gate"

  full-promote:
    capital_pct: 10
    per_trade_cap_pctADV: 0.12
    aggregate_exposure_pct: 10
    broker_order_route: "not implemented in current RootA gate"
```

## 4. Existing RootA Gate Mapping

| Proposed gate item | Current RootA equivalent | Current status |
| --- | --- | --- |
| Paper/shadow stage | `trading_stage_validation_latest.json` paper block | Exists; paper judgment is `운영가능` |
| Paper -> live transition | `transition_gate.paper_to_live` | Exists; current status is `HOLD` |
| Canary gate | `kis_live_canary_first_latest.json`, `live_canary_gate`, `canary_execute_mode` | Exists; current status blocks transition |
| Live preflight | `live_preflight_health` | Exists; current status fails |
| run_id/as_of consistency | `paper_pnl_summary_last.json`, `live_vs_bt_feedback_latest.json` | Partly aligned for paper/BT pair |
| Ledger/live fill reconciliation | `ledger_live_fills_dry_run_latest.json` | Exists; latest checked status `PASS` |
| Signed evidence bundle | `tools\evidence_bundle_dropin.ps1` | Tool exists; CI gate not connected |
| ENBPI/MSPRT gate | dedicated check script | Not confirmed as current promotion CI |
| Promotion tag and deploy annotation | GitHub Actions / deploy patch | Not confirmed |

## 5. Evidence Manifest Mapping Draft

```yaml
evidence_manifest_required:
  existing_or_mappable:
    operation_artifacts:
      source: "E:\\1_Data\\tools\\evidence_bundle_dropin.ps1"
      outputs:
        - "E:\\1_Data\\2_Logs\\proof\\<run_id>\\operation_artifacts.json"
        - "E:\\1_Data\\2_Logs\\proof\\<run_id>.zip"
        - "E:\\1_Data\\2_Logs\\proof\\<run_id>.zip.sig"
      status: "tool_exists_not_ci_enforced"

    paper_stage_validation:
      source: "E:\\1_Data\\2_Logs\\trading_stage_validation_latest.json"
      required_fields:
        - "generated_at"
        - "paper.judgment"
        - "transition_gate.paper_to_live.status"
        - "transition_gate.paper_to_live.blockers"

    paper_bt_alignment:
      source: "E:\\1_Data\\2_Logs\\live_vs_bt_feedback_latest.json"
      required_fields:
        - "as_of"
        - "run_id"

    paper_pnl_summary:
      source: "E:\\1_Data\\2_Logs\\paper_pnl_summary_last.json"
      required_fields:
        - "as_of"
        - "as_of_ymd"
        - "run_id"

    live_canary:
      source: "E:\\1_Data\\2_Logs\\kis_live_canary_first_latest.json"
      required_fields:
        - "generated_at"
        - "ok"
        - "mode"
        - "mock"
        - "execute"

    ledger_live_fills:
      source: "E:\\1_Data\\2_Logs\\ledger_live_fills_dry_run_latest.json"
      required_fields:
        - "generated_at"
        - "status"
        - "missing_rows"

  missing_or_not_confirmed:
    paired_replay_60d.tar.gz:
      current_mapping: "not confirmed"
      note: "RootA has replay/alignment artifacts, but this exact bundle contract is not confirmed."
    fills_signals.csv:
      current_mapping: "not confirmed"
      note: "RootA has orders/fills/ledger files, but a single signal->order->fill mapping file is not confirmed."
    slippage_samples.json:
      current_mapping: "not confirmed"
      note: "sample>=500 contract not confirmed."
    pnl_minute_ts.csv:
      current_mapping: "not confirmed"
      note: "minute-level PnL time series contract not confirmed."
    msprt_log.csv:
      current_mapping: "not confirmed"
      note: "dedicated promotion gate artifact not confirmed."
    enbpi_report.json:
      current_mapping: "not confirmed"
      note: "microstructure observe code mentions pending ENBPI, but dedicated promotion artifact is not confirmed."
```

## 6. Promotion Decision Rules Draft

```yaml
promotion_decision:
  common_fail_closed_conditions:
    - "orders_exec missing for affected D"
    - "exec_date != D"
    - "paper/broker date mixed"
    - "as_of or run_id mismatch across required evidence"
    - "required evidence file missing"
    - "required evidence file stale"
    - "signature required but missing or unverifiable"
    - "runtime_chain_status_latest.overall != OK"
    - "transition_gate.paper_to_live.status != READY"

  shadow:
    required:
      - "paper stage is operational or explicitly allowed for paper-only operation"
      - "broker_order_route=false"
      - "trading_effect=false for shadow diagnostics"
    current_status: "allowed only as paper/shadow operation"

  small-canary:
    required:
      - "paper_quality_gate PASS"
      - "live_canary_gate PASS"
      - "live_preflight_health PASS"
      - "canary_execute_mode PASS after explicit user approval"
      - "live_fills/ledger reconciliation PASS"
      - "signed evidence bundle PASS if signature is required"
    current_status: "blocked"

  stage1:
    required:
      - "small-canary completed with current evidence"
      - "stage1 capital/risk cap exists in enforceable config"
      - "CI blocks deploy on evidence mismatch"
    current_status: "not implemented"

  full-promote:
    required:
      - "stage1 completed with current evidence"
      - "full-promote capital/risk cap exists in enforceable config"
      - "rollback and kill-switch evidence current"
    current_status: "not implemented"
```

## 7. CI Mapping Draft

Current confirmed CI:

- `.github\workflows\codex-llm-review-advisory.yml`
  - advisory context/parser workflow only
  - not a promotion gate

Draft CI actions if later implemented:

```yaml
ci_actions_draft:
  - "validate manifest schema"
  - "verify required artifact existence and freshness"
  - "verify run_id/as_of/D consistency"
  - "verify signed evidence bundle when required"
  - "run statistical gates only after dedicated scripts exist"
  - "block promotion on any FAIL/UNKNOWN/NOT_EVALUABLE required item"
  - "create promotion tag only after all required gates PASS"
```

This draft does not create the CI workflow.

## 8. Current Blockers

Small-canary blockers from current evidence:

- `paper_quality_gate`: FAIL
- `live_canary_gate`: FAIL
- `live_preflight_health`: FAIL
- `canary_execute_mode`: blocked by execute flag off
- `runtime_chain_status_latest.overall`: WARN

Missing implementation pieces:

- enforceable capital cap config for proposed stages
- exact evidence bundle schema for all proposed files
- dedicated ENBPI/MSPRT promotion checks
- CI job that enforces the promotion gate
- deploy annotation patch flow
- promotion tag flow

## 9. Non-Goals

This draft does not:

- change thresholds
- relax any gate
- enable canary execution
- connect broker order routing
- patch CI
- patch dashboard
- modify latest JSON/CSV/log artifacts
- mark small-canary, stage1, or full-promote as approved

## 10. Next Allowed Step

If this draft is approved, the next narrow step is a read-only validator design:

- input: this manifest draft plus current RootA evidence files
- output: a JSON report with `PASS`, `FAIL`, `UNKNOWN`, or `NA`
- default result: FAIL-CLOSED if any required current evidence is missing, stale, or mismatched

