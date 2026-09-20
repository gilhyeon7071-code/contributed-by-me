# Promotion Gate Read-Only Validator Design

Generated: 2026-06-19 KST
Scope: design only
Trading effect: false
Policy effect: false
Order path effect: false
Score effect: false

Related draft:

- `E:\1_Data\docs\exec-plans\active\20260619_promotion_gate_manifest_draft.md`

## 1. Purpose

Define a future read-only validator for the promotion gate manifest.

The validator should answer only this question:

> Based on current RootA evidence files, which promotion stage is allowed, blocked, unknown, or not implemented?

It must not change capital, thresholds, gates, scores, orders, fills, broker routing, CI, deploy tags, deploy annotations, JSON artifacts, CSV artifacts, or dashboards.

## 2. Proposed Files

Design target if implemented later:

- script: `E:\1_Data\tools\validate_promotion_gate_manifest.py`
- latest JSON output: `E:\1_Data\2_Logs\promotion_gate_validation_latest.json`
- latest CSV output: `E:\1_Data\2_Logs\promotion_gate_validation_latest.csv`
- optional history JSON: `E:\1_Data\2_Logs\promotion_gate_validation_history\promotion_gate_validation_<timestamp>.json`

This document does not create those files.

## 3. Inputs

Required design inputs:

```yaml
inputs:
  manifest_draft:
    path: "E:\\1_Data\\docs\\exec-plans\\active\\20260619_promotion_gate_manifest_draft.md"
    role: "human-readable draft contract"

  trading_stage_validation:
    path: "E:\\1_Data\\2_Logs\\trading_stage_validation_latest.json"
    required: true
    max_age_days: 1
    key_fields:
      - "generated_at"
      - "paper.judgment"
      - "paper.items"
      - "live.items"
      - "transition_gate.paper_to_live.status"
      - "transition_gate.paper_to_live.blockers"
      - "transition_gate.paper_to_live.policy_profile"

  paper_pnl_summary:
    path: "E:\\1_Data\\2_Logs\\paper_pnl_summary_last.json"
    required: true
    max_age_days: 1
    key_fields:
      - "generated_at"
      - "as_of"
      - "as_of_ymd"
      - "run_id"

  live_vs_bt_feedback:
    path: "E:\\1_Data\\2_Logs\\live_vs_bt_feedback_latest.json"
    required: true
    max_age_days: 1
    key_fields:
      - "as_of"
      - "run_id"

  runtime_chain_status:
    path: "E:\\1_Data\\2_Logs\\runtime_chain_status_latest.json"
    required: true
    max_age_hours: 3
    key_fields:
      - "recorded_at"
      - "overall"
      - "issues"

  live_canary:
    path: "E:\\1_Data\\2_Logs\\kis_live_canary_first_latest.json"
    required_for:
      - "small-canary"
      - "stage1"
      - "full-promote"
    max_age_days: 1
    key_fields:
      - "generated_at"
      - "ok"
      - "mode"
      - "mock"
      - "execute"

  ledger_live_fills:
    path: "E:\\1_Data\\2_Logs\\ledger_live_fills_dry_run_latest.json"
    required_for:
      - "small-canary"
      - "stage1"
      - "full-promote"
    max_age_days: 1
    key_fields:
      - "generated_at"
      - "status"
      - "missing_rows"

  fills:
    path: "E:\\1_Data\\paper\\fills.csv"
    required: true
    role: "D rule source"

  orders_exec:
    path_pattern: "E:\\1_Data\\paper\\orders_<D>_exec.xlsx"
    required_for:
      - "small-canary"
      - "stage1"
      - "full-promote"
    role: "orders(D) source for STOP condition"
```

Optional future inputs:

```yaml
optional_future_inputs:
  evidence_bundle:
    source_tool: "E:\\1_Data\\tools\\evidence_bundle_dropin.ps1"
    status: "tool exists; promotion CI enforcement not confirmed"
  paired_replay_60d:
    status: "not confirmed"
  fills_signals:
    status: "not confirmed"
  slippage_samples:
    status: "not confirmed"
  pnl_minute_ts:
    status: "not confirmed"
  msprt_log:
    status: "not confirmed"
  enbpi_report:
    status: "not confirmed"
```

## 4. Output Contract

The JSON output should be machine-readable and report facts separately from interpretation.

```json
{
  "generated_at": "YYYY-MM-DDTHH:MM:SS+09:00",
  "schema_version": "promotion_gate_validation_v1",
  "trading_effect": false,
  "policy_effect": false,
  "order_path_effect": false,
  "score_effect": false,
  "overall_status": "PASS|FAIL|UNKNOWN|NA",
  "allowed_stage": "shadow|none",
  "blocked_stages": ["small-canary", "stage1", "full-promote"],
  "facts": {
    "d_rule_ymd": "YYYYMMDD|UNKNOWN",
    "run_id": "value|UNKNOWN",
    "as_of": "YYYYMMDD|UNKNOWN",
    "transition_status": "HOLD|READY|UNKNOWN",
    "runtime_chain_overall": "OK|WARN|FAIL|UNKNOWN"
  },
  "stage_results": [
    {
      "stage": "shadow",
      "status": "PASS|FAIL|UNKNOWN|NA",
      "capital_pct": 0,
      "reason": "text",
      "checks": []
    }
  ],
  "checks": [
    {
      "name": "check name",
      "status": "PASS|FAIL|UNKNOWN|NA",
      "required_for": ["stage name"],
      "evidence_path": "path",
      "observed_value": "value",
      "expected_value": "value",
      "reason": "text"
    }
  ],
  "untested": [],
  "next_action": "text"
}
```

CSV output should be a flat view of `checks`:

```csv
stage,check,status,evidence_path,observed_value,expected_value,reason
```

## 5. Status Semantics

```yaml
status_semantics:
  PASS:
    meaning: "Required evidence exists, is current, and matches the expected value."
    promotion_effect: "May satisfy one validator check only."

  FAIL:
    meaning: "Required evidence exists and proves the condition is not met."
    promotion_effect: "Blocks the affected stage."

  UNKNOWN:
    meaning: "Evidence is missing, stale, unreadable, mismatched, or structurally ambiguous."
    promotion_effect: "Blocks the affected stage by FAIL-CLOSED."

  NA:
    meaning: "Check does not apply to the stage."
    promotion_effect: "Does not approve anything."
```

The validator must never treat `NA` or `UNKNOWN` as approval.

## 6. Stage Logic

```yaml
stage_logic:
  shadow:
    capital_pct: 0
    pass_when:
      - "trading_stage_validation_latest.json exists"
      - "paper stage is not failed"
      - "broker_order_route remains false"
    block_when:
      - "required paper evidence is missing or unreadable"
      - "runtime_chain_status is FAIL"
    output_allowed_stage: "shadow only"

  small-canary:
    capital_pct: 0.5
    pass_when_all:
      - "transition_gate.paper_to_live.status == READY"
      - "paper_quality_gate == PASS"
      - "live_canary_gate == PASS"
      - "live_preflight_health == PASS"
      - "canary_execute_mode == PASS"
      - "live_canary.ok == true"
      - "live_canary.execute == true"
      - "ledger_live_fills.status == PASS"
      - "ledger_live_fills.missing_rows == 0"
      - "runtime_chain_status.overall == OK"
      - "run_id and as_of match across required paper/BT evidence"
      - "orders_exec for D exists"
    current_expected_result: "FAIL or UNKNOWN until blockers are cleared"

  stage1:
    capital_pct: 2
    pass_when_all:
      - "small-canary has passed with current evidence"
      - "stage1 capital cap exists in enforceable config"
      - "stage1 evidence window exists"
      - "stage1 CI enforcement exists"
    current_expected_result: "NA or FAIL_NOT_IMPLEMENTED"

  full-promote:
    capital_pct: 10
    pass_when_all:
      - "stage1 has passed with current evidence"
      - "full-promote capital cap exists in enforceable config"
      - "full-promote evidence window exists"
      - "rollback and kill-switch evidence are current"
      - "promotion CI tag flow exists and is gated"
    current_expected_result: "NA or FAIL_NOT_IMPLEMENTED"
```

## 7. STOP Conditions

The validator must return `overall_status=FAIL` when any of these is true for a stage that requires it:

- `orders_exec` missing
- `exec_date != D`
- `as_of` mismatch
- `run_id` mismatch
- paper date and broker date mixed
- required evidence stale
- required evidence unreadable
- required latest JSON missing
- required gate status is `FAIL`, `HOLD`, `BLOCK`, `NOT_EVALUABLE`, or equivalent non-approval state

If a STOP condition cannot be checked, the check status must be `UNKNOWN`, and the affected stage must be blocked.

## 8. D Rule

Design D derivation:

1. Read `E:\1_Data\paper\fills.csv`.
2. If any `BUY` rows exist, set `D` to the latest `ymd` from `BUY`.
3. If no `BUY` rows exist, set `D` to the latest `datetime` prefix `YYYYMMDD`.
4. If `fills.csv` is missing, empty, or unreadable, set `D=UNKNOWN` and block all stages above shadow.

The validator must not infer D from wall-clock date.

## 9. Freshness Rules

Draft freshness defaults:

```yaml
freshness:
  trading_stage_validation_latest.json: "max_age_days <= 1"
  paper_pnl_summary_last.json: "max_age_days <= 1"
  live_vs_bt_feedback_latest.json: "max_age_days <= 1"
  runtime_chain_status_latest.json: "max_age_hours <= 3"
  kis_live_canary_first_latest.json: "max_age_days <= 1 for canary or above"
  ledger_live_fills_dry_run_latest.json: "max_age_days <= 1 for canary or above"
```

If freshness cannot be computed, use `UNKNOWN`.

## 10. Non-Goals

The validator must not:

- patch the manifest draft
- patch CI
- create a promotion tag
- patch a deploy annotation
- enable `CANARY_EXECUTE`
- write to broker/live order routes
- change capital settings
- change risk thresholds
- change gate interpretation
- change dashboard cards
- edit latest source evidence files

## 11. Expected Current Result

Given the evidence checked before this design:

```yaml
expected_current_result:
  overall_status: "FAIL"
  allowed_stage: "shadow"
  blocked_stages:
    - "small-canary"
    - "stage1"
    - "full-promote"
  known_blockers:
    - "paper_quality_gate FAIL"
    - "live_canary_gate FAIL"
    - "live_preflight_health FAIL"
    - "canary_execute_mode not executable"
    - "runtime_chain_status_latest overall WARN"
  not_implemented:
    - "stage1 enforceable capital config"
    - "full-promote enforceable capital config"
    - "ENBPI/MSPRT promotion CI"
    - "promotion tag gate"
    - "deploy annotation gate"
```

This expected result is a design baseline, not a fresh execution result.

## 12. Validation Plan If Implemented Later

Minimum validation after implementation:

1. Syntax validation
   - `python -m py_compile E:\1_Data\tools\validate_promotion_gate_manifest.py`
2. Execution validation
   - run the validator in read-only mode
   - confirm exit code behavior is documented
3. Result artifact validation
   - inspect `promotion_gate_validation_latest.json`
   - inspect `promotion_gate_validation_latest.csv`
   - confirm `trading_effect=false`, `policy_effect=false`, `order_path_effect=false`, `score_effect=false`
   - confirm current canary-or-above stages are blocked unless all required evidence is current and PASS
4. Mojibake validation
   - `E:\1_Data\run_scan_mojibake_text.bat --path <changed file>`

## 13. Report Format

Any validator run should report:

- facts
- interpretation
- evidence paths
- key observed values
- what was tested
- what was not tested
- PASS / FAIL / UNKNOWN / NA by check

It must not say ready, fixed, done, operational, or complete unless all required checks for that claim are PASS with current evidence.

