---
id: production-approval-2026-05-18-sample-blocked-001
type: production_approval
title: Sample Production Approval Blocked
created: 2026-05-18
updated: 2026-05-18
status: production_approval
stage: 6

market: KRX
ticker: "000000"
company: Sample Company
theme:
  - Sample Theme

paper_decision:
  paper_status: review_queue
  paper_review_allowed: true
  paper_simulation_allowed: false
  roota_paper_engine_connected: false
  broker_connected: false
  operational_effect: false

production_approval:
  approval_status: blocked
  explicit_user_approval_required: true
  explicit_user_approval_present: false
  roota_policy_mapping_required: true
  roota_policy_mapping_verified: false
  roota_runtime_evidence_required: true
  roota_runtime_evidence_verified: false
  regression_required: true
  regression_verified: false
  fail_closed_verified: false
  production_connection_allowed: false
  broker_connection_allowed: false
  order_creation_allowed: false
  fill_creation_allowed: false
  ledger_update_allowed: false
  stats_update_allowed: false
  gate_change_allowed: false
  operational_effect: false
  blocking_reasons:
    - Explicit user approval is not present.
    - RootA policy mapping is not verified.
    - RootA runtime evidence is not verified.
    - Regression verification is not complete.
    - FAIL-CLOSED verification is not complete.

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
  policy_link:

audit:
  human_reviewed: false
  ai_generated: false
  last_reviewed_at:
  change_reason: sample production approval blocked
---

# Sample Production Approval Blocked

## Paper Review Source
- [[SAMPLE_PAPER_DECISION_REVIEW_QUEUE]]

## Approval Status
- `blocked`

## Blocking Reasons
- Explicit user approval is not present.
- RootA policy mapping is not verified.
- RootA runtime evidence is not verified.
- Regression verification is not complete.
- FAIL-CLOSED verification is not complete.

## Required Evidence
- Explicit user approval.
- Verified RootA policy mapping.
- Verified RootA runtime evidence.
- Regression verification.
- FAIL-CLOSED verification.

## Operational Effect
- false

## Trading Boundary
- This approval checklist does not connect RootA trading, paper trading, broker submission, orders, fills, ledger rows, stats, or gate pass status.

