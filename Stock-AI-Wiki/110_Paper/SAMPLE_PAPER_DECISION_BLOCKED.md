---
id: paper-2026-05-18-sample-blocked-001
type: paper_decision
title: Sample Paper Decision Blocked
created: 2026-05-18
updated: 2026-05-18
status: paper_decision
stage: 5

market: KRX
ticker: "000000"
company: Sample Company
theme:
  - Sample Theme

verification:
  verified: true
  verification_status: verified
  source_count: 1
  confidence: medium

thesis:
  thesis_status: review
  source_verified: true
  claim: A verified sample event may affect a sample company or sample theme.

candidate_review:
  review_status: review_queue
  candidate_review_allowed: true

shadow_decision:
  shadow_status: blocked
  shadow_action: block
  operational_effect: false

paper_decision:
  paper_status: blocked_not_ready
  shadow_required: true
  shadow_status_required: watch_only
  shadow_status_observed: blocked
  paper_review_allowed: false
  paper_simulation_allowed: false
  roota_paper_engine_connected: false
  broker_connected: false
  order_creation_allowed: false
  fill_creation_allowed: false
  ledger_update_allowed: false
  stats_update_allowed: false
  operational_effect: false
  blocking_reasons:
    - shadow_status is blocked, not watch_only.

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
  change_reason: sample blocked paper decision
---

# Sample Paper Decision Blocked

## Shadow Source
- [[SAMPLE_SHADOW_DECISION_BLOCKED]]

## Paper Status
- `blocked_not_ready`

## Blocking Reasons
- `shadow_status` is `blocked`, not `watch_only`.

## RootA Paper Engine Connection
- false

## Broker Connection
- false

## Operational Effect
- false

## Trading Boundary
- This paper decision note does not create RootA paper orders, fills, ledger rows, stats updates, broker submissions, live trading, or gate pass status.

