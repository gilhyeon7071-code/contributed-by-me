---
id: shadow-2026-05-18-sample-watch-only-001
type: shadow_decision
title: Sample Shadow Decision Watch Only
created: 2026-05-18
updated: 2026-05-18
status: shadow_decision
stage: 4

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
  source_verified_required: true
  source_verified: true
  claim: A verified sample event may affect a sample company or sample theme.

candidate_review:
  review_status: review_queue
  candidate_review_allowed: true

shadow_decision:
  shadow_status: watch_only
  source_candidate_review_required: true
  candidate_review_status_required: review_queue
  candidate_review_status_observed: review_queue
  shadow_decision_allowed: true
  shadow_action: watch_only
  shadow_reason: Sample-only shadow result says watch without operational effect.
  policy_mapping_checked: false
  policy_mapping_result: not_checked
  human_review_required: true
  operational_effect: false

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
  change_reason: sample shadow watch only result
---

# Sample Shadow Decision Watch Only

## Candidate Review Source
- [[SAMPLE_CANDIDATE_REVIEW_QUEUE]]

## Shadow Status
- `watch_only`

## Shadow Action
- `watch_only`

## Shadow Reason
- Sample-only shadow result says watch without operational effect.

## Operational Effect
- false

## Trading Boundary
- This shadow result does not approve live trading, paper trading, orders, fills, ledger rows, stats, broker submission, or gate pass status.

