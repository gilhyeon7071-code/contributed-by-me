---
id: candidate-review-2026-05-18-sample-queue-001
type: candidate_review
title: Sample Candidate Review Queue
created: 2026-05-18
updated: 2026-05-18
status: candidate_review
stage: 3

market: KRX
ticker: "000000"
company: Sample Company
theme:
  - Sample Theme

source:
  type: sample_primary
  name: sample_primary_source
  url: sample-only
  published_at: 2026-05-18
  collected_at: 2026-05-18

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
  thesis_required: true
  thesis_status_required: review
  thesis_status_observed: review
  candidate_review_allowed: true
  policy_mapping_checked: false
  human_review_required: true
  review_questions:
    - Is the thesis relevant to any separate policy rule?
    - What evidence would invalidate the hypothesis?
    - Is there opposing source evidence?
  blocking_reasons:
    - Policy mapping is not checked.

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
  change_reason: sample candidate review queue
---

# Sample Candidate Review Queue

## Thesis Being Reviewed
- [[SAMPLE_THESIS_REVIEW_EXAMPLE]]

## Review Status
- `review_queue`

## Review Questions
- Is the thesis relevant to any separate policy rule?
- What evidence would invalidate the hypothesis?
- Is there opposing source evidence?

## Blocking Reasons
- Policy mapping is not checked.

## Policy Mapping
- not_checked

## Human Review
- required

## Trading Boundary
- This candidate review queue item does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

