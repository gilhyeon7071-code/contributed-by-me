---
id: candidate-review-2026-05-18-sample-blocked-001
type: candidate_review
title: Sample Candidate Review Blocked
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
  thesis_status: draft
  source_verified_required: true
  source_verified: true
  claim: A verified sample event may affect a sample company or sample theme.

candidate_review:
  review_status: blocked_not_reviewed
  thesis_required: true
  thesis_status_required: review
  thesis_status_observed: draft
  candidate_review_allowed: false
  policy_mapping_checked: false
  human_review_required: true
  review_questions:
    - Is the thesis ready for human review?
    - Is there a separate policy mapping?
  blocking_reasons:
    - thesis_status is draft, not review.
    - policy mapping has not been checked.

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
  change_reason: sample blocked candidate review
---

# Sample Candidate Review Blocked

## Thesis Being Reviewed
- [[SAMPLE_THESIS_VERIFIED_EXAMPLE]]

## Review Status
- `blocked_not_reviewed`

## Review Questions
- Is the thesis ready for human review?
- Is there a separate policy mapping?

## Blocking Reasons
- `thesis_status` is `draft`, not `review`.
- Policy mapping has not been checked.

## Policy Mapping
- not_checked

## Human Review
- required

## Trading Boundary
- This blocked candidate review does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

