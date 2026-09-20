---
id: verification-2026-05-18-sample-verified-source-001
type: verification
title: Sample Verified Source Verification
created: 2026-05-18
updated: 2026-05-18
status: verification
stage: 1

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

analysis:
  summary: Sample-only verification note for Stage 2 thesis demonstration.
  key_facts:
    - Sample source has complete sample metadata.
  related_entities:
    - Sample Company
    - Sample Theme
  possible_impact: unknown
  uncertainty:
    - This is sample-only evidence.

verification:
  verified: true
  verification_status: verified
  verified_at: 2026-05-18
  verified_by: sample
  source_count: 1
  primary_source_exists: true
  original_text_available: true
  numeric_values_checked: true
  date_values_checked: true
  entity_names_checked: true
  conflict_exists: false
  conflict_summary:
  confidence: medium

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
  change_reason: sample verified source verification
---

# Sample Verified Source Verification

## Source Being Checked
- [[SAMPLE_VERIFIED_SOURCE]]

## Facts Checked
- Sample event date is present.
- Sample company and theme names are present.

## Numbers Checked
- No real market numbers are used.
- Numeric check is true only for this sample structure.

## Dates Checked
- Published and collected dates are both `2026-05-18`.

## Entities Checked
- Sample Company.
- Sample Theme.

## Conflicts
- No sample conflict exists.

## Remaining Uncertainty
- This note is not real market evidence.

## Verification Result
- `verification_status: verified`
- `verified: true`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

