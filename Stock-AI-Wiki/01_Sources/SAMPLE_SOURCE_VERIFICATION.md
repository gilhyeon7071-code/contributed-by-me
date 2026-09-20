---
id: verification-2026-05-18-sample-source-001
type: verification
title: Sample Source Verification
created: 2026-05-18
updated: 2026-05-18
status: verification
stage: 1

market: KRX
ticker: "000000"
company: Sample Company
theme: []

source:
  type: sample
  name: sample
  url:
  published_at:
  collected_at: 2026-05-18

analysis:
  summary: Verification example for SAMPLE_SOURCE.
  key_facts: []
  related_entities: []
  possible_impact: unknown
  uncertainty:
    - Real source material is not populated.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: false
  numeric_values_checked: false
  date_values_checked: false
  entity_names_checked: false
  conflict_exists: false
  conflict_summary:
  confidence: unknown

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
  change_reason: sample verification
---

# Sample Source Verification

## Source Being Checked
- [[SAMPLE_SOURCE]]

## Facts Checked
- The source note exists.
- The source note is a sample, not a real external source.

## Numbers Checked
- No real numeric values are available.

## Dates Checked
- `collected_at: 2026-05-18` is present.
- Source publication date is unknown.

## Entities Checked
- `market: KRX` is present.
- `ticker: "000000"` is a sample value.
- `company: Sample Company` is a sample value.

## Conflicts
- No conflicting source was checked.

## Remaining Uncertainty
- Original source is unavailable.
- Primary source does not exist.
- Real company data is not populated.

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

