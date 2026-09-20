# Verification Schema

## Stage 1 Boundary

Stage 1 verifies source quality only.

It does not create trading approval, trading candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

```text
Stage 0: raw / extracted / interpreted
Stage 1: verification
```

## Verification Frontmatter Extension

Use these fields when a note enters source verification.

```yaml
verification:
  verified: false
  verification_status: pending
  verified_at:
  verified_by:
  source_count: 0
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
```

## Verification Status Values

```text
pending       verification not started
partial       some fields checked, but not enough for verified
verified      source facts checked against evidence
conflicted    credible source conflict remains
rejected      source is unusable or unreliable
unknown       evidence is insufficient
```

## Required Evidence For Verified

All required checks must be true:

```text
primary_source_exists
original_text_available
numeric_values_checked
date_values_checked
entity_names_checked
conflict_exists == false
source_count >= 1
```

## Important Meaning

`verified: true` means source facts were checked.

It does not mean:

```text
buy
sell
candidate
signal
gate pass
trading approved
execution allowed
```

