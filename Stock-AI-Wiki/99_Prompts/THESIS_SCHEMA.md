# Thesis Schema

## Stage 2 Boundary

Stage 2 creates research hypotheses only.

It does not create buy or sell decisions, trading candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

```text
Stage 0: analysis wiki
Stage 1: source verification
Stage 2: research thesis
```

## Thesis Meaning

A thesis is a reviewable research claim.

It can say:

```text
This verified event may affect this company or theme.
```

It cannot say:

```text
buy
sell
candidate
signal
entry
exit
position size
gate pass
trading approved
execution allowed
```

## Thesis Frontmatter

```yaml
thesis:
  thesis_status: draft
  source_verified_required: true
  source_verified: false
  claim:
  supporting_notes: []
  opposing_notes: []
  invalidation_conditions: []
  time_horizon: unknown
  impact_direction: unknown
  impact_strength: unknown

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
```

## Thesis Status Values

```text
blocked_unverified_source  source verification is insufficient
draft                      hypothesis text exists but is not reviewed
review                     ready for human review
rejected                   hypothesis rejected
archived                   no longer active
```

## Minimum Entry Condition

To create a valid Stage 2 thesis:

```yaml
verification:
  verified: true
  verification_status: verified
```

If this is not true, use:

```yaml
thesis:
  thesis_status: blocked_unverified_source
  source_verified_required: true
  source_verified: false
```

