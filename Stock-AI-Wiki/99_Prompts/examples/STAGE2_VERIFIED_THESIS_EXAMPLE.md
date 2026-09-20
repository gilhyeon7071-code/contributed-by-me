# Stage 2 Verified Thesis Example

## Purpose

This example shows the normal Stage 2 path when source verification has passed.

## Input Notes

```text
01_Sources/SAMPLE_VERIFIED_SOURCE.md
01_Sources/SAMPLE_VERIFIED_SOURCE_VERIFICATION.md
```

## Output Note

```text
20_Themes/SAMPLE_THESIS_VERIFIED_EXAMPLE.md
```

## Required Input State

```yaml
verification:
  verified: true
  verification_status: verified
```

## Allowed Output State

```yaml
thesis:
  thesis_status: draft
  source_verified_required: true
  source_verified: true
```

## Still Not Allowed

Even after thesis draft creation, do not write:

```text
buy
sell
candidate
signal
entry
exit
position size
order
fill
ledger row
stats update
gate pass
trading approved
execution allowed
```

## Required Safety State

```yaml
trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
```

