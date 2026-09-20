# Verification Example: SAMPLE_SOURCE

## Purpose

This example shows how Stage 1 handles a source that exists but does not contain real external evidence.

## Input Note

```text
01_Sources/SAMPLE_SOURCE.md
```

## Expected Result

```yaml
verification:
  verified: false
  verification_status: unknown
  source_count: 1
  primary_source_exists: false
  original_text_available: false
  numeric_values_checked: false
  date_values_checked: false
  entity_names_checked: false
  conflict_exists: false
  confidence: unknown
```

## Reason

The sample source is structurally valid, but it has no real original material, official source, numbers, dates, or external entity evidence.

Therefore it cannot become `verified`.

## Correct Output Note

```text
01_Sources/SAMPLE_SOURCE_VERIFICATION.md
```

## Incorrect Outputs

Do not write:

```yaml
verification:
  verified: true
  verification_status: verified
```

Do not write:

```yaml
trading:
  trading_approved: true
  direct_candidate_allowed: true
  execution_allowed: true
```

## Stage 1 Meaning

Stage 1 can say:

```text
source exists
source is incomplete
verification is unknown
```

Stage 1 cannot say:

```text
buy
sell
candidate
signal
gate pass
order
fill
ledger update
stats update
```

