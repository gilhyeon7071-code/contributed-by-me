# Verified Transition Rules

## Allowed Transitions

```text
raw -> extracted
extracted -> interpreted
interpreted -> verification:pending
verification:pending -> verification:partial
verification:partial -> verification:verified
verification:partial -> verification:conflicted
verification:partial -> verification:rejected
```

## Not Allowed In Stage 1

```text
verified -> buy
verified -> sell
verified -> candidate
verified -> signal
verified -> gate pass
verified -> trading approved
verified -> execution allowed
```

## Minimum Verified Conditions

Set `verification_status: verified` only when all are true:

```yaml
primary_source_exists: true
original_text_available: true
numeric_values_checked: true
date_values_checked: true
entity_names_checked: true
conflict_exists: false
```

Also require:

```yaml
source_count: 1
confidence: medium
```

`confidence: high` requires more than one independent source or one primary official source with complete original text.

## Failure Conditions

Use `verification_status: rejected` when:

```text
source cannot be found
source date is impossible to confirm
company or ticker cannot be matched
numbers are materially inconsistent
source appears fabricated
```

Use `verification_status: unknown` when evidence is insufficient but not proven wrong.

## Safety Lock

Verification status must not change any trading safety field.

These values remain false in Stage 1:

```yaml
used_for_trading: false
trading_approved: false
direct_candidate_allowed: false
execution_allowed: false
gate_checked: false
```

