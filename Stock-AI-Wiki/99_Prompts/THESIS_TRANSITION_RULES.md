# Thesis Transition Rules

## Allowed Transitions

```text
verification:verified -> thesis:draft
thesis:draft -> thesis:review
thesis:review -> thesis:rejected
thesis:review -> thesis:archived
```

## Blocked Transitions

```text
raw -> thesis
extracted -> thesis
interpreted -> thesis
verification:pending -> thesis
verification:partial -> thesis
verification:unknown -> thesis
verification:conflicted -> thesis
verification:rejected -> thesis
```

## Required Checks

Before setting `thesis_status: draft`, confirm:

```text
source note exists
verification note exists
verification.verified == true
verification.verification_status == verified
claim is written as a hypothesis, not a decision
opposing evidence section exists
invalidation conditions section exists
trading safety fields remain false
```

## Stage 2 Not Allowed

Stage 2 cannot write:

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

## Safety Lock

These fields remain false in Stage 2:

```yaml
used_for_trading: false
trading_approved: false
direct_candidate_allowed: false
execution_allowed: false
gate_checked: false
```

