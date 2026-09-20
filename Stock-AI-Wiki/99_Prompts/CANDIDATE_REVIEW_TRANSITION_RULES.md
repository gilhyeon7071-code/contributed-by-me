# Candidate Review Transition Rules

## Allowed Transitions

```text
thesis:review -> candidate_review:review_queue
candidate_review:review_queue -> candidate_review:rejected
candidate_review:review_queue -> candidate_review:archived
```

## Blocked Transitions

```text
raw -> candidate_review
extracted -> candidate_review
interpreted -> candidate_review
verification:pending -> candidate_review
verification:unknown -> candidate_review
verification:verified -> candidate_review
thesis:blocked_unverified_source -> candidate_review
thesis:draft -> candidate_review
```

## Required Checks

Before setting `review_status: review_queue`, confirm:

```text
source note exists
verification note exists
verification.verified == true
thesis note exists
thesis.thesis_status == review
candidate review asks questions only
policy mapping is recorded as checked or explicitly missing
trading safety fields remain false
```

## Stage 3 Not Allowed

Stage 3 cannot write:

```text
buy
sell
entry
exit
position size
signal
order
fill
ledger row
stats update
gate pass
trading approved
execution allowed
```

## Safety Lock

These fields remain false in Stage 3:

```yaml
used_for_trading: false
trading_approved: false
direct_candidate_allowed: false
execution_allowed: false
gate_checked: false
```

