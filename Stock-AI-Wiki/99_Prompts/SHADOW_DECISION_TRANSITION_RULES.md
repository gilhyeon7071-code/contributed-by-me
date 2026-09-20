# Shadow Decision Transition Rules

## Allowed Transitions

```text
candidate_review:review_queue -> shadow:queued
shadow:queued -> shadow:watch_only
shadow:queued -> shadow:blocked
shadow:queued -> shadow:ignored
shadow:watch_only -> shadow:archived
shadow:blocked -> shadow:archived
shadow:ignored -> shadow:archived
```

## Blocked Transitions

```text
raw -> shadow
extracted -> shadow
interpreted -> shadow
verification:verified -> shadow
thesis:draft -> shadow
thesis:review -> shadow
candidate_review:blocked_not_reviewed -> shadow
candidate_review:blocked_policy_unmapped -> shadow
candidate_review:rejected -> shadow
```

## Required Checks

Before setting `shadow_status: queued`, confirm:

```text
source note exists
verification note exists
thesis note exists
candidate review note exists
candidate_review.review_status == review_queue
candidate_review.candidate_review_allowed == true
shadow operational_effect == false
trading safety fields remain false
```

## Stage 4 Not Allowed

Stage 4 cannot write:

```text
live buy
live sell
paper buy
paper sell
order
fill
ledger row
stats update
broker submit
gate pass
trading approved
execution allowed
```

## Safety Lock

These fields remain false in Stage 4:

```yaml
used_for_trading: false
trading_approved: false
direct_candidate_allowed: false
execution_allowed: false
gate_checked: false
operational_effect: false
```

