# Stock AI Wiki Stage Index

## Stage Map

```text
0. analysis wiki
1. source verification
2. research thesis
3. candidate review
4. shadow decision
5. wiki paper review
6. production approval checklist
```

## Stage 0: Analysis Wiki

Purpose:

```text
Collect, summarize, classify, and link source material.
```

Allowed:

```text
raw
extracted
interpreted
```

Not allowed:

```text
buy
sell
candidate
signal
order
fill
ledger
stats
gate pass
```

## Stage 1: Source Verification

Purpose:

```text
Check source facts, dates, numbers, entities, and conflicts.
```

Allowed:

```text
pending
partial
verified
conflicted
rejected
unknown
```

Meaning:

```text
verified means source facts checked only.
```

## Stage 2: Research Thesis

Purpose:

```text
Create reviewable research hypotheses from verified sources.
```

Allowed:

```text
blocked_unverified_source
draft
review
rejected
archived
```

Meaning:

```text
thesis is not a trading decision.
```

## Stage 3: Candidate Review

Purpose:

```text
Place reviewed thesis items into a question-based review queue.
```

Allowed:

```text
blocked_not_reviewed
blocked_policy_unmapped
review_queue
rejected
archived
```

Meaning:

```text
candidate_review_allowed means review queue only, not trading candidate.
```

## Stage 4: Shadow Decision

Purpose:

```text
Record shadow-only watch, block, or ignore outcomes.
```

Allowed:

```text
queued
watch_only
blocked
ignored
archived
```

Required:

```yaml
operational_effect: false
```

## Stage 5: Wiki Paper Review

Purpose:

```text
Record wiki-only paper review intent.
```

Allowed:

```text
blocked_not_ready
blocked_no_policy
review_queue
sim_record_only
rejected
archived
```

Required:

```yaml
roota_paper_engine_connected: false
broker_connected: false
order_creation_allowed: false
fill_creation_allowed: false
ledger_update_allowed: false
stats_update_allowed: false
operational_effect: false
```

## Stage 6: Production Approval Checklist

Purpose:

```text
Record approval requirements only.
```

Allowed:

```text
blocked
review_only
rejected
archived
```

There is no approved status in this wiki schema.

Required:

```yaml
explicit_user_approval_present: false
production_connection_allowed: false
broker_connection_allowed: false
order_creation_allowed: false
fill_creation_allowed: false
ledger_update_allowed: false
stats_update_allowed: false
gate_change_allowed: false
operational_effect: false
```

## Global Safety Defaults

Every trading-related note must keep:

```yaml
trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
```

## Global Boundary

This wiki may organize research and review records.

It must not create or modify:

```text
RootA trading logic
RootA paper engine
broker connection
orders
fills
ledger rows
stats
gate status
lock meaning
thresholds
scores
```

