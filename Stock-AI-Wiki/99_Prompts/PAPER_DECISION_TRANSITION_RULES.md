# Paper Decision Transition Rules

## Allowed Transitions

```text
shadow:watch_only -> paper:review_queue
paper:review_queue -> paper:sim_record_only
paper:review_queue -> paper:rejected
paper:sim_record_only -> paper:archived
paper:rejected -> paper:archived
```

## Blocked Transitions

```text
raw -> paper
verification:verified -> paper
thesis:review -> paper
candidate_review:review_queue -> paper
shadow:queued -> paper
shadow:blocked -> paper
shadow:ignored -> paper
```

## Required Checks

Before setting `paper_status: review_queue`, confirm:

```text
source note exists
verification note exists
thesis note exists
candidate review note exists
shadow note exists
shadow.shadow_status == watch_only
shadow.operational_effect == false
paper operational_effect == false
roota_paper_engine_connected == false
broker_connected == false
order/fill/ledger/stats update allowed == false
trading safety fields remain false
```

## Stage 5 Not Allowed

Stage 5 cannot write:

```text
RootA paper order
RootA paper fill
live order
broker submit
ledger row
stats update
gate pass
trading approved
execution allowed
```

## Safety Lock

These fields remain false in Stage 5:

```yaml
roota_paper_engine_connected: false
broker_connected: false
order_creation_allowed: false
fill_creation_allowed: false
ledger_update_allowed: false
stats_update_allowed: false
operational_effect: false
used_for_trading: false
trading_approved: false
direct_candidate_allowed: false
execution_allowed: false
gate_checked: false
```

