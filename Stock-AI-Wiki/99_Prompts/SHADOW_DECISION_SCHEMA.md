# Shadow Decision Schema

## Stage 4 Boundary

Stage 4 records shadow-only decisions.

Shadow means no operational effect.

It does not create live or paper orders, fills, ledger rows, stats, broker submissions, gate pass status, or execution approval.

```text
Stage 0: analysis wiki
Stage 1: source verification
Stage 2: research thesis
Stage 3: candidate review
Stage 4: shadow decision
```

## Meaning

Shadow decision can say:

```text
If reviewed in isolation, this would be watched, blocked, or ignored in shadow.
```

It cannot say:

```text
buy
sell
submit order
paper order
broker order
fill
ledger update
stats update
gate pass
execution allowed
```

## Frontmatter

```yaml
shadow_decision:
  shadow_status: queued
  source_candidate_review_required: true
  candidate_review_status_required: review_queue
  candidate_review_status_observed:
  shadow_decision_allowed: false
  shadow_action: none
  shadow_reason:
  policy_mapping_checked: false
  policy_mapping_result: not_checked
  human_review_required: true
  operational_effect: false

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
```

## Shadow Status Values

```text
queued          ready for shadow-only review
watch_only      shadow says watch only
blocked         shadow says do not proceed
ignored         shadow says no action
archived        no longer active
```

## Shadow Action Values

```text
none
watch_only
block
ignore
```

No value may create an order, fill, ledger row, stats update, or gate pass.

