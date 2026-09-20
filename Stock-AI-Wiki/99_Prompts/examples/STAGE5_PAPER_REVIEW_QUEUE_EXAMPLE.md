# Stage 5 Paper Review Queue Example

## Purpose

This example shows the normal Stage 5 wiki-only paper review path.

## Input Note

```text
100_Shadow/SAMPLE_SHADOW_DECISION_WATCH_ONLY.md
```

## Output Note

```text
110_Paper/SAMPLE_PAPER_DECISION_REVIEW_QUEUE.md
```

## Required Input State

```yaml
shadow_decision:
  shadow_status: watch_only
  shadow_action: watch_only
  operational_effect: false
```

## Allowed Output State

```yaml
paper_decision:
  paper_status: review_queue
  paper_review_allowed: true
  paper_simulation_allowed: false
  roota_paper_engine_connected: false
  broker_connected: false
  order_creation_allowed: false
  fill_creation_allowed: false
  ledger_update_allowed: false
  stats_update_allowed: false
  operational_effect: false
```

## Meaning

`paper_review_allowed: true` means only that this wiki note may be reviewed for paper simulation.

It does not connect to RootA paper trading.

## Still Not Allowed

```text
RootA paper order
RootA paper fill
broker submit
ledger row
stats update
gate pass
trading approved
execution allowed
```

