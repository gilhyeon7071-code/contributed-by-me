# Stage 4 Shadow Result Examples

## Purpose

These examples show allowed shadow-only result states.

## Input Note

```text
100_Shadow/SAMPLE_SHADOW_DECISION_QUEUED.md
```

## Output Notes

```text
100_Shadow/SAMPLE_SHADOW_DECISION_WATCH_ONLY.md
100_Shadow/SAMPLE_SHADOW_DECISION_BLOCKED.md
100_Shadow/SAMPLE_SHADOW_DECISION_IGNORED.md
```

## Allowed Shadow Results

```yaml
shadow_decision:
  shadow_status: watch_only
  shadow_action: watch_only
  operational_effect: false
```

```yaml
shadow_decision:
  shadow_status: blocked
  shadow_action: block
  operational_effect: false
```

```yaml
shadow_decision:
  shadow_status: ignored
  shadow_action: ignore
  operational_effect: false
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

## Still Not Allowed

Shadow result notes cannot create:

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

