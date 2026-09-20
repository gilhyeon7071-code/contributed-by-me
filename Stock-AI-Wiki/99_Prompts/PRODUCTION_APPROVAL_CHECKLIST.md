# Production Approval Checklist

## Purpose

This checklist prevents the wiki from being mistaken for a trading system.

## Mandatory Checks

```text
[ ] Explicit user approval exists for production integration.
[ ] RootA policy mapping is written and verified.
[ ] RootA runtime evidence exists.
[ ] RootA FAIL-CLOSED behavior is verified.
[ ] RootA regression verification is complete.
[ ] Paper/broker date mixing is excluded.
[ ] Orders, fills, ledger, and stats chain is verified externally.
[ ] Gate meaning is unchanged.
[ ] Lock meaning is unchanged.
[ ] Rollback plan exists.
```

## Default Result

If any item is missing:

```yaml
production_approval:
  approval_status: blocked
  production_connection_allowed: false
  broker_connection_allowed: false
  order_creation_allowed: false
  fill_creation_allowed: false
  ledger_update_allowed: false
  stats_update_allowed: false
  gate_change_allowed: false
  operational_effect: false
```

## Important Rule

This wiki may document approval requirements.

It must not approve production by itself.

## Not Allowed Here

```text
RootA code patch
RootA batch patch
Gate change
Lock change
order creation
fill creation
ledger update
stats update
broker connection
production approval
```

