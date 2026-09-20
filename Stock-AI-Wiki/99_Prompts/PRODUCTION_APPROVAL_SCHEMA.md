# Production Approval Schema

## Stage 6 Boundary

Stage 6 is an approval checklist only.

It does not connect this wiki to RootA trading, paper trading, broker submission, orders, fills, ledger rows, stats, or gate pass status.

```text
Stage 0: analysis wiki
Stage 1: source verification
Stage 2: research thesis
Stage 3: candidate review
Stage 4: shadow decision
Stage 5: wiki paper review
Stage 6: production approval checklist
```

## Meaning

Production approval can record:

```text
What must be checked before any separate future production integration is considered.
```

It cannot record:

```text
production ready
trading approved
execution allowed
broker connected
gate pass
order allowed
fill allowed
ledger update allowed
stats update allowed
```

## Frontmatter

```yaml
production_approval:
  approval_status: blocked
  explicit_user_approval_required: true
  explicit_user_approval_present: false
  roota_policy_mapping_required: true
  roota_policy_mapping_verified: false
  roota_runtime_evidence_required: true
  roota_runtime_evidence_verified: false
  regression_required: true
  regression_verified: false
  fail_closed_verified: false
  production_connection_allowed: false
  broker_connection_allowed: false
  order_creation_allowed: false
  fill_creation_allowed: false
  ledger_update_allowed: false
  stats_update_allowed: false
  gate_change_allowed: false
  operational_effect: false
  blocking_reasons: []

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
```

## Approval Status Values

```text
blocked       one or more mandatory checks missing
review_only   checklist may be reviewed by human
rejected      production integration rejected
archived      no longer active
```

There is no `approved` status in this wiki schema.

