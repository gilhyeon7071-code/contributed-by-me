# Paper Decision Schema

## Stage 5 Boundary

Stage 5 records paper-simulation intent inside this wiki only.

It does not connect to RootA paper engine, live trading, broker submission, orders, fills, ledger rows, stats, or gate pass status.

```text
Stage 0: analysis wiki
Stage 1: source verification
Stage 2: research thesis
Stage 3: candidate review
Stage 4: shadow decision
Stage 5: wiki paper decision
```

## Meaning

Wiki paper decision can say:

```text
This shadow result may be reviewed for a separate paper-simulation record.
```

It cannot say:

```text
send paper order
create paper fill
write ledger
update stats
broker submit
gate pass
live trading approved
execution allowed
```

## Frontmatter

```yaml
paper_decision:
  paper_status: blocked_not_ready
  shadow_required: true
  shadow_status_required: watch_only
  shadow_status_observed:
  paper_review_allowed: false
  paper_simulation_allowed: false
  roota_paper_engine_connected: false
  broker_connected: false
  order_creation_allowed: false
  fill_creation_allowed: false
  ledger_update_allowed: false
  stats_update_allowed: false
  operational_effect: false
  blocking_reasons: []

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
```

## Paper Status Values

```text
blocked_not_ready       shadow result is not eligible
blocked_no_policy       policy mapping is missing
review_queue            ready for wiki paper review
sim_record_only         record-only paper simulation note
rejected                rejected for paper review
archived                no longer active
```

## Important Meaning

`paper_review_allowed: true` means only that a wiki note may enter paper review.

It does not mean RootA paper trading is enabled.

