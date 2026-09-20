# Candidate Review Schema

## Stage 3 Boundary

Stage 3 is candidate review only.

It can ask whether a research thesis is worth reviewing against a separate trading policy later.

It does not create buy or sell decisions, entry or exit signals, orders, fills, ledger rows, stats, or gate pass status.

```text
Stage 0: analysis wiki
Stage 1: source verification
Stage 2: research thesis
Stage 3: candidate review
```

## Meaning

Candidate review means:

```text
This thesis may be worth human or policy review.
```

It does not mean:

```text
trading candidate
order candidate
approved candidate
buy
sell
signal
gate pass
execution allowed
```

## Frontmatter

```yaml
candidate_review:
  review_status: blocked_not_reviewed
  thesis_required: true
  thesis_status_required: review
  thesis_status_observed:
  candidate_review_allowed: false
  policy_mapping_checked: false
  human_review_required: true
  review_questions: []
  blocking_reasons: []

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
```

## Review Status Values

```text
blocked_not_reviewed       thesis is not ready for review
blocked_policy_unmapped    no policy mapping exists
review_queue               ready for human/policy review
rejected                   not worth further review
archived                   no longer active
```

## Minimum Entry Condition

To enter `review_queue`:

```yaml
thesis:
  thesis_status: review
  source_verified: true
```

If the thesis is still `draft`, keep:

```yaml
candidate_review:
  review_status: blocked_not_reviewed
  candidate_review_allowed: false
```

