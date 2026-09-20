# Stage 3 Candidate Review Queue Example

## Purpose

This example shows the normal Stage 3 path when a thesis is ready for review.

## Input Note

```text
20_Themes/SAMPLE_THESIS_REVIEW_EXAMPLE.md
```

## Output Note

```text
90_Questions/SAMPLE_CANDIDATE_REVIEW_QUEUE.md
```

## Required Input State

```yaml
thesis:
  thesis_status: review
  source_verified: true
```

## Allowed Output State

```yaml
candidate_review:
  review_status: review_queue
  candidate_review_allowed: true
  human_review_required: true
```

## Meaning

`candidate_review_allowed: true` means only that the thesis may enter a review queue.

It does not mean:

```text
buy
sell
trading candidate
order candidate
signal
entry
exit
gate pass
trading approved
execution allowed
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

