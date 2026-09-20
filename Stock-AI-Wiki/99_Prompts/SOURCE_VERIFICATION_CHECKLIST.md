# Source Verification Checklist

## Purpose

Use this checklist before changing a note from `raw`, `extracted`, or `interpreted` to `verified`.

## Evidence Checks

```text
[ ] Original source is available.
[ ] Source type is recorded.
[ ] Source name is recorded.
[ ] Source date is recorded or explicitly unknown.
[ ] Collection date is recorded.
[ ] URL or local evidence path is recorded, if available.
[ ] Numbers are copied exactly from source.
[ ] Dates are copied exactly from source.
[ ] Company names and tickers are checked.
[ ] Market is checked.
[ ] Related theme is not guessed.
[ ] Conflicting sources were checked.
[ ] Remaining uncertainty is written.
```

## Source Quality Labels

```text
primary       official disclosure, exchange, company IR, regulator, original dataset
secondary     press article, analyst article, reproduced table
derived       AI summary, user summary, copied aggregation
unknown       source quality not clear
```

## Conflict Handling

If sources conflict:

```yaml
verification:
  verified: false
  verification_status: conflicted
  conflict_exists: true
  conflict_summary:
```

Do not resolve conflict by guessing.

## Missing Values

Use explicit unknown values:

```text
unknown
not_available
not_checked
```

Do not invent missing values.

## Trading Boundary

Even if a note passes source verification, keep:

```yaml
trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
```

