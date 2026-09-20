# Static Population Input Contract V1

Purpose: provide a stable local source for historical KOSPI/KOSDAQ population
membership before Candidate-Generation V2 performance exploration starts.

This contract is research-only. It must not change Gate, LOCK, orders, broker,
paper runtime, scores, or operating thresholds.

## Grain

One row is one listing interval for one security:

`code x market x listed_date x delisted_date`

The validator expands this interval later into `as_of_date x code` population
membership. Do not provide future-derived labels that only became known after
the `as_of` date unless the source evidence is explicit and dated.

## Required Columns

- `code`: six-digit KRX short code.
- `name`: security name at or near the interval.
- `market`: `KOSPI` or `KOSDAQ`.
- `security_type`: one of `COMMON`, `PREFERRED`, `REIT`, `SPAC`, `ETF`, `ETN`,
  `OTHER`.
- `listed_date`: `YYYYMMDD`.
- `delisted_date`: `YYYYMMDD` or blank if still listed.
- `source`: short source id, for example `manual_dump_202607`.
- `evidence_source`: file, URL, vendor export, or note that supports the row.

## Required Rules

- `code` must match `^[0-9]{6}$`.
- `market` must be `KOSPI` or `KOSDAQ` for V2 candidate population.
- `security_type` must be explicit. Name heuristics are allowed only when
  `source` and `evidence_source` state that the classification is heuristic.
- `listed_date` must be present and valid.
- `delisted_date`, when present, must be valid and not earlier than
  `listed_date`.
- Duplicate `code x market x listed_date` rows are invalid.
- Overlapping intervals for the same `code x market` are invalid unless they
  represent separate non-overlapping relisting periods.

## V2 Use

Only rows with `security_type=COMMON` are eligible for the default V2 common
equity population. Other security types may be retained for diagnostics but are
not part of the default candidate population unless a later preregistration
explicitly changes the population rule.

## Validation Output

The validator must write:

- JSON status report
- CSV issue table
- Markdown summary

The validator must record:

- `performance_calculated=false`
- `candidate_selection_calculated=false`
- `operational_change=false`
- `broker_order=false`

PASS means the static input can be used to build a research population source.
It does not mean any candidate rule, strategy, signal, or operating logic is
approved.
