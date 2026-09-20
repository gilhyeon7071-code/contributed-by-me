# Date and As-of Interpretation

## Scope

This document defines how the LLM Wiki should interpret RootA date fields.

It is a read-only interpretation rule. It does not change Gate, STOP, LOCK,
risk, score, order, fill, ledger, stats, scheduler, or trading runtime behavior.

## Fields

| field | meaning | source |
|---|---|---|
| `D` | latest BUY ymd from `paper\fills.csv` | fills/current trade chain |
| `asof_ymd` | score artifact as-of date | `2_Logs\final_score_merge_status_latest.json` |
| `score_expected_ymd` | expected score date for the current session | latest freshness source artifact |
| `score_expected_mode` | reason for expected date selection | latest freshness source artifact |

## Interpretation Rules

1. Candidate and score freshness must be judged against `score_expected_ymd`.
2. Order, fill, ledger, and stats interpretation must still surface strict `D`
   versus `asof_ymd` mismatch.
3. `score_asof_matches_expected=true` means the score artifact matches the
   expected session date.
4. `strict_asof_matches_D=false` does not automatically mean the score artifact
   is stale, but it remains a STOP concern for order/fill/ledger/stat
   interpretation until canonical RootA artifacts are inspected.
5. `score_expected_mode=session_previous` can be valid when the current trading
   session uses the previous market date as the score source.

## Status Labels

| status | condition | interpretation |
|---|---|---|
| `READY` | orders exec exists and score as-of matches expected date | score context is aligned with expected session source |
| `REVIEW_STRICT_D_MISMATCH` | `READY` plus strict `D` mismatch | score context can be current, but order/fill/ledger/stat interpretation needs extra care |
| `BLOCKED` | orders exec is missing or score as-of does not match expected date | do not treat current score context as aligned |

## Boundary

These labels are for LLM retrieval and reporting only.

They do not approve trading, loosen fail-closed behavior, change gate semantics,
or replace the canonical RootA validation checks.
