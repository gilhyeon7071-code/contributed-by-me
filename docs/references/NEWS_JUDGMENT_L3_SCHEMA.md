# News Judgment L3 Schema

## Purpose

This document defines the minimum schema and decision rules required before a news judgment can affect the paper order-precheck execution path.

This is not a live-trading approval rule.

## Layer Names

| Layer | Meaning |
|---|---|
| news_judgment_layer | Source judgment layer for stock, sector, market, macro, and policy news. |
| candidate_effect | Derived candidate-level effect consumed by candidate/order-precheck logic. |
| execution_allowed | Automatic L3 execution permission result. |
| reviewed_execution_allowed | Manual override for L3 execution permission. |

## Field Contract

| Field | Required | Type | Meaning |
|---|---:|---|---|
| judgment_id | yes | string | Stable id for one judgment row. |
| asof_ymd | yes | string YYYYMMDD | Judgment reference date. |
| source_url | yes | string | Source URL or local source id. |
| source_title | yes | string | Source title. |
| scope | yes | enum | `stock`, `sector`, `market`, `macro`, `policy`. |
| target | yes | string | Stock code, sector name, market name, macro item, or policy name. |
| related_codes | no | list/string | Related stock codes. Required for `stock` or sector-to-stock promotion. |
| sector_tag | no | string | Sector name/tag. |
| time_axis | yes | enum | `past`, `current`, `future`. |
| horizon | yes | enum | `intraday`, `short`, `medium`, `long`. |
| direction | yes | enum | `positive`, `negative`, `mixed`, `neutral`. |
| action | yes | enum | Source action: `boost`, `watch`, `reduce_size`, `penalize`, `block`, `ignore`. |
| candidate_effect | yes | enum | Derived effect: `positive_context`, `watch_only`, `avoid_chase`, `reduce_size_context`, `block_review`, `none`. |
| strength | yes | float 0-1 | Source signal strength. |
| confidence | yes | float 0-1 | Source confidence. |
| confirm_level | yes | int 1-3 | 1=noise/early, 2=direction confirmed, 3=fact/event confirmed. |
| heat_state | no | enum | `normal`, `active`, `overheated`. Heat is not negative by itself. |
| active_for_l3 | yes | bool | True only if not stale and within allowed horizon. |
| source_trading_effect | yes | bool | Whether the source row itself is allowed to affect execution. Default false for generated/reference rows. |
| execution_allowed | yes | bool | Automatic L3 permission result after rules. |
| execution_denied_reason | yes | string | Reason when execution is not allowed. |
| reviewed_execution_allowed | yes | bool | Manual override. Default false. |
| reviewer | no | string | Reviewer id for manual override. |
| reviewed_at | no | ISO timestamp | Manual override time. |
| override_reason | no | string | Required if `reviewed_execution_allowed=true`. |
| promoted_from_scope | no | enum | Original scope if sector/market judgment is promoted to stock. |
| promotion_reason | no | string | Why promotion to stock-level effect was allowed. |
| consumer | yes | enum/list | `risk`, `candidate`, `sector`, `report`. |
| processing_state | yes | enum | `raw`, `normalized`, `promoted`, `consumed`, `blocked`, `reported`. |

## Action vs Candidate Effect

`action` and `candidate_effect` must coexist.

| Field | Role |
|---|---|
| action | Original news implication action. It preserves what the source said. |
| candidate_effect | Derived candidate-level effect after topic, scope, confidence, and conflict rules. |

Do not replace `action` with `candidate_effect`.

## L3 Automatic Permission Rule

L3 means the judgment can affect the paper order-precheck path by blocking a candidate or reducing candidate quantity.

Minimum automatic L3 rule:

```text
scope == stock
candidate_effect in {block_review, avoid_chase, reduce_size_context}
confirm_level >= 2
confidence >= 0.60
active_for_l3 == true
source_trading_effect == true
execution_allowed == true
```

If `source_trading_effect == false`, automatic L3 is forbidden unless `reviewed_execution_allowed == true`.

If `reviewed_execution_allowed == true`, the row must include:

```text
reviewer
reviewed_at
override_reason
```

## Sector To Stock Promotion

Sector-level judgments do not directly become L3 stock execution effects.

Promotion requires a derived row with:

```text
scope = stock
promoted_from_scope = sector
promotion_reason
related_codes
candidate_effect
execution_allowed
```

Promotion is allowed only when the target stock is already in the candidate universe or an approved watchlist. It must not append new direct buy candidates.

## Confidence Aggregation

For L3, `avg_confidence` must be calculated from:

```text
active rows only
same topic_key
same stock code
same horizon bucket
stale rows excluded
```

Rows older than the active lag rule must not enter L3 confidence averages.

## Conflict Resolution

When multiple active judgments exist for the same code/topic/horizon:

```text
block_review
> reduce_size_context
> avoid_chase
> watch_only
> positive_context
> none
```

Positive context never cancels a block or reduce effect by itself.

## Consumer Order

Consumers must run in fixed order:

```text
risk -> candidate -> sector -> report
```

The report consumer records all rows, including rows already blocked by risk or candidate consumers.

## Current Gap

As of 2026-06-29, current `manual_news_implications.jsonl` does not contain all required L3 fields.

Therefore existing `news_topic_execution_effect` use must be treated as an experiment until the schema above is implemented.
