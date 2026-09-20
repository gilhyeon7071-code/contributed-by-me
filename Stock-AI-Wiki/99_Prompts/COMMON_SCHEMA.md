# Common Schema

Use this frontmatter for Stage 0 wiki notes.

```yaml
---
id:
type:
title:
created:
updated:
status: raw
stage: 0

market:
ticker:
company:
theme: []

source:
  type:
  name:
  url:
  published_at:
  collected_at:

analysis:
  summary:
  key_facts: []
  related_entities: []
  possible_impact:
  uncertainty: []

verification:
  verified: false
  source_count: 0
  confidence: unknown
  conflict_exists: false

trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
  policy_link:

audit:
  human_reviewed: false
  ai_generated: false
  last_reviewed_at:
  change_reason:
---
```

## Status Values

```text
raw          original or lightly captured material
extracted    facts, dates, numbers, entities extracted
interpreted  possible meaning and uncertainty written
verified     reserved for later source verification
archived     no longer active
```

## Type Values

```text
source
company
theme
news
filing
earnings
macro
price_action
daily_review
question
```

