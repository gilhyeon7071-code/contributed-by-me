# AI Analysis Rules

## Allowed Tasks

- Summarize source material.
- Extract facts, numbers, dates, people, companies, and events.
- Separate fact from interpretation.
- List uncertainty and missing evidence.
- Link related notes.
- Generate questions for human review.

## Prohibited Tasks

- Do not decide buy or sell.
- Do not recommend position size.
- Do not create entry or exit signals.
- Do not mark trading approval as true.
- Do not mark direct candidate allowed as true.
- Do not mark execution allowed as true.
- Do not change gate, lock, score, threshold, order, fill, ledger, or stats meaning.
- Do not invent missing values.

## Output Rule

Always separate:

```text
Facts
Interpretation
Uncertainty
Questions
```

## Default Trading Safety

```yaml
trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
```

