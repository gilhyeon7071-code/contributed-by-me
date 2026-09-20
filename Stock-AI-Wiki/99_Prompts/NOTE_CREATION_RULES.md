# Note Creation Rules

## Stage 0 Boundary

Create notes only for analysis wiki use.

Do not create trading decisions, candidates, signals, gates, orders, fills, ledger rows, or stats.

## File Naming

Use stable names that can be linked from Obsidian.

```text
10_Companies/{market}_{ticker}_{company}.md
20_Themes/{theme}.md
30_News/{YYYY-MM-DD}_{market}_{ticker}_{short-title}.md
40_Filings/{YYYY-MM-DD}_{market}_{ticker}_{filing-type}.md
50_Earnings/{YYYY-MM-DD}_{market}_{ticker}_{period}.md
60_Macro/{YYYY-MM-DD}_{topic}.md
70_Price_Action/{YYYY-MM-DD}_{market}_{ticker}_{observation}.md
80_Daily_Review/{YYYY-MM-DD}.md
90_Questions/{YYYY-MM-DD}_{topic}.md
```

## ID Format

```text
company-{market}-{ticker}
theme-{slug}
news-{YYYY-MM-DD}-{market}-{ticker}-{seq}
filing-{YYYY-MM-DD}-{market}-{ticker}-{seq}
earnings-{YYYY-MM-DD}-{market}-{ticker}-{seq}
macro-{YYYY-MM-DD}-{slug}
price-{YYYY-MM-DD}-{market}-{ticker}-{seq}
review-{YYYY-MM-DD}
question-{YYYY-MM-DD}-{seq}
```

## Required Sections

Every note must separate:

```text
Facts
Interpretation
Uncertainty
Questions
```

If a value is unknown, write `unknown`.

Do not fill missing values by guess.

## Safety Defaults

Every Stage 0 note must keep these values:

```yaml
trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
```

## Allowed Links

Allowed:

```text
source -> news / filing / earnings / macro / price_action
news / filing / earnings / macro / price_action -> company / theme
daily_review -> all analysis notes
question -> related analysis notes
```

Not allowed in Stage 0:

```text
analysis note -> order
analysis note -> fill
analysis note -> ledger
analysis note -> broker
analysis note -> gate pass
analysis note -> trading approval
```

