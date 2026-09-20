# Stock AI Wiki

## Scope

This vault is a read-only analysis wiki for stock research notes.

Allowed:
- Collect source notes.
- Summarize facts.
- Extract dates, numbers, entities, and uncertainty.
- Link companies, themes, news, filings, earnings, macro notes, and price observations.
- Generate review questions.

Not allowed:
- Decide buy or sell.
- Recommend position size.
- Create entry or exit signals.
- Mark any gate as passed.
- Connect directly to orders, fills, ledger, stats, paper trading, or broker execution.
- Raise score, status, or confidence by hardcoding.

## Folder Map

```text
00_Inbox        temporary unprocessed notes
01_Sources      original sources and source records
10_Companies    company-level memory
20_Themes       theme-level memory
30_News         news analysis notes
40_Filings      filing and disclosure notes
50_Earnings     earnings and financial notes
60_Macro        macro, rates, FX, index, sector context
70_Price_Action price, volume, chart, and flow observations
80_Daily_Review daily market and research review
90_Questions    unresolved questions
99_Prompts      AI prompts and templates
100_Shadow      shadow-only decision records
110_Paper       wiki-only paper review records
120_Production_Approval production approval checklist records
```

## Stage Index

See `STAGE_INDEX.md` for the full stage map.

```text
0. analysis wiki
1. source verification
2. research thesis
3. candidate review
4. shadow decision
5. wiki paper review
6. production approval checklist
```

## Safety Contract

This wiki may organize research and review records.

It must not create or modify:

```text
RootA trading logic
RootA paper engine
broker connection
orders
fills
ledger rows
stats
gate status
```

All trading safety fields must remain false by default.
