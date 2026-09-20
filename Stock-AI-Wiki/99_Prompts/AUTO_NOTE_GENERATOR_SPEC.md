# Auto Note Generator Spec

## Scope

The generator creates Stage 0-2 wiki notes from a local coverage CSV.

Default mode is dry-run.

It must not write or modify RootA trading code, paper engine data, broker data, orders, fills, ledger rows, stats, gates, locks, thresholds, or scores.

## Tool

```text
tools/generate_coverage_notes.py
```

## Input

```text
E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
```

Optional metadata input:

```text
E:\1_Data\2_Logs\google_news_rss_probe_latest.json
```

Optional article body archive input:

```text
local UTF-8 JSON matching 99_Prompts\ARTICLE_BODY_ARCHIVE_SCHEMA.md
```

## Output Notes

For each CSV row:

```text
01_Sources/{date}_KRX_{code}_google-rss-coverage-source.md
30_News/{date}_KRX_{code}_google-rss-coverage.md
01_Sources/{date}_KRX_{code}_google-rss-coverage-verification.md
20_Themes/{date}_KRX_{code}_google-rss-thesis-blocked.md
```

## Safety Behavior

```text
default: dry-run only
--apply: write missing files only
existing file: skip, never overwrite
verification: verified=false
verification_status: unknown
thesis_status: blocked_unverified_source
probe-json: read as UTF-8 or UTF-8-SIG when provided
RSS metadata: title/link/published/source only
article-archive-json: read as UTF-8 or UTF-8-SIG when provided
article body: can be attached from local archive, but not auto-verified
verified: always false for generated notes
verification_status: unknown
```

## Required Trading Defaults

```yaml
trading:
  used_for_trading: false
  trading_approved: false
  direct_candidate_allowed: false
  execution_allowed: false
  gate_checked: false
```
