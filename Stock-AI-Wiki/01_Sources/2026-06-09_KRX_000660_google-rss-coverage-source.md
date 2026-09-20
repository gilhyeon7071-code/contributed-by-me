---
id: source-2026-06-09-KRX-000660-google-rss-coverage
type: source
title: KRX 000660 Google RSS Coverage Source
created: 2026-06-09
updated: 2026-06-09
status: raw
stage: 0

market: KRX
ticker: "000660"
company: nan
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-09

analysis:
  summary: Local coverage report row shows news coverage for KRX 000660.
  key_facts:
    - code=000660
    - name=nan
    - naver_article_count=7
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 000660
    - nan
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is not stored locally.

verification:
  verified: false
  source_count: 1
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
  change_reason: generated coverage source note
---

# KRX 000660 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=000660`
- `name=nan`
- `naver_article_count=7`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 000660.

## RSS Item Metadata
- Title: `2026 북중미 월드컵 실시간 경기 결과 및 일정 - BBC`
- Source: `BBC`
- Published at: `2026-06-01T15:57:28+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTFA3T1hvYWpZUnU5Y080azVfVDR2YWllM3F6b0VjMm5WMUpYRGVSbDcwMWJ3ZzV6eGh1dHdNcVZKSVp5cmJLYkQxLW9pWUljYl9fVWVzZThobHZ4OVpn0gFiQVVfeXFMUDF2LVlWVk9xRWRkQ1hlVWZ6blFraGJqOHlpZFJSWXZfT2FaOU1XWlNzVjJkd0J5cUE2SkdMXzQxV2RSaHJWVlFBMVlJS2g2NWV6S0tsaXNRcTY3dHJSa0lQY0E?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-09T15:05:13+09:00`
- Company: [[KRX_000660_nan]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-09.json`
- Latest observation title: `2026 북중미 월드컵 실시간 경기 결과 및 일정 - BBC`
- Latest observation source: `BBC`
- Latest observation published_at: `2026-06-01T15:57:28+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTFA3T1hvYWpZUnU5Y080azVfVDR2YWllM3F6b0VjMm5WMUpYRGVSbDcwMWJ3ZzV6eGh1dHdNcVZKSVp5cmJLYkQxLW9pWUljYl9fVWVzZThobHZ4OVpn0gFiQVVfeXFMUDF2LVlWVk9xRWRkQ1hlVWZ6blFraGJqOHlpZFJSWXZfT2FaOU1XWlNzVjJkd0J5cUE2SkdMXzQxV2RSaHJWVlFBMVlJS2g2NWV6S0tsaXNRcTY3dHJSa0lQY0E?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
