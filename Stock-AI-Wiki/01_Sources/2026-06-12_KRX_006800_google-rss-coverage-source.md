---
id: source-2026-06-12-KRX-006800-google-rss-coverage
type: source
title: KRX 006800 Google RSS Coverage Source
created: 2026-06-12
updated: 2026-06-12
status: raw
stage: 0

market: KRX
ticker: "006800"
company: nan
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-12

analysis:
  summary: Local coverage report row shows news coverage for KRX 006800.
  key_facts:
    - code=006800
    - name=nan
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=28
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 006800
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

# KRX 006800 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=006800`
- `name=nan`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=28`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 006800.

## RSS Item Metadata
- Title: `2026 북중미 월드컵 실시간 경기 결과 및 일정 - BBC`
- Source: `BBC`
- Published at: `2026-06-01T16:00:00+09:00`
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
- Updated at: `2026-08-21T19:20:30+09:00`
- Company: [[KRX_006800_nan]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-12.json`
- Latest observation title: `2026 북중미 월드컵 실시간 경기 결과 및 일정 - BBC`
- Latest observation source: `BBC`
- Latest observation published_at: `2026-06-01T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTFA3T1hvYWpZUnU5Y080azVfVDR2YWllM3F6b0VjMm5WMUpYRGVSbDcwMWJ3ZzV6eGh1dHdNcVZKSVp5cmJLYkQxLW9pWUljYl9fVWVzZThobHZ4OVpn0gFiQVVfeXFMUDF2LVlWVk9xRWRkQ1hlVWZ6blFraGJqOHlpZFJSWXZfT2FaOU1XWlNzVjJkd0J5cUE2SkdMXzQxV2RSaHJWVlFBMVlJS2g2NWV6S0tsaXNRcTY3dHJSa0lQY0E?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
