---
id: source-2026-06-13-KRX-027360-google-rss-coverage
type: source
title: KRX 027360 Google RSS Coverage Source
created: 2026-06-13
updated: 2026-06-13
status: raw
stage: 0

market: KRX
ticker: "027360"
company: 아주IB투자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-13

analysis:
  summary: Local coverage report row shows news coverage for KRX 027360.
  key_facts:
    - code=027360
    - name=아주IB투자
    - naver_article_count=2
    - google_rss_article_count=2
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 027360
    - 아주IB투자
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

# KRX 027360 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=027360`
- `name=아주IB투자`
- `naver_article_count=2`
- `google_rss_article_count=2`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 027360.

## RSS Item Metadata
- Title: `아주IB투자 오버행 여전…㈜아주 블록딜 120만주 남았다 - 톱데일리`
- Source: `톱데일리`
- Published at: `2026-06-11T17:17:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUEFVX3lxTE5mdkpORUZRcG00bU5BX181VHFwZDhSbHlwbnZYYU1ueUZHbUFtbWRLdHkya3RRRDZPU3NONzNHQnkyLU1LdkRWd3JGR2FrakF0?oc=5`

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
- Updated at: `2026-06-13T11:10:39+09:00`
- Company: [[KRX_027360_아주IB투자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-13.json`
- Latest observation title: `아주IB투자 오버행 여전…㈜아주 블록딜 120만주 남았다 - 톱데일리`
- Latest observation source: `톱데일리`
- Latest observation published_at: `2026-06-11T17:17:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUEFVX3lxTE5mdkpORUZRcG00bU5BX181VHFwZDhSbHlwbnZYYU1ueUZHbUFtbWRLdHkya3RRRDZPU3NONzNHQnkyLU1LdkRWd3JGR2FrakF0?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
