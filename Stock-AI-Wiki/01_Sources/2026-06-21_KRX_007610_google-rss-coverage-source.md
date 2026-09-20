---
id: source-2026-06-21-KRX-007610-google-rss-coverage
type: source
title: KRX 007610 Google RSS Coverage Source
created: 2026-06-21
updated: 2026-06-21
status: raw
stage: 0

market: KRX
ticker: "007610"
company: 선도전기
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-21

analysis:
  summary: Local coverage report row shows news coverage for KRX 007610.
  key_facts:
    - code=007610
    - name=선도전기
    - naver_article_count=1
    - google_rss_article_count=4
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 007610
    - 선도전기
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

# KRX 007610 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=007610`
- `name=선도전기`
- `naver_article_count=1`
- `google_rss_article_count=4`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 007610.

## RSS Item Metadata
- Title: `특징주, 선도전기-전력설비 테마 상승세에 10.99% ↑ - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-06-17T10:43:26+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE03c1ozdUpjU1pYOERYd1dIal9yM3pFTV80am1zUFhXOHRKcll6czJiY1ZqVGJhcXJDdDRZM1FORU1FMjQtM1QybWJLV3lweU1xZXc?oc=5`

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
- Updated at: `2026-06-21T01:05:13+09:00`
- Company: [[KRX_007610_선도전기]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-21.json`
- Latest observation title: `특징주, 선도전기-전력설비 테마 상승세에 10.99% ↑ - 매일경제 마켓`
- Latest observation source: `매일경제 마켓`
- Latest observation published_at: `2026-06-17T10:43:26+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE03c1ozdUpjU1pYOERYd1dIal9yM3pFTV80am1zUFhXOHRKcll6czJiY1ZqVGJhcXJDdDRZM1FORU1FMjQtM1QybWJLV3lweU1xZXc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
