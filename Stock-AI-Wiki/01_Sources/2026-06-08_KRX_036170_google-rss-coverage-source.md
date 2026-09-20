---
id: source-2026-06-08-KRX-036170-google-rss-coverage
type: source
title: KRX 036170 Google RSS Coverage Source
created: 2026-06-08
updated: 2026-06-08
status: raw
stage: 0

market: KRX
ticker: "036170"
company: 에이치엠넥스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-08

analysis:
  summary: Local coverage report row shows news coverage for KRX 036170.
  key_facts:
    - code=036170
    - name=에이치엠넥스
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 036170
    - 에이치엠넥스
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

# KRX 036170 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=036170`
- `name=에이치엠넥스`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 036170.

## RSS Item Metadata
- Title: `[장중수급포착] 에이치엠넥스, 외국인/기관 동시 순매수… 주가 +8.67% - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-06-08T10:15:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5NakFFS3FFMTR1b1o5azZzYkV6RnpxZ2hmUXpyamFwRnJQbWx3Y2l2Mlh4Wnh4QjAtd1RFMFJUVHl5S3lDdFBpbnYzM3pWSTg2alc3cExwQUJRQnJj?oc=5`

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
- Updated at: `2026-08-21T19:19:12+09:00`
- Company: [[KRX_036170_에이치엠넥스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-08.json`
- Latest observation title: `[장중수급포착] 에이치엠넥스, 외국인/기관 동시 순매수… 주가 +8.67% - 뉴스핌`
- Latest observation source: `뉴스핌`
- Latest observation published_at: `2026-06-08T10:15:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5NakFFS3FFMTR1b1o5azZzYkV6RnpxZ2hmUXpyamFwRnJQbWx3Y2l2Mlh4Wnh4QjAtd1RFMFJUVHl5S3lDdFBpbnYzM3pWSTg2alc3cExwQUJRQnJj?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
