---
id: source-2026-05-25-KRX-039490-google-rss-coverage
type: source
title: KRX 039490 Google RSS Coverage Source
created: 2026-05-25
updated: 2026-05-25
status: raw
stage: 0

market: KRX
ticker: "039490"
company: 키움증권
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Local coverage report row shows news coverage for KRX 039490.
  key_facts:
    - code=039490
    - name=키움증권
    - naver_article_count=2
    - google_rss_article_count=19
    - kis_title_count=13
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 039490
    - 키움증권
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

# KRX 039490 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=039490`
- `name=키움증권`
- `naver_article_count=2`
- `google_rss_article_count=19`
- `kis_title_count=13`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 039490.

## RSS Item Metadata
- Title: `키움증권 "LS, 목표가 100%↑…자사주소각 등 정책에 힘입을 듯" - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-05-22T08:42:24+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE52a3BVZFNQNUpRU2dZSGxfTWVqbkllWFVIMjhBeVVNYV9WV1dBTXl5R3RHbzJKa0RuR1ppS3Rpcl92bGhXbUE4VzJjb3ZiaFJ5RzBIbUh0SnMxbGvSAWBBVV95cUxQbEVONkJRNWI3bExKU0ppeENteUdVb3lxWVdNY3RzNXBtRTdneHhUdGtCS2Y4bVFzcXdjb0h4TThyZTk0MklZVHMtN1lrZlJXU0NSa2NaREdJaEJpd0ZSc2I?oc=5`

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
- Updated at: `2026-05-25T16:05:04+09:00`
- Company: [[KRX_039490_키움증권]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `키움증권 "LS, 목표가 100%↑…자사주소각 등 정책에 힘입을 듯" - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-05-22T08:42:24+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE52a3BVZFNQNUpRU2dZSGxfTWVqbkllWFVIMjhBeVVNYV9WV1dBTXl5R3RHbzJKa0RuR1ppS3Rpcl92bGhXbUE4VzJjb3ZiaFJ5RzBIbUh0SnMxbGvSAWBBVV95cUxQbEVONkJRNWI3bExKU0ppeENteUdVb3lxWVdNY3RzNXBtRTdneHhUdGtCS2Y4bVFzcXdjb0h4TThyZTk0MklZVHMtN1lrZlJXU0NSa2NaREdJaEJpd0ZSc2I?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
