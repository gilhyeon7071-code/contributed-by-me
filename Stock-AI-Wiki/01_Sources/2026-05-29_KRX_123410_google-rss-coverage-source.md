---
id: source-2026-05-29-KRX-123410-google-rss-coverage
type: source
title: KRX 123410 Google RSS Coverage Source
created: 2026-05-29
updated: 2026-05-29
status: raw
stage: 0

market: KRX
ticker: "123410"
company: 코리아에프티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-29

analysis:
  summary: Local coverage report row shows news coverage for KRX 123410.
  key_facts:
    - code=123410
    - name=코리아에프티
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 123410
    - 코리아에프티
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

# KRX 123410 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=123410`
- `name=코리아에프티`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 123410.

## RSS Item Metadata
- Title: `코리아에프티 투자분석 2026. 05. 27 - 주달`
- Source: `주달`
- Published at: `2026-05-27T16:49:24+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBfS3N4ajBwLUExRjVURjZQQnBRbzB3UUY0LWFheU1OdXYyTkprTlRuN2l5VWRtX0hyTVFsWWt3R0RuQkxvZ1R6YlNTRFRDWkNtVHc0bjN3VG5KekJ2OExKeXNFZ1h1WVhvNzdBRmlza1pYX0k?oc=5`

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
- Updated at: `2026-05-29T08:05:03+09:00`
- Company: [[KRX_123410_코리아에프티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-29.json`
- Latest observation title: `코리아에프티 투자분석 2026. 05. 27 - 주달`
- Latest observation source: `주달`
- Latest observation published_at: `2026-05-27T16:49:24+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBfS3N4ajBwLUExRjVURjZQQnBRbzB3UUY0LWFheU1OdXYyTkprTlRuN2l5VWRtX0hyTVFsWWt3R0RuQkxvZ1R6YlNTRFRDWkNtVHc0bjN3VG5KekJ2OExKeXNFZ1h1WVhvNzdBRmlza1pYX0k?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
