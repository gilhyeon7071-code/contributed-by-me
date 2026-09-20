---
id: source-2026-07-28-KRX-251970-google-rss-coverage
type: source
title: KRX 251970 Google RSS Coverage Source
created: 2026-07-28
updated: 2026-07-28
status: raw
stage: 0

market: KRX
ticker: "251970"
company: 펌텍코리아
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 251970.
  key_facts:
    - code=251970
    - name=펌텍코리아
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 251970
    - 펌텍코리아
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

# KRX 251970 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=251970`
- `name=펌텍코리아`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 251970.

## RSS Item Metadata
- Title: `펌텍코리아 투자분석 2026. 07. 26 - 주달`
- Source: `주달`
- Published at: `2026-07-27T22:04:43+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE00VGUzQURlZHRJT2pQZGpfdE5nM0JZMXhLX09CYkh3RHNZUE4xRVdHSm9FQnpaazRlUkJ4eDJxSHVTQ2FhYUlHWTE1ai1VNjM2a01rWEdXYlpxSVNFWDhlMWNVVTZSVEdUUXRiVWVmRFNwbVU?oc=5`

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
- Updated at: `2026-07-28T21:05:05+09:00`
- Company: [[KRX_251970_펌텍코리아]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `펌텍코리아(251970)밀릴때마다 물량 모아둘 기회로 보이며 이후 전망 및 대응전략. - ThinkPool`
- Latest observation source: `ThinkPool`
- Latest observation published_at: `2026-07-27T15:53:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTFBURmN4SFd0X3d5V05OSFUzenhVWVV4SnFnRGdOcmxuZDQtWVNzVHBWaTBYb3VqZTZ6bjg5b21LMkQzM3lNdFAwOXZheEpING9HNXdJUjJrbUVzYVlY?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
