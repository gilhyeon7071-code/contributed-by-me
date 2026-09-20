---
id: source-2026-06-01-KRX-032640-google-rss-coverage
type: source
title: KRX 032640 Google RSS Coverage Source
created: 2026-06-01
updated: 2026-06-01
status: raw
stage: 0

market: KRX
ticker: "032640"
company: LG유플러스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-01

analysis:
  summary: Local coverage report row shows news coverage for KRX 032640.
  key_facts:
    - code=032640
    - name=LG유플러스
    - naver_article_count=2
    - google_rss_article_count=43
    - kis_title_count=30
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 032640
    - LG유플러스
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

# KRX 032640 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=032640`
- `name=LG유플러스`
- `naver_article_count=2`
- `google_rss_article_count=43`
- `kis_title_count=30`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 032640.

## RSS Item Metadata
- Title: `LG유플러스, 사내망 '와이파이 7' 전환…'스마트오피스' 고도화 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-05-31T10:06:16+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFBXMk15QVU0MDVkUkxDVFdlM2pFaTNnZmJ4OXFyeE5oV1dSdURwVDVRN2JDZm5tZVFoS2RHVFVYYlU1eVI3Z3FabERKcTRwWkE?oc=5`

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
- Updated at: `2026-06-01T08:42:44+09:00`
- Company: [[KRX_032640_LG유플러스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-01.json`
- Latest observation title: `LG유플러스, 사내망 '와이파이 7' 전환…'스마트오피스' 고도화 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-05-31T10:06:16+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFBXMk15QVU0MDVkUkxDVFdlM2pFaTNnZmJ4OXFyeE5oV1dSdURwVDVRN2JDZm5tZVFoS2RHVFVYYlU1eVI3Z3FabERKcTRwWkE?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
