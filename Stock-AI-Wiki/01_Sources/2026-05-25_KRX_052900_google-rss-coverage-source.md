---
id: source-2026-05-25-KRX-052900-google-rss-coverage
type: source
title: KRX 052900 Google RSS Coverage Source
created: 2026-05-25
updated: 2026-05-25
status: raw
stage: 0

market: KRX
ticker: "052900"
company: KX하이텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Local coverage report row shows news coverage for KRX 052900.
  key_facts:
    - code=052900
    - name=KX하이텍
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 052900
    - KX하이텍
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

# KRX 052900 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=052900`
- `name=KX하이텍`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 052900.

## RSS Item Metadata
- Title: `[여의도 클라쓰] '에치에프알, 삼성전자, KX하이텍' 클라쓰 올릴 종목은? - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-05-21T06:20:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiS0FVX3lxTFBrUnRablhLM0NwOUFNNTNHNUhIcW5GLUk2UlBueU9jUUViazdFNFVLdEQ0RXp2clJHV1FialF3eHZnWHlGY3dRTVZaMA?oc=5`

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
- Updated at: `2026-08-21T19:15:10+09:00`
- Company: [[KRX_052900_KX하이텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `[여의도 클라쓰] '에치에프알, 삼성전자, KX하이텍' 클라쓰 올릴 종목은? - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-05-21T06:20:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiS0FVX3lxTFBrUnRablhLM0NwOUFNNTNHNUhIcW5GLUk2UlBueU9jUUViazdFNFVLdEQ0RXp2clJHV1FialF3eHZnWHlGY3dRTVZaMA?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
