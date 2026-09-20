---
id: verification-2026-06-19-KRX-052900-google-rss-coverage
type: verification
title: KRX 052900 Google RSS Coverage Verification
created: 2026-06-19
updated: 2026-06-19
status: verification
stage: 1

market: KRX
ticker: "052900"
company: KX하이텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-19

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=052900
    - name=KX하이텍
    - naver_article_count=0
    - google_rss_article_count=2
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 052900
    - KX하이텍
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is unavailable.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: false
  numeric_values_checked: true
  date_values_checked: true
  entity_names_checked: false
  conflict_exists: false
  conflict_summary:
  confidence: unknown

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
  change_reason: generated coverage verification note
---

# KRX 052900 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-19_KRX_052900_google-rss-coverage-source]]

## Facts Checked
- `code=052900`
- `name=KX하이텍`
- `naver_article_count=0`
- `google_rss_article_count=2`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `키움소부장제3호신기술사업투자조합, KX하이텍 지분 50억원 ↓ - 데일리인베스트`
- Source: `데일리인베스트`
- Published at: `2026-06-18T08:48:19+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE91UjRCbExmbll1R1FybFBVM3FCdjZsNzZZb0wxN2pBTlNQelhHT1hQcV94eFBUNXYzczZ0bnczNWpxOTFUajBxaXkxTlRrdjVhN2RwRW1JUlBvYS01N0prd1FMdUJfNVNiOWJ3QtIBb0FVX3lxTFA1ZG1xOGNZY0IwNzVFN0s1endFQ0ZRVTBSNVFhd19DT1ZNQXRYVDhpOG55LVY0cENVZkdpN241SmJGQW0xRnJnUmh1QWVObmVRbjFWZGRFWTFPOXYwSmFrb0ZLR3BXX2xjeXBEYjVqSQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:22:23+09:00`
- Company: [[KRX_052900_KX하이텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-19.json`
- Latest observation title: `KX하이텍, 제12회차 전환사채 전환청구권 행사로 359만3429주 발행 - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-06-19T15:50:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9OYWU3RENnUU9QcTZQVkhjemxhVEdjWWpIQTFiQWdxQzQxVFQwNmRmbnU0VXRQdW50ekpjOUlQbXZjWG5yR2VON2NoMHRSbHc3RGdRNFhaVjUxa1U4cXBPNW9SWEUzbTNmTkZXNXBXNUQ5eDQ?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
