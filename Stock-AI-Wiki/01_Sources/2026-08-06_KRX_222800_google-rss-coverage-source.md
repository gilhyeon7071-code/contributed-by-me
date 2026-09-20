---
id: source-2026-08-06-KRX-222800-google-rss-coverage
type: source
title: KRX 222800 Google RSS Coverage Source
created: 2026-08-06
updated: 2026-08-06
status: raw
stage: 0

market: KRX
ticker: "222800"
company: 심텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-06

analysis:
  summary: Local coverage report row shows news coverage for KRX 222800.
  key_facts:
    - code=222800
    - name=심텍
    - naver_article_count=2
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 222800
    - 심텍
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

# KRX 222800 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=222800`
- `name=심텍`
- `naver_article_count=2`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 222800.

## RSS Item Metadata
- Title: `심텍 2분기 영업이익 629억원…작년 동기 대비 1034%↑ - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-08-05T15:57:46+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE5BMWp6TFhJcVh0ZWcyZ3JsSTI2aWEwZm5lWDZzWWRpX0kzc1NPdERIOHZWaXNtTl9sSkJtZXZvQjNFNjhnQ0tTSUdoOTZwTzhELTNQOGlYUF81UnfSAWBBVV95cUxNSG15OUNaTmpOUGxCS2ZlUHhGaE8xOEVnblU2VUU3NUlIbU8xZURpZGlfaFozUEh6UnNtN2ViQXRYTlJGLXZXOUd6bE94YmVtTjVCdldHcEVuMUdpVmppaUE?oc=5`

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
- Updated at: `2026-08-21T19:33:31+09:00`
- Company: [[KRX_222800_심텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-06.json`
- Latest observation title: `[특징주] 심텍, 2분기 '깜짝 실적'에 5%대 강세…증권가 잇따라 눈높이 상향 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-08-06T13:51:31+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5iOEY2Y2hFZXpva1Z2TmVwTkNJM1QzbUFLbENJTlMwRHlpN2paR3FwbFg3bTIxam9ZSFYtZks0cDVseTZUcVp1eUhVSDZJN1E?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
