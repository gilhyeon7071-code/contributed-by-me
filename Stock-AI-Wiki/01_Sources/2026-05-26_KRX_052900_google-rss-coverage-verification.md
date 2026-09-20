---
id: verification-2026-05-26-KRX-052900-google-rss-coverage
type: verification
title: KRX 052900 Google RSS Coverage Verification
created: 2026-05-26
updated: 2026-05-26
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
  collected_at: 2026-05-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
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
- [[2026-05-26_KRX_052900_google-rss-coverage-source]]

## Facts Checked
- `code=052900`
- `name=KX하이텍`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[여의도 클라쓰] '에치에프알, 삼성전자, KX하이텍' 클라쓰 올릴 종목은? - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-05-21T06:20:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiS0FVX3lxTFBrUnRablhLM0NwOUFNNTNHNUhIcW5GLUk2UlBueU9jUUViazdFNFVLdEQ0RXp2clJHV1FialF3eHZnWHlGY3dRTVZaMA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:15:28+09:00`
- Company: [[KRX_052900_KX하이텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-26.json`
- Latest observation title: `HBM 다음은 'eSSD'…리딩證 "KX하이텍, AI발 데이터센터 수요 폭증" - 연합인포맥스`
- Latest observation source: `연합인포맥스`
- Latest observation published_at: `2026-05-26T08:53:43+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTFBUM3gxOXBfbXZ6OGgxdHVFMmdjNVlDRGgxajV2RnMtb3RQU01fczNNcUNJUFYtUVVHRTQtZ2Q0TnBrdU9mcm9GNDdSYl9QODBVbkhONlQ5SU9nbmREVkNBc0pVSGhfRkpQNVhrV3M4LVg?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
