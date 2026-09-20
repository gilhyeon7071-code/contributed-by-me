---
id: verification-2026-08-11-KRX-174900-google-rss-coverage
type: verification
title: KRX 174900 Google RSS Coverage Verification
created: 2026-08-11
updated: 2026-08-11
status: verification
stage: 1

market: KRX
ticker: "174900"
company: 앱클론
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-11

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=174900
    - name=앱클론
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 174900
    - 앱클론
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

# KRX 174900 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-11_KRX_174900_google-rss-coverage-source]]

## Facts Checked
- `code=174900`
- `name=앱클론`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[여의도 클라쓰] '앱클론, 원익홀딩스, 브이엠' 클라쓰 올릴 종목은? - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-08-11T06:48:04+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5TRjh6SjV6M3M2RG1obnJxZHNXMDhmY0JoaHdEOHNvdDZ6U3JqNHBZenVoU1hFeGwyaWdoWml5T1o3ak1wbHd6czRsVjcyenNEdWZCc1l4bTRJNHNTQkh0Q1gyYklaNlV60gFuQVVfeXFMTms3elhLbHV0YjZveHIzaDJMcWg2QnJFWWpnbzNqRU1jb1NZbEpLVm0yT1A0MkFfUl9iUndsWWRHbHJrWTdIbjJjMWczUjU1R29naGpmaUV3ZzF2c29oaUJzTWtqdWoyMTFoYjJYV2c?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:35:08+09:00`
- Company: [[KRX_174900_앱클론]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-11.json`
- Latest observation title: `[여의도 클라쓰] '앱클론, 원익홀딩스, 브이엠' 클라쓰 올릴 종목은? - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-08-11T06:48:04+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5TRjh6SjV6M3M2RG1obnJxZHNXMDhmY0JoaHdEOHNvdDZ6U3JqNHBZenVoU1hFeGwyaWdoWml5T1o3ak1wbHd6czRsVjcyenNEdWZCc1l4bTRJNHNTQkh0Q1gyYklaNlV60gFuQVVfeXFMTms3elhLbHV0YjZveHIzaDJMcWg2QnJFWWpnbzNqRU1jb1NZbEpLVm0yT1A0MkFfUl9iUndsWWRHbHJrWTdIbjJjMWczUjU1R29naGpmaUV3ZzF2c29oaUJzTWtqdWoyMTFoYjJYV2c?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_gas-energy_가스-에너지]]
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
