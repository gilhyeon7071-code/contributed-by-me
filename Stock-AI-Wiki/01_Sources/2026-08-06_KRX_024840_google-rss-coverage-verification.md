---
id: verification-2026-08-06-KRX-024840-google-rss-coverage
type: verification
title: KRX 024840 Google RSS Coverage Verification
created: 2026-08-06
updated: 2026-08-06
status: verification
stage: 1

market: KRX
ticker: "024840"
company: KBI메탈
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-06

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=024840
    - name=KBI메탈
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 024840
    - KBI메탈
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

# KRX 024840 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-06_KRX_024840_google-rss-coverage-source]]

## Facts Checked
- `code=024840`
- `name=KBI메탈`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[적중! 대박 예감] 'KBI메탈, 두산로보틱스' 내일장 예감 좋은 대박 종목은? - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-08-05T23:46:53+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE5sMVhBWS1nMF9ZNl9PelJmUmZMZVZfUjQ1a0lTai1oME1FSTRCQ25oazdnQVd2aGtxM0xUVzhSOXh4OWxSRDF5NnJYYTg2TGVtcU5hTk1Vek5uNzdHQ0ZfR21ITnI5d0VBWWFqS1hB0gFuQVVfeXFMTmwxWEFZLWcwX1k2X096UmZSZkxlVl9SNDVrSVNqLWgwTUVJNEJDbmhrN2dBV3Zoa3EzTFRXOFI5eHg5bFJEMXk2clhhODZMZW1xTmFOTVV6Tm43N0dDRl9HbUhOcjl3RUFZYWpLWEE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:33:31+09:00`
- Company: [[KRX_024840_KBI메탈]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-06.json`
- Latest observation title: `[적중! 대박 예감] 'KBI메탈, 두산로보틱스' 내일장 예감 좋은 대박 종목은? - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-08-05T23:46:53+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFBwaGpJbGVsUHlZVVZRMXJrOEtfQlRXbkdTVjh3TW5xNUdhUWwxSzFNLXBESmZkSTJ5Ml9ISmEyRUgxQ0lFbGcyS2FNaUtyT1VKRkhrV0lsb2RNVTViUUpDU0VWLVRpRzVr0gFuQVVfeXFMTmwxWEFZLWcwX1k2X096UmZSZkxlVl9SNDVrSVNqLWgwTUVJNEJDbmhrN2dBV3Zoa3EzTFRXOFI5eHg5bFJEMXk2clhhODZMZW1xTmFOTVV6Tm43N0dDRl9HbUhOcjl3RUFZYWpLWEE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
