---
id: verification-2026-05-28-KRX-006660-google-rss-coverage
type: verification
title: KRX 006660 Google RSS Coverage Verification
created: 2026-05-28
updated: 2026-05-28
status: verification
stage: 1

market: KRX
ticker: "006660"
company: 삼성공조
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=006660
    - name=삼성공조
    - naver_article_count=1
    - google_rss_article_count=7
    - kis_title_count=34
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 006660
    - 삼성공조
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

# KRX 006660 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-28_KRX_006660_google-rss-coverage-source]]

## Facts Checked
- `code=006660`
- `name=삼성공조`
- `naver_article_count=1`
- `google_rss_article_count=7`
- `kis_title_count=34`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[적중! 대박 예감] '삼성공조, LG씨엔에스' 내일장 예감 좋은 대박 종목은? - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-05-28T06:26:28+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTE1PLWE2aGduRFJleFY4YmQ5T0s3QVVqcmZaTlQ2aHREQ1RmbmxDbkJGSWpRS01vT3VFMDQ0WkRoLU5kNDYwVVZ4RjZCRjFBVGlOS0M5TmtnRmZ2VlU5dmRRLTJkU0ZKM2VxaHoxNm530gFuQVVfeXFMTU8tYTZoZ25EUmV4VjhiZDlPSzdBVWpyZlpOVDZodERDVGZubENuQkZJalFLTW9PdUUwNDRaRGgtTmQ0NjBVVnhGNkJGMUFUaU5LQzlOa2dGZnZWVTl2ZFEtMmRTRkozZXFoejE2bnc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:16:01+09:00`
- Company: [[KRX_006660_삼성공조]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-28.json`
- Latest observation title: `[적중! 대박 예감] '삼성공조, LG씨엔에스' 내일장 예감 좋은 대박 종목은? - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-05-28T06:24:24+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVEFVX3lxTE1ULTlMS1RBNmJCU3VMLU9Pb2g2WnAwbEFadEoxbkhNWEJ6eHNUMWNPOVkxSlJCc3ZaQ25IbEFHX0tVRXcyUDdBeVNNdHpvelRybnJqcw?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
