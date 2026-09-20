---
id: verification-2026-07-11-KRX-047040-google-rss-coverage
type: verification
title: KRX 047040 Google RSS Coverage Verification
created: 2026-07-11
updated: 2026-07-11
status: verification
stage: 1

market: KRX
ticker: "047040"
company: 대우건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-11

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=047040
    - name=대우건설
    - naver_article_count=1
    - google_rss_article_count=77
    - kis_title_count=11
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 047040
    - 대우건설
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

# KRX 047040 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-11_KRX_047040_google-rss-coverage-source]]

## Facts Checked
- `code=047040`
- `name=대우건설`
- `naver_article_count=1`
- `google_rss_article_count=77`
- `kis_title_count=11`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `대우건설 신용등급, 대형 건설사 중 유일하게 전망 하향 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-07-08T15:06:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMimAFBVV95cUxNQ05BeXMwMVdSQnFjTEtVaE42NzRxRy01RU14bFl5N3M4aDlNOUlNM2J3M05TbThyWTVJSFlCN3otd1NfZnkxOEFJSHBKRUNfU2sxaU5uZW9NcXV0dGo4dnB1Zmp3eTI4cVBuWHdMalg5OWFRSFlvb25CaEFBQk9ZbGQyU1JnSnpSa3pLckNleVJib3o2bXdsYtIBrAFBVV95cUxOb2RjTW0zamRqdkxVR1BjUk9sbUpiMWg5eXozWnhtUE55M0hUTjdMaXZWdUhFNTROdk9Sc0E4SjFVQVVuMmN3UnliSXY5bkEtSDVkOUVMZGVWMi1yNjF6UjFud0dWOFVCX0o3MkVSVHpqcmQ3WHFBdXFJOWJxWVVVZlh4XzM0NUFCVzJpcUhJUzBPRGpxWUptRXYzMlBOSTY1Wjg5b3dtcG1OZ1RY?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:28:43+09:00`
- Company: [[KRX_047040_대우건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-11.json`
- Latest observation title: `대우건설 신용등급, 대형 건설사 중 유일하게 전망 하향 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-07-08T15:06:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMimAFBVV95cUxNQ05BeXMwMVdSQnFjTEtVaE42NzRxRy01RU14bFl5N3M4aDlNOUlNM2J3M05TbThyWTVJSFlCN3otd1NfZnkxOEFJSHBKRUNfU2sxaU5uZW9NcXV0dGo4dnB1Zmp3eTI4cVBuWHdMalg5OWFRSFlvb25CaEFBQk9ZbGQyU1JnSnpSa3pLckNleVJib3o2bXdsYtIBrAFBVV95cUxOb2RjTW0zamRqdkxVR1BjUk9sbUpiMWg5eXozWnhtUE55M0hUTjdMaXZWdUhFNTROdk9Sc0E4SjFVQVVuMmN3UnliSXY5bkEtSDVkOUVMZGVWMi1yNjF6UjFud0dWOFVCX0o3MkVSVHpqcmQ3WHFBdXFJOWJxWVVVZlh4XzM0NUFCVzJpcUhJUzBPRGpxWUptRXYzMlBOSTY1Wjg5b3dtcG1OZ1RY?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
