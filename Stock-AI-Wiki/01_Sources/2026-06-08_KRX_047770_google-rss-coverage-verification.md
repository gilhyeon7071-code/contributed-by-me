---
id: verification-2026-06-08-KRX-047770-google-rss-coverage
type: verification
title: KRX 047770 Google RSS Coverage Verification
created: 2026-06-08
updated: 2026-06-08
status: verification
stage: 1

market: KRX
ticker: "047770"
company: 코데즈컴바인
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=047770
    - name=코데즈컴바인
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 047770
    - 코데즈컴바인
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

# KRX 047770 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-08_KRX_047770_google-rss-coverage-source]]

## Facts Checked
- `code=047770`
- `name=코데즈컴바인`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `코데즈컴바인, +7.43% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-05T11:06:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNM1pCSFlvRG5BTXVjemZfYXozRzRMVkpQclpTSnpaUWFlYUprSUpxRlpjU2txZXN0LTh6cy15dmNkVjUybmNGS0hIT0pTQzgteV9vd25VXzRLd1FLZ2NsRjAzclVXdWxFV2pLOGdjdVJNTmZKTXRqV3RPYVRqZ0owQkJCTdIBlwFBVV95cUxQNWVpMlB3RjZlNHh4dklxaklFUVBpNFFuTVR6RUlyZXc5YUZ1SzNOd2xfcXhIRkk0RDJtZ1RnenFOY3ZtaXlITWJEOVo2TzVodXVMUnRmLXdHdGl1ZGFfZFp2VzNuNHBBSVpzVDFtZ3loamk2YmhLeUxicThzYl84NVBhU1pMS21ySHcwdElPa0FSb3pkZThN?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-08T18:15:43+09:00`
- Company: [[KRX_047770_코데즈컴바인]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-08.json`
- Latest observation title: `코데즈컴바인, +7.43% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-05T11:06:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNM1pCSFlvRG5BTXVjemZfYXozRzRMVkpQclpTSnpaUWFlYUprSUpxRlpjU2txZXN0LTh6cy15dmNkVjUybmNGS0hIT0pTQzgteV9vd25VXzRLd1FLZ2NsRjAzclVXdWxFV2pLOGdjdVJNTmZKTXRqV3RPYVRqZ0owQkJCTdIBlwFBVV95cUxQNWVpMlB3RjZlNHh4dklxaklFUVBpNFFuTVR6RUlyZXc5YUZ1SzNOd2xfcXhIRkk0RDJtZ1RnenFOY3ZtaXlITWJEOVo2TzVodXVMUnRmLXdHdGl1ZGFfZFp2VzNuNHBBSVpzVDFtZ3loamk2YmhLeUxicThzYl84NVBhU1pMS21ySHcwdElPa0FSb3pkZThN?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
