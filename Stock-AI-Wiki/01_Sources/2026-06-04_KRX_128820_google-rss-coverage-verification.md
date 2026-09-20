---
id: verification-2026-06-04-KRX-128820-google-rss-coverage
type: verification
title: KRX 128820 Google RSS Coverage Verification
created: 2026-06-04
updated: 2026-06-04
status: verification
stage: 1

market: KRX
ticker: "128820"
company: 대성산업
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-04

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=128820
    - name=대성산업
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 128820
    - 대성산업
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

# KRX 128820 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-04_KRX_128820_google-rss-coverage-source]]

## Facts Checked
- `code=128820`
- `name=대성산업`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `대성산업, +8.24% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-02T09:27:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxQS3ota0UyYmtiMW05aEhyclZuM1lVYzF3R0VsQTJYZkZJaXdGSUxyR1FkajNNbWswYWRqU3BvU0VtNmR4TGU3U3dLLUtuNHQ3bDNyRGYyZVVrbFNIMEt1MTgzaFk3V29VVnFMYzFZVE9GMjU5bjNnUjFTRXBYLUlrRjlNd9IBlwFBVV95cUxQdHYyenZIQ0xST2tneW9iN0IxUHpmSHBKMEo4SjdlUWJleDdpN3YwMENITXM1TWplYTI3amJrak1iaVJlc0ltMVJhTjJrNEp5OUxPa19qdnZyU0pBU1IzdmxSUDRqZ0lZMVdJNGtXSW9FbG5MbGtRRTlKRjNzRnFTQVFZRjdhdWp3V0RxS25TYVdOcm55Y0RV?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-04T09:05:04+09:00`
- Company: [[KRX_128820_대성산업]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-04.json`
- Latest observation title: `대성산업, +8.24% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-02T09:27:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxQS3ota0UyYmtiMW05aEhyclZuM1lVYzF3R0VsQTJYZkZJaXdGSUxyR1FkajNNbWswYWRqU3BvU0VtNmR4TGU3U3dLLUtuNHQ3bDNyRGYyZVVrbFNIMEt1MTgzaFk3V29VVnFMYzFZVE9GMjU5bjNnUjFTRXBYLUlrRjlNd9IBlwFBVV95cUxQdHYyenZIQ0xST2tneW9iN0IxUHpmSHBKMEo4SjdlUWJleDdpN3YwMENITXM1TWplYTI3amJrak1iaVJlc0ltMVJhTjJrNEp5OUxPa19qdnZyU0pBU1IzdmxSUDRqZ0lZMVdJNGtXSW9FbG5MbGtRRTlKRjNzRnFTQVFZRjdhdWp3V0RxS25TYVdOcm55Y0RV?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
