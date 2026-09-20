---
id: verification-2026-06-05-KRX-014470-google-rss-coverage
type: verification
title: KRX 014470 Google RSS Coverage Verification
created: 2026-06-05
updated: 2026-06-05
status: verification
stage: 1

market: KRX
ticker: "014470"
company: 부방
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-05

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=014470
    - name=부방
    - naver_article_count=0
    - google_rss_article_count=3
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 014470
    - 부방
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

# KRX 014470 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-05_KRX_014470_google-rss-coverage-source]]

## Facts Checked
- `code=014470`
- `name=부방`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `부방, +9.52% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-02T13:01:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxPc09Lc1FJOTdBQzBKQUJfTE1pd1dHTFB2dUl5VkhmYjY1cE9PZ3IwZE9BOFRVc19odzZRVG1NNjY1UFVLQkpsVHFaZm9iSmdFMmRiMDhpYUt5Z2dtSjRmX0VtSmRnMHFhSUlSMjFZRXRFLWttODlGZUh6RWxETDRxeEQ3WdIBlwFBVV95cUxOLWIySm5KUWZOTk9sTHpUN29CSndxQklJMEpqSVZXaXI3ZGJQSlk0TVFDcG5ocERtbFNESmZvTHB4M21zYWZGRjFacTJqcVN6TV9kRUt4aWJwYUtsdDlKZks2NGZ1b1JTRUx3REFOT3RoT09henpqeWxuS01Qd1MxSXdQcFhPcy1QOGpDVzZaUldpNm52TEhN?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-05T08:11:49+09:00`
- Company: [[KRX_014470_부방]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `부방, +9.52% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-02T13:01:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxPc09Lc1FJOTdBQzBKQUJfTE1pd1dHTFB2dUl5VkhmYjY1cE9PZ3IwZE9BOFRVc19odzZRVG1NNjY1UFVLQkpsVHFaZm9iSmdFMmRiMDhpYUt5Z2dtSjRmX0VtSmRnMHFhSUlSMjFZRXRFLWttODlGZUh6RWxETDRxeEQ3WdIBlwFBVV95cUxOLWIySm5KUWZOTk9sTHpUN29CSndxQklJMEpqSVZXaXI3ZGJQSlk0TVFDcG5ocERtbFNESmZvTHB4M21zYWZGRjFacTJqcVN6TV9kRUt4aWJwYUtsdDlKZks2NGZ1b1JTRUx3REFOT3RoT09henpqeWxuS01Qd1MxSXdQcFhPcy1QOGpDVzZaUldpNm52TEhN?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
