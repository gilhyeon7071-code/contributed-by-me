---
id: verification-2026-06-04-KRX-045340-google-rss-coverage
type: verification
title: KRX 045340 Google RSS Coverage Verification
created: 2026-06-04
updated: 2026-06-04
status: verification
stage: 1

market: KRX
ticker: "045340"
company: 토탈소프트
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
    - code=045340
    - name=토탈소프트
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 045340
    - 토탈소프트
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

# KRX 045340 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-04_KRX_045340_google-rss-coverage-source]]

## Facts Checked
- `code=045340`
- `name=토탈소프트`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `토탈소프트, -9.69% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-02T10:06:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxPbXhwT1dRVTJ2Qnc2d01KUHZuVGlzOWlrVE1ROHgxV1VRa1htVy1UMXN5MmVqa2FZZzNHSF9FcW5HdTFhNTdDb19uclZDaldjRV9RWFhLSnlSLVphcjNBc1pLZ0pJVnRxaXZ2Zl9RY1FmaWhsYXZ1Mmc0bHBFZU5takhHRdIBlwFBVV95cUxQaTY4cTc5cFRxVkJfNkFfcVZTU1VJVWlQWE1zS0o2VUFrRmU5TjNILTZub3lsX1F1VHU3ejFMdVBNdHhyRUtIMlhUNkVFQlA2YkxZbUYzX1M1a3VZQ2l5QVNkRGN5VmNzTlVLdlRqY2ZodG5CNFhhNU5uLXU3VlBjczYwWnRwUzM5ZGkzSDlrUm5EaTBrZkFN?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:18:01+09:00`
- Company: [[KRX_045340_토탈소프트]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-04.json`
- Latest observation title: `토탈소프트, -9.69% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-02T10:06:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxPbXhwT1dRVTJ2Qnc2d01KUHZuVGlzOWlrVE1ROHgxV1VRa1htVy1UMXN5MmVqa2FZZzNHSF9FcW5HdTFhNTdDb19uclZDaldjRV9RWFhLSnlSLVphcjNBc1pLZ0pJVnRxaXZ2Zl9RY1FmaWhsYXZ1Mmc0bHBFZU5takhHRdIBlwFBVV95cUxQaTY4cTc5cFRxVkJfNkFfcVZTU1VJVWlQWE1zS0o2VUFrRmU5TjNILTZub3lsX1F1VHU3ejFMdVBNdHhyRUtIMlhUNkVFQlA2YkxZbUYzX1M1a3VZQ2l5QVNkRGN5VmNzTlVLdlRqY2ZodG5CNFhhNU5uLXU3VlBjczYwWnRwUzM5ZGkzSDlrUm5EaTBrZkFN?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
