---
id: verification-2026-05-19-KRX-383800-google-rss-coverage
type: verification
title: KRX 383800 Google RSS Coverage Verification
created: 2026-05-19
updated: 2026-05-19
status: verification
stage: 1

market: KRX
ticker: "383800"
company: LX홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-19

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=383800
    - name=LX홀딩스
    - naver_article_count=1
    - google_rss_article_count=4
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 383800
    - LX홀딩스
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

# KRX 383800 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-19_KRX_383800_google-rss-coverage-source]]

## Facts Checked
- `code=383800`
- `name=LX홀딩스`
- `naver_article_count=1`
- `google_rss_article_count=4`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LX홀딩스, -1.67% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-19T10:51:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNYk9mZnQ3UmtCX2hFZHU0UXFLTHloNVhfQnFtVnY1SHRJVkpfTXN2ZWJ4ZG12NG9iWENBTmN0YVU0NVZvZ1FpbmQ5cWxOQ0Z6UjAyNmx6cmpkMjJJWFFCajdaeVd6ZzFvQmFSZ25oZy1hSVVWTFpWRzVHbVZWdzI3UjVDUdIBlwFBVV95cUxNMlAwbkhyZkE2YjJrZXdfaEFmWXRZaTlrR0xiWDM1cTBRNHhCTHUyaFZNV2hNc3pueDF6UFF3NU02QXZERzFaSUV5Qk1hVFFjZG5IQmxFNmlDMXdBbGd2ME9ZTW5GVHBUcXFHRS1fR0xvaEIzR1podVAtVmI0WGg4Y1JJQ1dLVzRtbkNOMWI2SXk4U2VvRUE0?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:14:34+09:00`
- Company: [[KRX_383800_LX홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-19.json`
- Latest observation title: `LX홀딩스, -1.67% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-19T10:51:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNYk9mZnQ3UmtCX2hFZHU0UXFLTHloNVhfQnFtVnY1SHRJVkpfTXN2ZWJ4ZG12NG9iWENBTmN0YVU0NVZvZ1FpbmQ5cWxOQ0Z6UjAyNmx6cmpkMjJJWFFCajdaeVd6ZzFvQmFSZ25oZy1hSVVWTFpWRzVHbVZWdzI3UjVDUdIBlwFBVV95cUxNMlAwbkhyZkE2YjJrZXdfaEFmWXRZaTlrR0xiWDM1cTBRNHhCTHUyaFZNV2hNc3pueDF6UFF3NU02QXZERzFaSUV5Qk1hVFFjZG5IQmxFNmlDMXdBbGd2ME9ZTW5GVHBUcXFHRS1fR0xvaEIzR1podVAtVmI0WGg4Y1JJQ1dLVzRtbkNOMWI2SXk4U2VvRUE0?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
