---
id: verification-2026-06-13-KRX-002630-google-rss-coverage
type: verification
title: KRX 002630 Google RSS Coverage Verification
created: 2026-06-13
updated: 2026-06-13
status: verification
stage: 1

market: KRX
ticker: "002630"
company: nan
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-13

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=002630
    - name=nan
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=14
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 002630
    - nan
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

# KRX 002630 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-13_KRX_002630_google-rss-coverage-source]]

## Facts Checked
- `code=002630`
- `name=nan`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=14`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `“나 잡아” 이하늬, 김남길 도와주는 ‘구세주’ - sbs.co.kr`
- Source: `sbs.co.kr`
- Published at: `2026-06-12T06:23:44+09:00`
- Link: `https://news.google.com/rss/articles/CBMif0FVX3lxTFBVcklUMTBQbHlTRHRxUzlSQzlCbFpFV25PM3AxQ0x1ZFpvNGRod3B4aHNrUV9qMEpkX2lkc2hLNHh4YjBHZGhQZ282N3haVWEzTUtnWUZZdTdJdC1DeF9DUTVUdHo1ZGhIN2VVYmFiczBYTWJBMVNleUpiLWRoR1k?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:20:49+09:00`
- Company: [[KRX_002630_nan]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-13.json`
- Latest observation title: `2026 북중미 월드컵 실시간 경기 결과 및 일정 - BBC`
- Latest observation source: `BBC`
- Latest observation published_at: `2026-06-01T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTFA3T1hvYWpZUnU5Y080azVfVDR2YWllM3F6b0VjMm5WMUpYRGVSbDcwMWJ3ZzV6eGh1dHdNcVZKSVp5cmJLYkQxLW9pWUljYl9fVWVzZThobHZ4OVpn0gFiQVVfeXFMUDF2LVlWVk9xRWRkQ1hlVWZ6blFraGJqOHlpZFJSWXZfT2FaOU1XWlNzVjJkd0J5cUE2SkdMXzQxV2RSaHJWVlFBMVlJS2g2NWV6S0tsaXNRcTY3dHJSa0lQY0E?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
