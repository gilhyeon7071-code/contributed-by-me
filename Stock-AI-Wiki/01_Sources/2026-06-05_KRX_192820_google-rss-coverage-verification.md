---
id: verification-2026-06-05-KRX-192820-google-rss-coverage
type: verification
title: KRX 192820 Google RSS Coverage Verification
created: 2026-06-05
updated: 2026-06-05
status: verification
stage: 1

market: KRX
ticker: "192820"
company: 코스맥스
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
    - code=192820
    - name=코스맥스
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 192820
    - 코스맥스
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

# KRX 192820 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-05_KRX_192820_google-rss-coverage-source]]

## Facts Checked
- `code=192820`
- `name=코스맥스`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `코스맥스, 파트너사 원료 제안 프로세스 디지털화…공급망 다변화 - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-06-05T13:45:37+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE1jUFRrRnZwQ1NHVW05RmhzSmIxRjd2Q2JlODQ1VXZoRHVvaXRDOWU5SWY5R2VCZThkVXh5MHd2RFdIX1BKWUtyU3hITVVVSU5MV2d2dV92V0lqRG0zeHQtYk04OXlBdENSbnfSAW9BVV95cUxOTjNielVtTzJaYVpGbVVSQjRRZWJnNkt5SGV3alRyaXVTRktGVklmTlZZZU5FS0hneWVCQXNwWFAxb3pwXzVLWGEyMGRYQTh6WGszRVRid3dkQ2xwRXNGUkhveHN3RzQ1UzFaSFdTb0U?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:18:19+09:00`
- Company: [[KRX_192820_코스맥스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `코스맥스, 파트너사 원료 제안 프로세스 디지털화…공급망 다변화 - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-06-05T13:45:37+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE1jUFRrRnZwQ1NHVW05RmhzSmIxRjd2Q2JlODQ1VXZoRHVvaXRDOWU5SWY5R2VCZThkVXh5MHd2RFdIX1BKWUtyU3hITVVVSU5MV2d2dV92V0lqRG0zeHQtYk04OXlBdENSbnfSAW9BVV95cUxOTjNielVtTzJaYVpGbVVSQjRRZWJnNkt5SGV3alRyaXVTRktGVklmTlZZZU5FS0hneWVCQXNwWFAxb3pwXzVLWGEyMGRYQTh6WGszRVRid3dkQ2xwRXNGUkhveHN3RzQ1UzFaSFdTb0U?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
