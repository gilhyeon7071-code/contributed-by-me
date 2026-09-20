---
id: verification-2026-07-28-KRX-278470-google-rss-coverage
type: verification
title: KRX 278470 Google RSS Coverage Verification
created: 2026-07-28
updated: 2026-07-28
status: verification
stage: 1

market: KRX
ticker: "278470"
company: 에이피알
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=278470
    - name=에이피알
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 278470
    - 에이피알
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

# KRX 278470 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-28_KRX_278470_google-rss-coverage-source]]

## Facts Checked
- `code=278470`
- `name=에이피알`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `K뷰티 브라질 공략법…아모레 '헤어'·에이피알 '유통망' - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-07-27T16:14:12+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE1VR190Z3FaV2owRGw3MVNLZmFWYzh6VVBCdFpHeDZwUkhCOGhVUm0wSzdKRXh5T0EyMEg3MmVEaFEyelNfRGd4ZFJfVVJ5Sk0ybU5qZndoZ1lIUnJfU095YlkxNVFkeGJ0QUHSAW9BVV95cUxPNkVkT3NENkxldFhGdFdYRVhLQ1Z0SW9lQkJwT0N0Z2REdm54VUM1Z0xfbXYyWUd1NHZQeFRodmJfR2lOX1h3bm1BSXJFUUpkM2NfS3RjYWNNWUVhbk5DWTdZWkxyUG5zRGQ2RTNQS3M?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-28T21:05:05+09:00`
- Company: [[KRX_278470_에이피알]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `K뷰티 브라질 공략법…아모레 '헤어'·에이피알 '유통망' - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-07-27T16:14:12+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE1VR190Z3FaV2owRGw3MVNLZmFWYzh6VVBCdFpHeDZwUkhCOGhVUm0wSzdKRXh5T0EyMEg3MmVEaFEyelNfRGd4ZFJfVVJ5Sk0ybU5qZndoZ1lIUnJfU095YlkxNVFkeGJ0QUHSAW9BVV95cUxPNkVkT3NENkxldFhGdFdYRVhLQ1Z0SW9lQkJwT0N0Z2REdm54VUM1Z0xfbXYyWUd1NHZQeFRodmJfR2lOX1h3bm1BSXJFUUpkM2NfS3RjYWNNWUVhbk5DWTdZWkxyUG5zRGQ2RTNQS3M?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
