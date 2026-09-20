---
id: verification-2026-08-19-KRX-078930-google-rss-coverage
type: verification
title: KRX 078930 Google RSS Coverage Verification
created: 2026-08-19
updated: 2026-08-19
status: verification
stage: 1

market: KRX
ticker: "078930"
company: GS
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-19

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=078930
    - name=GS
    - naver_article_count=2
    - google_rss_article_count=8
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 078930
    - GS
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

# KRX 078930 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-19_KRX_078930_google-rss-coverage-source]]

## Facts Checked
- `code=078930`
- `name=GS`
- `naver_article_count=2`
- `google_rss_article_count=8`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `GS더프레시, SSM 역신장 속 나홀로 성장...독주 체제 굳힌다 - 머니투데이 - mt.co.kr`
- Source: `mt.co.kr`
- Published at: `2026-08-18T15:03:22+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE5EZmZ5R0dZV1E3RnFVbUZEcGhzV0daMGtabll2OUN3T2NGcUxKdXVULUhqZU5qaWdFR0hZa2M1a2RDZ2pQaTc1cEJsRHFVQWg5OG9FbXJSZjc1UWhNQVlwSGhQalQ3QUJYOUHSAW9BVV95cUxNLWJlQlV0UTNnMm9yT093SHlzazE5N3dLT2xtRnRHZzk2NERIV0pSR3ltUWg4YjRQSWQyVGZIY01idnZVWmdNRkhiYW5LVUNLZkdzNlhIaHk1d2FmYlAxRFBUSE9iVjE5MjVRQk1HX1k?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:37:11+09:00`
- Company: [[KRX_078930_GS]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-19.json`
- Latest observation title: `GS더프레시, SSM 역신장 속 나홀로 성장...독주 체제 굳힌다 - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-08-18T15:03:22+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE5EZmZ5R0dZV1E3RnFVbUZEcGhzV0daMGtabll2OUN3T2NGcUxKdXVULUhqZU5qaWdFR0hZa2M1a2RDZ2pQaTc1cEJsRHFVQWg5OG9FbXJSZjc1UWhNQVlwSGhQalQ3QUJYOUHSAW9BVV95cUxNLWJlQlV0UTNnMm9yT093SHlzazE5N3dLT2xtRnRHZzk2NERIV0pSR3ltUWg4YjRQSWQyVGZIY01idnZVWmdNRkhiYW5LVUNLZkdzNlhIaHk1d2FmYlAxRFBUSE9iVjE5MjVRQk1HX1k?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
