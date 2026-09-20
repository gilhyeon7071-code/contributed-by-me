---
id: verification-2026-05-20-KRX-131290-google-rss-coverage
type: verification
title: KRX 131290 Google RSS Coverage Verification
created: 2026-05-20
updated: 2026-05-20
status: verification
stage: 1

market: KRX
ticker: "131290"
company: 티에스이
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-20

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=131290
    - name=티에스이
    - naver_article_count=2
    - google_rss_article_count=3
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 131290
    - 티에스이
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

# KRX 131290 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-20_KRX_131290_google-rss-coverage-source]]

## Facts Checked
- `code=131290`
- `name=티에스이`
- `naver_article_count=2`
- `google_rss_article_count=3`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[특징주] 1분기 호실적 기록한 티에스이, 주가도 17% '급등' - 뉴스드림`
- Source: `뉴스드림`
- Published at: `2026-05-18T14:02:22+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE5PVkQ4RmJZaDVxTDZrLUJYRkcxZG9PVUlBYXlRczlVblF2blVzQkF4NkdqUGhYRl90VmhBWVl0RXB0UWJGOWFZcWV2dEM1UElsZ3p2TWlXck1zdzh0UFF4bVM2d1EzbC1pOHfSAW5BVV95cUxQNUdqTlowTzl1Mzg4Qk1UNHNFWk94OHQtZmhwYnc4QktIUDdZOEVDRlNvN2taRkpNd0MybkFjZGNJdUM2dDZWcURtQjZ0dGNVRTFWSElYdFdBaGhZc0RNcHJvLUo4S2wtc0NET3RLUQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-20T16:05:04+09:00`
- Company: [[KRX_131290_티에스이]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-20.json`
- Latest observation title: `티에스이, +14.29% 상승폭 확대 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-20T13:46:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxPZXo5b3pHcGt2ckFaYXZnX1gyQzYtbTM0c1JBcnhxVDBkbGU0VUl5alRFSS01RkV4WU5VWXJ0S3ViS1F1Z0xWd0EtakF3d1dZN0Fyc0h2NGFXTTJnYk9kRk1lc3oxTnVlbGg1a3JSUGYxQVVZQ1VKeUV4c0hic3RXV3lGa9IBlwFBVV95cUxOWkJ4bDNtU1NUTzBBRlJxV0MwX0lqTTZOSHJ2a1dUd212N0FIVUFWczBtcGNxbjN1SE5EMGZKQjJNTzgwOHZwajY5NlU0MWxDNy10ZzRCbUpEdEVCaF9kampDT19pM0VmZktSdHBHSWVDcGFseGRDelZiZXhWZVdOS21VOWxUTFN4V1FCQk9XcEw4TGhrM1dv?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
