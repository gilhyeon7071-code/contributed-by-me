---
id: verification-2026-06-30-KRX-011070-google-rss-coverage
type: verification
title: KRX 011070 Google RSS Coverage Verification
created: 2026-06-30
updated: 2026-06-30
status: verification
stage: 1

market: KRX
ticker: "011070"
company: LG이노텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-30

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=011070
    - name=LG이노텍
    - naver_article_count=1
    - google_rss_article_count=2
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 011070
    - LG이노텍
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

# KRX 011070 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-30_KRX_011070_google-rss-coverage-source]]

## Facts Checked
- `code=011070`
- `name=LG이노텍`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `카리스마 리더십 시대 끝났다...허영호 LG이노텍 전 사장 조직은 어떻게 강해지는가 출간 - 아주경제`
- Source: `아주경제`
- Published at: `2026-06-29T17:50:42+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWEFVX3lxTFBpS0E1dUtienQ3RGRWSlBzOHZVUTR6UmozMENNZ1I5emFvUy0wcFpEN2lrQlRqNVFBbnpSUVhvVHFTRnFxRk5DU0J1ekNtRFlXcF9oelUwX0_SAVhBVV95cUxQaUtBNXVLYnp0N0RkVkpQczh2VVE0elJqMzBDTWdSOXphb1MtMHBaRDdpa0JUajVRQW56UlFYb1RxU0ZxcUZOQ1NCdXpDbURZV3BfaHpVMF9P?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:25:36+09:00`
- Company: [[KRX_011070_LG이노텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-30.json`
- Latest observation title: `카리스마 리더십 시대 끝났다...허영호 LG이노텍 전 사장 조직은 어떻게 강해지는가 출간 - 아주경제`
- Latest observation source: `아주경제`
- Latest observation published_at: `2026-06-29T17:50:42+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWEFVX3lxTFBpS0E1dUtienQ3RGRWSlBzOHZVUTR6UmozMENNZ1I5emFvUy0wcFpEN2lrQlRqNVFBbnpSUVhvVHFTRnFxRk5DU0J1ekNtRFlXcF9oelUwX0_SAVhBVV95cUxQaUtBNXVLYnp0N0RkVkpQczh2VVE0elJqMzBDTWdSOXphb1MtMHBaRDdpa0JUajVRQW56UlFYb1RxU0ZxcUZOQ1NCdXpDbURZV3BfaHpVMF9P?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
