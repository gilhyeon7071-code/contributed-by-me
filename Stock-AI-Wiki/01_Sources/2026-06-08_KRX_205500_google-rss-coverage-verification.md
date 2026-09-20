---
id: verification-2026-06-08-KRX-205500-google-rss-coverage
type: verification
title: KRX 205500 Google RSS Coverage Verification
created: 2026-06-08
updated: 2026-06-08
status: verification
stage: 1

market: KRX
ticker: "205500"
company: 넥써쓰
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=205500
    - name=넥써쓰
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 205500
    - 넥써쓰
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

# KRX 205500 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-08_KRX_205500_google-rss-coverage-source]]

## Facts Checked
- `code=205500`
- `name=넥써쓰`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `[Game & Now] 넥써쓰, 방치형 RPG '아이들 판타지' 크로쓰 플랫폼 온보딩 - ebn.co.kr`
- Source: `ebn.co.kr`
- Published at: `2026-06-08T19:10:18+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFAzaFNYa09WN0VhcGxOUG5HVjNzUlhzT0lNamxGdEs1WnpZMnM2RXR3TndaWmZXVE5wQmFHbnR4YnZJSHBBMk9JN09mNHVTbUF1RFQ2aFFfT2J4aTNXU1dUWUpSYzFVbVZ2?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:19:12+09:00`
- Company: [[KRX_205500_넥써쓰]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-08.json`
- Latest observation title: `[Game & Now] 넥써쓰, 방치형 RPG '아이들 판타지' 크로쓰 플랫폼 온보딩 - ebn.co.kr`
- Latest observation source: `ebn.co.kr`
- Latest observation published_at: `2026-06-08T19:10:18+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFAzaFNYa09WN0VhcGxOUG5HVjNzUlhzT0lNamxGdEs1WnpZMnM2RXR3TndaWmZXVE5wQmFHbnR4YnZJSHBBMk9JN09mNHVTbUF1RFQ2aFFfT2J4aTNXU1dUWUpSYzFVbVZ2?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
