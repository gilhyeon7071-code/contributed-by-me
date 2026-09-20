---
id: verification-2026-05-20-KRX-001740-google-rss-coverage
type: verification
title: KRX 001740 Google RSS Coverage Verification
created: 2026-05-20
updated: 2026-05-20
status: verification
stage: 1

market: KRX
ticker: "001740"
company: SK네트웍스
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
    - code=001740
    - name=SK네트웍스
    - naver_article_count=3
    - google_rss_article_count=13
    - kis_title_count=16
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 001740
    - SK네트웍스
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

# KRX 001740 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-20_KRX_001740_google-rss-coverage-source]]

## Facts Checked
- `code=001740`
- `name=SK네트웍스`
- `naver_article_count=3`
- `google_rss_article_count=13`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `SK네트웍스, +1.09% 상승폭 확대 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-19T12:47:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxQaHM3clNBOHBkTXZUTTFKeko5WE5rOFEwQ0FVVUVsYkc0MDNUYnBueUhRWE9aMGtaVkxvYXJRb1o5WHRjdDRkUVYxektxdmZ0YkZyYTFOenBUSV96NUdLTDV5elpEMHItSlFfUm51MDNQaXJKdjg2cDMxWGlST3M4a0U0Z9IBlwFBVV95cUxQZ1h0VWM2ZV8xY195SWdOb2RLc2djczRteXpxNlRpa2U3dGhpUTBrTTRmOUhuUG5CajR4ZUl6MzFvVWJNZWVhNlZLcmZaanNrNUVBQWVrT0szY3RIWW1mNDhfdnNFNHhDd09tSk1LWFQ3R3oyRGh4WVV4bHBud0owWXJRNGIyWWc3TmVjNFl6ZDVpZ3dzdDRr?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-20T08:45:13+09:00`
- Company: [[KRX_001740_SK네트웍스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-20.json`
- Latest observation title: `SK네트웍스, +1.09% 상승폭 확대 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-19T12:47:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxQaHM3clNBOHBkTXZUTTFKeko5WE5rOFEwQ0FVVUVsYkc0MDNUYnBueUhRWE9aMGtaVkxvYXJRb1o5WHRjdDRkUVYxektxdmZ0YkZyYTFOenBUSV96NUdLTDV5elpEMHItSlFfUm51MDNQaXJKdjg2cDMxWGlST3M4a0U0Z9IBlwFBVV95cUxQZ1h0VWM2ZV8xY195SWdOb2RLc2djczRteXpxNlRpa2U3dGhpUTBrTTRmOUhuUG5CajR4ZUl6MzFvVWJNZWVhNlZLcmZaanNrNUVBQWVrT0szY3RIWW1mNDhfdnNFNHhDd09tSk1LWFQ3R3oyRGh4WVV4bHBud0owWXJRNGIyWWc3TmVjNFl6ZDVpZ3dzdDRr?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
