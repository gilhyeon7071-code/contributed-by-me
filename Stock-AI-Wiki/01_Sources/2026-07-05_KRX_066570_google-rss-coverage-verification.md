---
id: verification-2026-07-05-KRX-066570-google-rss-coverage
type: verification
title: KRX 066570 Google RSS Coverage Verification
created: 2026-07-05
updated: 2026-07-05
status: verification
stage: 1

market: KRX
ticker: "066570"
company: LG전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-05

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=066570
    - name=LG전자
    - naver_article_count=3
    - google_rss_article_count=102
    - kis_title_count=31
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 066570
    - LG전자
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

# KRX 066570 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-05_KRX_066570_google-rss-coverage-source]]

## Facts Checked
- `code=066570`
- `name=LG전자`
- `naver_article_count=3`
- `google_rss_article_count=102`
- `kis_title_count=31`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `“환율 호재에 이노텍 날개까지…LG전자, 2분기 역대급 ‘판타스틱’ 실적 예고” - 뉴스퀘스트`
- Source: `뉴스퀘스트`
- Published at: `2026-07-03T13:53:37+09:00`
- Link: `https://news.google.com/rss/articles/CBMib0FVX3lxTE50aHpGdFJQWnB1QWE1ZlhWSEdkcnFKOWhRaHZQY1Z0SGdGT2FJcUxpZDBkdklWN2l6Y2pIUWZNQTdVNXRkM1NYNHIzdXYtWjFYUHFhTndKTU83T3JRV044VTVmaDBCemFaNHZQcUZVNNIBc0FVX3lxTE5NMGtHbnBHa0d6eEVoWFNsV3FEZ1pOVDdsY0ZLdkhvWEI4YTdpZzMzTV96NERqYVNfRjFKc1diWnQ2NU1LWFZRYkpjRmJCQXJvTHpBSTkxQ0prUV9UVDlyakdzemkxMkxRNlFhdXkzRWp5VzQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:26:51+09:00`
- Company: [[KRX_066570_LG전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-05.json`
- Latest observation title: `“환율 호재에 이노텍 날개까지…LG전자, 2분기 역대급 ‘판타스틱’ 실적 예고” - 뉴스퀘스트`
- Latest observation source: `뉴스퀘스트`
- Latest observation published_at: `2026-07-03T13:53:37+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMib0FVX3lxTE50aHpGdFJQWnB1QWE1ZlhWSEdkcnFKOWhRaHZQY1Z0SGdGT2FJcUxpZDBkdklWN2l6Y2pIUWZNQTdVNXRkM1NYNHIzdXYtWjFYUHFhTndKTU83T3JRV044VTVmaDBCemFaNHZQcUZVNNIBc0FVX3lxTE5NMGtHbnBHa0d6eEVoWFNsV3FEZ1pOVDdsY0ZLdkhvWEI4YTdpZzMzTV96NERqYVNfRjFKc1diWnQ2NU1LWFZRYkpjRmJCQXJvTHpBSTkxQ0prUV9UVDlyakdzemkxMkxRNlFhdXkzRWp5VzQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
