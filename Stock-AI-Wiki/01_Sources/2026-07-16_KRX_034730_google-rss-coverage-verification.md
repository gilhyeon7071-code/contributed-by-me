---
id: verification-2026-07-16-KRX-034730-google-rss-coverage
type: verification
title: KRX 034730 Google RSS Coverage Verification
created: 2026-07-16
updated: 2026-07-16
status: verification
stage: 1

market: KRX
ticker: "034730"
company: SK
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-16

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=034730
    - name=SK
    - naver_article_count=2
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 034730
    - SK
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

# KRX 034730 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-16_KRX_034730_google-rss-coverage-source]]

## Facts Checked
- `code=034730`
- `name=SK`
- `naver_article_count=2`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[단독]SK하이닉스, '영업익 10% 성과급' 공식 바꾼다 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-07-15T15:17:05+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFBnX1A1RFpCMXIyY1ZUUUp1X3g5WldXaHluaXBHQmlSYVdJalZZYURHX0FpaUJMczhNQ3IwNUI4bU5ReU12YzR5a2Zpa1V4Z0U?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:29:40+09:00`
- Company: [[KRX_034730_SK]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-16.json`
- Latest observation title: `SK하닉 ADR 상호전환 29일 이후 가능…美 프리미엄 수혜 가능성 - 뉴스1`
- Latest observation source: `뉴스1`
- Latest observation published_at: `2026-07-15T10:49:29+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiX0FVX3lxTE9QTUpSNk5ibnZKaHdfZlo5NVdaQzhTa2FzbVdWYmxrQk5DLUJTNURXNV9PQlJlbWtZQ2NPb0RVVUlGNVdicTVhZFFjQ2ZORXNXTlFhYWdnUi1YWmszeFQ0?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
