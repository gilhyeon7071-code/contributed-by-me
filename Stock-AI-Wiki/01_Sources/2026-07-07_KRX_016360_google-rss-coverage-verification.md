---
id: verification-2026-07-07-KRX-016360-google-rss-coverage
type: verification
title: KRX 016360 Google RSS Coverage Verification
created: 2026-07-07
updated: 2026-07-07
status: verification
stage: 1

market: KRX
ticker: "016360"
company: 삼성증권
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-07

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=016360
    - name=삼성증권
    - naver_article_count=1
    - google_rss_article_count=58
    - kis_title_count=39
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 016360
    - 삼성증권
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

# KRX 016360 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-07_KRX_016360_google-rss-coverage-source]]

## Facts Checked
- `code=016360`
- `name=삼성증권`
- `naver_article_count=1`
- `google_rss_article_count=58`
- `kis_title_count=39`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼성증권 "삼전, 2분기 영업익 86조 추정…상여충당금 16.3조" - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-07-06T08:48:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE9yYnVsaWgxUnZ4ei1LWkZyZWhJZXQwcDdTVkVMTzdHRC1QejNWOHN6ZVZac3VBOW5yM1VXbG5IMTF6Si1rYmRwYUUyZmdTdEU?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-07T08:05:22+09:00`
- Company: [[KRX_016360_삼성증권]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-07.json`
- Latest observation title: `금융위 이달 의결 안건 주목…홍콩ELS·삼성증권 제재 초읽기 - 뉴시스`
- Latest observation source: `뉴시스`
- Latest observation published_at: `2026-07-07T07:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFBQdXhpVVJhLUJjUE1aVktPNHFJd3FYSnh4NUV2TGt5RU40aF9yVGxIUUg0SXRHaDMtTzh5M0ZrVzdtVndHUXQ3Z2h6Z2xiX3BQTUFYbkNXMFFlME9VNXNwd9IBeEFVX3lxTE56b3F3YV9NR09pNjJvVGRILTZQdmI3aFhqd2kwaGR1SmdWYTFIMmRLdWgzZ0JqTmJTV2xMUGhNQlpYeG1MaFB0MkZXckl6aWVkVWpZRU5UN29jLTI2cVVHaDhTLUtqaDVaZHdGdUtkNUMzWHFvWE15Vg?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
