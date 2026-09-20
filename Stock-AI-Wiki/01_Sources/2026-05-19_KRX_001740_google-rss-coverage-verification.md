---
id: verification-2026-05-19-KRX-001740-google-rss-coverage
type: verification
title: KRX 001740 Google RSS Coverage Verification
created: 2026-05-19
updated: 2026-05-19
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
  collected_at: 2026-05-19

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=001740
    - name=SK네트웍스
    - naver_article_count=3
    - google_rss_article_count=10
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
- [[2026-05-19_KRX_001740_google-rss-coverage-source]]

## Facts Checked
- `code=001740`
- `name=SK네트웍스`
- `naver_article_count=3`
- `google_rss_article_count=10`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `AI가 분석해주는 SK네트웍스(001740) 상승확률은? - 네이버 프리미엄콘텐츠`
- Source: `네이버 프리미엄콘텐츠`
- Published at: `2026-05-19T09:22:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMihgFBVV95cUxNaXdTN0xhNU10ek5ZY1NjUXNCWlZDaFhQUjN5UlRGeVZQaVN3X1dkMXc2TGMxUE9QOGRmQkk3S202bkNacmszX1Y0aXRwLVVpZ24wb3p3bHBXUXJpcW84MTZ2UXpVZlRRaHh3ZWdnRW5oY0VvQm94ZnA0R3ltS1ZiWkduVUlidw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:14:34+09:00`
- Company: [[KRX_001740_SK네트웍스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-19.json`
- Latest observation title: `AI가 분석해주는 SK네트웍스(001740) 상승확률은? - 네이버 프리미엄콘텐츠`
- Latest observation source: `네이버 프리미엄콘텐츠`
- Latest observation published_at: `2026-05-19T09:22:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMihgFBVV95cUxNaXdTN0xhNU10ek5ZY1NjUXNCWlZDaFhQUjN5UlRGeVZQaVN3X1dkMXc2TGMxUE9QOGRmQkk3S202bkNacmszX1Y0aXRwLVVpZ24wb3p3bHBXUXJpcW84MTZ2UXpVZlRRaHh3ZWdnRW5oY0VvQm94ZnA0R3ltS1ZiWkduVUlidw?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
