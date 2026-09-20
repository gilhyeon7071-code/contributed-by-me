---
id: verification-2026-06-07-KRX-073490-google-rss-coverage
type: verification
title: KRX 073490 Google RSS Coverage Verification
created: 2026-06-07
updated: 2026-06-07
status: verification
stage: 1

market: KRX
ticker: "073490"
company: 이노와이어리스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-07

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=073490
    - name=이노와이어리스
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 073490
    - 이노와이어리스
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

# KRX 073490 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-07_KRX_073490_google-rss-coverage-source]]

## Facts Checked
- `code=073490`
- `name=이노와이어리스`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `이노와이어리스, 'LIG아큐버'로 사명 변경…AI·방산·전장 등 사업 확장 - 전자신문`
- Source: `전자신문`
- Published at: `2026-01-16T17:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiTkFVX3lxTE5ITGo4TGRnOU5wQV9iY0ZIeERNX3hzWnlseHF4UEwtY2Q0NzlLUGNwWmhKVzZfYkdxUGZMaGVLWkZ3Q0JaU1Iza3JrLTNXZw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:18:54+09:00`
- Company: [[KRX_073490_이노와이어리스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-07.json`
- Latest observation title: `이노와이어리스, 'LIG아큐버'로 사명 변경…AI·방산·전장 등 사업 확장 - 전자신문`
- Latest observation source: `전자신문`
- Latest observation published_at: `2026-01-16T17:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiTkFVX3lxTE5ITGo4TGRnOU5wQV9iY0ZIeERNX3hzWnlseHF4UEwtY2Q0NzlLUGNwWmhKVzZfYkdxUGZMaGVLWkZ3Q0JaU1Iza3JrLTNXZw?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
