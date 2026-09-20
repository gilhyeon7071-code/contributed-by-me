---
id: verification-2026-05-21-KRX-017800-google-rss-coverage
type: verification
title: KRX 017800 Google RSS Coverage Verification
created: 2026-05-21
updated: 2026-05-21
status: verification
stage: 1

market: KRX
ticker: "017800"
company: 현대엘리베이터
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=017800
    - name=현대엘리베이터
    - naver_article_count=2
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 017800
    - 현대엘리베이터
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

# KRX 017800 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-21_KRX_017800_google-rss-coverage-source]]

## Facts Checked
- `code=017800`
- `name=현대엘리베이터`
- `naver_article_count=2`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `승강기를 넘어 도시를 움직인다…현대엘리베이터, '도시 운영체제' 기업으로 진화 - 폴리뉴스 Polinews`
- Source: `폴리뉴스 Polinews`
- Published at: `2026-05-18T10:57:48+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTE9MQUQ2TFNpSkpmNUlUMkwzT1MyakRpblNjNEpfZFdQLVdDVEg2ZEotZ2ZnZkgxY1BxeDBWYXlPYU5KNEVkd016bHNXMkJRSVh5WGR1c1FHbEI1OTItTXJtVnlGaDVFbkRwYzY4UEZYVEpMd9IBckFVX3lxTE9MQUQ2TFNpSkpmNUlUMkwzT1MyakRpblNjNEpfZFdQLVdDVEg2ZEotZ2ZnZkgxY1BxeDBWYXlPYU5KNEVkd016bHNXMkJRSVh5WGR1c1FHbEI1OTItTXJtVnlGaDVFbkRwYzY4UEZYVEpMdw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:14:51+09:00`
- Company: [[KRX_017800_현대엘리베이터]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-21.json`
- Latest observation title: `승강기를 넘어 도시를 움직인다…현대엘리베이터, '도시 운영체제' 기업으로 진화 - 폴리뉴스 Polinews`
- Latest observation source: `폴리뉴스 Polinews`
- Latest observation published_at: `2026-05-18T10:57:48+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTE9MQUQ2TFNpSkpmNUlUMkwzT1MyakRpblNjNEpfZFdQLVdDVEg2ZEotZ2ZnZkgxY1BxeDBWYXlPYU5KNEVkd016bHNXMkJRSVh5WGR1c1FHbEI1OTItTXJtVnlGaDVFbkRwYzY4UEZYVEpMd9IBckFVX3lxTE9MQUQ2TFNpSkpmNUlUMkwzT1MyakRpblNjNEpfZFdQLVdDVEg2ZEotZ2ZnZkgxY1BxeDBWYXlPYU5KNEVkd016bHNXMkJRSVh5WGR1c1FHbEI1OTItTXJtVnlGaDVFbkRwYzY4UEZYVEpMdw?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
