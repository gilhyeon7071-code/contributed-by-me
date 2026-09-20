---
id: verification-2026-07-01-KRX-032820-google-rss-coverage
type: verification
title: KRX 032820 Google RSS Coverage Verification
created: 2026-07-01
updated: 2026-07-01
status: verification
stage: 1

market: KRX
ticker: "032820"
company: 우리기술
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-01

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=032820
    - name=우리기술
    - naver_article_count=1
    - google_rss_article_count=8
    - kis_title_count=6
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 032820
    - 우리기술
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

# KRX 032820 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-01_KRX_032820_google-rss-coverage-source]]

## Facts Checked
- `code=032820`
- `name=우리기술`
- `naver_article_count=1`
- `google_rss_article_count=8`
- `kis_title_count=6`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `인베스팅프로 적정가치, 우리기술 57% 폭락 예측 성공 - Investing.com 한국어`
- Source: `Investing.com 한국어`
- Published at: `2026-06-29T20:48:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMidkFVX3lxTE5waVVUY2ZQRWJuSnF6YXlMZWVuUkJ3ZHhaaVJndWlMSEJnc2h5T2FqZDNZcFU2ZTkyQjEtVERlSjhCeWpVT2t0VDNJd1dWbjEtQjRxN0VRV1JRMWtYMDRTdzE2VXYzc3FaSjd6WEd5R0JGa3huS2c?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-01T11:05:15+09:00`
- Company: [[KRX_032820_우리기술]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-01.json`
- Latest observation title: `인베스팅프로 적정가치, 우리기술 57% 폭락 예측 성공 - Investing.com 한국어`
- Latest observation source: `Investing.com 한국어`
- Latest observation published_at: `2026-06-29T20:48:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMidkFVX3lxTE5waVVUY2ZQRWJuSnF6YXlMZWVuUkJ3ZHhaaVJndWlMSEJnc2h5T2FqZDNZcFU2ZTkyQjEtVERlSjhCeWpVT2t0VDNJd1dWbjEtQjRxN0VRV1JRMWtYMDRTdzE2VXYzc3FaSjd6WEd5R0JGa3huS2c?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
