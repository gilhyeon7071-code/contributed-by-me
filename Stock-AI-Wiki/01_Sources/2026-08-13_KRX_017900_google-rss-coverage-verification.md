---
id: verification-2026-08-13-KRX-017900-google-rss-coverage
type: verification
title: KRX 017900 Google RSS Coverage Verification
created: 2026-08-13
updated: 2026-08-13
status: verification
stage: 1

market: KRX
ticker: "017900"
company: 광전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-13

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=017900
    - name=광전자
    - naver_article_count=1
    - google_rss_article_count=10
    - kis_title_count=16
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 017900
    - 광전자
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

# KRX 017900 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-13_KRX_017900_google-rss-coverage-source]]

## Facts Checked
- `code=017900`
- `name=광전자`
- `naver_article_count=1`
- `google_rss_article_count=10`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `광전자 주가, 8월 12일 장중 9,090원 4.60% 상승 - 톱스타뉴스`
- Source: `톱스타뉴스`
- Published at: `2026-08-12T09:50:56+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTFBHQkNIc2d4eXBTNGlaLVNPaXhwY183TTVvYXBoMi03bHRfRjBSQV84MWhSaVh4WGZaWEtGWTJvWGtQbnA4bmtwZ0JWUTl2QUF5QjRDWTV2aUwtRkJNLVNYQ0xmbVRVaEM5QUw5LXROUTN3dw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-13T10:05:56+09:00`
- Company: [[KRX_017900_광전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-13.json`
- Latest observation title: `특징주, 광전자-마이크로 LED 테마 상승세에 15.24% ↑ - 매일경제 마켓`
- Latest observation source: `매일경제 마켓`
- Latest observation published_at: `2026-08-10T09:44:16+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE9KbGtIWHQtbHh6VHUzUTlscW84WWZOVWJHT2ZIaXRrLVZoYTk4Sm1PdVNESFdvbE1PdUFTbEdyWTBBeVFtUWNEd1V1emtZMVNJaXc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
