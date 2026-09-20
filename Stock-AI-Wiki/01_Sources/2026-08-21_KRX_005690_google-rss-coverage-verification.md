---
id: verification-2026-08-21-KRX-005690-google-rss-coverage
type: verification
title: KRX 005690 Google RSS Coverage Verification
created: 2026-08-21
updated: 2026-08-21
status: verification
stage: 1

market: KRX
ticker: "005690"
company: 파미셀
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=005690
    - name=파미셀
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=19
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005690
    - 파미셀
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

# KRX 005690 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-21_KRX_005690_google-rss-coverage-source]]

## Facts Checked
- `code=005690`
- `name=파미셀`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=19`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `파미셀 주가, 8월 20일 고점 대비 하락 11,860원 4.22% 상승 마감 - 톱스타뉴스`
- Source: `톱스타뉴스`
- Published at: `2026-08-20T16:01:50+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTE5BRmtieGdxa2t5Y1o3cVpZUkhheWdoRnVsV2pJS2Jud2FJQXNrUWxad21GNXhNSVZtS2xiSUI0U21hVHZ3Q3J5SDUwdml5c0hIajFqU2o4dkFLRjI5YXhmZmlDYzlraWJyRHB4anVaY0RrUQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T23:05:26+09:00`
- Company: [[KRX_005690_파미셀]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-21.json`
- Latest observation title: `파미셀, 더마 코스메틱 브랜드 ‘플레이셀’ 리뉴얼 출시 - 팜이데일리`
- Latest observation source: `팜이데일리`
- Latest observation published_at: `2026-08-14T10:38:40+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibkFVX3lxTFB3R1haXzV5cmVMUlJjaFY5azBUa3p3SEt0VHdzRS1YVk5tU3dPYnlQamttX3A5UXFEVDN2aWtIb09WaUlqX0pFdzVEVTVQeHBBcEVfWGppd3lfQkNiM3VTODkySDFreF90YzNSSUVB?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
