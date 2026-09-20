---
id: verification-2026-07-21-KRX-267260-google-rss-coverage
type: verification
title: KRX 267260 Google RSS Coverage Verification
created: 2026-07-21
updated: 2026-07-21
status: verification
stage: 1

market: KRX
ticker: "267260"
company: HD현대일렉트릭
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=267260
    - name=HD현대일렉트릭
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 267260
    - HD현대일렉트릭
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

# KRX 267260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-21_KRX_267260_google-rss-coverage-source]]

## Facts Checked
- `code=267260`
- `name=HD현대일렉트릭`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `HD현대일렉, 글로벌 빅테크 기업과 1.1조 전력기기 공급 계약 - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-07-02T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE5qTWlWM3B5SFhQcVhBeUo0dFNhR3psbk1WaGRpRkxrTUF0dkpjRmFIa2xudTlNckhTUVR6dFhxU2ZpSTR0QUloS1lMYXhaYVBsa0RVanBCdDhZRjVqdHlCU2I4SURZa0d2d1k4WdIBckFVX3lxTE1lcEFkMWp5Yl9qY1o4T0lUYjZkcXc1dnJob0c0OWZ1eFNhU0hUWHZxNGtaOWdXQlhvYlEtU3J5a3V2OU9vd1I0RWFGb29CVHFxRG5MalZweDNvQWlZai1HQXJQaWVrTVYzOWZyZFE4Z09Qdw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:30:18+09:00`
- Company: [[KRX_267260_HD현대일렉트릭]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-21.json`
- Latest observation title: `HD현대일렉트릭, 9월 8일 임시주주총회 소집 결의…감사위원 독립이사에 김우찬 후보 신규 선임 안건 - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-07-21T17:44:01+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE1pejhmTnFYdVV5Nm1IeEI4VTd6RWtOTnhCR3JsanJTSGdFb0tpNGpzWGdWbEpoRkxuSkZVblA1czVIbldsTXJMekEwN3pJUEwzX0NscU9KaDI2b0hySTB0UmFvdjBqNC1lN0dOdy1vdXNNWWc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
