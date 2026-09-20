---
id: verification-2026-07-21-KRX-000660-google-rss-coverage
type: verification
title: KRX 000660 Google RSS Coverage Verification
created: 2026-07-21
updated: 2026-07-21
status: verification
stage: 1

market: KRX
ticker: "000660"
company: SK하이닉스
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
    - code=000660
    - name=SK하이닉스
    - naver_article_count=6
    - google_rss_article_count=131
    - kis_title_count=48
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000660
    - SK하이닉스
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

# KRX 000660 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-21_KRX_000660_google-rss-coverage-source]]

## Facts Checked
- `code=000660`
- `name=SK하이닉스`
- `naver_article_count=6`
- `google_rss_article_count=131`
- `kis_title_count=48`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `엔비디아 독주에 제동 건 AMD, 삼성전자·SK하이닉스 HBM4 ‘큰손’으로 부상 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-07-21T06:05:07+09:00`
- Link: `https://news.google.com/rss/articles/CBMiggFBVV95cUxOaTZqazVaR1RoTXZCdlZIUTdXUEVjUDM0Nm9KcWJDYncyOEFWbU9ka1U5cC16b0dVNmdGTldRR25BdlZyczRKVE9FWUpGSGRNQWFpOEd2LWhUY2d0VDBDTXJrcURTcEUxa2IxdjNqY2FIdnFsUmhjcWtKZU5aNmJyWURn0gGWAUFVX3lxTE1IUUZBdTFoQ2NlTk8tS3Y2ZEN1RzhONF9XYkd5NHJ6cmhsUFZFVUlwVjUzb003SGI3WDNqaG9JMkp1VjlVUGhCVWd0OGcyLW5TTnFDcXpKNkw0c3VLcFNjMEFPTi1zU0IwYlNXS2dTZjRuYjc0d3ZvZEJWNW0zX3Y0U3VqVFVYVzVFcVhDand2WDdYWlhrZw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-21T21:05:15+09:00`
- Company: [[KRX_000660_SK하이닉스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-21.json`
- Latest observation title: `엔비디아 독주에 제동 건 AMD, 삼성전자·SK하이닉스 HBM4 ‘큰손’으로 부상 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-07-21T06:05:07+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiggFBVV95cUxOaTZqazVaR1RoTXZCdlZIUTdXUEVjUDM0Nm9KcWJDYncyOEFWbU9ka1U5cC16b0dVNmdGTldRR25BdlZyczRKVE9FWUpGSGRNQWFpOEd2LWhUY2d0VDBDTXJrcURTcEUxa2IxdjNqY2FIdnFsUmhjcWtKZU5aNmJyWURn0gGWAUFVX3lxTE1IUUZBdTFoQ2NlTk8tS3Y2ZEN1RzhONF9XYkd5NHJ6cmhsUFZFVUlwVjUzb003SGI3WDNqaG9JMkp1VjlVUGhCVWd0OGcyLW5TTnFDcXpKNkw0c3VLcFNjMEFPTi1zU0IwYlNXS2dTZjRuYjc0d3ZvZEJWNW0zX3Y0U3VqVFVYVzVFcVhDand2WDdYWlhrZw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
