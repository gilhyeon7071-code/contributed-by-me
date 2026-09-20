---
id: verification-2026-06-20-KRX-028670-google-rss-coverage
type: verification
title: KRX 028670 Google RSS Coverage Verification
created: 2026-06-20
updated: 2026-06-20
status: verification
stage: 1

market: KRX
ticker: "028670"
company: 팬오션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-20

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=028670
    - name=팬오션
    - naver_article_count=0
    - google_rss_article_count=6
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 028670
    - 팬오션
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

# KRX 028670 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-20_KRX_028670_google-rss-coverage-source]]

## Facts Checked
- `code=028670`
- `name=팬오션`
- `naver_article_count=0`
- `google_rss_article_count=6`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `팬오션, SK에너지·SK인천석유화학과 2조7천억 규모 원유 장기 운송계약 - 비즈니스포스트`
- Source: `비즈니스포스트`
- Published at: `2026-06-19T19:50:58+09:00`
- Link: `https://news.google.com/rss/articles/CBMib0FVX3lxTE1keEpQMGYzY2c0Y3daWW1TUHFWRFEtYl9KVU04WTJRRzcwYmZfR0ZfbXZZckdwY3loMGpFaUZzYXNKZ0pvQkZ0UGpzTUdGOGk0eUFPRTRTd09fWVpZR3J5ZnVVVFZIOTRKNHMzRXJnOA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:22:42+09:00`
- Company: [[KRX_028670_팬오션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-20.json`
- Latest observation title: `팬오션, SK에너지·SK인천석유화학 2조5000억원 계약 체결…전년 매출 대비 45.5% - 데일리인베스트`
- Latest observation source: `데일리인베스트`
- Latest observation published_at: `2026-06-19T14:06:59+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE94aHd2ck5oc3J6cFNGSW44dkltMm1sYVc3YzJvX3paOHhlU2daZFVLSkZtY1BLZmt1MEJEMzZsN2lPZnVkZWNOaDhEMkNGYl95TjcwbHU5eTJzekxGaU40WnRzNWsybm9VcXJz0gFvQVVfeXFMUEQ0eEo3VjN2elBzX1lfNDZqMEg3T3dsZ3JFbUhtMW5SWG9MWTBtVUVlVnhNZGZZYzRscjFySThSUC1RQlMyTm4wOFg4LWlDb2ZCWHBBd2VQU3pqV1BOUU1Hc3JhSnlaWEdZbmZMVmpN?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
