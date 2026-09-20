---
id: verification-2026-06-03-KRX-417860-google-rss-coverage
type: verification
title: KRX 417860 Google RSS Coverage Verification
created: 2026-06-03
updated: 2026-06-03
status: verification
stage: 1

market: KRX
ticker: "417860"
company: 오브젠
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-03

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=417860
    - name=오브젠
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 417860
    - 오브젠
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

# KRX 417860 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-03_KRX_417860_google-rss-coverage-source]]

## Facts Checked
- `code=417860`
- `name=오브젠`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `젠슨 황이 불 지핀 AI 랠리…오브젠, '삼성·LG·네이버가 택한' AI 플랫폼 기술력 '조명' - 프라임경제`
- Source: `프라임경제`
- Published at: `2026-05-29T13:27:01+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE9EWEdBeVl4ZkVvMGVDeS1EUkRQUWJsVXpwYWpLSHhzTTVab1VBTXNNTDRJbHYwczZPYWlsaGVpckR1X2tYRnN3N0hVbUZybWFRM292OXZGODJZSGNmUzVqcThWd3RHczg4RmR0NA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:17:43+09:00`
- Company: [[KRX_417860_오브젠]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-03.json`
- Latest observation title: `젠슨 황이 불 지핀 AI 랠리…오브젠, '삼성·LG·네이버가 택한' AI 플랫폼 기술력 '조명' - 프라임경제`
- Latest observation source: `프라임경제`
- Latest observation published_at: `2026-05-29T13:27:01+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTE9EWEdBeVl4ZkVvMGVDeS1EUkRQUWJsVXpwYWpLSHhzTTVab1VBTXNNTDRJbHYwczZPYWlsaGVpckR1X2tYRnN3N0hVbUZybWFRM292OXZGODJZSGNmUzVqcThWd3RHczg4RmR0NA?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
