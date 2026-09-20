---
id: verification-2026-07-08-KRX-005930-google-rss-coverage
type: verification
title: KRX 005930 Google RSS Coverage Verification
created: 2026-07-08
updated: 2026-07-08
status: verification
stage: 1

market: KRX
ticker: "005930"
company: 삼성전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=005930
    - name=삼성전자
    - naver_article_count=10
    - google_rss_article_count=0
    - kis_title_count=105
    - google_rss_covered=False
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005930
    - 삼성전자
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

# KRX 005930 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-08_KRX_005930_google-rss-coverage-source]]

## Facts Checked
- `code=005930`
- `name=삼성전자`
- `naver_article_count=10`
- `google_rss_article_count=0`
- `kis_title_count=105`
- `google_rss_covered=False`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `"삼성전자 60만원 간다"…주가 급락에도 목표가 상향 - 파이낸셜뉴스`
- Source: `파이낸셜뉴스`
- Published at: `2026-07-08T07:27:35+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE5lVER4WS1aYjNqLUxJRXlsRmlvYmR2amhGQVNhMzcxc0xpbFZwOXhMUU1fOHY2Z0J1SWdRa2dEUVZKTkt2c0hSa3Q1Q3RwQnZzYWcxOXNJRHNmZw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:27:46+09:00`
- Company: [[KRX_005930_삼성전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-08.json`
- Latest observation title: `삼성전자, 베라 루빈 탑재 'SSD PM1763' 양산 개시… AI 저장장치 경쟁 본격화 - 전자신문`
- Latest observation source: `전자신문`
- Latest observation published_at: `2026-07-08T10:25:40+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiTkFVX3lxTFA4c1BQRTljd2lkNmRzU05jN3VmTnJKckYyYlRYVld4QWR5TTFZOWVPd3p5TXZuY1paek4xdEluaDhNTzVudXRCZUhHRGpSZw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
