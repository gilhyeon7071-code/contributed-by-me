---
id: verification-2026-07-23-KRX-028260-google-rss-coverage
type: verification
title: KRX 028260 Google RSS Coverage Verification
created: 2026-07-23
updated: 2026-07-23
status: verification
stage: 1

market: KRX
ticker: "028260"
company: 삼성물산
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-23

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=028260
    - name=삼성물산
    - naver_article_count=2
    - google_rss_article_count=52
    - kis_title_count=28
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 028260
    - 삼성물산
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

# KRX 028260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-23_KRX_028260_google-rss-coverage-source]]

## Facts Checked
- `code=028260`
- `name=삼성물산`
- `naver_article_count=2`
- `google_rss_article_count=52`
- `kis_title_count=28`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `뉴로메카, 삼성물산 건설부문과 MOU…스마트빌딩 협력 - 뉴시스`
- Source: `뉴시스`
- Published at: `2026-07-21T08:38:35+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE8zX1NOZ2I4TWd2QjlNSXk3NUxaclFaeW40Y0FMX1hqZjZtczR0eDJDUG9wTTlJZzFwZzNaeGlvRWxQR01lQ3pib05XNzBiSVEwQkZ6NVEwMjF2a2U2Wnhsb9IBeEFVX3lxTE5iRVp1dWNyelZBR2JRajdSb3QxM1ZJeEhUNWpSNE9RT1pUQWFka3k0ZjJBamVweHBQT3VLTnhBZmZ3RS02X21lTUk4UHFlbG9QTDZfdmRPaS1XSk5oUnpzRzhkT1dFTnRvLUF5bGpIVTl5MmhFQUl1Zw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:30:57+09:00`
- Company: [[KRX_028260_삼성물산]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-23.json`
- Latest observation title: `뉴로메카, 삼성물산 건설부문과 MOU…스마트빌딩 협력 - 뉴시스`
- Latest observation source: `뉴시스`
- Latest observation published_at: `2026-07-21T08:38:35+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE8zX1NOZ2I4TWd2QjlNSXk3NUxaclFaeW40Y0FMX1hqZjZtczR0eDJDUG9wTTlJZzFwZzNaeGlvRWxQR01lQ3pib05XNzBiSVEwQkZ6NVEwMjF2a2U2Wnhsb9IBeEFVX3lxTE5iRVp1dWNyelZBR2JRajdSb3QxM1ZJeEhUNWpSNE9RT1pUQWFka3k0ZjJBamVweHBQT3VLTnhBZmZ3RS02X21lTUk4UHFlbG9QTDZfdmRPaS1XSk5oUnpzRzhkT1dFTnRvLUF5bGpIVTl5MmhFQUl1Zw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
