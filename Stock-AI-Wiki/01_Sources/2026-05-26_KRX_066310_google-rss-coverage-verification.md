---
id: verification-2026-05-26-KRX-066310-google-rss-coverage
type: verification
title: KRX 066310 Google RSS Coverage Verification
created: 2026-05-26
updated: 2026-05-26
status: verification
stage: 1

market: KRX
ticker: "066310"
company: 큐에스아이
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=066310
    - name=큐에스아이
    - naver_article_count=1
    - google_rss_article_count=2
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 066310
    - 큐에스아이
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

# KRX 066310 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-26_KRX_066310_google-rss-coverage-source]]

## Facts Checked
- `code=066310`
- `name=큐에스아이`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `큐에스아이, 국내 최초 InP MPW 서비스 시행 - 뉴스와이어`
- Source: `뉴스와이어`
- Published at: `2026-02-23T17:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiX0FVX3lxTFBjS0ZsTF90a0NUdUFhdUdVUktoMVp2emlEaW0wRDZVaW1teGRPeS12dnlER05iWnFUSEtGbnBMM1ZZeTRPV2NIM2pPdEJuMzEwbi1TZ3JZSF9heVdyeFFF?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:15:28+09:00`
- Company: [[KRX_066310_큐에스아이]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-26.json`
- Latest observation title: `큐에스아이, +13.21% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-26T14:02:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNR2xJS1dvSWN6dUtDMHlRZm1nOTlmZHo4dUt5X1hDLWF0VjM3VnNUWGpiYml0OUJiQWJJZzNiWk9vLThnS1NVOEZpV2JQbXA0M2FCY1VRQ29BOTR0WUNScU15MzlkM1ppYjY2c09tRjFJU0lxbXlpRUgzWGhOR2dVVm43ONIBlwFBVV95cUxPZ2pibkhITC1nM25Dd2toLVUyUnlKVm83cWwwUGNZaXZkWWtMM3Q3U09mQVU5LVlMNldBdEVvUlVNWXBqZmQ0cmFMdXBjX01TbWludXdYSnZBZEdzM2JzWmM1WFZOaDJEQXl2YTYzYnFJeEVyT0VVdlB4TFFybjNsMlFoMk90eERYYllmR19vcnVNVTJnNEpr?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
