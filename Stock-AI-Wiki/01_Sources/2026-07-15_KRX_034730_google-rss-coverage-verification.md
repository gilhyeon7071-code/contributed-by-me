---
id: verification-2026-07-15-KRX-034730-google-rss-coverage
type: verification
title: KRX 034730 Google RSS Coverage Verification
created: 2026-07-15
updated: 2026-07-15
status: verification
stage: 1

market: KRX
ticker: "034730"
company: SK
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-15

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=034730
    - name=SK
    - naver_article_count=21
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 034730
    - SK
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

# KRX 034730 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-15_KRX_034730_google-rss-coverage-source]]

## Facts Checked
- `code=034730`
- `name=SK`
- `naver_article_count=21`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `SK하이닉스, 용인 'Y1' 팹 구축 본격화…장비 발주 시작 - 지디넷코리아`
- Source: `지디넷코리아`
- Published at: `2026-07-14T10:40:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE1HUmJPUHd6NFhxT09DdE93cUNkOWFyNldDNWd2MXNROTY4UEprQWNiT1dxeXZBX1JqSHJhZHNhamVmQ0tDSXRNUDVJeHRzZUtta1d3Z0ZR?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-15T23:05:24+09:00`
- Company: [[KRX_034730_SK]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-15.json`
- Latest observation title: `젠슨 황 “SK하이닉스 ADR, 믿을 수 없을 정도로 성공적” - 경향신문`
- Latest observation source: `경향신문`
- Latest observation published_at: `2026-07-15T19:55:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE1tUHh6SlYyZS1xQ0toWGEzNHFPQVJQdmQtcVhQNThaaE1qZDVwNnBiUUlEN0lCRW9TQ1lJNS1heW4ySWlOTlFxQlpxY3ZudGNCSnREbC1IT0NEd9IBX0FVX3lxTE9kZ29DTWlOVEZra0piZjRac0kxcjB1MEFMQlJXNlVxU0tCM1NGX2tGUXlSMl8xX093N0ZMSTdxSERCc05aRW8wZUp2eUVFbF9WYjRpTUktRmE4VHFrYl9r?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
