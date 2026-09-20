---
id: verification-2026-05-27-KRX-066310-google-rss-coverage
type: verification
title: KRX 066310 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
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
  collected_at: 2026-05-27

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
- [[2026-05-27_KRX_066310_google-rss-coverage-source]]

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
- Title: `큐에스아이, +13.21% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-26T13:56:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMilwFBVV95cUxPZ2pibkhITC1nM25Dd2toLVUyUnlKVm83cWwwUGNZaXZkWWtMM3Q3U09mQVU5LVlMNldBdEVvUlVNWXBqZmQ0cmFMdXBjX01TbWludXdYSnZBZEdzM2JzWmM1WFZOaDJEQXl2YTYzYnFJeEVyT0VVdlB4TFFybjNsMlFoMk90eERYYllmR19vcnVNVTJnNEpr0gGXAUFVX3lxTE9namJuSEhMLWczbkN3a2gtVTJSeUpWbzdxbDBQY1lpdmRZa0wzdDdTT2ZBVTktWUw2V0F0RW9SVU1ZcGpmZDRyYUx1cGNfTVNtaW51d1hKdkFkR3MzYnNaYzVYVk5oMkRBeXZhNjNicUl4RXJPRVV2UHhMUXJuM2wyUWgyT3R4RFhiWWZHX29ydU1VMmc0Sms?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-27T09:05:50+09:00`
- Company: [[KRX_066310_큐에스아이]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `큐에스아이, +13.21% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-26T13:56:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxPZ2pibkhITC1nM25Dd2toLVUyUnlKVm83cWwwUGNZaXZkWWtMM3Q3U09mQVU5LVlMNldBdEVvUlVNWXBqZmQ0cmFMdXBjX01TbWludXdYSnZBZEdzM2JzWmM1WFZOaDJEQXl2YTYzYnFJeEVyT0VVdlB4TFFybjNsMlFoMk90eERYYllmR19vcnVNVTJnNEpr0gGXAUFVX3lxTE9namJuSEhMLWczbkN3a2gtVTJSeUpWbzdxbDBQY1lpdmRZa0wzdDdTT2ZBVTktWUw2V0F0RW9SVU1ZcGpmZDRyYUx1cGNfTVNtaW51d1hKdkFkR3MzYnNaYzVYVk5oMkRBeXZhNjNicUl4RXJPRVV2UHhMUXJuM2wyUWgyT3R4RFhiWWZHX29ydU1VMmc0Sms?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
