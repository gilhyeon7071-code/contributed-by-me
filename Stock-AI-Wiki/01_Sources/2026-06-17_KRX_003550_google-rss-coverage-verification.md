---
id: verification-2026-06-17-KRX-003550-google-rss-coverage
type: verification
title: KRX 003550 Google RSS Coverage Verification
created: 2026-06-17
updated: 2026-06-17
status: verification
stage: 1

market: KRX
ticker: "003550"
company: LG
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-17

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=003550
    - name=LG
    - naver_article_count=0
    - google_rss_article_count=37
    - kis_title_count=16
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003550
    - LG
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

# KRX 003550 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-17_KRX_003550_google-rss-coverage-source]]

## Facts Checked
- `code=003550`
- `name=LG`
- `naver_article_count=0`
- `google_rss_article_count=37`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LG전자, 스페인 1천세대 주거단지에 고효율 히트펌프 수주 - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-06-17T10:05:14+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE9ELXl2U0lGUldFS1V4SVJOX1NKOF83LVZhd2R2QlF2Um9tRjZISmo1TVlhcVF1NTNOekh2UmcwdlZIQUNaaXVxTTdMaE9aYXVQVXc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-17T17:05:11+09:00`
- Company: [[KRX_003550_LG]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-17.json`
- Latest observation title: `“AI 통과 못하면 월급 깎여” LG전자 방문점검원 ‘한숨’ - 매일노동뉴스`
- Latest observation source: `매일노동뉴스`
- Latest observation published_at: `2026-06-15T06:30:03+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTE9KWktEYmd5VlFVUHZEeFB1ZXBQT0FXbFV1MTlfczJrUUFpTld3OHIwWWhObW5WNlAxaTc1cTZsRDQ4eFR6SkZncllwUmMydjRGaTJGdU5MSGdpN052dE9FSlA5REx6VlNpdUY4LTJWOHU?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
