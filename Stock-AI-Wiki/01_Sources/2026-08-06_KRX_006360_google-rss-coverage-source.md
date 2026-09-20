---
id: source-2026-08-06-KRX-006360-google-rss-coverage
type: source
title: KRX 006360 Google RSS Coverage Source
created: 2026-08-06
updated: 2026-08-06
status: raw
stage: 0

market: KRX
ticker: "006360"
company: GS건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-06

analysis:
  summary: Local coverage report row shows news coverage for KRX 006360.
  key_facts:
    - code=006360
    - name=GS건설
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 006360
    - GS건설
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is not stored locally.

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

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
  change_reason: generated coverage source note
---

# KRX 006360 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=006360`
- `name=GS건설`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 006360.

## RSS Item Metadata
- Title: `서울역 1조 재개발…삼성물산·GS건설 '11년 만의 리턴매치' 성사되나 - 뉴스1`
- Source: `뉴스1`
- Published at: `2026-08-03T06:15:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTFBGMTNjOWZWUnpicVNLSXdCNkZwV014dktpbmJRRlNqb1hEWUdVbkk3OUp0a1djTzMyZWxqV1F4VFZpQU9rQkdfQkNTTE9mYXh3SHhHVkhkd3otSkU?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:33:31+09:00`
- Company: [[KRX_006360_GS건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-06.json`
- Latest observation title: `[특징주] GS건설, SMR 세제 혜택 및 빌 게이츠 방한 겹호재에 15%대 급등 - 와이드경제`
- Latest observation source: `와이드경제`
- Latest observation published_at: `2026-08-04T11:11:42+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTE1uSFZYQXlDd0F0R2l3eVhnUXhiX1M3cW4xTFR4VEowd3FSTy1vRFg1bkdiTjdsUFRVUVlOU2JYVzlXS2Y5OW8wYU8zT0lDTlNoOEFxQ0FzOHZ4cUZ3Y1RmVmc1MlcyZWVQTWZLOA?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
