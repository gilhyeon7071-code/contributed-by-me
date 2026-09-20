---
id: source-2026-07-28-KRX-034730-google-rss-coverage
type: source
title: KRX 034730 Google RSS Coverage Source
created: 2026-07-28
updated: 2026-07-28
status: raw
stage: 0

market: KRX
ticker: "034730"
company: SK
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 034730.
  key_facts:
    - code=034730
    - name=SK
    - naver_article_count=12
    - google_rss_article_count=124
    - kis_title_count=36
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034730
    - SK
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

# KRX 034730 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=034730`
- `name=SK`
- `naver_article_count=12`
- `google_rss_article_count=124`
- `kis_title_count=36`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 034730.

## RSS Item Metadata
- Title: `[단독]SK하이닉스, 서울 한복판에 거점 세운다...강남 르메르디앙 호텔 자리에 사옥 건립 추진 - 아시아경제`
- Source: `아시아경제`
- Published at: `2026-07-27T17:07:23+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE14RU9BX051M3l5aU9BWGZZMlJtbm5MbHdvX2psa21jbE12V3FSNlZOdFFTWnNNQTdlWDdQdU1RcHk1RUViOEdmTm9reExrVjJUVWd3dnhHSXNIRkVVdlY3bA?oc=5`

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
- Updated at: `2026-07-28T08:05:50+09:00`
- Company: [[KRX_034730_SK]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `[단독]SK하이닉스, 서울 한복판에 거점 세운다...강남 르메르디앙 호텔 자리에 사옥 건립 추진 - 아시아경제`
- Latest observation source: `아시아경제`
- Latest observation published_at: `2026-07-27T17:07:23+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE14RU9BX051M3l5aU9BWGZZMlJtbm5MbHdvX2psa21jbE12V3FSNlZOdFFTWnNNQTdlWDdQdU1RcHk1RUViOEdmTm9reExrVjJUVWd3dnhHSXNIRkVVdlY3bA?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
