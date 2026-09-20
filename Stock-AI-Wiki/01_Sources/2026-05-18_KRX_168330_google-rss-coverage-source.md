---
id: source-2026-05-18-KRX-168330-google-rss-coverage
type: source
title: KRX 168330 Google RSS Coverage Source
created: 2026-05-18
updated: 2026-05-18
status: raw
stage: 0

market: KRX
ticker: "168330"
company: 내츄럴엔도텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-18

analysis:
  summary: Local coverage report row shows news coverage for KRX 168330.
  key_facts:
    - code=168330
    - name=내츄럴엔도텍
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=6
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 168330
    - 내츄럴엔도텍
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

# KRX 168330 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=168330`
- `name=내츄럴엔도텍`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=6`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 168330.

## RSS Item Metadata
- Title: `내츄럴엔도텍, 2026년 1분기 실적 발표...매출액 57억 원 - Investing.com 한국어`
- Source: `Investing.com 한국어`
- Published at: `2026-05-15T11:41:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBRVzdQN1d6OW84UjM3YXFBTmNDUWkwNDI5R1pPWnZadUlpSkpyOUVVQm9HOUhNOTA5RkZQSG5WRUVLNGJXVjRubGZnY0lsUFM4U3o1RUp4VFZJZy1nZG9rYkY5YThmS19qWmNVNnVVekxYM0k?oc=5`

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
- Updated at: `2026-08-21T19:14:22+09:00`
- Company: [[KRX_168330_내츄럴엔도텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-18.json`
- Latest observation title: `내츄럴엔도텍, 2026년 1분기 실적 발표...매출액 57억 원 - Investing.com 한국어`
- Latest observation source: `Investing.com 한국어`
- Latest observation published_at: `2026-05-15T11:41:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBRVzdQN1d6OW84UjM3YXFBTmNDUWkwNDI5R1pPWnZadUlpSkpyOUVVQm9HOUhNOTA5RkZQSG5WRUVLNGJXVjRubGZnY0lsUFM4U3o1RUp4VFZJZy1nZG9rYkY5YThmS19qWmNVNnVVekxYM0k?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
