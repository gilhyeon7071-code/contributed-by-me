---
id: source-2026-07-08-KRX-042700-google-rss-coverage
type: source
title: KRX 042700 Google RSS Coverage Source
created: 2026-07-08
updated: 2026-07-08
status: raw
stage: 0

market: KRX
ticker: "042700"
company: 한미반도체
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-08

analysis:
  summary: Local coverage report row shows news coverage for KRX 042700.
  key_facts:
    - code=042700
    - name=한미반도체
    - naver_article_count=1
    - google_rss_article_count=2
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 042700
    - 한미반도체
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

# KRX 042700 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=042700`
- `name=한미반도체`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 042700.

## RSS Item Metadata
- Title: `3800% 폭등 한미반도체 맞힌 남자 “다음엔 이 종목 10배 뛴다” - 중앙일보`
- Source: `중앙일보`
- Published at: `2026-07-06T16:21:11+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE01Z0VTNmUxTTdZWWtoY1UyUkM5Rk5XTWdjS0NwTkwxc2VfTlduN3BsXzYzTjlZUnN5OUhUTUdMMVRYUWZkOWo5YmRlUW9pRW9yV3JlV01B?oc=5`

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
- Updated at: `2026-07-08T08:05:11+09:00`
- Company: [[KRX_042700_한미반도체]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-08.json`
- Latest observation title: `3800% 폭등 한미반도체 맞힌 남자 “다음엔 이 종목 10배 뛴다” - 중앙일보`
- Latest observation source: `중앙일보`
- Latest observation published_at: `2026-07-06T16:21:11+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE01Z0VTNmUxTTdZWWtoY1UyUkM5Rk5XTWdjS0NwTkwxc2VfTlduN3BsXzYzTjlZUnN5OUhUTUdMMVRYUWZkOWo5YmRlUW9pRW9yV3JlV01B?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
