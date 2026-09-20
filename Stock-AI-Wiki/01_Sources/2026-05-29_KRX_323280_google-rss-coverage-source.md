---
id: source-2026-05-29-KRX-323280-google-rss-coverage
type: source
title: KRX 323280 Google RSS Coverage Source
created: 2026-05-29
updated: 2026-05-29
status: raw
stage: 0

market: KRX
ticker: "323280"
company: 태성
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-29

analysis:
  summary: Local coverage report row shows news coverage for KRX 323280.
  key_facts:
    - code=323280
    - name=태성
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 323280
    - 태성
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

# KRX 323280 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=323280`
- `name=태성`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 323280.

## RSS Item Metadata
- Title: `태성, -11.76% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-28T13:17:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNX1ZyYmpJWktFSjNJeENPbWo1b0swYlRxem5JeXZzS1pxU0pCSE9vOWt6d3A0UVBWRk05RnVUT0RRNWxnRG9Ib3FBNDMzdHM4Q2d5ZEdPTHdYM1Y2dkpUWnhvZXlFazBOalo1eUFmRHdndXYtTWJLZ2FSNWQ2a0pBY0tuQdIBlwFBVV95cUxPUl9pbDJzME81SVFFbHdiQ2xFb2w5RUNrbDRCOFR2OTljUEhvUnhfNThuWGxWa1hKbkNOZVFXYUgweWkzR1ZhbW8zc1VGRWw5Sm1PazRDUHliZGhmOUw0ZUU2MkdrVVF4MHNOOHNGSkpqSkRQdTlJMjlEYzBSM1JRelBuMnpENUI1YTA2S2lWTHpHRVhfcUYw?oc=5`

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
- Updated at: `2026-05-29T08:05:03+09:00`
- Company: [[KRX_323280_태성]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-29.json`
- Latest observation title: `태성, -11.76% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-28T13:17:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNX1ZyYmpJWktFSjNJeENPbWo1b0swYlRxem5JeXZzS1pxU0pCSE9vOWt6d3A0UVBWRk05RnVUT0RRNWxnRG9Ib3FBNDMzdHM4Q2d5ZEdPTHdYM1Y2dkpUWnhvZXlFazBOalo1eUFmRHdndXYtTWJLZ2FSNWQ2a0pBY0tuQdIBlwFBVV95cUxPUl9pbDJzME81SVFFbHdiQ2xFb2w5RUNrbDRCOFR2OTljUEhvUnhfNThuWGxWa1hKbkNOZVFXYUgweWkzR1ZhbW8zc1VGRWw5Sm1PazRDUHliZGhmOUw0ZUU2MkdrVVF4MHNOOHNGSkpqSkRQdTlJMjlEYzBSM1JRelBuMnpENUI1YTA2S2lWTHpHRVhfcUYw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
