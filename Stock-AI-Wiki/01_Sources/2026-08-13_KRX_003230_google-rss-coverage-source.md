---
id: source-2026-08-13-KRX-003230-google-rss-coverage
type: source
title: KRX 003230 Google RSS Coverage Source
created: 2026-08-13
updated: 2026-08-13
status: raw
stage: 0

market: KRX
ticker: "003230"
company: 삼양식품
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-13

analysis:
  summary: Local coverage report row shows news coverage for KRX 003230.
  key_facts:
    - code=003230
    - name=삼양식품
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003230
    - 삼양식품
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

# KRX 003230 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=003230`
- `name=삼양식품`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 003230.

## RSS Item Metadata
- Title: `원주시 노사민정·삼양식품, 원·하청 산업안전 상생협력 - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-08-11T15:27:19+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE5oN01qU05RTFFaT3lXcHlZTDJqVEg4dWFoMlhWN0NNdTdreTA2bVRkRHlfeEtYcUpSeW5SSTZmNF9lRGhIaWdCazBQd19DMWRDNC1FMnZ3WDJONHfSAWBBVV95cUxQV2EwalVCZ3hGOWR4NG5XYXozTDBoWEVyNC1rUDFRcHUzRUQwLTByU2g2ZkRzVHJLWkIxZFdIQVZicnpsUTJaMnc0UXpTZURfS0VGS2JhZDFMTl84NFRDcTk?oc=5`

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
- Updated at: `2026-08-13T10:05:56+09:00`
- Company: [[KRX_003230_삼양식품]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-13.json`
- Latest observation title: `원주시 노사민정·삼양식품, 원·하청 산업안전 상생협력 - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-08-11T15:27:19+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE5oN01qU05RTFFaT3lXcHlZTDJqVEg4dWFoMlhWN0NNdTdreTA2bVRkRHlfeEtYcUpSeW5SSTZmNF9lRGhIaWdCazBQd19DMWRDNC1FMnZ3WDJONHfSAWBBVV95cUxQV2EwalVCZ3hGOWR4NG5XYXozTDBoWEVyNC1rUDFRcHUzRUQwLTByU2g2ZkRzVHJLWkIxZFdIQVZicnpsUTJaMnc0UXpTZURfS0VGS2JhZDFMTl84NFRDcTk?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
