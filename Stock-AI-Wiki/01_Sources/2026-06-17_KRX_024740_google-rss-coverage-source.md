---
id: source-2026-06-17-KRX-024740-google-rss-coverage
type: source
title: KRX 024740 Google RSS Coverage Source
created: 2026-06-17
updated: 2026-06-17
status: raw
stage: 0

market: KRX
ticker: "024740"
company: 한일단조
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-17

analysis:
  summary: Local coverage report row shows news coverage for KRX 024740.
  key_facts:
    - code=024740
    - name=한일단조
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 024740
    - 한일단조
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

# KRX 024740 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=024740`
- `name=한일단조`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 024740.

## RSS Item Metadata
- Title: `[특징주] 한일단조, 천궁-Ⅱ 탄두 구조체 생산..중동 리스크 재확전 우려에 10% - 데이터투자`
- Source: `데이터투자`
- Published at: `2026-04-09T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE41TWp5UTk0cktNaVpkM3ExM2xkS0RrQVlHNTZwYXFWS3E3SG1CTFA4dFJqaGVERkRraGpYYlhVSmZPOW1ZSnRDLVN4VThpUTAyN0M2ZmxPaHV3Nkp0WHZ3NVA3NTBtZHJVWXl5X0hhNmpmSWs?oc=5`

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
- Updated at: `2026-06-17T08:05:04+09:00`
- Company: [[KRX_024740_한일단조]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-17.json`
- Latest observation title: `한일단조 투자분석 2026. 06. 16 - 주달`
- Latest observation source: `주달`
- Latest observation published_at: `2026-06-17T03:55:37+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE42dzVuVmJGazB6NkJvZzk0dUpnVXRLQVROd2FOZjNNT2NBdkg4SWNKczEyY3ZHTGljOUxpMUhyekRHTlBDeVlXSnBzOXlBV09FZE1ScWRLZGxEMHplY2FVQWJkN3V0YVVtWUh5eXlINEowS1E?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
