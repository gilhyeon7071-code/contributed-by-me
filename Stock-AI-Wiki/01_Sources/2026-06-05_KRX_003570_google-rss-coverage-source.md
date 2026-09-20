---
id: source-2026-06-05-KRX-003570-google-rss-coverage
type: source
title: KRX 003570 Google RSS Coverage Source
created: 2026-06-05
updated: 2026-06-05
status: raw
stage: 0

market: KRX
ticker: "003570"
company: SNT다이내믹스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-05

analysis:
  summary: Local coverage report row shows news coverage for KRX 003570.
  key_facts:
    - code=003570
    - name=SNT다이내믹스
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 003570
    - SNT다이내믹스
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

# KRX 003570 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=003570`
- `name=SNT다이내믹스`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 003570.

## RSS Item Metadata
- Title: `SNT다이내믹스 투자분석 2026. 06. 04 - 주달`
- Source: `주달`
- Published at: `2026-06-04T17:35:32+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE92RWxXbjNJVW5OdDRNbHhXWGYzYXVieEZyMFR4QmtobFlrbTJKcl91R1BlMUsydGp0QVRiOXlKRVlHUTNHcXJxVlFzQUlKRnZtcUFEQ2pqUFRTRlVUU2R6Q3JuUVl0cTlRTXA2XzgyLTdON1k?oc=5`

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
- Updated at: `2026-06-05T08:11:49+09:00`
- Company: [[KRX_003570_SNT다이내믹스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `SNT다이내믹스 투자분석 2026. 06. 04 - 주달`
- Latest observation source: `주달`
- Latest observation published_at: `2026-06-04T17:35:32+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE92RWxXbjNJVW5OdDRNbHhXWGYzYXVieEZyMFR4QmtobFlrbTJKcl91R1BlMUsydGp0QVRiOXlKRVlHUTNHcXJxVlFzQUlKRnZtcUFEQ2pqUFRTRlVUU2R6Q3JuUVl0cTlRTXA2XzgyLTdON1k?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
