---
id: source-2026-07-05-KRX-042700-google-rss-coverage
type: source
title: KRX 042700 Google RSS Coverage Source
created: 2026-07-05
updated: 2026-07-05
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
  collected_at: 2026-07-05

analysis:
  summary: Local coverage report row shows news coverage for KRX 042700.
  key_facts:
    - code=042700
    - name=한미반도체
    - naver_article_count=1
    - google_rss_article_count=52
    - kis_title_count=16
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
- `google_rss_article_count=52`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 042700.

## RSS Item Metadata
- Title: `곽동신 한미반도체 회장, 50억 자사주 추가 매입…3년간 695억원어치 사들여 - 한국경제`
- Source: `한국경제`
- Published at: `2026-07-02T14:39:28+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE0zcXN3QlJsWmZjNXNNS0EyNEs4VEYtRUFHdTYteklZOHhwbzRaeHNpZGpCTnhfNDhPWGRrR2NvV2JXdzBVTksydm9vam1adENqWjF6STFkZWRCUdIBVEFVX3lxTE1rUWNSSlZERmdEbElWajdfU1lfVm02dzlqUEo4TVBoVGg5cG1GYWk1SVV2cENHNGhQbm0wbEl6YWtnb3FON3NpVjRaV0hzMXA0RU1FWQ?oc=5`

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
- Updated at: `2026-08-21T19:26:51+09:00`
- Company: [[KRX_042700_한미반도체]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-05.json`
- Latest observation title: `[1% 초고수의 선택] ’현대차’ 사고 ’한미반도체’ 팔았다 By EBN - Investing.com 한국어`
- Latest observation source: `Investing.com 한국어`
- Latest observation published_at: `2026-07-02T21:18:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTE1GRTNjbDY3NVlDaDR3NGlySDVta3l2elBTWVZLeGxkRHlEUnAwYWxvN0RWQmtUM2VZUGNsbzdRV3FiWjNjekN1WmpyNEI0SG9wZmhnbjI5Y2ZpNVhPcVpCNmFiNmlialllMENyNGI1XzI?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
