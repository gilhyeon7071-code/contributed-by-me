---
id: source-2026-07-07-KRX-018260-google-rss-coverage
type: source
title: KRX 018260 Google RSS Coverage Source
created: 2026-07-07
updated: 2026-07-07
status: raw
stage: 0

market: KRX
ticker: "018260"
company: 삼성에스디에스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-07

analysis:
  summary: Local coverage report row shows news coverage for KRX 018260.
  key_facts:
    - code=018260
    - name=삼성에스디에스
    - naver_article_count=4
    - google_rss_article_count=2
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 018260
    - 삼성에스디에스
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

# KRX 018260 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=018260`
- `name=삼성에스디에스`
- `naver_article_count=4`
- `google_rss_article_count=2`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 018260.

## RSS Item Metadata
- Title: `삼성SDS 노조, '5833명 과반노조' 달성…"성과급 투표 중지해야" - 뉴스1`
- Source: `뉴스1`
- Published at: `2026-07-07T21:41:45+09:00`
- Link: `https://news.google.com/rss/articles/CBMiX0FVX3lxTE11VkpsWFp6eUNCeUZ5TFd3VERrYlQxbHNweFRTS2w5X01lMW5FUU5vSmRrdk53bkdpUGY0SERuSmVqSE9SSXhpZkxXZ0Z2S1l5bC1KYTdQRUl1R0dOREtN?oc=5`

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
- Updated at: `2026-08-21T19:27:28+09:00`
- Company: [[KRX_018260_삼성에스디에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-07.json`
- Latest observation title: `삼성SDS 노조, '5833명 과반노조' 달성…"성과급 투표 중지해야" - 뉴스1`
- Latest observation source: `뉴스1`
- Latest observation published_at: `2026-07-07T21:41:45+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiX0FVX3lxTE11VkpsWFp6eUNCeUZ5TFd3VERrYlQxbHNweFRTS2w5X01lMW5FUU5vSmRrdk53bkdpUGY0SERuSmVqSE9SSXhpZkxXZ0Z2S1l5bC1KYTdQRUl1R0dOREtN?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
