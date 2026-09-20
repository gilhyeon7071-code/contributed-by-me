---
id: source-2026-06-30-KRX-018260-google-rss-coverage
type: source
title: KRX 018260 Google RSS Coverage Source
created: 2026-06-30
updated: 2026-06-30
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
  collected_at: 2026-06-30

analysis:
  summary: Local coverage report row shows news coverage for KRX 018260.
  key_facts:
    - code=018260
    - name=삼성에스디에스
    - naver_article_count=1
    - google_rss_article_count=8
    - kis_title_count=10
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
- `naver_article_count=1`
- `google_rss_article_count=8`
- `kis_title_count=10`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 018260.

## RSS Item Metadata
- Title: `이태희 부사장, 삼성에스디에스 주식 500주 매수 - 디지털투데이`
- Source: `디지털투데이`
- Published at: `2026-06-29T16:11:01+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBWVHNvbkZ0WF9lQ05IYU5OS2FobDNPYUk4Wm1NOEdNTWltN05QYk9qb1ZEY3ZpYnJEMHZsR2JmMk9oSjJfdGV1cFNHbGM5dXNPRkUwOHJMYXhqSi1WbzBFa1hJNXJMM0pMaTRFdHUtR2xPOE0?oc=5`

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
- Updated at: `2026-08-21T19:25:36+09:00`
- Company: [[KRX_018260_삼성에스디에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-30.json`
- Latest observation title: `이태희 부사장, 삼성에스디에스 주식 500주 매수 - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-06-29T16:11:01+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBWVHNvbkZ0WF9lQ05IYU5OS2FobDNPYUk4Wm1NOEdNTWltN05QYk9qb1ZEY3ZpYnJEMHZsR2JmMk9oSjJfdGV1cFNHbGM5dXNPRkUwOHJMYXhqSi1WbzBFa1hJNXJMM0pMaTRFdHUtR2xPOE0?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
