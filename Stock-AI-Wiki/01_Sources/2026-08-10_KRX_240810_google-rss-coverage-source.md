---
id: source-2026-08-10-KRX-240810-google-rss-coverage
type: source
title: KRX 240810 Google RSS Coverage Source
created: 2026-08-10
updated: 2026-08-10
status: raw
stage: 0

market: KRX
ticker: "240810"
company: 원익IPS
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-10

analysis:
  summary: Local coverage report row shows news coverage for KRX 240810.
  key_facts:
    - code=240810
    - name=원익IPS
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=19
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 240810
    - 원익IPS
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

# KRX 240810 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=240810`
- `name=원익IPS`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=19`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 240810.

## RSS Item Metadata
- Title: `코스닥 800선 다시 내줘…레인보우로보틱스·원익IPS 약세, HLB·알테오젠·펩트론은 강세 - CBC뉴스`
- Source: `CBC뉴스`
- Published at: `2026-08-09T16:31:05+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE8zOTZUVWFZbDBqOHVXZDQ3bEVBSjh5UzhBN1pyYUY4aGVaMXVyTzZJZnJ2bEpNM3NNWUJmU1hmclVNQ2NZbm9VUlpDWGhkeTMwS1NJVnhCTExsX2pwODNSVUJhRF9wZjBS?oc=5`

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
- Updated at: `2026-08-10T08:05:20+09:00`
- Company: [[KRX_240810_원익IPS]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-10.json`
- Latest observation title: `SK증권 "원익IPS, 수주잔고 증가로 하반기 실적 개선 기대" - 연합뉴스`
- Latest observation source: `연합뉴스`
- Latest observation published_at: `2026-08-07T08:40:06+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiW0FVX3lxTFBXYU9paEpoRkVTLTVHUGlhVnVERkV1TElCS05Rc2JwNkdmWU9KYzN0NnJNcHo5RmdiYndLQzVNc1c2WmlDdDg2N0FGdTRLZEE4MzNsM25Za05pS0XSAWBBVV95cUxObmc5YVA3QkhndUZscFM3VlpXbF9LMkZGY3BwclVKUHg2YTJYMThnZ2d6YllHRGs0X2xmdFZNOVVvMkpIdTE5OUpuMk1Zb3R5aVp0OTI1REZDazlGY0pEbFA?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
