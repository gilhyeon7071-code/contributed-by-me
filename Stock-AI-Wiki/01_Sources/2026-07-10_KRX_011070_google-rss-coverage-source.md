---
id: source-2026-07-10-KRX-011070-google-rss-coverage
type: source
title: KRX 011070 Google RSS Coverage Source
created: 2026-07-10
updated: 2026-07-10
status: raw
stage: 0

market: KRX
ticker: "011070"
company: LG이노텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-10

analysis:
  summary: Local coverage report row shows news coverage for KRX 011070.
  key_facts:
    - code=011070
    - name=LG이노텍
    - naver_article_count=1
    - google_rss_article_count=2
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 011070
    - LG이노텍
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

# KRX 011070 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=011070`
- `name=LG이노텍`
- `naver_article_count=1`
- `google_rss_article_count=2`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 011070.

## RSS Item Metadata
- Title: `"LG이노텍, 2분기 실적 기대치 넘어설 전망"-대신 - 한국경제`
- Source: `한국경제`
- Published at: `2026-07-09T07:31:09+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTFB2MWswTVdLX1BkQjRtYzVJVnV5YUs3WUdXTEJVWHBJTmU0dGRwa2ZCZkNWcWRPV3ctMnFtZnFlT1dIX2pITDJHWEJBaVlBUXY1cThXNkk5bTE1UdIBVEFVX3lxTE9jSmVsTjlGaXRNaC1BTzZfMHhReHVGQTA1cTNxbldXTng5TlNTZHVPdTdVNm9rNXhXeWt0TFJsNVpVQmJoZy1mOHFpX19pblZfc3lsUA?oc=5`

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
- Updated at: `2026-08-21T19:28:24+09:00`
- Company: [[KRX_011070_LG이노텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-10.json`
- Latest observation title: `"LG이노텍, 2분기 실적 기대치 넘어설 전망"-대신 - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-07-09T07:31:09+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTFB2MWswTVdLX1BkQjRtYzVJVnV5YUs3WUdXTEJVWHBJTmU0dGRwa2ZCZkNWcWRPV3ctMnFtZnFlT1dIX2pITDJHWEJBaVlBUXY1cThXNkk5bTE1UdIBVEFVX3lxTE9jSmVsTjlGaXRNaC1BTzZfMHhReHVGQTA1cTNxbldXTng5TlNTZHVPdTdVNm9rNXhXeWt0TFJsNVpVQmJoZy1mOHFpX19pblZfc3lsUA?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
