---
id: source-2026-07-30-KRX-066570-google-rss-coverage
type: source
title: KRX 066570 Google RSS Coverage Source
created: 2026-07-30
updated: 2026-07-30
status: raw
stage: 0

market: KRX
ticker: "066570"
company: LG전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-30

analysis:
  summary: Local coverage report row shows news coverage for KRX 066570.
  key_facts:
    - code=066570
    - name=LG전자
    - naver_article_count=1
    - google_rss_article_count=27
    - kis_title_count=9
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 066570
    - LG전자
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

# KRX 066570 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=066570`
- `name=LG전자`
- `naver_article_count=1`
- `google_rss_article_count=27`
- `kis_title_count=9`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 066570.

## RSS Item Metadata
- Title: `"엔비디아 인증" LG전자 일냈다…젠슨 황 찜한 기술 뭐길래 - 한국경제`
- Source: `한국경제`
- Published at: `2026-07-27T10:00:16+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTFBKOURtYUpGdW0wMXhUdFpZTHBaOEVsd2NHeWpLLWd4dTdkdFhlY2hKRHdESjVfSElFc0xmZDNNZ1I2Qk40blhFQ0tGTVhpbUNPQ0k3bWttQ1IyUQ?oc=5`

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
- Updated at: `2026-08-21T19:33:12+09:00`
- Company: [[KRX_066570_LG전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-30.json`
- Latest observation title: `LG전자, 역대 2분기 최대 실적... ‘가전·전장’ 실적 견인 - 조선일보`
- Latest observation source: `조선일보`
- Latest observation published_at: `2026-07-30T16:20:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxOMkhvaFg1SnZWV1poc2U0NWNndVU1WXh1ZGRlZ2lKUEFFcmxGWWNOSXJpVHhHcEdmSzdWTEpkeHpWek5UTFREWU1FSzFrd3FVazBuaG9LbjdWT3IyR3B5RE1RcGZPTUR1VEtkNWFsQkxGMVBYRTBrb3pJMXZJS1l1endQYw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
