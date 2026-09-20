---
id: source-2026-05-29-KRX-117730-google-rss-coverage
type: source
title: KRX 117730 Google RSS Coverage Source
created: 2026-05-29
updated: 2026-05-29
status: raw
stage: 0

market: KRX
ticker: "117730"
company: 티로보틱스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-29

analysis:
  summary: Local coverage report row shows news coverage for KRX 117730.
  key_facts:
    - code=117730
    - name=티로보틱스
    - naver_article_count=3
    - google_rss_article_count=29
    - kis_title_count=15
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 117730
    - 티로보틱스
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

# KRX 117730 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=117730`
- `name=티로보틱스`
- `naver_article_count=3`
- `google_rss_article_count=29`
- `kis_title_count=15`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 117730.

## RSS Item Metadata
- Title: `티로보틱스, 유리기판 이송로봇 日 특허 등록 - 이데일리`
- Source: `이데일리`
- Published at: `2026-05-28T08:32:07+09:00`
- Link: `https://news.google.com/rss/articles/CBMigAFBVV95cUxPcThVaTNLeHlfdHVFMkJiQWlkS2VCMTRsaU9rcExEQjI2UThDTU1xUThXWEtuNDdUeGY4ZkV4YlJUeDd4bGNTbURPdGljWTRZS29zVldoZzlwajdKMlczUkFYREt6aGVnY0wtMVNyYVFrOTc4MU9oS01XWFVaLVpBZg?oc=5`

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
- Updated at: `2026-08-21T19:16:19+09:00`
- Company: [[KRX_117730_티로보틱스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-29.json`
- Latest observation title: `티로보틱스, 유리기판 이송로봇 日 특허 등록 - 이데일리`
- Latest observation source: `이데일리`
- Latest observation published_at: `2026-05-28T08:32:07+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigAFBVV95cUxPcThVaTNLeHlfdHVFMkJiQWlkS2VCMTRsaU9rcExEQjI2UThDTU1xUThXWEtuNDdUeGY4ZkV4YlJUeDd4bGNTbURPdGljWTRZS29zVldoZzlwajdKMlczUkFYREt6aGVnY0wtMVNyYVFrOTc4MU9oS01XWFVaLVpBZg?oc=5`
- Body status: `fetch_failed_no_body`
- Original text available: `false`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
