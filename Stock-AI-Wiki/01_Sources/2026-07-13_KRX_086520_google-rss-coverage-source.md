---
id: source-2026-07-13-KRX-086520-google-rss-coverage
type: source
title: KRX 086520 Google RSS Coverage Source
created: 2026-07-13
updated: 2026-07-13
status: raw
stage: 0

market: KRX
ticker: "086520"
company: 에코프로
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-13

analysis:
  summary: Local coverage report row shows news coverage for KRX 086520.
  key_facts:
    - code=086520
    - name=에코프로
    - naver_article_count=2
    - google_rss_article_count=1
    - kis_title_count=16
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 086520
    - 에코프로
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

# KRX 086520 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=086520`
- `name=에코프로`
- `naver_article_count=2`
- `google_rss_article_count=1`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 086520.

## RSS Item Metadata
- Title: `에코프로비엠, 인도네시아 니켈 투자로 전기차 150만대분 확보 - BBS불교방송`
- Source: `BBS불교방송`
- Published at: `2026-07-13T07:56:37+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTE9Td1YycXV2QXhiOWFjMldMazc3c29ER0Z6cmptMy0zQ2JLQjVrRWJYUzBkM21CODV5cm5SNUdEb0NQYWsxRTlyUTlxNWk3NnVRSHhyaVZCMWVyaHdMSEZXSDZtRUtCaHVTaVNJ?oc=5`

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
- Updated at: `2026-07-13T09:05:56+09:00`
- Company: [[KRX_086520_에코프로]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-13.json`
- Latest observation title: `에코프로비엠, 인도네시아 니켈 투자로 전기차 150만대분 확보 - BBS불교방송`
- Latest observation source: `BBS불교방송`
- Latest observation published_at: `2026-07-13T07:56:37+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE9Td1YycXV2QXhiOWFjMldMazc3c29ER0Z6cmptMy0zQ2JLQjVrRWJYUzBkM21CODV5cm5SNUdEb0NQYWsxRTlyUTlxNWk3NnVRSHhyaVZCMWVyaHdMSEZXSDZtRUtCaHVTaVNJ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
