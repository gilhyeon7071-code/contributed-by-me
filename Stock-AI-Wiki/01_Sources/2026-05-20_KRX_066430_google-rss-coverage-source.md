---
id: source-2026-05-20-KRX-066430-google-rss-coverage
type: source
title: KRX 066430 Google RSS Coverage Source
created: 2026-05-20
updated: 2026-05-20
status: raw
stage: 0

market: KRX
ticker: "066430"
company: 아이로보틱스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-20

analysis:
  summary: Local coverage report row shows news coverage for KRX 066430.
  key_facts:
    - code=066430
    - name=아이로보틱스
    - naver_article_count=1
    - google_rss_article_count=7
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 066430
    - 아이로보틱스
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

# KRX 066430 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=066430`
- `name=아이로보틱스`
- `naver_article_count=1`
- `google_rss_article_count=7`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 066430.

## RSS Item Metadata
- Title: `아이로보틱스 주가, 웃음꽃... 왜? - 금강일보`
- Source: `금강일보`
- Published at: `2026-05-19T10:03:06+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE5qSGZmTlFoODBSd1RhdXFMdlpDUFY2d29uYm5DRXBBOS1UOHJMeklTa24tVU9HWUlDaTE1WnVnblZBMl9fQlJSaC1zaDdnY1pGVjJsVnllQ2Z6WHNlcGtwV0ZYTEdZWFNoemc?oc=5`

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
- Updated at: `2026-05-20T16:05:04+09:00`
- Company: [[KRX_066430_아이로보틱스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-20.json`
- Latest observation title: `아이로보틱스 주가, 웃음꽃... 왜? - 금강일보`
- Latest observation source: `금강일보`
- Latest observation published_at: `2026-05-19T10:03:06+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE5qSGZmTlFoODBSd1RhdXFMdlpDUFY2d29uYm5DRXBBOS1UOHJMeklTa24tVU9HWUlDaTE1WnVnblZBMl9fQlJSaC1zaDdnY1pGVjJsVnllQ2Z6WHNlcGtwV0ZYTEdZWFNoemc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
