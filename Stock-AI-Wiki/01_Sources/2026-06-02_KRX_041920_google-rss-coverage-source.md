---
id: source-2026-06-02-KRX-041920-google-rss-coverage
type: source
title: KRX 041920 Google RSS Coverage Source
created: 2026-06-02
updated: 2026-06-02
status: raw
stage: 0

market: KRX
ticker: "041920"
company: 메디아나
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-02

analysis:
  summary: Local coverage report row shows news coverage for KRX 041920.
  key_facts:
    - code=041920
    - name=메디아나
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 041920
    - 메디아나
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

# KRX 041920 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=041920`
- `name=메디아나`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 041920.

## RSS Item Metadata
- Title: `메디아나, 퓨리오사AI와 의료AI 플랫폼 구축 - 팜이데일리`
- Source: `팜이데일리`
- Published at: `2026-06-01T08:31:36+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTFB5VXNmWm9feFlfeFZVMEk2U2RJOEthdjZSUVNLSWpQLWZCdW1tNzVJdHJHNGR2YWlESnk4X0F1ZVBMZzlHSzdmWjBkVEhzcUNhZUd3dTNjenMyVGY1eTZHN2ctS3cxOTItdWZNZUpn?oc=5`

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
- Updated at: `2026-08-21T19:17:26+09:00`
- Company: [[KRX_041920_메디아나]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-02.json`
- Latest observation title: `"메디아나, 의료 AI 플랫폼 진화 가능성 높아" - 리딩투자 - 뉴스핌`
- Latest observation source: `뉴스핌`
- Latest observation published_at: `2026-06-02T10:02:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE1xUUFVUjZZY2pURW14WkxpRC00M1B5NUtZazhfdkVWalY2NjZ4UmxrZjdmMThNaEd1dXVUS1Vnd2s1WGFGcXlhNzRZOHlJVlV2V0MzTWJGc2tpNENG?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_medical-cooperation_의료-협진]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
