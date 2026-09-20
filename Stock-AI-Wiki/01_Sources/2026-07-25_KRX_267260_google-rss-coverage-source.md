---
id: source-2026-07-25-KRX-267260-google-rss-coverage
type: source
title: KRX 267260 Google RSS Coverage Source
created: 2026-07-25
updated: 2026-07-25
status: raw
stage: 0

market: KRX
ticker: "267260"
company: HD현대일렉트릭
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-25

analysis:
  summary: Local coverage report row shows news coverage for KRX 267260.
  key_facts:
    - code=267260
    - name=HD현대일렉트릭
    - naver_article_count=1
    - google_rss_article_count=4
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 267260
    - HD현대일렉트릭
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

# KRX 267260 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=267260`
- `name=HD현대일렉트릭`
- `naver_article_count=1`
- `google_rss_article_count=4`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 267260.

## RSS Item Metadata
- Title: `HD현대일렉트릭, 북미서 첫 친환경 GIS 수주 따냈다…美 사업 가속 - dt.co.kr`
- Source: `dt.co.kr`
- Published at: `2026-07-24T06:00:04+09:00`
- Link: `https://news.google.com/rss/articles/CBMiTkFVX3lxTE5mOEtZc3diQk93VExYek4yaHlvQVZkVmNXc1kzZkg4RDZGS05RUFNES2lBcktEVXVVenpMLVJjLVEwY3BhZzJRWVgtNFNqdw?oc=5`

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
- Updated at: `2026-08-21T19:31:34+09:00`
- Company: [[KRX_267260_HD현대일렉트릭]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-25.json`
- Latest observation title: `조정끝났나…LS일렉트릭·HD현대일렉트릭·효성중공업 동반 급등 - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-07-23T15:43:31+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE1DcjBFRWhScUFybEhDMWQ3RUtvX1BCZWt1RHlyNXVvZjZHbUt3NzlWV3ByTVh0SFFSTzN2ZDFScnBpZDd6QjFjbHpMak5aTUFEZDRLRDdCTU0tZw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
