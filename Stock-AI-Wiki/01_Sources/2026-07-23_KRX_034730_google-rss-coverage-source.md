---
id: source-2026-07-23-KRX-034730-google-rss-coverage
type: source
title: KRX 034730 Google RSS Coverage Source
created: 2026-07-23
updated: 2026-07-23
status: raw
stage: 0

market: KRX
ticker: "034730"
company: SK
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-23

analysis:
  summary: Local coverage report row shows news coverage for KRX 034730.
  key_facts:
    - code=034730
    - name=SK
    - naver_article_count=14
    - google_rss_article_count=208
    - kis_title_count=43
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034730
    - SK
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

# KRX 034730 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=034730`
- `name=SK`
- `naver_article_count=14`
- `google_rss_article_count=208`
- `kis_title_count=43`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 034730.

## RSS Item Metadata
- Title: `7억 기대했는데 “터무니없는 수준”…SK하닉 성과급 1년 만에 재협상 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-07-22T11:24:50+09:00`
- Link: `https://news.google.com/rss/articles/CBMiRkFVX3lxTE9rSFJkRGduOVREbEl2NjB0MFlBcTRMcWZfQmNJM1ZrV04wVHZSdk5NU2xZOFdPWWl3NWZ2aE5zaGxZbmFtVkE?oc=5`

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
- Updated at: `2026-08-21T19:30:57+09:00`
- Company: [[KRX_034730_SK]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-23.json`
- Latest observation title: `7억 기대했는데 “터무니없는 수준”…SK하닉 성과급 1년 만에 재협상 - 중앙일보`
- Latest observation source: `중앙일보`
- Latest observation published_at: `2026-07-22T11:22:38+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTFB6elhTR1hLOEVjTVloX1lZYXZWMV9Cd2psXzkyTHF3YU9tN2RDNlRVMExMOU1YenlHY0FRVFN5MVpVNmtHSVNIdVE4VFZNMWMxUl96QVBR?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
