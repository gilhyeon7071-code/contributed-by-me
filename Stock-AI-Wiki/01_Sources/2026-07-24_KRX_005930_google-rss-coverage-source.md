---
id: source-2026-07-24-KRX-005930-google-rss-coverage
type: source
title: KRX 005930 Google RSS Coverage Source
created: 2026-07-24
updated: 2026-07-24
status: raw
stage: 0

market: KRX
ticker: "005930"
company: 삼성전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-24

analysis:
  summary: Local coverage report row shows news coverage for KRX 005930.
  key_facts:
    - code=005930
    - name=삼성전자
    - naver_article_count=5
    - google_rss_article_count=121
    - kis_title_count=40
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005930
    - 삼성전자
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

# KRX 005930 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=005930`
- `name=삼성전자`
- `naver_article_count=5`
- `google_rss_article_count=121`
- `kis_title_count=40`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 005930.

## RSS Item Metadata
- Title: `삼성전자, 4:3 화면비 '갤럭시Z8폴드' 공개... “폴더블 이용자 맞춤 공략” - 전자신문`
- Source: `전자신문`
- Published at: `2026-07-22T22:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiTkFVX3lxTE9xcGxsX2ZTQzFFbW90akRaMW5LVmJ4MHZQX3k2MzBXQTJHMkJFTU9vdnlzV3kwcTdXZ3R6M1pvLWJWTzBMNG01V2M5b09EUQ?oc=5`

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
- Updated at: `2026-08-21T19:31:16+09:00`
- Company: [[KRX_005930_삼성전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-24.json`
- Latest observation title: `삼성전자 소액주주 "37조 자사주 매입 후 美 ADR 상장해야" - 더팩트`
- Latest observation source: `더팩트`
- Latest observation published_at: `2026-07-24T17:22:33+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE45SUlqZHdiU1VOcFFuRXd0NHNmMDJFTU5rekFITmVqaC1CMW1GVFpzbmRjcWZOODFWYUpYYWNOY1R6dDJBdWlQRzQ0WDVPU2tIblpiVGYzQmVYd9IBVEFVX3lxTFBTYTQ0VmtOZlFyVkMwa0prYUhYVHJlclJhNGR4R2NiQnlhd0huZHBjX1VlcjJFMzRZVUMxLXFaZ09yNzZuekNGeTFrNG1WVkNZNlhwYg?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
