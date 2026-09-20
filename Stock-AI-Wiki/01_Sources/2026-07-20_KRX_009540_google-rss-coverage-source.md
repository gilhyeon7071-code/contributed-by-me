---
id: source-2026-07-20-KRX-009540-google-rss-coverage
type: source
title: KRX 009540 Google RSS Coverage Source
created: 2026-07-20
updated: 2026-07-20
status: raw
stage: 0

market: KRX
ticker: "009540"
company: HD한국조선해양
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-20

analysis:
  summary: Local coverage report row shows news coverage for KRX 009540.
  key_facts:
    - code=009540
    - name=HD한국조선해양
    - naver_article_count=1
    - google_rss_article_count=14
    - kis_title_count=6
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 009540
    - HD한국조선해양
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

# KRX 009540 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=009540`
- `name=HD한국조선해양`
- `naver_article_count=1`
- `google_rss_article_count=14`
- `kis_title_count=6`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 009540.

## RSS Item Metadata
- Title: `네이버클라우드, HD한국조선해양과 협업…조선 특화 AI·클라우드 구축 - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-07-20T09:56:26+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZ0FVX3lxTFBGTWtJWDl6M2ZpNkExTjdfS1NFVkRFR2tzenpRbVhRRDZlUDF6SEN5eGJKMmVXMXpBWktBXzNYeGFCdThMNFpDRzJCNzkwMmZfN3hrTk5rOERPbkFrVnBST0Z3cHpydFnSAWxBVV95cUxQMWJqVFl1MDZ1cUhKX1h2cDdkS2JYZ0YyZDFNa0NEMkUtMTNRTkFTRVNKejhZN0xRUmk4SG5LdFA2TWVzVmRoVzlEbEVod292TXRzN1JwR0N4eExRcUNmeGRfT1N4UFAxZHgtMVA?oc=5`

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
- Updated at: `2026-07-20T21:05:06+09:00`
- Company: [[KRX_009540_HD한국조선해양]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-20.json`
- Latest observation title: `HD한국조선해양, 네이버클라우드와 조선업 특화 AI 생태계 구축 나서 - 비즈니스포스트`
- Latest observation source: `비즈니스포스트`
- Latest observation published_at: `2026-07-20T09:32:57+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE00MHlpNzdRZnJGMURLWXQ2Yi1Cc0NzRmNaZnFNY2VMX2JrSGQ5LU54WlNTQzVVMEJsTktJTVVWUzFjM3JTNzFvQ3E2MEVaUUtjRFhOZTlMYXVwaDh0eGZNRXBlTnZWMUU4Mm0zVmp1eGozcEE?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
