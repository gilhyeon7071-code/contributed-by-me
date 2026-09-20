---
id: source-2026-07-27-KRX-196170-google-rss-coverage
type: source
title: KRX 196170 Google RSS Coverage Source
created: 2026-07-27
updated: 2026-07-27
status: raw
stage: 0

market: KRX
ticker: "196170"
company: 알테오젠
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-27

analysis:
  summary: Local coverage report row shows news coverage for KRX 196170.
  key_facts:
    - code=196170
    - name=알테오젠
    - naver_article_count=1
    - google_rss_article_count=7
    - kis_title_count=16
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 196170
    - 알테오젠
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

# KRX 196170 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=196170`
- `name=알테오젠`
- `naver_article_count=1`
- `google_rss_article_count=7`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 196170.

## RSS Item Metadata
- Title: `바이오 ETF Top3 편입종목, 알테오젠-셀트리온-올릭스 순 - 메디파나뉴스`
- Source: `메디파나뉴스`
- Published at: `2026-07-23T12:15:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTE1pOTNxWFo0c2QwU25aNnNUbWJXSTUyT2t5WC1uSG5hQ2Jpd3RNVGJTMUNEaGl1bGFIVF9IcWRsUnVRMVZHUDZsOXQ1ZlhXRDZvTUdOR2VfZEljbkRaRFJUSThHZTkwcEl5SXQw?oc=5`

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
- Updated at: `2026-08-21T19:32:13+09:00`
- Company: [[KRX_196170_알테오젠]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-27.json`
- Latest observation title: `'소송'보다 '2043년'…빅파마가 주목하는 알테오젠 ALT-B4 - 바이오타임즈`
- Latest observation source: `바이오타임즈`
- Latest observation published_at: `2026-07-27T13:23:22+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTE5NMk9pYWhjSEhKdkdGTVhiemVUNGRaZDI1TTNGWlY5U2xvWG5oc2FkWVpzUENCTGlHRzdEelRndDZJY2o4NWtGX2pLYk9yNmRSeF9VNmhZWXphT3Z6cS1Dc2xyLU1tNnNacnNoQg?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
