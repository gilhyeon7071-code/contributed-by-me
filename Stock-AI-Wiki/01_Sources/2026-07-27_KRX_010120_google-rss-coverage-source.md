---
id: source-2026-07-27-KRX-010120-google-rss-coverage
type: source
title: KRX 010120 Google RSS Coverage Source
created: 2026-07-27
updated: 2026-07-27
status: raw
stage: 0

market: KRX
ticker: "010120"
company: 엘에스일렉트릭
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-27

analysis:
  summary: Local coverage report row shows news coverage for KRX 010120.
  key_facts:
    - code=010120
    - name=엘에스일렉트릭
    - naver_article_count=1
    - google_rss_article_count=11
    - kis_title_count=27
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 010120
    - 엘에스일렉트릭
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

# KRX 010120 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=010120`
- `name=엘에스일렉트릭`
- `naver_article_count=1`
- `google_rss_article_count=11`
- `kis_title_count=27`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 010120.

## RSS Item Metadata
- Title: `LS ELECTRIC (010120) - ThinkPool`
- Source: `ThinkPool`
- Published at: `2026-07-23T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiS0FVX3lxTE40eVhTUk9qWFpoRlNtbm41dkFSRDVXOXhXcUt2WVZpWldKenNGU2lFNm1nTWVwcHphTThyZnM0NndpT0V2czVhWGUtcw?oc=5`

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
- Updated at: `2026-07-27T21:05:13+09:00`
- Company: [[KRX_010120_엘에스일렉트릭]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-27.json`
- Latest observation title: `LS ELECTRIC, 매출 1조6000억원, 영업이익 1800억원 /연결잠정실적 - 데일리인베스트`
- Latest observation source: `데일리인베스트`
- Latest observation published_at: `2026-07-23T12:17:15+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTFBQVUY5bVpXYkpDV25pTXpzVmVEZTlUTjVXdWRlbTJjSk5BbVMtSmFIdG9IajJCVjZIT05aY3k5d01WaWpGNjFrd1Q5VnBMRUdqTXVBdWF0SzJxbWltUk8tdmE5VHUxRFlQMHJz0gFvQVVfeXFMTnktOElSTzlRX3UyNy1DeUxmRHpJMk5VdU81SXJlaUI4eVpqaGh6dnBWXzlYMXROZDQ4MUtTS0VGaW5tRGdPdnBNQ0h1UXc2ZGFhS2o5ZEgwV2VFZklVTnlsbzJhVnAzZTBSVzR2dHNv?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
