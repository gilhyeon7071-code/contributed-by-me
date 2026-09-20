---
id: source-2026-05-29-KRX-058650-google-rss-coverage
type: source
title: KRX 058650 Google RSS Coverage Source
created: 2026-05-29
updated: 2026-05-29
status: raw
stage: 0

market: KRX
ticker: "058650"
company: 세아홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-29

analysis:
  summary: Local coverage report row shows news coverage for KRX 058650.
  key_facts:
    - code=058650
    - name=세아홀딩스
    - naver_article_count=0
    - google_rss_article_count=3
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 058650
    - 세아홀딩스
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

# KRX 058650 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=058650`
- `name=세아홀딩스`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 058650.

## RSS Item Metadata
- Title: `세아홀딩스, 통상임금·CAPEX 이중 부담…차입금 2.2조 육박 - DealSite경제TV`
- Source: `DealSite경제TV`
- Published at: `2026-05-29T08:00:26+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE52NlZGcW5jNzJreWN4UHVxbk9xUG5VTEZpWC1QTlU2SUxmRXZBZVMtYWZSbkpzbnlocGZpbU9FRWZxUFZUYnVvdUFYVHBsUFhOcVRPYTJB?oc=5`

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
- Company: [[KRX_058650_세아홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-29.json`
- Latest observation title: `캐나다 PE, 美 '어센드' 자산 1억불 인수…채권단에 세아홀딩스 투자 자회사 포함 - 아시아경제`
- Latest observation source: `아시아경제`
- Latest observation published_at: `2026-05-29T07:05:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE9ybTBHTzdtSTdWVTZFeEVKNGlZUmRQU1VHaG9zcVh3MHNIZFVSOWFROUFHU2ZxdEdTdmYyQ3dxb2Jwck5NQnk2OTQ4TjlreVlsbm5yQ2tYbGNrS2cwWVo2QQ?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
