---
id: source-2026-05-25-KRX-078350-google-rss-coverage
type: source
title: KRX 078350 Google RSS Coverage Source
created: 2026-05-25
updated: 2026-05-25
status: raw
stage: 0

market: KRX
ticker: "078350"
company: 한양디지텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Local coverage report row shows news coverage for KRX 078350.
  key_facts:
    - code=078350
    - name=한양디지텍
    - naver_article_count=0
    - google_rss_article_count=3
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 078350
    - 한양디지텍
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

# KRX 078350 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=078350`
- `name=한양디지텍`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 078350.

## RSS Item Metadata
- Title: `한양디지텍, 투자경고종목 지정 예고→투자자 주의 당부 - 톱스타뉴스`
- Source: `톱스타뉴스`
- Published at: `2026-05-24T11:47:24+09:00`
- Link: `https://news.google.com/rss/articles/CBMickFVX3lxTFBrM2d2SWVmTzVvTVRXejVKWE9jY3pPRk1RTjB6WFhFNUlTWDFDc010cVBlbjNVeTNCR1hiQWJGY2dONXJoVDloYThTaUEzcTdjQml1VnJ0T2RpSmF4cWVUZTQzSDZJWUhNVThRUG1CMXFQUQ?oc=5`

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
- Updated at: `2026-08-21T19:15:10+09:00`
- Company: [[KRX_078350_한양디지텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `한양디지텍, 투자경고종목 지정 예고→투자자 주의 당부 - 톱스타뉴스`
- Latest observation source: `톱스타뉴스`
- Latest observation published_at: `2026-05-24T11:47:24+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTFBrM2d2SWVmTzVvTVRXejVKWE9jY3pPRk1RTjB6WFhFNUlTWDFDc010cVBlbjNVeTNCR1hiQWJGY2dONXJoVDloYThTaUEzcTdjQml1VnJ0T2RpSmF4cWVUZTQzSDZJWUhNVThRUG1CMXFQUQ?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
