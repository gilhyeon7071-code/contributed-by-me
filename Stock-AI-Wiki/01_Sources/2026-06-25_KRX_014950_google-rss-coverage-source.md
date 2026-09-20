---
id: source-2026-06-25-KRX-014950-google-rss-coverage
type: source
title: KRX 014950 Google RSS Coverage Source
created: 2026-06-25
updated: 2026-06-25
status: raw
stage: 0

market: KRX
ticker: "014950"
company: 삼익제약
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-25

analysis:
  summary: Local coverage report row shows news coverage for KRX 014950.
  key_facts:
    - code=014950
    - name=삼익제약
    - naver_article_count=2
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 014950
    - 삼익제약
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

# KRX 014950 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=014950`
- `name=삼익제약`
- `naver_article_count=2`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 014950.

## RSS Item Metadata
- Title: `삼익제약, 2030년 1300억 목표…ETC·CMO·물류 확장 - 메디파나뉴스`
- Source: `메디파나뉴스`
- Published at: `2026-06-24T11:22:20+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTE5RajYyaFZoeWZlWWpyakZKcExTcDZ2dXJIMHY4R1NRaVNBazgxeDk0aEdJMnc5VWFXZzByVlZiUVphUy1zTXhnV0pxSVlIY1lGWE5ldGludUk1UDVnclZDX0hvM1QwVEtURFl3?oc=5`

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
- Updated at: `2026-06-25T08:05:11+09:00`
- Company: [[KRX_014950_삼익제약]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-25.json`
- Latest observation title: `삼익제약, 2030년 1300억 목표…ETC·CMO·물류 확장 - 메디파나뉴스`
- Latest observation source: `메디파나뉴스`
- Latest observation published_at: `2026-06-24T11:22:20+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE5RajYyaFZoeWZlWWpyakZKcExTcDZ2dXJIMHY4R1NRaVNBazgxeDk0aEdJMnc5VWFXZzByVlZiUVphUy1zTXhnV0pxSVlIY1lGWE5ldGludUk1UDVnclZDX0hvM1QwVEtURFl3?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
