---
id: source-2026-07-28-KRX-277810-google-rss-coverage
type: source
title: KRX 277810 Google RSS Coverage Source
created: 2026-07-28
updated: 2026-07-28
status: raw
stage: 0

market: KRX
ticker: "277810"
company: 레인보우로보틱스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-28

analysis:
  summary: Local coverage report row shows news coverage for KRX 277810.
  key_facts:
    - code=277810
    - name=레인보우로보틱스
    - naver_article_count=2
    - google_rss_article_count=8
    - kis_title_count=16
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 277810
    - 레인보우로보틱스
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

# KRX 277810 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=277810`
- `name=레인보우로보틱스`
- `naver_article_count=2`
- `google_rss_article_count=8`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 277810.

## RSS Item Metadata
- Title: `투모로로보틱스, 레인보우로보틱스와 ‘AI 기반 휴머노이드’ 개발 나선다 - 로봇신문`
- Source: `로봇신문`
- Published at: `2026-07-23T14:18:05+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE9lMmcyTy1SUUNpakV2VEZIMmV5M3N5c1NDbEZUQjZuQkpzaEo4ZDYxbnBRSnR1X3oxU3pHN09oUmpjQjc1eWp4RGRxMkRheFcxcEpyeGlPMGEtNG1XeWxncUNNN082ZGF0WVRQSw?oc=5`

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
- Updated at: `2026-07-28T21:05:05+09:00`
- Company: [[KRX_277810_레인보우로보틱스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `투모로로보틱스, 레인보우로보틱스와 ‘AI 기반 휴머노이드’ 개발 나선다 - 로봇신문`
- Latest observation source: `로봇신문`
- Latest observation published_at: `2026-07-23T14:18:05+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTE9lMmcyTy1SUUNpakV2VEZIMmV5M3N5c1NDbEZUQjZuQkpzaEo4ZDYxbnBRSnR1X3oxU3pHN09oUmpjQjc1eWp4RGRxMkRheFcxcEpyeGlPMGEtNG1XeWxncUNNN082ZGF0WVRQSw?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
