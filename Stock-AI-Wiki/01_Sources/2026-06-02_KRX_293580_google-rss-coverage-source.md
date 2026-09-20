---
id: source-2026-06-02-KRX-293580-google-rss-coverage
type: source
title: KRX 293580 Google RSS Coverage Source
created: 2026-06-02
updated: 2026-06-02
status: raw
stage: 0

market: KRX
ticker: "293580"
company: 나우IB
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-02

analysis:
  summary: Local coverage report row shows news coverage for KRX 293580.
  key_facts:
    - code=293580
    - name=나우IB
    - naver_article_count=4
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 293580
    - 나우IB
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

# KRX 293580 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=293580`
- `name=나우IB`
- `naver_article_count=4`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 293580.

## RSS Item Metadata
- Title: `나우IB, 관리보수 기반 체질 강화…평가손실에 수익성은 뒷걸음 - 톱데일리`
- Source: `톱데일리`
- Published at: `2026-06-01T15:18:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUEFVX3lxTE5hUE9CLXFldUdxOG1IclFoM1Q2dkRWSTNtNVBxcVdDd2tIWU1XRFlzMTNRME1pOWR1bXN4aENvc3ZMQVduUmVfalBZTlA4b0tx?oc=5`

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
- Updated at: `2026-08-21T19:17:26+09:00`
- Company: [[KRX_293580_나우IB]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-02.json`
- Latest observation title: `나우IB, 관리보수 기반 체질 강화…평가손실에 수익성은 뒷걸음 - 톱데일리`
- Latest observation source: `톱데일리`
- Latest observation published_at: `2026-06-01T15:18:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUEFVX3lxTE5hUE9CLXFldUdxOG1IclFoM1Q2dkRWSTNtNVBxcVdDd2tIWU1XRFlzMTNRME1pOWR1bXN4aENvc3ZMQVduUmVfalBZTlA4b0tx?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
