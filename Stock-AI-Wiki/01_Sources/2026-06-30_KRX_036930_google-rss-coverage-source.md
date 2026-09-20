---
id: source-2026-06-30-KRX-036930-google-rss-coverage
type: source
title: KRX 036930 Google RSS Coverage Source
created: 2026-06-30
updated: 2026-06-30
status: raw
stage: 0

market: KRX
ticker: "036930"
company: 주성엔지니어링
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-30

analysis:
  summary: Local coverage report row shows news coverage for KRX 036930.
  key_facts:
    - code=036930
    - name=주성엔지니어링
    - naver_article_count=1
    - google_rss_article_count=6
    - kis_title_count=12
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 036930
    - 주성엔지니어링
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

# KRX 036930 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=036930`
- `name=주성엔지니어링`
- `naver_article_count=1`
- `google_rss_article_count=6`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 036930.

## RSS Item Metadata
- Title: `[DQN] 주성엔지니어링, 수주 75% 줄었는데 PBR은 14배 - 한국금융신문`
- Source: `한국금융신문`
- Published at: `2026-06-25T05:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMifEFVX3lxTE5IcHI0MTh5RjNBS2cya2dKVGVGUlhxMlZMS1hlZTdUU0VtcTNycHRWLWF1dFJWX2VyNjZZRUZybVVnSktUY0JENmZaMzRGd3Q4M3pFeUNJdWZ6ZGtFakV3WEVpSEFHS3VrWjRXYUZheWE4dnQ5NWJ5ZkFUcEY?oc=5`

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
- Updated at: `2026-06-30T03:05:26+09:00`
- Company: [[KRX_036930_주성엔지니어링]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-30.json`
- Latest observation title: `[DQN] 주성엔지니어링, 수주 75% 줄었는데 PBR은 14배 - 한국금융신문`
- Latest observation source: `한국금융신문`
- Latest observation published_at: `2026-06-25T05:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMifEFVX3lxTE5IcHI0MTh5RjNBS2cya2dKVGVGUlhxMlZMS1hlZTdUU0VtcTNycHRWLWF1dFJWX2VyNjZZRUZybVVnSktUY0JENmZaMzRGd3Q4M3pFeUNJdWZ6ZGtFakV3WEVpSEFHS3VrWjRXYUZheWE4dnQ5NWJ5ZkFUcEY?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
