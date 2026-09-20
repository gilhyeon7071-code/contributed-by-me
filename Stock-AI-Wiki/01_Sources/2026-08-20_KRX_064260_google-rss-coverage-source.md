---
id: source-2026-08-20-KRX-064260-google-rss-coverage
type: source
title: KRX 064260 Google RSS Coverage Source
created: 2026-08-20
updated: 2026-08-20
status: raw
stage: 0

market: KRX
ticker: "064260"
company: 다날
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-20

analysis:
  summary: Local coverage report row shows news coverage for KRX 064260.
  key_facts:
    - code=064260
    - name=다날
    - naver_article_count=2
    - google_rss_article_count=2
    - kis_title_count=6
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 064260
    - 다날
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

# KRX 064260 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=064260`
- `name=다날`
- `naver_article_count=2`
- `google_rss_article_count=2`
- `kis_title_count=6`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 064260.

## RSS Item Metadata
- Title: `[단독] 쓴맛 본 달콤…다날, 결국 '아픈 손가락' 판다 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-08-20T11:42:02+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1relp4SFRBZUlwVmpYeElTTVFQdjd6djJRNHhnMDVHMzh0a0F3N0R6SkptWU40MVI2NmZqbC12T0FqTEtjLUVYUmpjanFHbFE?oc=5`

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
- Updated at: `2026-08-21T19:37:31+09:00`
- Company: [[KRX_064260_다날]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-20.json`
- Latest observation title: `[단독] 쓴맛 본 달콤…다날, 결국 '아픈 손가락' 판다 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-08-20T11:42:02+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE1relp4SFRBZUlwVmpYeElTTVFQdjd6djJRNHhnMDVHMzh0a0F3N0R6SkptWU40MVI2NmZqbC12T0FqTEtjLUVYUmpjanFHbFE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
