---
id: source-2026-08-14-KRX-024840-google-rss-coverage
type: source
title: KRX 024840 Google RSS Coverage Source
created: 2026-08-14
updated: 2026-08-14
status: raw
stage: 0

market: KRX
ticker: "024840"
company: KBI메탈
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-14

analysis:
  summary: Local coverage report row shows news coverage for KRX 024840.
  key_facts:
    - code=024840
    - name=KBI메탈
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 024840
    - KBI메탈
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

# KRX 024840 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=024840`
- `name=KBI메탈`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 024840.

## RSS Item Metadata
- Title: `KBI메탈, 상반기 영업익 283억ⵈ 상장 이래 최대 실적 - 국토일보`
- Source: `국토일보`
- Published at: `2026-08-13T20:10:48+09:00`
- Link: `https://news.google.com/rss/articles/CBMiY0FVX3lxTE5ENTBzVGt3YWFqNlUyN3RVWXc1akJYX2dBN01iMEtRQWVUems2Q0t1U3dCMFF3c09aNVluUS16UTdVaG9uS2NUazBUYU9VOTRuMGtKanV1RXNTczBJQW0teE80bw?oc=5`

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
- Updated at: `2026-08-21T19:36:10+09:00`
- Company: [[KRX_024840_KBI메탈]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-14.json`
- Latest observation title: `KBI메탈, 상반기 역대 최대 실적…영업익 813% 급증 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-08-13T15:42:41+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5SR0dMQUQ5YXpwNFdKRFhwVHVTNGg1cDV2dW9USVplQXhVcUp6RmdGbXluSF9hLU03dEJtazhWNHdBNktqZXZYNW0yLUJQZ1U?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
