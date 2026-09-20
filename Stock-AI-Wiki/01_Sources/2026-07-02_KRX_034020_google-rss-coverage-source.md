---
id: source-2026-07-02-KRX-034020-google-rss-coverage
type: source
title: KRX 034020 Google RSS Coverage Source
created: 2026-07-02
updated: 2026-07-02
status: raw
stage: 0

market: KRX
ticker: "034020"
company: 두산에너빌리티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-02

analysis:
  summary: Local coverage report row shows news coverage for KRX 034020.
  key_facts:
    - code=034020
    - name=두산에너빌리티
    - naver_article_count=2
    - google_rss_article_count=20
    - kis_title_count=13
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034020
    - 두산에너빌리티
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

# KRX 034020 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=034020`
- `name=두산에너빌리티`
- `naver_article_count=2`
- `google_rss_article_count=20`
- `kis_title_count=13`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 034020.

## RSS Item Metadata
- Title: `두산에너빌리티, 경제부총리 표창 수상 - 뉴스1`
- Source: `뉴스1`
- Published at: `2026-07-01T14:52:35+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE9RZ2pIQkZkUjdCZTdhWVlreWM4VDIzQnRxLUYtYWNNaGMyLVFQYVZ1cWlJN0hTdDFxSnF3YTNhYjdOLUU1eGRUWW5MR1JIUkhJaGVLNWtwQmdkcHZXckpfRWZMRnE?oc=5`

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
- Updated at: `2026-07-02T09:06:42+09:00`
- Company: [[KRX_034020_두산에너빌리티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-02.json`
- Latest observation title: `12차 전기본 막바지…추가 원전 기대에 두산에너빌리티 '주목' - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-07-02T07:12:31+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5VRkJvdm5haXg1SUpQaHp1VFRxdUdHcG5idEVYZVlwbGRjN1pPYWR2aVpjQUlMaGNHT1BOTW5Mc01fcFJSM2ZtRUhfNG4tYnc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
