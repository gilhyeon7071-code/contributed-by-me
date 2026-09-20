---
id: source-2026-05-27-KRX-900250-google-rss-coverage
type: source
title: KRX 900250 Google RSS Coverage Source
created: 2026-05-27
updated: 2026-05-27
status: raw
stage: 0

market: KRX
ticker: "900250"
company: 크리스탈신소재
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Local coverage report row shows news coverage for KRX 900250.
  key_facts:
    - code=900250
    - name=크리스탈신소재
    - naver_article_count=1
    - google_rss_article_count=8
    - kis_title_count=11
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 900250
    - 크리스탈신소재
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

# KRX 900250 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=900250`
- `name=크리스탈신소재`
- `naver_article_count=1`
- `google_rss_article_count=8`
- `kis_title_count=11`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 900250.

## RSS Item Metadata
- Title: `크리스탈신소재 주식병합으로 거래 정지... 5월 29일부터 신주 상장 전일까지 - 데이터투자`
- Source: `데이터투자`
- Published at: `2026-05-26T15:56:57+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE8zWFptd1JGU18yTDBwNUNjMDFWY01kV2tnSTZEMXloUUtNeC1rVlUwdmhPSmlobFZsSklhQWV2d29leUd3S0NrVU1GTFZSUVR6S1V3NTVndmk2NndyRjRHc0hvSmVHdHZIM0tkcDR1S0JHY1U?oc=5`

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
- Updated at: `2026-05-27T10:05:05+09:00`
- Company: [[KRX_900250_크리스탈신소재]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `크리스탈신소재 주식병합으로 거래 정지... 5월 29일부터 신주 상장 전일까지 - 데이터투자`
- Latest observation source: `데이터투자`
- Latest observation published_at: `2026-05-26T15:56:57+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTE8zWFptd1JGU18yTDBwNUNjMDFWY01kV2tnSTZEMXloUUtNeC1rVlUwdmhPSmlobFZsSklhQWV2d29leUd3S0NrVU1GTFZSUVR6S1V3NTVndmk2NndyRjRHc0hvSmVHdHZIM0tkcDR1S0JHY1U?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
