---
id: source-2026-05-25-KRX-299660-google-rss-coverage
type: source
title: KRX 299660 Google RSS Coverage Source
created: 2026-05-25
updated: 2026-05-25
status: raw
stage: 0

market: KRX
ticker: "299660"
company: 셀리드
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Local coverage report row shows news coverage for KRX 299660.
  key_facts:
    - code=299660
    - name=셀리드
    - naver_article_count=0
    - google_rss_article_count=3
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 299660
    - 셀리드
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

# KRX 299660 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=299660`
- `name=셀리드`
- `naver_article_count=0`
- `google_rss_article_count=3`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 299660.

## RSS Item Metadata
- Title: `셀리드 투자분석 2026. 05. 24 - 주달`
- Source: `주달`
- Published at: `2026-05-24T16:42:21+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE00RWU2ZkQ4TTR1YWFIVlRjMHIwU0ppdEstZjZNa3Z3OUtzSnhLUE16aG00ZzNkY2pfZWNiVWNoMVVnQXZHUlQtX2RKNHVEeFd0cmdUMmx3UW5UWDdxa0NySlhydkFYLW11aUNjaUhRSHd4aFE?oc=5`

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
- Updated at: `2026-05-25T16:05:04+09:00`
- Company: [[KRX_299660_셀리드]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `셀리드, 사외이사 이동준 신규 선임 - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-05-22T14:50:01+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBzaGxZS2xUMTFtVG5ySDdWUTZaX29ZODRGR2U4QnpnYWhKT2xjS0FGV3V1WHBqc3N0NXlUOV85OWFmcXBjeFhWcUZsX29KZk5IcEpDWUNoY3F2OVQ3ZlZEbzJFeG1lN2lWRGo1YXU4ZXV3Zmc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
