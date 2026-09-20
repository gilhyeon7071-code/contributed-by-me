---
id: source-2026-06-11-KRX-051900-google-rss-coverage
type: source
title: KRX 051900 Google RSS Coverage Source
created: 2026-06-11
updated: 2026-06-11
status: raw
stage: 0

market: KRX
ticker: "051900"
company: LG생활건강
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-11

analysis:
  summary: Local coverage report row shows news coverage for KRX 051900.
  key_facts:
    - code=051900
    - name=LG생활건강
    - naver_article_count=1
    - google_rss_article_count=6
    - kis_title_count=5
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 051900
    - LG생활건강
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

# KRX 051900 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=051900`
- `name=LG생활건강`
- `naver_article_count=1`
- `google_rss_article_count=6`
- `kis_title_count=5`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 051900.

## RSS Item Metadata
- Title: `LG생활건강株 15년래 최저 수준?… 시장은 '성장성'을 더 따진다 - 디지털포스트(PC사랑)`
- Source: `디지털포스트(PC사랑)`
- Published at: `2026-06-11T10:45:28+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTE1lT05iNTFWVkNzcmh3TTEzV3BTMXdxckhMN0I3QU94YXVydnBGVkJBZGh3N2hqV0t2NzMtcmVscmd2dUl4amcwOTFLWjhlNzNEblhPVHBDc25sMjJ3YXJZQXRGY2pPd0ZOQllZ?oc=5`

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
- Updated at: `2026-06-11T17:05:49+09:00`
- Company: [[KRX_051900_LG생활건강]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-11.json`
- Latest observation title: `LG생활건강株 15년래 최저 수준?… 시장은 '성장성'을 더 따진다 - 디지털포스트(PC사랑)`
- Latest observation source: `디지털포스트(PC사랑)`
- Latest observation published_at: `2026-06-11T10:45:28+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE1lT05iNTFWVkNzcmh3TTEzV3BTMXdxckhMN0I3QU94YXVydnBGVkJBZGh3N2hqV0t2NzMtcmVscmd2dUl4amcwOTFLWjhlNzNEblhPVHBDc25sMjJ3YXJZQXRGY2pPd0ZOQllZ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
