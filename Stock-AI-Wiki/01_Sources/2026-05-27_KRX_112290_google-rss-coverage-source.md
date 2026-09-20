---
id: source-2026-05-27-KRX-112290-google-rss-coverage
type: source
title: KRX 112290 Google RSS Coverage Source
created: 2026-05-27
updated: 2026-05-27
status: raw
stage: 0

market: KRX
ticker: "112290"
company: 와이씨켐
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Local coverage report row shows news coverage for KRX 112290.
  key_facts:
    - code=112290
    - name=와이씨켐
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 112290
    - 와이씨켐
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

# KRX 112290 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=112290`
- `name=와이씨켐`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 112290.

## RSS Item Metadata
- Title: `와이씨켐, 유리기판용 포토레지스트 업계 첫 공급 - 디일렉`
- Source: `디일렉`
- Published at: `2026-05-15T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE9UTjN5ZS1RSXNld2ExWWpZQ3g3MFdWU0kzajliWXpuQVR1YU5naTNRRUVRTGZiaFgwU2hTTzRUM1ZHUU1xdFp0YjRxVjhTSmppY3NwSFZwNjZCcG1uVVdWQndaU0NRQQ?oc=5`

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
- Updated at: `2026-05-27T17:05:04+09:00`
- Company: [[KRX_112290_와이씨켐]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `와이씨켐, 유리기판용 포토레지스트 업계 첫 공급 - 디일렉`
- Latest observation source: `디일렉`
- Latest observation published_at: `2026-05-15T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE9UTjN5ZS1RSXNld2ExWWpZQ3g3MFdWU0kzajliWXpuQVR1YU5naTNRRUVRTGZiaFgwU2hTTzRUM1ZHUU1xdFp0YjRxVjhTSmppY3NwSFZwNjZCcG1uVVdWQndaU0NRQQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
