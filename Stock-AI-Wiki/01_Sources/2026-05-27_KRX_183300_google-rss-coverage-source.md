---
id: source-2026-05-27-KRX-183300-google-rss-coverage
type: source
title: KRX 183300 Google RSS Coverage Source
created: 2026-05-27
updated: 2026-05-27
status: raw
stage: 0

market: KRX
ticker: "183300"
company: 코미코
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Local coverage report row shows news coverage for KRX 183300.
  key_facts:
    - code=183300
    - name=코미코
    - naver_article_count=2
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 183300
    - 코미코
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

# KRX 183300 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=183300`
- `name=코미코`
- `naver_article_count=2`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 183300.

## RSS Item Metadata
- Title: `[오늘의 증시일정] 코미코‧케이티스카이라이프 등 - 이투데이`
- Source: `이투데이`
- Published at: `2026-05-27T08:36:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVEFVX3lxTFBtN3ZFdWRWNUxOallBZmViaVlJY1NaQlAwSTdhUDhrY3BoUFBsY3AzNTRBalVZV1NkOFVFZ2ZhQTNabWFBMUg0MFhNOFdac1k3NC1lWg?oc=5`

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
- Updated at: `2026-05-27T11:06:36+09:00`
- Company: [[KRX_183300_코미코]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `[오늘의 증시일정] 코미코‧케이티스카이라이프 등 - 이투데이`
- Latest observation source: `이투데이`
- Latest observation published_at: `2026-05-27T08:36:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVEFVX3lxTFBtN3ZFdWRWNUxOallBZmViaVlJY1NaQlAwSTdhUDhrY3BoUFBsY3AzNTRBalVZV1NkOFVFZ2ZhQTNabWFBMUg0MFhNOFdac1k3NC1lWg?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
