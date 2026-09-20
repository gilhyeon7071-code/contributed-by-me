---
id: source-2026-08-11-KRX-096770-google-rss-coverage
type: source
title: KRX 096770 Google RSS Coverage Source
created: 2026-08-11
updated: 2026-08-11
status: raw
stage: 0

market: KRX
ticker: "096770"
company: SK이노베이션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-11

analysis:
  summary: Local coverage report row shows news coverage for KRX 096770.
  key_facts:
    - code=096770
    - name=SK이노베이션
    - naver_article_count=1
    - google_rss_article_count=19
    - kis_title_count=17
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 096770
    - SK이노베이션
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

# KRX 096770 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=096770`
- `name=SK이노베이션`
- `naver_article_count=1`
- `google_rss_article_count=19`
- `kis_title_count=17`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 096770.

## RSS Item Metadata
- Title: `SK이노베이션 울산CLX, 폭염 속 협력사 노동자에 간식 지원 - 2news.co.kr`
- Source: `2news.co.kr`
- Published at: `2026-08-10T11:16:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE9hVkxhb041TnEyTTNFT0p4ZDdHOFJxNE5uWDQzUHdTc0ZtTEpKOUN2ZFRvOXM2bFhpbEJnZ2Vha1FwTUxZaTBwbnZwWVVYNFNaNHhWYV84b0xoT2Q2a3VWekxiWUIySTk2?oc=5`

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
- Updated at: `2026-08-21T19:35:08+09:00`
- Company: [[KRX_096770_SK이노베이션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-11.json`
- Latest observation title: `SK이노베이션 주가, 8월 11일 124,100원 0.32% 하락 마감 - 톱스타뉴스`
- Latest observation source: `톱스타뉴스`
- Latest observation published_at: `2026-08-11T15:51:35+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTE1adXFZbmlwZUpqbjhvYzFPSXBPdUlPZ21fb3ZJUnl5OG1aWjJHS01WXzZ4WWpiM2RScWt4ZXlHRzlUTDI5YXcyZTFzMV9jQjJ5aUhBQmd6UER1d2tsTDJrSjVvX1FYbElWWVhZbWdjQjB5dw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
