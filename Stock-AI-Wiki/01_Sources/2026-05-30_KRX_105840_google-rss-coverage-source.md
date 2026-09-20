---
id: source-2026-05-30-KRX-105840-google-rss-coverage
type: source
title: KRX 105840 Google RSS Coverage Source
created: 2026-05-30
updated: 2026-05-30
status: raw
stage: 0

market: KRX
ticker: "105840"
company: 우진
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-30

analysis:
  summary: Local coverage report row shows news coverage for KRX 105840.
  key_facts:
    - code=105840
    - name=우진
    - naver_article_count=1
    - google_rss_article_count=19
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 105840
    - 우진
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

# KRX 105840 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=105840`
- `name=우진`
- `naver_article_count=1`
- `google_rss_article_count=19`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 105840.

## RSS Item Metadata
- Title: `우진플라임 TB160S 하이브리드 사출성형기 - 다아라`
- Source: `다아라`
- Published at: `2026-05-28T11:49:09+09:00`
- Link: `https://news.google.com/rss/articles/CBMiX0FVX3lxTFA3NlNkdmdxUjJsQlZPWFktMmhrcks4c0tLekRQeGFLZy1qVy1tM1lpZkZXMFJMbl9FSGFuV2ZBUkQ5SjRRb3BiT0tOQ1N0QXZMRFJjYVRJNXM3LU5lUFR3?oc=5`

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
- Updated at: `2026-08-21T19:16:37+09:00`
- Company: [[KRX_105840_우진]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-30.json`
- Latest observation title: `우진플라임 TB160S 하이브리드 사출성형기 - 다아라`
- Latest observation source: `다아라`
- Latest observation published_at: `2026-05-28T11:49:09+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiX0FVX3lxTFA3NlNkdmdxUjJsQlZPWFktMmhrcks4c0tLekRQeGFLZy1qVy1tM1lpZkZXMFJMbl9FSGFuV2ZBUkQ5SjRRb3BiT0tOQ1N0QXZMRFJjYVRJNXM3LU5lUFR3?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
