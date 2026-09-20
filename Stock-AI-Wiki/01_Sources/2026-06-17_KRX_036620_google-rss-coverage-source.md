---
id: source-2026-06-17-KRX-036620-google-rss-coverage
type: source
title: KRX 036620 Google RSS Coverage Source
created: 2026-06-17
updated: 2026-06-17
status: raw
stage: 0

market: KRX
ticker: "036620"
company: 감성코퍼레이션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-17

analysis:
  summary: Local coverage report row shows news coverage for KRX 036620.
  key_facts:
    - code=036620
    - name=감성코퍼레이션
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 036620
    - 감성코퍼레이션
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

# KRX 036620 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=036620`
- `name=감성코퍼레이션`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 036620.

## RSS Item Metadata
- Title: `[장중수급포착] 감성코퍼레이션, 외국인 5일 연속 순매수행진... 주가 +2.26% - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-06-17T10:17:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5Tb0QxaUZJelUySV9RTl9nMnItVk9ubm1fT0k1SmhrQmRKRGU5ckpKXzBOUFk5WUhNSmFzV1VWVWRZMXktTVBETktaWDRCenpnVkpoQXNTMkZZSGY5?oc=5`

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
- Updated at: `2026-08-21T19:21:46+09:00`
- Company: [[KRX_036620_감성코퍼레이션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-17.json`
- Latest observation title: `[장중수급포착] 감성코퍼레이션, 외국인 5일 연속 순매수행진... 주가 +2.26% - 뉴스핌`
- Latest observation source: `뉴스핌`
- Latest observation published_at: `2026-06-17T10:17:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5Tb0QxaUZJelUySV9RTl9nMnItVk9ubm1fT0k1SmhrQmRKRGU5ckpKXzBOUFk5WUhNSmFzV1VWVWRZMXktTVBETktaWDRCenpnVkpoQXNTMkZZSGY5?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
