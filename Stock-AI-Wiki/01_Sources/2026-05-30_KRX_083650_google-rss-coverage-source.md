---
id: source-2026-05-30-KRX-083650-google-rss-coverage
type: source
title: KRX 083650 Google RSS Coverage Source
created: 2026-05-30
updated: 2026-05-30
status: raw
stage: 0

market: KRX
ticker: "083650"
company: 비에이치아이
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-30

analysis:
  summary: Local coverage report row shows news coverage for KRX 083650.
  key_facts:
    - code=083650
    - name=비에이치아이
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 083650
    - 비에이치아이
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

# KRX 083650 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=083650`
- `name=비에이치아이`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 083650.

## RSS Item Metadata
- Title: `비에이치아이, 이스라엘서 575억원 규모 HRSG 공급 수주 성사 - 파이낸스스코프`
- Source: `파이낸스스코프`
- Published at: `2026-05-26T12:32:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE5BeWptT0x3MXFkNFJsNHMwR1JVUjh2dXNVNmhkbDN2TmVsV0VqUklJNEc1ZkdrRGlSX2hwSFlTaVg4bFcydDJZanI1Y3gzOTB5SE5Za2Y5bW14WUU0eHpSajh3ZDZvLTcwUmc?oc=5`

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
- Company: [[KRX_083650_비에이치아이]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-30.json`
- Latest observation title: `비에이치아이, 이스라엘서 575억원 규모 HRSG 공급 수주 성사 - 파이낸스스코프`
- Latest observation source: `파이낸스스코프`
- Latest observation published_at: `2026-05-26T12:32:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE5BeWptT0x3MXFkNFJsNHMwR1JVUjh2dXNVNmhkbDN2TmVsV0VqUklJNEc1ZkdrRGlSX2hwSFlTaVg4bFcydDJZanI1Y3gzOTB5SE5Za2Y5bW14WUU0eHpSajh3ZDZvLTcwUmc?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
