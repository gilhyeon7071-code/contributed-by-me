---
id: source-2026-07-06-KRX-095340-google-rss-coverage
type: source
title: KRX 095340 Google RSS Coverage Source
created: 2026-07-06
updated: 2026-07-06
status: raw
stage: 0

market: KRX
ticker: "095340"
company: ISC
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-06

analysis:
  summary: Local coverage report row shows news coverage for KRX 095340.
  key_facts:
    - code=095340
    - name=ISC
    - naver_article_count=8
    - google_rss_article_count=2
    - kis_title_count=8
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 095340
    - ISC
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

# KRX 095340 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=095340`
- `name=ISC`
- `naver_article_count=8`
- `google_rss_article_count=2`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 095340.

## RSS Item Metadata
- Title: `[코스닥 현미경 분석] ISC, ASIC·xPU 등 구조적 수혜 구간 진입…주가 재도약? - 데일리인베스트`
- Source: `데일리인베스트`
- Published at: `2026-06-12T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTE5uenhTQUg5U3BNSHpXaGg5aEp4dmhONmM5Wk93eDItWFVRVC16NnZNeV9fRW9KVkZtOW5wQ3hseUk4OGZWMlU4M282Q2dlV2ZsejZBdFI3eHFmVnlYXzZaaFFMSnJWRFE5MUpj0gFvQVVfeXFMTkhTam50V3N0TEZIMlNjOGNHOFJnZ29qM1hpWkNDQ255aUE1Yko4c0Fmb29iSzJpMk84V29oOVotX3dXYVcyWVlRcmhna0tDU1ZlYkphdzhHTzRzWHRaUTY2Y1hUNy1yZU9CQjBTZ013?oc=5`

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
- Updated at: `2026-07-06T08:05:14+09:00`
- Company: [[KRX_095340_ISC]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-06.json`
- Latest observation title: `[코스닥 현미경 분석] ISC, ASIC·xPU 등 구조적 수혜 구간 진입…주가 재도약? - 데일리인베스트`
- Latest observation source: `데일리인베스트`
- Latest observation published_at: `2026-06-12T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE5uenhTQUg5U3BNSHpXaGg5aEp4dmhONmM5Wk93eDItWFVRVC16NnZNeV9fRW9KVkZtOW5wQ3hseUk4OGZWMlU4M282Q2dlV2ZsejZBdFI3eHFmVnlYXzZaaFFMSnJWRFE5MUpj0gFvQVVfeXFMTkhTam50V3N0TEZIMlNjOGNHOFJnZ29qM1hpWkNDQ255aUE1Yko4c0Fmb29iSzJpMk84V29oOVotX3dXYVcyWVlRcmhna0tDU1ZlYkphdzhHTzRzWHRaUTY2Y1hUNy1yZU9CQjBTZ013?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
