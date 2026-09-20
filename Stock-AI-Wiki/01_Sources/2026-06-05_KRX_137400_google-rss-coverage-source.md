---
id: source-2026-06-05-KRX-137400-google-rss-coverage
type: source
title: KRX 137400 Google RSS Coverage Source
created: 2026-06-05
updated: 2026-06-05
status: raw
stage: 0

market: KRX
ticker: "137400"
company: 피엔티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-05

analysis:
  summary: Local coverage report row shows news coverage for KRX 137400.
  key_facts:
    - code=137400
    - name=피엔티
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 137400
    - 피엔티
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

# KRX 137400 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=137400`
- `name=피엔티`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## Facts
- The local coverage report contains a coverage row for KRX 137400.

## RSS Item Metadata
- Title: `피엔티, 1분기 실적 절반 감소...하반기 반등 노린다 - 디일렉`
- Source: `디일렉`
- Published at: `2026-05-14T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE0yTFl0Q2hEQlRaakRTSDhkZjE1YW5xUnlJUnNHY0pFMGdVSWxPaldvN25SRGpEbU5LbnQtTnB0Tkp3bUhsWUNFaDUzU0JSYmRYMS1LR0NoOHFsb21VQWJHNjdNeW5Rdw?oc=5`

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
- Updated at: `2026-08-21T19:18:19+09:00`
- Company: [[KRX_137400_피엔티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `피엔티, 1분기 실적 절반 감소...하반기 반등 노린다 - 디일렉`
- Latest observation source: `디일렉`
- Latest observation published_at: `2026-05-14T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZkFVX3lxTE0yTFl0Q2hEQlRaakRTSDhkZjE1YW5xUnlJUnNHY0pFMGdVSWxPaldvN25SRGpEbU5LbnQtTnB0Tkp3bUhsWUNFaDUzU0JSYmRYMS1LR0NoOHFsb21VQWJHNjdNeW5Rdw?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
