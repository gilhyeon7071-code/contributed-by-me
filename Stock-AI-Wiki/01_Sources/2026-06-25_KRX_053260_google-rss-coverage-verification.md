---
id: verification-2026-06-25-KRX-053260-google-rss-coverage
type: verification
title: KRX 053260 Google RSS Coverage Verification
created: 2026-06-25
updated: 2026-06-25
status: verification
stage: 1

market: KRX
ticker: "053260"
company: 금강철강
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=053260
    - name=금강철강
    - naver_article_count=2
    - google_rss_article_count=7
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 053260
    - 금강철강
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is unavailable.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: false
  numeric_values_checked: true
  date_values_checked: true
  entity_names_checked: false
  conflict_exists: false
  conflict_summary:
  confidence: unknown

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
  change_reason: generated coverage verification note
---

# KRX 053260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-25_KRX_053260_google-rss-coverage-source]]

## Facts Checked
- `code=053260`
- `name=금강철강`
- `naver_article_count=2`
- `google_rss_article_count=7`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `특징주, 금강철강-철강_중소형 테마 상승세에 27.26% ↑ - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-06-22T13:14:27+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFBORDdUTUVTRlRaMC1mZlRtUjBrZlZ0NVFCZVhNMzBEaDRnM3RQbnM1NzdxZURIQ29jRVBEN1FOTGVLQ2J3TmVzS1RQc29FaVlXQnc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:24:18+09:00`
- Company: [[KRX_053260_금강철강]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-25.json`
- Latest observation title: `특징주, 금강철강-철강_중소형 테마 상승세에 27.26% ↑ - 매일경제 마켓`
- Latest observation source: `매일경제 마켓`
- Latest observation published_at: `2026-06-22T13:14:27+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFBORDdUTUVTRlRaMC1mZlRtUjBrZlZ0NVFCZVhNMzBEaDRnM3RQbnM1NzdxZURIQ29jRVBEN1FOTGVLQ2J3TmVzS1RQc29FaVlXQnc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
