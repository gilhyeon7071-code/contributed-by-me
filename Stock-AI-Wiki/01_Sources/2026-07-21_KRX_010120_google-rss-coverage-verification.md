---
id: verification-2026-07-21-KRX-010120-google-rss-coverage
type: verification
title: KRX 010120 Google RSS Coverage Verification
created: 2026-07-21
updated: 2026-07-21
status: verification
stage: 1

market: KRX
ticker: "010120"
company: 엘에스일렉트릭
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=010120
    - name=엘에스일렉트릭
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=18
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 010120
    - 엘에스일렉트릭
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

# KRX 010120 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-21_KRX_010120_google-rss-coverage-source]]

## Facts Checked
- `code=010120`
- `name=엘에스일렉트릭`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=18`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `KB證 "LS일렉트릭, 원활한 신규 수주…목표가 16.7% 상향" - 뉴스1`
- Source: `뉴스1`
- Published at: `2026-07-20T09:57:09+09:00`
- Link: `https://news.google.com/rss/articles/CBMiX0FVX3lxTE95YkIxRWl3c0VsekU4X1JCNUVqTHl1VHVCekY2TjdEZGxMbk9nTmptcU90SUZ0Y294cTdyWFdHMW1od2NqOXdZWnJGcHBqbDNJN2VsdFNndGFyWVBhNHNj?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:30:18+09:00`
- Company: [[KRX_010120_엘에스일렉트릭]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-21.json`
- Latest observation title: `엘에스일렉트릭, 자기주식 3만2520주 직원에게 처분…61억7880만원 규모 - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-07-21T17:14:01+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBfenJhU2xhUlBtNmhHWWlmaTh1bnFNaHF5bDFmQzNoRGNGa1A0c0VPblhzRWJzLS1FUTJIM1NyeUNKcGZIMHRPTTRTclhmMzJDVmEzcnR1VE5wQnRlc1RXVFZ3cmpOeW11bmpQaHExdU9pM3c?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
