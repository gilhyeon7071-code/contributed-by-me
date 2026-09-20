---
id: verification-2026-05-27-KRX-251270-google-rss-coverage
type: verification
title: KRX 251270 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

market: KRX
ticker: "251270"
company: 넷마블
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=251270
    - name=넷마블
    - naver_article_count=2
    - google_rss_article_count=72
    - kis_title_count=8
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 251270
    - 넷마블
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

# KRX 251270 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_251270_google-rss-coverage-source]]

## Facts Checked
- `code=251270`
- `name=넷마블`
- `naver_article_count=2`
- `google_rss_article_count=72`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `넷마블, 전 직원에 본인 업무 특화된 AI 에이전트 붙인다 - 게임메카`
- Source: `게임메카`
- Published at: `2026-05-26T19:32:32+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWEFVX3lxTE5IaDNCM1o5eUo5ZVYyZEhMUmtmSFViWkdnd3AwbEhrYlRtUmljMHhSSG1lNDhuQVVldnN5cUtjaEpnVGkyRzFxX2I0MzNxQlluQUJWVkxSUXbSAVtBVV95cUxPRVIycXplU1Fmdk1vY0pCR0tXRXF0TVRVdm1BZkhNbG8wTkdBZ051MUw4RHFYMGxUdXJOLTM5S0tyaHdxQVZiWjJuNUx0YzZJVUtqUlYydEZyY1Vr?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:15:44+09:00`
- Company: [[KRX_251270_넷마블]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `넷마블, 전 직원에 본인 업무 특화된 AI 에이전트 붙인다 - 게임메카`
- Latest observation source: `게임메카`
- Latest observation published_at: `2026-05-26T19:32:32+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWEFVX3lxTE5IaDNCM1o5eUo5ZVYyZEhMUmtmSFViWkdnd3AwbEhrYlRtUmljMHhSSG1lNDhuQVVldnN5cUtjaEpnVGkyRzFxX2I0MzNxQlluQUJWVkxSUXbSAVtBVV95cUxPRVIycXplU1Fmdk1vY0pCR0tXRXF0TVRVdm1BZkhNbG8wTkdBZ051MUw4RHFYMGxUdXJOLTM5S0tyaHdxQVZiWjJuNUx0YzZJVUtqUlYydEZyY1Vr?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
