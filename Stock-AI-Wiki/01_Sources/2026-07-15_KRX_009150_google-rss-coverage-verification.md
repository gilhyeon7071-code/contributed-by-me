---
id: verification-2026-07-15-KRX-009150-google-rss-coverage
type: verification
title: KRX 009150 Google RSS Coverage Verification
created: 2026-07-15
updated: 2026-07-15
status: verification
stage: 1

market: KRX
ticker: "009150"
company: 삼성전기
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-15

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=009150
    - name=삼성전기
    - naver_article_count=4
    - google_rss_article_count=6
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 009150
    - 삼성전기
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

# KRX 009150 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-15_KRX_009150_google-rss-coverage-source]]

## Facts Checked
- `code=009150`
- `name=삼성전기`
- `naver_article_count=4`
- `google_rss_article_count=6`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼성전기, 견조한 실적 전망 무색…9% 급락[핫종목] - 뉴스1`
- Source: `뉴스1`
- Published at: `2026-07-13T10:16:40+09:00`
- Link: `https://news.google.com/rss/articles/CBMiX0FVX3lxTE5SSTVZTUotNDMtakFtN01OWUNQTElzTWc0OVp1aHVUS3lJN0c1M1I3YWFqcXZsakllT2xlcjloS1NwTUlwX18zMlJNMHhNXzAwRm8wcUxUdVVjVzN1V1hz?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-15T23:05:24+09:00`
- Company: [[KRX_009150_삼성전기]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-15.json`
- Latest observation title: `삼성전기, 4년만 영업이익률 '두 자릿수'…기판·MLCC 수익 ↑ - 지디넷코리아`
- Latest observation source: `지디넷코리아`
- Latest observation published_at: `2026-07-15T10:18:25+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTFBLSmx6MW1BVmZ3VW1zVVNvdG9mZ3F5OHdETG5ORUtHN2k0TFY3Tkg0NnhueGMwMjB6aUNTVkFjMjRFYmV1akdqMUZXT1NTbmFtWWlyTkxn?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
