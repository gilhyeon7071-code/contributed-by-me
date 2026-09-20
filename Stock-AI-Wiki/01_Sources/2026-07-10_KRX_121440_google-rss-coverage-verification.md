---
id: verification-2026-07-10-KRX-121440-google-rss-coverage
type: verification
title: KRX 121440 Google RSS Coverage Verification
created: 2026-07-10
updated: 2026-07-10
status: verification
stage: 1

market: KRX
ticker: "121440"
company: 골프존홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-10

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=121440
    - name=골프존홀딩스
    - naver_article_count=2
    - google_rss_article_count=11
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 121440
    - 골프존홀딩스
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

# KRX 121440 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-10_KRX_121440_google-rss-coverage-source]]

## Facts Checked
- `code=121440`
- `name=골프존홀딩스`
- `naver_article_count=2`
- `google_rss_article_count=11`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `골프존홀딩스 상폐 추진에…개미들 부글부글, 왜? - 뉴시스`
- Source: `뉴시스`
- Published at: `2026-06-30T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE5DLXI2MFBPcGhubkxSZUZpeDQ3bWczSVVWODliMEx1bXdlRkhqLTE5Zlp0LUNQMU1BdlItQVpTMHR1ZUh6a0prTVl0OWxVcjRYRHBYcFU2UFZJaVE3VGo5RtIBeEFVX3lxTFBnTkxUaXRwRmE5WWV6ZnR3Rk9DWlJyQ2VLQ1NqbHRLaFBUSWktUng4aUhFLUd4S2ZtTlhNMGJNVGJybWg2UzM0X3RnMDRKN0l6d3JIOXowSTZjLWlqYXY1NGl3bUdDdFRMaGZkYkV5UVlFanFTalhKTg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:28:24+09:00`
- Company: [[KRX_121440_골프존홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-10.json`
- Latest observation title: `골프존홀딩스 상폐 추진에…개미들 부글부글, 왜? - 뉴시스`
- Latest observation source: `뉴시스`
- Latest observation published_at: `2026-06-30T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE5DLXI2MFBPcGhubkxSZUZpeDQ3bWczSVVWODliMEx1bXdlRkhqLTE5Zlp0LUNQMU1BdlItQVpTMHR1ZUh6a0prTVl0OWxVcjRYRHBYcFU2UFZJaVE3VGo5RtIBeEFVX3lxTFBnTkxUaXRwRmE5WWV6ZnR3Rk9DWlJyQ2VLQ1NqbHRLaFBUSWktUng4aUhFLUd4S2ZtTlhNMGJNVGJybWg2UzM0X3RnMDRKN0l6d3JIOXowSTZjLWlqYXY1NGl3bUdDdFRMaGZkYkV5UVlFanFTalhKTg?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
