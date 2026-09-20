---
id: verification-2026-08-13-KRX-010060-google-rss-coverage
type: verification
title: KRX 010060 Google RSS Coverage Verification
created: 2026-08-13
updated: 2026-08-13
status: verification
stage: 1

market: KRX
ticker: "010060"
company: OCI홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-13

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=010060
    - name=OCI홀딩스
    - naver_article_count=2
    - google_rss_article_count=62
    - kis_title_count=25
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 010060
    - OCI홀딩스
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

# KRX 010060 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-13_KRX_010060_google-rss-coverage-source]]

## Facts Checked
- `code=010060`
- `name=OCI홀딩스`
- `naver_article_count=2`
- `google_rss_article_count=62`
- `kis_title_count=25`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `'스페이스X 1조원 공급계약 논의' OCI 말련 자회사 "美 태양광 관세 환영" 존재감 드러내 - 더구루`
- Source: `더구루`
- Published at: `2026-08-11T10:27:30+09:00`
- Link: `https://news.google.com/rss/articles/CBMiY0FVX3lxTFAtbDVkU1d3cmd6eW82dHBQalZ6bzRadEFxR1JzaWpva0FYQWNpeEZldUxyR0t6SHVYSHNtVGQtTlpvT2tDaXhmMklWQ05LSUhhbmhnMnhRMXJ3UFk3YkloQ1hoQQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:35:49+09:00`
- Company: [[KRX_010060_OCI홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-13.json`
- Latest observation title: `OCI홀딩스 주가, 8월 13일 장중 273,500원 6.84% 상승 - topstarnews.net`
- Latest observation source: `topstarnews.net`
- Latest observation published_at: `2026-08-13T10:27:37+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTFB2U0Jaa0w4YmItU3VUN0lfejBPTWhlRDcxdHRkcFpTdld5RVFVcGtTWkRmV0NlcnpDZWxjSk1tTXItcjlNbzQ2dW1iMWpmZy1oem94QzhvQzRmNk1qRVN3UFRLNWJjUDlLRzlJZTBzNW5VZw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
