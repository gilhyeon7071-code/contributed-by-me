---
id: verification-2026-07-06-KRX-016360-google-rss-coverage
type: verification
title: KRX 016360 Google RSS Coverage Verification
created: 2026-07-06
updated: 2026-07-06
status: verification
stage: 1

market: KRX
ticker: "016360"
company: 삼성증권
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-06

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=016360
    - name=삼성증권
    - naver_article_count=1
    - google_rss_article_count=54
    - kis_title_count=39
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 016360
    - 삼성증권
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

# KRX 016360 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-06_KRX_016360_google-rss-coverage-source]]

## Facts Checked
- `code=016360`
- `name=삼성증권`
- `naver_article_count=1`
- `google_rss_article_count=54`
- `kis_title_count=39`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼성증권 "삼전, 2분기 영업익 86조 추정…상여충당금 16.3조" - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-07-06T08:48:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE9yYnVsaWgxUnZ4ei1LWkZyZWhJZXQwcDdTVkVMTzdHRC1QejNWOHN6ZVZac3VBOW5yM1VXbG5IMTF6Si1rYmRwYUUyZmdTdEU?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:27:09+09:00`
- Company: [[KRX_016360_삼성증권]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-06.json`
- Latest observation title: `삼성증권 "삼전, 2분기 영업익 86조 추정…상여충당금 16.3조" - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-07-06T08:48:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE9yYnVsaWgxUnZ4ei1LWkZyZWhJZXQwcDdTVkVMTzdHRC1QejNWOHN6ZVZac3VBOW5yM1VXbG5IMTF6Si1rYmRwYUUyZmdTdEU?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
