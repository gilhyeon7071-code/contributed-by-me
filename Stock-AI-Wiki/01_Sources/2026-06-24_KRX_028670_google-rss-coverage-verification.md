---
id: verification-2026-06-24-KRX-028670-google-rss-coverage
type: verification
title: KRX 028670 Google RSS Coverage Verification
created: 2026-06-24
updated: 2026-06-24
status: verification
stage: 1

market: KRX
ticker: "028670"
company: 팬오션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-24

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=028670
    - name=팬오션
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 028670
    - 팬오션
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

# KRX 028670 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-24_KRX_028670_google-rss-coverage-source]]

## Facts Checked
- `code=028670`
- `name=팬오션`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[2026 100대 CEO] 안중호 팬오션 대표, ‘해운 외길 37년’ 100년 기업 향한 질주 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-06-24T07:12:18+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFBiSGl4ajlaeDNyNjZfRG1HalJhdDNIeGdJVlIzbnllQ2c2NmRoNXBPb2tjODVhNXhoTG1LMjh4Vm4wRFhjZVc3YXBVQTFxOTg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:23:59+09:00`
- Company: [[KRX_028670_팬오션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-24.json`
- Latest observation title: `팬오션 최대주주 하림지주, 팬오션 주식등의 수 2000주 증가…총 지분율 54.91% - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-06-24T18:32:04+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBBN2VhMV8xTE5IUVdCYWdWRjI5bEhjMnV0UVVWSHRMSFFVcS1HRVFjZDFMOVZET19VX3F6c2NpR0N6bkw3SzRVaTNqRUF6eEhRYVllLV9ibGxiYi1MTDUwV185c0FsT2IzTEY3QUJiZjNfMVU?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
