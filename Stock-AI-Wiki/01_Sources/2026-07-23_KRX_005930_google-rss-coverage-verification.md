---
id: verification-2026-07-23-KRX-005930-google-rss-coverage
type: verification
title: KRX 005930 Google RSS Coverage Verification
created: 2026-07-23
updated: 2026-07-23
status: verification
stage: 1

market: KRX
ticker: "005930"
company: 삼성전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-23

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=005930
    - name=삼성전자
    - naver_article_count=8
    - google_rss_article_count=98
    - kis_title_count=40
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 005930
    - 삼성전자
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

# KRX 005930 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-23_KRX_005930_google-rss-coverage-source]]

## Facts Checked
- `code=005930`
- `name=삼성전자`
- `naver_article_count=8`
- `google_rss_article_count=98`
- `kis_title_count=40`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼성전자, CSS 사업팀 인력 메모리·시스템LSI 등으로 재배치 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-07-23T19:39:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiggFBVV95cUxOOGZRQnRQVVNmRm5keXpvX2hEYXJVaEIwOS1ScEZNeXVHdUIxRnFTeUZWLTA3SEt1T2haUVRfbURfdEFha25fMWhjbFhXNXpxTm1GbVlvNW1mZ0Z1Qkh0ZUxEVUhlRF9iQVhBV1lhcjRKRElvVkNZbVRLZUtXY0hMSVJB0gGWAUFVX3lxTE5SckZtaFJPdlF2TlhjaHdqWHRQUlM5b1d3VXVpQUZrOXRPOTZCejBXRjljWTdBTVp1aTFfSUFici1aaUpzN1lHTDhMdlRPU0NlVzFFYVBlRGhmaTdOSnhtNHNvZHQzN3RST211ZDMtQzZIMkd2SVNnZmJXMFgtc0F0YUNhQXkyWjI0Y3ItZ3I1TXpQdXFoUQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:30:57+09:00`
- Company: [[KRX_005930_삼성전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-23.json`
- Latest observation title: `삼성전자, CSS 사업팀 인력 메모리·시스템LSI 등으로 재배치 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-07-23T19:39:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiggFBVV95cUxOOGZRQnRQVVNmRm5keXpvX2hEYXJVaEIwOS1ScEZNeXVHdUIxRnFTeUZWLTA3SEt1T2haUVRfbURfdEFha25fMWhjbFhXNXpxTm1GbVlvNW1mZ0Z1Qkh0ZUxEVUhlRF9iQVhBV1lhcjRKRElvVkNZbVRLZUtXY0hMSVJB0gGWAUFVX3lxTE5SckZtaFJPdlF2TlhjaHdqWHRQUlM5b1d3VXVpQUZrOXRPOTZCejBXRjljWTdBTVp1aTFfSUFici1aaUpzN1lHTDhMdlRPU0NlVzFFYVBlRGhmaTdOSnhtNHNvZHQzN3RST211ZDMtQzZIMkd2SVNnZmJXMFgtc0F0YUNhQXkyWjI0Y3ItZ3I1TXpQdXFoUQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
