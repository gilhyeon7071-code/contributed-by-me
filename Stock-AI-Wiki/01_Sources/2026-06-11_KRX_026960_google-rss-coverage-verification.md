---
id: verification-2026-06-11-KRX-026960-google-rss-coverage
type: verification
title: KRX 026960 Google RSS Coverage Verification
created: 2026-06-11
updated: 2026-06-11
status: verification
stage: 1

market: KRX
ticker: "026960"
company: 동서
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-11

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=026960
    - name=동서
    - naver_article_count=1
    - google_rss_article_count=48
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 026960
    - 동서
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

# KRX 026960 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-11_KRX_026960_google-rss-coverage-source]]

## Facts Checked
- `code=026960`
- `name=동서`
- `naver_article_count=1`
- `google_rss_article_count=48`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `동서발전, 태화강 국가정원서 에너지 절약 캠페인 - 전기신문`
- Source: `전기신문`
- Published at: `2026-06-11T15:13:36+09:00`
- Link: `https://news.google.com/rss/articles/CBMicEFVX3lxTE84YjZZeXA4bUVMaUoyMzlsM1c5TGZLZE45RllYTHI5aEhmd3JBQlZjMTJacUNlT2phYmNhODdlQWZzcExPMmx6Z3cyV2NvNEdaUU9EN1V3N0VYSEZ0cFpRYmFILWQxVzRMcHltZDVIT2LSAXBBVV95cUxPOGI2WXlwOG1FTGlKMjM5bDNXOUxmS2ROOUZZWExyOWhIZndyQUJWYzEyWnFDZU9qYWJjYTg3ZUFmc3BMTzJsemd3MldjbzRHWlFPRDdVdzdFWEhGdHBaUWJhSC1kMVc0THB5bWQ1SE9i?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:20:10+09:00`
- Company: [[KRX_026960_동서]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-11.json`
- Latest observation title: `동서발전, 제주복합 5500억 계약…계통안정·수소전환 겨냥 - 전기신문`
- Latest observation source: `전기신문`
- Latest observation published_at: `2026-06-11T17:31:44+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTE85dFNLQy1KZWd3cDBjLVBvM091Vjlrb0t5ZVczTHlXRGZtYUpkWExYVlFPczB0QjhqZm9BNHBEX0I3QVM3eS1uXzBEaXkzTVl6TnoxQjBjbVF2ZDNUZEpZeUxCRTRFVVpLZm11TWxZVWHSAXBBVV95cUxPOXRTS0MtSmVnd3AwYy1QbzNPdVY5a29LeWVXM0x5V0RmbWFKZFhMWFZRT3MwdEI4amZvQTRwRF9CN0FTN3ktbl8wRGl5M01Zek56MUIwY21RdmQzVGRKWXlMQkU0RVVaS2ZtdU1sWVVh?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
