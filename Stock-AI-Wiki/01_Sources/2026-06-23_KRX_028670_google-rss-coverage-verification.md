---
id: verification-2026-06-23-KRX-028670-google-rss-coverage
type: verification
title: KRX 028670 Google RSS Coverage Verification
created: 2026-06-23
updated: 2026-06-23
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
  collected_at: 2026-06-23

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
- [[2026-06-23_KRX_028670_google-rss-coverage-source]]

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
- Title: `[공시] 서산·크레오에스지 '거래정지 예고', 카카오게임즈·팬오션 '공매도 금지' 등...71건 - 뉴스티앤티`
- Source: `뉴스티앤티`
- Published at: `2026-06-22T21:04:45+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE9uaHM5N1EyTm44T0pOVjM0ckd6RUgxR2tsSExoUVY5Xy1ncEtuaUhLSnVxWTJ3SGtxZWRxQ3hOcmtPRjdyZ1ZmY0dQU2g5MHRWcks3Y1pyX21QUWEzUjFBM3JERkExWmtDeVE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:23:39+09:00`
- Company: [[KRX_028670_팬오션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-23.json`
- Latest observation title: `오너 2세 전면에 선 팬오션, 정보보호 전담인력 `0명`…모회사 하림과 닮은 꼴 - 디지털데일리`
- Latest observation source: `디지털데일리`
- Latest observation published_at: `2026-06-23T06:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZEFVX3lxTE1FY0tRc0xSWkV1d3ZfeEhWZ3RmdGphYmNSclIzWUVSRGFfWmRSTkxIdHJlR0paSmZ0c3RMZUN3S25WWC15dF85WkE2QkU1TTdkVEJoWWdDc09Lc2JUQWVnTVNXOXo?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
