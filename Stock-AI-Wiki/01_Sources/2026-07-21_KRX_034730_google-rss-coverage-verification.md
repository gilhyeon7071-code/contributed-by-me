---
id: verification-2026-07-21-KRX-034730-google-rss-coverage
type: verification
title: KRX 034730 Google RSS Coverage Verification
created: 2026-07-21
updated: 2026-07-21
status: verification
stage: 1

market: KRX
ticker: "034730"
company: SK
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
    - code=034730
    - name=SK
    - naver_article_count=10
    - google_rss_article_count=125
    - kis_title_count=29
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034730
    - SK
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

# KRX 034730 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-21_KRX_034730_google-rss-coverage-source]]

## Facts Checked
- `code=034730`
- `name=SK`
- `naver_article_count=10`
- `google_rss_article_count=125`
- `kis_title_count=29`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `‘-40%인데 더 버텨야 해?’ SK하이닉스 투자했다가 참담…‘22일’이 진짜 중요 [투자360] - 헤럴드경제`
- Source: `헤럴드경제`
- Published at: `2026-07-20T18:40:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTE1FU1hTOVhhTDhiVUJkbC14cTBkdU5JU1hqUVRfTWdFTU1VY2NrZy1rN090ODdUYno0c2ZiSnlja3FUTDFQLVJzUGdVMkRUNHdZOVFXbldB?oc=5`

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
- Company: [[KRX_034730_SK]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-21.json`
- Latest observation title: `美 반도체 훈풍에 삼전·SK하닉, 프리마켓서 상승 …저가매수 유입 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-07-21T08:21:09+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFA0M1Q2bm9vWmd0eTJwME5uMmhRRTdMUmlwY1BpS2ZZYXNqREtHN3BaWEd3TnRiYkxNVVBtWDRnWmh4X0JmZ21DTHZILVdkNUE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
