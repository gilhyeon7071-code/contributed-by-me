---
id: verification-2026-08-14-KRX-078930-google-rss-coverage
type: verification
title: KRX 078930 Google RSS Coverage Verification
created: 2026-08-14
updated: 2026-08-14
status: verification
stage: 1

market: KRX
ticker: "078930"
company: GS
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-14

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=078930
    - name=GS
    - naver_article_count=2
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 078930
    - GS
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

# KRX 078930 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-14_KRX_078930_google-rss-coverage-source]]

## Facts Checked
- `code=078930`
- `name=GS`
- `naver_article_count=2`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `GS글로벌, 김 수출 본격화…K푸드 사업 영토 넓힌다 - 이투데이`
- Source: `이투데이`
- Published at: `2026-08-13T08:40:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVEFVX3lxTE9Dc2RIZ1BHZU5tVWRTaFZ4dklmNXBnYkh5UUVSOFpweGFjMjlFMWRreVZGSnh6Y3ZPMXpKRVByQnRyb2toeVZtRlZoVkYtT0VtVWt3VA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:36:10+09:00`
- Company: [[KRX_078930_GS]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-14.json`
- Latest observation title: `AI 날개 단 GS…"만년 저평가 해소 기대" - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-08-12T17:58:29+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE5XY0s5alh3Y212YlhxR0NZNW9CU2lkcUZnNlN2SklXSlhpR0hURk1MMEI0TVhsWFVKRW1XUHcwX3h1ZUFUOFJYTEtJSmM2blhDMlE5NFpfRlpCZw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
