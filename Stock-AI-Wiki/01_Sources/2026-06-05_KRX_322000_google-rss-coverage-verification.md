---
id: verification-2026-06-05-KRX-322000-google-rss-coverage
type: verification
title: KRX 322000 Google RSS Coverage Verification
created: 2026-06-05
updated: 2026-06-05
status: verification
stage: 1

market: KRX
ticker: "322000"
company: HD현대에너지솔루션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-05

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=322000
    - name=HD현대에너지솔루션
    - naver_article_count=0
    - google_rss_article_count=4
    - kis_title_count=12
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 322000
    - HD현대에너지솔루션
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

# KRX 322000 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-05_KRX_322000_google-rss-coverage-source]]

## Facts Checked
- `code=322000`
- `name=HD현대에너지솔루션`
- `naver_article_count=0`
- `google_rss_article_count=4`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `HD현대에너지솔루션, +9.56% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-04T09:30:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNV0dJczBxT01COW02STgyOEZfM3FaSEk3SWdzT3diVmFlQkZkNnJfakFPVUp0ZDNXMl9HQ0Vfc0ZKcjIxbEptQ3paME9QQUpDZXRrc1VMbTMycUxIazZhMEJianVmQTdqSGJycGhBQkpzaWZGMmZIRC02R1plWWlYWE9TONIBlwFBVV95cUxOQXJoTEZqWnVNejl4TVFXUUpnSmpJeXdpeFN1cnpGbTgxZDhlUE15SXVjWEhIYXVqWU1oUUpGZHExcXdXcWdIWjJhbUUtWTlOYnItN3RITkI3MzBXcmYzRlFiY2hGclpaWHpCRjdyaU5walFMWGRNRGdTOXNkbmpUejNtSkFhVkRXNXRqVFRjVUlvVDZJS3hn?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-05T21:10:20+09:00`
- Company: [[KRX_322000_HD현대에너지솔루션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `HD현대에너지솔루션, +9.56% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-04T09:30:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNV0dJczBxT01COW02STgyOEZfM3FaSEk3SWdzT3diVmFlQkZkNnJfakFPVUp0ZDNXMl9HQ0Vfc0ZKcjIxbEptQ3paME9QQUpDZXRrc1VMbTMycUxIazZhMEJianVmQTdqSGJycGhBQkpzaWZGMmZIRC02R1plWWlYWE9TONIBlwFBVV95cUxOQXJoTEZqWnVNejl4TVFXUUpnSmpJeXdpeFN1cnpGbTgxZDhlUE15SXVjWEhIYXVqWU1oUUpGZHExcXdXcWdIWjJhbUUtWTlOYnItN3RITkI3MzBXcmYzRlFiY2hGclpaWHpCRjdyaU5walFMWGRNRGdTOXNkbmpUejNtSkFhVkRXNXRqVFRjVUlvVDZJS3hn?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
