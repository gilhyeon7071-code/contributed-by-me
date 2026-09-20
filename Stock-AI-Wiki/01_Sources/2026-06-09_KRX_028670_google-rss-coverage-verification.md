---
id: verification-2026-06-09-KRX-028670-google-rss-coverage
type: verification
title: KRX 028670 Google RSS Coverage Verification
created: 2026-06-09
updated: 2026-06-09
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
  collected_at: 2026-06-09

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=028670
    - name=팬오션
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
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
- [[2026-06-09_KRX_028670_google-rss-coverage-source]]

## Facts Checked
- `code=028670`
- `name=팬오션`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `"팬오션, 운임·환율 다 좋은데…주가만 조정받아"-한국 - 한국경제`
- Source: `한국경제`
- Published at: `2026-06-08T07:51:48+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVEFVX3lxTFAyekxXZHhWWFA5TFJRbnZRZHlFN3NkWmtZdkZvdjlXQ2lwRGpQTm5wZDBaSWliTWl2bVFQX21Ndi1pYXVCWktpWUZLNEliallpMDRsNNIBVEFVX3lxTFAyekxXZHhWWFA5TFJRbnZRZHlFN3NkWmtZdkZvdjlXQ2lwRGpQTm5wZDBaSWliTWl2bVFQX21Ndi1pYXVCWktpWUZLNEliallpMDRsNA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:19:32+09:00`
- Company: [[KRX_028670_팬오션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-09.json`
- Latest observation title: `"팬오션, 운임·환율 다 좋은데…주가만 조정받아"-한국 - 한국경제`
- Latest observation source: `한국경제`
- Latest observation published_at: `2026-06-08T07:51:48+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVEFVX3lxTFAyekxXZHhWWFA5TFJRbnZRZHlFN3NkWmtZdkZvdjlXQ2lwRGpQTm5wZDBaSWliTWl2bVFQX21Ndi1pYXVCWktpWUZLNEliallpMDRsNNIBVEFVX3lxTFAyekxXZHhWWFA5TFJRbnZRZHlFN3NkWmtZdkZvdjlXQ2lwRGpQTm5wZDBaSWliTWl2bVFQX21Ndi1pYXVCWktpWUZLNEliallpMDRsNA?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
