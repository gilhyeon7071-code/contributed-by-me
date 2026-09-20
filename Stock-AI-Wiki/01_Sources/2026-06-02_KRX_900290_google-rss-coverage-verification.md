---
id: verification-2026-06-02-KRX-900290-google-rss-coverage
type: verification
title: KRX 900290 Google RSS Coverage Verification
created: 2026-06-02
updated: 2026-06-02
status: verification
stage: 1

market: KRX
ticker: "900290"
company: GRT
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-02

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=900290
    - name=GRT
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 900290
    - GRT
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

# KRX 900290 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-02_KRX_900290_google-rss-coverage-source]]

## Facts Checked
- `code=900290`
- `name=GRT`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `GRT, -6.53% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-01T14:04:15+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNOWFRWVpKZEdEdmRQOWQxejNMQXUwRi16RUpwZGlYWEptYklwVGZtUlFWZF9uT21DMmk5WFdCeFVXSFNUMjRUcndvUEI5alRyVXJsOXdER3JROUR6OUVmTE02WDZMSTZQRE45aHYtV3lYd2tqLWRWUldiN25YeE9ZaHlkONIBlwFBVV95cUxNTFpobEVlemRLWktpLVVTTGRuczU4aF9kaVNOWk9EakFkOV9wNlYyTkFrN2RtQ2Uxb3dQWlRkZWRta0pVaUJUWUpTVDJpSEdkNHZqU19pUnJtOXJDaEpCcml5UW5SVnNXVVlBamdyTFI2NlM0cFVCVldmM0l4NFBTal9LWGRkQWF3V2ExWV8tclQweVZmWGZZ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:17:26+09:00`
- Company: [[KRX_900290_GRT]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-02.json`
- Latest observation title: `GRT, -6.53% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-01T14:04:15+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxNOWFRWVpKZEdEdmRQOWQxejNMQXUwRi16RUpwZGlYWEptYklwVGZtUlFWZF9uT21DMmk5WFdCeFVXSFNUMjRUcndvUEI5alRyVXJsOXdER3JROUR6OUVmTE02WDZMSTZQRE45aHYtV3lYd2tqLWRWUldiN25YeE9ZaHlkONIBlwFBVV95cUxNTFpobEVlemRLWktpLVVTTGRuczU4aF9kaVNOWk9EakFkOV9wNlYyTkFrN2RtQ2Uxb3dQWlRkZWRta0pVaUJUWUpTVDJpSEdkNHZqU19pUnJtOXJDaEpCcml5UW5SVnNXVVlBamdyTFI2NlM0cFVCVldmM0l4NFBTal9LWGRkQWF3V2ExWV8tclQweVZmWGZZ?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
