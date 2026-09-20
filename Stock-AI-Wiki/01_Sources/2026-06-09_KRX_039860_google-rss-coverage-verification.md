---
id: verification-2026-06-09-KRX-039860-google-rss-coverage
type: verification
title: KRX 039860 Google RSS Coverage Verification
created: 2026-06-09
updated: 2026-06-09
status: verification
stage: 1

market: KRX
ticker: "039860"
company: 나노엔텍
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
    - code=039860
    - name=나노엔텍
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 039860
    - 나노엔텍
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

# KRX 039860 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-09_KRX_039860_google-rss-coverage-source]]

## Facts Checked
- `code=039860`
- `name=나노엔텍`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `나노엔텍, 0.00% VI 발동 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-08T13:51:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMigwFBVV95cUxQLUNpT1dfemYtemtKRTVMbmhGVHB4N3BnNVNmNlRrTWxjTUdOMmVtaFJxNkFCWlloaXRvWGUwMjhGdXBhRk1sR01Ub1hwVV9IQWhlNE9kR3k3WFRsVFRGajlXXzlrbjJKYm5jVWgyU3hVSF9UUEdDdDZRM3JtNWtIUFpENNIBlwFBVV95cUxORTNWVHhYakRBUE1uLXVSUjg3a3JVOWhpTWllTDdZWVV0NTdYeWREb2VhQ3p3REE2NmhSUGtsZ2RoekhPYWx0UnNkZDFiRVhuWms5dksxMUVfYzEtc1BTSDEyYkVpZ2JVWXdzbUxKWTdGSDNVdWdDamtqaWxHSUR3ZGxaWTM1MUp2STh5ZDBESjhfUC15SmRZ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-09T08:05:10+09:00`
- Company: [[KRX_039860_나노엔텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-09.json`
- Latest observation title: `나노엔텍, 0.00% VI 발동 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-06-08T13:51:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigwFBVV95cUxQLUNpT1dfemYtemtKRTVMbmhGVHB4N3BnNVNmNlRrTWxjTUdOMmVtaFJxNkFCWlloaXRvWGUwMjhGdXBhRk1sR01Ub1hwVV9IQWhlNE9kR3k3WFRsVFRGajlXXzlrbjJKYm5jVWgyU3hVSF9UUEdDdDZRM3JtNWtIUFpENNIBlwFBVV95cUxORTNWVHhYakRBUE1uLXVSUjg3a3JVOWhpTWllTDdZWVV0NTdYeWREb2VhQ3p3REE2NmhSUGtsZ2RoekhPYWx0UnNkZDFiRVhuWms5dksxMUVfYzEtc1BTSDEyYkVpZ2JVWXdzbUxKWTdGSDNVdWdDamtqaWxHSUR3ZGxaWTM1MUp2STh5ZDBESjhfUC15SmRZ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
