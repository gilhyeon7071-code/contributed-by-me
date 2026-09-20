---
id: verification-2026-06-17-KRX-039200-google-rss-coverage
type: verification
title: KRX 039200 Google RSS Coverage Verification
created: 2026-06-17
updated: 2026-06-17
status: verification
stage: 1

market: KRX
ticker: "039200"
company: 오스코텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-17

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=039200
    - name=오스코텍
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 039200
    - 오스코텍
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

# KRX 039200 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-17_KRX_039200_google-rss-coverage-source]]

## Facts Checked
- `code=039200`
- `name=오스코텍`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `오스코텍, 바이오 USA 참가…항내성 항암제·신장 섬유화 신약 알린다 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-06-16T18:04:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMihwFBVV95cUxPY3R4dDFTWS0td3lhSkNxRjk5MGNTVVNsRDlZTkhhU2l1MzVhUGhHeWhmSEpmMlFldlRhMEtCZmNxVGEzZTlVSmN0aFRobl9Qc0o4NWVSTHpNYWZhanBpMDBWMDVfcTdtdlNUaDI4azF6TXdFakQ3MHhUenV0VFBLaEVLOG1sc1nSAZsBQVVfeXFMTWJtR1lOcHdiN0ppNk8ybThadkFvLXVkTHIwcF82UzlOWWNiUDA4c29MVlZ6aDc4MkJuaWt6S2Fyc3MzRElTN2JVaU1CdFNOSzA5cXB3YTFDQXVISnAyT1hPWm42bzd3Y3NiQk1yNG90S1NPVnJyNFN0NEl1cHU5bm1MeU9FNjQzSk94S1R3TFlhNkFSZTQzODhyc28?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:21:46+09:00`
- Company: [[KRX_039200_오스코텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-17.json`
- Latest observation title: `오스코텍, 1조원 규모 기술수출 '세비도플레닙' 선급금 수령 - 약업신문`
- Latest observation source: `약업신문`
- Latest observation published_at: `2026-06-17T16:26:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTE4yZzR0ZGsycEctWVhQU0ZybzZZd0REMDVGLVFoa2ZKcm5PTGtkZEdtVTdGZHhYNWV2clp5R2tfdDRoY1hacnVERXZ6S2JWRkp2OFMxQ19wWERoeUFvSjdDQ2Q5dXYxQWg1R0hWN2RjLUpYZw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
