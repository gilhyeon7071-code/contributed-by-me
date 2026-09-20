---
id: verification-2026-07-10-KRX-009540-google-rss-coverage
type: verification
title: KRX 009540 Google RSS Coverage Verification
created: 2026-07-10
updated: 2026-07-10
status: verification
stage: 1

market: KRX
ticker: "009540"
company: HD한국조선해양
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-10

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=009540
    - name=HD한국조선해양
    - naver_article_count=6
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 009540
    - HD한국조선해양
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

# KRX 009540 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-10_KRX_009540_google-rss-coverage-source]]

## Facts Checked
- `code=009540`
- `name=HD한국조선해양`
- `naver_article_count=6`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `한국조선해양(009540) - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-07-10T00:47:45+09:00`
- Link: `https://news.google.com/rss/articles/CBMilwFBVV95cUxOLXplWXJ3NEQyLTFRRkVkSFFfRXBGRE5qdkxFNFNhNldXTV9IMHdvMURpVE1GYzRlaUNIT2k5bXNFU3ljN1BxaHg2YjlheGdrU3FWMG00SURXbGRIREFyaVhPZ3YxeDUwWTRTdWVkUEUySmd5bzBwUFVObUpGSFlMU3Izd19PS25mUjZCQ0IxVlZMaU1EOGlB?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:28:24+09:00`
- Company: [[KRX_009540_HD한국조선해양]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-10.json`
- Latest observation title: `한국조선해양(009540) - 매일경제 마켓`
- Latest observation source: `매일경제 마켓`
- Latest observation published_at: `2026-07-10T00:47:45+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxOLXplWXJ3NEQyLTFRRkVkSFFfRXBGRE5qdkxFNFNhNldXTV9IMHdvMURpVE1GYzRlaUNIT2k5bXNFU3ljN1BxaHg2YjlheGdrU3FWMG00SURXbGRIREFyaVhPZ3YxeDUwWTRTdWVkUEUySmd5bzBwUFVObUpGSFlMU3Izd19PS25mUjZCQ0IxVlZMaU1EOGlB?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
