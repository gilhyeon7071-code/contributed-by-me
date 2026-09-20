---
id: verification-2026-06-24-KRX-017900-google-rss-coverage
type: verification
title: KRX 017900 Google RSS Coverage Verification
created: 2026-06-24
updated: 2026-06-24
status: verification
stage: 1

market: KRX
ticker: "017900"
company: 광전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-24

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=017900
    - name=광전자
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 017900
    - 광전자
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

# KRX 017900 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-24_KRX_017900_google-rss-coverage-source]]

## Facts Checked
- `code=017900`
- `name=광전자`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `대한광통신·서울반도체·한미반도체 강세, 광전자는 하락…광통신·광반도체주, AI 인프라 기대감 지속 - CBC뉴스`
- Source: `CBC뉴스`
- Published at: `2026-06-24T10:42:56+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5XdkNYd2d3dWEybkJnMl8wckRZY1hYMkF1VGZELVZzZXRZaG9Ddi1TamJEU25PNWtzMVFmQ2w0dUdOTVpLS2x5UUlBT1N6MmU3OHdDTVV3TEhtelg1N1ZzQlBBMEFtcXcz?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:23:59+09:00`
- Company: [[KRX_017900_광전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-24.json`
- Latest observation title: `대한광통신·서울반도체·한미반도체 강세, 광전자는 하락…광통신·광반도체주, AI 인프라 기대감 지속 - CBC뉴스`
- Latest observation source: `CBC뉴스`
- Latest observation published_at: `2026-06-24T10:42:56+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5XdkNYd2d3dWEybkJnMl8wckRZY1hYMkF1VGZELVZzZXRZaG9Ddi1TamJEU25PNWtzMVFmQ2w0dUdOTVpLS2x5UUlBT1N6MmU3OHdDTVV3TEhtelg1N1ZzQlBBMEFtcXcz?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
