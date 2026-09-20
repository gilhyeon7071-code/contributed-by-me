---
id: verification-2026-06-05-KRX-071050-google-rss-coverage
type: verification
title: KRX 071050 Google RSS Coverage Verification
created: 2026-06-05
updated: 2026-06-05
status: verification
stage: 1

market: KRX
ticker: "071050"
company: 한국금융지주
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
    - code=071050
    - name=한국금융지주
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 071050
    - 한국금융지주
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

# KRX 071050 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-05_KRX_071050_google-rss-coverage-source]]

## Facts Checked
- `code=071050`
- `name=한국금융지주`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `특징주, 한국금융지주-증권 테마 상승세에 8.18% ↑ - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-06-05T12:23:34+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE1nUXB3T0tZVF9UTDRaaFR0S0JyS2I2R0RxWlV4alMyU0R5ejM4czN5T3RMUTRMRUdsX1l1RVlUdnpCdjRhSDNiUEZuanVSX1dYdmc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:18:19+09:00`
- Company: [[KRX_071050_한국금융지주]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `금융권, 소비자보호 전문가 양성 및 역량 강화에 ‘한뜻’ - 한국보험신문`
- Latest observation source: `한국보험신문`
- Latest observation published_at: `2026-06-05T13:07:28+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE10QzYya0UzdXc2U2JoSGU1Mkg3a2tSRU53eGhNNEY5d3BXWXRMZDhkX2J3TjM2Nkx0bU1iRWtNelloQVl5aS1ZRzlfUUk5TkFRLUV2T3h1RFZZRk80MHZlVEhIcXdiMWhJNldz0gFvQVVfeXFMUFgxTEhub3kwQ3J1YmE3QmtPZEZQYmdFRll1Y0hRMWJyLTdrUURZWEx4ZGlXRGFlTDB1b2F3V19fb1Z3LTFBaVFLQTFMV3J2cEZXTXlpSm1WUDJJUkthM2NIVmZKVTQ1ZmRmSGR2aURV?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
