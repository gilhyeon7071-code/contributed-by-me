---
id: verification-2026-08-08-KRX-024840-google-rss-coverage
type: verification
title: KRX 024840 Google RSS Coverage Verification
created: 2026-08-08
updated: 2026-08-08
status: verification
stage: 1

market: KRX
ticker: "024840"
company: KBI메탈
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=024840
    - name=KBI메탈
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 024840
    - KBI메탈
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

# KRX 024840 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-08_KRX_024840_google-rss-coverage-source]]

## Facts Checked
- `code=024840`
- `name=KBI메탈`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[적중! 대박 예감] 'KBI메탈, 두산로보틱스' 내일장 예감 좋은 대박 종목은? - MTN 머니투데이방송`
- Source: `MTN 머니투데이방송`
- Published at: `2026-08-05T23:44:34+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZEFVX3lxTFBMaFBTSHVNU1l1N2lDRk5kdDM2WFNaSjhSVjQxS1QyNGFfWFlJY1ZHMkxTZ0tVSy1OS3o1ZXhiX01OS2x5eVpVSUtMUkxQd3VFUXVsT0JBbC1IVUJMWEhkbEJSc0Y?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-08T12:07:14+09:00`
- Company: [[KRX_024840_KBI메탈]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-08.json`
- Latest observation title: `[적중! 대박 예감] 'KBI메탈, 두산로보틱스' 내일장 예감 좋은 대박 종목은? - MTN 머니투데이방송`
- Latest observation source: `MTN 머니투데이방송`
- Latest observation published_at: `2026-08-05T23:44:34+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZEFVX3lxTFBMaFBTSHVNU1l1N2lDRk5kdDM2WFNaSjhSVjQxS1QyNGFfWFlJY1ZHMkxTZ0tVSy1OS3o1ZXhiX01OS2x5eVpVSUtMUkxQd3VFUXVsT0JBbC1IVUJMWEhkbEJSc0Y?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
