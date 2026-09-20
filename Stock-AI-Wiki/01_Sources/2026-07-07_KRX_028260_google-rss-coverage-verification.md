---
id: verification-2026-07-07-KRX-028260-google-rss-coverage
type: verification
title: KRX 028260 Google RSS Coverage Verification
created: 2026-07-07
updated: 2026-07-07
status: verification
stage: 1

market: KRX
ticker: "028260"
company: 삼성물산
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-07

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=028260
    - name=삼성물산
    - naver_article_count=6
    - google_rss_article_count=77
    - kis_title_count=35
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 028260
    - 삼성물산
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

# KRX 028260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-07_KRX_028260_google-rss-coverage-source]]

## Facts Checked
- `code=028260`
- `name=삼성물산`
- `naver_article_count=6`
- `google_rss_article_count=77`
- `kis_title_count=35`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `래미안 원페를라 국제무대서 호평... 삼성물산 런던디자인 어워즈 금상 - 파이낸셜뉴스`
- Source: `파이낸셜뉴스`
- Published at: `2026-07-06T18:20:27+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9mZWVwREtYZDJ0R3Bscnc2WFQyTFNYNGNNNE9TbWxDWnVybXpQbjNqMkJRTEJKVmNmcTR1eER1ZXRyNjJWUmJIbF9aSHF2S0syZzg2cy1yalhpQQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-07T08:05:22+09:00`
- Company: [[KRX_028260_삼성물산]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-07.json`
- Latest observation title: `래미안 원페를라 국제무대서 호평... 삼성물산 런던디자인 어워즈 금상 - 파이낸셜뉴스`
- Latest observation source: `파이낸셜뉴스`
- Latest observation published_at: `2026-07-06T18:20:27+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE9mZWVwREtYZDJ0R3Bscnc2WFQyTFNYNGNNNE9TbWxDWnVybXpQbjNqMkJRTEJKVmNmcTR1eER1ZXRyNjJWUmJIbF9aSHF2S0syZzg2cy1yalhpQQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
