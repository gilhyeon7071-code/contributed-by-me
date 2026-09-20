---
id: verification-2026-06-07-KRX-082640-google-rss-coverage
type: verification
title: KRX 082640 Google RSS Coverage Verification
created: 2026-06-07
updated: 2026-06-07
status: verification
stage: 1

market: KRX
ticker: "082640"
company: 동양생명
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-07

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=082640
    - name=동양생명
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 082640
    - 동양생명
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

# KRX 082640 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-07_KRX_082640_google-rss-coverage-source]]

## Facts Checked
- `code=082640`
- `name=동양생명`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `[이달의 신상품] 동양생명, ‘7배 종신보험’ 출시…20년간 매년 30%↑ - fetv.co.kr`
- Source: `fetv.co.kr`
- Published at: `2026-06-06T06:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE9oLTU4Y00xUjBrdXBSTTNWa0xwMUJ2WmY1ZXdzUWFZQ3NxM3lfVHhwVFE3QTE0SHhHVEJrQlIweEVXTGNPY0RqYk1HeG9ibE1aRl9NeUZKUUt3bW8zd0lmVVdCY0VSUWdC?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:18:54+09:00`
- Company: [[KRX_082640_동양생명]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-07.json`
- Latest observation title: `우리금융, 동양생명 편입 막바지… 당국 "설명 보완" 요구에 "적극 소명" - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-06-03T08:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE54dy1iY0pGeU9LSzJKN20zN0lySnIwS0tfQ2lDVnBpV2VLNGlYdkg4cHdXUWxtM0MwTHoyZV9Mb2xLYlRrRnJkY0duLVdSVTBOOGlZR2xVWFRZMWVYVThOTEVLclFMekpaTVdv0gFwQVVfeXFMTTNnekhrZDViZVczTm1oQ1hHa25KUFJpN2p5YUZzLVNmZFl3WHVWQ19rUHBSZC1DTGJzazZoYzJ6eGtsSlVxR1RrY3FOcS1nUHduc3ZEeHJQdkhReHpnOExyemRaZVJ6VGhVakt5OW41dQ?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
