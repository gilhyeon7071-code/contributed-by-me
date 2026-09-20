---
id: verification-2026-06-07-KRX-036170-google-rss-coverage
type: verification
title: KRX 036170 Google RSS Coverage Verification
created: 2026-06-07
updated: 2026-06-07
status: verification
stage: 1

market: KRX
ticker: "036170"
company: 에이치엠넥스
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
    - code=036170
    - name=에이치엠넥스
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 036170
    - 에이치엠넥스
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

# KRX 036170 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-07_KRX_036170_google-rss-coverage-source]]

## Facts Checked
- `code=036170`
- `name=에이치엠넥스`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `[고래사냥] '태성·에이치엠넥스·한진칼! 내일장 고래 종목은?! - MTN 머니투데이방송`
- Source: `MTN 머니투데이방송`
- Published at: `2026-06-05T06:31:56+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZEFVX3lxTFBuXzhHR1JIZGo4bEsxMGx2c0drTTkwMHUxSzFGd2x4NVRCcW83YVkxbjB3eG5YejR4eDFjRjJhUGRFQndvU3R0b3pHTXB3UWlzSUV0R181UWE2bnNYX0l2SVo5SGQ?oc=5`

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
- Company: [[KRX_036170_에이치엠넥스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-07.json`
- Latest observation title: `[고래사냥] '태성·에이치엠넥스·한진칼! 내일장 고래 종목은?! - MTN 머니투데이방송`
- Latest observation source: `MTN 머니투데이방송`
- Latest observation published_at: `2026-06-05T06:31:56+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiZEFVX3lxTFBuXzhHR1JIZGo4bEsxMGx2c0drTTkwMHUxSzFGd2x4NVRCcW83YVkxbjB3eG5YejR4eDFjRjJhUGRFQndvU3R0b3pHTXB3UWlzSUV0R181UWE2bnNYX0l2SVo5SGQ?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
