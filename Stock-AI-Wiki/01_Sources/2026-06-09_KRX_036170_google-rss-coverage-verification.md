---
id: verification-2026-06-09-KRX-036170-google-rss-coverage
type: verification
title: KRX 036170 Google RSS Coverage Verification
created: 2026-06-09
updated: 2026-06-09
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
  collected_at: 2026-06-09

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
- [[2026-06-09_KRX_036170_google-rss-coverage-source]]

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
- Title: `[장중수급포착] 에이치엠넥스, 외국인/기관 동시 순매수… 주가 +8.67% - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-06-08T10:15:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5NakFFS3FFMTR1b1o5azZzYkV6RnpxZ2hmUXpyamFwRnJQbWx3Y2l2Mlh4Wnh4QjAtd1RFMFJUVHl5S3lDdFBpbnYzM3pWSTg2alc3cExwQUJRQnJj?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:19:32+09:00`
- Company: [[KRX_036170_에이치엠넥스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-09.json`
- Latest observation title: `[고래사냥] '태성·에이치엠넥스·한진칼! 내일장 고래 종목은?! - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-06-05T06:32:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5vSnd2bVlXcHN1b3hsamxEZ2lab0pQV3JpMV9vaDB6ZU5YdW5DZFF5SzlILVRIdGxoVjB0aGV1N05GVEFNcXItNGFFVjk0OGc?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
