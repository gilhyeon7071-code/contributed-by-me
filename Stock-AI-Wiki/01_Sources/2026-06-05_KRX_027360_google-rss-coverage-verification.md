---
id: verification-2026-06-05-KRX-027360-google-rss-coverage
type: verification
title: KRX 027360 Google RSS Coverage Verification
created: 2026-06-05
updated: 2026-06-05
status: verification
stage: 1

market: KRX
ticker: "027360"
company: 아주IB투자
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
    - code=027360
    - name=아주IB투자
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 027360
    - 아주IB투자
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

# KRX 027360 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-05_KRX_027360_google-rss-coverage-source]]

## Facts Checked
- `code=027360`
- `name=아주IB투자`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `아주, 아주IB투자 지분 6% 팔아 ‘LP 실탄’ 마련할까 - 뉴스톱`
- Source: `뉴스톱`
- Published at: `2026-06-04T17:03:31+09:00`
- Link: `https://news.google.com/rss/articles/CBMib0FVX3lxTE1aTHJLRVJwb19nZUNUUHNaX0Jab3FvX3ZCb0ZDSC1VS0ptRjQ5dzZsb3p0Sk4zTUw2S1g0WUZzY3JDbHNtM1UxQ3o4NHQxR0IzT2RTLWlUZzdRR2Nhem5OLUV3ZFg4ZmRrYXNsaEhGWQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-05T21:10:20+09:00`
- Company: [[KRX_027360_아주IB투자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-05.json`
- Latest observation title: `아주IB투자, 스페이스X IPO 기대감 속 단기 급등 피로감에 3%대 약세 : 경제 - 재경일보`
- Latest observation source: `재경일보`
- Latest observation published_at: `2026-06-05T12:44:10+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiSEFVX3lxTFBBcWgyWVdkM0pkNU14bkNqWkJIcXZEc1lFY3pkNzQ2NzdVcXNyTS1HMDN6SzR3Y2FOREVLekJYWlpBeEpLYVRGWg?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
