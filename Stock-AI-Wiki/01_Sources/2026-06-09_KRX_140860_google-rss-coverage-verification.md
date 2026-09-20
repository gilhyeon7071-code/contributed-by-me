---
id: verification-2026-06-09-KRX-140860-google-rss-coverage
type: verification
title: KRX 140860 Google RSS Coverage Verification
created: 2026-06-09
updated: 2026-06-09
status: verification
stage: 1

market: KRX
ticker: "140860"
company: 파크시스템스
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
    - code=140860
    - name=파크시스템스
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 140860
    - 파크시스템스
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

# KRX 140860 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-09_KRX_140860_google-rss-coverage-source]]

## Facts Checked
- `code=140860`
- `name=파크시스템스`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `파크시스템스, 1분기 영업익 84%↓...1000억원 투자 선제적 증설 - 금융소비자뉴스`
- Source: `금융소비자뉴스`
- Published at: `2026-06-08T08:34:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE03YVl1Nm9MWnFrWnM4X1Q0eHl3UktkUGlfSXdFcFBHT3RXbjJJZXpkS2sxR2U2dk40cFlrOHdWVklCdHFEVG5fUGV2LXFFeG0zRlItckFZUjd5SjltR0pJTGUtYVA5MlpyM2c?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-09T08:05:10+09:00`
- Company: [[KRX_140860_파크시스템스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-09.json`
- Latest observation title: `파크시스템스, 올해 연간 실적 상저하고 예상…목표가↑-iM - 이데일리`
- Latest observation source: `이데일리`
- Latest observation published_at: `2026-06-09T07:45:25+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMigAFBVV95cUxPQzl4bmNzM2l1M1U4Rm00VWdWZ0VUVzdsalBrQ2U0UEJLUTA1c0NGVXJXNjl0VEQwTzdIWGhxdTlWZnk1U1FZZFhiZm92MThfRnhTSUVPcVIzTmV6TnlpT2g0RVZSNmlkS0dYUXJNTUluQWp6cFh0MHEtNEN5aWp5aQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
