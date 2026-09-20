---
id: verification-2026-06-12-KRX-035250-google-rss-coverage
type: verification
title: KRX 035250 Google RSS Coverage Verification
created: 2026-06-12
updated: 2026-06-12
status: verification
stage: 1

market: KRX
ticker: "035250"
company: 강원랜드
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-12

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=035250
    - name=강원랜드
    - naver_article_count=1
    - google_rss_article_count=13
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 035250
    - 강원랜드
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

# KRX 035250 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-12_KRX_035250_google-rss-coverage-source]]

## Facts Checked
- `code=035250`
- `name=강원랜드`
- `naver_article_count=1`
- `google_rss_article_count=13`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `강원랜드 사장 선임 앞두고 도계 주민 반발 확산 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-06-11T11:13:35+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE9hRDJ3UVhKT1k5TmhJdlIzalhUNjhPNmh3WU1MYXJ4QXlDVmlXN3ZVWTM1SUlLUGhJUXk1NDB0bTJtcnVuejZVU2k4VVpjbjA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-12T08:05:08+09:00`
- Company: [[KRX_035250_강원랜드]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-12.json`
- Latest observation title: `“강원랜드 낙하산 인사 반대” 지역사회 성명 잇따라 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-06-11T15:06:51+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFB2bmhDR0NxdDMzeHhJSWNSdDF1MmVkRGUxTHhWcDFveXZYN2w2M0hXejUtQUszcm52WWdweXdYd21WckxEdUhCT0Rib2lvVTg?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
