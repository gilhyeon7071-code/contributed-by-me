---
id: verification-2026-07-24-KRX-034730-google-rss-coverage
type: verification
title: KRX 034730 Google RSS Coverage Verification
created: 2026-07-24
updated: 2026-07-24
status: verification
stage: 1

market: KRX
ticker: "034730"
company: SK
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-24

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=034730
    - name=SK
    - naver_article_count=10
    - google_rss_article_count=216
    - kis_title_count=37
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034730
    - SK
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

# KRX 034730 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-24_KRX_034730_google-rss-coverage-source]]

## Facts Checked
- `code=034730`
- `name=SK`
- `naver_article_count=10`
- `google_rss_article_count=216`
- `kis_title_count=37`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `'10년 유지' 합의했던 SK하이닉스 성과급…1년 만에 다시 협상 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-07-22T16:15:27+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFB2MzFENW1aa01RdTJ6NUpFbE5kNklsRmRibmZjYjlEUnhqSjN4RTM0MkRfUk5LdUd4b0ZyaXlFcWhLMGRONGE2cXNMNUZmVmc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:31:16+09:00`
- Company: [[KRX_034730_SK]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-24.json`
- Latest observation title: `“최태원 SK 주식도 재산 분할 대상…노소영에게 9440억원 지급” 판단 근거는? [뉴스분석] - 경향신문`
- Latest observation source: `경향신문`
- Latest observation published_at: `2026-07-24T18:44:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTFAtenQ0VWpoWDhQdElPVTJhek9obkl6WlUtcHByUzdfUVZYeU9OaUdBR0FuUG95eklZaGZPZXRYdDhPMFg1MV8tdG42UVVkQV9kZ2k1TjNEeFlIZ9IBX0FVX3lxTE1nNjZhaHRYYkZJUkpNUWNubmhGX3NUa2NDRFdZcUp2UW1Fb3hXNkhMTU42TjJCSGlMOWxTaGVhdExHSnItZEpRTzAwX0VwX0dySWh3ZzlPcThudzZoTVZn?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
