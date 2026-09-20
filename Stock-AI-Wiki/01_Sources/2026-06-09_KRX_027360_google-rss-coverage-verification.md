---
id: verification-2026-06-09-KRX-027360-google-rss-coverage
type: verification
title: KRX 027360 Google RSS Coverage Verification
created: 2026-06-09
updated: 2026-06-09
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
  collected_at: 2026-06-09

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=027360
    - name=아주IB투자
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
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
- [[2026-06-09_KRX_027360_google-rss-coverage-source]]

## Facts Checked
- `code=027360`
- `name=아주IB투자`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[VC 밸류업 진단] 아주IB투자, 임원 줄매도…블록딜 단가 20% 낮아져 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-06-08T10:37:33+09:00`
- Link: `https://news.google.com/rss/articles/CBMiS0FVX3lxTE5PX1N5WVdWbmw3c3RSYmI1bEhFc2VsVXdIblVUU1BWN1NsVzN1OXhWTGwtYms1dU55OHZjTG0zdWxOZVlBSzhiV3dVTQ?oc=5`

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
- Company: [[KRX_027360_아주IB투자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-09.json`
- Latest observation title: `청산 펀드 쌓인 아주IB투자, 성과보수 효과 가시화 - 톱데일리`
- Latest observation source: `톱데일리`
- Latest observation published_at: `2026-06-09T08:38:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUEFVX3lxTE5qOEtjazc5RHQ3bFI1OXRrUmNkV1J1YmNncDNSMjN5OThnUjQ2VHlBR3NjTFFzMTVFYlU5Mkd0TTd4NHlvUi1MTXFhVGhQY3Zt?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
