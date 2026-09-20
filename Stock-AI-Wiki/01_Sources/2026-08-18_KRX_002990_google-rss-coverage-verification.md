---
id: verification-2026-08-18-KRX-002990-google-rss-coverage
type: verification
title: KRX 002990 Google RSS Coverage Verification
created: 2026-08-18
updated: 2026-08-18
status: verification
stage: 1

market: KRX
ticker: "002990"
company: 금호건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-18

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=002990
    - name=금호건설
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 002990
    - 금호건설
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

# KRX 002990 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-18_KRX_002990_google-rss-coverage-source]]

## Facts Checked
- `code=002990`
- `name=금호건설`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `금호건설, 부채비율 개선했지만 현금흐름 악화한 이유 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-08-18T06:07:27+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFBQMkZtQXdvUW9Ldld6VHdjQTVteTZQdUFmYmIzQUNySG9JbEdTMWtScUwwS19MZmxxRVFndGlhNE1WemJwbTN3TWV6YnhESUU?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:36:51+09:00`
- Company: [[KRX_002990_금호건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-18.json`
- Latest observation title: `금호건설, 부채비율 개선했지만 현금흐름 악화한 이유 - 비즈워치`
- Latest observation source: `비즈워치`
- Latest observation published_at: `2026-08-18T06:06:02+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTFBrMGRnRWRfeVdjWkpqVEZFUFM5bWtxUmF2YTFaZ3lwaHkyQ3JfYkpqMWFTaVRDVVdSVkpubXEtTlJDc2hEM3BveEZvVTJ3d3dGWkxMaUpSODFuNkkxdVNBY1dyZ0xvamd2MEhnV09GSlE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
