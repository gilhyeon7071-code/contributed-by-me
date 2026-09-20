---
id: verification-2026-05-25-KRX-056360-google-rss-coverage
type: verification
title: KRX 056360 Google RSS Coverage Verification
created: 2026-05-25
updated: 2026-05-25
status: verification
stage: 1

market: KRX
ticker: "056360"
company: 코위버
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=056360
    - name=코위버
    - naver_article_count=2
    - google_rss_article_count=2
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 056360
    - 코위버
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

# KRX 056360 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-25_KRX_056360_google-rss-coverage-source]]

## Facts Checked
- `code=056360`
- `name=코위버`
- `naver_article_count=2`
- `google_rss_article_count=2`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `국내 광전송 1위 자존심… 코위버, 신기술로 무장하고 글로벌 진격 - 핀포인트뉴스`
- Source: `핀포인트뉴스`
- Published at: `2026-05-11T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBSSEIxNG1nLU9MNG1DZ3RBcWhBYlFwamN5SVFtWXhZRncwUldxc3hpbmNJOTEtcGZrX3BLYlNNMndFTzlrU0dBWC1RVFRZODBTVmNSRE5WNDd2OHBjcjBWZGpxQkFyOEFyYlBZS052aS14Z0HSAXdBVV95cUxOSmU4bWRBSVdYZW1Na1A2UWp4eXJIY2FhNWJaRVpLcC1hLWRONWtRcloyeDFZQzFNbk9xSEU4NjVPLURpTXJUdTJULW02VDBuYWRRaGVPVFVQRXhnbVV2YWFCdm1tUEFSSTk2YnJtdndKRTdTWUdPRQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-25T21:05:04+09:00`
- Company: [[KRX_056360_코위버]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `국내 광전송 1위 자존심… 코위버, 신기술로 무장하고 글로벌 진격 - 핀포인트뉴스`
- Latest observation source: `핀포인트뉴스`
- Latest observation published_at: `2026-05-11T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBSSEIxNG1nLU9MNG1DZ3RBcWhBYlFwamN5SVFtWXhZRncwUldxc3hpbmNJOTEtcGZrX3BLYlNNMndFTzlrU0dBWC1RVFRZODBTVmNSRE5WNDd2OHBjcjBWZGpxQkFyOEFyYlBZS052aS14Z0HSAXdBVV95cUxOSmU4bWRBSVdYZW1Na1A2UWp4eXJIY2FhNWJaRVpLcC1hLWRONWtRcloyeDFZQzFNbk9xSEU4NjVPLURpTXJUdTJULW02VDBuYWRRaGVPVFVQRXhnbVV2YWFCdm1tUEFSSTk2YnJtdndKRTdTWUdPRQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
