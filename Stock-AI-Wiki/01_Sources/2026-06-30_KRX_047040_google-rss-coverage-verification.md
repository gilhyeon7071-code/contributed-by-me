---
id: verification-2026-06-30-KRX-047040-google-rss-coverage
type: verification
title: KRX 047040 Google RSS Coverage Verification
created: 2026-06-30
updated: 2026-06-30
status: verification
stage: 1

market: KRX
ticker: "047040"
company: 대우건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-30

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=047040
    - name=대우건설
    - naver_article_count=11
    - google_rss_article_count=33
    - kis_title_count=15
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 047040
    - 대우건설
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

# KRX 047040 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-30_KRX_047040_google-rss-coverage-source]]

## Facts Checked
- `code=047040`
- `name=대우건설`
- `naver_article_count=11`
- `google_rss_article_count=33`
- `kis_title_count=15`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `대우건설, 하이엔드 주거 서비스 '써밋 컬처 살롱' 운영 - 뉴시스`
- Source: `뉴시스`
- Published at: `2026-06-29T11:17:03+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE5UZktHRnMxXzZXU0tqZ0pWMEJtaG9MazJJMmxra25xZERfRlRWdUhiMDNYMmxKQzlHOEJuaUJHcjNvd24xbmp1YTh2U29pWFlTOGJLcnRrUmxpa0puYktsTdIBeEFVX3lxTE41SnV6Z0htS2RSQjVLWGs1N3JyT0xrMVZYVEtmRjM2dFlBcjNVN2w3LUo2VXJuZXl4WDJLX3BrOTFIMDM4d25MeVhTYVpnVlUteXVjLWZFdktQUWJhMDhJTGd2WUdxMk5RZTlCWlduNkxYcGxVSmVfNQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-30T03:05:26+09:00`
- Company: [[KRX_047040_대우건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-30.json`
- Latest observation title: `대우건설, '써밋 컬처 살롱' 운영…하이엔드 주거 서비스 강화 - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-06-29T10:30:01+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTE12a3N3MVhKVC1pZ2dtR2NfOVByMVBxb3FaanV2bXNQcUhBbDdpWkdBWHJsNEhVSW9uTmItMjl3VWV1aTZOOEk0RVNVb3hJeVZ1N3JOSEVOWmhGdkYxeENTOXV5UThEMTlXLUHSAW9BVV95cUxNd1hXbGV3OHZGRjZieF9HbktLX2doU1FlYmUwSEdHZHNrV0E2d0t5Y29kWkk5VnYtNzBUYnRYSkZkQnh5TzFBc2xBVnJBdDdHLVNMMDJvU29Hd2hVTmV6ZXRONHE5aExCb20tRWVGNmM?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
