---
id: verification-2026-06-29-KRX-047040-google-rss-coverage
type: verification
title: KRX 047040 Google RSS Coverage Verification
created: 2026-06-29
updated: 2026-06-29
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
  collected_at: 2026-06-29

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
- [[2026-06-29_KRX_047040_google-rss-coverage-source]]

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
- Title: `대우건설, '써밋 컬처 살롱' 운영…하이엔드 주거 서비스 강화 - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-06-29T10:30:01+09:00`
- Link: `https://news.google.com/rss/articles/CBMiakFVX3lxTE12a3N3MVhKVC1pZ2dtR2NfOVByMVBxb3FaanV2bXNQcUhBbDdpWkdBWHJsNEhVSW9uTmItMjl3VWV1aTZOOEk0RVNVb3hJeVZ1N3JOSEVOWmhGdkYxeENTOXV5UThEMTlXLUHSAW9BVV95cUxNd1hXbGV3OHZGRjZieF9HbktLX2doU1FlYmUwSEdHZHNrV0E2d0t5Y29kWkk5VnYtNzBUYnRYSkZkQnh5TzFBc2xBVnJBdDdHLVNMMDJvU29Hd2hVTmV6ZXRONHE5aExCb20tRWVGNmM?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:25:16+09:00`
- Company: [[KRX_047040_대우건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-29.json`
- Latest observation title: `'장위 푸르지오 마크원' 특공에 5200명 몰려…평균 경쟁률 9.9대 1 - 뉴시스`
- Latest observation source: `뉴시스`
- Latest observation published_at: `2026-06-29T20:32:17+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFA3NWQ4WVhVX2E4ZTBiUVRhNjY0X2lNaGNLVUx3MlVMUldKeUE5c2IycV9WWkd0amdYNTBZR1h3UXNIblowUVNLbGhHc0w2WGc5UFhUeEhZNnl0QkkzbW1laNIBeEFVX3lxTE1uVThSSzNaMWlFLWRjT3cwSjlkbG9kNzhpYVJKS2tFTTJrcUU1TXM2VFI4YUJWNHNLa1FGRTZqTS1WNUVNT0Z6bGY3NEJObWZKODdsa3VhR3BfN3JLeXNyMjcyRmVzRTVGZUgyMjNkcTlBZE5Fc0dULQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
