---
id: verification-2026-08-13-KRX-174900-google-rss-coverage
type: verification
title: KRX 174900 Google RSS Coverage Verification
created: 2026-08-13
updated: 2026-08-13
status: verification
stage: 1

market: KRX
ticker: "174900"
company: 앱클론
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-13

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=174900
    - name=앱클론
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 174900
    - 앱클론
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

# KRX 174900 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-13_KRX_174900_google-rss-coverage-source]]

## Facts Checked
- `code=174900`
- `name=앱클론`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[시선강탈] 앱클론 vs 비에이치 vs 실리콘투, 공략법은? - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-08-12T07:08:37+09:00`
- Link: `https://news.google.com/rss/articles/CBMibkFVX3lxTFBZV0xxRG1JYjBTMVpEOUlHcGdkNXpvMWt3U0lJTjVsSmZpV3Nya256RFNBZHR5YVpWRzdTZmE5ODUxLVgwcUdBRkQ5OUZEYlBUYTdkNGF4SUhXRTgxU19WOExvc2hDQ0RBam92TEhR0gFuQVVfeXFMUFlXTHFEbUliMFMxWkQ5SUdwZ2Q1em8xa3dTSUlONWxKZmlXc3JrbnpEU0FkdHlhWlZHN1NmYTk4NTEtWDBxR0FGRDk5RkRiUFRhN2Q0YXhJSFdFODFTX1Y4TG9zaENDREFqb3ZMSFE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-13T15:06:42+09:00`
- Company: [[KRX_174900_앱클론]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-13.json`
- Latest observation title: `앱클론, HER2 표적 항체 ‘AC101’ 위암 글로벌 3상 내년 상반기 톱라인 발표 - 더바이오`
- Latest observation source: `더바이오`
- Latest observation published_at: `2026-08-13T10:15:49+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTE9QUXJZbjYyWGcwYVZ4WGItbFZOZmJqNDdBVjNGRENMRWUwazNyOFFUQ0sxZzJkbHc0d3Z1bmhrR3IzU1FjM2hZV1BPYV9hWjhpQzEtcUlKYjlFYVNBNm5RMmVSVktmX3FfT1Z2V9IBcEFVX3lxTE5yNFNucWRKSmEzdjh5NTF4QlN0Z2RWTGVMRjVUM1U2b29DVE4wSE5mOVNsN21wNG5MaVpCXzZMblRYT2RUWFFOUDBPOUJncjhTUFdnOTFsUXNINXBsRHVTWWxtX0pDOGhqTnhZUUxXbUI?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
