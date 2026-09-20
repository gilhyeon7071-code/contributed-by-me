---
id: verification-2026-07-24-KRX-000250-google-rss-coverage
type: verification
title: KRX 000250 Google RSS Coverage Verification
created: 2026-07-24
updated: 2026-07-24
status: verification
stage: 1

market: KRX
ticker: "000250"
company: 삼천당제약
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
    - code=000250
    - name=삼천당제약
    - naver_article_count=4
    - google_rss_article_count=33
    - kis_title_count=10
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000250
    - 삼천당제약
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

# KRX 000250 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-24_KRX_000250_google-rss-coverage-source]]

## Facts Checked
- `code=000250`
- `name=삼천당제약`
- `naver_article_count=4`
- `google_rss_article_count=33`
- `kis_title_count=10`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼천당제약 "FDA Pre-ANDA 결과 바탕으로 日 사업 본격화" - 뉴스1`
- Source: `뉴스1`
- Published at: `2026-07-23T10:54:48+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE1VemkxbHVSYktpLUpNZ1g5dkJhVGlMc2FJek1NdGI3LUhVcUkxa2c1WlV4bXoxMTVEZndTTmlPUTVVSDJ5NjhGTlNnNzBSWkpBSTBhb0c3ZEN2bHZWU1pwRw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-24T21:05:14+09:00`
- Company: [[KRX_000250_삼천당제약]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-24.json`
- Latest observation title: `삼천당제약, 경구용 인슐린 후보물질 'SCD0503' 글로벌 임상1상 순항 - 더바이오`
- Latest observation source: `더바이오`
- Latest observation published_at: `2026-07-24T12:27:02+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTFBKU2x1NmxnNzJFUzB3anRjYTJsSFFES2lsM3NQMjJjenYzNmU5VXYtZGZRY19YQXpzTmVWWFNLNVY0NmtPTVJpMElDeE01TDIyM3FiUm5ZV0R5M215eU16SWE5Y1dVZWtvejJ6etIBcEFVX3lxTE5PeE5EV3lJTXA2QVlMLXo3Zkh6MDhnbDc0MC04UDgzd2pmRC1Zb2hiYTZCRUpPQUpoZ21QblNGdUxrY1pIeGVFSHNEMk10TmcxRk8wWmptWnNlX19LeXdSb2F1bFJVYnpKZkxhaHlDZGo?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
