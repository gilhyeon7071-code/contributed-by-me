---
id: verification-2026-05-20-KRX-383800-google-rss-coverage
type: verification
title: KRX 383800 Google RSS Coverage Verification
created: 2026-05-20
updated: 2026-05-20
status: verification
stage: 1

market: KRX
ticker: "383800"
company: LX홀딩스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-20

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=383800
    - name=LX홀딩스
    - naver_article_count=1
    - google_rss_article_count=4
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 383800
    - LX홀딩스
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

# KRX 383800 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-20_KRX_383800_google-rss-coverage-source]]

## Facts Checked
- `code=383800`
- `name=LX홀딩스`
- `naver_article_count=1`
- `google_rss_article_count=4`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LX홀딩스, 신사옥 효과 본격화…수익 다변화 물꼬 - 데일리한국`
- Source: `데일리한국`
- Published at: `2026-05-18T15:58:30+09:00`
- Link: `https://news.google.com/rss/articles/CBMib0FVX3lxTE9IS0tWVndndUsyZkZaTHNTTEZBM01ZeTN5MjJsQzZuS2sya3pXWXlFVEprWXlBLXVSZGxtd0JXcmxBd0JQNmlJZWswZVUzWHJ0blRZT0JqVTZzRi1Eb2d1RlI3S2QtOFdpMVVQSUVXNNIBc0FVX3lxTFBkZmZfRXVXazYwN0dNamo4N3dIWGk1VnFqdFQ0RExhNGgxUHN2SllXOGxvV0dXa2hENUVjYkFBVkswM3Jac3FlUDhOR1NZUE5jU1dGMjNjSTdBNXI5cE9RWkYwQTlsdHlqZW9Lbl9BdkVZbHM?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-20T16:05:04+09:00`
- Company: [[KRX_383800_LX홀딩스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-20.json`
- Latest observation title: `LX홀딩스, 신사옥 효과 본격화…수익 다변화 물꼬 - 데일리한국`
- Latest observation source: `데일리한국`
- Latest observation published_at: `2026-05-18T15:58:30+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMib0FVX3lxTE9IS0tWVndndUsyZkZaTHNTTEZBM01ZeTN5MjJsQzZuS2sya3pXWXlFVEprWXlBLXVSZGxtd0JXcmxBd0JQNmlJZWswZVUzWHJ0blRZT0JqVTZzRi1Eb2d1RlI3S2QtOFdpMVVQSUVXNNIBc0FVX3lxTFBkZmZfRXVXazYwN0dNamo4N3dIWGk1VnFqdFQ0RExhNGgxUHN2SllXOGxvV0dXa2hENUVjYkFBVkswM3Jac3FlUDhOR1NZUE5jU1dGMjNjSTdBNXI5cE9RWkYwQTlsdHlqZW9Lbl9BdkVZbHM?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_holding-company_지주회사]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
