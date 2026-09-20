---
id: verification-2026-06-01-KRX-083650-google-rss-coverage
type: verification
title: KRX 083650 Google RSS Coverage Verification
created: 2026-06-01
updated: 2026-06-01
status: verification
stage: 1

market: KRX
ticker: "083650"
company: 비에이치아이
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-01

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=083650
    - name=비에이치아이
    - naver_article_count=0
    - google_rss_article_count=6
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 083650
    - 비에이치아이
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

# KRX 083650 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-01_KRX_083650_google-rss-coverage-source]]

## Facts Checked
- `code=083650`
- `name=비에이치아이`
- `naver_article_count=0`
- `google_rss_article_count=6`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `신한울 이어 미국까지…비에이치아이, 원전 슈퍼사이클 진입하나 - 핀포인트뉴스`
- Source: `핀포인트뉴스`
- Published at: `2026-05-31T13:46:21+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBkNGJUYzZpRVJDTnp0TXZRQUd4eDVnQktiRDBHbTQ0RndXYlFVdG0zY0U4Y2d0Y0xyUXNmTXUtQnlpYmdRU1V0UU1SZ0N5RXY3WDRZVmJHWHZnT1k0Z05UdUZqVVZsSWc2NDZlWUVMd0VnWjDSAXdBVV95cUxQWE9pVW5lV2ZySUJNVFBPTEhWOW0zblV1MXNValJqRGZnM0R0ajNFdFpaYnBicTRwaDFVNEt1aGkyY3E1SWw2cXlxZDdhVmVGQVBScEVURTF3NkhLdlVRdUYwY25DUS1UUlBMeTZfTUJiTllGdEY3UQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-01T08:42:44+09:00`
- Company: [[KRX_083650_비에이치아이]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-01.json`
- Latest observation title: `신한울 이어 미국까지…비에이치아이, 원전 슈퍼사이클 진입하나 - 핀포인트뉴스`
- Latest observation source: `핀포인트뉴스`
- Latest observation published_at: `2026-05-31T13:46:21+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBkNGJUYzZpRVJDTnp0TXZRQUd4eDVnQktiRDBHbTQ0RndXYlFVdG0zY0U4Y2d0Y0xyUXNmTXUtQnlpYmdRU1V0UU1SZ0N5RXY3WDRZVmJHWHZnT1k0Z05UdUZqVVZsSWc2NDZlWUVMd0VnWjDSAXdBVV95cUxQWE9pVW5lV2ZySUJNVFBPTEhWOW0zblV1MXNValJqRGZnM0R0ajNFdFpaYnBicTRwaDFVNEt1aGkyY3E1SWw2cXlxZDdhVmVGQVBScEVURTF3NkhLdlVRdUYwY25DUS1UUlBMeTZfTUJiTllGdEY3UQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
