---
id: verification-2026-07-16-KRX-087010-google-rss-coverage
type: verification
title: KRX 087010 Google RSS Coverage Verification
created: 2026-07-16
updated: 2026-07-16
status: verification
stage: 1

market: KRX
ticker: "087010"
company: 펩트론
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-16

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=087010
    - name=펩트론
    - naver_article_count=2
    - google_rss_article_count=1
    - kis_title_count=3
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 087010
    - 펩트론
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

# KRX 087010 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-16_KRX_087010_google-rss-coverage-source]]

## Facts Checked
- `code=087010`
- `name=펩트론`
- `naver_article_count=2`
- `google_rss_article_count=1`
- `kis_title_count=3`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[바이오 공시 추적기] 펩트론·삼천당…흔들린 신뢰, 엇갈린 대응 - 데일리한국`
- Source: `데일리한국`
- Published at: `2026-07-14T16:54:34+09:00`
- Link: `https://news.google.com/rss/articles/CBMib0FVX3lxTFBORVZYRzhiNG1KQ1JUMWRUaHVaSzd1alJrUjJPdldUOFE3N1I0VjFkMDNzX2pubE9YazNNUDNOWmFQS2lkTGhWX3p6dDllQTFJdGx4M1JsdkNHbHluRmVUdVh4NWdyamg5MVp2bkYwa9IBc0FVX3lxTFBGVWYzRzVWb1RSd3lIdWVKUXF1WGF1WkFGNmJ3akV2Y3o0NXFXMnhHTXhSb2E1aVNzbHFScWtpSURzZ1I5NVduTG5MR3J0czU0UGQzSmVkY0pFenFoczc2NUFVODVUaldsT256ZUZZcWNQeHc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:29:40+09:00`
- Company: [[KRX_087010_펩트론]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-16.json`
- Latest observation title: `[바이오 공시 추적기] 펩트론·삼천당…흔들린 신뢰, 엇갈린 대응 - 데일리한국`
- Latest observation source: `데일리한국`
- Latest observation published_at: `2026-07-14T16:54:34+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMib0FVX3lxTFBORVZYRzhiNG1KQ1JUMWRUaHVaSzd1alJrUjJPdldUOFE3N1I0VjFkMDNzX2pubE9YazNNUDNOWmFQS2lkTGhWX3p6dDllQTFJdGx4M1JsdkNHbHluRmVUdVh4NWdyamg5MVp2bkYwa9IBc0FVX3lxTFBGVWYzRzVWb1RSd3lIdWVKUXF1WGF1WkFGNmJ3akV2Y3o0NXFXMnhHTXhSb2E1aVNzbHFScWtpSURzZ1I5NVduTG5MR3J0czU0UGQzSmVkY0pFenFoczc2NUFVODVUaldsT256ZUZZcWNQeHc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
