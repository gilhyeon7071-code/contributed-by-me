---
id: verification-2026-05-21-KRX-085660-google-rss-coverage
type: verification
title: KRX 085660 Google RSS Coverage Verification
created: 2026-05-21
updated: 2026-05-21
status: verification
stage: 1

market: KRX
ticker: "085660"
company: 차바이오텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-21

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=085660
    - name=차바이오텍
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 085660
    - 차바이오텍
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

# KRX 085660 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-21_KRX_085660_google-rss-coverage-source]]

## Facts Checked
- `code=085660`
- `name=차바이오텍`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `차바이오텍, 연세대와 바이오 스타트업 육성 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-20T09:27:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMihwFBVV95cUxQODdxY21VV3N3ZzBMUTcxenZ6WFhwTzlvUTNmVm1BdHhrSHd3YUl0TkhqQVBLYktJMUVZWUtfSWQzbDlRV1JPZzQ3WWpTR2ZmQVJJdHlRaFNsNWlaV2h2dHJIaVNpbU52TnYxckx4N2JKclA5X3ZQd2JKVnY3UUk4ZXBncDc0NXfSAZsBQVVfeXFMTWVTOVhpSlNNczJ4bEhoUzhLaXpyZ2Etamx6eGtRd1lrNk81U0hxdnRkaWtrX0hXd3N6ZTZibWtib1RuQVZId1VWQ0lzUW5EZTdnRTZyaDAtV3VDTEhsX3BkSmVkMkl4a2FjSGV3Uy0xN2l5RXRoc0IxQ09KTC1YR3Nkc0p1ZjVRMFQ5N05rQW1fWERvTDRpcUJ0Tlk?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-21T15:05:23+09:00`
- Company: [[KRX_085660_차바이오텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-21.json`
- Latest observation title: `차바이오텍, 연세대와 바이오 스타트업 육성 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-20T09:27:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMihwFBVV95cUxQODdxY21VV3N3ZzBMUTcxenZ6WFhwTzlvUTNmVm1BdHhrSHd3YUl0TkhqQVBLYktJMUVZWUtfSWQzbDlRV1JPZzQ3WWpTR2ZmQVJJdHlRaFNsNWlaV2h2dHJIaVNpbU52TnYxckx4N2JKclA5X3ZQd2JKVnY3UUk4ZXBncDc0NXfSAZsBQVVfeXFMTWVTOVhpSlNNczJ4bEhoUzhLaXpyZ2Etamx6eGtRd1lrNk81U0hxdnRkaWtrX0hXd3N6ZTZibWtib1RuQVZId1VWQ0lzUW5EZTdnRTZyaDAtV3VDTEhsX3BkSmVkMkl4a2FjSGV3Uy0xN2l5RXRoc0IxQ09KTC1YR3Nkc0p1ZjVRMFQ5N05rQW1fWERvTDRpcUJ0Tlk?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
