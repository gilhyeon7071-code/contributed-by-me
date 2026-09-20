---
id: verification-2026-08-08-KRX-214450-google-rss-coverage
type: verification
title: KRX 214450 Google RSS Coverage Verification
created: 2026-08-08
updated: 2026-08-08
status: verification
stage: 1

market: KRX
ticker: "214450"
company: 파마리서치
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=214450
    - name=파마리서치
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=12
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 214450
    - 파마리서치
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

# KRX 214450 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-08_KRX_214450_google-rss-coverage-source]]

## Facts Checked
- `code=214450`
- `name=파마리서치`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=12`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `파마리서치 2분기 영업이익·매출 '역대 최대'(종합) - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-08-07T13:50:16+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE45b0hvRXBxblRMMmQ3UWdveXJVM1d2Q3Y0WmJoemI3RWRvV05oM3lQXzNoQmMyYjZzdFFtekJ6b29waElna2VRNG16U3BPN3JMamc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:34:10+09:00`
- Company: [[KRX_214450_파마리서치]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-08.json`
- Latest observation title: `파마리서치, 리쥬란 타고 상반기 매출·영업익 ‘역대 최대’ - 팜이데일리`
- Latest observation source: `팜이데일리`
- Latest observation published_at: `2026-08-07T14:31:12+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMihAFBVV95cUxPeDR3YXhHN1lzNXFZbXJ2OUxRRkpLYVJLNzY3dUNrV0c4VVdZVENRdFliMDRNNWRVRU9URFpBUWJVYUt2d3NRVEp3Y1RCbFFER2JsUXZiLXBGaWNvVEctcllLLVJFclRZMFIyRi15d3lISDBnam5Bb2RUVWh6bU9vSHlkcms?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_medical-cooperation_의료-협진]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
