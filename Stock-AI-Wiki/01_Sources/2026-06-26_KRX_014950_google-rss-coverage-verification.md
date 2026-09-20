---
id: verification-2026-06-26-KRX-014950-google-rss-coverage
type: verification
title: KRX 014950 Google RSS Coverage Verification
created: 2026-06-26
updated: 2026-06-26
status: verification
stage: 1

market: KRX
ticker: "014950"
company: 삼익제약
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-26

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=014950
    - name=삼익제약
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 014950
    - 삼익제약
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

# KRX 014950 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-26_KRX_014950_google-rss-coverage-source]]

## Facts Checked
- `code=014950`
- `name=삼익제약`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `삼익제약 주주 하나증권, 삼익제약 주식등의 수 19만2303주 감소…총 지분율 4.82% - 디지털투데이`
- Source: `디지털투데이`
- Published at: `2026-06-25T17:35:03+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFB6bnhsNWZpTGhMU1c3aDd5cExycjBZY2pWSVdESkdHaFBFYmplYUFQRFB5akRYOW5CMmhRZjBkcDhGNjZ4ZVE3dEl4RXQzR0dGQjZ2ZFBhU3M3eWFZTE95UW5SbWhUNlpkLVlCTmYwLVFvb00?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:24:38+09:00`
- Company: [[KRX_014950_삼익제약]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-26.json`
- Latest observation title: `삼익제약, 5년 내 매출 2배 성장 목표…전문약·CMO·물류로 체질 개선 - 메디코파마`
- Latest observation source: `메디코파마`
- Latest observation published_at: `2026-06-24T07:43:09+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMickFVX3lxTFB1ZTEySVpERHpMVXl1cXMzSzhuNnlFT2FmUGFucVZvNWM5R3Nsb1FMNEdBcWZPVEppZGMyR2FWdnFNUzJZaXhxU3M5Qldwc0p5NlpvYlUwU2o2QUxxZERDRlBoaE9idU1tYmp0aDBwQTUtUQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
