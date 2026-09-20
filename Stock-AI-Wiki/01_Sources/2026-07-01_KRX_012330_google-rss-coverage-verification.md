---
id: verification-2026-07-01-KRX-012330-google-rss-coverage
type: verification
title: KRX 012330 Google RSS Coverage Verification
created: 2026-07-01
updated: 2026-07-01
status: verification
stage: 1

market: KRX
ticker: "012330"
company: 현대모비스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-01

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=012330
    - name=현대모비스
    - naver_article_count=1
    - google_rss_article_count=108
    - kis_title_count=14
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 012330
    - 현대모비스
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

# KRX 012330 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-01_KRX_012330_google-rss-coverage-source]]

## Facts Checked
- `code=012330`
- `name=현대모비스`
- `naver_article_count=1`
- `google_rss_article_count=108`
- `kis_title_count=14`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `프로농구 현대모비스, 프랜차이즈 스타 출신 함지훈 코치 선임 - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-06-30T10:48:07+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTFBEejhhOHVkLTNQb3NzeFpEQ1ZrUEFoMUhoNl8yRlAyNE5nTUg3YXlNcXNTUFlZbC1NNDRJeHdsWkJxa3k4ZUhkYkxicUJHSjhUOVRvcDNKWVVDYUHSAWBBVV95cUxNN2dBa2FCSWtNNTFRTmRPVEcwZFpnN2NMSVZ1NnVJTFhUTDRVeHJBSkNJSzV0RjBzVEdHZHJjT2ItNFlYVE5wZkpJazJwcGp3XzdRNHl3bVBMOWRTMGxOalg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:25:55+09:00`
- Company: [[KRX_012330_현대모비스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-01.json`
- Latest observation title: `임직원 아이디어에서 성장동력 찾는다… 현대모비스가 그리는 모빌리티 청사진 - 동아일보`
- Latest observation source: `동아일보`
- Latest observation published_at: `2026-07-01T14:24:25+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMidkFVX3lxTE9pYURXMF94WFpGLTROdU1lRC0wUmVPLUJteTgzbk5jTTh5ZDNTRmt2NDM1Z0NOU3FtZ3FMaEFVdFdrZThhZjlNTjBwck54Nmo4eUowVmIwTl9sUXF5clVWRzlQYU1fc0RUNjZjUDF1X1ZFcXoydUHSAWZBVV95cUxNX0ZoRVZac3d3dzVGQ25SVmJBRkJaMEJkaDFxbHY4U1pIeXB6WmhQb0stNFB4YXhGcWdjQjY2czN4b0F1R1JhS2NGQUNSSElmNC15Ri11bFNTT0YtOUdvZzBNUnBwZlE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
