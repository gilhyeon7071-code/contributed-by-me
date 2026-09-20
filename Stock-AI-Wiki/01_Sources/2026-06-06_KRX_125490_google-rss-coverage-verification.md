---
id: verification-2026-06-06-KRX-125490-google-rss-coverage
type: verification
title: KRX 125490 Google RSS Coverage Verification
created: 2026-06-06
updated: 2026-06-06
status: verification
stage: 1

market: KRX
ticker: "125490"
company: 한라캐스트
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-06

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=125490
    - name=한라캐스트
    - naver_article_count=0
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=False
  related_entities:
    - KRX 125490
    - 한라캐스트
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

# KRX 125490 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-06_KRX_125490_google-rss-coverage-source]]

## Facts Checked
- `code=125490`
- `name=한라캐스트`
- `naver_article_count=0`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=False`

## RSS Item Metadata Checked
- Title: `한라캐스트, 글로벌 고객사 휴머노이드 로봇 '핵심 기업' 등극…"발열·무게 잡은 기술 주목" - 프라임경제`
- Source: `프라임경제`
- Published at: `2026-05-07T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMibEFVX3lxTE1wVU1rUHFyckptQWtySk1hYlhlSjVJUXJ5b3VIbFZuMHZRcG8zQkNGZFczY0dacmFEMEtjX2VWWnRHTmlKTXpsYWJfRml2cDFrQktkLXZqT1BYdzRTaEFzWFloWThOZzA0OXY2cw?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:18:37+09:00`
- Company: [[KRX_125490_한라캐스트]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-06.json`
- Latest observation title: `한라캐스트, 글로벌 고객사 휴머노이드 로봇 '핵심 기업' 등극…"발열·무게 잡은 기술 주목" - 프라임경제`
- Latest observation source: `프라임경제`
- Latest observation published_at: `2026-05-07T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTE1wVU1rUHFyckptQWtySk1hYlhlSjVJUXJ5b3VIbFZuMHZRcG8zQkNGZFczY0dacmFEMEtjX2VWWnRHTmlKTXpsYWJfRml2cDFrQktkLXZqT1BYdzRTaEFzWFloWThOZzA0OXY2cw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_exports_수출]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
