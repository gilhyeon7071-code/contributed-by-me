---
id: verification-2026-07-05-KRX-042700-google-rss-coverage
type: verification
title: KRX 042700 Google RSS Coverage Verification
created: 2026-07-05
updated: 2026-07-05
status: verification
stage: 1

market: KRX
ticker: "042700"
company: 한미반도체
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-05

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=042700
    - name=한미반도체
    - naver_article_count=1
    - google_rss_article_count=52
    - kis_title_count=16
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 042700
    - 한미반도체
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

# KRX 042700 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-05_KRX_042700_google-rss-coverage-source]]

## Facts Checked
- `code=042700`
- `name=한미반도체`
- `naver_article_count=1`
- `google_rss_article_count=52`
- `kis_title_count=16`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `곽동신 한미반도체 회장, 50억 자사주 추가 매입…3년간 695억원어치 사들여 - 한국경제`
- Source: `한국경제`
- Published at: `2026-07-02T14:39:28+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTE0zcXN3QlJsWmZjNXNNS0EyNEs4VEYtRUFHdTYteklZOHhwbzRaeHNpZGpCTnhfNDhPWGRrR2NvV2JXdzBVTksydm9vam1adENqWjF6STFkZWRCUdIBVEFVX3lxTE1rUWNSSlZERmdEbElWajdfU1lfVm02dzlqUEo4TVBoVGg5cG1GYWk1SVV2cENHNGhQbm0wbEl6YWtnb3FON3NpVjRaV0hzMXA0RU1FWQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:26:51+09:00`
- Company: [[KRX_042700_한미반도체]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-05.json`
- Latest observation title: `[1% 초고수의 선택] ’현대차’ 사고 ’한미반도체’ 팔았다 By EBN - Investing.com 한국어`
- Latest observation source: `Investing.com 한국어`
- Latest observation published_at: `2026-07-02T21:18:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTE1GRTNjbDY3NVlDaDR3NGlySDVta3l2elBTWVZLeGxkRHlEUnAwYWxvN0RWQmtUM2VZUGNsbzdRV3FiWjNjekN1WmpyNEI0SG9wZmhnbjI5Y2ZpNVhPcVpCNmFiNmlialllMENyNGI1XzI?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
