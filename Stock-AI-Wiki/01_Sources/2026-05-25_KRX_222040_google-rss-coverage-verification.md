---
id: verification-2026-05-25-KRX-222040-google-rss-coverage
type: verification
title: KRX 222040 Google RSS Coverage Verification
created: 2026-05-25
updated: 2026-05-25
status: verification
stage: 1

market: KRX
ticker: "222040"
company: 코스맥스엔비티
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=222040
    - name=코스맥스엔비티
    - naver_article_count=1
    - google_rss_article_count=6
    - kis_title_count=4
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 222040
    - 코스맥스엔비티
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

# KRX 222040 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-25_KRX_222040_google-rss-coverage-source]]

## Facts Checked
- `code=222040`
- `name=코스맥스엔비티`
- `naver_article_count=1`
- `google_rss_article_count=6`
- `kis_title_count=4`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `코스맥스엔비티, +11.70% 52주 신고가 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-05-22T09:14:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMilwFBVV95cUxOZjRBU1pzdGNzUk9GeEVyRTVTYklrNC1HcklUMWw2WkJ6Y1RfOHpINW5XeC01NEZPaENaYjJGNkNwS0hHZl83ZkRIenJCdndBV0hLYlExTGpZdzRDOUQ3ZjlieWUycFlMNDdyeHJUZmpPU3hVM1E0OC1KMW9teHdoSS0taWhKZ2lwa1J3MV9sbFFZZEt2NDZr0gGXAUFVX3lxTE5mNEFTWnN0Y3NST0Z4RXJFNVNiSWs0LUdySVQxbDZaQnpjVF84ekg1bld4LTU0Rk9oQ1piMkY2Q3BLSEdmXzdmREh6ckJ2d0FXSEtiUTFMall3NEM5RDdmOWJ5ZTJwWUw0N3J4clRmak9TeFUzUTQ4LUoxb214d2hJLS1paEpnaXBrUncxX2xsUVlkS3Y0Nms?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-25T21:05:04+09:00`
- Company: [[KRX_222040_코스맥스엔비티]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-25.json`
- Latest observation title: `코스맥스엔비티, +11.70% 52주 신고가 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-05-22T09:14:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMilwFBVV95cUxOZjRBU1pzdGNzUk9GeEVyRTVTYklrNC1HcklUMWw2WkJ6Y1RfOHpINW5XeC01NEZPaENaYjJGNkNwS0hHZl83ZkRIenJCdndBV0hLYlExTGpZdzRDOUQ3ZjlieWUycFlMNDdyeHJUZmpPU3hVM1E0OC1KMW9teHdoSS0taWhKZ2lwa1J3MV9sbFFZZEt2NDZr0gGXAUFVX3lxTE5mNEFTWnN0Y3NST0Z4RXJFNVNiSWs0LUdySVQxbDZaQnpjVF84ekg1bld4LTU0Rk9oQ1piMkY2Q3BLSEdmXzdmREh6ckJ2d0FXSEtiUTFMall3NEM5RDdmOWJ5ZTJwWUw0N3J4clRmak9TeFUzUTQ4LUoxb214d2hJLS1paEpnaXBrUncxX2xsUVlkS3Y0Nms?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
