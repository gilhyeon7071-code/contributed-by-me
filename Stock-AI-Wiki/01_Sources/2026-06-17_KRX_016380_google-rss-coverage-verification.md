---
id: verification-2026-06-17-KRX-016380-google-rss-coverage
type: verification
title: KRX 016380 Google RSS Coverage Verification
created: 2026-06-17
updated: 2026-06-17
status: verification
stage: 1

market: KRX
ticker: "016380"
company: KG스틸
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-17

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=016380
    - name=KG스틸
    - naver_article_count=0
    - google_rss_article_count=2
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 016380
    - KG스틸
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

# KRX 016380 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-17_KRX_016380_google-rss-coverage-source]]

## Facts Checked
- `code=016380`
- `name=KG스틸`
- `naver_article_count=0`
- `google_rss_article_count=2`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `KG스틸S&D·S&I 통합경영 돌입…'운영 효율·소통 강화' - 뉴시스`
- Source: `뉴시스`
- Published at: `2026-06-01T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFAxWnpMTmRMVUt0OEdPc3pKMkpWQmJEZ1M5MjI4ZW8zaTlsN0kzSGFPbEREaFlOZUFkcDZnMUtrUE9KRkFEWW1hM1JHcV9QZ3R0SjVRaWwyR3pLRlAyRmM4d9IBeEFVX3lxTFBnQXhaZnFlUkVUUi1RZWJHNC1fODJpMjVvMmZtSzU3Y2kyRktoUEJ4dVZ6OXdQODRGeXJfVjdXQ2VfTWd6TGx6V2ZjVS1tM2pIbnA2eDQzeTJvSkJnV052UlVPcC1VNEcyQ2FyUDFtbDNVZzhfS1dnRA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:21:46+09:00`
- Company: [[KRX_016380_KG스틸]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-17.json`
- Latest observation title: `KG스틸S&D·S&I 통합경영 돌입…'운영 효율·소통 강화' - 뉴시스`
- Latest observation source: `뉴시스`
- Latest observation published_at: `2026-06-01T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFAxWnpMTmRMVUt0OEdPc3pKMkpWQmJEZ1M5MjI4ZW8zaTlsN0kzSGFPbEREaFlOZUFkcDZnMUtrUE9KRkFEWW1hM1JHcV9QZ3R0SjVRaWwyR3pLRlAyRmM4d9IBeEFVX3lxTFBnQXhaZnFlUkVUUi1RZWJHNC1fODJpMjVvMmZtSzU3Y2kyRktoUEJ4dVZ6OXdQODRGeXJfVjdXQ2VfTWd6TGx6V2ZjVS1tM2pIbnA2eDQzeTJvSkJnV052UlVPcC1VNEcyQ2FyUDFtbDNVZzhfS1dnRA?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
