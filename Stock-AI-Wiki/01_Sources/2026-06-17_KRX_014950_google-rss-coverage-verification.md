---
id: verification-2026-06-17-KRX-014950-google-rss-coverage
type: verification
title: KRX 014950 Google RSS Coverage Verification
created: 2026-06-17
updated: 2026-06-17
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
  collected_at: 2026-06-17

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=014950
    - name=삼익제약
    - naver_article_count=0
    - google_rss_article_count=6
    - kis_title_count=11
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
- [[2026-06-17_KRX_014950_google-rss-coverage-source]]

## Facts Checked
- `code=014950`
- `name=삼익제약`
- `naver_article_count=0`
- `google_rss_article_count=6`
- `kis_title_count=11`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[특징주] 삼익제약, 탈모 정책 수혜 기대…上 - 아주경제`
- Source: `아주경제`
- Published at: `2026-06-15T14:21:05+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTFBkSWFqRHJUb3h1dlB6dUpZdEhqX2NZY00xMG1pRnN6cnVxY2RlaU84cXlNRUszamZYcGJ0Z2ZxWk5lcG55MXRfVGdjU1JuNmJTR1RobXhIUm1vQdIBWEFVX3lxTE8wZGY4TEl6QjZEOTJmX1ZSemVwUnhTS3gtWjQ3MTZ2aTI3QU4zYS14YWxGMURNdUNENTJnMUlWUDl3cjZ3MUpsS2t0LUpQbUI2RFh0OWFtdDk?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-06-17T08:05:04+09:00`
- Company: [[KRX_014950_삼익제약]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-17.json`
- Latest observation title: `[특징주] 삼익제약, 탈모 정책 수혜 기대…上 - 아주경제`
- Latest observation source: `아주경제`
- Latest observation published_at: `2026-06-15T14:21:05+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiWkFVX3lxTFBkSWFqRHJUb3h1dlB6dUpZdEhqX2NZY00xMG1pRnN6cnVxY2RlaU84cXlNRUszamZYcGJ0Z2ZxWk5lcG55MXRfVGdjU1JuNmJTR1RobXhIUm1vQdIBWEFVX3lxTE8wZGY4TEl6QjZEOTJmX1ZSemVwUnhTS3gtWjQ3MTZ2aTI3QU4zYS14YWxGMURNdUNENTJnMUlWUDl3cjZ3MUpsS2t0LUpQbUI2RFh0OWFtdDk?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
