---
id: verification-2026-07-08-KRX-006800-google-rss-coverage
type: verification
title: KRX 006800 Google RSS Coverage Verification
created: 2026-07-08
updated: 2026-07-08
status: verification
stage: 1

market: KRX
ticker: "006800"
company: 미래에셋증권
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-08

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=006800
    - name=미래에셋증권
    - naver_article_count=6
    - google_rss_article_count=75
    - kis_title_count=31
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 006800
    - 미래에셋증권
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

# KRX 006800 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-08_KRX_006800_google-rss-coverage-source]]

## Facts Checked
- `code=006800`
- `name=미래에셋증권`
- `naver_article_count=6`
- `google_rss_article_count=75`
- `kis_title_count=31`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[단독] 미래에셋, 스페이스X 청약 무산에 개인에게 14억2천만원 보상 검토 - 조선일보`
- Source: `조선일보`
- Published at: `2026-07-06T18:30:37+09:00`
- Link: `https://news.google.com/rss/articles/CBMiiwFBVV95cUxPaVBpU01YdHotTVhpbkRZX2pzaE5CVEJUb0VaLUh6SkNqZXk3bUpGUWMyMWk2dzFXY0hrZ3dtRFJDY3RqLW9CZUE1N2NySS1xaEZ5VkxSTVNQbjFMbzk1RHRhcDBOdEtUMnZ3V09uUGZodjliN3VLQlpoUTNlZUE0RTRPUTBiQ3U0Ym9v?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-08T08:05:11+09:00`
- Company: [[KRX_006800_미래에셋증권]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-08.json`
- Latest observation title: `[단독] 미래에셋, 스페이스X 청약 무산에 개인에게 14억2천만원 보상 검토 - 조선일보`
- Latest observation source: `조선일보`
- Latest observation published_at: `2026-07-06T18:30:37+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiiwFBVV95cUxPaVBpU01YdHotTVhpbkRZX2pzaE5CVEJUb0VaLUh6SkNqZXk3bUpGUWMyMWk2dzFXY0hrZ3dtRFJDY3RqLW9CZUE1N2NySS1xaEZ5VkxSTVNQbjFMbzk1RHRhcDBOdEtUMnZ3V09uUGZodjliN3VLQlpoUTNlZUE0RTRPUTBiQ3U0Ym9v?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
