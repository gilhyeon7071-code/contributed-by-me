---
id: verification-2026-05-29-KRX-229000-google-rss-coverage
type: verification
title: KRX 229000 Google RSS Coverage Verification
created: 2026-05-29
updated: 2026-05-29
status: verification
stage: 1

market: KRX
ticker: "229000"
company: 젠큐릭스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-29

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=229000
    - name=젠큐릭스
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 229000
    - 젠큐릭스
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

# KRX 229000 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-29_KRX_229000_google-rss-coverage-source]]

## Facts Checked
- `code=229000`
- `name=젠큐릭스`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `젠큐릭스, 암 진단키트 4종 유럽 32개국 수출 - 데일리메디`
- Source: `데일리메디`
- Published at: `2026-05-08T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiY0FVX3lxTFBnSXIyVnhWQVpra2drZ0NSZWcxU2p5WGtKYTFBZ18wY2hrUXRZLVM3Y2txd1ZiNTUxWElZc3FMaFBTb05GbVhsYXJBeHZONUFQbGFOdVhvLWQ2amhzLURYNGFLQQ?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-29T15:05:05+09:00`
- Company: [[KRX_229000_젠큐릭스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-29.json`
- Latest observation title: `젠큐릭스, 암 진단키트 4종 유럽 32개국 수출 - 데일리메디`
- Latest observation source: `데일리메디`
- Latest observation published_at: `2026-05-08T16:00:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiY0FVX3lxTFBnSXIyVnhWQVpra2drZ0NSZWcxU2p5WGtKYTFBZ18wY2hrUXRZLVM3Y2txd1ZiNTUxWElZc3FMaFBTb05GbVhsYXJBeHZONUFQbGFOdVhvLWQ2amhzLURYNGFLQQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_exports_수출]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
