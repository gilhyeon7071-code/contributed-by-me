---
id: verification-2026-07-25-KRX-000660-google-rss-coverage
type: verification
title: KRX 000660 Google RSS Coverage Verification
created: 2026-07-25
updated: 2026-07-25
status: verification
stage: 1

market: KRX
ticker: "000660"
company: SK하이닉스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=000660
    - name=SK하이닉스
    - naver_article_count=5
    - google_rss_article_count=92
    - kis_title_count=61
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 000660
    - SK하이닉스
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

# KRX 000660 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-25_KRX_000660_google-rss-coverage-source]]

## Facts Checked
- `code=000660`
- `name=SK하이닉스`
- `naver_article_count=5`
- `google_rss_article_count=92`
- `kis_title_count=61`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `SK하이닉스, 또 쌓는다...온디바이스 AI용 '3D 적층 D램' 개발 시동 - 지디넷코리아`
- Source: `지디넷코리아`
- Published at: `2026-07-24T11:03:31+09:00`
- Link: `https://news.google.com/rss/articles/CBMiVkFVX3lxTFBRcG1LQVFmSXhNcHNKWDY4aDdYY0c3aUJKc2xsS1FmWVpwZENDYmNaYmJ6amFpczloaTZUSk44NkZ2VHpBSGlldWY3Um0zOVZOVXN6UXFn?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:31:34+09:00`
- Company: [[KRX_000660_SK하이닉스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-25.json`
- Latest observation title: `SK하이닉스, 또 쌓는다...온디바이스 AI용 '3D 적층 D램' 개발 시동 - 지디넷코리아`
- Latest observation source: `지디넷코리아`
- Latest observation published_at: `2026-07-24T11:03:31+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiVkFVX3lxTFBRcG1LQVFmSXhNcHNKWDY4aDdYY0c3aUJKc2xsS1FmWVpwZENDYmNaYmJ6amFpczloaTZUSk44NkZ2VHpBSGlldWY3Um0zOVZOVXN6UXFn?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
