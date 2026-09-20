---
id: verification-2026-05-27-KRX-476830-google-rss-coverage
type: verification
title: KRX 476830 Google RSS Coverage Verification
created: 2026-05-27
updated: 2026-05-27
status: verification
stage: 1

market: KRX
ticker: "476830"
company: 알지노믹스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-27

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=476830
    - name=알지노믹스
    - naver_article_count=1
    - google_rss_article_count=7
    - kis_title_count=9
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 476830
    - 알지노믹스
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

# KRX 476830 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-27_KRX_476830_google-rss-coverage-source]]

## Facts Checked
- `code=476830`
- `name=알지노믹스`
- `naver_article_count=1`
- `google_rss_article_count=7`
- `kis_title_count=9`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `알지노믹스 신약 후보 'RZ-001', 국제일반명 '타스피티마젠 아드벡' 선정 - 히트뉴스`
- Source: `히트뉴스`
- Published at: `2026-05-26T11:59:49+09:00`
- Link: `https://news.google.com/rss/articles/CBMia0FVX3lxTE4yOHhqRm51bjNYRXk4WGJncGYzRVMxWk9Wc1lZZ1ZqMTZEdEt2VmdmNElYX09JaHdWMnBnMEVacUVLLWZXVUJGUmFqM2lMWmFIUVdld05LMEs5UkFOYlFCMHFMSUZ6MEFSUlpF?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-05-27T17:05:04+09:00`
- Company: [[KRX_476830_알지노믹스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-27.json`
- Latest observation title: `알지노믹스 신약 후보 'RZ-001', 국제일반명 '타스피티마젠 아드벡' 선정 - 히트뉴스`
- Latest observation source: `히트뉴스`
- Latest observation published_at: `2026-05-26T11:59:49+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMia0FVX3lxTE4yOHhqRm51bjNYRXk4WGJncGYzRVMxWk9Wc1lZZ1ZqMTZEdEt2VmdmNElYX09JaHdWMnBnMEVacUVLLWZXVUJGUmFqM2lMWmFIUVdld05LMEs5UkFOYlFCMHFMSUZ6MEFSUlpF?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
