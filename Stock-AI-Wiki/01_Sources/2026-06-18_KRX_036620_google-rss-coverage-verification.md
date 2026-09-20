---
id: verification-2026-06-18-KRX-036620-google-rss-coverage
type: verification
title: KRX 036620 Google RSS Coverage Verification
created: 2026-06-18
updated: 2026-06-18
status: verification
stage: 1

market: KRX
ticker: "036620"
company: 감성코퍼레이션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-18

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=036620
    - name=감성코퍼레이션
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 036620
    - 감성코퍼레이션
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

# KRX 036620 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-18_KRX_036620_google-rss-coverage-source]]

## Facts Checked
- `code=036620`
- `name=감성코퍼레이션`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `감성코퍼레이션 투자분석 2026. 06. 17 - 주달`
- Source: `주달`
- Published at: `2026-06-17T21:03:34+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTE9GNV9ZcUE5UlZITnZjZ3NrZFotM3RaN0doeXA5TG13SjU4OEJhZkxkZ2UxZG1Jdjg0Qms3WlJDYWY2b2MtUDBTdkhlRHNjZS0yZFJaR0IxMkxFTWRHMkFMc281dmpBam81dng2Zi11M1hKOVE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:22:04+09:00`
- Company: [[KRX_036620_감성코퍼레이션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-18.json`
- Latest observation title: `감성코퍼레이션, 자사주 64만주 전량 소각 결정…중장기 주주환원 로드맵 이행 - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-06-15T13:54:32+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFA2NTgxOWhLemtJclhtc1ZSeFo2SENCR09FNUxmZjlzX0QydlJERjIzZ2xzY21aYmxWSDF5clJVM1hodGx2TGhyelRjM3dQX2ZvaUJYWGdYZ3RaTXRvSmxfM3dJclRhbGkz0gFuQVVfeXFMTWstQl9GNVNDWXhsWGlhVTBsSHlPSnByTE9tZ0I5UzkzMjM0ai1FYlp6d2FJcVZYQ0ZRamJXQkRVSE1SZU9oTTFJYUw1VG1xb1plc2pBekNfZnRLOE1fYjk5QUxkU1NJOVVDOEtoOEE?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
