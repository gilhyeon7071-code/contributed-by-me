---
id: verification-2026-08-06-KRX-125490-google-rss-coverage
type: verification
title: KRX 125490 Google RSS Coverage Verification
created: 2026-08-06
updated: 2026-08-06
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
  collected_at: 2026-08-06

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=125490
    - name=한라캐스트
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
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
- [[2026-08-06_KRX_125490_google-rss-coverage-source]]

## Facts Checked
- `code=125490`
- `name=한라캐스트`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[리포트 브리핑]한라캐스트, '점점 더 커지는 존재감' Not Rated - SK증권 - 뉴스핌`
- Source: `뉴스핌`
- Published at: `2026-08-06T10:17:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5BSVVPUGh0NFo0eWh0bDVIZ1VrV1dHMGJJMU9MMVFYTk1IZmJSYmtla1pQRWg0b3gyUzQ1OEx5LTRlYktKQmVmU2pLRmJkYUhRZGZiRnJ1TUd0aDRu?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:33:31+09:00`
- Company: [[KRX_125490_한라캐스트]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-06.json`
- Latest observation title: `[리포트 브리핑]한라캐스트, '점점 더 커지는 존재감' Not Rated - SK증권 - 뉴스핌`
- Latest observation source: `뉴스핌`
- Latest observation published_at: `2026-08-06T10:17:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiXEFVX3lxTE5BSVVPUGh0NFo0eWh0bDVIZ1VrV1dHMGJJMU9MMVFYTk1IZmJSYmtla1pQRWg0b3gyUzQ1OEx5LTRlYktKQmVmU2pLRmJkYUhRZGZiRnJ1TUd0aDRu?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
