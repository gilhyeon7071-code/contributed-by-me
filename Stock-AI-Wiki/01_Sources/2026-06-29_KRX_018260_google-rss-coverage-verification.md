---
id: verification-2026-06-29-KRX-018260-google-rss-coverage
type: verification
title: KRX 018260 Google RSS Coverage Verification
created: 2026-06-29
updated: 2026-06-29
status: verification
stage: 1

market: KRX
ticker: "018260"
company: 삼성에스디에스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-06-29

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=018260
    - name=삼성에스디에스
    - naver_article_count=1
    - google_rss_article_count=7
    - kis_title_count=10
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 018260
    - 삼성에스디에스
  possible_impact: unknown
  uncertainty:
    - Article body archive is available locally, but entity/event verification is still unknown.

verification:
  verified: false
  verification_status: unknown
  verified_at:
  verified_by:
  source_count: 1
  primary_source_exists: false
  original_text_available: true
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

# KRX 018260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-06-29_KRX_018260_google-rss-coverage-source]]

## Facts Checked
- `code=018260`
- `name=삼성에스디에스`
- `naver_article_count=1`
- `google_rss_article_count=7`
- `kis_title_count=10`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `“삼성에스디에스, ‘AI인프라 시대’ 수혜주 등극”…목표가↑ - 매일경제`
- Source: `매일경제`
- Published at: `2026-06-11T16:00:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFB1ckxJaXhYajRqU2hOR0lYTzBCUkF6TVpSZE1kaEQzUk1IVklNTW05LW9SVm92X0lIaHJXU3psX1NVdGhMNkdBaEVVZG4wN1Vwc3c?oc=5`

## Article Body Archive Checked
- Title: `“삼성에스디에스, ‘AI인프라 시대’ 수혜주 등극”…목표가↑ - 매일경제`
- Source: `매일경제`
- Published at: `2026-06-11T16:00:00+09:00`
- URL: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFB1ckxJaXhYajRqU2hOR0lYTzBCUkF6TVpSZE1kaEQzUk1IVklNTW05LW9SVm92X0lIaHJXU3psX1NVdGhMNkdBaEVVZG4wN1Vwc3c?oc=5`
- Evidence path: `https://news.google.com/rss/articles/CBMiUkFVX3lxTFB1ckxJaXhYajRqU2hOR0lYTzBCUkF6TVpSZE1kaEQzUk1IVklNTW05LW9SVm92X0lIaHJXU3psX1NVdGhMNkdBaEVVZG4wN1Vwc3c?oc=5`
- Body excerpt: “삼성에스디에스, ‘AI인프라 시대’ 수혜주 등극”…목표가↑ 매일경제

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:25:16+09:00`
- Company: [[KRX_018260_삼성에스디에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-06-29.json`
- Latest observation title: `이태희 부사장, 삼성에스디에스 주식 500주 매수 - 디지털투데이`
- Latest observation source: `디지털투데이`
- Latest observation published_at: `2026-06-29T16:11:01+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMic0FVX3lxTFBWVHNvbkZ0WF9lQ05IYU5OS2FobDNPYUk4Wm1NOEdNTWltN05QYk9qb1ZEY3ZpYnJEMHZsR2JmMk9oSjJfdGV1cFNHbGM5dXNPRkUwOHJMYXhqSi1WbzBFa1hJNXJMM0pMaTRFdHUtR2xPOE0?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
