---
id: verification-2026-07-29-KRX-079550-google-rss-coverage
type: verification
title: KRX 079550 Google RSS Coverage Verification
created: 2026-07-29
updated: 2026-07-29
status: verification
stage: 1

market: KRX
ticker: "079550"
company: LIG넥스원
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-29

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=079550
    - name=LIG넥스원
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=7
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 079550
    - LIG넥스원
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

# KRX 079550 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-29_KRX_079550_google-rss-coverage-source]]

## Facts Checked
- `code=079550`
- `name=LIG넥스원`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=7`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LIG D&A, 소방관 위한 무인수상정 개발 착수 - 한국재난안전뉴스`
- Source: `한국재난안전뉴스`
- Published at: `2026-07-27T13:07:11+09:00`
- Link: `https://news.google.com/rss/articles/CBMiXkFVX3lxTE9oMzlyYVhBcVNPZWJ2NWtxSlY1Q24xa2lEdFNKaVFka21lek1qZ2JMN01WdDA4UFpjXzJEQUZpUXo3WXprU3VnNWtkVENKMkNhRlY0WmJjbjVLNmJzT2c?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:32:53+09:00`
- Company: [[KRX_079550_LIG넥스원]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-29.json`
- Latest observation title: `K-방산 호황의 그늘…납품 비리 의혹 속 글로벌 신뢰도 '시험대' - 뉴스웍스`
- Latest observation source: `뉴스웍스`
- Latest observation published_at: `2026-07-29T10:48:57+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMib0FVX3lxTE8xakRfczEwSWxQYTFSTFduY0JBaUxUMXNEdk1YaDgzamxFLUFqdmNuT0tOTFJ4em56UTBNWlFOSFRhTExWVEU0dGxBMWZ3LTNyOFlVLUJ4bW1oT2pfOGZLN2pEVFNQZ2lhNTkzLUxEY9IBc0FVX3lxTE51akZqRURFWFZWbjJSc3V1dzVCRGFtZzFlVTlFb2xLc1o0cklKSVBlR0RUcWd3dUV5VEpvMW5iYnBraExjNGpYNWR5VXRheUJMRWZiX01FTC1Xd0k1V0ZONjYtVHZVcHFZVWRZeWU2cjBvM2M?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_exports_수출]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
