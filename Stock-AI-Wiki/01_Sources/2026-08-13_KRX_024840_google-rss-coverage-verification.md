---
id: verification-2026-08-13-KRX-024840-google-rss-coverage
type: verification
title: KRX 024840 Google RSS Coverage Verification
created: 2026-08-13
updated: 2026-08-13
status: verification
stage: 1

market: KRX
ticker: "024840"
company: KBI메탈
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-13

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=024840
    - name=KBI메탈
    - naver_article_count=1
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 024840
    - KBI메탈
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

# KRX 024840 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-13_KRX_024840_google-rss-coverage-source]]

## Facts Checked
- `code=024840`
- `name=KBI메탈`
- `naver_article_count=1`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `특징주, KBI메탈-전선 테마 상승세에 5.9% ↑ - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-08-13T09:57:16+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE1uMndQWHcxR0pONnd0dDdsejN5dzVsRnR3OEZnVnpta3hlWEplR1VBQ3RsX3FJOV9nS0xCSF9pQWNxeDBpR2lIeDhvakpqTzRLR2c?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:35:49+09:00`
- Company: [[KRX_024840_KBI메탈]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-13.json`
- Latest observation title: `KBI메탈, 상반기 영업익 283억ⵈ 상장 이래 최대 실적 - 국토일보`
- Latest observation source: `국토일보`
- Latest observation published_at: `2026-08-13T20:10:48+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiY0FVX3lxTE5ENTBzVGt3YWFqNlUyN3RVWXc1akJYX2dBN01iMEtRQWVUems2Q0t1U3dCMFF3c09aNVluUS16UTdVaG9uS2NUazBUYU9VOTRuMGtKanV1RXNTczBJQW0teE80bw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
