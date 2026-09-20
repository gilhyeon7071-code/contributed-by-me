---
id: verification-2026-07-22-KRX-079550-google-rss-coverage
type: verification
title: KRX 079550 Google RSS Coverage Verification
created: 2026-07-22
updated: 2026-07-22
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
  collected_at: 2026-07-22

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=079550
    - name=LIG넥스원
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
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
- [[2026-07-22_KRX_079550_google-rss-coverage-source]]

## Facts Checked
- `code=079550`
- `name=LIG넥스원`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `LIG D&A·LG AI연구원, AI 기반 지휘통제 체계 공동 개발 - 조선비즈 - Chosunbiz`
- Source: `Chosunbiz`
- Published at: `2026-07-21T14:35:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMihAFBVV95cUxQdzA4bWEzdzQ5VHlObFdyc001V3FtLWJiblNDZVFfXzNuOXY3R1dKQzcyb2N1ZElmTmxoaUZ6Nll1Y1R3RTdNYzUzSGtJaHJlYU1STmZ1ODRMdlJwcjhuc2dUY2VXSjdta2F5VjE3VWhlZjhqdHhVS0J0cTg0d3F2amZoTGzSAZgBQVVfeXFMUHM1Y0NzSjlIRDNDbTdvaHJYeXZIOVIwZHZZS25QTWU1NGFaTmNtQVB1LUFLdnYxZHFLc2NFQm5FM1ZBQWd3cHRodFNhYkxzcU1iY3VWcTlkeHgwYklCMWVzQlJCcV8yZUVrbjc3SjhHT0IzSF9JRVlyX2dvRGx2S25PNUQzelkzZ1hybExwMTZlSG5SYUdpSVk?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-22T21:05:15+09:00`
- Company: [[KRX_079550_LIG넥스원]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-22.json`
- Latest observation title: `“베트남선 19년간 전쟁” 트럼프 외침에도…LIG넥스원 목표가 하향 왜 [오늘 나온 보고서] - 매일경제`
- Latest observation source: `매일경제`
- Latest observation published_at: `2026-07-22T08:42:11+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE9xMjFybE9xeGRUZUFpbzZOcjAxU080djV6b0kzcWtJVGtjbDNFY2N6OEFGLVc4TlZkbmwxZ0NSZ243WGl3cmp1SHR4c2N3cDdvX3c?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
