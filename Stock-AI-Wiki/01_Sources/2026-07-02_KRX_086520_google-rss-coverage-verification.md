---
id: verification-2026-07-02-KRX-086520-google-rss-coverage
type: verification
title: KRX 086520 Google RSS Coverage Verification
created: 2026-07-02
updated: 2026-07-02
status: verification
stage: 1

market: KRX
ticker: "086520"
company: 에코프로
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-02

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=086520
    - name=에코프로
    - naver_article_count=3
    - google_rss_article_count=31
    - kis_title_count=36
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 086520
    - 에코프로
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

# KRX 086520 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-02_KRX_086520_google-rss-coverage-source]]

## Facts Checked
- `code=086520`
- `name=에코프로`
- `naver_article_count=3`
- `google_rss_article_count=31`
- `kis_title_count=36`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `[특징주] 에코프로비엠, 1.2조 유상증자 소식에 6.9% 하락(종합) - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-07-01T15:55:20+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTFBoMWJMbkxnc2l2dHNVZzd0ckVkZU5WYVJLQndiY2VWMVlwY2FOenN1ZEYxOTUwMDBOX2hZR0wxaU9DMmZqM1FBN1ViR200OFJYeURoRTVhVkVidVXSAWBBVV95cUxNY250MkJwVExSd2NaamR2YlR1cEs0djFqLXlDVUxNajlVSWZsVGlYUmxIWDhwZ0xhSTRzUDI0RHZ5cno2S3VGeXh5QXg5ZURkM2s0QmNXX2FVN1ZUTmlMTjY?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-02T09:06:42+09:00`
- Company: [[KRX_086520_에코프로]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-02.json`
- Latest observation title: `“빚도 메자닌도 비싸다”... 에코프로비엠, 유증 택할 수밖에 없던 속사정 - 조선비즈 - Chosunbiz`
- Latest observation source: `Chosunbiz`
- Latest observation published_at: `2026-07-01T15:32:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiiAFBVV95cUxPSEQtQjJWazNoMmNOQXNXMzBWSmFReFVpMTc4amxaYVo1Uk5xeFNVM29nY20xX2VfenJCbUZiVFBLNUk1a2YtSjE3NGdNdi03eFJXU2lxNEV2ZVQzOTJxeUwtUG5QYm8zNENUTzlRWmdNNWFqdjNYQm8zdWFxMkVoUEl5NW5sUnU50gGcAUFVX3lxTE50UGNjcFB5N1V4Y3F3ZWZZN2hOZnFfcjVaNHRUT1l0M1A1b2FzbEtkaWN3ekpfLUlwSmZvcjJEYkUtVmV4Yllja0drREg3SGU4UTRxdnl4TjlObnd2Mkw2T2lqSFEyV0VFWkpCalBHVTg5dWpIc0NscjA1bG9abnlDUktxRTRVWnJteG5GTDN3dWVJbUdpQ2xFZDVyVg?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
