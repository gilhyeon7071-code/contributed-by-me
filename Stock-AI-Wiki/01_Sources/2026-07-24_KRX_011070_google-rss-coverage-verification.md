---
id: verification-2026-07-24-KRX-011070-google-rss-coverage
type: verification
title: KRX 011070 Google RSS Coverage Verification
created: 2026-07-24
updated: 2026-07-24
status: verification
stage: 1

market: KRX
ticker: "011070"
company: LG이노텍
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-24

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=011070
    - name=LG이노텍
    - naver_article_count=1
    - google_rss_article_count=9
    - kis_title_count=9
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 011070
    - LG이노텍
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

# KRX 011070 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-24_KRX_011070_google-rss-coverage-source]]

## Facts Checked
- `code=011070`
- `name=LG이노텍`
- `naver_article_count=1`
- `google_rss_article_count=9`
- `kis_title_count=9`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `특징주, LG이노텍-반도체 기판(FC-BGA/PCB/MLB 등) 테마 상승세에 6.34% ↑ - 매일경제 마켓`
- Source: `매일경제 마켓`
- Published at: `2026-07-23T10:16:25+09:00`
- Link: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE1jQlQtWUpBVUxWbjBzVU9QS256SEhxOHhLdHJCMTdmZ0cyRjNXSjdBTDBtUzBUWFJyaF9NMERDeVBTeFRDN1FVc1dub25OYUlMUFE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-24T21:05:14+09:00`
- Company: [[KRX_011070_LG이노텍]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-24.json`
- Latest observation title: `문혁수 LG이노텍 대표, 회사 주식 200주 추가 매입 - 뉴시스`
- Latest observation source: `뉴시스`
- Latest observation published_at: `2026-07-24T09:22:41+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiYEFVX3lxTFBRaWZiM1RrRS1fZ3BaVWdIS0Y4M3N0aHQ4YXpQUnFJSmJybFF2U3ZXYndoblJNMVJ6anNBX254TmlUSVZ0cm1jaGF4Ti1VU1A0SjlzZXVQaEdib3BWbUotVtIBeEFVX3lxTE5INmJ2RVBHc1p1NEZRMXVlYUJURnpZTTJNU0hEV1BYUjBuTnM5U19Ucm92SXExdHFYM3NKTWdSLV9mQy13YlUzd19aaGtoVF9WdVlDUHYtOGhheGxqTXN4YkN4NUZyclpnWnRVSHpnTVRGcXdQVk1BYQ?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
