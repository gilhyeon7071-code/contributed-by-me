---
id: verification-2026-07-15-KRX-028300-google-rss-coverage
type: verification
title: KRX 028300 Google RSS Coverage Verification
created: 2026-07-15
updated: 2026-07-15
status: verification
stage: 1

market: KRX
ticker: "028300"
company: HLB
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-15

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=028300
    - name=HLB
    - naver_article_count=4
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 028300
    - HLB
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

# KRX 028300 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-15_KRX_028300_google-rss-coverage-source]]

## Facts Checked
- `code=028300`
- `name=HLB`
- `naver_article_count=4`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `HLB "미국 허가 보류됐던 '간암 신약' 중대 사유 해소" - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-07-15T09:17:51+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTFBaM0FLbkxJd2FkUEZtRlRHcTE3VGoyYnZfajAtQzJNTnR3Q0x2OW5IdVQ0enY2Q3dWTUFUV0lJYktERk1oWGRaZl9iUXFJUzg?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-15T21:05:34+09:00`
- Company: [[KRX_028300_HLB]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-15.json`
- Latest observation title: `HLB "간암 신약, 미국 승인 문제 된 사안 대부분 해소" - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-07-15T08:56:59+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE92WjkyVVJCTDhWdlJOY196YzgyOXhEMmI3MThGam5fcGt3RkN5YUVFdHVXMUt0X3JGaC1VYWN6dnVpRE5UY1JOc1NWM1NYVnNXaFRRUjBzV3VlSWpuSFpDb2hpRFlkQlpS0gFuQVVfeXFMUFFicjJuYVNoMmJsM3NiSVVPT3ZPb1NuRXJGVkdxR1JaYUFOaFBWOWFOeGtPd1c2VkRjUGV5bUV5MVdMU0tlYkpodzZZc0pZUDQ5UjFhVEVjVnZiT0lPWFFVWUExYUJURjA3bW5veXc?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_bio_바이오]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
