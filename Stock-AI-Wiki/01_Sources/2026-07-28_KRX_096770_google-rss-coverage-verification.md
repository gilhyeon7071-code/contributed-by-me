---
id: verification-2026-07-28-KRX-096770-google-rss-coverage
type: verification
title: KRX 096770 Google RSS Coverage Verification
created: 2026-07-28
updated: 2026-07-28
status: verification
stage: 1

market: KRX
ticker: "096770"
company: SK이노베이션
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-28

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=096770
    - name=SK이노베이션
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 096770
    - SK이노베이션
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

# KRX 096770 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-28_KRX_096770_google-rss-coverage-source]]

## Facts Checked
- `code=096770`
- `name=SK이노베이션`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `"언론도, 국회도 가지 마라"...SK이노베이션E&S의 유족 입막음 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-07-28T10:26:57+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5RTmh0enhndk5YYkFqRTZRWFAyZnNUa05Ra1JOZ3NMVFhydGxCZG51WVh0VmhpTWFDd2owcXVtU0xxa21FcUpzZ1FKd09RbmM?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-07-28T13:06:41+09:00`
- Company: [[KRX_096770_SK이노베이션]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-28.json`
- Latest observation title: `SK이노베이션 E&S, 호주산 초경질유 첫 도입… 30만 배럴 8월 입항 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-07-27T11:01:50+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE9mRWR0WHVBWnB2TVlyaVRMb2Q3UUh3ZDVkWTZENzd3bFJCT0ZudGVibWM1cFhoOFphLVdEOWIxdUxpcHB6aGFJajVOMXIyR2M?oc=5`
- Body status: `missing_original_text`
- Original text available: `false`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
