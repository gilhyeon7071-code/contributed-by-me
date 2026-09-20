---
id: verification-2026-08-14-KRX-066570-google-rss-coverage
type: verification
title: KRX 066570 Google RSS Coverage Verification
created: 2026-08-14
updated: 2026-08-14
status: verification
stage: 1

market: KRX
ticker: "066570"
company: LG전자
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-14

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=066570
    - name=LG전자
    - naver_article_count=2
    - google_rss_article_count=29
    - kis_title_count=8
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 066570
    - LG전자
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

# KRX 066570 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-14_KRX_066570_google-rss-coverage-source]]

## Facts Checked
- `code=066570`
- `name=LG전자`
- `naver_article_count=2`
- `google_rss_article_count=29`
- `kis_title_count=8`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `"LG전자, 냉각솔루션·로보틱스 성과 모멘텀…목표가 25만원으로↑" - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-08-13T09:00:35+09:00`
- Link: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE5tMXNWc1hkODc0MTRydGlva1c1SmRnblFuRVl2SUZEV2NzeURsVDZjSk11aDdRTURtOUdadXloQzlKZ3NVX3VPZl8zclc3WF9ZVHJiaDNpX1JRUVBCdnQwNG1PaVNSV1U30gFuQVVfeXFMT09FdVp3cmh5OHBaUFY5dE5Fc1Vvd09MS1lfak0waloxYkJsTWE4Wm1ITkdZUEowNDM4ZXJXaDdFUkVYdHZWM1B0NGl6VEtvSUQ5YVEyWkctb2t3TTBrVldCMkxMS25Ic0NzWnNGekE?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:36:10+09:00`
- Company: [[KRX_066570_LG전자]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-14.json`
- Latest observation title: `주식 열풍에 LG전자 주주 100만 돌파…삼전은 800만 육박 - 노컷뉴스`
- Latest observation source: `노컷뉴스`
- Latest observation published_at: `2026-08-14T20:51:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE9YTm9JU3otQldFREJZYmV3N1BYYW9UQ0pHY1Z2S2NGeU5Vc0pzLVo5YWpmRFdZU2ZSdmhnY0ZIT0RZQjdvN2h2TjNnV2Nkdm56eWc?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
