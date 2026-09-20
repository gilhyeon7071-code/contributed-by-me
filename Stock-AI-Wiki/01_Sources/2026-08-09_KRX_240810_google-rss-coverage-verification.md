---
id: verification-2026-08-09-KRX-240810-google-rss-coverage
type: verification
title: KRX 240810 Google RSS Coverage Verification
created: 2026-08-09
updated: 2026-08-09
status: verification
stage: 1

market: KRX
ticker: "240810"
company: 원익IPS
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-09

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=240810
    - name=원익IPS
    - naver_article_count=1
    - google_rss_article_count=5
    - kis_title_count=19
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 240810
    - 원익IPS
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

# KRX 240810 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-09_KRX_240810_google-rss-coverage-source]]

## Facts Checked
- `code=240810`
- `name=원익IPS`
- `naver_article_count=1`
- `google_rss_article_count=5`
- `kis_title_count=19`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `SK증권 "원익IPS, 수주잔고 증가로 하반기 실적 개선 기대" - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-08-07T08:40:06+09:00`
- Link: `https://news.google.com/rss/articles/CBMiYEFVX3lxTE5uZzlhUDdCSGd1RmxwUzdWWldsX0syRkZjcHByVUpQeDZhMlgxOGdnZ3piWUdEazRfbGZ0Vk05VW8ySkh1MTk5Sm4yTVlvdHlpWnQ5MjVERkNrOUZjSkRsUNIBYEFVX3lxTE5uZzlhUDdCSGd1RmxwUzdWWldsX0syRkZjcHByVUpQeDZhMlgxOGdnZ3piWUdEazRfbGZ0Vk05VW8ySkh1MTk5Sm4yTVlvdHlpWnQ5MjVERkNrOUZjSkRsUA?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:34:30+09:00`
- Company: [[KRX_240810_원익IPS]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-09.json`
- Latest observation title: `코스닥 800선 다시 내줘…레인보우로보틱스·원익IPS 약세, HLB·알테오젠·펩트론은 강세 - CBC뉴스`
- Latest observation source: `CBC뉴스`
- Latest observation published_at: `2026-08-09T16:31:05+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTE8zOTZUVWFZbDBqOHVXZDQ3bEVBSjh5UzhBN1pyYUY4aGVaMXVyTzZJZnJ2bEpNM3NNWUJmU1hmclVNQ2NZbm9VUlpDWGhkeTMwS1NJVnhCTExsX2pwODNSVUJhRF9wZjBS?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
