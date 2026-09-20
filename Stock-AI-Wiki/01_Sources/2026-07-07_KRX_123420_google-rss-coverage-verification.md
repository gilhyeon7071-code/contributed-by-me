---
id: verification-2026-07-07-KRX-123420-google-rss-coverage
type: verification
title: KRX 123420 Google RSS Coverage Verification
created: 2026-07-07
updated: 2026-07-07
status: verification
stage: 1

market: KRX
ticker: "123420"
company: 위메이드플레이
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-07

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=123420
    - name=위메이드플레이
    - naver_article_count=1
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 123420
    - 위메이드플레이
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

# KRX 123420 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-07_KRX_123420_google-rss-coverage-source]]

## Facts Checked
- `code=123420`
- `name=위메이드플레이`
- `naver_article_count=1`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `위메이드플레이, ‘애니팡4’ 출시 6주년 이벤트 진행 - 리버티코리아포스트`
- Source: `리버티코리아포스트`
- Published at: `2026-07-07T15:43:04+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZ0FVX3lxTE9rVmpFRGcxUlozRTJ2UFpqLWdtOFU5QjBCNE4wTWZRQTZZaExIZXRlVmw0ZE9QS2c1MEVnSXMxMUpSS2dfWmdQSW5DNUxCdWtaZDN0aERuNE02Tjg5bG0ta1RiMjRQejjSAWdBVV95cUxPa1ZqRURnMVJaM0UydlBaai1nbThVOUIwQjROME1mUUE2WWhMSGV0ZVZsNGRPUEtnNTBFZ0lzMTFKUktnX1pnUEluQzVMQnVrWmQzdGhEbjRNNk44OWxtLWtUYjI0UHo4?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:27:28+09:00`
- Company: [[KRX_123420_위메이드플레이]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-07.json`
- Latest observation title: `위메이드플레이 '애니팡4' 6주년 이벤트… 장수 IP 저력에 하반기 모멘텀 '기대' - 비즈트리뷴`
- Latest observation source: `비즈트리뷴`
- Latest observation published_at: `2026-07-07T21:22:54+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMicEFVX3lxTE1vUWtvcVYyUHc5WUl6QjFtczJHTHNEMVRpNzliR3BJS2ozOHBpTnQxQVkzWWxNTTczcDVwWWt1UHAwdjJEdVVlWmd6MHVBNzRkTExleThkcnB6em1Bd2s0ejdiblZDd0dTV1FmdzhkRkE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
