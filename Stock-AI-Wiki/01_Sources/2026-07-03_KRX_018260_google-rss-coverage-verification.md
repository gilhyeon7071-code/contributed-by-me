---
id: verification-2026-07-03-KRX-018260-google-rss-coverage
type: verification
title: KRX 018260 Google RSS Coverage Verification
created: 2026-07-03
updated: 2026-07-03
status: verification
stage: 1

market: KRX
ticker: "018260"
company: 삼성에스디에스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-03

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=018260
    - name=삼성에스디에스
    - naver_article_count=1
    - google_rss_article_count=3
    - kis_title_count=1
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 018260
    - 삼성에스디에스
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

# KRX 018260 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-03_KRX_018260_google-rss-coverage-source]]

## Facts Checked
- `code=018260`
- `name=삼성에스디에스`
- `naver_article_count=1`
- `google_rss_article_count=3`
- `kis_title_count=1`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `"10년간 주가 겨우 5만원 올라"…삼성SDS 직원들, 성과급 개편 두고 고심 - 머니투데이 - 머니투데이`
- Source: `머니투데이`
- Published at: `2026-07-01T15:16:18+09:00`
- Link: `https://news.google.com/rss/articles/CBMiZ0FVX3lxTE9RT1IwR2xiUHpVa1ltVGFiaHpzMlQyenIyWnp1bjJ2ZHNDUFZWUGRFMXdZUmZpUG4yVVBsaC1WNXJWbTR1Y2s3b3JHT2V3dU5tMlhEQmw3TzVkMHF3SEVYaUJBZWFrSEHSAWxBVV95cUxQNHRPTFpRSUs0emVoaW1ocklnMTQxRFBoR3N1TmFHN013MVBpYkFzZjd5ZUhRbXBVazdiVVhCaHZ1Wk9sYXM1QjVFeVV6MERibVBfSUlPNExrS0NQdkRPcnNCZGtNMTZUbUJhLU4?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:26:32+09:00`
- Company: [[KRX_018260_삼성에스디에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-03.json`
- Latest observation title: `[단독]삼성, 반도체 공정에 양자컴 심는다 - 서울경제`
- Latest observation source: `서울경제`
- Latest observation published_at: `2026-07-01T17:40:37+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE05SDdiMlA1NWxwcHVOS09objVqSnBVeWhPZ1QxNmd6cHgwRngzTUppYXJGRVZ2WE95SFlYdFA1NmlEa1hSRFFIMzhESURuQVRxcHfSAVNBVV95cUxNMWNLd3Flb3BjWUg3Q25YWkdiRUt5ZzRJR0l5b05xeFExSnZrdDNPVUNabUFLeEFkMU1MM3lsQ0xKSVpSZW5uWWdWUGtBVVE3Z2djUQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_bio_바이오]]
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
