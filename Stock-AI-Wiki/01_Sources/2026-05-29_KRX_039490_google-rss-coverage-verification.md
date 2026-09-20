---
id: verification-2026-05-29-KRX-039490-google-rss-coverage
type: verification
title: KRX 039490 Google RSS Coverage Verification
created: 2026-05-29
updated: 2026-05-29
status: verification
stage: 1

market: KRX
ticker: "039490"
company: 키움증권
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-29

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=039490
    - name=키움증권
    - naver_article_count=3
    - google_rss_article_count=74
    - kis_title_count=26
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 039490
    - 키움증권
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

# KRX 039490 Google RSS Coverage Verification

## Source Being Checked
- [[2026-05-29_KRX_039490_google-rss-coverage-source]]

## Facts Checked
- `code=039490`
- `name=키움증권`
- `naver_article_count=3`
- `google_rss_article_count=74`
- `kis_title_count=26`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `키움증권, 대주전자재료 목표가 상향…"하반기 실적개선 전망" - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-05-29T08:36:24+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5pUWVvdEJLejBQRS1HTjktdjRPUEw1alBSRkdBbTdKNlM4ejU4UXBwM2pHcXp3T2JzWHdKSGYyLS1RajRWSVFhY0JkUUNDRnM?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:16:19+09:00`
- Company: [[KRX_039490_키움증권]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-29.json`
- Latest observation title: `키움증권, 대주전자재료 목표가 상향…"하반기 실적개선 전망" - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-05-29T08:36:24+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE5pUWVvdEJLejBQRS1HTjktdjRPUEw1alBSRkdBbTdKNlM4ejU4UXBwM2pHcXp3T2JzWHdKSGYyLS1RajRWSVFhY0JkUUNDRnM?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_earnings_실적]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
