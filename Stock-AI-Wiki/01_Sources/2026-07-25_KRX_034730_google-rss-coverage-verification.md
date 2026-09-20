---
id: verification-2026-07-25-KRX-034730-google-rss-coverage
type: verification
title: KRX 034730 Google RSS Coverage Verification
created: 2026-07-25
updated: 2026-07-25
status: verification
stage: 1

market: KRX
ticker: "034730"
company: SK
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-25

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=034730
    - name=SK
    - naver_article_count=34
    - google_rss_article_count=140
    - kis_title_count=45
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 034730
    - SK
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

# KRX 034730 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-25_KRX_034730_google-rss-coverage-source]]

## Facts Checked
- `code=034730`
- `name=SK`
- `naver_article_count=34`
- `google_rss_article_count=140`
- `kis_title_count=45`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `“최태원 SK 주식도 재산 분할 대상…노소영에게 9440억원 지급” 판단 근거는? [뉴스분석] - 경향신문`
- Source: `경향신문`
- Published at: `2026-07-24T18:44:00+09:00`
- Link: `https://news.google.com/rss/articles/CBMiWkFVX3lxTFAtenQ0VWpoWDhQdElPVTJhek9obkl6WlUtcHByUzdfUVZYeU9OaUdBR0FuUG95eklZaGZPZXRYdDhPMFg1MV8tdG42UVVkQV9kZ2k1TjNEeFlIZ9IBX0FVX3lxTE1nNjZhaHRYYkZJUkpNUWNubmhGX3NUa2NDRFdZcUp2UW1Fb3hXNkhMTU42TjJCSGlMOWxTaGVhdExHSnItZEpRTzAwX0VwX0dySWh3ZzlPcThudzZoTVZn?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:31:34+09:00`
- Company: [[KRX_034730_SK]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-25.json`
- Latest observation title: `SK, 엔비디아와 AI 인프라 구축..MS와 메모리 장기 협력 - 머니투데이 - 머니투데이`
- Latest observation source: `머니투데이`
- Latest observation published_at: `2026-07-25T14:33:13+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMibEFVX3lxTFBHNTFLMEdqNXM3NmEwN0hDaHhZcUlnVGo2Q0pKU2RrdmIta3JnN1JtaE43d2pZSTZxaDE0Qk9kTm04MlBZSkx5NW1ULUZVNWNFdmdrbmNPR2FjOVAzSjZUOFdQWTloSm1udmlVbdIBckFVX3lxTFBVZXpvTGh0UWVKSmg0dEZaZzlZQld1dVc3clFBbExLUDNxY3pxd0htLUdyR0laenp0bHB3MmNGNDl3SHRDQ3c4X1I3dndCRzBkaGhuQlVyeHktay12OHpya3hycG1RMUh2OUl2a3RfcG1NZw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
