---
id: verification-2026-08-15-KRX-080220-google-rss-coverage
type: verification
title: KRX 080220 Google RSS Coverage Verification
created: 2026-08-15
updated: 2026-08-15
status: verification
stage: 1

market: KRX
ticker: "080220"
company: 제주반도체
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-15

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=080220
    - name=제주반도체
    - naver_article_count=2
    - google_rss_article_count=22
    - kis_title_count=17
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 080220
    - 제주반도체
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

# KRX 080220 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-15_KRX_080220_google-rss-coverage-source]]

## Facts Checked
- `code=080220`
- `name=제주반도체`
- `naver_article_count=2`
- `google_rss_article_count=22`
- `kis_title_count=17`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `"삼전닉스 못지않아" 메모리 호황 덕본 제주반도체 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-08-14T08:03:08+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE82eFFsOU1vUjBnTUxCcHBld2c4OEt0RURoMS0tVVZvSWppTHhYZnpWRmRvMlkyYnB1ZVVkanBKSkFuUUNReG9VZk4yMDZPX00?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:36:30+09:00`
- Company: [[KRX_080220_제주반도체]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-15.json`
- Latest observation title: `"삼전닉스 못지않아" 메모리 호황 덕본 제주반도체 - v.daum.net`
- Latest observation source: `v.daum.net`
- Latest observation published_at: `2026-08-14T08:03:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiRkFVX3lxTE1rUVlGX2xTRV9kVVZvSHVKblEyVUFMcTJpQ1p4aEVQM3dEdkxCOHYxMGZRZkt6WTVJTXV5ZWtvZ3Mzdld5YUE?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_robotics_로봇]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
