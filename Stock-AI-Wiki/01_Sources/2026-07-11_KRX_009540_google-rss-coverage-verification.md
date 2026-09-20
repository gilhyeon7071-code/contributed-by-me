---
id: verification-2026-07-11-KRX-009540-google-rss-coverage
type: verification
title: KRX 009540 Google RSS Coverage Verification
created: 2026-07-11
updated: 2026-07-11
status: verification
stage: 1

market: KRX
ticker: "009540"
company: HD한국조선해양
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-07-11

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=009540
    - name=HD한국조선해양
    - naver_article_count=6
    - google_rss_article_count=0
    - kis_title_count=0
    - google_rss_covered=False
    - kis_title_covered=False
    - any_covered=True
  related_entities:
    - KRX 009540
    - HD한국조선해양
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

# KRX 009540 Google RSS Coverage Verification

## Source Being Checked
- [[2026-07-11_KRX_009540_google-rss-coverage-source]]

## Facts Checked
- `code=009540`
- `name=HD한국조선해양`
- `naver_article_count=6`
- `google_rss_article_count=0`
- `kis_title_count=0`
- `google_rss_covered=False`
- `kis_title_covered=False`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `HD한국조선해양, 중동 선사서 PC선 6척 수주…올 목표 68.8% 달성 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-07-09T11:16:41+09:00`
- Link: `https://news.google.com/rss/articles/CBMiRkFVX3lxTFBOdndjNTdyd2ZiMlhPZjY0X19IZEFQUkFQQzJTeVJ1Y0E5SjVDb2pSZzBRMmhjUER2VWsxMlFyMjRvd00zLXc?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:28:43+09:00`
- Company: [[KRX_009540_HD한국조선해양]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-07-11.json`
- Latest observation title: `HD한국조선해양, 뱃머리를 에너지로 돌리다 - 파이낸셜투데이`
- Latest observation source: `파이낸셜투데이`
- Latest observation published_at: `2026-07-11T14:01:36+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiakFVX3lxTFBrSXVxQVdEemRYSGVYSjF2ckx4NlBBZzE2aXI5S2JTaXgzT3doazJmanlJdFZCSW5TWGNfZDlERnB1eTNqVFAzN0tHUVpURDVXRExwWFB6ZEtrMXhOdWZDSnVUMXJhVU5sM0HSAW5BVV95cUxQOTh5dm9YVFVfQnV5WTlqdU9qZFVJYnNNM25lZ19GeHplXzRFQV9HZ2hPSE00cS02azl1UXl0ZVl6VGZiQWJOZW0wcGVGbDNmakYtRVB2a1BMOWhPWmpQZkdheHBhZ183UW95SUhQQQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_gas-energy_가스-에너지]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
