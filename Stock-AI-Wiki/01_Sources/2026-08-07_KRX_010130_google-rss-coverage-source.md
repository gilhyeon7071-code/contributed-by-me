---
id: source-2026-08-07-KRX-010130-google-rss-coverage
type: source
title: KRX 010130 Google RSS Coverage Source
created: 2026-08-07
updated: 2026-08-07
status: raw
stage: 0

market: KRX
ticker: "010130"
company: 고려아연
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-07

analysis:
  summary: Local coverage report row shows news coverage for KRX 010130.
  key_facts:
    - code=010130
    - name=고려아연
    - naver_article_count=2
    - google_rss_article_count=68
    - kis_title_count=40
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 010130
    - 고려아연
  possible_impact: unknown
  uncertainty:
    - RSS item metadata is available, but full original article body is not stored locally.

verification:
  verified: false
  source_count: 1
  confidence: unknown
  conflict_exists: false

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
  change_reason: generated coverage source note
---

# KRX 010130 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=010130`
- `name=고려아연`
- `naver_article_count=2`
- `google_rss_article_count=68`
- `kis_title_count=40`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 010130.

## RSS Item Metadata
- Title: `iM증권, 고려아연 목표가 4%↓…"3분기 이익 전 분기보다 감소" - 연합뉴스`
- Source: `연합뉴스`
- Published at: `2026-08-07T08:33:17+09:00`
- Link: `https://news.google.com/rss/articles/CBMiW0FVX3lxTE9pdm1vSVFWeEIwb3lVS2VESFNqdW9JZUQ0MUxvOVBhOFVGeGh6ZTFFeHJMWFZaX3dJdHkwSWdEN0xrSmhXdmg1QUZXN0ZRcWpUclZlNThfdThTVUXSAWBBVV95cUxPa0E5cW9ZSWJ0WHVCVVNZc1ZWbjhLSWR2aHBYZGRHRFZvMllwcm9ZMGdreUk2dElZM0NWai1aaEFucGQwTEtrNmFUdzg2S0JBZFAzTWl6eURmYi1vYmFDRWs?oc=5`

## Article Body Archive
- not_available

## Interpretation
- No trading interpretation is assigned at source stage.

## Uncertainty
- Original article body verification has not passed.

## Questions
- Which original article should be attached before source verification can pass?

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:33:51+09:00`
- Company: [[KRX_010130_고려아연]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-07.json`
- Latest observation title: `영풍 “비철금속협회 공식 사과…고려아연도 사과해야” - 서울경제`
- Latest observation source: `서울경제`
- Latest observation published_at: `2026-08-07T15:04:34+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiUkFVX3lxTE5nWnM0aXlaNXVxLVYwRjdoa0RCTFlFcEVyNEV4N1BXX2pVY0YwbjRxd1U1TnNTQkg2UE0tQl9sWGd5WUdYOGE3MzdzWWtRak5qMkHSAVNBVV95cUxOaDRHcjhzNFJHMzFuaDNoMWVwSE1rSzVMaEVFcDFQRXlWWHhEZU96elVCOFBGaFNUeEJfY3NoaUE4NmZJRWc3cXh2S2NpT0gtUUp5VQ?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
