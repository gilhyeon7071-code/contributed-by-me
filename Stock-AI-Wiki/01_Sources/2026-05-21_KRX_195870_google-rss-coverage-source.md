---
id: source-2026-05-21-KRX-195870-google-rss-coverage
type: source
title: KRX 195870 Google RSS Coverage Source
created: 2026-05-21
updated: 2026-05-21
status: raw
stage: 0

market: KRX
ticker: "195870"
company: 해성디에스
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-05-21

analysis:
  summary: Local coverage report row shows news coverage for KRX 195870.
  key_facts:
    - code=195870
    - name=해성디에스
    - naver_article_count=0
    - google_rss_article_count=1
    - kis_title_count=2
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 195870
    - 해성디에스
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

# KRX 195870 Google RSS Coverage Source

## Original Material
- Evidence path: `E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv`
- `code=195870`
- `name=해성디에스`
- `naver_article_count=0`
- `google_rss_article_count=1`
- `kis_title_count=2`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## Facts
- The local coverage report contains a coverage row for KRX 195870.

## RSS Item Metadata
- Title: `생산효율↑·클레임↓… 창원산단 ‘디지털 전환’ 가속 - v.daum.net`
- Source: `v.daum.net`
- Published at: `2026-05-20T21:22:48+09:00`
- Link: `https://news.google.com/rss/articles/CBMiT0FVX3lxTE9VTzNPQ2Y1Z1V1ZmZKUXcwSU1WTTdZeFdtVzY2bklVNnR0V3lVbjE4dDhCM0NxVlF3Mzk1QkRrU1U1RVI5bHJpZ2RNWmZtYjg?oc=5`

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
- Updated at: `2026-08-21T19:14:51+09:00`
- Company: [[KRX_195870_해성디에스]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-05-21.json`
- Latest observation title: `[1% 초고수의 선택] '해성디에스' 사고 '두산퓨얼셀' 팔았다 - ebn.co.kr`
- Latest observation source: `ebn.co.kr`
- Latest observation published_at: `2026-05-21T16:00:52+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiaEFVX3lxTFBuRW9YUWRQR2xGSURSNlRJVzhhc1hhZDlDN2ZmZDc1UG41X1VmQnNIZ1BaX3JXNDZwVFBMUnplWFRaX0FVYjFQZFpsSjVsQVA1MjZsN1p0TXNFbTlUOXR1bUYyUHJROTNM?oc=5`
- Body status: `description_fallback`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
