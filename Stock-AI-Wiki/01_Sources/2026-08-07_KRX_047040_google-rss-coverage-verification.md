---
id: verification-2026-08-07-KRX-047040-google-rss-coverage
type: verification
title: KRX 047040 Google RSS Coverage Verification
created: 2026-08-07
updated: 2026-08-07
status: verification
stage: 1

market: KRX
ticker: "047040"
company: 대우건설
theme: []

source:
  type: local_coverage_csv
  name: google_news_rss_coverage_report_latest.csv
  url: E:\1_Data\2_Logs\google_news_rss_coverage_report_latest.csv
  published_at:
  collected_at: 2026-08-07

analysis:
  summary: Coverage row exists, but source verification remains unknown because original article text is unavailable.
  key_facts:
    - code=047040
    - name=대우건설
    - naver_article_count=1
    - google_rss_article_count=78
    - kis_title_count=18
    - google_rss_covered=True
    - kis_title_covered=True
    - any_covered=True
  related_entities:
    - KRX 047040
    - 대우건설
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

# KRX 047040 Google RSS Coverage Verification

## Source Being Checked
- [[2026-08-07_KRX_047040_google-rss-coverage-source]]

## Facts Checked
- `code=047040`
- `name=대우건설`
- `naver_article_count=1`
- `google_rss_article_count=78`
- `kis_title_count=18`
- `google_rss_covered=True`
- `kis_title_covered=True`
- `any_covered=True`

## RSS Item Metadata Checked
- Title: `김보현 대우건설 대표이사 사임, 후임으로 이강석 부사장 추천 예정 - 비즈니스포스트`
- Source: `비즈니스포스트`
- Published at: `2026-08-06T17:05:21+09:00`
- Link: `https://news.google.com/rss/articles/CBMic0FVX3lxTFA1Ymw4OUl1QklOZzdaMDZMUDEzTjNoemlSeFQ5cDdRNi0xX1VNUW43Z3p2RlJwWHpMa3ZuMXJESjBuMlNmaXVUTVp4Wnp1dFlUeWVWTWdZQl81VDQ5d1FvMmVCQnV5OWl6YjVlMi00QjFBNnM?oc=5`

## Article Body Archive Checked
- not_available

## Verification Result
- `verification_status: unknown`
- `verified: false`

## Trading Boundary
- This verification does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.

<!-- STOCK_AI_AUTO_LINKS_BEGIN -->
## Auto Links
- Updated at: `2026-08-21T19:33:51+09:00`
- Company: [[KRX_047040_대우건설]]
- Article archive seed: `E:\1_Data\Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_2026-08-07.json`
- Latest observation title: `대우건설, 신임 대표이사에 이강석 부사장 추천 예정 - 조선일보`
- Latest observation source: `조선일보`
- Latest observation published_at: `2026-08-06T17:47:00+09:00`
- Latest observation url: `https://news.google.com/rss/articles/CBMiiAFBVV95cUxPYm1jRzZhU0RCVjJtdmdJOTd6aS1LUjZPRDZQY1BCZ2tnR3BhMXZ6bjFINGwzWWZYUngzWTdabkFWY3B1TFVlMkZaQVVfUXBRSVE3MVk0b2VPb1pXQTZMdC0xLWtRU1FLbkU3S3pwNTFGaUdLY0N2SFlTektqZFJjVkgzQi0xVENw?oc=5`
- Body status: `original_text_archived`
- Original text available: `true`
- Concept: [[concept_unclassified-news_미분류-뉴스]]

## Safety Boundary
- This auto-link block does not approve trading, candidates, signals, orders, fills, ledger rows, stats, or gate pass status.
<!-- STOCK_AI_AUTO_LINKS_END -->
